// Copyright 2018-2020 Celer Network

package monitor_test

import (
	"container/heap"
	"context"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/celer-network/goutils/eth/monitor"
	"github.com/celer-network/goutils/eth/watcher"
	"github.com/celer-network/goutils/log"
	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Fake Ethereum client.
type fakeClient struct {
	quit    chan bool
	blkChan chan int64
}

func NewFakeClient() *fakeClient {
	fc := &fakeClient{
		quit:    make(chan bool),
		blkChan: make(chan int64),
	}
	go fc.blkTick()
	return fc
}

func (fc *fakeClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	blkNum := <-fc.blkChan
	head := &types.Header{
		Number: big.NewInt(blkNum),
	}
	return head, nil
}

func (fc *fakeClient) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	blkNum := <-fc.blkChan

	logs := []types.Log{
		types.Log{
			Data:        []byte("dummy data"),
			BlockNumber: uint64(blkNum),
			Index:       0,
		},
	}
	return logs, nil
}

func (fc *fakeClient) Close() {
	close(fc.quit)
}

func (fc *fakeClient) blkTick() {
	blkNum := int64(1)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-fc.quit:
			return

		case <-ticker.C:
			blkNum++

		case fc.blkChan <- blkNum:
		}
	}
}

type mdata struct {
	blockNum uint64
	blockIdx int64
	restart  bool
}

// fake watcher data access layer
type fakeDAL struct {
	data map[string]*mdata
}

func NewFakeDAL() *fakeDAL {
	return &fakeDAL{data: make(map[string]*mdata)}
}

func (fd *fakeDAL) InsertMonitor(event string, blockNum uint64, blockIdx int64, restart bool) error {
	if fd.data[event] != nil {
		return fmt.Errorf("event already exists")
	}
	fd.data[event] = &mdata{
		blockNum: blockNum,
		blockIdx: blockIdx,
		restart:  restart,
	}
	return nil
}

func (fd *fakeDAL) GetMonitorBlock(event string) (uint64, int64, bool, error) {
	if fd.data[event] == nil {
		return 0, 0, false, nil
	}
	return fd.data[event].blockNum, fd.data[event].blockIdx, true, nil
}

func (fd *fakeDAL) UpdateMonitorBlock(event string, blockNum uint64, blockIdx int64) error {
	if fd.data[event] == nil {
		return fmt.Errorf("event not exist")
	}
	fd.data[event].blockNum = blockNum
	fd.data[event].blockIdx = blockIdx
	return nil
}

func (fd *fakeDAL) UpsertMonitorBlock(event string, blockNum uint64, blockIdx int64, restart bool) error {
	if fd.data[event] != nil {
		fd.data[event].blockNum = blockNum
		fd.data[event].blockIdx = blockIdx
	} else {
		fd.data[event] = &mdata{
			blockNum: blockNum,
			blockIdx: blockIdx,
			restart:  restart,
		}
	}
	return nil
}

// Creates a watch service with a KVStore and a fake Ethereum client.
// Returns the watch service, its fake client, and the KVStore directory.
func watchService() (*watcher.WatchService, *fakeClient, error) {
	dal := NewFakeDAL()
	client := NewFakeClient()

	ws := watcher.NewWatchService(client, dal, 1)
	if ws == nil {
		return nil, nil, fmt.Errorf("cannot create watch service")
	}
	return ws, client, nil
}

var deadline_cb_count uint64

func deadline_callback() {
	n := atomic.AddUint64(&deadline_cb_count, 1)
	log.Infof("Call deadline callback: %d", n)
}

func TestDeadline(t *testing.T) {
	deadline_cb_count = 0

	ws, client, err := watchService()
	if err != nil {
		t.Fatalf("fail to create watch service: %s", err)
	}
	defer client.Close()
	defer ws.Close()

	ms := monitor.NewService(ws, 0 /* blockDelay */, true /* enabled */)
	ms.Init()

	// Note: the block number producer is ticking every 10ms.
	// The code below adds 4 deadline events then removes one
	// before the block number reaches the one removed, thus
	// only 3 callbacks should trigger.

	e1 := monitor.Deadline{
		BlockNum: big.NewInt(100),
		Callback: deadline_callback,
	}
	id1 := ms.RegisterDeadline(e1)
	log.Infof("Register callback %d\n", id1)

	e2 := monitor.Deadline{
		BlockNum: big.NewInt(90),
		Callback: deadline_callback,
	}
	id2 := ms.RegisterDeadline(e2)
	log.Infof("Register callback %d\n", id2)

	e3 := monitor.Deadline{
		BlockNum: big.NewInt(1000000),
		Callback: deadline_callback,
	}
	id3 := ms.RegisterDeadline(e3)
	log.Infof("Register callback %d\n", id3)

	e4 := monitor.Deadline{
		BlockNum: big.NewInt(80),
		Callback: deadline_callback,
	}
	id4 := ms.RegisterDeadline(e4)
	log.Infof("Register callback %d\n", id4)

	ms.RemoveDeadline(id3)

	time.Sleep(3 * time.Second)

	count := atomic.LoadUint64(&deadline_cb_count)
	if count != 3 {
		t.Errorf("wrong count of deadline callback: %d != 3", count)
	}
}

func TestDeadlineQueue(t *testing.T) {
	dq := &monitor.DeadlineQueue{}
	heap.Init(dq)

	heap.Push(dq, big.NewInt(2))
	heap.Push(dq, big.NewInt(4))
	heap.Push(dq, big.NewInt(3))
	heap.Push(dq, big.NewInt(5))
	heap.Push(dq, big.NewInt(1))

	for dq.Len() != 0 {
		// dequeue
		log.Infoln(dq.Top())
		log.Infoln(heap.Pop(dq))
	}
}

func event_callback(id monitor.CallbackID, ethlog types.Log) bool {
	log.Infof("Call event callback with log: %v\n", ethlog)
	time.Sleep(200 * time.Millisecond)
	return false
}

var (
	ev_cb_recreate = true
)

func event_callback_recreate(id monitor.CallbackID, ethlog types.Log) bool {
	log.Infof("Call event callback (recreate %t) with log: %v\n", ev_cb_recreate, ethlog)
	time.Sleep(200 * time.Millisecond)
	ret := ev_cb_recreate
	ev_cb_recreate = false
	return ret
}

func TestEvent(t *testing.T) {
	ws, client, err := watchService()
	if err != nil {
		t.Fatalf("fail to create watch service: %s", err)
	}
	defer client.Close()
	defer ws.Close()

	ms := monitor.NewService(ws, 0 /* blockDelay */, true /* enabled */)
	ms.Init()

	ledgerMigrateABI := "[{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"channelId\",\"type\":\"bytes32\"},{\"indexed\":true,\"name\":\"newLedgerAddr\",\"type\":\"address\"}],\"name\":\"MigrateChannelTo\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"channelId\",\"type\":\"bytes32\"},{\"indexed\":true,\"name\":\"oldLedgerAddr\",\"type\":\"address\"}],\"name\":\"MigrateChannelFrom\",\"type\":\"event\"}]"
	addr := "5963e46cf9f9700e70d4d1bc09210711ab4a20b4"
	e1 := monitor.Event{
		Name:      "MigrateChannelTo",
		Addr:      common.HexToAddress(addr),
		RawAbi:    ledgerMigrateABI,
		WatchName: "Event 1",
		Callback:  event_callback,
	}

	id1, err := ms.MonitorEvent(e1, false /* reset */)
	if err != nil {
		t.Fatalf("register event #1 failed: %s", err)
	}
	log.Infof("Register callback %d\n", id1)

	e2 := monitor.Event{
		Name:      "MigrateChannelFrom",
		Addr:      common.HexToAddress(addr),
		RawAbi:    ledgerMigrateABI,
		WatchName: "Event 2",
		Callback:  event_callback,
	}

	id2, err := ms.MonitorEvent(e2, false /* reset */)
	if err != nil {
		t.Fatalf("register event #2 failed: %s", err)
	}
	log.Infof("Register callback %d\n", id2)

	e3 := monitor.Event{
		Name:      "MigrateChannelFrom",
		Addr:      common.HexToAddress(addr),
		RawAbi:    ledgerMigrateABI,
		WatchName: "Event 3",
		Callback:  event_callback_recreate,
	}

	id3, err := ms.MonitorEvent(e3, false /* reset */)
	if err != nil {
		t.Fatalf("register event #3 failed: %s", err)
	}
	log.Infof("Register callback %d\n", id3)

	time.Sleep(1 * time.Second)
	ms.RemoveEvent(id1)

	time.Sleep(2 * time.Second)
}
