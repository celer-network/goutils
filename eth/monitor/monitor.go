// Copyright 2018-2020 Celer Network

package monitor

import (
	"container/heap"
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"time"

	"github.com/celer-network/goutils/eth/watcher"
	"github.com/celer-network/goutils/log"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	// default event polling interval as a multiplier of WatchService.polling
	// ie. 1 means check log every WatchService.polling
	defaultCheckInterval = uint64(1)
)

type Contract interface {
	GetAddr() common.Address
	GetABI() string
}

// CallbackID is the unique callback ID for deadlines and events
type CallbackID uint64

// Deadline is the metadata of a deadline
type Deadline struct {
	BlockNum *big.Int
	Callback func()
}

// DeadlineQueue is the priority queue for deadlines
type DeadlineQueue []*big.Int

func (dq DeadlineQueue) Len() int { return len(dq) }

func (dq DeadlineQueue) Less(i, j int) bool { return dq[i].Cmp(dq[j]) == -1 }

func (dq DeadlineQueue) Swap(i, j int) { dq[i], dq[j] = dq[j], dq[i] }

func (dq *DeadlineQueue) Push(x interface{}) { *dq = append(*dq, x.(*big.Int)) }

func (dq *DeadlineQueue) Pop() (popped interface{}) {
	popped = (*dq)[len(*dq)-1]
	*dq = (*dq)[:len(*dq)-1]
	return
}

func (dq *DeadlineQueue) Top() (top interface{}) {
	if len(*dq) > 0 {
		top = (*dq)[0]
	}
	return
}

// Config is used by external callers to pass in info, will be converted to Event for internal use
// Reason not use Event directly: Event is more like internal struct
// most fields are from previous MonitorService Monitor func args
// CheckInterval means to check log for event every CheckInterval x WatchService.polling
// if 0 or not set, defaultCheckInterval (1) will be used
type Config struct {
	ChainId   uint64
	Contract  Contract
	EventName string

	StartBlock, EndBlock *big.Int

	Reset         bool
	CheckInterval uint64
	BlockDelay    uint64 // if zero, use service.blockDelay by default
	ForwardDelay  uint64 // if zero, use BlockDelay
}

// Event is the metadata for an event
type Event struct {
	Addr          common.Address
	RawAbi        string
	Name          string
	WatchName     string
	StartBlock    *big.Int
	EndBlock      *big.Int
	BlockDelay    uint64
	ForwardDelay  uint64
	CheckInterval uint64
	Callback      func(CallbackID, types.Log) bool
	watch         *watcher.Watch
}

// Service struct stores service parameters and registered deadlines and events
type Service struct {
	watch         *watcher.WatchService   // persistent watch service
	deadlines     map[CallbackID]Deadline // deadlines
	deadlinecbs   map[string][]CallbackID // deadline callbacks
	deadlineQueue DeadlineQueue           // deadline priority queue
	events        map[CallbackID]Event    // events
	mu            sync.Mutex
	blockDelay    uint64
	enabled       bool
}

// NewService starts a new monitor service.
// If "enabled" is false, event monitoring will be disabled.
func NewService(
	watch *watcher.WatchService, blockDelay uint64, enabled bool) *Service {
	s := &Service{
		watch:       watch,
		deadlines:   make(map[CallbackID]Deadline),
		deadlinecbs: make(map[string][]CallbackID),
		events:      make(map[CallbackID]Event),
		blockDelay:  blockDelay,
		enabled:     enabled,
	}
	return s
}

// Init creates the event map
func (s *Service) Init() {
	heap.Init(&s.deadlineQueue)
	go s.monitorDeadlines() // start monitoring deadlines
}

// Close only set events map to empty map so all monitorEvent will exit due to isEventRemoved is true
func (s *Service) Close() {
	s.mu.Lock()
	s.events = make(map[CallbackID]Event)
	s.mu.Unlock()
}

func (s *Service) GetCurrentBlockNumber() *big.Int {
	return s.watch.GetCurrentBlockNumber()
}

// RegisterDeadline registers the deadline and returns the ID
func (s *Service) RegisterDeadline(d Deadline) CallbackID {
	// get a unique callback ID
	s.mu.Lock()
	defer s.mu.Unlock()
	var id CallbackID
	for {
		id = CallbackID(rand.Uint64())
		if _, exist := s.deadlines[id]; !exist {
			break
		}
	}

	// register deadline
	s.deadlines[id] = d
	_, ok := s.deadlinecbs[d.BlockNum.String()]
	if !ok {
		heap.Push(&s.deadlineQueue, d.BlockNum)
	}
	s.deadlinecbs[d.BlockNum.String()] = append(s.deadlinecbs[d.BlockNum.String()], id)
	return id
}

// continuously monitoring deadlines
func (s *Service) monitorDeadlines() {
	for {
		time.Sleep(2 * time.Second)
		s.mu.Lock()
		blockNumber := s.GetCurrentBlockNumber()
		for s.deadlineQueue.Len() > 0 && s.deadlineQueue.Top().(*big.Int).Cmp(blockNumber) < 1 {
			timeblock := heap.Pop(&s.deadlineQueue).(*big.Int)
			cbs, ok := s.deadlinecbs[timeblock.String()]
			if ok {
				dlCbs := make(map[CallbackID]Deadline)
				for _, id := range cbs {
					deadline, ok := s.deadlines[id]
					if ok {
						dlCbs[id] = deadline
						delete(s.deadlines, id)
					}
				}
				delete(s.deadlinecbs, timeblock.String())

				s.mu.Unlock()
				for _, deadline := range dlCbs {
					deadline.Callback()
				}
				s.mu.Lock()
			}
		}
		s.mu.Unlock()
	}
}

// Create a watch for the given event. Use or skip using the StartBlock
// value from the event: the first time a watch is created for an event,
// the StartBlock should be used.  In follow-up re-creation of the watch
// after the previous watch was disconnected, skip using the StartBlock
// because the watch itself has persistence and knows the most up-to-date
// block to resume from instead of the original event StartBlock which is
// stale information by then. If "reset" is enabled, the watcher ignores the
// previously stored position in the subscription which resets the stream to its start.
func (s *Service) createEventWatch(
	e Event, useStartBlock bool, reset bool) (*watcher.Watch, error) {
	var startBlock *big.Int
	if useStartBlock {
		startBlock = e.StartBlock
	}

	q, err := s.watch.MakeFilterQuery(e.Addr, e.RawAbi, e.Name, startBlock)
	if err != nil {
		return nil, err
	}
	if e.CheckInterval == 0 {
		e.CheckInterval = defaultCheckInterval
	}
	return s.watch.NewWatch(e.WatchName, q, e.BlockDelay, e.ForwardDelay, e.CheckInterval, reset)
}

func (s *Service) Monitor(cfg *Config, callback func(CallbackID, types.Log) bool) (CallbackID, error) {
	if !s.enabled {
		log.Info("monitor disabled, not listening to on-chain logs")
		return 0, nil
	}
	addr := cfg.Contract.GetAddr()
	watchName := NewEventStr(cfg.ChainId, addr, cfg.EventName)
	eventToListen := &Event{
		Addr:          addr,
		RawAbi:        cfg.Contract.GetABI(),
		Name:          cfg.EventName,
		WatchName:     watchName,
		StartBlock:    cfg.StartBlock,
		EndBlock:      cfg.EndBlock,
		BlockDelay:    cfg.BlockDelay,
		ForwardDelay:  cfg.ForwardDelay,
		CheckInterval: cfg.CheckInterval,
		Callback:      callback,
	}
	if eventToListen.BlockDelay == 0 {
		eventToListen.BlockDelay = s.blockDelay
	}
	if eventToListen.ForwardDelay == 0 {
		eventToListen.ForwardDelay = eventToListen.BlockDelay
	}
	if eventToListen.CheckInterval == 0 {
		eventToListen.CheckInterval = defaultCheckInterval
	}
	log.Infof("Starting watch: %s. startBlk: %s, endBlk: %s, blkDelay: %d, checkInterval: %d, reset: %t", watchName,
		cfg.StartBlock, cfg.EndBlock, eventToListen.BlockDelay, eventToListen.CheckInterval, cfg.Reset)
	id, err := s.MonitorEvent(*eventToListen, cfg.Reset)
	if err != nil {
		log.Errorf("Cannot register event %s: %s", watchName, err)
		return 0, err
	}
	return id, nil
}

func (s *Service) MonitorEvent(e Event, reset bool) (CallbackID, error) {
	// Construct the watch now to return up-front errors to the caller.
	w, err := s.createEventWatch(e, true /* useStartBlock */, reset)
	if err != nil {
		log.Errorln("register event error:", err)
		return 0, err
	}

	// get a unique callback ID
	s.mu.Lock()
	var id CallbackID
	for {
		id = CallbackID(rand.Uint64())
		if _, exist := s.events[id]; !exist {
			break
		}
	}
	e.watch = w

	// register event
	s.events[id] = e
	s.mu.Unlock()

	go s.monitorEvent(e, id)

	return id, nil
}

func (s *Service) isEventRemoved(id CallbackID) bool {
	s.mu.Lock()
	_, ok := s.events[id]
	s.mu.Unlock()
	return !ok
}

// subscribes to events using a persistent watcher.
func (s *Service) monitorEvent(e Event, id CallbackID) {
	// WatchEvent blocks until an event is caught
	log.Debugln("monitoring event", e.Name)

	recreate := false // app callback can tell monitor to recreate the watcher
	for {
		var eventLog types.Log
		var err error

		if recreate {
			err = fmt.Errorf("app callback wants watcher recreated")
		} else {
			eventLog, err = e.watch.Next()
		}
		if err != nil {
			log.Errorln("monitoring event error:", e.Name, err)
			e.watch.Close()

			var w *watcher.Watch
			for {
				if s.isEventRemoved(id) {
					e.watch.Close()
					return
				}
				w, err = s.createEventWatch(e, false /* useStartBlock */, false /* reset */)
				if err == nil {
					break
				}
				log.Errorln("recreate event watch error:", e.Name, err)
				time.Sleep(1 * time.Second)
			}

			s.mu.Lock()
			e.watch = w
			s.mu.Unlock()
			log.Debugln("event watch recreated", e.Name)
			recreate = false
			continue
		}

		// When event log is removed due to chain re-org, just ignore it
		// TODO: emit error msg and properly roll back upon catching removed event log
		if eventLog.Removed {
			log.Warnf("Receive removed %s event log", e.Name)
			if err = e.watch.Ack(); err != nil {
				log.Errorln("monitoring event ACK error:", e.Name, err)
				e.watch.Close()
				return
			}
			continue
		}

		// Stop watching if the event was removed
		// TODO: Also stop monitoring if timeout has passed
		if s.isEventRemoved(id) {
			e.watch.Close()
			return
		}

		// The app callback logic could detect cases where the on-chain events arriving
		// are not as expected (e.g. missing or skipped events), which may be caused by
		// inconsistencies between different provider nodes (e.g. Infura).  By returning
		// a "true" recreate bool, it informs the monitor to skip the Ack() on this event
		// and trigger the closing and recreation of the event watcher.  This could make
		// it reconnect to a provider node having up-to-date on-chain events and recover
		// from the inconsistency.
		recreate = e.Callback(id, eventLog)
		if recreate {
			log.Warnln("app callback wants the watcher recreated:", e.Name)
			continue
		}

		if err = e.watch.Ack(); err != nil {
			// This is a coding bug, just exit the loop.
			log.Errorln("monitoring event ACK error:", e.Name, err)
			e.watch.Close()
			return
		}
		if s.isEventRemoved(id) {
			e.watch.Close()
			return
		}
	}
}

// RemoveDeadline removes a deadline from the monitor
func (s *Service) RemoveDeadline(id CallbackID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Debugf("revoke deadline monitoring %d", id)
	delete(s.deadlines, id)
}

// RemoveEvent removes an event from the monitor
func (s *Service) RemoveEvent(id CallbackID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.events[id]
	if ok {
		log.Debugf("revoke event monitoring %d event %s", id, e.Name)
		e.watch.Close()
		delete(s.events, id)
	}
}

// NewEventStr generates the event using chainID, contract address and event name in the format
// "<chainID>-<contractAddr>-<eventName>". For backwards compatibility, If chainID is not set,
// this generates "<contractAddr>-<eventName">.
func NewEventStr(chainId uint64, contractAddr common.Address, eventName string) string {
	encodedAddr := hex.EncodeToString(contractAddr.Bytes())
	if chainId == 0 {
		return fmt.Sprintf("%s-%s", encodedAddr, eventName)
	}
	return fmt.Sprintf("%d-%s-%s", chainId, encodedAddr, eventName)
}
