package mon2

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var zeroHash common.Hash

const zeroKey = "0-0000000000000000000000000000000000000000"

// only scalar types
func chkEq(v, exp interface{}, t *testing.T) {
	if v != exp {
		t.Errorf("mismatch value exp: %v, got %v", exp, v)
	}
}

func TestDoOneQuery(t *testing.T) {
	ec := new(MockEc)
	m := &Monitor{
		ec: ec,
	}
	q := &ethereum.FilterQuery{
		FromBlock: toBigInt(50),
		ToBlock:   toBigInt(80),
	}
	saved := &LogEventID{50, 10}
	// first 2 logs should be skipped
	ec.addLog(50, 2)
	ec.addLog(50, 10)
	ec.addLog(60, 1)
	gotLogs := m.doOneQuery(q, saved)
	chkEq(len(gotLogs), 1, t)
	chkEq(gotLogs[0].BlockNumber, uint64(60), t)
	chkEq(gotLogs[0].Index, uint(1), t)
}

// resume from db saved event, or use blknum
func TestInitFromInQ(t *testing.T) {
	dal := make(MockDAL)
	expLog := LogEventID{100, 10}
	dbkey := "key1"
	dal[dbkey] = expLog
	m := &Monitor{
		dal:    dal,
		blkNum: 123,
	}
	q := &ethereum.FilterQuery{
		FromBlock: new(big.Int), // will be changed in initFromInQ
	}
	saved := m.initFromInQ(q, dbkey)
	chkEq(q.FromBlock.Uint64(), expLog.BlkNum, t)
	chkEq(saved.BlkNum, expLog.BlkNum, t)
	chkEq(saved.Index, expLog.Index, t)
	// not found in db, use m.blkNum case
	dbkey = "key2"
	saved = m.initFromInQ(q, dbkey)
	if saved != nil {
		t.Error("expect nil to be returned by initFromInQ, but got: ", saved)
	}
	chkEq(q.FromBlock.Uint64(), m.blkNum, t)
}

func TestCalcToBlkNum(t *testing.T) {
	// tdata is array of test input and expected result
	// each row is one case, with 5 uint64, they are m.blkNum, cfg.BlkDelay and MaxBlkDelta
	// 4th is the from arg to CalcToBlkNum
	// 5th is expected return toBlk from CalcToBlkNum
	tdata := [][5]uint64{
		{10, 0, 0, 10, 10}, // current blk = from, blkdelay 0, so to must eq from
		{10, 1, 0, 10, 0},  // from+blkdelay > cur blknum, return 0, so won't fetch
		{10, 1, 0, 9, 9},   // from+delay=cur blk, to should eq from
		{10, 1, 0, 6, 9},   // normal case, to = cur blk - delay
		{10, 3, 0, 6, 7},   // normal case again
		{8, 1, 0, 10, 0},   // chain re-org, cur blknum < from
		// now maxblkdelta isn't 0, will cap toBlk
		{100, 5, 10, 90, 95}, // not exceed max delta, to = cur - from
		{100, 5, 10, 80, 90}, // hit max delta, to = from + maxdelta
	}
	m := &Monitor{
		cfg: PerChainCfg{},
	}
	for _, d := range tdata {
		m.blkNum = d[0]
		m.cfg.BlkDelay = d[1]
		m.cfg.MaxBlkDelta = d[2]
		chkEq(m.CalcToBlkNum(d[3]), d[4], t)
	}
}

func TestCalcNextFrom(t *testing.T) {
	// each row is
	// blknum, blkdelay, fwdBlkDelay, lastfrom, lastto, expect next from
	tdata := [][6]uint64{
		{100, 5, 0, 80, 90, 90},   // 0 fwdBlkDelay, nextfrom = lastto
		{100, 5, 0, 80, 100, 100}, // 0 fwdBlkDelay, nextfrom = lastto
		{100, 5, 0, 80, 110, 110}, // chain re-org, nexfrom = lastto
		// now non-zero fwdBlkdelay
		{100, 10, 20, 50, 90, 70}, // lastTo+blkdelay+fwdDelay > blknum, minus fwddelay
		{100, 10, 20, 80, 90, 80}, // lastTo+blkdelay+fwdDelay > blknum, minus fwddelay but less than lastfrom, set to lastfrom
	}
	m := &Monitor{
		cfg: PerChainCfg{},
	}
	for _, d := range tdata {
		m.blkNum = d[0]
		m.cfg.BlkDelay = d[1]
		m.cfg.ForwardBlkDelay = d[2]
		chkEq(m.CalcNextFromBlkNum(d[3], d[4]), d[5], t)
	}
}

// test from and to in FilterQuery are set correctly
func TestFilterQuery(t *testing.T) {
	dal := make(MockDAL)
	ec := &MockEc{
		T:      t,
		blkNum: 100,
	}
	// m starts w/ blknum 100
	m, _ := NewMonitor(ec, dal, PerChainCfg{
		BlkIntv:  time.Minute, // we'll manually call updateBlkNum
		BlkDelay: 5,
	})
	defer m.Close()
	chkIntv := time.Millisecond // increase this if test err on slow/busy machine
	go m.MonAddr(PerAddrCfg{
		ChkIntv: chkIntv,
		FromBlk: 50, // explicit set, will take effect
	}, func(string, types.Log) {})

	ec.addLog(75, 10) // one log at blk 75
	// when ec.FilterLogs first gets called, q.From should be 50, q.To should be 100-5
	// one log at 75, so next from is 76
	ec.expFrom = append(ec.expFrom, 50, 76, 95)
	ec.expTo = append(ec.expTo, 95, 95, 95) // 95 = 100 - 5

	// enough time to ensure MonAddr ticker triggers several times
	time.Sleep(time.Duration(len(ec.expFrom)+1) * chkIntv)
	// make sure expFrom/expTo are empty now, meaning FilterLogs has been calld
	chkEq(len(ec.expFrom), 0, t)
	chkEq(len(ec.expTo), 0, t)
	// check dal has correct fromblk/index, as last query has no logs, it should be CalcNextFromBlkNum
	chkEq(dal[zeroKey].BlkNum, uint64(95), t)
	chkEq(dal[zeroKey].Index, int64(-1), t)
}

// mock eth client
type MockEc struct {
	*testing.T
	chid, blkNum uint64
	// when FilterLogs is called, expected value for q.FromBlock and q.ToBlock
	// will be popped in each call
	expFrom, expTo []uint64
	// logs to be returned in next FilterLogs call
	logs []types.Log
}

func (ec *MockEc) ChainID(ctx context.Context) (*big.Int, error) {
	return new(big.Int).SetUint64(ec.chid), nil
}

// incr by 1 everytime this is called
func (ec *MockEc) BlockNumber(ctx context.Context) (uint64, error) {
	return ec.blkNum, nil
}

// pop first from expFrom/expTo and compare, if list is empty, do nothing
func (ec *MockEc) chkFromTo(qfrom, qto uint64) {
	var exp uint64
	if len(ec.expFrom) > 0 {
		exp, ec.expFrom = ec.expFrom[0], ec.expFrom[1:]
		chkEq(qfrom, exp, ec.T)
	}
	if len(ec.expTo) > 0 {
		exp, ec.expTo = ec.expTo[0], ec.expTo[1:]
		chkEq(qto, exp, ec.T)
	}
}

func (ec *MockEc) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	ec.chkFromTo(q.FromBlock.Uint64(), q.ToBlock.Uint64())
	var ret []types.Log
	keep := ec.logs[:0] // share same backing array and capacity as ec.logs so can modify it in-place
	for _, elog := range ec.logs {
		if elog.BlockNumber < q.FromBlock.Uint64() {
			continue // skip old logs
		} else if elog.BlockNumber <= q.ToBlock.Uint64() {
			ret = append(ret, elog)
		} else {
			// logs for next fetch, in-place modify ec.logs
			// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
			keep = append(keep, elog)
		}
	}
	return ret, nil
}

// append to the end of ec.logs, if ec.logs isn't empty, check new blkNum and idx is bigger than last log
func (ec *MockEc) addLog(blkNum uint64, idx uint) {
	loglen := len(ec.logs)
	if loglen > 0 {
		if blkNum < ec.logs[loglen-1].BlockNumber {
			panic("new log blkNum < last log")
		}
		if blkNum == ec.logs[loglen-1].BlockNumber && idx <= ec.logs[loglen-1].Index {
			panic("new log same blkNum as last but idx <= last log idx")
		}
	}
	ec.logs = append(ec.logs, newLog(blkNum, idx))
}

func newLog(blkNum uint64, idx uint) types.Log {
	return types.Log{
		BlockNumber: blkNum,
		Index:       idx,
		Topics:      []common.Hash{zeroHash},
	}
}

type MockDAL map[string]LogEventID

func (d MockDAL) GetMonitorBlock(key string) (uint64, int64, bool, error) {
	le, ok := d[key]
	return le.BlkNum, le.Index, ok, nil
}

func (d MockDAL) SetMonitorBlock(key string, blockNum uint64, blockIdx int64) error {
	d[key] = LogEventID{BlkNum: blockNum, Index: blockIdx}
	return nil
}
