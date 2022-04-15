package mon2

import (
	"context"
	"math/big"
	"sync"
	"time"

	"github.com/celer-network/goutils/log"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// simplified and efficient monitor util
// base layer Monitor takes care of per chain params
// each MonAddr call has per-addr params

// interfaces
type EthClient interface {
	// so we don't need chainid in func arg
	ChainID(ctx context.Context) (*big.Int, error)
	// new get latest block number, available since geth 1.9.22
	BlockNumber(ctx context.Context) (uint64, error)
	// get logs, q should not set topics to get all events from address
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
}

// DAL is an interface to persist block info to resume note we no longer have event granularity
// instead it'll be contract level only. key will be prefixed with chainid. sprintf("%d-%x", chainid, addr)
type DAL interface {
	GetMonitorBlock(key string) (uint64, int64, bool, error)
	SetMonitorBlock(key string, blockNum uint64, blockIdx int64) error
}

// signature for event callback func, first arg is event name matched by abi, could be empty string if no match
type EventCallback func(string, types.Log)

// per chain config to affect how often call BlockNumber and from/to block in FilterQuery
type PerChainCfg struct {
	BlkIntv time.Duration // interval to call BlockNumber
	// below are params affecting from/to block in query filter
	BlkDelay, MaxBlkDelta, ForwardBlkDelay uint64
}

// mon config for each contract
type PerAddrCfg struct {
	Addr    common.Address  // contract addr
	ChkIntv time.Duration   // interval to call FilterLogs
	AbiStr  string          // XxxABI or XxxMetaData.ABI abi string from this contract's go binding, needed to match log topic to event name, if empty string, evname in callback is also empty
	FromBlk uint64          // optional. if > 0, means ignore persisted blocknum and use this for FromBlk in queries, don't set unless you know what you're doing
	Topics  [][]common.Hash // optional. topic filters. position sensitive. keccak256 hashed values. for usage, see go-ethereum's FilterQuery https://pkg.go.dev/github.com/ethereum/go-ethereum#FilterQuery
}

type Monitor struct {
	ec  EthClient // Ethereum client interface
	dal DAL       // Data access layer to persist blknum/idx of last handled log
	cfg PerChainCfg
	// first set in NewMonitor, then blkNum will be updated every cfg.BlkIntv
	chainId, blkNum uint64

	lock sync.RWMutex // protect blkNum, can be also used to protect ec when later we support replace failed ec
	quit chan bool    // this ch will close in m.Close so all loops know to exit
	// support keep loopUpdateBlkNum running but stop all getLogs, needed for use case that monAddr when first start, then later knows no need, stop to save queries
	stopMon chan bool
}

// cfg is not pointer to ensure value copy and avoid unexpected change by other code
func NewMonitor(ec EthClient, dal DAL, cfg PerChainCfg) (*Monitor, error) {
	chid, err := ec.ChainID(context.Background())
	if err != nil {
		return nil, err
	}
	blkNum, err := ec.BlockNumber(context.Background())
	if err != nil {
		return nil, err
	}
	m := &Monitor{
		ec:      ec,
		dal:     dal,
		cfg:     cfg,
		chainId: chid.Uint64(),
		blkNum:  blkNum,
		quit:    make(chan bool),
		stopMon: make(chan bool),
	}
	go m.loopUpdateBlkNum()
	return m, nil
}

// Stops all MonAddr and updating blk num to release all resources. No longer usable after this call
func (m *Monitor) Close() {
	close(m.quit)
}

// StopMon will quit all MonAddr loops, but keep updating blk num.
func (m *Monitor) StopMon() {
	close(m.stopMon)
}

// return cached block number from last BlockNumber call, no rpc, return in memory value directly
func (m *Monitor) GetBlkNum() uint64 {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.blkNum
}

// given from block, return toblocknum for query consider BlkDelay and MaxBlkDelta
// caller MUST check return value and only do query if returned >= from so from==to is allowed to query
// single block for logs
func (m *Monitor) CalcToBlkNum(from uint64) uint64 {
	curBlkNum := m.GetBlkNum()
	if from+m.cfg.BlkDelay > curBlkNum {
		// no need to query because blkdelay not passed yet
		return 0
	}
	toBlkNum := curBlkNum - m.cfg.BlkDelay
	// if rpc limits how many blocks we can query, don't exceed it
	if m.cfg.MaxBlkDelta > 0 && toBlkNum > from+m.cfg.MaxBlkDelta {
		toBlkNum = from + m.cfg.MaxBlkDelta
	}
	return toBlkNum
}

// return next from blknum based on last To blknum and didn't see any event
// guarantee return value >= lastFrom
func (m *Monitor) CalcNextFromBlkNum(lastFrom, lastTo uint64) uint64 {
	ret := lastTo // default to start from lastTo as we didn't see events, no need to worry about dup
	// for some chain we've seen if lastTo is close to blknum, events may be missing coudl be due to
	// latest state are still inconsisten among nodes. so minus ForwardBlkDelay will help
	if ret+m.cfg.ForwardBlkDelay+m.cfg.BlkDelay >= m.GetBlkNum() {
		ret -= m.cfg.ForwardBlkDelay
	}
	if ret < lastFrom {
		return lastFrom
	}
	return ret
}

// call onchain to get latest blknumber and update cached
// note due to chain re-org, we may receive a new blk num that's smaller than cached
func (m *Monitor) updateBlkNum() {
	// don't lock upfront in case rpc takes long time
	blkNum, err := m.ec.BlockNumber(context.Background())
	if err != nil {
		log.Warnln("get blknum err:", err)
		// todo: switch to backup ec
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.blkNum = blkNum
}

// periodically fetch onchain block number and update m.blkNum
func (m *Monitor) loopUpdateBlkNum() {
	ticker := time.NewTicker(m.cfg.BlkIntv)
	defer ticker.Stop()

	for {
		select {
		case <-m.quit:
			log.Debugln("loopUpdateBlkNum: quit")
			return

		case <-ticker.C:
			m.updateBlkNum()
		}
	}
}

// LogEventID tracks the position of an event by blocknum and its index in the block
type LogEventID struct {
	BlkNum uint64 // Number of the block containing the event
	Index  int64  // Index of the event within the block, use int to support -1 in fast forward case
}

// go over logs and return how many should be skipped
func (le *LogEventID) CountSkip(logs []types.Log) (skipped int) {
	if le == nil {
		return
	}
	for _, elog := range logs {
		if le.ShouldSkip(elog.BlockNumber, elog.Index) {
			skipped += 1
		} else {
			break
		}
	}
	return
}

// compare w/ saved blknum/idx to decide if we already handled this log so skip
func (le *LogEventID) ShouldSkip(blknum uint64, idx uint) bool {
	if le.BlkNum < blknum {
		return false
	}
	if le.BlkNum > blknum {
		return true
	}
	// same block, now compare idx, if idx <= saved idx, means already handled
	return le.Index >= int64(idx)
}

// uint64 to big.Int
func toBigInt(n uint64) *big.Int {
	return new(big.Int).SetUint64(n)
}
