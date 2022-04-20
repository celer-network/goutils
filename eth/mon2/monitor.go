package mon2

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/celer-network/goutils/log"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// start monitoring events from addr with configured interval, calls cbfn for each recived log
// BLOCKING until m is Closed
func (m *Monitor) MonAddr(cfg PerAddrCfg, cbfn EventCallback) {
	key := fmt.Sprintf("%d-%x", m.chainId, cfg.Addr)
	// needed to provide evname to callback func
	topicEvMap := EventIDMap(cfg.AbiStr)

	q := &ethereum.FilterQuery{
		Addresses: []common.Address{cfg.Addr},
		Topics:    cfg.Topics,
	}

	// savedLogID is only non-nil if we are resuming from db blockNum and blockIdx, used to skip
	// already processed events on the same block
	var savedLogID *LogEventID

	// if cfg explicitly has FromBlk, use it, otherwise try to figure out based on db and m.blkNum
	if cfg.FromBlk > 0 {
		q.FromBlock = toBigInt(cfg.FromBlk)
	} else {
		savedLogID = m.initFromInQ(q, key)
	}

	log.Infoln("start monitoring", key, "interval:", cfg.ChkIntv, "fromBlk:", q.FromBlock)

	// create ticker by cfg.ChkIntv
	ticker := time.NewTicker(cfg.ChkIntv)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopMon:
			log.Infoln("MonAddr:", key, "stopped")
			return

		case <-m.quit:
			log.Infoln("MonAddr:", key, "quit")
			return

		case <-ticker.C:
			// q.FromBlock is set before first ticker and updated in the end of each ticker
			fromBlk := q.FromBlock.Uint64()
			toBlk := m.CalcToBlkNum(fromBlk)
			if toBlk < fromBlk {
				continue // no need to query yet
			}
			q.ToBlock = toBigInt(toBlk)
			// call m.ec.FilterLogs and skip already logs before savedLogID, if savedLogID is nil, return all received logs
			todoLogs, err := m.doOneQuery(q, savedLogID)
			if err != nil {
				continue // keep same fromBlk and try again in next ticker, next to may be different
			}
			// now go over todoLogs and call callback func
			// it's possible all have been skipped so we don't do anything
			for _, elog := range todoLogs {
				if elog.Removed {
					log.Debugln("skip removed log:", key, elog.BlockNumber, elog.Index)
					continue
				}
				cbfn(topicEvMap[elog.Topics[0]], elog)
			}

			var nextFrom uint64
			// if len(todoLogs) > 0, we've handled new logs, should update db and next fromBlock,
			if len(todoLogs) > 0 {
				// last handled log's blknum + 1, we don't move to toBlock in case there are inconsistency
				// between nodes, so handled log blk+1 will guarantee no miss event
				lastHandledLog := todoLogs[len(todoLogs)-1]
				nextFrom = lastHandledLog.BlockNumber + 1
				m.dal.SetMonitorBlock(key, lastHandledLog.BlockNumber, int64(lastHandledLog.Index))
			} else {
				// didn't handle any new log, fast forward block
				nextFrom = m.CalcNextFromBlkNum(fromBlk, toBlk)
				// if nextFrom is the same as fromBlk, we must not override blknum/idx in db
				if nextFrom > fromBlk {
					// if nextFrom is larger, we set nextFrom and -1 idx so idx 0 log will be handled
					// note this is an optimization to avoid resume from last seen log position if contract is mostly idle
					// ie. if process restarts, we'll start from a safe from instead of last seen log blknum which may be long time ago
					m.dal.SetMonitorBlock(key, nextFrom, -1)
				}
			}
			// update q.FromBlock
			q.FromBlock.SetUint64(nextFrom)

			// set savedLogID to nil so next ticker won't need to check again because from is bigger
			if savedLogID != nil && nextFrom > savedLogID.BlkNum {
				savedLogID = nil
			}
		}
	}
}

// set q.FromBLock from db or latest blkNum. return non-nil if resumed from db
func (m *Monitor) initFromInQ(q *ethereum.FilterQuery, key string) *LogEventID {
	// try resume from last saved block and log idx
	blockNum, blockIdx, found, err := m.dal.GetMonitorBlock(key)
	if err == nil && found {
		q.FromBlock = toBigInt(blockNum)
		return &LogEventID{
			BlkNum: blockNum,
			Index:  blockIdx, // this may be -1 in fast forward case
		}
	}
	// not found in db, use current blkNum
	q.FromBlock = toBigInt(m.GetBlkNum())
	return nil
}

// calls FilterLogs and skip already processed log blk/idx, if FilterLogs returns non-nil err, return nil logs and err directly
func (m *Monitor) doOneQuery(q *ethereum.FilterQuery, savedLogID *LogEventID) ([]types.Log, error) {
	logs, err := m.ec.FilterLogs(context.TODO(), *q)
	if err != nil {
		log.Warnf("%d-%x getLogs fromBlk %s toBlk %s failed. err: %v", m.chainId, q.Addresses[0], q.FromBlock, q.ToBlock, err)
	}
	if len(logs) == 0 {
		return logs, nil
	}
	// if resume from db and on first ticker, as fromblock is same as db, we may get same events again
	// how many logs should be skipped, only could be non-zero if savedLogID isn't nil
	// if savedLogID is nil, return 0 directly
	return logs[savedLogID.CountSkip(logs):], nil
}

// parse abi and return map from event.ID to its name eg. Deposited
// if abistr is empty string, return empty map (not nil)
func EventIDMap(abistr string) map[common.Hash]string {
	// now build topic to ev name map
	topicEvMap := make(map[common.Hash]string)
	if abistr == "" {
		return topicEvMap
	}
	parsedAbi, _ := abi.JSON(strings.NewReader(abistr))
	for evname, ev := range parsedAbi.Events {
		topicEvMap[ev.ID] = evname
	}
	return topicEvMap
}
