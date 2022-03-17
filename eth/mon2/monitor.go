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
)

// start monitoring events from addr with configured interval, calls cbfn for each recived log
func (m *Monitor) MonAddr(cfg PerAddrCfg, cbfn EventCallback) {
	key := fmt.Sprintf("%d-%x", m.chainId, cfg.Addr)
	// needed to provide evname to callback func
	topicEvMap := EventIDMap(cfg.AbiStr)

	// no topics to receive all events from this address
	q := ethereum.FilterQuery{
		Addresses: []common.Address{cfg.Addr},
	}

	// savedLogID is only non-nil if we are resuming from db blockNum and blockIdx, used to skip
	// already processed events on the same block
	var savedLogID *LogEventID

	// now figure out first query's fromblock, default to current, if FromBlock is set, use it
	// otherwise try to get blknum from db if found, use it.
	q.FromBlock = toBigInt(m.blkNum)
	// use FromBlk if it's set
	if cfg.FromBlk > 0 {
		q.FromBlock.SetUint64(cfg.FromBlk)
	} else {
		// try resume from last saved block and log idx
		blockNum, blockIdx, found, err := m.dal.GetMonitorBlock(key)
		if err == nil && found {
			q.FromBlock.SetUint64(blockNum)
			// set to last saved index so we can skip already processed logs from same block
			savedLogID = &LogEventID{
				BlkNum: blockNum,
				Index:  blockIdx, // this may be -1 in fast forward case
			}
		}
	}

	log.Infoln("start monitoring", key, "interval:", cfg.ChkInterval, "fromBlk:", q.FromBlock)

	// create ticker by cfg.ChkInterval
	ticker := time.NewTicker(cfg.ChkInterval)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-m.quit:
				log.Debugln("MonAddr:", key, "quit")
				return

			case <-ticker.C:
				// q.FromBlock is set before first ticker and updated in the end of each ticker
				fromBlk := q.FromBlock.Uint64()
				toBlk := m.CalcToBlkNum(fromBlk)
				if toBlk < fromBlk {
					continue // no need to query yet
				}
				q.ToBlock = toBigInt(toBlk)

				logs, err := m.ec.FilterLogs(context.TODO(), q)
				if err != nil {
					log.Warnln(key, "getlogs failed. err:", err)
				}

				// if resume from db and on first ticker, as fromblock is same as db, we may get same events again
				// how many logs should be skipped, only could be non-zero if savedLogID isn't nil
				// if savedLogID is nil, return 0 directly
				skipped := savedLogID.CountSkip(logs)

				// now go over logs[skipped:], and call callback func
				// it's possible all have been skipped so we don't do anything
				todoLogs := logs[skipped:]
				for _, elog := range todoLogs {
					if elog.Removed {
						log.Debugln("skip removed log:", key, elog.BlockNumber, elog.Index)
						continue
					}
					cbfn(topicEvMap[elog.Topics[0]], elog)
				}

				// if len(todoLogs) > 0, we've handled new logs, should update db and next fromBlock,
				var nextFrom uint64
				if len(todoLogs) > 0 {
					// last handled log's blknum + 1
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
	}()
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
