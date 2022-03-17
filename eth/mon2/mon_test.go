package mon2

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestMon(t *testing.T) {
	// todo: add tests
}

// mock eth client
type MockEc struct {
	chid, blkNum uint64
	// logs to be returned in next FilterLogs call
	logs []types.Log
}

func (ec *MockEc) ChainID(ctx context.Context) (*big.Int, error) {
	return new(big.Int).SetUint64(ec.chid), nil
}

// incr by 1 everytime this is called
func (ec *MockEc) BlockNumber(ctx context.Context) (uint64, error) {
	ec.blkNum += 1
	return ec.blkNum, nil
}

func (ec *MockEc) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
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
	var fakeTopic common.Hash
	return types.Log{
		BlockNumber: blkNum,
		Index:       idx,
		Topics:      []common.Hash{fakeTopic},
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
