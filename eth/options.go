// Copyright 2020 Celer Network

package eth

import (
	"math/big"
	"time"
)

type txOptions struct {
	// Transact
	ethValue *big.Int
	// Legacy Tx gas price
	minGasGwei   uint64
	maxGasGwei   uint64
	addGasGwei   uint64
	forceGasGwei uint64
	// EIP-1559 Tx gas price
	maxFeePerGasGwei         uint64
	maxPriorityFeePerGasGwei uint64
	// Gas limit
	gasLimit            uint64
	addGasEstimateRatio float64

	// WaitMined
	blockDelay         uint64
	pollingInterval    time.Duration
	timeout            time.Duration
	queryTimeout       time.Duration
	queryRetryInterval time.Duration
	dropDetection      bool
}

const (
	defaultPollingInterval      = 15 * time.Second
	defaultTxTimeout            = 6 * time.Hour
	defaultTxQueryTimeout       = 2 * time.Minute
	defaultTxQueryRetryInterval = 10 * time.Second
)

// do not return pointer here as defaultTxOptions is always deep copied when used
func defaultTxOptions() txOptions {
	return txOptions{
		pollingInterval:    defaultPollingInterval,
		timeout:            defaultTxTimeout,
		queryTimeout:       defaultTxQueryTimeout,
		queryRetryInterval: defaultTxQueryRetryInterval,
	}
}

type TxOption interface {
	apply(*txOptions)
}

type funcTxOption struct {
	f func(*txOptions)
}

func (fdo *funcTxOption) apply(do *txOptions) {
	fdo.f(do)
}

func newFuncTxOption(f func(*txOptions)) *funcTxOption {
	return &funcTxOption{
		f: f,
	}
}

func WithEthValue(v *big.Int) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		o.ethValue = v
	})
}

func WithMinGasGwei(g uint64) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		o.minGasGwei = g
	})
}

func WithMaxGasGwei(g uint64) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		o.maxGasGwei = g
	})
}

func WithAddGasGwei(g uint64) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		o.addGasGwei = g
	})
}

func WithForceGasGwei(g uint64) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		o.forceGasGwei = g
	})
}

func WithMaxFeePerGasGwei(g uint64) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		o.maxFeePerGasGwei = g
	})
}

func WithMaxPriorityFeePerGasGwei(g uint64) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		o.maxPriorityFeePerGasGwei = g
	})
}

func WithGasLimit(l uint64) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		o.gasLimit = l
	})
}

func WithAddGasEstimateRatio(r float64) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		o.addGasEstimateRatio = r
	})
}

func WithBlockDelay(d uint64) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		o.blockDelay = d
	})
}

func WithPollingInterval(t time.Duration) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		if t != 0 {
			o.pollingInterval = t
		}
	})
}

func WithTimeout(t time.Duration) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		if t != 0 {
			o.timeout = t
		}
	})
}

func WithQueryTimeout(t time.Duration) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		if t != 0 {
			o.queryTimeout = t
		}
	})
}

func WithQueryRetryInterval(t time.Duration) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		if t != 0 {
			o.queryRetryInterval = t
		}
	})
}

func WithDropDetection(d bool) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		o.dropDetection = d
	})
}
