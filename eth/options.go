// Copyright 2020 Celer Network

package eth

import (
	"math/big"
	"time"
)

type txOptions struct {
	ethValue           *big.Int      // transact
	minGasGwei         uint64        // transact
	maxGasGwei         uint64        // transact
	addGasGwei         uint64        // transact
	gasLimit           uint64        // transact
	blockDelay         uint64        // waitMined
	pollingInterval    time.Duration // waitMined
	timeout            time.Duration // waitMined
	queryTimeout       time.Duration // waitMined
	queryRetryInterval time.Duration // waitMined
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

func WithGasLimit(l uint64) TxOption {
	return newFuncTxOption(func(o *txOptions) {
		o.gasLimit = l
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
