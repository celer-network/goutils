// Copyright 2020 Celer Network

package eth

import (
	"math/big"
	"sync"
	"time"
)

type config struct {
	txTimeout, txQueryTimeout, txQueryRetryInterval time.Duration // used by waitmined
	minGasGwei, maxGasGwei                          uint64        // used by transactor
	blockDelay, quickCatchBlockDelay                uint64        // used by transactor
	blockPollingIntervalSec                         uint64        // used by transactor
	chainId                                         *big.Int      // used by signer
}

var conf = &config{
	txTimeout:               6 * time.Hour,
	txQueryTimeout:          2 * time.Minute,
	txQueryRetryInterval:    10 * time.Second,
	blockDelay:              5,
	quickCatchBlockDelay:    2,
	blockPollingIntervalSec: 15,
}

// Currently use package level singleton config for simplicity.
// This is could be a bit risky as multiple libs may override config against each other.
// TODO: enable per-object and per-method config
var confLock sync.RWMutex

func SetWaitMinedConfig(txTimeoutSec, txQueryTimeoutSec, txQueryRetryIntervalSec uint64) {
	confLock.Lock()
	defer confLock.Unlock()
	if txTimeoutSec != 0 {
		conf.txTimeout = time.Duration(txTimeoutSec) * time.Second
	}
	if txQueryTimeoutSec != 0 {
		conf.txQueryTimeout = time.Duration(txQueryTimeoutSec) * time.Second
	}
	if txQueryRetryIntervalSec != 0 {
		conf.txQueryRetryInterval = time.Duration(txQueryRetryIntervalSec) * time.Second
	}
}

func SetGasLimit(minGasGwei, maxGasGwei uint64) {
	confLock.Lock()
	defer confLock.Unlock()
	conf.minGasGwei = minGasGwei
	conf.maxGasGwei = maxGasGwei
}

func SetBlockDelay(blockDelay uint64) {
	confLock.Lock()
	defer confLock.Unlock()
	conf.blockDelay = blockDelay
}

func SetQuickCatchBlockDelay(quickCatchBlockDelay uint64) {
	confLock.Lock()
	defer confLock.Unlock()
	conf.quickCatchBlockDelay = quickCatchBlockDelay
}

func SetBlockPollingInterval(pollingIntervalSec uint64) {
	confLock.Lock()
	defer confLock.Unlock()
	if pollingIntervalSec != 0 {
		conf.blockPollingIntervalSec = pollingIntervalSec
	}
}

func SetChainId(chainId *big.Int) {
	confLock.Lock()
	defer confLock.Unlock()
	conf.chainId = chainId
}

func GetTxTimeout() time.Duration {
	confLock.RLock()
	defer confLock.RUnlock()
	return conf.txTimeout
}

func GetTxQueryTimeout() time.Duration {
	confLock.RLock()
	defer confLock.RUnlock()
	return conf.txQueryTimeout
}

func GetTxQueryRetryInterval() time.Duration {
	confLock.RLock()
	defer confLock.RUnlock()
	return conf.txQueryRetryInterval
}

func GetMinGasGwei() uint64 {
	confLock.RLock()
	defer confLock.RUnlock()
	return conf.minGasGwei
}

func GetMaxGasGwei() uint64 {
	confLock.RLock()
	defer confLock.RUnlock()
	return conf.maxGasGwei
}

func GetBlockDelay() uint64 {
	confLock.RLock()
	defer confLock.RUnlock()
	return conf.blockDelay
}

func GetQuickCatchBlockDelay() uint64 {
	confLock.RLock()
	defer confLock.RUnlock()
	return conf.quickCatchBlockDelay
}

func GetBlockPollingIntervalSec() uint64 {
	confLock.RLock()
	defer confLock.RUnlock()
	return conf.blockPollingIntervalSec
}

func GetChainId() *big.Int {
	confLock.RLock()
	defer confLock.RUnlock()
	return conf.chainId
}
