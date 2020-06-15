// Copyright 2018-2020 Celer Network

package eth

import (
	"fmt"
	"math/big"
	"sync"

	"github.com/celer-network/goutils/log"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type TransactorPool struct {
	transactors []*Transactor
	current     int
	mu          sync.Mutex
}

type TransactorConfig struct {
	Keyjson    string
	Passphrase string
}

func NewTransactorConfig(keyjson string, passphrase string) *TransactorConfig {
	return &TransactorConfig{Keyjson: keyjson, Passphrase: passphrase}
}

func NewTransactorPool(transactors []*Transactor) (*TransactorPool, error) {
	if len(transactors) == 0 {
		return nil, fmt.Errorf("Empty transactor TransactorPool")
	}
	return &TransactorPool{transactors: transactors, current: 0}, nil
}

func NewTransactorPoolFromConfig(
	client *ethclient.Client,
	configs []*TransactorConfig,
	chainId *big.Int,
	opts ...TxOption) (*TransactorPool, error) {
	transactors := []*Transactor{}
	for _, config := range configs {
		transactor, err := NewTransactor(config.Keyjson, config.Passphrase, client, chainId, opts...)
		if err != nil {
			log.Errorln(err)
		} else {
			transactors = append(transactors, transactor)
		}
	}
	return NewTransactorPool(transactors)
}

func (p *TransactorPool) Submit(
	handler *TransactionStateHandler,
	method TxMethod,
	opts ...TxOption) (*types.Transaction, error) {
	return p.nextTransactor().Transact(handler, method, opts...)
}

func (p *TransactorPool) SubmitWaitMined(
	description string,
	method TxMethod,
	opts ...TxOption) (*types.Receipt, error) {
	return p.nextTransactor().TransactWaitMined(description, method, opts...)
}

func (p *TransactorPool) nextTransactor() *Transactor {
	p.mu.Lock()
	defer p.mu.Unlock()
	current := p.current
	p.current = (p.current + 1) % len(p.transactors)
	return p.transactors[current]
}

func (p *TransactorPool) ContractCaller() bind.ContractCaller {
	return p.nextTransactor().client
}

func (p *TransactorPool) WaitMined(txHash string, opts ...TxOption) (*types.Receipt, error) {
	return p.nextTransactor().WaitMined(txHash, opts...)
}
