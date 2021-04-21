// Copyright 2018-2020 Celer Network

package eth

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/celer-network/goutils/log"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	parityErrIncrementNonce = "incrementing the nonce"
)

type Transactor struct {
	address common.Address
	chainId *big.Int
	signer  Signer
	client  *ethclient.Client
	nonce   uint64
	sentTx  bool
	dopts   txOptions // default transactor tx options
	lock    sync.Mutex
}

type TxMethod func(transactor bind.ContractTransactor, opts *bind.TransactOpts) (*types.Transaction, error)

type TransactionStateHandler struct {
	OnMined func(receipt *types.Receipt)
	OnError func(tx *types.Transaction, err error)
}

func NewTransactor(
	keyjson string,
	passphrase string,
	client *ethclient.Client,
	chainId *big.Int,
	opts ...TxOption) (*Transactor, error) {
	address, privKey, err := GetAddrPrivKeyFromKeystore(keyjson, passphrase)
	if err != nil {
		return nil, err
	}
	signer, err := NewSigner(privKey, chainId)
	if err != nil {
		return nil, err
	}
	txopts := defaultTxOptions()
	for _, o := range opts {
		o.apply(&txopts)
	}
	return &Transactor{
		address: address,
		chainId: chainId,
		signer:  signer,
		client:  client,
		dopts:   txopts,
	}, nil
}

func NewTransactorByExternalSigner(
	address common.Address,
	signer Signer,
	client *ethclient.Client,
	opts ...TxOption) *Transactor {
	txopts := defaultTxOptions()
	for _, o := range opts {
		o.apply(&txopts)
	}
	return &Transactor{
		address: address,
		signer:  signer,
		client:  client,
		dopts:   txopts,
	}
}

func (t *Transactor) Transact(
	handler *TransactionStateHandler,
	method TxMethod,
	opts ...TxOption) (*types.Transaction, error) {
	return t.transact(handler, method, opts...)
}

func (t *Transactor) TransactWaitMined(
	description string,
	method TxMethod,
	opts ...TxOption) (*types.Receipt, error) {
	receiptChan := make(chan *types.Receipt, 1)
	errChan := make(chan error, 1)
	_, err := t.transact(newTxWaitMinedHandler(description, receiptChan, errChan), method, opts...)
	if err != nil {
		return nil, err
	}
	select {
	case res := <-receiptChan:
		return res, nil
	case err := <-errChan:
		return nil, err
	}
}

func (t *Transactor) transact(
	handler *TransactionStateHandler,
	method TxMethod,
	opts ...TxOption) (*types.Transaction, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	txopts := t.dopts
	for _, o := range opts {
		o.apply(&txopts)
	}
	signer := t.newTransactOpts()
	client := t.client
	suggestedPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		return nil, fmt.Errorf("SuggestGasPrice err: %w", err)
	}
	signer.GasPrice = suggestedPrice
	if txopts.addGasGwei > 0 { // add gas price to the suggested value to speed up transactions
		addPrice := new(big.Int).SetUint64(txopts.addGasGwei * 1e9) // 1e9 is 1G
		signer.GasPrice = signer.GasPrice.Add(signer.GasPrice, addPrice)
	}
	if txopts.minGasGwei > 0 { // gas can't be lower than minGas
		minPrice := new(big.Int).SetUint64(txopts.minGasGwei * 1e9)
		// minPrice is larger than suggested, use minPrice
		if minPrice.Cmp(signer.GasPrice) > 0 {
			signer.GasPrice = minPrice
		}
	}
	if txopts.maxGasGwei > 0 { // maxGas 0 means no cap on gas price, otherwise won't set bigger than it
		maxPrice := new(big.Int).SetUint64(txopts.maxGasGwei * 1e9)
		// GasPrice is larger than allowed cap, set to cap
		if maxPrice.Cmp(signer.GasPrice) < 0 {
			log.Warnf("suggested gas price %s larger than cap %s, set to cap", signer.GasPrice, maxPrice)
			signer.GasPrice = maxPrice
		}
	}
	signer.Value = txopts.ethValue
	if txopts.addGasEstimateRatio > 0.0 {
		// Enable gas estimation
		signer.NoSend = true
		dryTx, err := method(client, signer)
		if err != nil {
			return nil, fmt.Errorf("dry-run err: %w", err)
		}
		signer.NoSend = false
		typesMsg, err := dryTx.AsMessage(types.NewEIP155Signer(t.chainId))
		if err != nil {
			return nil, fmt.Errorf("failed to get typesMsg err: %w", err)
		}
		callMsg := ethereum.CallMsg{
			From:     typesMsg.From(),
			To:       typesMsg.To(),
			GasPrice: typesMsg.GasPrice(),
			Value:    typesMsg.Value(),
			Data:     typesMsg.Data(),
		}
		estimatedGas, err := client.EstimateGas(context.Background(), callMsg)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate gas err: %w", err)
		}
		signer.GasLimit = uint64(float64(estimatedGas) * (1 + txopts.addGasEstimateRatio))
	} else {
		// Just use the specified limit
		signer.GasLimit = txopts.gasLimit
	}
	pendingNonce, err := t.client.PendingNonceAt(context.Background(), t.address)
	if err != nil {
		return nil, fmt.Errorf("PendingNonceAt err: %w", err)
	}
	nonce := t.nonce
	if pendingNonce > nonce || !t.sentTx {
		nonce = pendingNonce
	} else {
		nonce++
	}
	for {
		nonceInt := big.NewInt(0)
		nonceInt.SetUint64(nonce)
		signer.Nonce = nonceInt
		tx, err := method(client, signer)
		if err != nil {
			errStr := err.Error()
			if errStr == core.ErrNonceTooLow.Error() ||
				errStr == core.ErrReplaceUnderpriced.Error() ||
				strings.Contains(errStr, parityErrIncrementNonce) {
				nonce++
			} else {
				return nil, fmt.Errorf("TxMethod err: %w", err)
			}
		} else {
			t.sentTx = true
			if handler != nil {
				go func() {
					txHash := tx.Hash().Hex()
					log.Debugf("Waiting for tx %s to be mined", txHash)
					receipt, err := WaitMined(
						context.Background(), client, tx,
						WithBlockDelay(txopts.blockDelay),
						WithPollingInterval(txopts.pollingInterval),
						WithTimeout(txopts.timeout),
						WithQueryTimeout(txopts.queryTimeout),
						WithQueryRetryInterval(txopts.queryRetryInterval))
					if err != nil {
						if handler.OnError != nil {
							handler.OnError(tx, err)
						}
						return
					}
					log.Debugf("Tx %s mined, status: %d, gas estimate: %d, gas used: %d",
						txHash, receipt.Status, tx.Gas(), receipt.GasUsed)
					if handler.OnMined != nil {
						handler.OnMined(receipt)
					}
				}()
			}
			t.nonce = nonce
			return tx, nil
		}
	}
}

func (t *Transactor) ContractCaller() bind.ContractCaller {
	return t.client
}

func (t *Transactor) Address() common.Address {
	return t.address
}

func (t *Transactor) WaitMined(txHash string, opts ...TxOption) (*types.Receipt, error) {
	txopts := t.dopts
	for _, o := range opts {
		o.apply(&txopts)
	}
	return WaitMinedWithTxHash(
		context.Background(), t.client, txHash,
		WithBlockDelay(txopts.blockDelay),
		WithPollingInterval(txopts.pollingInterval),
		WithTimeout(txopts.timeout),
		WithQueryTimeout(txopts.queryTimeout),
		WithQueryRetryInterval(txopts.queryRetryInterval))
}

func (t *Transactor) newTransactOpts() *bind.TransactOpts {
	return &bind.TransactOpts{
		From: t.address,
		// Ignore the passed in Signer to enforce EIP-155
		Signer: func(
			address common.Address,
			tx *types.Transaction) (*types.Transaction, error) {
			if address != t.address {
				return nil, errors.New("not authorized to sign this account")
			}
			rawTx, err := rlp.EncodeToBytes(tx)
			if err != nil {
				return nil, err
			}
			rawTx, err = t.signer.SignEthTransaction(rawTx)
			if err != nil {
				return nil, err
			}
			err = rlp.DecodeBytes(rawTx, tx)
			if err != nil {
				return nil, err
			}
			return tx, nil
		},
	}
}

func newTxWaitMinedHandler(
	description string, receiptChan chan *types.Receipt, errChan chan error) *TransactionStateHandler {
	return &TransactionStateHandler{
		OnMined: func(receipt *types.Receipt) {
			if receipt.Status == types.ReceiptStatusSuccessful {
				log.Infof("%s transaction %x succeeded", description, receipt.TxHash)
			} else {
				log.Errorf("%s transaction %x failed", description, receipt.TxHash)
			}
			receiptChan <- receipt
		},
		OnError: func(tx *types.Transaction, err error) {
			log.Errorf("%s transaction %x err: %s", description, tx.Hash(), err)
			errChan <- err
		},
	}
}
