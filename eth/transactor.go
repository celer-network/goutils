// Copyright 2018-2020 Celer Network

package eth

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"

	"github.com/celer-network/goutils/log"
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
	signer  Signer
	client  *ethclient.Client
	nonce   uint64
	sentTx  bool
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
	chainId *big.Int) (*Transactor, error) {
	address, privKey, err := GetAddrPrivKeyFromKeystore(keyjson, passphrase)
	if err != nil {
		return nil, err
	}
	signer, err := NewSigner(privKey, WithChainId(chainId))
	if err != nil {
		return nil, err
	}
	return &Transactor{
		address: address,
		signer:  signer,
		client:  client,
	}, nil
}

func NewTransactorByExternalSigner(
	address common.Address,
	signer Signer,
	client *ethclient.Client) *Transactor {
	return &Transactor{
		address: address,
		signer:  signer,
		client:  client,
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
	txopts := &txOptions{}
	for _, o := range opts {
		o.apply(txopts)
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	signer := t.newTransactOpts()
	client := t.client
	suggestedPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		return nil, err
	}
	signer.GasPrice = suggestedPrice
	minGas := txopts.minGasGwei
	maxGas := txopts.maxGasGwei
	if minGas > 0 { // gas can't be lower than minGas
		minPrice := new(big.Int).SetUint64(minGas * 1e9) // 1e9 is 1G
		// minPrice is larger than suggested, use minPrice
		if minPrice.Cmp(suggestedPrice) > 0 {
			signer.GasPrice = minPrice
		} else {
			signer.GasPrice = suggestedPrice
		}
	}
	if maxGas > 0 { // maxGas 0 means no cap on gas price, otherwise won't set bigger than it
		capPrice := new(big.Int).SetUint64(maxGas * 1e9) // 1e9 is 1G
		// GasPrice is larger than allowed cap, set to cap
		if capPrice.Cmp(signer.GasPrice) < 0 {
			log.Warnf("suggested gas price %s larger than cap %s, set to cap", signer.GasPrice, capPrice)
			signer.GasPrice = capPrice
		}
	}
	signer.GasLimit = txopts.gasLimit
	signer.Value = txopts.ethValue
	pendingNonce, err := t.client.PendingNonceAt(context.Background(), t.address)
	if err != nil {
		return nil, err
	}
	if pendingNonce > t.nonce || !t.sentTx {
		t.nonce = pendingNonce
	} else {
		t.nonce++
	}
	for {
		nonceInt := big.NewInt(0)
		nonceInt.SetUint64(t.nonce)
		signer.Nonce = nonceInt
		tx, err := method(client, signer)
		if err != nil {
			errStr := err.Error()
			if errStr == core.ErrNonceTooLow.Error() ||
				errStr == core.ErrReplaceUnderpriced.Error() ||
				strings.Contains(errStr, parityErrIncrementNonce) {
				t.nonce++
			} else {
				return nil, err
			}
		} else {
			t.sentTx = true
			if handler != nil {
				go func() {
					txHash := tx.Hash().Hex()
					log.Debugf("Waiting for tx %s to be mined", txHash)
					receipt, err := WaitMined(context.Background(), client, tx, opts...)
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
	return WaitMinedWithTxHash(context.Background(), t.client, txHash, opts...)
}

func (t *Transactor) newTransactOpts() *bind.TransactOpts {
	return &bind.TransactOpts{
		From: t.address,
		// Ignore the passed in Signer to enforce EIP-155
		Signer: func(
			signer types.Signer,
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
