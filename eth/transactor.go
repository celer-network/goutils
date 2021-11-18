// Copyright 2018-2021 Celer Network

package eth

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

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

var (
	ErrExceedMaxGas        = errors.New("suggested gas price exceeds max allowed")
	ErrConflictingGasFlags = errors.New("cannot specify both legacy and EIP-1559 gas flags")

	ctxTimeout = 3 * time.Second
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
	chainId *big.Int,
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
		chainId: chainId,
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
	// Set gas price and limit
	err := t.determineGas(method, signer, txopts, client)
	if err != nil {
		return nil, fmt.Errorf("determineGas err: %w", err)
	}
	// Set value
	signer.Value = txopts.ethValue
	// Set nonce
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
					log.Debugf("Waiting for tx %s to be mined, chain %s nonce %d", txHash, t.chainId, nonce)
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
						if errors.Is(err, ethereum.NotFound) && pendingNonce > 0 {
							// reset transactor nonce to pending nonce
							// this means pending txs after this will probably fail
							t.lock.Lock()
							t.nonce = pendingNonce - 1
							log.Warnf("Reset chain %s transactor nonce to %d", t.chainId, t.nonce)
							t.lock.Unlock()
						}
						return
					}
					log.Debugf("Tx %s mined, status %d, chain %s, gas estimate %d, gas used %d",
						txHash, receipt.Status, t.chainId, tx.Gas(), receipt.GasUsed)
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

// determineGas sets the gas price and gas limit on the signer
func (t *Transactor) determineGas(method TxMethod, signer *bind.TransactOpts, txopts txOptions, client *ethclient.Client) error {
	// 1. Determine gas price
	// Only accept legacy flags or EIP-1559 flags, not both
	hasLegacyFlags := txopts.forceGasGwei > 0 || txopts.minGasGwei > 0 || txopts.maxGasGwei > 0 || txopts.addGasGwei > 0
	has1559Flags := txopts.maxFeePerGasGwei > 0 || txopts.maxPriorityFeePerGasGwei > 0
	if hasLegacyFlags && has1559Flags {
		return ErrConflictingGasFlags
	}
	// Check if chain supports EIP-1559
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancel()
	head, err := client.HeaderByNumber(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to call HeaderByNumber: %w", err)
	}
	send1559Tx := false
	if head.BaseFee != nil && !hasLegacyFlags {
		send1559Tx = true
		err = determine1559GasPrice(ctx, signer, txopts, client, head)
		if err != nil {
			return fmt.Errorf("failed to determine EIP-1559 gas price: %w", err)
		}
	} else {
		// Legacy pricing
		err = determineLegacyGasPrice(ctx, signer, txopts, client)
		if err != nil {
			return fmt.Errorf("failed to determine legacy gas price: %w", err)
		}
	}

	// 2. Determine gas limit
	if txopts.gasLimit > 0 {
		// Use the specified limit if set
		signer.GasLimit = txopts.gasLimit
		return nil
	} else if txopts.addGasEstimateRatio > 0.0 {
		// 2.1. Estimate gas
		signer.NoSend = true
		dryTx, err := method(client, signer)
		if err != nil {
			return fmt.Errorf("tx dry-run err: %w", err)
		}
		signer.NoSend = false
		var typesMsg types.Message
		londonSigner := types.NewLondonSigner(t.chainId)
		if send1559Tx {
			typesMsg, err = dryTx.AsMessage(londonSigner, head.BaseFee)
		} else {
			typesMsg, err = dryTx.AsMessage(londonSigner, nil)
		}
		if err != nil {
			return fmt.Errorf("failed to get typesMsg: %w", err)
		}
		callMsg := ethereum.CallMsg{
			From:     typesMsg.From(),
			To:       typesMsg.To(),
			GasPrice: typesMsg.GasPrice(),
			Value:    typesMsg.Value(),
			Data:     typesMsg.Data(),
		}
		estimatedGas, err := client.EstimateGas(ctx, callMsg)
		if err != nil {
			return fmt.Errorf("failed to call EstimateGas: %w", err)
		}
		// 2.2. Multiply gas limit by the configured ratio
		signer.GasLimit = uint64(float64(estimatedGas) * (1 + txopts.addGasEstimateRatio))
	}
	// If addGasEstimateRatio not specified, just defer to go-ethereum for gas limit estimation
	return nil
}

// determine1559GasPrice sets the gas price on the signer based on the EIP-1559 fee model
func determine1559GasPrice(
	ctx context.Context, signer *bind.TransactOpts, txopts txOptions, client *ethclient.Client, head *types.Header) error {
	if txopts.maxPriorityFeePerGasGwei == 0 {
		gasTipCap, suggestGasTipCapErr := client.SuggestGasTipCap(ctx)
		if suggestGasTipCapErr != nil {
			return fmt.Errorf("failed to call SuggestGasTipCap: %w", suggestGasTipCapErr)
		}
		signer.GasTipCap = gasTipCap
	} else {
		signer.GasTipCap = new(big.Int).SetUint64(txopts.maxPriorityFeePerGasGwei * 1e9)
	}
	if txopts.maxFeePerGasGwei == 0 {
		// Use (maxPriorityFeePerGas + 2x the curent basefee), the same heuristic as go-ethereum
		gasFeeCap := new(big.Int).Add(
			signer.GasTipCap,
			new(big.Int).Mul(head.BaseFee, big.NewInt(2)),
		)
		signer.GasFeeCap = gasFeeCap
	} else {
		signer.GasFeeCap = new(big.Int).SetUint64(txopts.maxFeePerGasGwei * 1e9)
	}
	return nil
}

// determineLegacyGasPrice sets the gas price on the signer based on the legacy fee model
func determineLegacyGasPrice(
	ctx context.Context, signer *bind.TransactOpts, txopts txOptions, client *ethclient.Client) error {
	if txopts.forceGasGwei > 0 {
		signer.GasPrice = new(big.Int).SetUint64(txopts.forceGasGwei * 1e9)
		return nil
	}
	gasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		return fmt.Errorf("failed to call SuggestGasPrice: %w", err)
	}
	if txopts.addGasGwei > 0 { // Add gas price to the suggested value to speed up transactions
		addPrice := new(big.Int).SetUint64(txopts.addGasGwei * 1e9)
		gasPrice.Add(gasPrice, addPrice)
	}
	if txopts.minGasGwei > 0 { // gas can't be lower than minGas
		minPrice := new(big.Int).SetUint64(txopts.minGasGwei * 1e9)
		// minPrice is larger than suggested, use minPrice
		if minPrice.Cmp(gasPrice) > 0 {
			gasPrice = minPrice
		}
	}
	if txopts.maxGasGwei > 0 { // maxGas 0 means no cap on gas price
		maxPrice := new(big.Int).SetUint64(txopts.maxGasGwei * 1e9)
		// GasPrice is larger than allowed cap, return error
		if maxPrice.Cmp(gasPrice) < 0 {
			log.Warnf("suggested gas price %s larger than cap %s", gasPrice, maxPrice)
			return ErrExceedMaxGas
		}
	}
	signer.GasPrice = gasPrice
	return nil
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
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancel()
	return WaitMinedWithTxHash(
		ctx, t.client, txHash,
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
