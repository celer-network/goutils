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
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	parityErrIncrementNonce = "incrementing the nonce"
)

var (
	ErrConflictingGasFlags = errors.New("cannot specify both legacy and EIP-1559 gas flags")
	ErrTooManyPendingTx    = errors.New("too many txs in pending status")
	ErrTooManySubmittingTx = errors.New("too many txs in submitting status")

	ctxTimeout = 10 * time.Second
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
	// Set value
	signer.Value = txopts.ethValue
	client := t.client
	// Set gas price and limit
	err := t.determineGas(method, signer, txopts, client)
	if err != nil {
		return nil, fmt.Errorf("determineGas err: %w", err)
	}
	// Set nonce
	nonce, pendingNonce, err := t.determineNonce(txopts)
	if err != nil {
		return nil, err
	}
	for {
		nonceInt := big.NewInt(0)
		nonceInt.SetUint64(nonce)
		signer.Nonce = nonceInt
		tx, err := method(client, signer)
		if err != nil {
			errStr := err.Error()
			if errStr == core.ErrNonceTooLow.Error() ||
				errStr == txpool.ErrReplaceUnderpriced.Error() ||
				strings.Contains(errStr, parityErrIncrementNonce) {
				nonce++
			} else {
				return nil, fmt.Errorf("TxMethod err: %w. nonce %s", err, signer.Nonce)
			}
		} else {
			t.sentTx = true
			logmsg := fmt.Sprintf("Tx sent %x chain %s nonce %d gas %s", tx.Hash(), t.chainId, nonce, printGasGwei(signer))
			if handler != nil {
				go func() {
					log.Debugf("%s, wait to be mined", logmsg)
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
					log.Debugf("Tx mined %x, status %d, chain %s, gas limit %d used %d",
						tx.Hash(), receipt.Status, t.chainId, tx.Gas(), receipt.GasUsed)
					if handler.OnMined != nil {
						handler.OnMined(receipt)
					}
				}()
			} else {
				log.Debug(logmsg)
			}
			t.nonce = nonce
			return tx, nil
		}
	}
}

func (t *Transactor) determineNonce(txopts txOptions) (uint64, uint64, error) {
	pendingNonce, err := t.client.PendingNonceAt(context.Background(), t.address)
	if err != nil {
		return 0, 0, fmt.Errorf("PendingNonceAt err: %w", err)
	}
	nonce := txopts.nonce

	if txopts.nonce != 0 {
		nonce = t.nonce
		if pendingNonce > nonce || !t.sentTx || txopts.maxSubmittingTxNum == 1 {
			nonce = pendingNonce
		} else {
			nonce++
		}
		if txopts.maxPendingTxNum > 0 {
			accountNonce, err := t.client.NonceAt(context.Background(), t.address, nil)
			if err != nil {
				return 0, 0, fmt.Errorf("NonceAt err: %w", err)
			}
			if pendingNonce-accountNonce >= txopts.maxPendingTxNum {
				return 0, 0, fmt.Errorf("%w, pendingNonce:%d accountNonce:%d limit:%d",
					ErrTooManyPendingTx, pendingNonce, accountNonce, txopts.maxPendingTxNum)
			}
		}
	}

	if txopts.maxSubmittingTxNum > 0 && nonce-pendingNonce >= txopts.maxSubmittingTxNum {
		return 0, 0, fmt.Errorf("%w, submittingNonce:%d pendingNonce:%d limit:%d",
			ErrTooManySubmittingTx, nonce, pendingNonce, txopts.maxSubmittingTxNum)
	}
	return nonce, pendingNonce, nil
}

// determineGas sets the gas price and gas limit on the signer
func (t *Transactor) determineGas(method TxMethod, signer *bind.TransactOpts, txopts txOptions, client *ethclient.Client) error {
	// 1. Determine gas price
	// Only accept legacy flags or EIP-1559 flags, not both
	hasLegacyFlags := txopts.forceGasGwei != nil || txopts.minGasGwei > 0 || txopts.maxGasGwei > 0 || txopts.addGasGwei > 0
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
	if head.BaseFee != nil && !hasLegacyFlags {
		err = determine1559GasPrice(signer, txopts)
		if err != nil {
			return fmt.Errorf("failed to determine EIP-1559 gas price: %w", err)
		}
	} else {
		// Legacy pricing
		err = determineLegacyGasPrice(signer, txopts, client)
		if err != nil {
			return fmt.Errorf("failed to determine legacy gas price: %w", err)
		}
	}

	// 2. Determine gas limit
	// 2.1 Always dry-run the tx
	signer.NoSend = true
	dryTx, err := method(client, signer)
	if err != nil {
		return fmt.Errorf("tx dry-run err: %w", err)
	}
	signer.NoSend = false
	// 2.2 Set the gas limit
	if txopts.gasLimit > 0 {
		// Use the specified limit if set
		signer.GasLimit = txopts.gasLimit
	} else {
		// Use estimiated gas limit multiplied by the configured ratio
		signer.GasLimit = uint64(float64(dryTx.Gas()) * (1 + txopts.addGasEstimateRatio))
	}
	if signer.GasLimit < dryTx.Gas() {
		return fmt.Errorf("gas limit lower than estimated value")
	}
	return nil
}

// determine1559GasPrice sets the gas price on the signer based on the EIP-1559 fee model
func determine1559GasPrice(signer *bind.TransactOpts, txopts txOptions) error {
	if txopts.maxPriorityFeePerGasGwei > 0 {
		signer.GasTipCap = new(big.Int).SetUint64(uint64(txopts.maxPriorityFeePerGasGwei * 1e9))
	}
	if txopts.maxFeePerGasGwei > 0 {
		signer.GasFeeCap = new(big.Int).SetUint64(txopts.maxFeePerGasGwei * 1e9)
	}
	return nil
}

// determineLegacyGasPrice sets the gas price on the signer based on the legacy fee model
func determineLegacyGasPrice(
	signer *bind.TransactOpts, txopts txOptions, client *ethclient.Client) error {
	if txopts.forceGasGwei != nil {
		signer.GasPrice = new(big.Int).SetUint64(*txopts.forceGasGwei * 1e9)
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
		// GasPrice is larger than allowed cap, use maxPrice but log warning
		if maxPrice.Cmp(gasPrice) < 0 {
			log.Warnf("suggested gas price %s larger than cap %s", gasPrice, maxPrice)
			gasPrice = maxPrice
		}
	}
	signer.GasPrice = gasPrice
	return nil
}

func printGasGwei(signer *bind.TransactOpts) string {
	if signer.GasPrice == nil && signer.GasFeeCap == nil && signer.GasTipCap == nil {
		return "auto"
	}
	res := ""
	if signer.GasPrice != nil && signer.GasPrice.Sign() > 0 {
		res += fmt.Sprintf("price %d ", signer.GasPrice.Uint64()/1e9)
	}
	if signer.GasFeeCap != nil && signer.GasFeeCap.Sign() > 0 {
		res += fmt.Sprintf("feecap %d ", signer.GasFeeCap.Uint64()/1e9)
	}
	if signer.GasTipCap != nil && signer.GasTipCap.Sign() > 0 {
		res += fmt.Sprintf("tipcap %d ", signer.GasTipCap.Uint64()/1e9)
	}
	return strings.TrimSpace(res)
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
