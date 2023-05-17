package eth

import (
	"context"
	"errors"
	"fmt"
	"github.com/celer-network/goutils/log"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"math/big"
	"strings"
)

func (t *Transactor) RawTransact(handler *TransactionStateHandler, data []byte, to common.Address, opts ...TxOption) (*types.Transaction, error) {
	return t.rawTransact(handler, data, to, opts...)
}

func (t *Transactor) rawTransact(
	handler *TransactionStateHandler,
	calldata []byte,
	to common.Address,
	opts ...TxOption,
) (*types.Transaction, error) {
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
	err := t.setGasLimit(calldata, to, signer, txopts, client)
	if err != nil {
		return nil, fmt.Errorf("determineGas err: %w", err)
	}
	// Set nonce
	nonce, pendingNonce, err := t.getNextNonce(txopts)
	for {
		nonceInt := big.NewInt(0)
		nonceInt.SetUint64(nonce)
		signer.Nonce = nonceInt

		contract := bind.NewBoundContract(to, abi.ABI{}, nil, client, nil)
		tx, err := contract.RawTransact(signer, calldata)
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

func (t *Transactor) getNextNonce(txopts txOptions) (nonce, pendingNonce uint64, err error) {
	pendingNonce, err = t.client.PendingNonceAt(context.Background(), t.address)
	if err != nil {
		return 0, 0, fmt.Errorf("PendingNonceAt err: %w", err)
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
	nonce = t.nonce
	if pendingNonce > nonce || !t.sentTx || txopts.maxSubmittingTxNum == 1 {
		nonce = pendingNonce
	} else {
		nonce++
	}
	if txopts.maxSubmittingTxNum > 0 && nonce-pendingNonce >= txopts.maxSubmittingTxNum {
		return 0, 0, fmt.Errorf("%w, submittingNonce:%d pendingNonce:%d limit:%d",
			ErrTooManySubmittingTx, nonce, pendingNonce, txopts.maxSubmittingTxNum)
	}
	return nonce, pendingNonce, nil
}

func (t *Transactor) setGasLimit(calldata []byte, to common.Address, signer *bind.TransactOpts, txopts txOptions, client *ethclient.Client) error {
	if err := ensureLegacyOrEIP1559Flags(txopts); err != nil {
		return err
	}
	if txopts.gasLimit > 0 {
		// Use the specified limit if set
		signer.GasLimit = txopts.gasLimit
		return nil
	} else if txopts.addGasEstimateRatio > 0.0 {
		signer.NoSend = true
		contract := bind.NewBoundContract(to, abi.ABI{}, nil, client, nil)
		dryTx, err := contract.RawTransact(signer, calldata)
		if err != nil {
			return fmt.Errorf("tx dry-run err: %w", err)
		}
		signer.NoSend = false
		signer.GasLimit = uint64(float64(dryTx.Gas()) * (1 + txopts.addGasEstimateRatio))
	}
	// If addGasEstimateRatio not specified, just defer to go-ethereum for gas limit estimation
	return nil
}

func ensureLegacyOrEIP1559Flags(txopts txOptions) error {
	// Only accept legacy flags or EIP-1559 flags, not both
	hasLegacyFlags := txopts.forceGasGwei != nil || txopts.minGasGwei > 0 || txopts.maxGasGwei > 0 || txopts.addGasGwei > 0
	has1559Flags := txopts.maxFeePerGasGwei > 0 || txopts.maxPriorityFeePerGasGwei > 0
	if hasLegacyFlags && has1559Flags {
		return ErrConflictingGasFlags
	}
	return nil
}
