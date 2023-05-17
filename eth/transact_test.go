package eth

import (
	"context"
	"errors"
	"github.com/celer-network/goutils/log"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"
	"math/big"
	"testing"
)

func TestTransact(t *testing.T) {
	ec, err := ethclient.Dial("https://rpc.ankr.com/eth_goerli")
	check(err)

	bytes := hexutil.MustDecode("0x5d5e863d0338f95c1d3b10846ac120ba27a2a264") // Ping
	var addr common.Address
	copy(addr[:], bytes)
	contract := bind.NewBoundContract(addr, abi.ABI{}, nil, ec, nil)

	calldata := hexutil.MustDecode("0x3adb191b000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000033132330000000000000000000000000000000000000000000000000000000000") // 123
	signer, err := NewSigner("4e6b79814cc0a257f55f1b5c32a183e8600aa6992b4e5712046d41e56e06d696", big.NewInt(5))
	check(err)

	bytes2 := hexutil.MustDecode("0x9532f934EfcE6c4Bf5BA078b25fDd81a780FBdfB")
	var from common.Address
	copy(from[:], bytes2)

	opts := newTxOpts(from, signer)
	nonce, err := ec.PendingNonceAt(context.Background(), from)
	check(err)
	log.Infof("nonce %d", nonce)

	opts.Nonce = big.NewInt(int64(nonce))
	tx, err := contract.RawTransact(opts, calldata)
	check(err)
	log.Infof("tx %s", tx.Hash())
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func newTxOpts(from common.Address, signer *CelerSigner) *bind.TransactOpts {
	return &bind.TransactOpts{
		From: from,
		// Ignore the passed in Signer to enforce EIP-155
		Signer: func(
			address common.Address,
			tx *types.Transaction) (*types.Transaction, error) {
			if address != from {
				return nil, errors.New("not authorized to sign this account")
			}
			rawTx, err := rlp.EncodeToBytes(tx)
			if err != nil {
				return nil, err
			}
			rawTx, err = signer.SignEthTransaction(rawTx)
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
