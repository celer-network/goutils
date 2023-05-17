package eth

import (
	"github.com/celer-network/goutils/log"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"math/big"
	"sync"
	"testing"
)

func TestTransactor_RawTransact(t *testing.T) {
	ec, err := ethclient.Dial("https://rpc.ankr.com/eth_goerli")
	check(err)

	ping := decodeAddr("0x5d5e863d0338f95c1d3b10846ac120ba27a2a264")
	acc := decodeAddr("0x9532f934EfcE6c4Bf5BA078b25fDd81a780FBdfB")
	signer, err := NewSigner("4e6b79814cc0a257f55f1b5c32a183e8600aa6992b4e5712046d41e56e06d696", big.NewInt(5))
	check(err)

	calldata := hexutil.MustDecode("0x3adb191b000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000033132330000000000000000000000000000000000000000000000000000000000") // 123

	txr := &Transactor{
		address: acc,
		chainId: big.NewInt(5),
		signer:  signer,
		client:  ec,
		nonce:   0,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	tx, err := txr.RawTransact(&TransactionStateHandler{
		OnMined: func(receipt *types.Receipt) {
			defer wg.Done()
			if receipt.Status == types.ReceiptStatusFailed {
				log.Errorf("receipt status failed")
				t.FailNow()
			}
		},
		OnError: func(tx *types.Transaction, err error) {
			defer wg.Done()
			log.Error(err)
			t.FailNow()
		},
	}, calldata, ping, WithBlockDelay(1), WithAddGasEstimateRatio(1))
	if err != nil {
		log.Error(err)
		t.FailNow()
	}
	log.Infof("tx %s", tx.Hash())
	wg.Wait()
}

func decodeAddr(hex string) common.Address {
	bytes := hexutil.MustDecode(hex)
	var addr common.Address
	copy(addr[:], bytes)
	return addr
}
