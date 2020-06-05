// Copyright 2020 Celer Network

package eth

import (
	"crypto/ecdsa"
	"encoding/hex"

	"github.com/celer-network/goutils/log"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

type Signer interface {
	// input data: a byte array of raw message to be signed
	// return a byte array signature in the R,S,V format
	// The implementation should hash data w/ keccak256, and add
	// "\x19Ethereum Signed Message:\n32" prefix (32 is the length of hash result)
	// for ECDSA sign. If some library handles prefix automatically, pass hash
	// result is sufficient
	SignEthMessage(data []byte) ([]byte, error)
	// input rawTx: a byte array of a RLP-encoded unsigned Ethereum raw transaction
	// return a byte array signed raw tx in RLP-encoded format
	SignEthTransaction(rawTx []byte) ([]byte, error)
}

type CelerSigner struct {
	key *ecdsa.PrivateKey
}

func NewSigner(privateKey string) (*CelerSigner, error) {
	key, err := crypto.HexToECDSA(privateKey)
	if err != nil {
		return nil, err
	}
	c := &CelerSigner{key: key}
	return c, nil
}

func NewSignerFromKeystore(keyjson, passphrase string) (*CelerSigner, error) {
	_, privkey, err := GetAddrPrivKeyFromKeystore(keyjson, passphrase)
	if err != nil {
		return nil, err
	}
	return NewSigner(privkey)
}

// input data: a byte array of raw message to be signed
// return a byte array signature in the R,S,V format
func (s *CelerSigner) SignEthMessage(data []byte) ([]byte, error) {
	sig, err := crypto.Sign(GeneratePrefixedHash(data), s.key)
	if err != nil {
		return nil, err
	}
	return sig, nil
}

// input rawTx: a byte array of a RLP-encoded unsigned Ethereum raw transaction
// return a byte array signed raw tx in RLP-encoded format
func (s *CelerSigner) SignEthTransaction(rawTx []byte) ([]byte, error) {
	tx := new(types.Transaction)
	if err := rlp.DecodeBytes(rawTx, tx); err != nil {
		return nil, err
	}
	eip155Signer := types.NewEIP155Signer(getChainId())
	signature, err := crypto.Sign(eip155Signer.Hash(tx).Bytes(), s.key)
	if err != nil {
		return nil, err
	}
	tx, err = tx.WithSignature(eip155Signer, signature)
	if err != nil {
		return nil, err
	}
	return rlp.EncodeToBytes(tx)
}

func SigIsValid(signer common.Address, data []byte, sig []byte) bool {
	recoveredAddr := RecoverSigner(data, sig)
	return recoveredAddr == signer
}

func RecoverSigner(data []byte, sig []byte) common.Address {
	if len(sig) == 65 { // we could return zeroAddr if len not 65
		if sig[64] == 27 || sig[64] == 28 {
			// SigToPub only expect v to be 0 or 1,
			// see https://github.com/ethereum/go-ethereum/blob/v1.8.23/internal/ethapi/api.go#L468.
			// we've been ok as our own code only has v 0 or 1, but using external signer may cause issue
			// we also fix v in celersdk.PublishSignedResult to be extra safe
			sig[64] -= 27
		}
	}
	pubKey, err := crypto.SigToPub(GeneratePrefixedHash(data), sig)
	if err != nil {
		log.Errorf("sig error: %v, sig: %x", err, sig)
		return common.Address{}
	}
	recoveredAddr := crypto.PubkeyToAddress(*pubKey)
	return recoveredAddr
}

func GeneratePrefixedHash(data []byte) []byte {
	return crypto.Keccak256([]byte("\x19Ethereum Signed Message:\n32"), crypto.Keccak256(data))
}

func GetAddrPrivKeyFromKeystore(keyjson, passphrase string) (common.Address, string, error) {
	key, err := keystore.DecryptKey([]byte(keyjson), passphrase)
	if err != nil {
		return common.Address{}, "", err
	}
	privKey := hex.EncodeToString(crypto.FromECDSA(key.PrivateKey))
	return key.Address, privKey, nil
}
