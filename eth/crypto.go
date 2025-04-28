// Copyright 2020 Celer Network

package eth

import (
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

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
	key     *ecdsa.PrivateKey
	chainId *big.Int
}

// Create a new Signer object from the private key
// chainId could be nil if the signer is expected to only call SignEthMessage func
func NewSigner(privateKey string, chainId *big.Int) (*CelerSigner, error) {
	key, err := crypto.HexToECDSA(privateKey)
	if err != nil {
		return nil, err
	}
	c := &CelerSigner{key: key, chainId: chainId}
	return c, nil
}

// Create a new Signer object from the keystore json and passphrase
// chainId could be nil if the signer is expected to only call SignEthMessage func
func NewSignerFromKeystore(keyjson, passphrase string, chainId *big.Int) (*CelerSigner, error) {
	_, privkey, err := GetAddrPrivKeyFromKeystore(keyjson, passphrase)
	if err != nil {
		return nil, err
	}
	return NewSigner(privkey, chainId)
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

// input data: a byte array of raw message to be signed, note the raw data and its length will be used
// unlike SignEthMessage which does keccak first to avoid malleability issue
// return a byte array signature in the R,S,V format
func (s *CelerSigner) SignRawMessage(data []byte) ([]byte, error) {
	sig, err := crypto.Sign(GeneratePrefixedHashForRaw(data), s.key)
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
	londonSigner := types.NewLondonSigner(s.chainId)
	signature, err := crypto.Sign(londonSigner.Hash(tx).Bytes(), s.key)
	if err != nil {
		return nil, err
	}
	tx, err = tx.WithSignature(londonSigner, signature)
	if err != nil {
		return nil, err
	}
	return rlp.EncodeToBytes(tx)
}

func IsSignatureValid(signer common.Address, data []byte, sig []byte) bool {
	recoveredAddr, err := RecoverSigner(data, sig)
	if err != nil {
		return false
	}
	return recoveredAddr == signer
}

// optional isRaw to support recover raw message. use variadic to be compatible w/ legacy code
func RecoverSigner(data []byte, sig []byte, isRaw ...bool) (common.Address, error) {
	if len(sig) != 65 {
		return common.Address{}, errors.New("invalid signature length")
	}

	// clone one to use, to make sure the original sig won't be changed by me
	tmpSig := make([]byte, len(sig))
	copy(tmpSig, sig)

	if tmpSig[64] == 27 || tmpSig[64] == 28 {
		// SigToPub only expect v to be 0 or 1,
		// see https://github.com/ethereum/go-ethereum/blob/v1.8.23/internal/ethapi/api.go#L468.
		// we've been ok as our own code only has v 0 or 1, but using external signer may cause issue
		// we also fix v in celersdk.PublishSignedResult to be extra safe
		tmpSig[64] -= 27
	}
	hash := GeneratePrefixedHash(data)
	if len(isRaw) > 0 && isRaw[0] {
		hash = GeneratePrefixedHashForRaw(data)
	}
	pubKey, err := crypto.SigToPub(hash, tmpSig)
	if err != nil {
		return common.Address{}, err
	}
	recoveredAddr := crypto.PubkeyToAddress(*pubKey)
	return recoveredAddr, nil
}

// keccak data first, then prefix "\x19Ethereum Signed Message:\n" and length 32
func GeneratePrefixedHash(data []byte) []byte {
	return crypto.Keccak256([]byte("\x19Ethereum Signed Message:\n32"), crypto.Keccak256(data))
}

// add length and raw as is, only does keccak once of the concat result. to be compatible with ethers signmessage behavior
func GeneratePrefixedHashForRaw(raw []byte) []byte {
	return crypto.Keccak256([]byte(fmt.Sprintf("\x19Ethereum Signed Message:\n%d", len(raw))), raw)
}

func GetAddrPrivKeyFromKeystore(keyjson, passphrase string) (common.Address, string, error) {
	key, err := keystore.DecryptKey([]byte(keyjson), passphrase)
	if err != nil {
		return common.Address{}, "", err
	}
	privKey := hex.EncodeToString(crypto.FromECDSA(key.PrivateKey))
	return key.Address, privKey, nil
}
