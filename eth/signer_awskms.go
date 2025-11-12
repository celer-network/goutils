package eth

import (
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

// from go-ethereum crypto.go
var (
	secp256k1N, _  = new(big.Int).SetString("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141", 16)
	secp256k1halfN = new(big.Int).Div(secp256k1N, big.NewInt(2))
)

// impl Signer interface
type KmsSigner struct {
	Addr     common.Address
	chainId  *big.Int
	keyAlias *string
	kms      *kms.KMS
}

// region and keyAlias must be valid, eg. us-west-1 alias/mytestkey
// if awsKey, awsSec are empty string, will use aws sdk auto search
func NewKmsSigner(region, keyAlias, awsKey, awsSec string, chainId *big.Int) (*KmsSigner, error) {
	cfg := &aws.Config{
		Region: aws.String(region),
	}
	if awsKey != "" && awsSec != "" {
		cfg.Credentials = credentials.NewStaticCredentials(awsKey, awsSec, "")
	}
	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, fmt.Errorf("NewSession err: %w", err)
	}
	svc := kms.New(sess)
	resp, err := svc.GetPublicKey(&kms.GetPublicKeyInput{
		KeyId: aws.String(keyAlias),
	})
	if err != nil {
		return nil, fmt.Errorf("GetPublicKey err: %w", err)
	}
	pub := new(PubKeyAsn)
	_, err = asn1.Unmarshal(resp.PublicKey, pub)
	if err != nil {
		return nil, fmt.Errorf("asn1.Unmarshal err: %w, resp.PublicKey: %x", err, resp.PublicKey)
	}
	// skip first byte as it's just an indicator whether compress, and use last 20 bytes of hash
	// see PubkeyToAddress https://github.com/ethereum/go-ethereum/blob/master/crypto/crypto.go#L276
	addr := common.BytesToAddress(crypto.Keccak256(pub.PubKey.Bytes[1:])[12:])
	var cid *big.Int
	if chainId != nil {
		cid = new(big.Int).Set(chainId)
	}
	return &KmsSigner{
		Addr:     addr,
		chainId:  cid,
		keyAlias: aws.String(keyAlias),
		kms:      svc,
	}, nil
}

// satisfy Signer interface SignEthMessage
func (s *KmsSigner) SignEthMessage(data []byte) ([]byte, error) {
	return s.Sign(GeneratePrefixedHash(data))
}

// satisfy Signer interface SignEthTransaction
func (s *KmsSigner) SignEthTransaction(rawTx []byte) ([]byte, error) {
	tx := new(types.Transaction)
	err := rlp.DecodeBytes(rawTx, tx)
	if err != nil {
		return nil, err
	}
	tx, err = s.SignerFn(s.Addr, tx)
	if err != nil {
		return nil, err
	}
	return rlp.EncodeToBytes(tx)
}

// return bind.TransactOpts to be used in bound contract tx
func (s *KmsSigner) NewTransactOpts() *bind.TransactOpts {
	return &bind.TransactOpts{
		From:   s.Addr,
		Signer: s.SignerFn,
	}
}

// satisfy bind/base.go SignerFn
func (s *KmsSigner) SignerFn(addr common.Address, tx *types.Transaction) (*types.Transaction, error) {
	if addr != s.Addr {
		return nil, bind.ErrNotAuthorized
	}
	signer := types.LatestSignerForChainID(s.chainId)
	signature, err := s.Sign(signer.Hash(tx).Bytes())
	if err != nil {
		return nil, err
	}
	return tx.WithSignature(signer, signature)
}

// input must be hash, return 65 bytes sig or error. sig[64] is 0 or 1
func (s *KmsSigner) Sign(hash []byte) ([]byte, error) {
	sig, err := s.kms.Sign(&kms.SignInput{
		KeyId:            s.keyAlias,
		MessageType:      aws.String("DIGEST"),
		Message:          hash,
		SigningAlgorithm: aws.String("ECDSA_SHA_256"),
	})
	if err != nil {
		return nil, fmt.Errorf("Sign err: %w", err)
	}
	rs := new(RS)
	_, err = asn1.Unmarshal(sig.Signature, rs)
	if err != nil {
		return nil, fmt.Errorf("asn1.Unmarshal err: %w, sig.Signature: %x", err, sig.Signature)
	}
	retSig := make([]byte, 65)
	copy(retSig, padBigInt(rs.R))
	// per EIP-2, S must be less than secp256k1n/2
	if rs.S.Cmp(secp256k1halfN) > 0 {
		rs.S.Sub(secp256k1N, rs.S) // s = secp256k1N - s
	}
	copy(retSig[32:], padBigInt(rs.S))
	// now try recover to see if v is 0 or 1
	pubKey, err := crypto.SigToPub(hash, retSig)
	if err != nil {
		return nil, fmt.Errorf("SigToPub: %w, hash: %x, retSig: %x", err, hash, retSig)
	}
	addr := crypto.PubkeyToAddress(*pubKey)
	if addr == s.Addr {
		return retSig, nil
	}
	retSig[64] = 1
	pubKey, err = crypto.SigToPub(hash, retSig)
	if err != nil {
		return nil, fmt.Errorf("SigToPub: %w, hash: %x, retSig: %x", err, hash, retSig)
	}
	addr = crypto.PubkeyToAddress(*pubKey)
	if addr == s.Addr {
		return retSig, nil
	}
	return nil, fmt.Errorf("recovered addr %x not match expected %x", addr, s.Addr)
}

type PubKeyAsn struct {
	Algo   pkix.AlgorithmIdentifier
	PubKey asn1.BitString
}

type RS struct {
	R, S *big.Int
}

// guarantee return len([]byte) = 32
func padBigInt(i *big.Int) []byte {
	ret := i.Bytes()
	if len(ret) == 32 {
		return ret
	}
	ret2 := make([]byte, 32)
	copy(ret2[32-len(ret):], ret)
	return ret2
}

// if ksfile is like awskms:us-west-2:alias/mytestkey, use KmsSigner
// passphrase will be awsKey:awsSec or if empty, will use aws auto search env variable etc
// otherwise normal ks json file based signer
const awskmsPre = "awskms:"

// return signer, address
func CreateSigner(ksfile, passphrase string, chainid *big.Int) (Signer, common.Address, error) {
	if strings.HasPrefix(ksfile, awskmsPre) {
		kmskeyinfo := strings.SplitN(ksfile, ":", 3)
		if len(kmskeyinfo) != 3 {
			return nil, common.Address{}, fmt.Errorf("%s has wrong format, expected 'awskms:<region>:<alias/...>'", ksfile)
		}
		awskeysec := []string{"", ""}
		if passphrase != "" {
			awskeysec = strings.SplitN(passphrase, ":", 2)
			if len(awskeysec) != 2 {
				return nil, common.Address{}, fmt.Errorf("%s has wrong format, expected '<awsKey>:<awsSecret>'", passphrase)
			}
		}
		kmsSigner, err := NewKmsSigner(kmskeyinfo[1], kmskeyinfo[2], awskeysec[0], awskeysec[1], chainid)
		if err != nil {
			return nil, common.Address{}, err
		}
		return kmsSigner, kmsSigner.Addr, nil
	}
	ksBytes, err := os.ReadFile(ksfile)
	if err != nil {
		return nil, common.Address{}, err
	}
	key, err := keystore.DecryptKey(ksBytes, passphrase)
	if err != nil {
		return nil, common.Address{}, err
	}
	signer, err := NewPrivateKeySigner(hex.EncodeToString(crypto.FromECDSA(key.PrivateKey)), chainid)
	return signer, key.Address, err
}
