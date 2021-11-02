package eth

import (
	"crypto/x509/pkix"
	"encoding/asn1"
	"fmt"
	"math/big"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

// impl Signer
type KmsSigner struct {
	chainId  *big.Int
	keyAlias *string
	kms      *kms.KMS
	Addr     common.Address
}

func NewKmsSigner(region, keyAlias, awsKey, awsSec string, chainId *big.Int) (*KmsSigner, error) {
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(awsKey, awsSec, ""),
		Region:      aws.String(region)},
	)
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
	return &KmsSigner{
		chainId:  new(big.Int).Set(chainId),
		keyAlias: aws.String(keyAlias),
		kms:      svc,
		Addr:     addr,
	}, nil
}

func (s *KmsSigner) SignEthMessage(data []byte) ([]byte, error) {
	return s.Sign(GeneratePrefixedHash(data))
}

func (s *KmsSigner) SignEthTransaction(rawTx []byte) ([]byte, error) {
	tx := new(types.Transaction)
	if err := rlp.DecodeBytes(rawTx, tx); err != nil {
		return nil, err
	}
	londonSigner := types.NewLondonSigner(s.chainId)
	signature, err := s.Sign(londonSigner.Hash(tx).Bytes())
	if err != nil {
		return nil, err
	}
	tx, err = tx.WithSignature(londonSigner, signature)
	if err != nil {
		return nil, err
	}
	return rlp.EncodeToBytes(tx)
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
