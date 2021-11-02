package eth

import (
	"math/big"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

// impl Signer
type KmsSigner struct {
	chainId  *big.Int
	keyAlias *string
	kms      *kms.KMS
}

func NewKmsSigner(region, keyAlias, awsKey, awsSec string, chainId *big.Int) (*KmsSigner, error) {
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(awsKey, awsSec, ""),
		Region:      aws.String(region)},
	)
	if err != nil {
		return nil, err
	}
	return &KmsSigner{
		chainId:  new(big.Int).Set(chainId),
		keyAlias: aws.String(keyAlias),
		kms:      kms.New(sess),
	}, nil
}

func (s *KmsSigner) SignEthMessage(data []byte) ([]byte, error) {
	return nil, nil
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

func (s *KmsSigner) Sign(raw []byte) ([]byte, error) {
	return nil, nil
}
