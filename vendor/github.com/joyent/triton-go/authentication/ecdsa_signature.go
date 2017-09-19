package authentication

import (
	"encoding/asn1"
	"encoding/base64"
	"fmt"
	"math/big"

	"github.com/hashicorp/errwrap"
	"golang.org/x/crypto/ssh"
)

type ecdsaSignature struct {
	hashAlgorithm string
	R             *big.Int
	S             *big.Int
}

func (s *ecdsaSignature) SignatureType() string {
	return fmt.Sprintf("ecdsa-%s", s.hashAlgorithm)
}

func (s *ecdsaSignature) String() string {
	toEncode := struct {
		R *big.Int
		S *big.Int
	}{
		R: s.R,
		S: s.S,
	}

	signatureBytes, err := asn1.Marshal(toEncode)
	if err != nil {
		panic(fmt.Sprintf("Error marshaling signature: %s", err))
	}

	return base64.StdEncoding.EncodeToString(signatureBytes)
}

func newECDSASignature(signatureBlob []byte) (*ecdsaSignature, error) {
	var ecSig struct {
		R *big.Int
		S *big.Int
	}

	if err := ssh.Unmarshal(signatureBlob, &ecSig); err != nil {
		return nil, errwrap.Wrapf("Error unmarshaling signature: {{err}}", err)
	}

	rValue := ecSig.R.Bytes()
	var hashAlgorithm string
	switch len(rValue) {
	case 31, 32:
		hashAlgorithm = "sha256"
	case 65, 66:
		hashAlgorithm = "sha512"
	default:
		return nil, fmt.Errorf("Unsupported key length: %d", len(rValue))
	}

	return &ecdsaSignature{
		hashAlgorithm: hashAlgorithm,
		R:             ecSig.R,
		S:             ecSig.S,
	}, nil
}
