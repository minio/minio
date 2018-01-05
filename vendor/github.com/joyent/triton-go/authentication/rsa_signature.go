package authentication

import (
	"encoding/base64"
)

type rsaSignature struct {
	hashAlgorithm string
	signature     []byte
}

func (s *rsaSignature) SignatureType() string {
	return s.hashAlgorithm
}

func (s *rsaSignature) String() string {
	return base64.StdEncoding.EncodeToString(s.signature)
}

func newRSASignature(signatureBlob []byte) (*rsaSignature, error) {
	return &rsaSignature{
		hashAlgorithm: "rsa-sha1",
		signature:     signatureBlob,
	}, nil
}
