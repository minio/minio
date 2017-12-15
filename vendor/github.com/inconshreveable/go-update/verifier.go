package update

import (
	"crypto"
	"crypto/dsa"
	"crypto/ecdsa"
	"crypto/rsa"
	"encoding/asn1"
	"errors"
	"math/big"
)

// Verifier defines an interface for verfiying an update's signature with a public key.
type Verifier interface {
	VerifySignature(checksum, signature []byte, h crypto.Hash, publicKey crypto.PublicKey) error
}

type verifyFn func([]byte, []byte, crypto.Hash, crypto.PublicKey) error

func (fn verifyFn) VerifySignature(checksum []byte, signature []byte, hash crypto.Hash, publicKey crypto.PublicKey) error {
	return fn(checksum, signature, hash, publicKey)
}

// NewRSAVerifier returns a Verifier that uses the RSA algorithm to verify updates.
func NewRSAVerifier() Verifier {
	return verifyFn(func(checksum, signature []byte, hash crypto.Hash, publicKey crypto.PublicKey) error {
		key, ok := publicKey.(*rsa.PublicKey)
		if !ok {
			return errors.New("not a valid RSA public key")
		}
		return rsa.VerifyPKCS1v15(key, hash, checksum, signature)
	})
}

type rsDER struct {
	R *big.Int
	S *big.Int
}

// NewECDSAVerifier returns a Verifier that uses the ECDSA algorithm to verify updates.
func NewECDSAVerifier() Verifier {
	return verifyFn(func(checksum, signature []byte, hash crypto.Hash, publicKey crypto.PublicKey) error {
		key, ok := publicKey.(*ecdsa.PublicKey)
		if !ok {
			return errors.New("not a valid ECDSA public key")
		}
		var rs rsDER
		if _, err := asn1.Unmarshal(signature, &rs); err != nil {
			return err
		}
		if !ecdsa.Verify(key, checksum, rs.R, rs.S) {
			return errors.New("failed to verify ecsda signature")
		}
		return nil
	})
}

// NewDSAVerifier returns a Verifier that uses the DSA algorithm to verify updates.
func NewDSAVerifier() Verifier {
	return verifyFn(func(checksum, signature []byte, hash crypto.Hash, publicKey crypto.PublicKey) error {
		key, ok := publicKey.(*dsa.PublicKey)
		if !ok {
			return errors.New("not a valid DSA public key")
		}
		var rs rsDER
		if _, err := asn1.Unmarshal(signature, &rs); err != nil {
			return err
		}
		if !dsa.Verify(key, checksum, rs.R, rs.S) {
			return errors.New("failed to verify ecsda signature")
		}
		return nil
	})
}
