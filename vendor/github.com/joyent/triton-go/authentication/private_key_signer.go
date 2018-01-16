//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package authentication

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"path"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

type PrivateKeySigner struct {
	formattedKeyFingerprint string
	keyFingerprint          string
	algorithm               string
	accountName             string
	userName                string
	hashFunc                crypto.Hash

	privateKey *rsa.PrivateKey
}

type PrivateKeySignerInput struct {
	KeyID              string
	PrivateKeyMaterial []byte
	AccountName        string
	Username           string
}

func NewPrivateKeySigner(input PrivateKeySignerInput) (*PrivateKeySigner, error) {
	keyFingerprintMD5 := strings.Replace(input.KeyID, ":", "", -1)

	block, _ := pem.Decode(input.PrivateKeyMaterial)
	if block == nil {
		return nil, errors.New("Error PEM-decoding private key material: nil block received")
	}

	rsakey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse private key")
	}

	sshPublicKey, err := ssh.NewPublicKey(rsakey.Public())
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse SSH key from private key")
	}

	matchKeyFingerprint := formatPublicKeyFingerprint(sshPublicKey, false)
	displayKeyFingerprint := formatPublicKeyFingerprint(sshPublicKey, true)
	if matchKeyFingerprint != keyFingerprintMD5 {
		return nil, errors.New("Private key file does not match public key fingerprint")
	}

	signer := &PrivateKeySigner{
		formattedKeyFingerprint: displayKeyFingerprint,
		keyFingerprint:          input.KeyID,
		accountName:             input.AccountName,

		hashFunc:   crypto.SHA1,
		privateKey: rsakey,
	}

	if input.Username != "" {
		signer.userName = input.Username
	}

	_, algorithm, err := signer.SignRaw("HelloWorld")
	if err != nil {
		return nil, fmt.Errorf("Cannot sign using ssh agent: %s", err)
	}
	signer.algorithm = algorithm

	return signer, nil
}

func (s *PrivateKeySigner) Sign(dateHeader string) (string, error) {
	const headerName = "date"

	hash := s.hashFunc.New()
	hash.Write([]byte(fmt.Sprintf("%s: %s", headerName, dateHeader)))
	digest := hash.Sum(nil)

	signed, err := rsa.SignPKCS1v15(rand.Reader, s.privateKey, s.hashFunc, digest)
	if err != nil {
		return "", errors.Wrap(err, "unable to sign date header")
	}
	signedBase64 := base64.StdEncoding.EncodeToString(signed)

	var keyID string
	if s.userName != "" {

		keyID = path.Join("/", s.accountName, "users", s.userName, "keys", s.formattedKeyFingerprint)
	} else {
		keyID = path.Join("/", s.accountName, "keys", s.formattedKeyFingerprint)
	}
	return fmt.Sprintf(authorizationHeaderFormat, keyID, "rsa-sha1", headerName, signedBase64), nil
}

func (s *PrivateKeySigner) SignRaw(toSign string) (string, string, error) {
	hash := s.hashFunc.New()
	hash.Write([]byte(toSign))
	digest := hash.Sum(nil)

	signed, err := rsa.SignPKCS1v15(rand.Reader, s.privateKey, s.hashFunc, digest)
	if err != nil {
		return "", "", errors.Wrap(err, "unable to sign date header")
	}
	signedBase64 := base64.StdEncoding.EncodeToString(signed)
	return signedBase64, "rsa-sha1", nil
}

func (s *PrivateKeySigner) KeyFingerprint() string {
	return s.formattedKeyFingerprint
}

func (s *PrivateKeySigner) DefaultAlgorithm() string {
	return s.algorithm
}
