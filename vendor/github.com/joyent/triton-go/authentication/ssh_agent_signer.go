//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package authentication

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"strings"

	pkgerrors "github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

var (
	ErrUnsetEnvVar = pkgerrors.New("environment variable SSH_AUTH_SOCK not set")
)

type SSHAgentSigner struct {
	formattedKeyFingerprint string
	keyFingerprint          string
	algorithm               string
	accountName             string
	userName                string
	keyIdentifier           string

	agent agent.Agent
	key   ssh.PublicKey
}

type SSHAgentSignerInput struct {
	KeyID       string
	AccountName string
	Username    string
}

func NewSSHAgentSigner(input SSHAgentSignerInput) (*SSHAgentSigner, error) {
	sshAgentAddress, agentOk := os.LookupEnv("SSH_AUTH_SOCK")
	if !agentOk {
		return nil, ErrUnsetEnvVar
	}

	conn, err := net.Dial("unix", sshAgentAddress)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "unable to dial SSH agent")
	}

	ag := agent.NewClient(conn)

	signer := &SSHAgentSigner{
		keyFingerprint: input.KeyID,
		accountName:    input.AccountName,
		agent:          ag,
	}

	if input.Username != "" {
		signer.userName = input.Username
	}

	matchingKey, err := signer.MatchKey()
	if err != nil {
		return nil, err
	}
	signer.key = matchingKey
	signer.formattedKeyFingerprint = formatPublicKeyFingerprint(signer.key, true)

	_, algorithm, err := signer.SignRaw("HelloWorld")
	if err != nil {
		return nil, fmt.Errorf("Cannot sign using ssh agent: %s", err)
	}
	signer.algorithm = algorithm

	return signer, nil
}

func (s *SSHAgentSigner) MatchKey() (ssh.PublicKey, error) {
	keys, err := s.agent.List()
	if err != nil {
		return nil, pkgerrors.Wrap(err, "unable to list keys in SSH Agent")
	}

	keyFingerprintStripped := strings.TrimPrefix(s.keyFingerprint, "MD5:")
	keyFingerprintStripped = strings.TrimPrefix(keyFingerprintStripped, "SHA256:")
	keyFingerprintStripped = strings.Replace(keyFingerprintStripped, ":", "", -1)

	var matchingKey ssh.PublicKey
	for _, key := range keys {
		keyMD5 := md5.New()
		keyMD5.Write(key.Marshal())
		finalizedMD5 := fmt.Sprintf("%x", keyMD5.Sum(nil))

		keySHA256 := sha256.New()
		keySHA256.Write(key.Marshal())
		finalizedSHA256 := base64.RawStdEncoding.EncodeToString(keySHA256.Sum(nil))

		if keyFingerprintStripped == finalizedMD5 || keyFingerprintStripped == finalizedSHA256 {
			matchingKey = key
		}
	}

	if matchingKey == nil {
		return nil, fmt.Errorf("No key in the SSH Agent matches fingerprint: %s", s.keyFingerprint)
	}

	return matchingKey, nil
}

func (s *SSHAgentSigner) Sign(dateHeader string, isManta bool) (string, error) {
	const headerName = "date"

	signature, err := s.agent.Sign(s.key, []byte(fmt.Sprintf("%s: %s", headerName, dateHeader)))
	if err != nil {
		return "", pkgerrors.Wrap(err, "unable to sign date header")
	}

	keyFormat, err := keyFormatToKeyType(signature.Format)
	if err != nil {
		return "", pkgerrors.Wrap(err, "unable to format signature")
	}

	key := &KeyID{
		UserName:    s.userName,
		AccountName: s.accountName,
		Fingerprint: s.formattedKeyFingerprint,
		IsManta:     isManta,
	}

	var authSignature httpAuthSignature
	switch keyFormat {
	case "rsa":
		authSignature, err = newRSASignature(signature.Blob)
		if err != nil {
			return "", pkgerrors.Wrap(err, "unable to read RSA signature")
		}
	case "ecdsa":
		authSignature, err = newECDSASignature(signature.Blob)
		if err != nil {
			return "", pkgerrors.Wrap(err, "unable to read ECDSA signature")
		}
	default:
		return "", fmt.Errorf("Unsupported algorithm from SSH agent: %s", signature.Format)
	}

	return fmt.Sprintf(authorizationHeaderFormat, key.generate(),
		authSignature.SignatureType(), headerName, authSignature.String()), nil
}

func (s *SSHAgentSigner) SignRaw(toSign string) (string, string, error) {
	signature, err := s.agent.Sign(s.key, []byte(toSign))
	if err != nil {
		return "", "", pkgerrors.Wrap(err, "unable to sign string")
	}

	keyFormat, err := keyFormatToKeyType(signature.Format)
	if err != nil {
		return "", "", pkgerrors.Wrap(err, "unable to format key")
	}

	var authSignature httpAuthSignature
	switch keyFormat {
	case "rsa":
		authSignature, err = newRSASignature(signature.Blob)
		if err != nil {
			return "", "", pkgerrors.Wrap(err, "unable to read RSA signature")
		}
	case "ecdsa":
		authSignature, err = newECDSASignature(signature.Blob)
		if err != nil {
			return "", "", pkgerrors.Wrap(err, "unable to read ECDSA signature")
		}
	default:
		return "", "", fmt.Errorf("Unsupported algorithm from SSH agent: %s", signature.Format)
	}

	return authSignature.String(), authSignature.SignatureType(), nil
}

func (s *SSHAgentSigner) KeyFingerprint() string {
	return s.formattedKeyFingerprint
}

func (s *SSHAgentSigner) DefaultAlgorithm() string {
	return s.algorithm
}
