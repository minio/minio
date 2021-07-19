// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package config

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/ioutil"

	"github.com/minio/pkg/env"
)

// EnvCertPassword is the environment variable which contains the password used
// to decrypt the TLS private key. It must be set if the TLS private key is
// password protected.
const EnvCertPassword = "MINIO_CERT_PASSWD"

// ParsePublicCertFile - parses public cert into its *x509.Certificate equivalent.
func ParsePublicCertFile(certFile string) (x509Certs []*x509.Certificate, err error) {
	// Read certificate file.
	var data []byte
	if data, err = ioutil.ReadFile(certFile); err != nil {
		return nil, err
	}

	// Trimming leading and tailing white spaces.
	data = bytes.TrimSpace(data)

	// Parse all certs in the chain.
	current := data
	for len(current) > 0 {
		var pemBlock *pem.Block
		if pemBlock, current = pem.Decode(current); pemBlock == nil {
			return nil, ErrSSLUnexpectedData(nil).Msg("Could not read PEM block from file %s", certFile)
		}

		var x509Cert *x509.Certificate
		if x509Cert, err = x509.ParseCertificate(pemBlock.Bytes); err != nil {
			return nil, ErrSSLUnexpectedData(err)
		}

		x509Certs = append(x509Certs, x509Cert)
	}

	if len(x509Certs) == 0 {
		return nil, ErrSSLUnexpectedData(nil).Msg("Empty public certificate file %s", certFile)
	}

	return x509Certs, nil
}

// LoadX509KeyPair - load an X509 key pair (private key , certificate)
// from the provided paths. The private key may be encrypted and is
// decrypted using the ENV_VAR: MINIO_CERT_PASSWD.
func LoadX509KeyPair(certFile, keyFile string) (tls.Certificate, error) {
	certPEMBlock, err := ioutil.ReadFile(certFile)
	if err != nil {
		return tls.Certificate{}, ErrSSLUnexpectedError(err)
	}
	keyPEMBlock, err := ioutil.ReadFile(keyFile)
	if err != nil {
		return tls.Certificate{}, ErrSSLUnexpectedError(err)
	}
	key, rest := pem.Decode(keyPEMBlock)
	if len(rest) > 0 {
		return tls.Certificate{}, ErrSSLUnexpectedData(nil).Msg("The private key contains additional data")
	}
	if x509.IsEncryptedPEMBlock(key) {
		password := env.Get(EnvCertPassword, "")
		if len(password) == 0 {
			return tls.Certificate{}, ErrSSLNoPassword(nil)
		}
		decryptedKey, decErr := x509.DecryptPEMBlock(key, []byte(password))
		if decErr != nil {
			return tls.Certificate{}, ErrSSLWrongPassword(decErr)
		}
		keyPEMBlock = pem.EncodeToMemory(&pem.Block{Type: key.Type, Bytes: decryptedKey})
	}
	cert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		return tls.Certificate{}, ErrSSLUnexpectedData(nil).Msg(err.Error())
	}
	// Ensure that the private key is not a P-384 or P-521 EC key.
	// The Go TLS stack does not provide constant-time implementations of P-384 and P-521.
	if priv, ok := cert.PrivateKey.(crypto.Signer); ok {
		if pub, ok := priv.Public().(*ecdsa.PublicKey); ok {
			switch pub.Params().Name {
			case "P-384":
				fallthrough
			case "P-521":
				// unfortunately there is no cleaner way to check
				return tls.Certificate{}, ErrSSLUnexpectedData(nil).Msg("tls: the ECDSA curve '%s' is not supported", pub.Params().Name)
			}
		}
	}
	return cert, nil
}

// EnsureCertAndKey checks if both client certificate and key paths are provided
func EnsureCertAndKey(ClientCert, ClientKey string) error {
	if (ClientCert != "" && ClientKey == "") ||
		(ClientCert == "" && ClientKey != "") {
		return errors.New("cert and key must be specified as a pair")
	}
	return nil
}
