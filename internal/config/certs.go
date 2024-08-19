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
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"

	"github.com/minio/pkg/v3/env"
)

// EnvCertPassword is the environment variable which contains the password used
// to decrypt the TLS private key. It must be set if the TLS private key is
// password protected.
const EnvCertPassword = "MINIO_CERT_PASSWD"

// ParsePublicCertFile - parses public cert into its *x509.Certificate equivalent.
func ParsePublicCertFile(certFile string) (x509Certs []*x509.Certificate, err error) {
	// Read certificate file.
	var data []byte
	if data, err = os.ReadFile(certFile); err != nil {
		return nil, err
	}

	// Trimming leading and tailing white spaces.
	data = bytes.TrimSpace(data)

	// Parse all certs in the chain.
	current := data
	for len(current) > 0 {
		var pemBlock *pem.Block
		if pemBlock, current = pem.Decode(current); pemBlock == nil {
			return nil, ErrTLSUnexpectedData(nil).Msgf("Could not read PEM block from file %s", certFile)
		}

		var x509Cert *x509.Certificate
		if x509Cert, err = x509.ParseCertificate(pemBlock.Bytes); err != nil {
			return nil, ErrTLSUnexpectedData(nil).Msgf("Failed to parse `%s`: %s", certFile, err.Error())
		}

		x509Certs = append(x509Certs, x509Cert)
	}

	if len(x509Certs) == 0 {
		return nil, ErrTLSUnexpectedData(nil).Msgf("Empty public certificate file %s", certFile)
	}

	return x509Certs, nil
}

// LoadX509KeyPair - load an X509 key pair (private key , certificate)
// from the provided paths. The private key may be encrypted and is
// decrypted using the ENV_VAR: MINIO_CERT_PASSWD.
func LoadX509KeyPair(certFile, keyFile string) (tls.Certificate, error) {
	certPEMBlock, err := os.ReadFile(certFile)
	if err != nil {
		return tls.Certificate{}, ErrTLSReadError(nil).Msgf("Unable to read the public key: %s", err)
	}
	keyPEMBlock, err := os.ReadFile(keyFile)
	if err != nil {
		return tls.Certificate{}, ErrTLSReadError(nil).Msgf("Unable to read the private key: %s", err)
	}
	key, rest := pem.Decode(keyPEMBlock)
	if len(rest) > 0 {
		return tls.Certificate{}, ErrTLSUnexpectedData(nil).Msgf("The private key contains additional data")
	}
	if key == nil {
		return tls.Certificate{}, ErrTLSUnexpectedData(nil).Msgf("The private key is not readable")
	}
	if x509.IsEncryptedPEMBlock(key) {
		password := env.Get(EnvCertPassword, "")
		if len(password) == 0 {
			return tls.Certificate{}, ErrTLSNoPassword(nil)
		}
		decryptedKey, decErr := x509.DecryptPEMBlock(key, []byte(password))
		if decErr != nil {
			return tls.Certificate{}, ErrTLSWrongPassword(decErr)
		}
		keyPEMBlock = pem.EncodeToMemory(&pem.Block{Type: key.Type, Bytes: decryptedKey})
	}
	cert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		return tls.Certificate{}, ErrTLSUnexpectedData(nil).Msg(err.Error())
	}
	return cert, nil
}

// EnsureCertAndKey checks if both client certificate and key paths are provided
func EnsureCertAndKey(clientCert, clientKey string) error {
	if (clientCert != "" && clientKey == "") ||
		(clientCert == "" && clientKey != "") {
		return errors.New("cert and key must be specified as a pair")
	}
	return nil
}
