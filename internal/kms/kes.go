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

package kms

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"time"

	"github.com/minio/kes"
)

// Config contains various KMS-related configuration
// parameters - like KMS endpoints or authentication
// credentials.
type Config struct {
	// Endpoints contains a list of KMS server
	// HTTP endpoints.
	Endpoints []string

	// DefaultKeyID is the key ID used when
	// no explicit key ID is specified for
	// a cryptographic operation.
	DefaultKeyID string

	// Certificate is the client TLS certificate
	// to authenticate to KMS via mTLS.
	Certificate tls.Certificate

	// RootCAs is a set of root CA certificates
	// to verify the KMS server TLS certificate.
	RootCAs *x509.CertPool
}

// NewWithConfig returns a new KMS using the given
// configuration.
func NewWithConfig(config Config) (KMS, error) {
	if len(config.Endpoints) == 0 {
		return nil, errors.New("kms: no server endpoints")
	}
	var endpoints = make([]string, len(config.Endpoints)) // Copy => avoid being affect by any changes to the original slice
	copy(endpoints, config.Endpoints)

	client := kes.NewClientWithConfig("", &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{config.Certificate},
		RootCAs:      config.RootCAs,
	})
	client.Endpoints = endpoints
	return &kesClient{
		client:       client,
		defaultKeyID: config.DefaultKeyID,
	}, nil
}

type kesClient struct {
	defaultKeyID string
	client       *kes.Client
}

var _ KMS = (*kesClient)(nil) // compiler check

// Stat returns the current KES status containing a
// list of KES endpoints and the default key ID.
func (c *kesClient) Stat() (Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.client.Version(ctx); err != nil {
		return Status{}, err
	}
	var endpoints = make([]string, len(c.client.Endpoints))
	copy(endpoints, c.client.Endpoints)
	return Status{
		Name:       "KES",
		Endpoints:  endpoints,
		DefaultKey: c.defaultKeyID,
	}, nil
}

// CreateKey tries to create a new key at the KMS with the
// given key ID.
//
// If the a key with the same keyID already exists then
// CreateKey returns kes.ErrKeyExists.
func (c *kesClient) CreateKey(keyID string) error {
	return c.client.CreateKey(context.Background(), keyID)
}

// GenerateKey generates a new data encryption key using
// the key at the KES server referenced by the key ID.
//
// The default key ID will be used if keyID is empty.
//
// The context is associated and tied to the generated DEK.
// The same context must be provided when the generated
// key should be decrypted.
func (c *kesClient) GenerateKey(keyID string, ctx Context) (DEK, error) {
	if keyID == "" {
		keyID = c.defaultKeyID
	}
	ctxBytes, err := ctx.MarshalText()
	if err != nil {
		return DEK{}, err
	}
	dek, err := c.client.GenerateKey(context.Background(), keyID, ctxBytes)
	if err != nil {
		return DEK{}, err
	}
	return DEK{
		KeyID:      keyID,
		Plaintext:  dek.Plaintext,
		Ciphertext: dek.Ciphertext,
	}, nil
}

// DecryptKey decrypts the ciphertext with the key at the KES
// server referenced by the key ID. The context must match the
// context value used to generate the ciphertext.
func (c *kesClient) DecryptKey(keyID string, ciphertext []byte, ctx Context) ([]byte, error) {
	ctxBytes, err := ctx.MarshalText()
	if err != nil {
		return nil, err
	}
	return c.client.Decrypt(context.Background(), keyID, ciphertext, ctxBytes)
}
