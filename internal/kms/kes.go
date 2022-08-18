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
	"strings"
	"sync"

	"github.com/minio/kes"
	"github.com/minio/pkg/certs"
)

const (
	tlsClientSessionCacheSize = 100
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
	Certificate *certs.Certificate

	// ReloadCertEvents is an event channel that receives
	// the reloaded client certificate.
	ReloadCertEvents <-chan tls.Certificate

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
	endpoints := make([]string, len(config.Endpoints)) // Copy => avoid being affect by any changes to the original slice
	copy(endpoints, config.Endpoints)

	client := kes.NewClientWithConfig("", &tls.Config{
		MinVersion:         tls.VersionTLS12,
		Certificates:       []tls.Certificate{config.Certificate.Get()},
		RootCAs:            config.RootCAs,
		ClientSessionCache: tls.NewLRUClientSessionCache(tlsClientSessionCacheSize),
	})
	client.Endpoints = endpoints

	var bulkAvailable bool
	_, policy, err := client.DescribeSelf(context.Background())
	if err == nil {
		const BulkAPI = "/v1/key/bulk/decrypt/"
		for _, allow := range policy.Allow {
			if strings.HasPrefix(allow, BulkAPI) {
				bulkAvailable = true
				break
			}
		}
	}

	c := &kesClient{
		client:        client,
		defaultKeyID:  config.DefaultKeyID,
		bulkAvailable: bulkAvailable,
	}
	go func() {
		for {
			select {
			case certificate := <-config.ReloadCertEvents:
				client := kes.NewClientWithConfig("", &tls.Config{
					MinVersion:         tls.VersionTLS12,
					Certificates:       []tls.Certificate{certificate},
					RootCAs:            config.RootCAs,
					ClientSessionCache: tls.NewLRUClientSessionCache(tlsClientSessionCacheSize),
				})
				client.Endpoints = endpoints

				c.lock.Lock()
				c.client = client
				c.lock.Unlock()
			}
		}
	}()
	return c, nil
}

type kesClient struct {
	lock         sync.RWMutex
	defaultKeyID string
	client       *kes.Client

	bulkAvailable bool
}

var _ KMS = (*kesClient)(nil) // compiler check

// Stat returns the current KES status containing a
// list of KES endpoints and the default key ID.
func (c *kesClient) Stat(ctx context.Context) (Status, error) {
	if _, err := c.client.Version(ctx); err != nil {
		return Status{}, err
	}
	endpoints := make([]string, len(c.client.Endpoints))
	copy(endpoints, c.client.Endpoints)
	return Status{
		Name:       "KES",
		Endpoints:  endpoints,
		DefaultKey: c.defaultKeyID,
	}, nil
}

func (c *kesClient) Metrics(ctx context.Context) (kes.Metric, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.client.Metrics(ctx)
}

// CreateKey tries to create a new key at the KMS with the
// given key ID.
//
// If the a key with the same keyID already exists then
// CreateKey returns kes.ErrKeyExists.
func (c *kesClient) CreateKey(ctx context.Context, keyID string) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.client.CreateKey(ctx, keyID)
}

// GenerateKey generates a new data encryption key using
// the key at the KES server referenced by the key ID.
//
// The default key ID will be used if keyID is empty.
//
// The context is associated and tied to the generated DEK.
// The same context must be provided when the generated
// key should be decrypted.
func (c *kesClient) GenerateKey(ctx context.Context, keyID string, cryptoCtx Context) (DEK, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if keyID == "" {
		keyID = c.defaultKeyID
	}
	ctxBytes, err := cryptoCtx.MarshalText()
	if err != nil {
		return DEK{}, err
	}
	dek, err := c.client.GenerateKey(ctx, keyID, ctxBytes)
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
	c.lock.RLock()
	defer c.lock.RUnlock()

	ctxBytes, err := ctx.MarshalText()
	if err != nil {
		return nil, err
	}
	return c.client.Decrypt(context.Background(), keyID, ciphertext, ctxBytes)
}

func (c *kesClient) DecryptAll(ctx context.Context, keyID string, ciphertexts [][]byte, contexts []Context) ([][]byte, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if c.bulkAvailable {
		CCPs := make([]kes.CCP, 0, len(ciphertexts))
		for i := range ciphertexts {
			bCtx, err := contexts[i].MarshalText()
			if err != nil {
				return nil, err
			}
			CCPs = append(CCPs, kes.CCP{
				Ciphertext: ciphertexts[i],
				Context:    bCtx,
			})
		}
		PCPs, err := c.client.DecryptAll(ctx, keyID, CCPs...)
		if err != nil {
			return nil, err
		}
		plaintexts := make([][]byte, 0, len(PCPs))
		for _, p := range PCPs {
			plaintexts = append(plaintexts, p.Plaintext)
		}
		return plaintexts, nil
	}

	plaintexts := make([][]byte, 0, len(ciphertexts))
	for i := range ciphertexts {
		plaintext, err := c.DecryptKey(keyID, ciphertexts[i], contexts[i])
		if err != nil {
			return nil, err
		}
		plaintexts = append(plaintexts, plaintext)
	}
	return plaintexts, nil
}
