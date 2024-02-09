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
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/minio/kms-go/kes"
	"github.com/minio/pkg/v2/certs"
	"github.com/minio/pkg/v2/env"
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

	// APIKey is an credential provided by env. var.
	// to authenticate to a KES server. Either an
	// API key or a client certificate must be specified.
	APIKey kes.APIKey

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

	var client *kes.Client
	if config.APIKey != nil {
		cert, err := kes.GenerateCertificate(config.APIKey)
		if err != nil {
			return nil, err
		}
		client = kes.NewClientWithConfig("", &tls.Config{
			MinVersion:         tls.VersionTLS12,
			Certificates:       []tls.Certificate{cert},
			RootCAs:            config.RootCAs,
			ClientSessionCache: tls.NewLRUClientSessionCache(tlsClientSessionCacheSize),
		})
	} else {
		client = kes.NewClientWithConfig("", &tls.Config{
			MinVersion:         tls.VersionTLS12,
			Certificates:       []tls.Certificate{config.Certificate.Get()},
			RootCAs:            config.RootCAs,
			ClientSessionCache: tls.NewLRUClientSessionCache(tlsClientSessionCacheSize),
		})
	}
	client.Endpoints = endpoints

	c := &kesClient{
		client:       client,
		defaultKeyID: config.DefaultKeyID,
	}
	go func() {
		if config.Certificate == nil || config.ReloadCertEvents == nil {
			return
		}
		var prevCertificate tls.Certificate
		for {
			certificate, ok := <-config.ReloadCertEvents
			if !ok {
				return
			}
			sameCert := len(certificate.Certificate) == len(prevCertificate.Certificate)
			for i, b := range certificate.Certificate {
				if !sameCert {
					break
				}
				sameCert = sameCert && bytes.Equal(b, prevCertificate.Certificate[i])
			}
			// Do not reload if its the same cert as before.
			if !sameCert {
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

				prevCertificate = certificate
			}
		}
	}()
	return c, nil
}

type kesClient struct {
	lock         sync.RWMutex
	defaultKeyID string
	client       *kes.Client
}

var ( // compiler checks
	_ KMS             = (*kesClient)(nil)
	_ KeyManager      = (*kesClient)(nil)
	_ IdentityManager = (*kesClient)(nil)
	_ PolicyManager   = (*kesClient)(nil)
)

// Stat returns the current KES status containing a
// list of KES endpoints and the default key ID.
func (c *kesClient) Stat(ctx context.Context) (Status, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	st, err := c.client.Status(ctx)
	if err != nil {
		return Status{}, err
	}
	endpoints := make([]string, len(c.client.Endpoints))
	copy(endpoints, c.client.Endpoints)
	return Status{
		Name:       "KES",
		Endpoints:  endpoints,
		DefaultKey: c.defaultKeyID,
		Details:    st,
	}, nil
}

// IsLocal returns true if the KMS is a local implementation
func (c *kesClient) IsLocal() bool {
	return env.IsSet(EnvKMSSecretKey)
}

// List returns an array of local KMS Names
func (c *kesClient) List() []kes.KeyInfo {
	var kmsSecret []kes.KeyInfo
	envKMSSecretKey := env.Get(EnvKMSSecretKey, "")
	values := strings.SplitN(envKMSSecretKey, ":", 2)
	if len(values) == 2 {
		kmsSecret = []kes.KeyInfo{
			{
				Name: values[0],
			},
		}
	}
	return kmsSecret
}

// Metrics retrieves server metrics in the Prometheus exposition format.
func (c *kesClient) Metrics(ctx context.Context) (kes.Metric, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.client.Metrics(ctx)
}

// Version retrieves version information
func (c *kesClient) Version(ctx context.Context) (string, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.client.Version(ctx)
}

// APIs retrieves a list of supported API endpoints
func (c *kesClient) APIs(ctx context.Context) ([]kes.API, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.client.APIs(ctx)
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

// DeleteKey deletes a key at the KMS with the given key ID.
// Please note that is a dangerous operation.
// Once a key has been deleted all data that has been encrypted with it cannot be decrypted
// anymore, and therefore, is lost.
func (c *kesClient) DeleteKey(ctx context.Context, keyID string) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.client.DeleteKey(ctx, keyID)
}

// ListKeys returns an iterator over all key names.
func (c *kesClient) ListKeys(ctx context.Context) (*kes.ListIter[string], error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return &kes.ListIter[string]{
		NextFunc: c.client.ListKeys,
	}, nil
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

// ImportKey imports a cryptographic key into the KMS.
func (c *kesClient) ImportKey(ctx context.Context, keyID string, bytes []byte) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.client.ImportKey(ctx, keyID, &kes.ImportKeyRequest{
		Key: bytes,
	})
}

// EncryptKey Encrypts and authenticates a (small) plaintext with the cryptographic key
// The plaintext must not exceed 1 MB
func (c *kesClient) EncryptKey(keyID string, plaintext []byte, ctx Context) ([]byte, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	ctxBytes, err := ctx.MarshalText()
	if err != nil {
		return nil, err
	}
	return c.client.Encrypt(context.Background(), keyID, plaintext, ctxBytes)
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

	plaintexts := make([][]byte, 0, len(ciphertexts))
	for i := range ciphertexts {
		ctxBytes, err := contexts[i].MarshalText()
		if err != nil {
			return nil, err
		}
		plaintext, err := c.client.Decrypt(ctx, keyID, ciphertexts[i], ctxBytes)
		if err != nil {
			return nil, err
		}
		plaintexts = append(plaintexts, plaintext)
	}
	return plaintexts, nil
}

// HMAC generates the HMAC checksum of the given msg using the key
// with the given keyID at the KMS.
func (c *kesClient) HMAC(ctx context.Context, keyID string, msg []byte) ([]byte, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.client.HMAC(context.Background(), keyID, msg)
}

// DescribePolicy describes a policy by returning its metadata.
// e.g. who created the policy at which point in time.
func (c *kesClient) DescribePolicy(ctx context.Context, policy string) (*kes.PolicyInfo, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.client.DescribePolicy(ctx, policy)
}

// ListPolicies returns an iterator over all policy names.
func (c *kesClient) ListPolicies(ctx context.Context) (*kes.ListIter[string], error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return &kes.ListIter[string]{
		NextFunc: c.client.ListPolicies,
	}, nil
}

// GetPolicy gets a policy from KMS.
func (c *kesClient) GetPolicy(ctx context.Context, policy string) (*kes.Policy, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.client.GetPolicy(ctx, policy)
}

// DescribeIdentity describes an identity by returning its metadata.
// e.g. which policy is currently assigned and whether its an admin identity.
func (c *kesClient) DescribeIdentity(ctx context.Context, identity string) (*kes.IdentityInfo, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.client.DescribeIdentity(ctx, kes.Identity(identity))
}

// DescribeSelfIdentity describes the identity issuing the request.
// It infers the identity from the TLS client certificate used to authenticate.
// It returns the identity and policy information for the client identity.
func (c *kesClient) DescribeSelfIdentity(ctx context.Context) (*kes.IdentityInfo, *kes.Policy, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.client.DescribeSelf(ctx)
}

// ListPolicies returns an iterator over all identities.
func (c *kesClient) ListIdentities(ctx context.Context) (*kes.ListIter[kes.Identity], error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return &kes.ListIter[kes.Identity]{
		NextFunc: c.client.ListIdentities,
	}, nil
}

// Verify verifies all KMS endpoints and returns details
func (c *kesClient) Verify(ctx context.Context) []VerifyResult {
	c.lock.RLock()
	defer c.lock.RUnlock()

	results := []VerifyResult{}
	kmsContext := Context{"MinIO admin API": "ServerInfoHandler"} // Context for a test key operation
	for _, endpoint := range c.client.Endpoints {
		client := kes.Client{
			Endpoints:  []string{endpoint},
			HTTPClient: c.client.HTTPClient,
		}

		// 1. Get stats for the KES instance
		state, err := client.Status(ctx)
		if err != nil {
			results = append(results, VerifyResult{Status: "offline", Endpoint: endpoint})
			continue
		}

		// 2. Generate a new key using the KMS.
		kmsCtx, err := kmsContext.MarshalText()
		if err != nil {
			results = append(results, VerifyResult{Status: "offline", Endpoint: endpoint})
			continue
		}
		result := VerifyResult{Status: "online", Endpoint: endpoint, Version: state.Version}
		key, err := client.GenerateKey(ctx, env.Get(EnvKESKeyName, ""), kmsCtx)
		if err != nil {
			result.Encrypt = fmt.Sprintf("Encryption failed: %v", err)
		} else {
			result.Encrypt = "success"
		}
		// 3. Verify that we can indeed decrypt the (encrypted) key
		decryptedKey, err := client.Decrypt(ctx, env.Get(EnvKESKeyName, ""), key.Ciphertext, kmsCtx)
		switch {
		case err != nil:
			result.Decrypt = fmt.Sprintf("Decryption failed: %v", err)
		case subtle.ConstantTimeCompare(key.Plaintext, decryptedKey) != 1:
			result.Decrypt = "Decryption failed: decrypted key does not match generated key"
		default:
			result.Decrypt = "success"
		}
		results = append(results, result)
	}
	return results
}
