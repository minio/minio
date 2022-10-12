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

// ListKeys List all key names that match the specified pattern. In particular,
// the pattern * lists all keys.
func (c *kesClient) ListKeys(ctx context.Context, pattern string) (*kes.KeyIterator, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.client.ListKeys(ctx, pattern)
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
	return c.client.ImportKey(ctx, keyID, bytes)
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

// DescribePolicy describes a policy by returning its metadata.
// e.g. who created the policy at which point in time.
func (c *kesClient) DescribePolicy(ctx context.Context, policy string) (*kes.PolicyInfo, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.client.DescribePolicy(ctx, policy)
}

// AssignPolicy assigns a policy to an identity.
// An identity can have at most one policy while the same policy can be assigned to multiple identities.
// The assigned policy defines which API calls this identity can perform.
// It's not possible to assign a policy to the admin identity.
// Further, an identity cannot assign a policy to itself.
func (c *kesClient) AssignPolicy(ctx context.Context, policy, identity string) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.client.AssignPolicy(ctx, policy, kes.Identity(identity))
}

// DeletePolicy	deletes a policy from KMS.
// All identities that have been assigned to this policy will lose all authorization privileges.
func (c *kesClient) DeletePolicy(ctx context.Context, policy string) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.client.DeletePolicy(ctx, policy)
}

// ListPolicies list all policy metadata that match the specified pattern.
// In particular, the pattern * lists all policy metadata.
func (c *kesClient) ListPolicies(ctx context.Context, pattern string) (*kes.PolicyIterator, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.client.ListPolicies(ctx, pattern)
}

// SetPolicy creates or updates a policy.
func (c *kesClient) SetPolicy(ctx context.Context, policy string, policyItem *kes.Policy) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.client.SetPolicy(ctx, policy, policyItem)
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

// DeleteIdentity deletes an identity from KMS.
// The client certificate that corresponds to the identity is no longer authorized to perform any API operations.
// The admin identity cannot be deleted.
func (c *kesClient) DeleteIdentity(ctx context.Context, identity string) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.client.DeleteIdentity(ctx, kes.Identity(identity))
}

// ListIdentities list all identity metadata that match the specified pattern.
// In particular, the pattern * lists all identity metadata.
func (c *kesClient) ListIdentities(ctx context.Context, pattern string) (*kes.IdentityIterator, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.client.ListIdentities(ctx, pattern)
}
