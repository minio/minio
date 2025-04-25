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
	"errors"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/minio/kms-go/kms"
	"github.com/minio/madmin-go/v3"
)

// ListRequest is a structure containing fields
// and options for listing keys.
type ListRequest struct {
	// Prefix is an optional prefix for filtering names.
	// A list operation only returns elements that match
	// this prefix.
	// An empty prefix matches any value.
	Prefix string

	// ContinueAt is the name of the element from where
	// a listing should continue. It allows paginated
	// listings.
	ContinueAt string

	// Limit limits the number of elements returned by
	// a single list operation. If <= 0, a reasonable
	// limit is selected automatically.
	Limit int
}

// CreateKeyRequest is a structure containing fields
// and options for creating keys.
type CreateKeyRequest struct {
	// Name is the name of the key that gets created.
	Name string
}

// DeleteKeyRequest is a structure containing fields
// and options for deleting keys.
type DeleteKeyRequest struct {
	// Name is the name of the key that gets deleted.
	Name string
}

// GenerateKeyRequest is a structure containing fields
// and options for generating data keys.
type GenerateKeyRequest struct {
	// Name is the name of the master key used to generate
	// the data key.
	Name string

	// AssociatedData is optional data that is cryptographically
	// associated with the generated data key. The same data
	// must be provided when decrypting an encrypted data key.
	//
	// Typically, associated data is some metadata about the
	// data key. For example, the name of the object for which
	// the data key is used.
	AssociatedData Context
}

// DecryptRequest is a structure containing fields
// and options for decrypting data.
type DecryptRequest struct {
	// Name is the name of the master key used decrypt
	// the ciphertext.
	Name string

	// Version is the version of the master used for
	// decryption. If empty, the latest key version
	// is used.
	Version int

	// Ciphertext is the encrypted data that gets
	// decrypted.
	Ciphertext []byte

	// AssociatedData is the crypto. associated data.
	// It must match the data used during encryption
	// or data key generation.
	AssociatedData Context
}

// MACRequest is a structure containing fields
// and options for generating message authentication
// codes (MAC).
type MACRequest struct {
	// Name is the name of the master key used decrypt
	// the ciphertext.
	Name string

	Version int

	Message []byte
}

// Metrics is a structure containing KMS metrics.
type Metrics struct {
	ReqOK   uint64                   `json:"kms_req_success"` // Number of requests that succeeded
	ReqErr  uint64                   `json:"kms_req_error"`   // Number of requests that failed with a defined error
	ReqFail uint64                   `json:"kms_req_failure"` // Number of requests that failed with an undefined error
	Latency map[time.Duration]uint64 `json:"kms_resp_time"`   // Latency histogram of all requests
}

var defaultLatencyBuckets = []time.Duration{
	10 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	1000 * time.Millisecond, // 1s
	1500 * time.Millisecond,
	3000 * time.Millisecond,
	5000 * time.Millisecond,
	10000 * time.Millisecond, // 10s
}

// KMS is a connection to a key management system.
// It implements various cryptographic operations,
// like data key generation and decryption.
type KMS struct {
	// Type identifies the KMS implementation. Either,
	// MinKMS, MinKES or Builtin.
	Type Type

	// The default key, used for generating new data keys
	// if no explicit GenerateKeyRequest.Name is provided.
	DefaultKey string

	conn conn // Connection to the KMS

	// Metrics
	reqOK, reqErr, reqFail atomic.Uint64
	latencyBuckets         []time.Duration // expected to be sorted
	latency                []atomic.Uint64
}

// Version returns version information about the KMS.
//
// TODO(aead): refactor this API call since it does not account
// for multiple KMS/KES servers.
func (k *KMS) Version(ctx context.Context) (string, error) {
	return k.conn.Version(ctx)
}

// APIs returns a list of KMS server APIs.
//
// TODO(aead): remove this API since it's hardly useful.
func (k *KMS) APIs(ctx context.Context) ([]madmin.KMSAPI, error) {
	return k.conn.APIs(ctx)
}

// Metrics returns a current snapshot of the KMS metrics.
func (k *KMS) Metrics(ctx context.Context) (*Metrics, error) {
	latency := make(map[time.Duration]uint64, len(k.latencyBuckets))
	for i, b := range k.latencyBuckets {
		latency[b] = k.latency[i].Load()
	}

	return &Metrics{
		ReqOK:   k.reqOK.Load(),
		ReqErr:  k.reqErr.Load(),
		ReqFail: k.reqFail.Load(),
		Latency: latency,
	}, nil
}

// Status returns status information about the KMS.
//
// TODO(aead): refactor this API call since it does not account
// for multiple KMS/KES servers.
func (k *KMS) Status(ctx context.Context) (*madmin.KMSStatus, error) {
	endpoints, err := k.conn.Status(ctx)
	if err != nil {
		return nil, err
	}

	return &madmin.KMSStatus{
		Name:         k.Type.String(),
		DefaultKeyID: k.DefaultKey,
		Endpoints:    endpoints,
	}, nil
}

// CreateKey creates the master key req.Name. It returns
// ErrKeyExists if the key already exists.
func (k *KMS) CreateKey(ctx context.Context, req *CreateKeyRequest) error {
	start := time.Now()
	err := k.conn.CreateKey(ctx, req)
	k.updateMetrics(err, time.Since(start))

	return err
}

// ListKeys returns a list of keys with metadata and a potential
// next name from where to continue a subsequent listing.
func (k *KMS) ListKeys(ctx context.Context, req *ListRequest) ([]madmin.KMSKeyInfo, string, error) {
	if req.Prefix == "*" {
		req.Prefix = ""
	}
	return k.conn.ListKeys(ctx, req)
}

// GenerateKey generates a new data key using the master key req.Name.
// It returns ErrKeyNotFound if the key does not exist. If req.Name is
// empty, the KMS default key is used.
func (k *KMS) GenerateKey(ctx context.Context, req *GenerateKeyRequest) (DEK, error) {
	if req.Name == "" {
		req.Name = k.DefaultKey
	}

	start := time.Now()
	dek, err := k.conn.GenerateKey(ctx, req)
	k.updateMetrics(err, time.Since(start))

	return dek, err
}

// Decrypt decrypts a ciphertext using the master key req.Name.
// It returns ErrKeyNotFound if the key does not exist.
func (k *KMS) Decrypt(ctx context.Context, req *DecryptRequest) ([]byte, error) {
	start := time.Now()
	plaintext, err := k.conn.Decrypt(ctx, req)
	k.updateMetrics(err, time.Since(start))

	return plaintext, err
}

// MAC generates the checksum of the given req.Message using the key
// with the req.Name at the KMS.
func (k *KMS) MAC(ctx context.Context, req *MACRequest) ([]byte, error) {
	if req.Name == "" {
		req.Name = k.DefaultKey
	}

	start := time.Now()
	mac, err := k.conn.MAC(ctx, req)
	k.updateMetrics(err, time.Since(start))

	return mac, err
}

func (k *KMS) updateMetrics(err error, latency time.Duration) {
	// First, update the latency histogram
	// Therefore, find the first bucket that holds the counter for
	// requests with a latency at least as large as the given request
	// latency and update its and all subsequent counters.
	bucket := slices.IndexFunc(k.latencyBuckets, func(b time.Duration) bool { return latency < b })
	if bucket < 0 {
		bucket = len(k.latencyBuckets) - 1
	}
	for i := bucket; i < len(k.latency); i++ {
		k.latency[i].Add(1)
	}

	// Next, update the request counters
	if err == nil {
		k.reqOK.Add(1)
		return
	}

	var s3Err Error
	if errors.As(err, &s3Err) && s3Err.Code >= http.StatusInternalServerError {
		k.reqFail.Add(1)
	} else {
		k.reqErr.Add(1)
	}
}

type kmsConn struct {
	endpoints  []string
	enclave    string
	defaultKey string
	client     *kms.Client
}

func (c *kmsConn) Version(ctx context.Context) (string, error) {
	resp, err := c.client.Version(ctx, &kms.VersionRequest{})
	if len(resp) == 0 && err != nil {
		return "", err
	}
	return resp[0].Version, nil
}

func (c *kmsConn) APIs(ctx context.Context) ([]madmin.KMSAPI, error) {
	return nil, ErrNotSupported
}

func (c *kmsConn) Status(ctx context.Context) (map[string]madmin.ItemState, error) {
	stat := make(map[string]madmin.ItemState, len(c.endpoints))
	resp, err := c.client.Version(ctx, &kms.VersionRequest{})

	for _, r := range resp {
		stat[r.Host] = madmin.ItemOnline
	}
	for _, e := range kms.UnwrapHostErrors(err) {
		stat[e.Host] = madmin.ItemOffline
	}
	return stat, nil
}

func (c *kmsConn) ListKeys(ctx context.Context, req *ListRequest) ([]madmin.KMSKeyInfo, string, error) {
	resp, err := c.client.ListKeys(ctx, &kms.ListRequest{
		Enclave:    c.enclave,
		Prefix:     req.Prefix,
		ContinueAt: req.ContinueAt,
		Limit:      req.Limit,
	})
	if err != nil {
		return nil, "", errListingKeysFailed(err)
	}

	keyInfos := make([]madmin.KMSKeyInfo, len(resp.Items))
	for i, v := range resp.Items {
		keyInfos[i].Name = v.Name
		keyInfos[i].CreatedAt = v.CreatedAt
		keyInfos[i].CreatedBy = v.CreatedBy.String()
	}
	return keyInfos, resp.ContinueAt, nil
}

func (c *kmsConn) CreateKey(ctx context.Context, req *CreateKeyRequest) error {
	if err := c.client.CreateKey(ctx, c.enclave, &kms.CreateKeyRequest{
		Name: req.Name,
	}); err != nil {
		if errors.Is(err, kms.ErrKeyExists) {
			return ErrKeyExists
		}
		if errors.Is(err, kms.ErrPermission) {
			return ErrPermission
		}
		return errKeyCreationFailed(err)
	}
	return nil
}

func (c *kmsConn) GenerateKey(ctx context.Context, req *GenerateKeyRequest) (DEK, error) {
	aad, err := req.AssociatedData.MarshalText()
	if err != nil {
		return DEK{}, err
	}

	name := req.Name
	if name == "" {
		name = c.defaultKey
	}

	resp, err := c.client.GenerateKey(ctx, c.enclave, &kms.GenerateKeyRequest{
		Name:           name,
		AssociatedData: aad,
		Length:         32,
	})
	if err != nil {
		if errors.Is(err, kms.ErrKeyNotFound) {
			return DEK{}, ErrKeyNotFound
		}
		if errors.Is(err, kms.ErrPermission) {
			return DEK{}, ErrPermission
		}
		return DEK{}, errKeyGenerationFailed(err)
	}

	return DEK{
		KeyID:      name,
		Version:    resp[0].Version,
		Plaintext:  resp[0].Plaintext,
		Ciphertext: resp[0].Ciphertext,
	}, nil
}

func (c *kmsConn) Decrypt(ctx context.Context, req *DecryptRequest) ([]byte, error) {
	aad, err := req.AssociatedData.MarshalText()
	if err != nil {
		return nil, err
	}

	ciphertext, _ := parseCiphertext(req.Ciphertext)
	resp, err := c.client.Decrypt(ctx, c.enclave, &kms.DecryptRequest{
		Name:           req.Name,
		Ciphertext:     ciphertext,
		AssociatedData: aad,
	})
	if err != nil {
		if errors.Is(err, kms.ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}
		if errors.Is(err, kms.ErrPermission) {
			return nil, ErrPermission
		}
		return nil, errDecryptionFailed(err)
	}
	return resp[0].Plaintext, nil
}

// MAC generates the checksum of the given req.Message using the key
// with the req.Name at the KMS.
func (*kmsConn) MAC(context.Context, *MACRequest) ([]byte, error) {
	return nil, ErrNotSupported
}
