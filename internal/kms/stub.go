// Copyright (c) 2015-2024 MinIO, Inc.
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
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/pkg/v3/wildcard"
)

var (
	// StubCreatedAt is a constant timestamp for testing
	StubCreatedAt = time.Date(2024, time.January, 1, 15, 0, 0, 0, time.UTC)
	// StubCreatedBy is a constant created identity for testing
	StubCreatedBy = "MinIO"
)

// NewStub returns a stub of KMS for testing
func NewStub(defaultKeyName string) *KMS {
	return &KMS{
		Type:           Builtin,
		DefaultKey:     defaultKeyName,
		latencyBuckets: defaultLatencyBuckets,
		latency:        make([]atomic.Uint64, len(defaultLatencyBuckets)),
		conn: &StubKMS{
			KeyNames: []string{defaultKeyName},
		},
	}
}

// StubKMS is a KMS implementation for tests
type StubKMS struct {
	KeyNames []string
}

// Version returns the type of the KMS.
func (s StubKMS) Version(ctx context.Context) (string, error) {
	return "stub", nil
}

// APIs returns supported APIs
func (s StubKMS) APIs(ctx context.Context) ([]madmin.KMSAPI, error) {
	return []madmin.KMSAPI{
		{Method: http.MethodGet, Path: "stub/path"},
	}, nil
}

// Status returns a set of endpoints and their KMS status.
func (s StubKMS) Status(context.Context) (map[string]madmin.ItemState, error) {
	return map[string]madmin.ItemState{
		"127.0.0.1": madmin.ItemOnline,
	}, nil
}

// ListKeys returns a list of keys with metadata.
func (s StubKMS) ListKeys(ctx context.Context, req *ListRequest) ([]madmin.KMSKeyInfo, string, error) {
	matches := []madmin.KMSKeyInfo{}
	if req.Prefix == "" {
		req.Prefix = "*"
	}
	for _, keyName := range s.KeyNames {
		if wildcard.MatchAsPatternPrefix(req.Prefix, keyName) {
			matches = append(matches, madmin.KMSKeyInfo{Name: keyName, CreatedAt: StubCreatedAt, CreatedBy: StubCreatedBy})
		}
	}

	return matches, "", nil
}

// CreateKey creates a new key with the given name.
func (s *StubKMS) CreateKey(_ context.Context, req *CreateKeyRequest) error {
	if s.containsKeyName(req.Name) {
		return ErrKeyExists
	}
	s.KeyNames = append(s.KeyNames, req.Name)
	return nil
}

// GenerateKey is a non-functional stub.
func (s StubKMS) GenerateKey(_ context.Context, req *GenerateKeyRequest) (DEK, error) {
	if !s.containsKeyName(req.Name) {
		return DEK{}, ErrKeyNotFound
	}
	return DEK{
		KeyID:      req.Name,
		Version:    0,
		Plaintext:  []byte("stubplaincharswhichare32bytelong"),
		Ciphertext: []byte("stubplaincharswhichare32bytelong"),
	}, nil
}

// Decrypt is a non-functional stub.
func (s StubKMS) Decrypt(_ context.Context, req *DecryptRequest) ([]byte, error) {
	return req.Ciphertext, nil
}

// MAC is a non-functional stub.
func (s StubKMS) MAC(_ context.Context, m *MACRequest) ([]byte, error) {
	return m.Message, nil
}

// containsKeyName returns true if the given key name exists in the stub KMS.
func (s *StubKMS) containsKeyName(keyName string) bool {
	return slices.Contains(s.KeyNames, keyName)
}
