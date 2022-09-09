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
	"encoding"
	"encoding/json"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/kes"
)

// KMS is the generic interface that abstracts over
// different KMS implementations.
type KMS interface {
	// Stat returns the current KMS status.
	Stat(cxt context.Context) (Status, error)

	// Metrics returns a KMS metric snapshot.
	Metrics(ctx context.Context) (kes.Metric, error)

	// CreateKey creates a new key at the KMS with the given key ID.
	CreateKey(ctx context.Context, keyID string) error

	// DeleteKey deletes a key at the KMS with the given key ID.
	// Please note that is a dangerous operation.
	// Once a key has been deleted all data that has been encrypted with it cannot be decrypted
	// anymore, and therefore, is lost.
	DeleteKey(ctx context.Context, keyID string) error

	// ListKeys List all key names that match the specified pattern. In particular,
	// the pattern * lists all keys.
	ListKeys(ctx context.Context, pattern string) ([]KMSKey, error)

	// GenerateKey generates a new data encryption key using the
	// key referenced by the key ID.
	//
	// The KMS may use a default key if the key ID is empty.
	// GenerateKey returns an error if the referenced key does
	// not exist.
	//
	// The context is associated and tied to the generated DEK.
	// The same context must be provided when the generated key
	// should be decrypted. Therefore, it is the callers
	// responsibility to remember the corresponding context for
	// a particular DEK. The context may be nil.
	GenerateKey(ctx context.Context, keyID string, context Context) (DEK, error)

	// ImportKey imports a cryptographic key into the KMS.
	ImportKey(ctx context.Context, keyID string, bytes []byte) error

	// EncryptKey Encrypts and authenticates a (small) plaintext with the cryptographic key
	// The plaintext must not exceed 1 MB
	EncryptKey(keyID string, plaintext []byte, context Context) ([]byte, error)

	// DecryptKey decrypts the ciphertext with the key referenced
	// by the key ID. The context must match the context value
	// used to generate the ciphertext.
	DecryptKey(keyID string, ciphertext []byte, context Context) ([]byte, error)

	// DecryptAll decrypts all ciphertexts with the key referenced
	// by the key ID. The contexts must match the context value
	// used to generate the ciphertexts.
	DecryptAll(ctx context.Context, keyID string, ciphertext [][]byte, context []Context) ([][]byte, error)

	// DescribePolicy describes a policy by returning its metadata.
	// e.g. who created the policy at which point in time.
	DescribePolicy(ctx context.Context, policy string) (*PolicyInfo, error)
	// AssignPolicy assigns a policy to an identity.
	// An identity can have at most one policy while the same policy can be assigned to multiple identities.
	// The assigned policy defines which API calls this identity can perform.
	// It's not possible to assign a policy to the admin identity.
	// Further, an identity cannot assign a policy to itself.
	AssignPolicy(ctx context.Context, policy, identity string) error
	// SetPolicy creates or updates a policy.
	SetPolicy(ctx context.Context, policy, data string) error
	// GetPolicy gets a policy from KMS.
	GetPolicy(ctx context.Context, policy string) (*kes.Policy, error)
	// ListPolicies list all policy metadata that match the specified pattern.
	// In particular, the pattern * lists all policy metadata.
	ListPolicies(ctx context.Context, pattern string) ([]PolicyInfo, error)
	// DeletePolicy	deletes a policy from KMS.
	// All identities that have been assigned to this policy will lose all authorization privileges.
	DeletePolicy(ctx context.Context, policy string) error

	// DescribeIdentity describes an identity by returning its metadata.
	// e.g. which policy is currently assigned and whether its an admin identity.
	DescribeIdentity(ctx context.Context, identity string) (*Identity, error)
	// DescribeSelfIdentity describes the identity issuing the request.
	// It infers the identity from the TLS client certificate used to authenticate.
	// It returns the identity and policy information for the client identity.
	DescribeSelfIdentity(ctx context.Context) (*SelfIdentity, error)
	// 	DeleteIdentity deletes an identity from KMS.
	// The client certificate that corresponds to the identity is no longer authorized to perform any API operations.
	// The admin identity cannot be deleted.
	DeleteIdentity(ctx context.Context, identity string) error
	// ListIdentities list all identity metadata that match the specified pattern.
	// In particular, the pattern * lists all identity metadata.
	ListIdentities(ctx context.Context, pattern string) ([]Identity, error)
}

// Status describes the current state of a KMS.
type Status struct {
	Name      string   // The name of the KMS
	Endpoints []string // A set of the KMS endpoints

	// DefaultKey is the key used when no explicit key ID
	// is specified. It is empty if the KMS does not support
	// a default key.
	DefaultKey string
}

// DEK is a data encryption key. It consists of a
// plaintext-ciphertext pair and the ID of the key
// used to generate the ciphertext.
//
// The plaintext can be used for cryptographic
// operations - like encrypting some data. The
// ciphertext is the encrypted version of the
// plaintext data and can be stored on untrusted
// storage.
type DEK struct {
	KeyID      string
	Plaintext  []byte
	Ciphertext []byte
}

// PolicyInfo describes a KMS policy.
type PolicyInfo struct {
	Name      string
	CreatedAt time.Time
	CreatedBy string
}

// Identity describes a KMS identity.
type Identity struct {
	Identity  string
	IsAdmin   bool
	Policy    string
	CreatedAt time.Time
	CreatedBy string
}

type SelfIdentity struct {
	Identity *Identity
	Policy   *kes.Policy
}

// KMSKey describes a cryptographic key.
type KMSKey struct {
	Name      string
	CreatedAt time.Time
	CreatedBy string
}

var (
	_ encoding.TextMarshaler   = (*DEK)(nil)
	_ encoding.TextUnmarshaler = (*DEK)(nil)
)

// MarshalText encodes the DEK's key ID and ciphertext
// as JSON.
func (d DEK) MarshalText() ([]byte, error) {
	type JSON struct {
		KeyID      string `json:"keyid"`
		Ciphertext []byte `json:"ciphertext"`
	}
	return json.Marshal(JSON{
		KeyID:      d.KeyID,
		Ciphertext: d.Ciphertext,
	})
}

// UnmarshalText tries to decode text as JSON representation
// of a DEK and sets DEK's key ID and ciphertext to the
// decoded values.
//
// It sets DEK's plaintext to nil.
func (d *DEK) UnmarshalText(text []byte) error {
	type JSON struct {
		KeyID      string `json:"keyid"`
		Ciphertext []byte `json:"ciphertext"`
	}
	var v JSON
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(text, &v); err != nil {
		return err
	}
	d.KeyID, d.Plaintext, d.Ciphertext = v.KeyID, nil, v.Ciphertext
	return nil
}
