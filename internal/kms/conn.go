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
	"strconv"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go/v3"
)

// conn represents a connection to a KMS implementation.
// It's implemented by the MinKMS and KES client wrappers
// and the static / single key KMS.
type conn interface {
	// Version returns version information about the KMS.
	//
	// TODO(aead): refactor this API call. It does not account
	// for multiple endpoints.
	Version(context.Context) (string, error)

	// APIs returns a list of APIs supported by the KMS server.
	//
	// TODO(aead): remove this API call. It's hardly useful.
	APIs(context.Context) ([]madmin.KMSAPI, error)

	// Stat returns the current KMS status.
	Status(context.Context) (map[string]madmin.ItemState, error)

	// CreateKey creates a new key at the KMS with the given key ID.
	CreateKey(context.Context, *CreateKeyRequest) error

	ListKeys(context.Context, *ListRequest) ([]madmin.KMSKeyInfo, string, error)

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
	GenerateKey(context.Context, *GenerateKeyRequest) (DEK, error)

	// DecryptKey decrypts the ciphertext with the key referenced
	// by the key ID. The context must match the context value
	// used to generate the ciphertext.
	Decrypt(context.Context, *DecryptRequest) ([]byte, error)

	// MAC generates the checksum of the given req.Message using the key
	// with the req.Name at the KMS.
	MAC(context.Context, *MACRequest) ([]byte, error)
}

var ( // compiler checks
	_ conn = (*kmsConn)(nil)
	_ conn = (*kesConn)(nil)
	_ conn = secretKey{}
)

// Supported KMS types
const (
	MinKMS  Type = iota + 1 // MinIO KMS
	MinKES                  // MinIO MinKES
	Builtin                 // Builtin single key KMS implementation
)

// Type identifies the KMS type.
type Type uint

// String returns the Type's string representation
func (t Type) String() string {
	switch t {
	case MinKMS:
		return "MinIO KMS"
	case MinKES:
		return "MinIO KES"
	case Builtin:
		return "MinIO builtin"
	default:
		return "!INVALID:" + strconv.Itoa(int(t))
	}
}

// Status describes the current state of a KMS.
type Status struct {
	Online  map[string]struct{}
	Offline map[string]Error
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
	KeyID      string // Name of the master key
	Version    int    // Version of the master key (MinKMS only)
	Plaintext  []byte // Paintext of the data encryption key
	Ciphertext []byte // Ciphertext of the data encryption key
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
		Version    uint32 `json:"version,omitempty"`
		Ciphertext []byte `json:"ciphertext"`
	}
	return json.Marshal(JSON{
		KeyID:      d.KeyID,
		Version:    uint32(d.Version),
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
		Version    uint32 `json:"version"`
		Ciphertext []byte `json:"ciphertext"`
	}
	var v JSON
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(text, &v); err != nil {
		return err
	}
	d.KeyID, d.Version, d.Plaintext, d.Ciphertext = v.KeyID, int(v.Version), nil, v.Ciphertext
	return nil
}
