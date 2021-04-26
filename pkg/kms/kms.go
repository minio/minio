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
	"encoding"
	"encoding/json"

	jsoniter "github.com/json-iterator/go"
)

// KMS is the generic interface that abstracts over
// different KMS implementations.
type KMS interface {
	// Stat returns the current KMS status.
	Stat() (Status, error)

	// CreateKey creates a new key at the KMS with the given key ID.
	CreateKey(keyID string) error

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
	GenerateKey(keyID string, context Context) (DEK, error)

	// DecryptKey decrypts the ciphertext with the key referenced
	// by the key ID. The context must match the context value
	// used to generate the ciphertext.
	DecryptKey(keyID string, ciphertext []byte, context Context) ([]byte, error)
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
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(text, &v); err != nil {
		return err
	}
	d.KeyID, d.Plaintext, d.Ciphertext = v.KeyID, nil, v.Ciphertext
	return nil
}
