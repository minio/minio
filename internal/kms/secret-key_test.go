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
	"encoding/base64"
	"testing"
)

func TestSingleKeyRoundtrip(t *testing.T) {
	KMS, err := ParseSecretKey("my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=")
	if err != nil {
		t.Fatalf("Failed to initialize KMS: %v", err)
	}

	key, err := KMS.GenerateKey(t.Context(), &GenerateKeyRequest{Name: "my-key"})
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	plaintext, err := KMS.Decrypt(t.Context(), &DecryptRequest{
		Name:       key.KeyID,
		Ciphertext: key.Ciphertext,
	})
	if err != nil {
		t.Fatalf("Failed to decrypt key: %v", err)
	}
	if !bytes.Equal(key.Plaintext, plaintext) {
		t.Fatalf("Decrypted key does not match generated one: got %x - want %x", key.Plaintext, plaintext)
	}
}

func TestDecryptKey(t *testing.T) {
	KMS, err := ParseSecretKey("my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=")
	if err != nil {
		t.Fatalf("Failed to initialize KMS: %v", err)
	}

	for i, test := range decryptKeyTests {
		dataKey, err := base64.StdEncoding.DecodeString(test.Plaintext)
		if err != nil {
			t.Fatalf("Test %d: failed to decode plaintext key: %v", i, err)
		}
		plaintext, err := KMS.Decrypt(t.Context(), &DecryptRequest{
			Name:           test.KeyID,
			Ciphertext:     []byte(test.Ciphertext),
			AssociatedData: test.Context,
		})
		if err != nil {
			t.Fatalf("Test %d: failed to decrypt key: %v", i, err)
		}
		if !bytes.Equal(plaintext, dataKey) {
			t.Fatalf("Test %d: decrypted key does not generated one: got %x - want %x", i, plaintext, dataKey)
		}
	}
}

var decryptKeyTests = []struct {
	KeyID      string
	Plaintext  string
	Ciphertext string
	Context    Context
}{
	{
		KeyID:      "my-key",
		Plaintext:  "zmS7NrG765UZ0ZN85oPjybelxqVvpz01vxsSpOISy2M=",
		Ciphertext: `{"aead":"ChaCha20Poly1305","iv":"JbI+vwvYww1lCb5VpkAFuQ==","nonce":"ARjIjJxBSD541Gz8","bytes":"KCbEc2sA0TLvA7aWTWa23AdccVfJMpOxwgG8hm+4PaNrxYfy1xFWZg2gEenVrOgv"}`,
	},
	{
		KeyID:      "my-key",
		Plaintext:  "UnPWsZgVI+T4L9WGNzFlP1PsP1Z6hn2Fx8ISeZfDGnA=",
		Ciphertext: `{"aead":"ChaCha20Poly1305","iv":"r4+yfiVbVIYR0Z2I9Fq+6g==","nonce":"2YpwGwE59GcVraI3","bytes":"k/svMglOU7/Kgwv73heG38NWW575XLcFp3SaxQHDMjJGYyRI3Fiygu2OeutGPXNL"}`,
		Context:    Context{"key": "value"},
	},
}
