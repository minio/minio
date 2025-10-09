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

package config

import (
	"bytes"
	"encoding/hex"
	"io"
	"testing"

	"github.com/minio/minio/internal/kms"
)

var encryptDecryptTests = []struct {
	Data    []byte
	Context kms.Context
}{
	{
		Data:    nil,
		Context: nil,
	},
	{
		Data:    []byte{1},
		Context: nil,
	},
	{
		Data:    []byte{1},
		Context: kms.Context{"key": "value"},
	},
	{
		Data:    make([]byte, 1<<20),
		Context: kms.Context{"key": "value", "a": "b"},
	},
}

func TestEncryptDecrypt(t *testing.T) {
	key, err := hex.DecodeString("ddedadb867afa3f73bd33c25499a723ed7f9f51172ee7b1b679e08dc795debcc")
	if err != nil {
		t.Fatalf("Failed to decode master key: %v", err)
	}
	KMS, err := kms.NewBuiltin("my-key", key)
	if err != nil {
		t.Fatalf("Failed to create KMS: %v", err)
	}

	for i, test := range encryptDecryptTests {
		ciphertext, err := Encrypt(KMS, bytes.NewReader(test.Data), test.Context)
		if err != nil {
			t.Fatalf("Test %d: failed to encrypt stream: %v", i, err)
		}
		data, err := io.ReadAll(ciphertext)
		if err != nil {
			t.Fatalf("Test %d: failed to encrypt stream: %v", i, err)
		}

		plaintext, err := Decrypt(KMS, bytes.NewReader(data), test.Context)
		if err != nil {
			t.Fatalf("Test %d: failed to decrypt stream: %v", i, err)
		}
		data, err = io.ReadAll(plaintext)
		if err != nil {
			t.Fatalf("Test %d: failed to decrypt stream: %v", i, err)
		}

		if !bytes.Equal(data, test.Data) {
			t.Fatalf("Test %d: decrypted data does not match original data", i)
		}
	}
}

func BenchmarkEncrypt(b *testing.B) {
	key, err := hex.DecodeString("ddedadb867afa3f73bd33c25499a723ed7f9f51172ee7b1b679e08dc795debcc")
	if err != nil {
		b.Fatalf("Failed to decode master key: %v", err)
	}
	KMS, err := kms.NewBuiltin("my-key", key)
	if err != nil {
		b.Fatalf("Failed to create KMS: %v", err)
	}

	benchmarkEncrypt := func(size int, b *testing.B) {
		var (
			data      = make([]byte, size)
			plaintext = bytes.NewReader(data)
			context   = kms.Context{"key": "value"}
		)
		b.SetBytes(int64(size))
		for b.Loop() {
			ciphertext, err := Encrypt(KMS, plaintext, context)
			if err != nil {
				b.Fatal(err)
			}
			if _, err = io.Copy(io.Discard, ciphertext); err != nil {
				b.Fatal(err)
			}
			plaintext.Reset(data)
		}
	}
	b.Run("1KB", func(b *testing.B) { benchmarkEncrypt(1*1024, b) })
	b.Run("512KB", func(b *testing.B) { benchmarkEncrypt(512*1024, b) })
	b.Run("1MB", func(b *testing.B) { benchmarkEncrypt(1024*1024, b) })
	b.Run("10MB", func(b *testing.B) { benchmarkEncrypt(10*1024*1024, b) })
}
