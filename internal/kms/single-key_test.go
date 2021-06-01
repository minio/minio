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
	KMS, err := Parse("my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=")
	if err != nil {
		t.Fatalf("Failed to initialize KMS: %v", err)
	}

	key, err := KMS.GenerateKey("my-key", Context{})
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	plaintext, err := KMS.DecryptKey(key.KeyID, key.Ciphertext, Context{})
	if err != nil {
		t.Fatalf("Failed to decrypt key: %v", err)
	}
	if !bytes.Equal(key.Plaintext, plaintext) {
		t.Fatalf("Decrypted key does not match generated one: got %x - want %x", key.Plaintext, plaintext)
	}
}

func TestDecryptKey(t *testing.T) {
	KMS, err := Parse("my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=")
	if err != nil {
		t.Fatalf("Failed to initialize KMS: %v", err)
	}

	for i, test := range decryptKeyTests {
		dataKey, err := base64.StdEncoding.DecodeString(test.Plaintext)
		if err != nil {
			t.Fatalf("Test %d: failed to decode plaintext key: %v", i, err)
		}
		ciphertext, err := base64.StdEncoding.DecodeString(test.Ciphertext)
		if err != nil {
			t.Fatalf("Test %d: failed to decode ciphertext key: %v", i, err)
		}
		plaintext, err := KMS.DecryptKey(test.KeyID, ciphertext, test.Context)
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
		Ciphertext: "eyJhZWFkIjoiQ2hhQ2hhMjBQb2x5MTMwNSIsIml2IjoiSmJJK3Z3dll3dzFsQ2I1VnBrQUZ1UT09Iiwibm9uY2UiOiJBUmpJakp4QlNENTQxR3o4IiwiYnl0ZXMiOiJLQ2JFYzJzQTBUTHZBN2FXVFdhMjNBZGNjVmZKTXBPeHdnRzhobSs0UGFOcnhZZnkxeEZXWmcyZ0VlblZyT2d2In0=",
	},
	{
		KeyID:      "my-key",
		Plaintext:  "UnPWsZgVI+T4L9WGNzFlP1PsP1Z6hn2Fx8ISeZfDGnA=",
		Ciphertext: "eyJhZWFkIjoiQ2hhQ2hhMjBQb2x5MTMwNSIsIml2IjoicjQreWZpVmJWSVlSMFoySTlGcSs2Zz09Iiwibm9uY2UiOiIyWXB3R3dFNTlHY1ZyYUkzIiwiYnl0ZXMiOiJrL3N2TWdsT1U3L0tnd3Y3M2hlRzM4TldXNTc1WExjRnAzU2F4UUhETWpKR1l5UkkzRml5Z3UyT2V1dEdQWE5MIn0=",
		Context:    Context{"key": "value"},
	},
}
