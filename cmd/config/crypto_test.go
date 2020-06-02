/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config

import (
	"bytes"
	"encoding/hex"
	"testing"
)

var backendIsCompleteTests = []struct {
	Backend    Backend
	IsComplete bool
}{
	{Backend: Backend{Status: ""}, IsComplete: false},                       // 0
	{Backend: Backend{Status: BackendEncryptionPartial}, IsComplete: false}, // 1
	{Backend: Backend{Status: BackendEncryptionMissing}, IsComplete: false}, // 2
	{Backend: Backend{Status: BackendEncryptionComplete}, IsComplete: true}, // 3
}

func TestBackendIsComplete(t *testing.T) {
	for i, test := range backendIsCompleteTests {
		if isComplete := test.Backend.IsComplete(); isComplete != test.IsComplete {
			t.Fatalf("Test %d: got '%v' - want '%v'", i, isComplete, test.IsComplete)
		}
	}
}

var backendIsEncryptedTests = []struct {
	Backend     Backend
	IsEncrypted bool
}{
	{Backend: Backend{Status: ""}, IsEncrypted: false},                       // 0
	{Backend: Backend{Status: BackendEncryptionMissing}, IsEncrypted: false}, // 2
	{Backend: Backend{Status: BackendEncryptionPartial}, IsEncrypted: true},  // 1
	{Backend: Backend{Status: BackendEncryptionComplete}, IsEncrypted: true}, // 3
}

func TestBackendIsEncrypted(t *testing.T) {
	for i, test := range backendIsEncryptedTests {
		if isEncrypted := test.Backend.IsEncrypted(); isEncrypted != test.IsEncrypted {
			t.Fatalf("Test %d: got '%v' - want '%v'", i, isEncrypted, test.IsEncrypted)
		}
	}
}

var backendMarshalBinaryTests = []struct {
	Backend Backend
	Binary  string
}{
	{
		Backend: Backend{},
		Binary:  `{"status":"","derivation_params":null}`},
	{
		Backend: Backend{Status: BackendEncryptionMissing},
		Binary:  `{"status":"plaintext","derivation_params":null}`,
	},
	{
		Backend: Backend{
			Status: BackendEncryptionPartial,
			DerivationParams: &KeyDerivationParams{
				Algorithm:  "Argon2id",
				Salt:       make([]byte, 16),
				TimeCost:   1,
				MemoryCost: 64 * 1024,
				Threads:    4,
			},
		},
		Binary: `{"status":"incomplete","derivation_params":{"algorithm":"Argon2id","salt":"AAAAAAAAAAAAAAAAAAAAAA==","time":1,"memory":65536,"threads":4}}`,
	},
	{
		Backend: Backend{
			Status: BackendEncryptionComplete,
			DerivationParams: &KeyDerivationParams{
				Algorithm:  "Argon2id",
				Salt:       make([]byte, 16),
				TimeCost:   4,
				MemoryCost: 64 * 1024,
				Threads:    4,
			},
		},
		Binary: `{"status":"encrypted","derivation_params":{"algorithm":"Argon2id","salt":"AAAAAAAAAAAAAAAAAAAAAA==","time":4,"memory":65536,"threads":4}}`,
	},
}

func TestBackendMarshalBinary(t *testing.T) {
	for i, test := range backendMarshalBinaryTests {
		b, err := test.Backend.MarshalBinary()
		if err != nil {
			t.Fatalf("Test %d: binary marshaling failed: %v", i, err)
		}
		if binary := string(b); binary != test.Binary {
			t.Fatalf("Test %d: got '%v' - want '%v'", i, binary, test.Binary)
		}
	}
}

var backendUnmarshalBinaryTests = []struct {
	Binary  string
	Backend Backend
}{
	{
		Binary: `{"status":"incomplete","derivation_params":{"algorithm":"Argon2id","salt":"AAAAAAAAAAAAAAAAAAAAAA==","time":1,"memory":65536,"threads":4}}`,
		Backend: Backend{
			Status: BackendEncryptionPartial,
			DerivationParams: &KeyDerivationParams{
				Algorithm:  "Argon2id",
				Salt:       make([]byte, 16),
				TimeCost:   1,
				MemoryCost: 64 * 1024,
				Threads:    4,
			},
		},
	},
	{
		Binary: `{"status":"encrypted","derivation_params":{"algorithm":"Argon2id","salt":"AAAAAAAAAAAAAAAAAAAAAA==","time":4,"memory":65536,"threads":4}}`,
		Backend: Backend{
			Status: BackendEncryptionComplete,
			DerivationParams: &KeyDerivationParams{
				Algorithm:  "Argon2id",
				Salt:       make([]byte, 16),
				TimeCost:   4,
				MemoryCost: 64 * 1024,
				Threads:    4,
			},
		},
	},
	{
		Binary: `{"status":"plaintext","derivation_params":{"algorithm":"Argon2id","salt":"AAAAAAAAAAAAAAAAAAAAAA==","time":2,"memory":32768,"threads":4}}`,
		Backend: Backend{
			Status: BackendEncryptionMissing,
			DerivationParams: &KeyDerivationParams{
				Algorithm:  "Argon2id",
				Salt:       make([]byte, 16),
				TimeCost:   2,
				MemoryCost: 32 * 1024,
				Threads:    4,
			},
		},
	},
}

func TestBackendUnmarshalBinary(t *testing.T) {
	for i, test := range backendUnmarshalBinaryTests {
		var backend Backend
		if err := backend.UnmarshalBinary([]byte(test.Binary)); err != nil {
			t.Fatalf("Test %d: binary unmarshaling failed: %v", i, err)
		}
		if backend.Status != test.Backend.Status {
			t.Fatalf("Test %d: status mismatch: got '%v' - want '%v'", i, backend.Status, test.Backend.Status)
		}
		if backend.DerivationParams == nil || test.Backend.DerivationParams == nil {
			if backend.DerivationParams != test.Backend.DerivationParams {
				t.Fatalf("Test %d: derivation parameter mismatch: got '%v' - want '%v'", i, backend.DerivationParams, test.Backend.DerivationParams)
			}
		}
		if backend.DerivationParams != nil && test.Backend.DerivationParams != nil {
			if backend.DerivationParams.Algorithm != test.Backend.DerivationParams.Algorithm {
				t.Fatalf("Test %d: derivation algorithm mismatch: got '%v' - want '%v'", i, backend.DerivationParams.Algorithm, test.Backend.DerivationParams.Algorithm)
			}
			if !bytes.Equal(backend.DerivationParams.Salt, test.Backend.DerivationParams.Salt) {
				t.Fatalf("Test %d: derivation salt mismatch: got '%x' - want '%x'", i, backend.DerivationParams.Salt, test.Backend.DerivationParams.Salt)
			}
			if backend.DerivationParams.TimeCost != test.Backend.DerivationParams.TimeCost {
				t.Fatalf("Test %d: derivation time mismatch: got '%d' - want '%d'", i, backend.DerivationParams.TimeCost, test.Backend.DerivationParams.TimeCost)
			}
			if backend.DerivationParams.MemoryCost != test.Backend.DerivationParams.MemoryCost {
				t.Fatalf("Test %d: derivation memory mismatch: got '%d' - want '%d'", i, backend.DerivationParams.MemoryCost, test.Backend.DerivationParams.MemoryCost)
			}
			if backend.DerivationParams.Threads != test.Backend.DerivationParams.Threads {
				t.Fatalf("Test %d: derivation threads mismatch: got '%d' - want '%d'", i, backend.DerivationParams.Threads, test.Backend.DerivationParams.Threads)
			}
		}
	}
}

var deriveMasterKeyTests = []struct {
	Password         string
	DerivationParams *KeyDerivationParams

	MasterKey  MasterKey
	ShouldFail bool
}{
	{
		Password:         "minio123",
		DerivationParams: DefaultKeyDerivationParams(make([]byte, 16)),
		MasterKey:        decodeHex("415593908f292aed743d1eeebd1a5d84fe5976ead2ed65122097b2c39b37be5c"),
	},
	{
		Password:         "minio1234",
		DerivationParams: DefaultKeyDerivationParams(make([]byte, 16)),
		MasterKey:        decodeHex("4074879a268ab77295354cedf4ee2020eaf8a29a5a25273cf51e723eee27dc07"),
	},
	{
		Password: "minio123",
		DerivationParams: &KeyDerivationParams{
			Algorithm:  "Argon2id",
			Salt:       make([]byte, 16),
			TimeCost:   1,
			MemoryCost: 320 * 1024, // Memory cost too high
			Threads:    4,
		},
		ShouldFail: true,
	},
	{
		Password: "minio123",
		DerivationParams: &KeyDerivationParams{
			Algorithm:  "Argon2id",
			Salt:       make([]byte, 32),
			TimeCost:   12, // Time cost too high
			MemoryCost: 64 * 1024,
			Threads:    1,
		},
		ShouldFail: true,
	},
}

func TestDeriveMasterKey(t *testing.T) {
	for i, test := range deriveMasterKeyTests {
		key, err := DeriveMasterKey(test.Password, *test.DerivationParams)
		if test.ShouldFail && err == nil {
			t.Fatalf("Test %d: is expected to fail but it passed", i)
		}
		if !test.ShouldFail && err != nil {
			t.Fatalf("Test %d: is expected to pass but it failed: %v", i, err)
		}
		if err == nil {
			if !bytes.Equal(key, test.MasterKey) {
				t.Fatalf("Test %d: master key mismatch: got '%x' - want '%x'", i, key, test.MasterKey)
			}
		}
	}
}

var masterKeyDecryptBytesTests = []struct {
	MasterKey  MasterKey
	Ciphertext []byte
	Plaintext  []byte
}{
	{ // 0
		MasterKey:  decodeHex("78544e75116f665fb6b8167890154a2167669b32556522f2c68f7102fea2dd8f"),
		Ciphertext: decodeHex("0100f674e1e9e1e984fa8462462876315c42d350cf7c9f573d6e86160ece321d13c5305d231e4315288e"),
		Plaintext:  []byte(""),
	},
	{ // 1
		MasterKey:  decodeHex("78544e75116f665fb6b8167890154a2167669b32556522f2c68f7102fea2dd8f"),
		Ciphertext: decodeHex("0100f570a001213be9e97df3d35cac8ab78d7b05a3303757fab63275bdb130cca8c2dccffb0b1f16561df65d"),
		Plaintext:  []byte("{}"),
	},
	{ // 2
		MasterKey:  decodeHex("cd81bcb55a8420c60e5e297da72a747cc6928d7e78cebe56e88197c2efe58505"),
		Ciphertext: decodeHex("0100fd11881757b0d41cc68e454140dd22711fe45f4679b19e61f0ae8d1351df4a6ced29f50e1611f5581189de0b6f3a5fba3e60798cb36116eeee"),
		Plaintext:  []byte(`{"hello":"world"}`),
	},
}

func TestMasterKeyDecryptBytes(t *testing.T) {
	for i, test := range masterKeyDecryptBytesTests {
		plaintext, err := test.MasterKey.DecryptBytes(test.Ciphertext)
		if err != nil {
			t.Fatalf("Test %d: failed to decrypt ciphertext: %v", i, err)
		}
		if !bytes.Equal(plaintext, test.Plaintext) {
			t.Fatalf("Test %d: plaintext mismatch: got %x - want %x", i, plaintext, test.Plaintext)
		}

		ciphertext, err := test.MasterKey.EncryptBytes(plaintext)
		if err != nil {
			t.Fatalf("Test %d: failed to encrypt plaintext: %v", i, err)
		}
		plaintext, err = test.MasterKey.DecryptBytes(ciphertext)
		if err != nil {
			t.Fatalf("Test %d: failed to decrypt re-encrypted ciphertext: %v", i, err)
		}
		if !bytes.Equal(plaintext, test.Plaintext) {
			t.Fatalf("Test %d: plaintext mismatch: got %x - want %x", i, plaintext, test.Plaintext)
		}
	}
}

func decodeHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}
