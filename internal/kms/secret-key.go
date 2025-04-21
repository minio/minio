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
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/secure-io/sio-go/sioutil"
	"golang.org/x/crypto/chacha20"
	"golang.org/x/crypto/chacha20poly1305"

	"github.com/minio/kms-go/kms"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/hash/sha256"
)

// ParseSecretKey parses s as <key-id>:<base64> and returns a
// KMS that uses s as builtin single key as KMS implementation.
func ParseSecretKey(s string) (*KMS, error) {
	v := strings.SplitN(s, ":", 2)
	if len(v) != 2 {
		return nil, errors.New("kms: invalid secret key format")
	}

	keyID, b64Key := v[0], v[1]
	key, err := base64.StdEncoding.DecodeString(b64Key)
	if err != nil {
		return nil, err
	}
	return NewBuiltin(keyID, key)
}

// NewBuiltin returns a single-key KMS that derives new DEKs from the
// given key.
func NewBuiltin(keyID string, key []byte) (*KMS, error) {
	if len(key) != 32 {
		return nil, errors.New("kms: invalid key length " + strconv.Itoa(len(key)))
	}
	return &KMS{
		Type:       Builtin,
		DefaultKey: keyID,
		conn: secretKey{
			keyID: keyID,
			key:   key,
		},
		latencyBuckets: defaultLatencyBuckets,
		latency:        make([]atomic.Uint64, len(defaultLatencyBuckets)),
	}, nil
}

// secretKey is a KMS implementation that derives new DEKs
// from a single key.
type secretKey struct {
	keyID string
	key   []byte
}

// Version returns the version of the builtin KMS.
func (secretKey) Version(ctx context.Context) (string, error) { return "v1", nil }

// APIs returns an error since the builtin KMS does not provide a list of APIs.
func (secretKey) APIs(ctx context.Context) ([]madmin.KMSAPI, error) {
	return nil, ErrNotSupported
}

// Status returns a set of endpoints and their KMS status. Since, the builtin KMS is not
// external it returns "127.0.0.1: online".
func (secretKey) Status(context.Context) (map[string]madmin.ItemState, error) {
	return map[string]madmin.ItemState{
		"127.0.0.1": madmin.ItemOnline,
	}, nil
}

// ListKeys returns a list of keys with metadata. The builtin KMS consists of just a single key.
func (s secretKey) ListKeys(ctx context.Context, req *ListRequest) ([]madmin.KMSKeyInfo, string, error) {
	if strings.HasPrefix(s.keyID, req.Prefix) && strings.HasPrefix(s.keyID, req.ContinueAt) {
		return []madmin.KMSKeyInfo{{Name: s.keyID}}, "", nil
	}
	return []madmin.KMSKeyInfo{}, "", nil
}

// CreateKey returns ErrKeyExists unless req.Name is equal to the secretKey name.
// The builtin KMS does not support creating multiple keys.
func (s secretKey) CreateKey(_ context.Context, req *CreateKeyRequest) error {
	if req.Name != s.keyID {
		return ErrNotSupported
	}
	return ErrKeyExists
}

// GenerateKey decrypts req.Ciphertext. The key name req.Name must match the key
// name of the secretKey.
//
// The returned DEK is encrypted using AES-GCM and the ciphertext format is compatible
// with KES and MinKMS.
func (s secretKey) GenerateKey(_ context.Context, req *GenerateKeyRequest) (DEK, error) {
	if req.Name != s.keyID {
		return DEK{}, ErrKeyNotFound
	}
	associatedData, err := req.AssociatedData.MarshalText()
	if err != nil {
		return DEK{}, err
	}

	const randSize = 28
	random, err := sioutil.Random(randSize)
	if err != nil {
		return DEK{}, err
	}
	iv, nonce := random[:16], random[16:]

	prf := hmac.New(sha256.New, s.key)
	prf.Write(iv)
	key := prf.Sum(make([]byte, 0, prf.Size()))

	block, err := aes.NewCipher(key)
	if err != nil {
		return DEK{}, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return DEK{}, err
	}

	plaintext, err := sioutil.Random(32)
	if err != nil {
		return DEK{}, err
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, associatedData)
	ciphertext = append(ciphertext, random...)
	return DEK{
		KeyID:      req.Name,
		Version:    0,
		Plaintext:  plaintext,
		Ciphertext: ciphertext,
	}, nil
}

// Decrypt decrypts req.Ciphertext. The key name req.Name must match the key
// name of the secretKey.
//
// Decrypt supports decryption of binary-encoded ciphertexts, as produced by KES
// and MinKMS, and legacy JSON formatted ciphertexts.
func (s secretKey) Decrypt(_ context.Context, req *DecryptRequest) ([]byte, error) {
	if req.Name != s.keyID {
		return nil, ErrKeyNotFound
	}

	const randSize = 28
	ciphertext, keyType := parseCiphertext(req.Ciphertext)
	ciphertext, random := ciphertext[:len(ciphertext)-randSize], ciphertext[len(ciphertext)-randSize:]
	iv, nonce := random[:16], random[16:]

	var aead cipher.AEAD
	switch keyType {
	case kms.AES256:
		mac := hmac.New(sha256.New, s.key)
		mac.Write(iv)
		sealingKey := mac.Sum(nil)

		block, err := aes.NewCipher(sealingKey)
		if err != nil {
			return nil, err
		}
		aead, err = cipher.NewGCM(block)
		if err != nil {
			return nil, err
		}
	case kms.ChaCha20:
		sealingKey, err := chacha20.HChaCha20(s.key, iv)
		if err != nil {
			return nil, err
		}
		aead, err = chacha20poly1305.New(sealingKey)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrDecrypt
	}

	associatedData, _ := req.AssociatedData.MarshalText()
	plaintext, err := aead.Open(nil, nonce, ciphertext, associatedData)
	if err != nil {
		return nil, ErrDecrypt
	}
	return plaintext, nil
}

// MAC generate hmac for the request
func (s secretKey) MAC(_ context.Context, req *MACRequest) ([]byte, error) {
	mac := hmac.New(sha256.New, s.key)
	mac.Write(req.Message)
	return mac.Sum(make([]byte, 0, mac.Size())), nil
}

// parseCiphertext parses and converts a ciphertext into
// the format expected by a secretKey.
//
// Previous implementations of the secretKey produced a structured
// ciphertext. parseCiphertext converts all previously generated
// formats into the expected format.
func parseCiphertext(b []byte) ([]byte, kms.SecretKeyType) {
	if len(b) == 0 {
		return b, kms.AES256
	}

	if b[0] == '{' && b[len(b)-1] == '}' { // JSON object
		var c ciphertext
		if err := c.UnmarshalJSON(b); err != nil {
			// It may happen that a random ciphertext starts with '{' and ends with '}'.
			// In such a case, parsing will fail but we must not return an error. Instead
			// we return the ciphertext as it is.
			return b, kms.AES256
		}

		b = b[:0]
		b = append(b, c.Bytes...)
		b = append(b, c.IV...)
		b = append(b, c.Nonce...)
		return b, c.Algorithm
	}
	return b, kms.AES256
}

// ciphertext is a structure that contains the encrypted
// bytes and all relevant information to decrypt these
// bytes again with a cryptographic key.
type ciphertext struct {
	Algorithm kms.SecretKeyType
	ID        string
	IV        []byte
	Nonce     []byte
	Bytes     []byte
}

// UnmarshalJSON parses the given text as JSON-encoded
// ciphertext.
//
// UnmarshalJSON provides backward-compatible unmarsahaling
// of existing ciphertext. In the past, ciphertexts were
// JSON-encoded. Now, ciphertexts are binary-encoded.
// Therefore, there is no MarshalJSON implementation.
func (c *ciphertext) UnmarshalJSON(text []byte) error {
	const (
		IVSize    = 16
		NonceSize = 12

		AES256GCM        = "AES-256-GCM-HMAC-SHA-256"
		CHACHA20POLY1305 = "ChaCha20Poly1305"
	)

	type JSON struct {
		Algorithm string `json:"aead"`
		ID        string `json:"id"`
		IV        []byte `json:"iv"`
		Nonce     []byte `json:"nonce"`
		Bytes     []byte `json:"bytes"`
	}
	var value JSON
	if err := json.Unmarshal(text, &value); err != nil {
		return ErrDecrypt
	}

	if value.Algorithm != AES256GCM && value.Algorithm != CHACHA20POLY1305 {
		return ErrDecrypt
	}
	if len(value.IV) != IVSize {
		return ErrDecrypt
	}
	if len(value.Nonce) != NonceSize {
		return ErrDecrypt
	}

	switch value.Algorithm {
	case AES256GCM:
		c.Algorithm = kms.AES256
	case CHACHA20POLY1305:
		c.Algorithm = kms.ChaCha20
	default:
		c.Algorithm = 0
	}
	c.ID = value.ID
	c.IV = value.IV
	c.Nonce = value.Nonce
	c.Bytes = value.Bytes
	return nil
}
