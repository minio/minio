// MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/minio/minio/cmd/logger"
	sha256 "github.com/minio/sha256-simd"
	"github.com/minio/sio"
)

// Context is a list of key-value pairs cryptographically
// associated with a certain object.
type Context map[string]string

// WriteTo writes the context in a canonical from to w.
// It returns the number of bytes and the first error
// encounter during writing to w, if any.
//
// WriteTo sorts the context keys and writes the sorted
// key-value pairs as canonical JSON object to w.
func (c Context) WriteTo(w io.Writer) (n int64, err error) {
	sortedKeys := make(sort.StringSlice, 0, len(c))
	for k := range c {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Sort(sortedKeys)

	nn, err := io.WriteString(w, "{")
	if err != nil {
		return n + int64(nn), err
	}
	n += int64(nn)
	for i, k := range sortedKeys {
		s := fmt.Sprintf("\"%s\":\"%s\",", k, c[k])
		if i == len(sortedKeys)-1 {
			s = s[:len(s)-1] // remove last ','
		}

		nn, err = io.WriteString(w, s)
		if err != nil {
			return n + int64(nn), err
		}
		n += int64(nn)
	}
	nn, err = io.WriteString(w, "}")
	return n + int64(nn), err
}

// KMS represents an active and authenticted connection
// to a Key-Management-Service. It supports generating
// data key generation and unsealing of KMS-generated
// data keys.
type KMS interface {
	// KeyID - returns configured KMS key id.
	KeyID() string

	// GenerateKey generates a new random data key using
	// the master key referenced by the keyID. It returns
	// the plaintext key and the sealed plaintext key
	// on success.
	//
	// The context is cryptographically bound to the
	// generated key. The same context must be provided
	// again to unseal the generated key.
	GenerateKey(keyID string, context Context) (key [32]byte, sealedKey []byte, err error)

	// UnsealKey unseals the sealedKey using the master key
	// referenced by the keyID. The provided context must
	// match the context used to generate the sealed key.
	UnsealKey(keyID string, sealedKey []byte, context Context) (key [32]byte, err error)

	// UpdateKey re-wraps the sealedKey if the master key, referenced by
	// `keyID`, has changed in the meantime. This usually happens when the
	// KMS operator performs a key-rotation operation of the master key.
	// UpdateKey fails if the provided sealedKey cannot be decrypted using
	// the master key referenced by keyID.
	//
	// UpdateKey makes no guarantees whatsoever about whether the returned
	// rotatedKey is actually different from the sealedKey. If nothing has
	// changed at the KMS or if the KMS does not support updating generated
	// keys this method may behave like a NOP and just return the sealedKey
	// itself.
	UpdateKey(keyID string, sealedKey []byte, context Context) (rotatedKey []byte, err error)

	// Returns KMSInfo
	Info() (kmsInfo KMSInfo)
}

type masterKeyKMS struct {
	keyID     string
	masterKey [32]byte
}

// KMSInfo stores the details of KMS
type KMSInfo struct {
	Endpoint string
	Name     string
	AuthType string
}

// NewMasterKey returns a basic KMS implementation from a single 256 bit master key.
//
// The KMS accepts any keyID but binds the keyID and context cryptographically
// to the generated keys.
func NewMasterKey(keyID string, key [32]byte) KMS { return &masterKeyKMS{keyID: keyID, masterKey: key} }

func (kms *masterKeyKMS) KeyID() string {
	return kms.keyID
}

func (kms *masterKeyKMS) GenerateKey(keyID string, ctx Context) (key [32]byte, sealedKey []byte, err error) {
	if _, err = io.ReadFull(rand.Reader, key[:]); err != nil {
		logger.CriticalIf(context.Background(), errOutOfEntropy)
	}

	var (
		buffer     bytes.Buffer
		derivedKey = kms.deriveKey(keyID, ctx)
	)
	if n, err := sio.Encrypt(&buffer, bytes.NewReader(key[:]), sio.Config{Key: derivedKey[:]}); err != nil || n != 64 {
		logger.CriticalIf(context.Background(), errors.New("KMS: unable to encrypt data key"))
	}
	sealedKey = buffer.Bytes()
	return key, sealedKey, nil
}

// KMS is configured directly using master key
func (kms *masterKeyKMS) Info() (info KMSInfo) {
	return KMSInfo{
		Endpoint: "",
		Name:     "",
		AuthType: "master-key",
	}
}

func (kms *masterKeyKMS) UnsealKey(keyID string, sealedKey []byte, ctx Context) (key [32]byte, err error) {
	var (
		buffer     bytes.Buffer
		derivedKey = kms.deriveKey(keyID, ctx)
	)
	if n, err := sio.Decrypt(&buffer, bytes.NewReader(sealedKey), sio.Config{Key: derivedKey[:]}); err != nil || n != 32 {
		return key, err // TODO(aead): upgrade sio to use sio.Error
	}
	copy(key[:], buffer.Bytes())
	return key, nil
}

func (kms *masterKeyKMS) UpdateKey(keyID string, sealedKey []byte, ctx Context) ([]byte, error) {
	if _, err := kms.UnsealKey(keyID, sealedKey, ctx); err != nil {
		return nil, err
	}
	return sealedKey, nil // The master key cannot update data keys -> Do nothing.
}

func (kms *masterKeyKMS) deriveKey(keyID string, context Context) (key [32]byte) {
	if context == nil {
		context = Context{}
	}
	mac := hmac.New(sha256.New, kms.masterKey[:])
	mac.Write([]byte(keyID))
	context.WriteTo(mac)
	mac.Sum(key[:0])
	return key
}
