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
	"crypto/sha256"
	"errors"
	"io"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/kms"
	"github.com/minio/sio"
)

// Context is a list of key-value pairs cryptographically
// associated with a certain object.
type Context = kms.Context

// KMS represents an active and authenticted connection
// to a Key-Management-Service. It supports generating
// data key generation and unsealing of KMS-generated
// data keys.
type KMS = kms.KMS

type masterKeyKMS struct {
	keyID     string
	masterKey [32]byte
}

// NewMasterKey returns a basic KMS implementation from a single 256 bit master key.
//
// The KMS accepts any keyID but binds the keyID and context cryptographically
// to the generated keys.
func NewMasterKey(keyID string, key [32]byte) KMS { return &masterKeyKMS{keyID: keyID, masterKey: key} }

func (m *masterKeyKMS) Stat() (kms.Status, error) {
	return kms.Status{
		Name:       "MasterKey",
		DefaultKey: m.keyID,
	}, nil
}

func (m *masterKeyKMS) CreateKey(keyID string) error {
	return errors.New("crypto: creating keys is not supported by a static master key")
}

func (m *masterKeyKMS) GenerateKey(keyID string, ctx Context) (kms.DEK, error) {
	if keyID == "" {
		keyID = m.keyID
	}

	var key [32]byte
	if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
		logger.CriticalIf(context.Background(), errOutOfEntropy)
	}

	var (
		buffer     bytes.Buffer
		derivedKey = m.deriveKey(keyID, ctx)
	)
	if n, err := sio.Encrypt(&buffer, bytes.NewReader(key[:]), sio.Config{Key: derivedKey[:]}); err != nil || n != 64 {
		logger.CriticalIf(context.Background(), errors.New("KMS: unable to encrypt data key"))
	}
	return kms.DEK{
		KeyID:      m.keyID,
		Plaintext:  key[:],
		Ciphertext: buffer.Bytes(),
	}, nil
}

func (m *masterKeyKMS) DecryptKey(keyID string, sealedKey []byte, ctx Context) ([]byte, error) {
	var derivedKey = m.deriveKey(keyID, ctx)

	var key [32]byte
	out, err := sio.DecryptBuffer(key[:0], sealedKey, sio.Config{Key: derivedKey[:]})
	if err != nil || len(out) != 32 {
		return nil, err // TODO(aead): upgrade sio to use sio.Error
	}
	return key[:], nil
}

func (m *masterKeyKMS) deriveKey(keyID string, context Context) (key [32]byte) {
	if context == nil {
		context = Context{}
	}
	ctxBytes, _ := context.MarshalText()

	mac := hmac.New(sha256.New, m.masterKey[:])
	mac.Write([]byte(keyID))
	mac.Write(ctxBytes)
	mac.Sum(key[:0])
	return key
}
