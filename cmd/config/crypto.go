// MinIO Cloud Storage, (C) 2021 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio/pkg/kms"
	"github.com/secure-io/sio-go"
	"github.com/secure-io/sio-go/sioutil"
)

// EncryptBytes encrypts the plaintext with a key managed by KMS.
// The context is bound to the returned ciphertext.
//
// The same context must be provided when decrypting the
// ciphertext.
func EncryptBytes(KMS kms.KMS, plaintext []byte, context kms.Context) ([]byte, error) {
	ciphertext, err := Encrypt(KMS, bytes.NewReader(plaintext), context)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(ciphertext)
}

// DecryptBytes decrypts the ciphertext using a key managed by the KMS.
// The same context that have been used during encryption must be
// provided.
func DecryptBytes(KMS kms.KMS, ciphertext []byte, context kms.Context) ([]byte, error) {
	plaintext, err := Decrypt(KMS, bytes.NewReader(ciphertext), context)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(plaintext)
}

// Encrypt encrypts the plaintext with a key managed by KMS.
// The context is bound to the returned ciphertext.
//
// The same context must be provided when decrypting the
// ciphertext.
func Encrypt(KMS kms.KMS, plaintext io.Reader, context kms.Context) (io.Reader, error) {
	var algorithm = sio.AES_256_GCM
	if !sioutil.NativeAES() {
		algorithm = sio.ChaCha20Poly1305
	}

	key, err := KMS.GenerateKey("", context)
	if err != nil {
		return nil, err
	}
	stream, err := algorithm.Stream(key.Plaintext)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, stream.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	const (
		MaxMetadataSize = 1 << 20 // max. size of the metadata
		Version         = 1
	)
	var (
		header [5]byte
		buffer bytes.Buffer
	)
	metadata, err := json.Marshal(encryptedObject{
		KeyID:     key.KeyID,
		KMSKey:    key.Ciphertext,
		Algorithm: algorithm,
		Nonce:     nonce,
	})
	if err != nil {
		return nil, err
	}
	if len(metadata) > MaxMetadataSize {
		return nil, errors.New("config: encryption metadata is too large")
	}
	header[0] = Version
	binary.LittleEndian.PutUint32(header[1:], uint32(len(metadata)))
	buffer.Write(header[:])
	buffer.Write(metadata)

	return io.MultiReader(
		&buffer,
		stream.EncryptReader(plaintext, nonce, nil),
	), nil
}

// Decrypt decrypts the ciphertext using a key managed by the KMS.
// The same context that have been used during encryption must be
// provided.
func Decrypt(KMS kms.KMS, ciphertext io.Reader, context kms.Context) (io.Reader, error) {
	const (
		MaxMetadataSize = 1 << 20 // max. size of the metadata
		Version         = 1
	)

	var header [5]byte
	if _, err := io.ReadFull(ciphertext, header[:]); err != nil {
		return nil, err
	}
	if header[0] != Version {
		return nil, fmt.Errorf("config: unknown ciphertext version %d", header[0])
	}
	size := binary.LittleEndian.Uint32(header[1:])
	if size > MaxMetadataSize {
		return nil, errors.New("config: encryption metadata is too large")
	}

	var (
		metadataBuffer = make([]byte, size)
		metadata       encryptedObject
	)
	if _, err := io.ReadFull(ciphertext, metadataBuffer); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(metadataBuffer, &metadata); err != nil {
		return nil, err
	}

	key, err := KMS.DecryptKey(metadata.KeyID, metadata.KMSKey, context)
	if err != nil {
		return nil, err
	}
	stream, err := metadata.Algorithm.Stream(key)
	if err != nil {
		return nil, err
	}
	if stream.NonceSize() != len(metadata.Nonce) {
		return nil, sio.NotAuthentic
	}
	return stream.DecryptReader(ciphertext, metadata.Nonce, nil), nil
}

type encryptedObject struct {
	KeyID  string `json:"keyid"`
	KMSKey []byte `json:"kmskey"`

	Algorithm sio.Algorithm `json:"algorithm"`
	Nonce     []byte        `json:"nonce"`
}
