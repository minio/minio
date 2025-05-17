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
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/internal/kms"
	"github.com/secure-io/sio-go"
	"github.com/secure-io/sio-go/sioutil"
)

// EncryptBytes encrypts the plaintext with a key managed by KMS.
// The context is bound to the returned ciphertext.
//
// The same context must be provided when decrypting the
// ciphertext.
func EncryptBytes(k *kms.KMS, plaintext []byte, context kms.Context) ([]byte, error) {
	ciphertext, err := Encrypt(k, bytes.NewReader(plaintext), context)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(ciphertext)
}

// DecryptBytes decrypts the ciphertext using a key managed by the KMS.
// The same context that have been used during encryption must be
// provided.
func DecryptBytes(k *kms.KMS, ciphertext []byte, context kms.Context) ([]byte, error) {
	plaintext, err := Decrypt(k, bytes.NewReader(ciphertext), context)
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
func Encrypt(k *kms.KMS, plaintext io.Reader, ctx kms.Context) (io.Reader, error) {
	algorithm := sio.AES_256_GCM
	if !sioutil.NativeAES() {
		algorithm = sio.ChaCha20Poly1305
	}

	key, err := k.GenerateKey(context.Background(), &kms.GenerateKeyRequest{AssociatedData: ctx})
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
	json := jsoniter.ConfigCompatibleWithStandardLibrary
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
func Decrypt(k *kms.KMS, ciphertext io.Reader, associatedData kms.Context) (io.Reader, error) {
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
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(metadataBuffer, &metadata); err != nil {
		return nil, err
	}

	key, err := k.Decrypt(context.TODO(), &kms.DecryptRequest{
		Name:           metadata.KeyID,
		Ciphertext:     metadata.KMSKey,
		AssociatedData: associatedData,
	})
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
