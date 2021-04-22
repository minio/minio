/*
 * MinIO Cloud Storage, (C) 2018-2021 MinIO, Inc.
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
 *
 */

package madmin

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"io/ioutil"

	"github.com/minio/minio/pkg/argon2"
	"github.com/minio/minio/pkg/fips"
	"github.com/secure-io/sio-go"
	"github.com/secure-io/sio-go/sioutil"
	"golang.org/x/crypto/pbkdf2"
)

// EncryptData encrypts the data with an unique key
// derived from password using the Argon2id PBKDF.
//
// The returned ciphertext data consists of:
//    salt | AEAD ID | nonce | encrypted data
//     32      1         8      ~ len(data)
func EncryptData(password string, data []byte) ([]byte, error) {
	salt := sioutil.MustRandom(32)

	var (
		id     byte
		err    error
		stream *sio.Stream
	)
	if fips.Enabled() {
		key := pbkdf2.Key([]byte(password), salt, pbkdf2Cost, 32, sha256.New)
		stream, err = sio.AES_256_GCM.Stream(key)
		if err != nil {
			return nil, err
		}
		id = pbkdf2AESGCM
	} else {
		key := argon2.IDKey([]byte(password), salt, argon2idTime, argon2idMemory, argon2idThreads, 32)
		if sioutil.NativeAES() {
			stream, err = sio.AES_256_GCM.Stream(key)
			if err != nil {
				return nil, err
			}
			id = argon2idAESGCM
		} else {
			stream, err = sio.ChaCha20Poly1305.Stream(key)
			if err != nil {
				return nil, err
			}
			id = argon2idChaCHa20Poly1305
		}
	}

	nonce := sioutil.MustRandom(stream.NonceSize())

	// ciphertext = salt || AEAD ID | nonce | encrypted data
	cLen := int64(len(salt)+1+len(nonce)+len(data)) + stream.Overhead(int64(len(data)))
	ciphertext := bytes.NewBuffer(make([]byte, 0, cLen)) // pre-alloc correct length

	// Prefix the ciphertext with salt, AEAD ID and nonce
	ciphertext.Write(salt)
	ciphertext.WriteByte(id)
	ciphertext.Write(nonce)

	w := stream.EncryptWriter(ciphertext, nonce, nil)
	if _, err = w.Write(data); err != nil {
		return nil, err
	}
	if err = w.Close(); err != nil {
		return nil, err
	}
	return ciphertext.Bytes(), nil
}

// ErrMaliciousData indicates that the stream cannot be
// decrypted by provided credentials.
var ErrMaliciousData = sio.NotAuthentic

// DecryptData decrypts the data with the key derived
// from the salt (part of data) and the password using
// the PBKDF used in EncryptData. DecryptData returns
// the decrypted plaintext on success.
//
// The data must be a valid ciphertext produced by
// EncryptData. Otherwise, the decryption will fail.
func DecryptData(password string, data io.Reader) ([]byte, error) {
	var (
		salt  [32]byte
		id    [1]byte
		nonce [8]byte // This depends on the AEAD but both used ciphers have the same nonce length.
	)

	if _, err := io.ReadFull(data, salt[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(data, id[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(data, nonce[:]); err != nil {
		return nil, err
	}

	var (
		err    error
		stream *sio.Stream
	)
	switch {
	case id[0] == argon2idAESGCM:
		key := argon2.IDKey([]byte(password), salt[:], argon2idTime, argon2idMemory, argon2idThreads, 32)
		stream, err = sio.AES_256_GCM.Stream(key)
	case id[0] == argon2idChaCHa20Poly1305:
		key := argon2.IDKey([]byte(password), salt[:], argon2idTime, argon2idMemory, argon2idThreads, 32)
		stream, err = sio.ChaCha20Poly1305.Stream(key)
	case id[0] == pbkdf2AESGCM:
		key := pbkdf2.Key([]byte(password), salt[:], pbkdf2Cost, 32, sha256.New)
		stream, err = sio.AES_256_GCM.Stream(key)
	default:
		err = errors.New("madmin: invalid encryption algorithm ID")
	}
	if err != nil {
		return nil, err
	}

	plaintext, err := ioutil.ReadAll(stream.DecryptReader(data, nonce[:], nil))
	if err != nil {
		return nil, err
	}
	return plaintext, err
}

const (
	argon2idAESGCM           = 0x00
	argon2idChaCHa20Poly1305 = 0x01
	pbkdf2AESGCM             = 0x02
)

const (
	argon2idTime    = 1
	argon2idMemory  = 64 * 1024
	argon2idThreads = 4
	pbkdf2Cost      = 8192
)
