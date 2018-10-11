/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"crypto/rand"
	"io"
	"io/ioutil"

	"github.com/minio/sio"
	"golang.org/x/crypto/argon2"
)

// EncryptData - encrypts server config data.
func EncryptData(password string, data []byte) ([]byte, error) {
	salt := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, err
	}

	// derive an encryption key from the master key and the nonce
	var key [32]byte
	copy(key[:], argon2.IDKey([]byte(password), salt, 1, 64*1024, 4, 32))

	encrypted, err := sio.EncryptReader(bytes.NewReader(data), sio.Config{
		Key: key[:]},
	)
	if err != nil {
		return nil, err
	}
	edata, err := ioutil.ReadAll(encrypted)
	return append(salt, edata...), err
}

// DecryptData - decrypts server config data.
func DecryptData(password string, data io.Reader) ([]byte, error) {
	salt := make([]byte, 32)
	if _, err := io.ReadFull(data, salt); err != nil {
		return nil, err
	}
	// derive an encryption key from the master key and the nonce
	var key [32]byte
	copy(key[:], argon2.IDKey([]byte(password), salt, 1, 64*1024, 4, 32))

	decrypted, err := sio.DecryptReader(data, sio.Config{
		Key: key[:]},
	)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(decrypted)
}
