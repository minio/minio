/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
	"io/ioutil"

	"github.com/secure-io/sio-go"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/sys/cpu"
)

// EncryptData encrypts the data with an unique key
// derived from password using the Argon2id PBKDF.
//
// The returned ciphertext data consists of:
//    salt | AEAD ID | nonce | encrypted data
//     32      1         8      ~ len(data)
func EncryptData(password string, data []byte) ([]byte, error) {
	salt := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, err
	}

	// Derive an unique 256 bit key from the password and the random salt.
	key := argon2.IDKey([]byte(password), salt, 1, 64*1024, 4, 32)

	// Create a new AEAD instance - either AES-GCM if the CPU provides
	// an AES hardware implementation or ChaCha20-Poly1305 otherwise.
	id, cipher, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	stream := sio.NewStream(cipher, sio.BufSize)

	nonce := make([]byte, stream.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

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

	key := argon2.IDKey([]byte(password), salt[:], 1, 64*1024, 4, 32)
	cipher, err := parseAEAD(id[0], key)
	if err != nil {
		return nil, err
	}
	stream := sio.NewStream(cipher, sio.BufSize)
	return ioutil.ReadAll(stream.DecryptReader(data, nonce[:], nil))
}

const (
	aesGcm   = 0x00
	c20p1305 = 0x01
)

// newAEAD creates a new AEAD instance from the given key.
// If the CPU provides an AES hardware implementation it
// returns an AES-GCM instance. Otherwise it returns a
// ChaCha20-Poly1305 instance.
//
// newAEAD also returns a byte as algorithm ID indicating
// which AEAD scheme it has selected.
func newAEAD(key []byte) (byte, cipher.AEAD, error) {
	if (cpu.X86.HasAES && cpu.X86.HasPCLMULQDQ) || cpu.S390X.HasAES || cpu.PPC64.IsPOWER8 || cpu.ARM64.HasAES {
		block, err := aes.NewCipher(key)
		if err != nil {
			return 0, nil, err
		}
		c, err := cipher.NewGCM(block)
		return aesGcm, c, err
	}
	c, err := chacha20poly1305.New(key)
	return c20p1305, c, err
}

// parseAEAD creates a new AEAD instance from the id and key.
// The id must be either 0 (AES-GCM) or 1 (ChaCha20-Poly1305).
func parseAEAD(id byte, key []byte) (cipher.AEAD, error) {
	switch id {
	case aesGcm:
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		return cipher.NewGCM(block)
	case c20p1305:
		return chacha20poly1305.New(key)
	default:
		return nil, errors.New("madmin: invalid AEAD algorithm ID")
	}
}
