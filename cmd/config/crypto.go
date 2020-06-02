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
	"crypto/hmac"
	"crypto/sha256"
	"encoding"
	"encoding/json"
	"errors"
	"io"

	"github.com/secure-io/sio-go"
	"github.com/secure-io/sio-go/sioutil"
	"golang.org/x/crypto/argon2"
)

const (
	// BackendEncryptionComplete is the Backend status indicating
	// that the backend is encrypted and any potential migration
	// has been completed.
	BackendEncryptionComplete = "encrypted"

	// BackendEncryptionPartial is the Backend status indicating
	// that the backend is encrypted but that a migration has happened
	// which is not complete yet.
	BackendEncryptionPartial = "incomplete"

	// BackendEncryptionMissing is the Backend status indicating
	// that the backend is not encrypted.
	BackendEncryptionMissing = "plaintext"
)

var ( // compiler checks
	_ encoding.BinaryMarshaler   = (*Backend)(nil)
	_ encoding.BinaryUnmarshaler = (*Backend)(nil)
)

// Backend represents the backend configuration state.
// It contains the backend status - i.e. encrypted or plaintext
// and derivation parameters for the master key used to encrypt.
// configuration data - like IAM users.
//
// The backend configuration can be partially encrypted since
// the migration from a plaintext configuration to an encrypted
// configuration takes time and the server may be stopped in-between.
type Backend struct {
	Status           string               `json:"status"`
	DerivationParams *KeyDerivationParams `json:"derivation_params"` // nil if backend is not encrypted

	// latest provides a way to distinguish whether
	// the backend encryption is uses the latest scheme.
	// It should only be set to true during unmarshaling
	// if the latest 'backend-encrypted' file format is encountered.
	latest bool
}

// IsComplete returns true if the backend encryption
// is complete.
func (e *Backend) IsComplete() bool { return e.Status == BackendEncryptionComplete }

// IsEncrypted retruns true if the backend is encrypted
// or partially encrypted.
func (e *Backend) IsEncrypted() bool {
	return e.Status == BackendEncryptionComplete || e.Status == BackendEncryptionPartial
}

// MigrationRequired returns true if a backend migration is required
// even though it is completly encrypted.
func (e *Backend) MigrationRequired() bool { return !e.latest }

// MarshalBinary returns the Backend representation
// that can be stored on disk.
func (e Backend) MarshalBinary() ([]byte, error) { return json.Marshal(e) }

// UnmarshalBinary returns parses and unmarshals a
// disk representation of Backend.
func (e *Backend) UnmarshalBinary(data []byte) error {
	// We have to be able to parse the legacy 'backend-encrypted' object
	// for backward compatibility.
	if text := string(data); text == BackendEncryptionComplete || text == BackendEncryptionPartial {
		e.Status = text
		salt, err := sioutil.Random(16)
		if err != nil {
			return err
		}
		e.Status = text
		e.DerivationParams = DefaultKeyDerivationParams(salt)
		e.latest = false
		return nil
	}

	if err := json.Unmarshal(data, e); err != nil {
		return err
	}
	e.latest = true
	return nil
}

// KeyDerivationParams contains key deriviation
// parameters used to derive a master key from
// the S3 secret key.
type KeyDerivationParams struct {
	Algorithm  string `json:"algorithm"`
	Salt       []byte `json:"salt"`
	TimeCost   int    `json:"time"`
	MemoryCost int    `json:"memory"`
	Threads    int    `json:"threads"`
}

// DefaultKeyDerivationParams returns new Argon2id key
// derivation parameters with the given salt and reasonable
// default cost parameters.
func DefaultKeyDerivationParams(salt []byte) *KeyDerivationParams {
	return &KeyDerivationParams{
		Algorithm:  "Argon2id",
		Salt:       salt,
		TimeCost:   1,
		MemoryCost: 64 * 1024,
		Threads:    4,
	}
}

// MasterKey is a 256 bit cryptographic key that can be used to
// encrypt data streams.
type MasterKey []byte

// DeriveMasterKey derives a new master key from the given password
// and key deriviation parameters.
//
// In the context of S3, the password should be the S3 secret key.
//
// It returns an error if the key derivation parameters are invalid
// or would cause a very expensive key derivation.
func DeriveMasterKey(password string, params KeyDerivationParams) (MasterKey, error) {
	// Define some reasonable max. values. We should not
	// accept arbitrary cost parameters (from untrusted sources)
	// since large cost parameters can exhaust server resources.
	const (
		MaxTimeCost   = 10         // At most 10 passes over the memory
		MaxMemoryCost = 256 * 1024 // At most 256 MiB memory
		MaxThreads    = 16         // At most 16 threads
	)

	if params.Algorithm != "Argon2id" {
		return nil, errors.New("config: invalid key derivation algorithm")
	}
	if params.TimeCost < 1 || params.TimeCost > MaxTimeCost {
		return nil, errors.New("config: invalid key derivation time cost")
	}
	if params.MemoryCost > MaxMemoryCost {
		return nil, errors.New("config: invalid key derivation memory cost")
	}
	if params.Threads < 1 || params.Threads > MaxThreads {
		return nil, errors.New("config: invalid key derivation parallelism")
	}
	return MasterKey(argon2.IDKey(
		[]byte(password),
		params.Salt,
		uint32(params.TimeCost),
		uint32(params.MemoryCost),
		uint8(params.Threads),
		32,
	)), nil
}

// EncryptBytes encrypts and authenticates the given data
// and returns the encrypted ciphertext.
func (key MasterKey) EncryptBytes(data []byte) ([]byte, error) {
	ciphertext := bytes.NewBuffer(make([]byte, 0, len(data)))

	w, err := key.Encrypt(ciphertext)
	if err != nil {
		return nil, err
	}
	if _, err = io.Copy(w, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	if err = w.Close(); err != nil {
		return nil, err
	}
	return ciphertext.Bytes(), nil
}

// Encrypt returns a new io.WriterCloser that wraps w and
// encrypts and authenticates everything written to it.
//
// The returned io.WriteCloser has to be closed successfully to
// complete the encryption process.
func (key MasterKey) Encrypt(w io.Writer) (io.WriteCloser, error) {
	iv, err := sioutil.Random(16)
	if err != nil {
		return nil, err
	}

	prf := hmac.New(sha256.New, key)
	prf.Write(iv)
	fileKey := prf.Sum(nil)

	nonce, err := sioutil.Random(8)
	if err != nil {
		return nil, err
	}

	const (
		Version = 1

		AES256GCM        = 0
		ChaCha20Poly1305 = 1
	)

	var (
		header    [2]byte
		algorithm sio.Algorithm
	)
	header[0] = Version
	if sioutil.NativeAES() {
		algorithm = sio.AES_256_GCM
		header[1] = AES256GCM
	} else {
		algorithm = sio.ChaCha20Poly1305
		header[1] = ChaCha20Poly1305
	}

	stream, err := algorithm.Stream(fileKey)
	if err != nil {
		return nil, err
	}

	if _, err = w.Write(header[:]); err != nil {
		return nil, err
	}
	if _, err = w.Write(iv); err != nil {
		return nil, err
	}
	if _, err = w.Write(nonce); err != nil {
		return nil, err
	}
	return stream.EncryptWriter(w, nonce, nil), nil
}

// DecryptBytes decrypts the given data and returns the
// a plaintext on success. If the data is not an authentic
// ciphertext DecryptBytes returns sio.NotAuthentic.
func (key MasterKey) DecryptBytes(data []byte) ([]byte, error) {
	plaintext := bytes.NewBuffer(make([]byte, 0, len(data)))

	r, err := key.Decrypt(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(plaintext, r); err != nil {
		return nil, err
	}
	return plaintext.Bytes(), nil
}

// Decrypt returns a new io.Reader that wraps r and decrypts
// everything read from it.
func (key MasterKey) Decrypt(r io.Reader) (io.Reader, error) {
	var (
		header [2]byte
		iv     [16]byte
		nonce  [8]byte
	)
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, sio.NotAuthentic
		}
		return nil, err
	}
	if _, err := io.ReadFull(r, iv[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, sio.NotAuthentic
		}
		return nil, err
	}
	if _, err := io.ReadFull(r, nonce[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, sio.NotAuthentic
		}
		return nil, err
	}

	const (
		Version = 1

		AES256GCM        = 0
		ChaCha20Poly1305 = 1
	)
	if header[0] != Version {
		return nil, sio.NotAuthentic
	}
	var algorithm sio.Algorithm
	switch header[1] {
	case AES256GCM:
		algorithm = sio.AES_256_GCM
	case ChaCha20Poly1305:
		algorithm = sio.ChaCha20Poly1305
	default:
		return nil, sio.NotAuthentic
	}

	prf := hmac.New(sha256.New, key)
	prf.Write(iv[:])
	fileKey := prf.Sum(nil)

	stream, err := algorithm.Stream(fileKey)
	if err != nil {
		return nil, err
	}
	return stream.DecryptReader(r, nonce[:], nil), nil
}
