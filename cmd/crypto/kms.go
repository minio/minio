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
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"sort"

	"github.com/secure-io/sio-go/sioutil"
	"golang.org/x/crypto/chacha20"
	"golang.org/x/crypto/chacha20poly1305"
)

// Context is a list of key-value pairs cryptographically
// associated with a certain object.
type Context map[string]string

// MarshalText returns a canonical text representation of
// the Context.

// MarshalText sorts the context keys and writes the sorted
// key-value pairs as canonical JSON object. The sort order
// is based on the un-escaped keys. It never returns an error.
func (c Context) MarshalText() ([]byte, error) {
	if len(c) == 0 {
		return []byte{'{', '}'}, nil
	}

	// Pre-allocate a buffer - 128 bytes is an arbitrary
	// heuristic value that seems like a good starting size.
	var b = bytes.NewBuffer(make([]byte, 0, 128))
	if len(c) == 1 {
		for k, v := range c {
			b.WriteString(`{"`)
			EscapeStringJSON(b, k)
			b.WriteString(`":"`)
			EscapeStringJSON(b, v)
			b.WriteString(`"}`)
		}
		return b.Bytes(), nil
	}

	sortedKeys := make([]string, 0, len(c))
	for k := range c {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	b.WriteByte('{')
	for i, k := range sortedKeys {
		b.WriteByte('"')
		EscapeStringJSON(b, k)
		b.WriteString(`":"`)
		EscapeStringJSON(b, c[k])
		b.WriteByte('"')
		if i < len(sortedKeys)-1 {
			b.WriteByte(',')
		}
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// KMS represents an active and authenticted connection
// to a Key-Management-Service. It supports generating
// data key generation and unsealing of KMS-generated
// data keys.
type KMS interface {
	// DefaultKeyID returns the default master key ID. It should be
	// used for SSE-S3 and whenever a S3 client requests SSE-KMS but
	// does not specify an explicit SSE-KMS key ID.
	DefaultKeyID() string

	// CreateKey creates a new master key with the given key ID
	// at the KMS.
	CreateKey(keyID string) error

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

	// Info returns descriptive information about the KMS,
	// like the default key ID and authentication method.
	Info() KMSInfo
}

type masterKeyKMS struct {
	keyID     string
	masterKey [32]byte
}

// KMSInfo contains some describing information about
// the KMS.
type KMSInfo struct {
	Endpoints []string
	Name      string
	AuthType  string
}

// NewMasterKey returns a basic KMS implementation from a single 256 bit master key.
//
// The KMS accepts any keyID but binds the keyID and context cryptographically
// to the generated keys.
func NewMasterKey(keyID string, key [32]byte) KMS { return &masterKeyKMS{keyID: keyID, masterKey: key} }

func (kms *masterKeyKMS) DefaultKeyID() string {
	return kms.keyID
}

func (kms *masterKeyKMS) CreateKey(keyID string) error {
	return errors.New("crypto: creating keys is not supported by a static master key")
}

func (kms *masterKeyKMS) GenerateKey(keyID string, ctx Context) (key [32]byte, sealedKey []byte, err error) {
	k, err := sioutil.Random(32)
	if err != nil {
		return key, sealedKey, err
	}
	copy(key[:], k)

	iv, err := sioutil.Random(16)
	if err != nil {
		return key, sealedKey, err
	}

	var algorithm string
	if sioutil.NativeAES() {
		algorithm = "AES-256-GCM-HMAC-SHA-256"
	} else {
		algorithm = "ChaCha20Poly1305"
	}

	var aead cipher.AEAD
	switch algorithm {
	case "AES-256-GCM-HMAC-SHA-256":
		mac := hmac.New(sha256.New, kms.masterKey[:])
		mac.Write(iv)
		sealingKey := mac.Sum(nil)

		var block cipher.Block
		block, err = aes.NewCipher(sealingKey)
		if err != nil {
			return key, sealedKey, err
		}
		aead, err = cipher.NewGCM(block)
		if err != nil {
			return key, sealedKey, err
		}
	case "ChaCha20Poly1305":
		var sealingKey []byte
		sealingKey, err = chacha20.HChaCha20(kms.masterKey[:], iv)
		if err != nil {
			return key, sealedKey, err
		}
		aead, err = chacha20poly1305.New(sealingKey)
		if err != nil {
			return key, sealedKey, err
		}
	default:
		return key, sealedKey, errors.New("invalid algorithm: " + algorithm)
	}

	nonce, err := sioutil.Random(aead.NonceSize())
	if err != nil {
		return key, sealedKey, err
	}
	associatedData, _ := ctx.MarshalText()
	ciphertext := aead.Seal(nil, nonce, key[:], associatedData)

	type SealedKey struct {
		Algorithm string `json:"aead"`
		IV        []byte `json:"iv"`
		Nonce     []byte `json:"nonce"`
		Bytes     []byte `json:"bytes"`
	}
	sealedKey, err = json.Marshal(SealedKey{
		Algorithm: algorithm,
		IV:        iv,
		Nonce:     nonce,
		Bytes:     ciphertext,
	})
	return key, sealedKey, err
}

// KMS is configured directly using master key
func (kms *masterKeyKMS) Info() (info KMSInfo) {
	return KMSInfo{
		Endpoints: []string{},
		Name:      "",
		AuthType:  "master-key",
	}
}

func (kms *masterKeyKMS) UnsealKey(keyID string, encryptedKey []byte, ctx Context) (key [32]byte, err error) {
	if keyID != kms.keyID {
		return key, Errorf("crypto: key %q does not exist", keyID)
	}

	type SealedKey struct {
		Algorithm string `json:"aead"`
		IV        []byte `json:"iv"`
		Nonce     []byte `json:"nonce"`
		Bytes     []byte `json:"bytes"`
	}
	var sealedKey SealedKey
	if err := json.Unmarshal(encryptedKey, &sealedKey); err != nil {
		return key, err
	}
	if n := len(sealedKey.IV); n != 16 {
		return key, Errorf("crypto: invalid iv size")
	}

	var aead cipher.AEAD
	switch sealedKey.Algorithm {
	case "AES-256-GCM-HMAC-SHA-256":
		mac := hmac.New(sha256.New, kms.masterKey[:])
		mac.Write(sealedKey.IV)
		sealingKey := mac.Sum(nil)

		block, err := aes.NewCipher(sealingKey[:])
		if err != nil {
			return key, err
		}
		aead, err = cipher.NewGCM(block)
		if err != nil {
			return key, err
		}
	case "ChaCha20Poly1305":
		sealingKey, err := chacha20.HChaCha20(kms.masterKey[:], sealedKey.IV)
		if err != nil {
			return key, err
		}
		aead, err = chacha20poly1305.New(sealingKey)
		if err != nil {
			return key, err
		}
	default:
		return key, Errorf("crypto: invalid algorithm: %q", sealedKey.Algorithm)
	}

	if n := len(sealedKey.Nonce); n != aead.NonceSize() {
		return key, Errorf("crypto: invalid nonce size %d", n)
	}

	associatedData, _ := ctx.MarshalText()
	plaintext, err := aead.Open(nil, sealedKey.Nonce, sealedKey.Bytes, associatedData)
	if err != nil {
		return key, Errorf("crypto: sealed key is not authentic")
	}
	if len(plaintext) != 32 {
		return key, Errorf("crypto: invalid plaintext key length %d", len(plaintext))
	}
	copy(key[:], plaintext)
	return key, nil
}
