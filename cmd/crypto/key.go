// Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/minio/minio/cmd/logger"
	sha256 "github.com/minio/sha256-simd"
	"github.com/minio/sio"
)

// ObjectKey is a 256 bit secret key used to encrypt the object.
// It must never be stored in plaintext.
type ObjectKey [32]byte

// GenerateKey generates a unique ObjectKey from a 256 bit external key
// and a source of randomness. If random is nil the default PRNG of the
// system (crypto/rand) is used.
func GenerateKey(extKey [32]byte, random io.Reader) (key ObjectKey) {
	if random == nil {
		random = rand.Reader
	}
	var nonce [32]byte
	if _, err := io.ReadFull(random, nonce[:]); err != nil {
		logger.CriticalIf(context.Background(), errOutOfEntropy)
	}
	sha := sha256.New()
	sha.Write(extKey[:])
	sha.Write(nonce[:])
	sha.Sum(key[:0])
	return key
}

// GenerateIV generates a new random 256 bit IV from the provided source
// of randomness. If random is nil the default PRNG of the system
// (crypto/rand) is used.
func GenerateIV(random io.Reader) (iv [32]byte) {
	if random == nil {
		random = rand.Reader
	}
	if _, err := io.ReadFull(random, iv[:]); err != nil {
		logger.CriticalIf(context.Background(), errOutOfEntropy)
	}
	return iv
}

// SealedKey represents a sealed object key. It can be stored
// at an untrusted location.
type SealedKey struct {
	Key       [64]byte // The encrypted and authenticted object-key.
	IV        [32]byte // The random IV used to encrypt the object-key.
	Algorithm string   // The sealing algorithm used to encrypt the object key.
}

// Seal encrypts the ObjectKey using the 256 bit external key and IV. The sealed
// key is also cryptographically bound to the object's path (bucket/object) and the
// domain (SSE-C or SSE-S3).
func (key ObjectKey) Seal(extKey, iv [32]byte, domain, bucket, object string) SealedKey {
	var (
		sealingKey   [32]byte
		encryptedKey bytes.Buffer
	)
	mac := hmac.New(sha256.New, extKey[:])
	mac.Write(iv[:])
	mac.Write([]byte(domain))
	mac.Write([]byte(SealAlgorithm))
	mac.Write([]byte(path.Join(bucket, object))) // use path.Join for canonical 'bucket/object'
	mac.Sum(sealingKey[:0])
	if n, err := sio.Encrypt(&encryptedKey, bytes.NewReader(key[:]), sio.Config{Key: sealingKey[:]}); n != 64 || err != nil {
		logger.CriticalIf(context.Background(), errors.New("Unable to generate sealed key"))
	}
	sealedKey := SealedKey{
		IV:        iv,
		Algorithm: SealAlgorithm,
	}
	copy(sealedKey.Key[:], encryptedKey.Bytes())
	return sealedKey
}

// Unseal decrypts a sealed key using the 256 bit external key. Since the sealed key
// may be cryptographically bound to the object's path the same bucket/object as during sealing
// must be provided. On success the ObjectKey contains the decrypted sealed key.
func (key *ObjectKey) Unseal(extKey [32]byte, sealedKey SealedKey, domain, bucket, object string) error {
	var (
		unsealConfig sio.Config
		decryptedKey bytes.Buffer
	)
	switch sealedKey.Algorithm {
	default:
		return Error{fmt.Sprintf("The sealing algorithm '%s' is not supported", sealedKey.Algorithm)}
	case SealAlgorithm:
		mac := hmac.New(sha256.New, extKey[:])
		mac.Write(sealedKey.IV[:])
		mac.Write([]byte(domain))
		mac.Write([]byte(SealAlgorithm))
		mac.Write([]byte(path.Join(bucket, object))) // use path.Join for canonical 'bucket/object'
		unsealConfig = sio.Config{MinVersion: sio.Version20, Key: mac.Sum(nil)}
	case InsecureSealAlgorithm:
		sha := sha256.New()
		sha.Write(extKey[:])
		sha.Write(sealedKey.IV[:])
		unsealConfig = sio.Config{MinVersion: sio.Version10, Key: sha.Sum(nil)}
	}

	if n, err := sio.Decrypt(&decryptedKey, bytes.NewReader(sealedKey.Key[:]), unsealConfig); n != 32 || err != nil {
		return ErrSecretKeyMismatch
	}
	copy(key[:], decryptedKey.Bytes())
	return nil
}

// DerivePartKey derives an unique 256 bit key from an ObjectKey and the part index.
func (key ObjectKey) DerivePartKey(id uint32) (partKey [32]byte) {
	var bin [4]byte
	binary.LittleEndian.PutUint32(bin[:], id)

	mac := hmac.New(sha256.New, key[:])
	mac.Write(bin[:])
	mac.Sum(partKey[:0])
	return partKey
}

// SealETag seals the etag using the object key.
// It does not encrypt empty ETags because such ETags indicate
// that the S3 client hasn't sent an ETag = MD5(object) and
// the backend can pick an ETag value.
func (key ObjectKey) SealETag(etag []byte) []byte {
	if len(etag) == 0 { // don't encrypt empty ETag - only if client sent ETag = MD5(object)
		return etag
	}
	var buffer bytes.Buffer
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte("SSE-etag"))
	if _, err := sio.Encrypt(&buffer, bytes.NewReader(etag), sio.Config{Key: mac.Sum(nil)}); err != nil {
		logger.CriticalIf(context.Background(), errors.New("Unable to encrypt ETag using object key"))
	}
	return buffer.Bytes()
}

// UnsealETag unseals the etag using the provided object key.
// It does not try to decrypt the ETag if len(etag) == 16
// because such ETags indicate that the S3 client hasn't sent
// an ETag = MD5(object) and the backend has picked an ETag value.
func (key ObjectKey) UnsealETag(etag []byte) ([]byte, error) {
	if !IsETagSealed(etag) {
		return etag, nil
	}
	var buffer bytes.Buffer
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte("SSE-etag"))
	if _, err := sio.Decrypt(&buffer, bytes.NewReader(etag), sio.Config{Key: mac.Sum(nil)}); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
