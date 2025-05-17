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

package crypto

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"path"

	"github.com/minio/minio/internal/hash/sha256"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/sio"
)

// ObjectKey is a 256 bit secret key used to encrypt the object.
// It must never be stored in plaintext.
type ObjectKey [32]byte

// GenerateKey generates a unique ObjectKey from a 256 bit external key
// and a source of randomness. If random is nil the default PRNG of the
// system (crypto/rand) is used.
func GenerateKey(extKey []byte, random io.Reader) (key ObjectKey) {
	if random == nil {
		random = rand.Reader
	}
	if len(extKey) != 32 { // safety check
		logger.CriticalIf(context.Background(), errors.New("crypto: invalid key length"))
	}
	var nonce [32]byte
	if _, err := io.ReadFull(random, nonce[:]); err != nil {
		logger.CriticalIf(context.Background(), errOutOfEntropy)
	}

	const Context = "object-encryption-key generation"
	mac := hmac.New(sha256.New, extKey)
	mac.Write([]byte(Context))
	mac.Write(nonce[:])
	mac.Sum(key[:0])
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
	Key       [64]byte // The encrypted and authenticated object-key.
	IV        [32]byte // The random IV used to encrypt the object-key.
	Algorithm string   // The sealing algorithm used to encrypt the object key.
}

// Seal encrypts the ObjectKey using the 256 bit external key and IV. The sealed
// key is also cryptographically bound to the object's path (bucket/object) and the
// domain (SSE-C or SSE-S3).
func (key ObjectKey) Seal(extKey []byte, iv [32]byte, domain, bucket, object string) SealedKey {
	if len(extKey) != 32 {
		logger.CriticalIf(context.Background(), errors.New("crypto: invalid key length"))
	}
	var (
		sealingKey   [32]byte
		encryptedKey bytes.Buffer
	)
	mac := hmac.New(sha256.New, extKey)
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
func (key *ObjectKey) Unseal(extKey []byte, sealedKey SealedKey, domain, bucket, object string) error {
	var unsealConfig sio.Config
	switch sealedKey.Algorithm {
	default:
		return Errorf("The sealing algorithm '%s' is not supported", sealedKey.Algorithm)
	case SealAlgorithm:
		mac := hmac.New(sha256.New, extKey)
		mac.Write(sealedKey.IV[:])
		mac.Write([]byte(domain))
		mac.Write([]byte(SealAlgorithm))
		mac.Write([]byte(path.Join(bucket, object))) // use path.Join for canonical 'bucket/object'
		unsealConfig = sio.Config{MinVersion: sio.Version20, Key: mac.Sum(nil)}
	case InsecureSealAlgorithm:
		sha := sha256.New()
		sha.Write(extKey)
		sha.Write(sealedKey.IV[:])
		unsealConfig = sio.Config{MinVersion: sio.Version10, Key: sha.Sum(nil)}
	}

	if out, err := sio.DecryptBuffer(key[:0], sealedKey.Key[:], unsealConfig); len(out) != 32 || err != nil {
		return ErrSecretKeyMismatch
	}
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
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte("SSE-etag"))
	return sio.DecryptBuffer(make([]byte, 0, len(etag)), etag, sio.Config{Key: mac.Sum(nil)})
}
