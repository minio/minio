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
	"io"
	"path/filepath"

	"github.com/minio/minio/cmd/logger"
	sha256 "github.com/minio/sha256-simd"
	"github.com/minio/sio"
)

// ObjectKey is a 256 bit secret key used to encrypt the object.
// It must never be stored in plaintext.
type ObjectKey [32]byte

// GenerateKey generates a unique ObjectKey from a 256 bit external key
// and a source of randomness. If random is nil the default PRNG of system
// (crypto/rand) is used.
func GenerateKey(extKey [32]byte, random io.Reader) (key ObjectKey) {
	if random == nil {
		random = rand.Reader
	}
	var nonce [32]byte
	if _, err := io.ReadFull(random, nonce[:]); err != nil {
		logger.CriticalIf(context.Background(), errors.New("Unable to read enough randomness from the system"))
	}
	sha := sha256.New()
	sha.Write(extKey[:])
	sha.Write(nonce[:])
	sha.Sum(key[:0])
	return
}

// Seal encrypts the ObjectKey using the 256 bit external key and IV. The sealed
// key is also cryptographically bound to the object's path (bucket/object).
func (key ObjectKey) Seal(extKey, iv [32]byte, bucket, object string) []byte {
	var sealedKey bytes.Buffer
	mac := hmac.New(sha256.New, extKey[:])
	mac.Write(iv[:])
	mac.Write([]byte(filepath.Join(bucket, object)))

	if n, err := sio.Encrypt(&sealedKey, bytes.NewReader(key[:]), sio.Config{Key: mac.Sum(nil)}); n != 64 || err != nil {
		logger.CriticalIf(context.Background(), errors.New("Unable to generate sealed key"))
	}
	return sealedKey.Bytes()
}

// Unseal decrypts a sealed key using the 256 bit external key and IV. Since the sealed key
// is cryptographically bound to the object's path the same bucket/object as during sealing
// must be provided. On success the ObjectKey contains the decrypted sealed key.
func (key *ObjectKey) Unseal(sealedKey []byte, extKey, iv [32]byte, bucket, object string) error {
	var unsealedKey bytes.Buffer
	mac := hmac.New(sha256.New, extKey[:])
	mac.Write(iv[:])
	mac.Write([]byte(filepath.Join(bucket, object)))

	if n, err := sio.Decrypt(&unsealedKey, bytes.NewReader(sealedKey), sio.Config{Key: mac.Sum(nil)}); n != 32 || err != nil {
		return err // TODO(aead): upgrade sio to use sio.Error
	}
	copy(key[:], unsealedKey.Bytes())
	return nil
}

// DerivePartKey derives an unique 256 bit key from an ObjectKey and the part index.
func (key ObjectKey) DerivePartKey(id uint32) (partKey [32]byte) {
	var bin [4]byte
	binary.LittleEndian.PutUint32(bin[:], id)

	mac := hmac.New(sha256.New, key[:])
	mac.Write(bin[:])
	mac.Sum(partKey[:0])
	return
}
