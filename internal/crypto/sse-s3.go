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
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"path"
	"strings"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
)

type sses3 struct{}

var (
	// S3 represents AWS SSE-S3. It provides functionality to handle
	// SSE-S3 requests.
	S3 = sses3{}

	_ Type = S3
)

// String returns the SSE domain as string. For SSE-S3 the
// domain is "SSE-S3".
func (sses3) String() string { return "SSE-S3" }

func (sses3) IsRequested(h http.Header) bool {
	_, ok := h[xhttp.AmzServerSideEncryption]
	// Return only true if the SSE header is specified and does not contain the SSE-KMS value
	return ok && !strings.EqualFold(h.Get(xhttp.AmzServerSideEncryption), xhttp.AmzEncryptionKMS)
}

// ParseHTTP parses the SSE-S3 related HTTP headers and checks
// whether they contain valid values.
func (sses3) ParseHTTP(h http.Header) error {
	if h.Get(xhttp.AmzServerSideEncryption) != xhttp.AmzEncryptionAES {
		return ErrInvalidEncryptionMethod
	}
	return nil
}

// IsEncrypted returns true if the object metadata indicates
// that the object was uploaded using SSE-S3.
func (sses3) IsEncrypted(metadata map[string]string) bool {
	if _, ok := metadata[MetaSealedKeyS3]; ok {
		return true
	}
	return false
}

// UnsealObjectKey extracts and decrypts the sealed object key
// from the metadata using KMS and returns the decrypted object
// key.
func (s3 sses3) UnsealObjectKey(k *kms.KMS, metadata map[string]string, bucket, object string) (key ObjectKey, err error) {
	if k == nil {
		return key, Errorf("KMS not configured")
	}
	keyID, kmsKey, sealedKey, err := s3.ParseMetadata(metadata)
	if err != nil {
		return key, err
	}
	unsealKey, err := k.Decrypt(context.TODO(), &kms.DecryptRequest{
		Name:           keyID,
		Ciphertext:     kmsKey,
		AssociatedData: kms.Context{bucket: path.Join(bucket, object)},
	})
	if err != nil {
		return key, err
	}
	err = key.Unseal(unsealKey, sealedKey, s3.String(), bucket, object)
	return key, err
}

// UnsealObjectsKeys extracts and decrypts all sealed object keys
// from the metadata using the KMS and returns the decrypted object
// keys.
//
// The metadata, buckets and objects slices must have the same length.
func (s3 sses3) UnsealObjectKeys(ctx context.Context, k *kms.KMS, metadata []map[string]string, buckets, objects []string) ([]ObjectKey, error) {
	if k == nil {
		return nil, Errorf("KMS not configured")
	}

	if len(metadata) != len(buckets) || len(metadata) != len(objects) {
		return nil, Errorf("invalid metadata/object count: %d != %d != %d", len(metadata), len(buckets), len(objects))
	}
	keys := make([]ObjectKey, 0, len(metadata))
	for i := range metadata {
		key, err := s3.UnsealObjectKey(k, metadata[i], buckets[i], objects[i])
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// CreateMetadata encodes the sealed object key into the metadata and returns
// the modified metadata. If the keyID and the kmsKey is not empty it encodes
// both into the metadata as well. It allocates a new metadata map if metadata
// is nil.
func (sses3) CreateMetadata(metadata map[string]string, keyID string, kmsKey []byte, sealedKey SealedKey) map[string]string {
	if sealedKey.Algorithm != SealAlgorithm {
		logger.CriticalIf(context.Background(), Errorf("The seal algorithm '%s' is invalid for SSE-S3", sealedKey.Algorithm))
	}

	// There are two possibilities:
	// - We use a KMS -> There must be non-empty key ID and a KMS data key.
	// - We use a K/V -> There must be no key ID and no KMS data key.
	// Otherwise, the caller has passed an invalid argument combination.
	if keyID == "" && len(kmsKey) != 0 {
		logger.CriticalIf(context.Background(), errors.New("The key ID must not be empty if a KMS data key is present"))
	}
	if keyID != "" && len(kmsKey) == 0 {
		logger.CriticalIf(context.Background(), errors.New("The KMS data key must not be empty if a key ID is present"))
	}

	if metadata == nil {
		metadata = make(map[string]string, 5)
	}

	metadata[MetaAlgorithm] = sealedKey.Algorithm
	metadata[MetaIV] = base64.StdEncoding.EncodeToString(sealedKey.IV[:])
	metadata[MetaSealedKeyS3] = base64.StdEncoding.EncodeToString(sealedKey.Key[:])
	if len(kmsKey) > 0 && keyID != "" { // We use a KMS -> Store key ID and sealed KMS data key.
		metadata[MetaKeyID] = keyID
		metadata[MetaDataEncryptionKey] = base64.StdEncoding.EncodeToString(kmsKey)
	}
	return metadata
}

// ParseMetadata extracts all SSE-S3 related values from the object metadata
// and checks whether they are well-formed. It returns the sealed object key
// on success. If the metadata contains both, a KMS master key ID and a sealed
// KMS data key it returns both. If the metadata does not contain neither a
// KMS master key ID nor a sealed KMS data key it returns an empty keyID and
// KMS data key. Otherwise, it returns an error.
func (sses3) ParseMetadata(metadata map[string]string) (keyID string, kmsKey []byte, sealedKey SealedKey, err error) {
	// Extract all required values from object metadata
	b64IV, ok := metadata[MetaIV]
	if !ok {
		return keyID, kmsKey, sealedKey, errMissingInternalIV
	}
	algorithm, ok := metadata[MetaAlgorithm]
	if !ok {
		return keyID, kmsKey, sealedKey, errMissingInternalSealAlgorithm
	}
	b64SealedKey, ok := metadata[MetaSealedKeyS3]
	if !ok {
		return keyID, kmsKey, sealedKey, Errorf("The object metadata is missing the internal sealed key for SSE-S3")
	}

	// There are two possibilities:
	// - We use a KMS -> There must be a key ID and a KMS data key.
	// - We use a K/V -> There must be no key ID and no KMS data key.
	// Otherwise, the metadata is corrupted.
	keyID, idPresent := metadata[MetaKeyID]
	b64KMSSealedKey, kmsKeyPresent := metadata[MetaDataEncryptionKey]
	if !idPresent && kmsKeyPresent {
		return keyID, kmsKey, sealedKey, Errorf("The object metadata is missing the internal KMS key-ID for SSE-S3")
	}
	if idPresent && !kmsKeyPresent {
		return keyID, kmsKey, sealedKey, Errorf("The object metadata is missing the internal sealed KMS data key for SSE-S3")
	}

	// Check whether all extracted values are well-formed
	iv, err := base64.StdEncoding.DecodeString(b64IV)
	if err != nil || len(iv) != 32 {
		return keyID, kmsKey, sealedKey, errInvalidInternalIV
	}
	if algorithm != SealAlgorithm {
		return keyID, kmsKey, sealedKey, errInvalidInternalSealAlgorithm
	}
	encryptedKey, err := base64.StdEncoding.DecodeString(b64SealedKey)
	if err != nil || len(encryptedKey) != 64 {
		return keyID, kmsKey, sealedKey, Errorf("The internal sealed key for SSE-S3 is invalid")
	}
	if idPresent && kmsKeyPresent { // We are using a KMS -> parse the sealed KMS data key.
		kmsKey, err = base64.StdEncoding.DecodeString(b64KMSSealedKey)
		if err != nil {
			return keyID, kmsKey, sealedKey, Errorf("The internal sealed KMS data key for SSE-S3 is invalid")
		}
	}

	sealedKey.Algorithm = algorithm
	copy(sealedKey.IV[:], iv)
	copy(sealedKey.Key[:], encryptedKey)
	return keyID, kmsKey, sealedKey, nil
}
