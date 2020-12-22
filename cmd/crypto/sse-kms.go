/*
 * Minio Cloud Storage, (C) 2019-2020 Minio, Inc.
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

package crypto

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"path"
	"strings"

	jsoniter "github.com/json-iterator/go"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
)

type ssekms struct{}

var (
	// S3KMS represents AWS SSE-KMS. It provides functionality to
	// handle SSE-KMS requests.
	S3KMS = ssekms{}

	_ Type = S3KMS
)

// String returns the SSE domain as string. For SSE-KMS the
// domain is "SSE-KMS".
func (ssekms) String() string { return "SSE-KMS" }

// IsRequested returns true if the HTTP headers contains
// at least one SSE-KMS header.
func (ssekms) IsRequested(h http.Header) bool {
	if _, ok := h[xhttp.AmzServerSideEncryptionKmsID]; ok {
		return true
	}
	if _, ok := h[xhttp.AmzServerSideEncryptionKmsContext]; ok {
		return true
	}
	if _, ok := h[xhttp.AmzServerSideEncryption]; ok {
		return strings.ToUpper(h.Get(xhttp.AmzServerSideEncryption)) != xhttp.AmzEncryptionAES // Return only true if the SSE header is specified and does not contain the SSE-S3 value
	}
	return false
}

// ParseHTTP parses the SSE-KMS headers and returns the SSE-KMS key ID
// and the KMS context on success.
func (ssekms) ParseHTTP(h http.Header) (string, Context, error) {
	algorithm := h.Get(xhttp.AmzServerSideEncryption)
	if algorithm != xhttp.AmzEncryptionKMS {
		return "", nil, ErrInvalidEncryptionMethod
	}

	var ctx Context
	if context, ok := h[xhttp.AmzServerSideEncryptionKmsContext]; ok {
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		if err := json.Unmarshal([]byte(context[0]), &ctx); err != nil {
			return "", nil, err
		}
	}
	return h.Get(xhttp.AmzServerSideEncryptionKmsID), ctx, nil
}

// IsEncrypted returns true if the object metadata indicates
// that the object was uploaded using SSE-KMS.
func (ssekms) IsEncrypted(metadata map[string]string) bool {
	if _, ok := metadata[MetaSealedKeyKMS]; ok {
		return true
	}
	if _, ok := metadata[MetaKeyID]; ok {
		return true
	}
	if _, ok := metadata[MetaDataEncryptionKey]; ok {
		return true
	}
	return false
}

// UnsealObjectKey extracts and decrypts the sealed object key
// from the metadata using KMS and returns the decrypted object
// key.
func (s3 ssekms) UnsealObjectKey(kms KMS, metadata map[string]string, bucket, object string) (key ObjectKey, err error) {
	keyID, kmsKey, sealedKey, err := s3.ParseMetadata(metadata)
	if err != nil {
		return key, err
	}
	unsealKey, err := kms.UnsealKey(keyID, kmsKey, Context{bucket: path.Join(bucket, object)})
	if err != nil {
		return key, err
	}
	err = key.Unseal(unsealKey, sealedKey, s3.String(), bucket, object)
	return key, err
}

// CreateMetadata encodes the sealed object key into the metadata and returns
// the modified metadata. If the keyID and the kmsKey is not empty it encodes
// both into the metadata as well. It allocates a new metadata map if metadata
// is nil.
func (ssekms) CreateMetadata(metadata map[string]string, keyID string, kmsKey []byte, sealedKey SealedKey) map[string]string {
	if sealedKey.Algorithm != SealAlgorithm {
		logger.CriticalIf(context.Background(), Errorf("The seal algorithm '%s' is invalid for SSE-S3", sealedKey.Algorithm))
	}

	// There are two possibilites:
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
	metadata[MetaSealedKeyKMS] = base64.StdEncoding.EncodeToString(sealedKey.Key[:])
	if len(kmsKey) > 0 && keyID != "" { // We use a KMS -> Store key ID and sealed KMS data key.
		metadata[MetaKeyID] = keyID
		metadata[MetaDataEncryptionKey] = base64.StdEncoding.EncodeToString(kmsKey)
	}
	return metadata
}

// ParseMetadata extracts all SSE-KMS related values from the object metadata
// and checks whether they are well-formed. It returns the sealed object key
// on success. If the metadata contains both, a KMS master key ID and a sealed
// KMS data key it returns both. If the metadata does not contain neither a
// KMS master key ID nor a sealed KMS data key it returns an empty keyID and
// KMS data key. Otherwise, it returns an error.
func (ssekms) ParseMetadata(metadata map[string]string) (keyID string, kmsKey []byte, sealedKey SealedKey, err error) {
	// Extract all required values from object metadata
	b64IV, ok := metadata[MetaIV]
	if !ok {
		return keyID, kmsKey, sealedKey, errMissingInternalIV
	}
	algorithm, ok := metadata[MetaAlgorithm]
	if !ok {
		return keyID, kmsKey, sealedKey, errMissingInternalSealAlgorithm
	}
	b64SealedKey, ok := metadata[MetaSealedKeyKMS]
	if !ok {
		return keyID, kmsKey, sealedKey, Errorf("The object metadata is missing the internal sealed key for SSE-S3")
	}

	// There are two possibilites:
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
