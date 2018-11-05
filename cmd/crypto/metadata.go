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
	"context"
	"encoding/base64"
	"fmt"

	"github.com/minio/minio/cmd/logger"
)

// IsMultiPart returns true if the object metadata indicates
// that it was uploaded using some form of server-side-encryption
// and the S3 multipart API.
func IsMultiPart(metadata map[string]string) bool {
	if _, ok := metadata[SSEMultipart]; ok {
		return true
	}
	return false
}

// RemoveSensitiveEntries removes confidential encryption
// information - e.g. the SSE-C key - from the metadata map.
// It has the same semantics as RemoveSensitiveHeaders.
func RemoveSensitiveEntries(metadata map[string]string) { // The functions is tested in TestRemoveSensitiveHeaders for compatibility reasons
	delete(metadata, SSECKey)
	delete(metadata, SSECopyKey)
}

// RemoveInternalEntries removes all crypto-specific internal
// metadata entries from the metadata map.
func RemoveInternalEntries(metadata map[string]string) {
	delete(metadata, SSEMultipart)
	delete(metadata, SSEIV)
	delete(metadata, SSESealAlgorithm)
	delete(metadata, SSECSealedKey)
	delete(metadata, S3SealedKey)
	delete(metadata, S3KMSKeyID)
	delete(metadata, S3KMSSealedKey)
}

// IsEncrypted returns true if the object metadata indicates
// that it was uploaded using some form of server-side-encryption.
//
// IsEncrypted only checks whether the metadata contains at least
// one entry indicating SSE-C or SSE-S3.
func IsEncrypted(metadata map[string]string) bool {
	if _, ok := metadata[SSEIV]; ok {
		return true
	}
	if _, ok := metadata[SSESealAlgorithm]; ok {
		return true
	}
	if IsMultiPart(metadata) {
		return true
	}
	if S3.IsEncrypted(metadata) {
		return true
	}
	if SSEC.IsEncrypted(metadata) {
		return true
	}
	return false
}

// IsEncrypted returns true if the object metadata indicates
// that the object was uploaded using SSE-S3.
func (s3) IsEncrypted(metadata map[string]string) bool {
	if _, ok := metadata[S3SealedKey]; ok {
		return true
	}
	if _, ok := metadata[S3KMSKeyID]; ok {
		return true
	}
	if _, ok := metadata[S3KMSSealedKey]; ok {
		return true
	}
	return false
}

// IsEncrypted returns true if the object metadata indicates
// that the object was uploaded using SSE-C.
func (ssec) IsEncrypted(metadata map[string]string) bool {
	if _, ok := metadata[SSECSealedKey]; ok {
		return true
	}
	return false
}

// CreateMultipartMetadata adds the multipart flag entry to metadata
// and returns modifed metadata. It allocates a new metadata map if
// metadata is nil.
func CreateMultipartMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata[SSEMultipart] = ""
	return metadata
}

// CreateMetadata encodes the keyID, the sealed kms data key and the sealed key
// into the metadata and returns the modified metadata. It allocates a new
// metadata map if metadata is nil.
func (s3) CreateMetadata(metadata map[string]string, keyID string, kmsKey []byte, sealedKey SealedKey) map[string]string {
	if sealedKey.Algorithm != SealAlgorithm {
		logger.CriticalIf(context.Background(), fmt.Errorf("The seal algorithm '%s' is invalid for SSE-S3", sealedKey.Algorithm))
	}

	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata[S3KMSKeyID] = keyID
	metadata[SSESealAlgorithm] = sealedKey.Algorithm
	metadata[SSEIV] = base64.StdEncoding.EncodeToString(sealedKey.IV[:])
	metadata[S3SealedKey] = base64.StdEncoding.EncodeToString(sealedKey.Key[:])
	metadata[S3KMSSealedKey] = base64.StdEncoding.EncodeToString(kmsKey)
	return metadata
}

// ParseMetadata extracts all SSE-S3 related values from the object metadata
// and checks whether they are well-formed. It returns the KMS key-ID, the
// sealed KMS key and the sealed object key on success.
func (s3) ParseMetadata(metadata map[string]string) (keyID string, kmsKey []byte, sealedKey SealedKey, err error) {
	// Extract all required values from object metadata
	b64IV, ok := metadata[SSEIV]
	if !ok {
		return keyID, kmsKey, sealedKey, errMissingInternalIV
	}
	algorithm, ok := metadata[SSESealAlgorithm]
	if !ok {
		return keyID, kmsKey, sealedKey, errMissingInternalSealAlgorithm
	}
	b64SealedKey, ok := metadata[S3SealedKey]
	if !ok {
		return keyID, kmsKey, sealedKey, Error{"The object metadata is missing the internal sealed key for SSE-S3"}
	}
	keyID, ok = metadata[S3KMSKeyID]
	if !ok {
		return keyID, kmsKey, sealedKey, Error{"The object metadata is missing the internal KMS key-ID for SSE-S3"}
	}
	b64KMSSealedKey, ok := metadata[S3KMSSealedKey]
	if !ok {
		return keyID, kmsKey, sealedKey, Error{"The object metadata is missing the internal sealed KMS data key for SSE-S3"}
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
		return keyID, kmsKey, sealedKey, Error{"The internal sealed key for SSE-S3 is invalid"}
	}
	kmsKey, err = base64.StdEncoding.DecodeString(b64KMSSealedKey)
	if err != nil {
		return keyID, kmsKey, sealedKey, Error{"The internal sealed KMS data key for SSE-S3 is invalid"}
	}

	sealedKey.Algorithm = algorithm
	copy(sealedKey.IV[:], iv)
	copy(sealedKey.Key[:], encryptedKey)
	return keyID, kmsKey, sealedKey, nil
}

// CreateMetadata encodes the sealed key into the metadata and returns the modified metadata.
// It allocates a new metadata map if metadata is nil.
func (ssec) CreateMetadata(metadata map[string]string, sealedKey SealedKey) map[string]string {
	if sealedKey.Algorithm != SealAlgorithm {
		logger.CriticalIf(context.Background(), fmt.Errorf("The seal algorithm '%s' is invalid for SSE-C", sealedKey.Algorithm))
	}

	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata[SSESealAlgorithm] = SealAlgorithm
	metadata[SSEIV] = base64.StdEncoding.EncodeToString(sealedKey.IV[:])
	metadata[SSECSealedKey] = base64.StdEncoding.EncodeToString(sealedKey.Key[:])
	return metadata
}

// ParseMetadata extracts all SSE-C related values from the object metadata
// and checks whether they are well-formed. It returns the sealed object key
// on success.
func (ssec) ParseMetadata(metadata map[string]string) (sealedKey SealedKey, err error) {
	// Extract all required values from object metadata
	b64IV, ok := metadata[SSEIV]
	if !ok {
		return sealedKey, errMissingInternalIV
	}
	algorithm, ok := metadata[SSESealAlgorithm]
	if !ok {
		return sealedKey, errMissingInternalSealAlgorithm
	}
	b64SealedKey, ok := metadata[SSECSealedKey]
	if !ok {
		return sealedKey, Error{"The object metadata is missing the internal sealed key for SSE-C"}
	}

	// Check whether all extracted values are well-formed
	iv, err := base64.StdEncoding.DecodeString(b64IV)
	if err != nil || len(iv) != 32 {
		return sealedKey, errInvalidInternalIV
	}
	if algorithm != SealAlgorithm && algorithm != InsecureSealAlgorithm {
		return sealedKey, errInvalidInternalSealAlgorithm
	}
	encryptedKey, err := base64.StdEncoding.DecodeString(b64SealedKey)
	if err != nil || len(encryptedKey) != 64 {
		return sealedKey, Error{"The internal sealed key for SSE-C is invalid"}
	}

	sealedKey.Algorithm = algorithm
	copy(sealedKey.IV[:], iv)
	copy(sealedKey.Key[:], encryptedKey)
	return sealedKey, nil
}

// IsETagSealed returns true if the etag seems to be encrypted.
func IsETagSealed(etag []byte) bool { return len(etag) > 16 }
