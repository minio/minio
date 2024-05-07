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

	jsoniter "github.com/json-iterator/go"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
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
		// Return only true if the SSE header is specified and does not contain the SSE-S3 value
		return strings.ToUpper(h.Get(xhttp.AmzServerSideEncryption)) != xhttp.AmzEncryptionAES
	}
	return false
}

// ParseHTTP parses the SSE-KMS headers and returns the SSE-KMS key ID
// and the KMS context on success.
func (ssekms) ParseHTTP(h http.Header) (string, kms.Context, error) {
	if h == nil {
		return "", nil, ErrInvalidEncryptionMethod
	}

	algorithm := h.Get(xhttp.AmzServerSideEncryption)
	if algorithm != xhttp.AmzEncryptionKMS {
		return "", nil, ErrInvalidEncryptionMethod
	}

	var ctx kms.Context
	if context, ok := h[xhttp.AmzServerSideEncryptionKmsContext]; ok {
		b, err := base64.StdEncoding.DecodeString(context[0])
		if err != nil {
			return "", nil, err
		}

		json := jsoniter.ConfigCompatibleWithStandardLibrary
		if err := json.Unmarshal(b, &ctx); err != nil {
			return "", nil, err
		}
	}

	keyID := h.Get(xhttp.AmzServerSideEncryptionKmsID)
	spaces := strings.HasPrefix(keyID, " ") || strings.HasSuffix(keyID, " ")
	if spaces {
		return "", nil, ErrInvalidEncryptionKeyID
	}
	return strings.TrimPrefix(keyID, ARNPrefix), ctx, nil
}

// IsEncrypted returns true if the object metadata indicates
// that the object was uploaded using SSE-KMS.
func (ssekms) IsEncrypted(metadata map[string]string) bool {
	if _, ok := metadata[MetaSealedKeyKMS]; ok {
		return true
	}
	return false
}

// UnsealObjectKey extracts and decrypts the sealed object key
// from the metadata using KMS and returns the decrypted object
// key.
func (s3 ssekms) UnsealObjectKey(k *kms.KMS, metadata map[string]string, bucket, object string) (key ObjectKey, err error) {
	if k == nil {
		return key, Errorf("KMS not configured")
	}

	keyID, kmsKey, sealedKey, ctx, err := s3.ParseMetadata(metadata)
	if err != nil {
		return key, err
	}
	if ctx == nil {
		ctx = kms.Context{bucket: path.Join(bucket, object)}
	} else if _, ok := ctx[bucket]; !ok {
		ctx[bucket] = path.Join(bucket, object)
	}
	unsealKey, err := k.Decrypt(context.TODO(), &kms.DecryptRequest{
		Name:           keyID,
		Ciphertext:     kmsKey,
		AssociatedData: ctx,
	})
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
func (ssekms) CreateMetadata(metadata map[string]string, keyID string, kmsKey []byte, sealedKey SealedKey, ctx kms.Context) map[string]string {
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
	metadata[MetaSealedKeyKMS] = base64.StdEncoding.EncodeToString(sealedKey.Key[:])
	if len(ctx) > 0 {
		b, _ := ctx.MarshalText()
		metadata[MetaContext] = base64.StdEncoding.EncodeToString(b)
	}
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
func (ssekms) ParseMetadata(metadata map[string]string) (keyID string, kmsKey []byte, sealedKey SealedKey, ctx kms.Context, err error) {
	// Extract all required values from object metadata
	b64IV, ok := metadata[MetaIV]
	if !ok {
		return keyID, kmsKey, sealedKey, ctx, errMissingInternalIV
	}
	algorithm, ok := metadata[MetaAlgorithm]
	if !ok {
		return keyID, kmsKey, sealedKey, ctx, errMissingInternalSealAlgorithm
	}
	b64SealedKey, ok := metadata[MetaSealedKeyKMS]
	if !ok {
		return keyID, kmsKey, sealedKey, ctx, Errorf("The object metadata is missing the internal sealed key for SSE-S3")
	}

	// There are two possibilities:
	// - We use a KMS -> There must be a key ID and a KMS data key.
	// - We use a K/V -> There must be no key ID and no KMS data key.
	// Otherwise, the metadata is corrupted.
	keyID, idPresent := metadata[MetaKeyID]
	b64KMSSealedKey, kmsKeyPresent := metadata[MetaDataEncryptionKey]
	if !idPresent && kmsKeyPresent {
		return keyID, kmsKey, sealedKey, ctx, Errorf("The object metadata is missing the internal KMS key-ID for SSE-S3")
	}
	if idPresent && !kmsKeyPresent {
		return keyID, kmsKey, sealedKey, ctx, Errorf("The object metadata is missing the internal sealed KMS data key for SSE-S3")
	}

	// Check whether all extracted values are well-formed
	iv, err := base64.StdEncoding.DecodeString(b64IV)
	if err != nil || len(iv) != 32 {
		return keyID, kmsKey, sealedKey, ctx, errInvalidInternalIV
	}
	if algorithm != SealAlgorithm {
		return keyID, kmsKey, sealedKey, ctx, errInvalidInternalSealAlgorithm
	}
	encryptedKey, err := base64.StdEncoding.DecodeString(b64SealedKey)
	if err != nil || len(encryptedKey) != 64 {
		return keyID, kmsKey, sealedKey, ctx, Errorf("The internal sealed key for SSE-KMS is invalid")
	}
	if idPresent && kmsKeyPresent { // We are using a KMS -> parse the sealed KMS data key.
		kmsKey, err = base64.StdEncoding.DecodeString(b64KMSSealedKey)
		if err != nil {
			return keyID, kmsKey, sealedKey, ctx, Errorf("The internal sealed KMS data key for SSE-KMS is invalid")
		}
	}
	b64Ctx, ok := metadata[MetaContext]
	if ok {
		b, err := base64.StdEncoding.DecodeString(b64Ctx)
		if err != nil {
			return keyID, kmsKey, sealedKey, ctx, Errorf("The internal KMS context is not base64-encoded")
		}
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		if err = json.Unmarshal(b, &ctx); err != nil {
			return keyID, kmsKey, sealedKey, ctx, Errorf("The internal sealed KMS context is invalid %w", err)
		}
	}

	sealedKey.Algorithm = algorithm
	copy(sealedKey.IV[:], iv)
	copy(sealedKey.Key[:], encryptedKey)
	return keyID, kmsKey, sealedKey, ctx, nil
}
