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
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"net/http"

	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
)

type ssec struct{}

var (
	// SSEC represents AWS SSE-C. It provides functionality to handle
	// SSE-C requests.
	SSEC = ssec{}

	_ Type = SSEC
)

// String returns the SSE domain as string. For SSE-C the
// domain is "SSE-C".
func (ssec) String() string { return "SSE-C" }

// IsRequested returns true if the HTTP headers contains
// at least one SSE-C header. SSE-C copy headers are ignored.
func (ssec) IsRequested(h http.Header) bool {
	if _, ok := h[xhttp.AmzServerSideEncryptionCustomerAlgorithm]; ok {
		return true
	}
	if _, ok := h[xhttp.AmzServerSideEncryptionCustomerKey]; ok {
		return true
	}
	if _, ok := h[xhttp.AmzServerSideEncryptionCustomerKeyMD5]; ok {
		return true
	}
	return false
}

// IsEncrypted returns true if the metadata contains an SSE-C
// entry inidicating that the object has been encrypted using
// SSE-C.
func (ssec) IsEncrypted(metadata map[string]string) bool {
	if _, ok := metadata[MetaSealedKeySSEC]; ok {
		return true
	}
	return false
}

// ParseHTTP parses the SSE-C headers and returns the SSE-C client key
// on success. SSE-C copy headers are ignored.
func (ssec) ParseHTTP(h http.Header) (key [32]byte, err error) {
	if h.Get(xhttp.AmzServerSideEncryptionCustomerAlgorithm) != xhttp.AmzEncryptionAES {
		return key, ErrInvalidCustomerAlgorithm
	}
	if h.Get(xhttp.AmzServerSideEncryptionCustomerKey) == "" {
		return key, ErrMissingCustomerKey
	}
	if h.Get(xhttp.AmzServerSideEncryptionCustomerKeyMD5) == "" {
		return key, ErrMissingCustomerKeyMD5
	}

	clientKey, err := base64.StdEncoding.DecodeString(h.Get(xhttp.AmzServerSideEncryptionCustomerKey))
	if err != nil || len(clientKey) != 32 { // The client key must be 256 bits long
		return key, ErrInvalidCustomerKey
	}
	keyMD5, err := base64.StdEncoding.DecodeString(h.Get(xhttp.AmzServerSideEncryptionCustomerKeyMD5))
	if md5Sum := md5.Sum(clientKey); err != nil || !bytes.Equal(md5Sum[:], keyMD5) {
		return key, ErrCustomerKeyMD5Mismatch
	}
	copy(key[:], clientKey)
	return key, nil
}

// UnsealObjectKey extracts and decrypts the sealed object key
// from the metadata using the SSE-C client key of the HTTP headers
// and returns the decrypted object key.
func (s3 ssec) UnsealObjectKey(h http.Header, metadata map[string]string, bucket, object string) (key ObjectKey, err error) {
	clientKey, err := s3.ParseHTTP(h)
	if err != nil {
		return
	}
	return unsealObjectKey(clientKey, metadata, bucket, object)
}

// CreateMetadata encodes the sealed key into the metadata
// and returns the modified metadata. It allocates a new
// metadata map if metadata is nil.
func (ssec) CreateMetadata(metadata map[string]string, sealedKey SealedKey) map[string]string {
	if sealedKey.Algorithm != SealAlgorithm {
		logger.CriticalIf(context.Background(), Errorf("The seal algorithm '%s' is invalid for SSE-C", sealedKey.Algorithm))
	}

	if metadata == nil {
		metadata = make(map[string]string, 3)
	}
	metadata[MetaAlgorithm] = SealAlgorithm
	metadata[MetaIV] = base64.StdEncoding.EncodeToString(sealedKey.IV[:])
	metadata[MetaSealedKeySSEC] = base64.StdEncoding.EncodeToString(sealedKey.Key[:])
	return metadata
}

// ParseMetadata extracts all SSE-C related values from the object metadata
// and checks whether they are well-formed. It returns the sealed object key
// on success.
func (ssec) ParseMetadata(metadata map[string]string) (sealedKey SealedKey, err error) {
	// Extract all required values from object metadata
	b64IV, ok := metadata[MetaIV]
	if !ok {
		return sealedKey, errMissingInternalIV
	}
	algorithm, ok := metadata[MetaAlgorithm]
	if !ok {
		return sealedKey, errMissingInternalSealAlgorithm
	}
	b64SealedKey, ok := metadata[MetaSealedKeySSEC]
	if !ok {
		return sealedKey, Errorf("The object metadata is missing the internal sealed key for SSE-C")
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
		return sealedKey, Errorf("The internal sealed key for SSE-C is invalid")
	}

	sealedKey.Algorithm = algorithm
	copy(sealedKey.IV[:], iv)
	copy(sealedKey.Key[:], encryptedKey)
	return sealedKey, nil
}
