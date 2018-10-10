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
	"errors"
	"io"
	"net/http"
	"path"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/ioutil"
	"github.com/minio/sio"
)

const (
	// SSEMultipart is the metadata key indicating that the object
	// was uploaded using the S3 multipart API and stored using
	// some from of server-side-encryption.
	SSEMultipart = "X-Minio-Internal-Encrypted-Multipart"

	// SSEIV is the metadata key referencing the random initialization
	// vector (IV) used for SSE-S3 and SSE-C key derivation.
	SSEIV = "X-Minio-Internal-Server-Side-Encryption-Iv"

	// SSESealAlgorithm is the metadata key referencing the algorithm
	// used by SSE-C and SSE-S3 to encrypt the object.
	SSESealAlgorithm = "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm"

	// SSECSealedKey is the metadata key referencing the sealed object-key for SSE-C.
	SSECSealedKey = "X-Minio-Internal-Server-Side-Encryption-Sealed-Key"

	// S3SealedKey is the metadata key referencing the sealed object-key for SSE-S3.
	S3SealedKey = "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key"

	// S3KMSKeyID is the metadata key referencing the KMS key-id used to
	// generate/decrypt the S3-KMS-Sealed-Key. It is only used for SSE-S3 + KMS.
	S3KMSKeyID = "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id"

	// S3KMSSealedKey is the metadata key referencing the encrypted key generated
	// by KMS. It is only used for SSE-S3 + KMS.
	S3KMSSealedKey = "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key"
)

const (
	// SealAlgorithm is the encryption/sealing algorithm used to derive & seal
	// the key-encryption-key and to en/decrypt the object data.
	SealAlgorithm = "DAREv2-HMAC-SHA256"

	// InsecureSealAlgorithm is the legacy encryption/sealing algorithm used
	// to derive & seal the key-encryption-key and to en/decrypt the object data.
	// This algorithm should not be used for new objects because its key derivation
	// is not optimal. See: https://github.com/minio/minio/pull/6121
	InsecureSealAlgorithm = "DARE-SHA256"
)

// String returns the SSE domain as string. For SSE-S3 the
// domain is "SSE-S3".
func (s3) String() string { return "SSE-S3" }

// UnsealObjectKey extracts and decrypts the sealed object key
// from the metadata using KMS and returns the decrypted object
// key.
func (sse s3) UnsealObjectKey(kms KMS, metadata map[string]string, bucket, object string) (key ObjectKey, err error) {
	keyID, kmsKey, sealedKey, err := sse.ParseMetadata(metadata)
	if err != nil {
		return
	}
	unsealKey, err := kms.UnsealKey(keyID, kmsKey, Context{bucket: path.Join(bucket, object)})
	if err != nil {
		return
	}
	err = key.Unseal(unsealKey, sealedKey, sse.String(), bucket, object)
	return
}

// String returns the SSE domain as string. For SSE-C the
// domain is "SSE-C".
func (ssec) String() string { return "SSE-C" }

// UnsealObjectKey extracts and decrypts the sealed object key
// from the metadata using the SSE-C client key of the HTTP headers
// and returns the decrypted object key.
func (sse ssec) UnsealObjectKey(h http.Header, metadata map[string]string, bucket, object string) (key ObjectKey, err error) {
	clientKey, err := sse.ParseHTTP(h)
	if err != nil {
		return
	}
	return unsealObjectKey(clientKey, metadata, bucket, object)
}

// UnsealObjectKey extracts and decrypts the sealed object key
// from the metadata using the SSE-Copy client key of the HTTP headers
// and returns the decrypted object key.
func (sse ssecCopy) UnsealObjectKey(h http.Header, metadata map[string]string, bucket, object string) (key ObjectKey, err error) {
	clientKey, err := sse.ParseHTTP(h)
	if err != nil {
		return
	}
	return unsealObjectKey(clientKey, metadata, bucket, object)
}

// unsealObjectKey decrypts and returns the sealed object key
// from the metadata using the SSE-C client key.
func unsealObjectKey(clientKey [32]byte, metadata map[string]string, bucket, object string) (key ObjectKey, err error) {
	sealedKey, err := SSEC.ParseMetadata(metadata)
	if err != nil {
		return
	}
	err = key.Unseal(clientKey, sealedKey, SSEC.String(), bucket, object)
	return
}

// EncryptSinglePart encrypts an io.Reader which must be the
// the body of a single-part PUT request.
func EncryptSinglePart(r io.Reader, key ObjectKey) io.Reader {
	r, err := sio.EncryptReader(r, sio.Config{MinVersion: sio.Version20, Key: key[:]})
	if err != nil {
		logger.CriticalIf(context.Background(), errors.New("Unable to encrypt io.Reader using object key"))
	}
	return r
}

// EncryptMultiPart encrypts an io.Reader which must be the body of
// multi-part PUT request. It derives an unique encryption key from
// the partID and the object key.
func EncryptMultiPart(r io.Reader, partID int, key ObjectKey) io.Reader {
	partKey := key.DerivePartKey(uint32(partID))
	return EncryptSinglePart(r, ObjectKey(partKey))
}

// DecryptSinglePart decrypts an io.Writer which must an object
// uploaded with the single-part PUT API. The offset and length
// specify the requested range.
func DecryptSinglePart(w io.Writer, offset, length int64, key ObjectKey) io.WriteCloser {
	const PayloadSize = 1 << 16 // DARE 2.0
	w = ioutil.LimitedWriter(w, offset%PayloadSize, length)

	decWriter, err := sio.DecryptWriter(w, sio.Config{Key: key[:]})
	if err != nil {
		logger.CriticalIf(context.Background(), errors.New("Unable to decrypt io.Writer using object key"))
	}
	return decWriter
}
