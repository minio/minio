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
	xhttp "github.com/minio/minio/cmd/http"
)

const (
	// MetaMultipart indicates that the object has been uploaded
	// in multiple parts - via the S3 multipart API.
	MetaMultipart = "X-Minio-Internal-Encrypted-Multipart"

	// MetaIV is the random initialization vector (IV) used for
	// the MinIO-internal key derivation.
	MetaIV = "X-Minio-Internal-Server-Side-Encryption-Iv"

	// MetaAlgorithm is the algorithm used to derive internal keys
	// and encrypt the objects.
	MetaAlgorithm = "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm"

	// MetaSealedKeySSEC is the sealed object encryption key in case of SSE-C.
	MetaSealedKeySSEC = "X-Minio-Internal-Server-Side-Encryption-Sealed-Key"
	// MetaSealedKeyS3 is the sealed object encryption key in case of SSE-S3
	MetaSealedKeyS3 = "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key"
	// MetaSealedKeyKMS is the sealed object encryption key in case of SSE-KMS
	MetaSealedKeyKMS = "X-Minio-Internal-Server-Side-Encryption-Kms-Sealed-Key"

	// MetaKeyID is the KMS master key ID used to generate/encrypt the data
	// encryption key (DEK).
	MetaKeyID = "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id"
	// MetaDataEncryptionKey is the sealed data encryption key (DEK) received from
	// the KMS.
	MetaDataEncryptionKey = "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key"

	// MetaContext is the KMS context provided by a client when encrypting an
	// object with SSE-KMS. A client may not send a context in which case the
	// MetaContext will not be present.
	// MetaContext only contains the bucket/object name if the client explicitly
	// added it. However, when decrypting an object the bucket/object name must
	// be part of the object. Therefore, the bucket/object name must be added
	// to the context, if not present, whenever a decryption is performed.
	MetaContext = "X-Minio-Internal-Server-Side-Encryption-Context"
)

// IsMultiPart returns true if the object metadata indicates
// that it was uploaded using some form of server-side-encryption
// and the S3 multipart API.
func IsMultiPart(metadata map[string]string) bool {
	if _, ok := metadata[MetaMultipart]; ok {
		return true
	}
	return false
}

// RemoveSensitiveEntries removes confidential encryption
// information - e.g. the SSE-C key - from the metadata map.
// It has the same semantics as RemoveSensitiveHeaders.
func RemoveSensitiveEntries(metadata map[string]string) { // The functions is tested in TestRemoveSensitiveHeaders for compatibility reasons
	delete(metadata, xhttp.AmzServerSideEncryptionCustomerKey)
	delete(metadata, xhttp.AmzServerSideEncryptionCopyCustomerKey)
	delete(metadata, xhttp.AmzMetaUnencryptedContentLength)
	delete(metadata, xhttp.AmzMetaUnencryptedContentMD5)
}

// RemoveSSEHeaders removes all crypto-specific SSE
// header entries from the metadata map.
func RemoveSSEHeaders(metadata map[string]string) {
	delete(metadata, xhttp.AmzServerSideEncryption)
	delete(metadata, xhttp.AmzServerSideEncryptionKmsID)
	delete(metadata, xhttp.AmzServerSideEncryptionKmsContext)
	delete(metadata, xhttp.AmzServerSideEncryptionCustomerAlgorithm)
	delete(metadata, xhttp.AmzServerSideEncryptionCustomerKey)
	delete(metadata, xhttp.AmzServerSideEncryptionCustomerKeyMD5)
	delete(metadata, xhttp.AmzServerSideEncryptionCopyCustomerAlgorithm)
	delete(metadata, xhttp.AmzServerSideEncryptionCopyCustomerKey)
	delete(metadata, xhttp.AmzServerSideEncryptionCopyCustomerKeyMD5)
}

// RemoveInternalEntries removes all crypto-specific internal
// metadata entries from the metadata map.
func RemoveInternalEntries(metadata map[string]string) {
	delete(metadata, MetaMultipart)
	delete(metadata, MetaAlgorithm)
	delete(metadata, MetaIV)
	delete(metadata, MetaSealedKeySSEC)
	delete(metadata, MetaSealedKeyS3)
	delete(metadata, MetaSealedKeyKMS)
	delete(metadata, MetaKeyID)
	delete(metadata, MetaDataEncryptionKey)
}

// IsSourceEncrypted returns true if the source is encrypted
func IsSourceEncrypted(metadata map[string]string) bool {
	if _, ok := metadata[xhttp.AmzServerSideEncryptionCustomerAlgorithm]; ok {
		return true
	}
	if _, ok := metadata[xhttp.AmzServerSideEncryption]; ok {
		return true
	}
	return false
}

// IsEncrypted returns true if the object metadata indicates
// that it was uploaded using some form of server-side-encryption.
//
// IsEncrypted only checks whether the metadata contains at least
// one entry indicating SSE-C or SSE-S3.
func IsEncrypted(metadata map[string]string) (Type, bool) {
	if S3KMS.IsEncrypted(metadata) {
		return S3KMS, true
	}
	if S3.IsEncrypted(metadata) {
		return S3, true
	}
	if SSEC.IsEncrypted(metadata) {
		return SSEC, true
	}
	if IsMultiPart(metadata) {
		return nil, true
	}
	if _, ok := metadata[MetaIV]; ok {
		return nil, true
	}
	if _, ok := metadata[MetaAlgorithm]; ok {
		return nil, true
	}
	if _, ok := metadata[MetaKeyID]; ok {
		return nil, true
	}
	if _, ok := metadata[MetaDataEncryptionKey]; ok {
		return nil, true
	}
	if _, ok := metadata[MetaContext]; ok {
		return nil, true
	}
	return nil, false
}

// CreateMultipartMetadata adds the multipart flag entry to metadata
// and returns modifed metadata. It allocates a new metadata map if
// metadata is nil.
func CreateMultipartMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return map[string]string{MetaMultipart: ""}
	}
	metadata[MetaMultipart] = ""
	return metadata
}

// IsETagSealed returns true if the etag seems to be encrypted.
func IsETagSealed(etag []byte) bool { return len(etag) > 16 }
