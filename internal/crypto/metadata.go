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
	xhttp "github.com/minio/minio/internal/http"
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

	// MetaSsecCRC is the encrypted checksum of the SSE-C encrypted object.
	MetaSsecCRC = "X-Minio-Replication-Ssec-Crc"

	// MetaContext is the KMS context provided by a client when encrypting an
	// object with SSE-KMS. A client may not send a context in which case the
	// MetaContext will not be present.
	// MetaContext only contains the bucket/object name if the client explicitly
	// added it. However, when decrypting an object the bucket/object name must
	// be part of the object. Therefore, the bucket/object name must be added
	// to the context, if not present, whenever a decryption is performed.
	MetaContext = "X-Minio-Internal-Server-Side-Encryption-Context"

	// ARNPrefix prefix for "arn:aws:kms"
	ARNPrefix = "arn:aws:kms:"
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
	delete(metadata, MetaSsecCRC)
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
// and returns modified metadata. It allocates a new metadata map if
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
