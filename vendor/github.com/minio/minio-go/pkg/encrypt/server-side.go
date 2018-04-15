/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2018 Minio, Inc.
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

package encrypt

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"

	"golang.org/x/crypto/argon2"
)

const (
	// sseGenericHeader is the AWS SSE header used for SSE-S3 and SSE-KMS.
	sseGenericHeader = "X-Amz-Server-Side-Encryption"

	// sseKmsKeyID is the AWS SSE-KMS key id.
	sseKmsKeyID = sseGenericHeader + "-Aws-Kms-Key-Id"
	// sseEncryptionContext is the AWS SSE-KMS Encryption Context data.
	sseEncryptionContext = sseGenericHeader + "-Encryption-Context"

	// sseCustomerAlgorithm is the AWS SSE-C algorithm HTTP header key.
	sseCustomerAlgorithm = sseGenericHeader + "-Customer-Algorithm"
	// sseCustomerKey is the AWS SSE-C encryption key HTTP header key.
	sseCustomerKey = sseGenericHeader + "-Customer-Key"
	// sseCustomerKeyMD5 is the AWS SSE-C encryption key MD5 HTTP header key.
	sseCustomerKeyMD5 = sseGenericHeader + "-Customer-Key-MD5"

	// sseCopyCustomerAlgorithm is the AWS SSE-C algorithm HTTP header key for CopyObject API.
	sseCopyCustomerAlgorithm = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm"
	// sseCopyCustomerKey is the AWS SSE-C encryption key HTTP header key for CopyObject API.
	sseCopyCustomerKey = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key"
	// sseCopyCustomerKeyMD5 is the AWS SSE-C encryption key MD5 HTTP header key for CopyObject API.
	sseCopyCustomerKeyMD5 = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-MD5"
)

// PBKDF creates a SSE-C key from the provided password and salt.
// PBKDF is a password-based key derivation function
// which can be used to derive a high-entropy cryptographic
// key from a low-entropy password and a salt.
type PBKDF func(password, salt []byte) ServerSide

// DefaultPBKDF is the default PBKDF. It uses Argon2id with the
// recommended parameters from the RFC draft (1 pass, 64 MB memory, 4 threads).
var DefaultPBKDF PBKDF = func(password, salt []byte) ServerSide {
	sse := ssec{}
	copy(sse[:], argon2.IDKey(password, salt, 1, 64*1024, 4, 32))
	return sse
}

// Type is the server-side-encryption method. It represents one of
// the following encryption methods:
//  - SSE-C: server-side-encryption with customer provided keys
//  - KMS:   server-side-encryption with managed keys
//  - S3:    server-side-encryption using S3 storage encryption
type Type string

const (
	// SSEC represents server-side-encryption with customer provided keys
	SSEC Type = "SSE-C"
	// KMS represents server-side-encryption with managed keys
	KMS Type = "KMS"
	// S3 represents server-side-encryption using S3 storage encryption
	S3 Type = "S3"
)

// ServerSide is a form of S3 server-side-encryption.
type ServerSide interface {
	// Type returns the server-side-encryption method.
	Type() Type

	// Marshal adds encryption headers to the provided HTTP headers.
	// It marks an HTTP request as server-side-encryption request
	// and inserts the required data into the headers.
	Marshal(h http.Header)
}

// NewSSE returns a server-side-encryption using S3 storage encryption.
// Using SSE-S3 the server will encrypt the object with server-managed keys.
func NewSSE() ServerSide { return s3{} }

// NewSSEKMS returns a new server-side-encryption using SSE-KMS and the provided Key Id and context.
func NewSSEKMS(keyID string, context interface{}) (ServerSide, error) {
	if context == nil {
		return kms{key: keyID, hasContext: false}, nil
	}
	serializedContext, err := json.Marshal(context)
	if err != nil {
		return nil, err
	}
	return kms{key: keyID, context: serializedContext, hasContext: true}, nil
}

// NewSSEC returns a new server-side-encryption using SSE-C and the provided key.
// The key must be 32 bytes long.
func NewSSEC(key []byte) (ServerSide, error) {
	if len(key) != 32 {
		return nil, errors.New("encrypt: SSE-C key must be 256 bit long")
	}
	sse := ssec{}
	copy(sse[:], key)
	return sse, nil
}

// SSE transforms a SSE-C copy encryption into a SSE-C encryption.
// It is the inverse of SSECopy(...).
//
// If the provided sse is no SSE-C copy encryption SSE returns
// sse unmodified.
func SSE(sse ServerSide) ServerSide {
	if sse == nil || sse.Type() != SSEC {
		return sse
	}
	if sse, ok := sse.(ssecCopy); ok {
		return ssec(sse)
	}
	return sse
}

// SSECopy transforms a SSE-C encryption into a SSE-C copy
// encryption. This is required for SSE-C key rotation or a SSE-C
// copy where the source and the destination should be encrypted.
//
// If the provided sse is no SSE-C encryption SSECopy returns
// sse unmodified.
func SSECopy(sse ServerSide) ServerSide {
	if sse == nil || sse.Type() != SSEC {
		return sse
	}
	if sse, ok := sse.(ssec); ok {
		return ssecCopy(sse)
	}
	return sse
}

type ssec [32]byte

func (s ssec) Type() Type { return SSEC }

func (s ssec) Marshal(h http.Header) {
	keyMD5 := md5.Sum(s[:])
	h.Set(sseCustomerAlgorithm, "AES256")
	h.Set(sseCustomerKey, base64.StdEncoding.EncodeToString(s[:]))
	h.Set(sseCustomerKeyMD5, base64.StdEncoding.EncodeToString(keyMD5[:]))
}

type ssecCopy [32]byte

func (s ssecCopy) Type() Type { return SSEC }

func (s ssecCopy) Marshal(h http.Header) {
	keyMD5 := md5.Sum(s[:])
	h.Set(sseCopyCustomerAlgorithm, "AES256")
	h.Set(sseCopyCustomerKey, base64.StdEncoding.EncodeToString(s[:]))
	h.Set(sseCopyCustomerKeyMD5, base64.StdEncoding.EncodeToString(keyMD5[:]))
}

type s3 struct{}

func (s s3) Type() Type { return S3 }

func (s s3) Marshal(h http.Header) { h.Set(sseGenericHeader, "AES256") }

type kms struct {
	key        string
	context    []byte
	hasContext bool
}

func (s kms) Type() Type { return KMS }

func (s kms) Marshal(h http.Header) {
	h.Set(sseGenericHeader, "aws:kms")
	h.Set(sseKmsKeyID, s.key)
	if s.hasContext {
		h.Set(sseEncryptionContext, base64.StdEncoding.EncodeToString(s.context))
	}
}
