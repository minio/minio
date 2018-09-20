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

import "errors"

// Error is the generic type for any error happening during decrypting
// an object. It indicates that the object itself or its metadata was
// modified accidentally or maliciously.
type Error struct{ msg string }

func (e Error) Error() string { return e.msg }

var (
	// ErrInvalidEncryptionMethod indicates that the specified SSE encryption method
	// is not supported.
	ErrInvalidEncryptionMethod = errors.New("The encryption method is not supported")

	// ErrInvalidCustomerAlgorithm indicates that the specified SSE-C algorithm
	// is not supported.
	ErrInvalidCustomerAlgorithm = errors.New("The SSE-C algorithm is not supported")

	// ErrMissingCustomerKey indicates that the HTTP headers contains no SSE-C client key.
	ErrMissingCustomerKey = errors.New("The SSE-C request is missing the customer key")

	// ErrMissingCustomerKeyMD5 indicates that the HTTP headers contains no SSE-C client key
	// MD5 checksum.
	ErrMissingCustomerKeyMD5 = errors.New("The SSE-C request is missing the customer key MD5")

	// ErrInvalidCustomerKey indicates that the SSE-C client key is not valid - e.g. not a
	// base64-encoded string or not 256 bits long.
	ErrInvalidCustomerKey = errors.New("The SSE-C client key is invalid")

	// ErrSecretKeyMismatch indicates that the provided secret key (SSE-C client key / SSE-S3 KMS key)
	// does not match the secret key used during encrypting the object.
	ErrSecretKeyMismatch = errors.New("The secret key does not match the secret key used during upload")

	// ErrCustomerKeyMD5Mismatch indicates that the SSE-C key MD5 does not match the
	// computed MD5 sum. This means that the client provided either the wrong key for
	// a certain MD5 checksum or the wrong MD5 for a certain key.
	ErrCustomerKeyMD5Mismatch = errors.New("The provided SSE-C key MD5 does not match the computed MD5 of the SSE-C key")
	// ErrIncompatibleEncryptionMethod indicates that both SSE-C headers and SSE-S3 headers were specified, and are incompatible
	// The client needs to remove the SSE-S3 header or the SSE-C headers
	ErrIncompatibleEncryptionMethod = errors.New("Server side encryption specified with both SSE-C and SSE-S3 headers")
)

var (
	errMissingInternalIV            = Error{"The object metadata is missing the internal encryption IV"}
	errMissingInternalSealAlgorithm = Error{"The object metadata is missing the internal seal algorithm"}

	errInvalidInternalIV            = Error{"The internal encryption IV is malformed"}
	errInvalidInternalSealAlgorithm = Error{"The internal seal algorithm is invalid and not supported"}
)

var (
	// errOutOfEntropy indicates that the a source of randomness (PRNG) wasn't able
	// to produce enough random data. This is fatal error and should cause a panic.
	errOutOfEntropy = errors.New("Unable to read enough randomness from the system")
)
