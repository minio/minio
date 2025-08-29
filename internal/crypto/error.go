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
	"errors"
	"fmt"
)

// Error is the generic type for any error happening during decrypting
// an object. It indicates that the object itself or its metadata was
// modified accidentally or maliciously.
type Error struct {
	msg   string
	cause error
}

// Errorf - formats according to a format specifier and returns
// the string as a value that satisfies error of type crypto.Error
func Errorf(format string, a ...any) error {
	e := fmt.Errorf(format, a...)
	ee := Error{}
	ee.msg = e.Error()
	ee.cause = errors.Unwrap(e)
	return ee
}

// Unwrap the internal error.
func (e Error) Unwrap() error { return e.cause }

// Error 'error' compatible method.
func (e Error) Error() string {
	if e.msg == "" {
		return "crypto: cause <nil>"
	}
	return e.msg
}

var (
	// ErrInvalidEncryptionMethod indicates that the specified SSE encryption method
	// is not supported.
	ErrInvalidEncryptionMethod = Errorf("The encryption method is not supported")

	// ErrInvalidCustomerAlgorithm indicates that the specified SSE-C algorithm
	// is not supported.
	ErrInvalidCustomerAlgorithm = Errorf("The SSE-C algorithm is not supported")

	// ErrMissingCustomerKey indicates that the HTTP headers contains no SSE-C client key.
	ErrMissingCustomerKey = Errorf("The SSE-C request is missing the customer key")

	// ErrMissingCustomerKeyMD5 indicates that the HTTP headers contains no SSE-C client key
	// MD5 checksum.
	ErrMissingCustomerKeyMD5 = Errorf("The SSE-C request is missing the customer key MD5")

	// ErrInvalidCustomerKey indicates that the SSE-C client key is not valid - e.g. not a
	// base64-encoded string or not 256 bits long.
	ErrInvalidCustomerKey = Errorf("The SSE-C client key is invalid")

	// ErrSecretKeyMismatch indicates that the provided secret key (SSE-C client key / SSE-S3 KMS key)
	// does not match the secret key used during encrypting the object.
	ErrSecretKeyMismatch = Errorf("The secret key does not match the secret key used during upload")

	// ErrCustomerKeyMD5Mismatch indicates that the SSE-C key MD5 does not match the
	// computed MD5 sum. This means that the client provided either the wrong key for
	// a certain MD5 checksum or the wrong MD5 for a certain key.
	ErrCustomerKeyMD5Mismatch = Errorf("The provided SSE-C key MD5 does not match the computed MD5 of the SSE-C key")
	// ErrIncompatibleEncryptionMethod indicates that both SSE-C headers and SSE-S3 headers were specified, and are incompatible
	// The client needs to remove the SSE-S3 header or the SSE-C headers
	ErrIncompatibleEncryptionMethod = Errorf("Server side encryption specified with both SSE-C and SSE-S3 headers")
	// ErrIncompatibleEncryptionWithCompression indicates that both data compression and SSE-C not allowed at the same time
	ErrIncompatibleEncryptionWithCompression = Errorf("Server side encryption specified with SSE-C with compression not allowed")

	// ErrInvalidEncryptionKeyID returns error when KMS key id contains invalid characters
	ErrInvalidEncryptionKeyID = Errorf("KMS KeyID contains unsupported characters")
)

var (
	errMissingInternalIV            = Errorf("The object metadata is missing the internal encryption IV")
	errMissingInternalSealAlgorithm = Errorf("The object metadata is missing the internal seal algorithm")

	errInvalidInternalIV            = Errorf("The internal encryption IV is malformed")
	errInvalidInternalSealAlgorithm = Errorf("The internal seal algorithm is invalid and not supported")
)

// errOutOfEntropy indicates that the a source of randomness (PRNG) wasn't able
// to produce enough random data. This is fatal error and should cause a panic.
var errOutOfEntropy = Errorf("Unable to read enough randomness from the system")
