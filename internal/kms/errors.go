// Copyright (c) 2015-2023 MinIO, Inc.
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

package kms

import (
	"fmt"
	"net/http"
)

var (
	// ErrPermission is an error returned by the KMS when it has not
	// enough permissions to perform the operation.
	ErrPermission = Error{
		Code:    http.StatusForbidden,
		APICode: "kms:NotAuthorized",
		Err:     "insufficient permissions to perform KMS operation",
	}

	// ErrKeyExists is an error returned by the KMS when trying to
	// create a key that already exists.
	ErrKeyExists = Error{
		Code:    http.StatusConflict,
		APICode: "kms:KeyAlreadyExists",
		Err:     "key with given key ID already exits",
	}

	// ErrKeyNotFound is an error returned by the KMS when trying to
	// use a key that does not exist.
	ErrKeyNotFound = Error{
		Code:    http.StatusNotFound,
		APICode: "kms:KeyNotFound",
		Err:     "key with given key ID does not exist",
	}

	// ErrDecrypt is an error returned by the KMS when the decryption
	// of a ciphertext failed.
	ErrDecrypt = Error{
		Code:    http.StatusBadRequest,
		APICode: "kms:InvalidCiphertextException",
		Err:     "failed to decrypt ciphertext",
	}

	// ErrNotSupported is an error returned by the KMS when the requested
	// functionality is not supported by the KMS service.
	ErrNotSupported = Error{
		Code:    http.StatusNotImplemented,
		APICode: "kms:NotSupported",
		Err:     "requested functionality is not supported",
	}
)

// Error is a KMS error that can be translated into an S3 API error.
//
// It does not implement the standard error Unwrap interface for
// better error log messages.
type Error struct {
	Code    int    // The HTTP status code returned to the client
	APICode string // The API error code identifying the error
	Err     string // The error message returned to the client
	Cause   error  // Optional, lower level error cause.
}

func (e Error) Error() string {
	if e.Cause == nil {
		return e.Err
	}
	return fmt.Sprintf("%s: %v", e.Err, e.Cause)
}

func errKeyCreationFailed(err error) Error {
	return Error{
		Code:    http.StatusInternalServerError,
		APICode: "kms:KeyCreationFailed",
		Err:     "failed to create KMS key",
		Cause:   err,
	}
}

func errKeyDeletionFailed(err error) Error {
	return Error{
		Code:    http.StatusInternalServerError,
		APICode: "kms:KeyDeletionFailed",
		Err:     "failed to delete KMS key",
		Cause:   err,
	}
}

func errListingKeysFailed(err error) Error {
	return Error{
		Code:    http.StatusInternalServerError,
		APICode: "kms:KeyListingFailed",
		Err:     "failed to list keys at the KMS",
		Cause:   err,
	}
}

func errKeyGenerationFailed(err error) Error {
	return Error{
		Code:    http.StatusInternalServerError,
		APICode: "kms:KeyGenerationFailed",
		Err:     "failed to generate data key with KMS key",
		Cause:   err,
	}
}

func errDecryptionFailed(err error) Error {
	return Error{
		Code:    http.StatusInternalServerError,
		APICode: "kms:DecryptionFailed",
		Err:     "failed to decrypt ciphertext with KMS key",
		Cause:   err,
	}
}
