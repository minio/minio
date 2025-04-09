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

package cmd

import (
	"errors"
	"testing"

	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/hash"
)

var toAPIErrorTests = []struct {
	err     error
	errCode APIErrorCode
}{
	{err: hash.BadDigest{}, errCode: ErrBadDigest},
	{err: hash.SHA256Mismatch{}, errCode: ErrContentSHA256Mismatch},
	{err: IncompleteBody{}, errCode: ErrIncompleteBody},
	{err: ObjectExistsAsDirectory{}, errCode: ErrObjectExistsAsDirectory},
	{err: BucketNameInvalid{}, errCode: ErrInvalidBucketName},
	{err: BucketExists{}, errCode: ErrBucketAlreadyOwnedByYou},
	{err: ObjectNotFound{}, errCode: ErrNoSuchKey},
	{err: ObjectNameInvalid{}, errCode: ErrInvalidObjectName},
	{err: InvalidUploadID{}, errCode: ErrNoSuchUpload},
	{err: InvalidPart{}, errCode: ErrInvalidPart},
	{err: InsufficientReadQuorum{}, errCode: ErrSlowDownRead},
	{err: InsufficientWriteQuorum{}, errCode: ErrSlowDownWrite},
	{err: InvalidUploadIDKeyCombination{}, errCode: ErrNotImplemented},
	{err: MalformedUploadID{}, errCode: ErrNoSuchUpload},
	{err: PartTooSmall{}, errCode: ErrEntityTooSmall},
	{err: BucketNotEmpty{}, errCode: ErrBucketNotEmpty},
	{err: BucketNotFound{}, errCode: ErrNoSuchBucket},
	{err: StorageFull{}, errCode: ErrStorageFull},
	{err: NotImplemented{}, errCode: ErrNotImplemented},
	{err: errSignatureMismatch, errCode: ErrSignatureDoesNotMatch},

	// SSE-C errors
	{err: crypto.ErrInvalidCustomerAlgorithm, errCode: ErrInvalidSSECustomerAlgorithm},
	{err: crypto.ErrMissingCustomerKey, errCode: ErrMissingSSECustomerKey},
	{err: crypto.ErrInvalidCustomerKey, errCode: ErrAccessDenied},
	{err: crypto.ErrMissingCustomerKeyMD5, errCode: ErrMissingSSECustomerKeyMD5},
	{err: crypto.ErrCustomerKeyMD5Mismatch, errCode: ErrSSECustomerKeyMD5Mismatch},
	{err: errObjectTampered, errCode: ErrObjectTampered},

	{err: nil, errCode: ErrNone},
	{err: errors.New("Custom error"), errCode: ErrInternalError}, // Case where err type is unknown.
}

func TestAPIErrCode(t *testing.T) {
	ctx := t.Context()
	for i, testCase := range toAPIErrorTests {
		errCode := toAPIErrorCode(ctx, testCase.err)
		if errCode != testCase.errCode {
			t.Errorf("Test %d: Expected error code %d, got %d", i+1, testCase.errCode, errCode)
		}
	}
}

// Check if an API error is properly defined
func TestAPIErrCodeDefinition(t *testing.T) {
	for errAPI := ErrNone + 1; errAPI < apiErrCodeEnd; errAPI++ {
		errCode, ok := errorCodes[errAPI]
		if !ok {
			t.Fatal(errAPI, "error code is not defined in the API error code table")
		}
		if errCode.Code == "" {
			t.Fatal(errAPI, "error code has an empty XML code")
		}
		if errCode.HTTPStatusCode == 0 {
			t.Fatal(errAPI, "error code has a zero HTTP status code")
		}
	}
}
