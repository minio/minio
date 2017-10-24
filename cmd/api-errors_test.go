/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"errors"
	"testing"

	"github.com/minio/minio/pkg/hash"
)

func TestAPIErrCode(t *testing.T) {
	testCases := []struct {
		err     error
		errCode APIErrorCode
	}{
		// Valid cases.
		{
			hash.BadDigest{},
			ErrBadDigest,
		},
		{
			hash.SHA256Mismatch{},
			ErrContentSHA256Mismatch,
		},
		{
			IncompleteBody{},
			ErrIncompleteBody,
		},
		{
			ObjectExistsAsDirectory{},
			ErrObjectExistsAsDirectory,
		},
		{
			BucketNameInvalid{},
			ErrInvalidBucketName,
		},
		{
			BucketExists{},
			ErrBucketAlreadyOwnedByYou,
		},
		{
			ObjectNotFound{},
			ErrNoSuchKey,
		},
		{
			ObjectNameInvalid{},
			ErrInvalidObjectName,
		},
		{
			InvalidUploadID{},
			ErrNoSuchUpload,
		},
		{
			InvalidPart{},
			ErrInvalidPart,
		},
		{
			InsufficientReadQuorum{},
			ErrReadQuorum,
		},
		{
			InsufficientWriteQuorum{},
			ErrWriteQuorum,
		},
		{
			UnsupportedDelimiter{},
			ErrNotImplemented,
		},
		{
			InvalidMarkerPrefixCombination{},
			ErrNotImplemented,
		},
		{
			InvalidUploadIDKeyCombination{},
			ErrNotImplemented,
		},
		{
			MalformedUploadID{},
			ErrNoSuchUpload,
		},
		{
			PartTooSmall{},
			ErrEntityTooSmall,
		},
		{
			BucketNotEmpty{},
			ErrBucketNotEmpty,
		},
		{
			BucketNotFound{},
			ErrNoSuchBucket,
		},
		{
			StorageFull{},
			ErrStorageFull,
		},
		{
			NotImplemented{},
			ErrNotImplemented,
		},
		{
			errSignatureMismatch,
			ErrSignatureDoesNotMatch,
		}, // End of all valid cases.

		// Case where err is nil.
		{
			nil,
			ErrNone,
		},

		// Case where err type is unknown.
		{
			errors.New("Custom error"),
			ErrInternalError,
		},
	}

	// Validate all the errors with their API error codes.
	for i, testCase := range testCases {
		errCode := toAPIErrorCode(testCase.err)
		if errCode != testCase.errCode {
			t.Errorf("Test %d: Expected error code %d, got %d", i+1, testCase.errCode, errCode)
		}
	}
}
