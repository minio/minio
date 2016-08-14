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

import "testing"

func TestAPIErrCode(t *testing.T) {
	testCases := []struct {
		err     error
		errCode string
	}{
		// Valid cases.
		{
			eBadDigest("bucket", "bucket"),
			ErrBadDigest,
		},
		{
			eIncompleteBody(),
			ErrIncompleteBody,
		},
		{
			eObjectExistsAsDirectory("bucket", "bucket"),
			ErrObjectExistsAsDirectory,
		},
		{
			eBucketNameInvalid("bucket"),
			ErrInvalidBucketName,
		},
		{
			eBucketExists("bucket"),
			ErrBucketAlreadyOwnedByYou,
		},
		{
			eObjectNotFound("bucket", "bucket"),
			ErrNoSuchKey,
		},
		{
			eObjectNameInvalid("bucket", "bucket"),
			ErrInvalidObjectName,
		},
		{
			eInvalidUploadID("bucket", "bucket", "bucket"),
			ErrNoSuchUpload,
		},
		{
			eInvalidPart("bucket", "bucket", "bucket", 0),
			ErrInvalidPart,
		},
		{
			eInsufficientReadQuorum(),
			ErrReadQuorum,
		},
		{
			eInsufficientWriteQuorum(),
			ErrWriteQuorum,
		},
		{
			eUnsupportedDelimiter("%"),
			ErrUnsupportedDelimiter,
		},
		{
			eInvalidMarkerPrefixCombination("bucket", "bucket"),
			ErrInvalidMarkerKeyCombination,
		},
		{
			eInvalidUploadIDKeyCombination("bucket", "bucket"),
			ErrInvalidUploadIDKeyCombination,
		},
		{
			eMalformedUploadID("bucket", "bucket", "bucket"),
			ErrNoSuchUpload,
		},
		{
			ePartTooSmall(0, 0, 0, "bucket"),
			ErrEntityTooSmall,
		},
		{
			eBucketNotEmpty("bucket"),
			ErrBucketNotEmpty,
		},
		{
			eBucketNotFound("bucket"),
			ErrNoSuchBucket,
		},
		{
			eStorageFull(),
			ErrStorageFull,
		},
		{
			eSignatureDoesNotMatch("bucket", "bucket", "bucket",
				"bucket", "bucket", "bucket"),
			ErrSignatureDoesNotMatch,
		}, // End of all valid cases.
	}

	// Validate all the errors with their API error codes.
	for i, testCase := range testCases {
		if testCase.err != nil {
			aerr, ok := testCase.err.(APIError)
			if !ok {
				t.Fatal("Unable to validate the error response")
			}
			if aerr.Code() != testCase.errCode {
				t.Errorf("Test %d: Expected error code %s, got %s", i+1, testCase.errCode, aerr.Code())
			}
		}
	}
}
