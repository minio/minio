/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

	minio "github.com/minio/minio-go"
	"github.com/minio/minio/pkg/hash"
)

func errResponse(code string) minio.ErrorResponse {
	return minio.ErrorResponse{
		Code: code,
	}
}

func TestS3ToObjectError(t *testing.T) {
	testCases := []struct {
		inputErr       error
		expectedErr    error
		bucket, object string
	}{
		{
			inputErr:    errResponse("BucketAlreadyOwnedByYou"),
			expectedErr: BucketAlreadyOwnedByYou{},
		},
		{
			inputErr:    errResponse("BucketNotEmpty"),
			expectedErr: BucketNotEmpty{},
		},
		{
			inputErr:    errResponse("InvalidBucketName"),
			expectedErr: BucketNameInvalid{},
		},
		{
			inputErr:    errResponse("NoSuchBucketPolicy"),
			expectedErr: PolicyNotFound{},
		},
		{
			inputErr:    errResponse("NoSuchBucket"),
			expectedErr: BucketNotFound{},
		},
		// with empty Object in minio.ErrorRepsonse, NoSuchKey
		// is interpreted as BucketNotFound
		{
			inputErr:    errResponse("NoSuchKey"),
			expectedErr: BucketNotFound{},
		},
		{
			inputErr:    errResponse("NoSuchUpload"),
			expectedErr: InvalidUploadID{},
		},
		{
			inputErr:    errResponse("XMinioInvalidObjectName"),
			expectedErr: ObjectNameInvalid{},
		},
		{
			inputErr:    errResponse("AccessDenied"),
			expectedErr: PrefixAccessDenied{},
		},
		{
			inputErr:    errResponse("XAmzContentSHA256Mismatch"),
			expectedErr: hash.SHA256Mismatch{},
		},
		{
			inputErr:    errResponse("EntityTooSmall"),
			expectedErr: PartTooSmall{},
		},
		{
			inputErr:    nil,
			expectedErr: nil,
		},
		// Special test case for NoSuchKey with object name
		{
			inputErr: minio.ErrorResponse{
				Code: "NoSuchKey",
			},
			expectedErr: ObjectNotFound{},
			bucket:      "bucket",
			object:      "obbject",
		},

		// N B error values that aren't of expected types
		// should be left untouched.
		// Special test case for error that is not of type
		// minio.ErrorResponse
		{
			inputErr:    errors.New("not a minio.ErrorResponse"),
			expectedErr: errors.New("not a minio.ErrorResponse"),
		},
		// Special test case for error value that is not of
		// type (*Error)
		{
			inputErr:    errors.New("not a *Error"),
			expectedErr: errors.New("not a *Error"),
		},
	}

	for i, tc := range testCases {
		actualErr := s3ToObjectError(tc.inputErr, tc.bucket, tc.object)
		if e, ok := actualErr.(*Error); ok && e.e != tc.expectedErr {
			t.Errorf("Test case %d: Expected error %v but received error %v", i+1, tc.expectedErr, e.e)
		}
	}
}
