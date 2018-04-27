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

package s3

import (
	"fmt"
	"testing"

	miniogo "github.com/minio/minio-go"
	"github.com/minio/minio/pkg/hash"

	minio "github.com/minio/minio/cmd"
)

func errResponse(code string) miniogo.ErrorResponse {
	return miniogo.ErrorResponse{
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
			expectedErr: minio.BucketAlreadyOwnedByYou{},
		},
		{
			inputErr:    errResponse("BucketNotEmpty"),
			expectedErr: minio.BucketNotEmpty{},
		},
		{
			inputErr:    errResponse("InvalidBucketName"),
			expectedErr: minio.BucketNameInvalid{},
		},
		{
			inputErr:    errResponse("NoSuchBucketPolicy"),
			expectedErr: minio.BucketPolicyNotFound{},
		},
		{
			inputErr:    errResponse("NoSuchBucket"),
			expectedErr: minio.BucketNotFound{},
		},
		// with empty Object in miniogo.ErrorRepsonse, NoSuchKey
		// is interpreted as BucketNotFound
		{
			inputErr:    errResponse("NoSuchKey"),
			expectedErr: minio.BucketNotFound{},
		},
		{
			inputErr:    errResponse("NoSuchUpload"),
			expectedErr: minio.InvalidUploadID{},
		},
		{
			inputErr:    errResponse("XMinioInvalidObjectName"),
			expectedErr: minio.ObjectNameInvalid{},
		},
		{
			inputErr:    errResponse("AccessDenied"),
			expectedErr: minio.PrefixAccessDenied{},
		},
		{
			inputErr:    errResponse("XAmzContentSHA256Mismatch"),
			expectedErr: hash.SHA256Mismatch{},
		},
		{
			inputErr:    errResponse("EntityTooSmall"),
			expectedErr: minio.PartTooSmall{},
		},
		{
			inputErr:    nil,
			expectedErr: nil,
		},
		// Special test case for NoSuchKey with object name
		{
			inputErr: miniogo.ErrorResponse{
				Code: "NoSuchKey",
			},
			expectedErr: minio.ObjectNotFound{
				Bucket: "bucket",
				Object: "object",
			},
			bucket: "bucket",
			object: "object",
		},

		// N B error values that aren't of expected types
		// should be left untouched.
		// Special test case for error that is not of type
		// miniogo.ErrorResponse
		{
			inputErr:    fmt.Errorf("not a minio.ErrorResponse"),
			expectedErr: fmt.Errorf("not a minio.ErrorResponse"),
		},
	}

	for i, tc := range testCases {
		actualErr := minio.ErrorRespToObjectError(tc.inputErr, tc.bucket, tc.object)
		if actualErr != nil && tc.expectedErr != nil && actualErr.Error() != tc.expectedErr.Error() {
			t.Errorf("Test case %d: Expected error %v but received error %v", i+1, tc.expectedErr, actualErr)
		}
	}
}
