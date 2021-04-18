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

package s3

import (
	"fmt"
	"testing"

	miniogo "github.com/minio/minio-go/v7"
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
			inputErr:    errResponse("InvalidPart"),
			expectedErr: minio.InvalidPart{},
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
			inputErr:    fmt.Errorf("not a ErrorResponse"),
			expectedErr: fmt.Errorf("not a ErrorResponse"),
		},
	}

	for i, tc := range testCases {
		actualErr := minio.ErrorRespToObjectError(tc.inputErr, tc.bucket, tc.object)
		if actualErr != nil && tc.expectedErr != nil && actualErr.Error() != tc.expectedErr.Error() {
			t.Errorf("Test case %d: Expected error %v but received error %v", i+1, tc.expectedErr, actualErr)
		}
	}
}
