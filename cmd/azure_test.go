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
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/storage"
)

// Add tests for azure to object error.
func TestAzureToObjectError(t *testing.T) {
	testCases := []struct {
		actualErr      error
		expectedErr    error
		bucket, object string
	}{
		{
			nil, nil, "", "",
		},
		{
			traceError(errUnexpected), errUnexpected, "", "",
		},
		{
			traceError(errUnexpected), traceError(errUnexpected), "", "",
		},
		{
			traceError(storage.AzureStorageServiceError{
				Code: "ContainerAlreadyExists",
			}), BucketExists{Bucket: "bucket"}, "bucket", "",
		},
		{
			traceError(storage.AzureStorageServiceError{
				Code: "InvalidResourceName",
			}), BucketNameInvalid{Bucket: "bucket."}, "bucket.", "",
		},
		{
			traceError(storage.AzureStorageServiceError{
				StatusCode: http.StatusNotFound,
			}), ObjectNotFound{
				Bucket: "bucket",
				Object: "object",
			}, "bucket", "object",
		},
		{
			traceError(storage.AzureStorageServiceError{
				StatusCode: http.StatusNotFound,
			}), BucketNotFound{Bucket: "bucket"}, "bucket", "",
		},
		{
			traceError(storage.AzureStorageServiceError{
				StatusCode: http.StatusBadRequest,
			}), BucketNameInvalid{Bucket: "bucket."}, "bucket.", "",
		},
	}
	for i, testCase := range testCases {
		err := azureToObjectError(testCase.actualErr, testCase.bucket, testCase.object)
		if err != nil {
			if err.Error() != testCase.expectedErr.Error() {
				t.Errorf("Test %d: Expected error %s, got %s", i+1, testCase.expectedErr, err)
			}
		}
	}
}
