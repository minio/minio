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
	"reflect"
	"testing"

	"github.com/Azure/azure-sdk-for-go/storage"
)

// Test canonical metadata.
func TestCanonicalMetadata(t *testing.T) {
	metadata := map[string]string{
		"accept-encoding":  "gzip",
		"content-encoding": "gzip",
	}
	expectedCanonicalM := map[string]string{
		"Accept-Encoding":  "gzip",
		"Content-Encoding": "gzip",
	}
	actualCanonicalM := canonicalMetadata(metadata)
	if !reflect.DeepEqual(actualCanonicalM, expectedCanonicalM) {
		t.Fatalf("Test failed, expected %#v, got %#v", expectedCanonicalM, actualCanonicalM)
	}
}

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

// Test azureGetBlockID().
func TestAzureGetBlockID(t *testing.T) {
	testCases := []struct {
		partID  int
		md5     string
		blockID string
	}{
		{1, "d41d8cd98f00b204e9800998ecf8427e", "MDAwMDEuZDQxZDhjZDk4ZjAwYjIwNGU5ODAwOTk4ZWNmODQyN2U="},
		{2, "a7fb6b7b36ee4ed66b5546fac4690273", "MDAwMDIuYTdmYjZiN2IzNmVlNGVkNjZiNTU0NmZhYzQ2OTAyNzM="},
	}
	for _, test := range testCases {
		blockID := azureGetBlockID(test.partID, test.md5)
		if blockID != test.blockID {
			t.Fatalf("%s is not equal to %s", blockID, test.blockID)
		}
	}
}

// Test azureParseBlockID().
func TestAzureParseBlockID(t *testing.T) {
	testCases := []struct {
		partID  int
		md5     string
		blockID string
	}{
		{1, "d41d8cd98f00b204e9800998ecf8427e", "MDAwMDEuZDQxZDhjZDk4ZjAwYjIwNGU5ODAwOTk4ZWNmODQyN2U="},
		{2, "a7fb6b7b36ee4ed66b5546fac4690273", "MDAwMDIuYTdmYjZiN2IzNmVlNGVkNjZiNTU0NmZhYzQ2OTAyNzM="},
	}
	for _, test := range testCases {
		partID, md5, err := azureParseBlockID(test.blockID)
		if err != nil {
			t.Fatal(err)
		}
		if partID != test.partID {
			t.Fatalf("%d not equal to %d", partID, test.partID)
		}
		if md5 != test.md5 {
			t.Fatalf("%s not equal to %s", md5, test.md5)
		}
	}
	_, _, err := azureParseBlockID("junk")
	if err == nil {
		t.Fatal("Expected azureParseBlockID() to return error")
	}
}
