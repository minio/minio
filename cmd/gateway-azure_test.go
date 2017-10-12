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
	"net/url"
	"reflect"
	"testing"

	"github.com/Azure/azure-sdk-for-go/storage"
)

// Test azureToS3ETag.
func TestAzureToS3ETag(t *testing.T) {
	tests := []struct {
		etag     string
		expected string
	}{
		{`"etag"`, `etag-1`},
		{"etag", "etag-1"},
	}
	for i, test := range tests {
		got := azureToS3ETag(test.etag)
		if got != test.expected {
			t.Errorf("test %d: got:%s expected:%s", i+1, got, test.expected)
		}
	}
}

// Test canonical metadata.
func TestS3MetaToAzureProperties(t *testing.T) {
	headers := map[string]string{
		"accept-encoding":          "gzip",
		"content-encoding":         "gzip",
		"X-Amz-Meta-Hdr":           "value",
		"X-Amz-Meta-X_test_key":    "value",
		"X-Amz-Meta-X__test__key":  "value",
		"X-Amz-Meta-X-Test__key":   "value",
		"X-Amz-Meta-X-Amz-Key":     "hu3ZSqtqwn+aL4V2VhAeov4i+bG3KyCtRMSXQFRHXOk=",
		"X-Amz-Meta-X-Amz-Matdesc": "{}",
		"X-Amz-Meta-X-Amz-Iv":      "eWmyryl8kq+EVnnsE7jpOg==",
	}
	// Only X-Amz-Meta- prefixed entries will be returned in
	// Metadata (without the prefix!)
	expectedHeaders := map[string]string{
		"Hdr":              "value",
		"X__test__key":     "value",
		"X____test____key": "value",
		"X_Test____key":    "value",
		"X_Amz_Key":        "hu3ZSqtqwn+aL4V2VhAeov4i+bG3KyCtRMSXQFRHXOk=",
		"X_Amz_Matdesc":    "{}",
		"X_Amz_Iv":         "eWmyryl8kq+EVnnsE7jpOg==",
	}
	meta, _, err := s3MetaToAzureProperties(headers)
	if err != nil {
		t.Fatalf("Test failed, with %s", err)
	}
	if !reflect.DeepEqual(map[string]string(meta), expectedHeaders) {
		t.Fatalf("Test failed, expected %#v, got %#v", expectedHeaders, meta)
	}
	headers = map[string]string{
		"invalid--meta": "value",
	}
	_, _, err = s3MetaToAzureProperties(headers)
	if err = errorCause(err); err != nil {
		if _, ok := err.(UnsupportedMetadata); !ok {
			t.Fatalf("Test failed with unexpected error %s, expected UnsupportedMetadata", err)
		}
	}
}

func TestAzurePropertiesToS3Meta(t *testing.T) {
	// Just one testcase. Adding more test cases does not add value to the testcase
	// as azureToS3Metadata() just adds a prefix.
	metadata := map[string]string{
		"first_name":       "myname",
		"x_test_key":       "value",
		"x_test__key":      "value",
		"x__test__key":     "value",
		"x____test____key": "value",
		"x_amz_key":        "hu3ZSqtqwn+aL4V2VhAeov4i+bG3KyCtRMSXQFRHXOk=",
		"x_amz_matdesc":    "{}",
		"x_amz_iv":         "eWmyryl8kq+EVnnsE7jpOg==",
	}
	expectedMeta := map[string]string{
		"X-Amz-Meta-First-Name":    "myname",
		"X-Amz-Meta-X-Test-Key":    "value",
		"X-Amz-Meta-X-Test_key":    "value",
		"X-Amz-Meta-X_test_key":    "value",
		"X-Amz-Meta-X__test__key":  "value",
		"X-Amz-Meta-X-Amz-Key":     "hu3ZSqtqwn+aL4V2VhAeov4i+bG3KyCtRMSXQFRHXOk=",
		"X-Amz-Meta-X-Amz-Matdesc": "{}",
		"X-Amz-Meta-X-Amz-Iv":      "eWmyryl8kq+EVnnsE7jpOg==",
	}
	actualMeta := azurePropertiesToS3Meta(metadata, storage.BlobProperties{})
	if !reflect.DeepEqual(actualMeta, expectedMeta) {
		t.Fatalf("Test failed, expected %#v, got %#v", expectedMeta, actualMeta)
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
		partID        int
		subPartNumber int
		uploadID      string
		md5           string
		blockID       string
	}{
		{1, 7, "f328c35cad938137", "d41d8cd98f00b204e9800998ecf8427e", "MDAwMDEuMDcuZjMyOGMzNWNhZDkzODEzNy5kNDFkOGNkOThmMDBiMjA0ZTk4MDA5OThlY2Y4NDI3ZQ=="},
		{2, 19, "abcdc35cad938137", "a7fb6b7b36ee4ed66b5546fac4690273", "MDAwMDIuMTkuYWJjZGMzNWNhZDkzODEzNy5hN2ZiNmI3YjM2ZWU0ZWQ2NmI1NTQ2ZmFjNDY5MDI3Mw=="},
	}
	for _, test := range testCases {
		blockID := azureGetBlockID(test.partID, test.subPartNumber, test.uploadID, test.md5)
		if blockID != test.blockID {
			t.Fatalf("%s is not equal to %s", blockID, test.blockID)
		}
	}
}

// Test azureParseBlockID().
func TestAzureParseBlockID(t *testing.T) {
	testCases := []struct {
		blockID       string
		partID        int
		subPartNumber int
		uploadID      string
		md5           string
	}{
		{"MDAwMDEuMDcuZjMyOGMzNWNhZDkzODEzNy5kNDFkOGNkOThmMDBiMjA0ZTk4MDA5OThlY2Y4NDI3ZQ==", 1, 7, "f328c35cad938137", "d41d8cd98f00b204e9800998ecf8427e"},
		{"MDAwMDIuMTkuYWJjZGMzNWNhZDkzODEzNy5hN2ZiNmI3YjM2ZWU0ZWQ2NmI1NTQ2ZmFjNDY5MDI3Mw==", 2, 19, "abcdc35cad938137", "a7fb6b7b36ee4ed66b5546fac4690273"},
	}
	for _, test := range testCases {
		partID, subPartNumber, uploadID, md5, err := azureParseBlockID(test.blockID)
		if err != nil {
			t.Fatal(err)
		}
		if partID != test.partID {
			t.Fatalf("%d not equal to %d", partID, test.partID)
		}
		if subPartNumber != test.subPartNumber {
			t.Fatalf("%d not equal to %d", subPartNumber, test.subPartNumber)
		}
		if uploadID != test.uploadID {
			t.Fatalf("%s not equal to %s", uploadID, test.uploadID)
		}
		if md5 != test.md5 {
			t.Fatalf("%s not equal to %s", md5, test.md5)
		}
	}

	_, _, _, _, err := azureParseBlockID("junk")
	if err == nil {
		t.Fatal("Expected azureParseBlockID() to return error")
	}
}

// Test azureListBlobsGetParameters()
func TestAzureListBlobsGetParameters(t *testing.T) {

	// Test values set 1
	expectedURLValues := url.Values{}
	expectedURLValues.Set("prefix", "test")
	expectedURLValues.Set("delimiter", "_")
	expectedURLValues.Set("marker", "marker")
	expectedURLValues.Set("include", "metadata")
	expectedURLValues.Set("maxresults", "20")
	expectedURLValues.Set("timeout", "10")

	setBlobParameters := storage.ListBlobsParameters{
		Prefix:     "test",
		Delimiter:  "_",
		Marker:     "marker",
		Include:    &storage.IncludeBlobDataset{Metadata: true},
		MaxResults: 20,
		Timeout:    10,
	}

	// Test values set 2
	expectedURLValues1 := url.Values{}

	setBlobParameters1 := storage.ListBlobsParameters{
		Prefix:     "",
		Delimiter:  "",
		Marker:     "",
		Include:    nil,
		MaxResults: 0,
		Timeout:    0,
	}

	testCases := []struct {
		name string
		args storage.ListBlobsParameters
		want url.Values
	}{
		{"TestIfValuesSet", setBlobParameters, expectedURLValues},
		{"TestIfValuesNotSet", setBlobParameters1, expectedURLValues1},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			if got := azureListBlobsGetParameters(test.args); !reflect.DeepEqual(got, test.want) {
				t.Errorf("azureListBlobsGetParameters() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestAnonErrToObjectErr(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
		params     []string
		wantErr    error
	}{
		{"ObjectNotFound",
			http.StatusNotFound,
			[]string{"testBucket", "testObject"},
			ObjectNotFound{Bucket: "testBucket", Object: "testObject"},
		},
		{"BucketNotFound",
			http.StatusNotFound,
			[]string{"testBucket", ""},
			BucketNotFound{Bucket: "testBucket"},
		},
		{"ObjectNameInvalid",
			http.StatusBadRequest,
			[]string{"testBucket", "testObject"},
			ObjectNameInvalid{Bucket: "testBucket", Object: "testObject"},
		},
		{"BucketNameInvalid",
			http.StatusBadRequest,
			[]string{"testBucket", ""},
			BucketNameInvalid{Bucket: "testBucket"},
		},
		{"UnexpectedError",
			http.StatusBadGateway,
			[]string{"testBucket", "testObject"},
			errUnexpected,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			if err := anonErrToObjectErr(test.statusCode, test.params...); !reflect.DeepEqual(err, test.wantErr) {
				t.Errorf("anonErrToObjectErr() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestCheckAzureUploadID(t *testing.T) {
	invalidUploadIDs := []string{
		"123456789abcdefg",
		"hello world",
		"0x1234567890",
		"1234567890abcdef1234567890abcdef",
	}

	for _, uploadID := range invalidUploadIDs {
		if err := checkAzureUploadID(uploadID); err == nil {
			t.Fatalf("%s: expected: <error>, got: <nil>", uploadID)
		}
	}

	validUploadIDs := []string{
		"1234567890abcdef",
		"1122334455667788",
	}

	for _, uploadID := range validUploadIDs {
		if err := checkAzureUploadID(uploadID); err != nil {
			t.Fatalf("%s: expected: <nil>, got: %s", uploadID, err)
		}
	}
}
