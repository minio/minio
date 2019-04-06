/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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

package azure

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"github.com/Azure/azure-sdk-for-go/storage"
	minio "github.com/minio/minio/cmd"
)

// Test canonical metadata.
func TestS3MetaToAzureProperties(t *testing.T) {
	headers := map[string]string{
		"accept-encoding":          "gzip",
		"content-encoding":         "gzip",
		"cache-control":            "age: 3600",
		"content-disposition":      "dummy",
		"content-length":           "10",
		"content-type":             "application/javascript",
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
	meta, _, err := s3MetaToAzureProperties(context.Background(), headers)
	if err != nil {
		t.Fatalf("Test failed, with %s", err)
	}
	if !reflect.DeepEqual(map[string]string(meta), expectedHeaders) {
		t.Fatalf("Test failed, expected %#v, got %#v", expectedHeaders, meta)
	}
	headers = map[string]string{
		"invalid--meta": "value",
	}
	_, _, err = s3MetaToAzureProperties(context.Background(), headers)
	if err != nil {
		if _, ok := err.(minio.UnsupportedMetadata); !ok {
			t.Fatalf("Test failed with unexpected error %s, expected UnsupportedMetadata", err)
		}
	}

	headers = map[string]string{
		"content-md5": "Dce7bmCX61zvxzP5QmfelQ==",
	}
	_, props, err := s3MetaToAzureProperties(context.Background(), headers)
	if err != nil {
		t.Fatalf("Test failed, with %s", err)
	}
	if props.ContentMD5 != headers["content-md5"] {
		t.Fatalf("Test failed, expected %s, got %s", headers["content-md5"], props.ContentMD5)
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
		"Cache-Control":            "max-age: 3600",
		"Content-Disposition":      "dummy",
		"Content-Encoding":         "gzip",
		"Content-Length":           "10",
		"Content-MD5":              "base64-md5",
		"Content-Type":             "application/javascript",
	}
	actualMeta := azurePropertiesToS3Meta(metadata, storage.BlobProperties{
		CacheControl:       "max-age: 3600",
		ContentDisposition: "dummy",
		ContentEncoding:    "gzip",
		ContentLength:      10,
		ContentMD5:         "base64-md5",
		ContentType:        "application/javascript",
	})
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
			fmt.Errorf("Non azure error"),
			fmt.Errorf("Non azure error"), "", "",
		},
		{
			storage.AzureStorageServiceError{
				Code: "ContainerAlreadyExists",
			}, minio.BucketExists{Bucket: "bucket"}, "bucket", "",
		},
		{
			storage.AzureStorageServiceError{
				Code: "InvalidResourceName",
			}, minio.BucketNameInvalid{Bucket: "bucket."}, "bucket.", "",
		},
		{
			storage.AzureStorageServiceError{
				Code: "RequestBodyTooLarge",
			}, minio.PartTooBig{}, "", "",
		},
		{
			storage.AzureStorageServiceError{
				Code: "InvalidMetadata",
			}, minio.UnsupportedMetadata{}, "", "",
		},
		{
			storage.AzureStorageServiceError{
				StatusCode: http.StatusNotFound,
			}, minio.ObjectNotFound{
				Bucket: "bucket",
				Object: "object",
			}, "bucket", "object",
		},
		{
			storage.AzureStorageServiceError{
				StatusCode: http.StatusNotFound,
			}, minio.BucketNotFound{Bucket: "bucket"}, "bucket", "",
		},
		{
			storage.AzureStorageServiceError{
				StatusCode: http.StatusBadRequest,
			}, minio.BucketNameInvalid{Bucket: "bucket."}, "bucket.", "",
		},
	}
	for i, testCase := range testCases {
		if err := azureToObjectError(testCase.actualErr, testCase.bucket, testCase.object); err != nil {
			if err.Error() != testCase.expectedErr.Error() {
				t.Errorf("Test %d: Expected error %s, got %s", i+1, testCase.expectedErr, err)
			}
		}
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
			minio.ObjectNotFound{Bucket: "testBucket", Object: "testObject"},
		},
		{"BucketNotFound",
			http.StatusNotFound,
			[]string{"testBucket", ""},
			minio.BucketNotFound{Bucket: "testBucket"},
		},
		{"ObjectNameInvalid",
			http.StatusBadRequest,
			[]string{"testBucket", "testObject"},
			minio.ObjectNameInvalid{Bucket: "testBucket", Object: "testObject"},
		},
		{"BucketNameInvalid",
			http.StatusBadRequest,
			[]string{"testBucket", ""},
			minio.BucketNameInvalid{Bucket: "testBucket"},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			if err := minio.AnonErrToObjectErr(test.statusCode, test.params...); !reflect.DeepEqual(err, test.wantErr) {
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
		if err := checkAzureUploadID(context.Background(), uploadID); err == nil {
			t.Fatalf("%s: expected: <error>, got: <nil>", uploadID)
		}
	}

	validUploadIDs := []string{
		"1234567890abcdef",
		"1122334455667788",
	}

	for _, uploadID := range validUploadIDs {
		if err := checkAzureUploadID(context.Background(), uploadID); err != nil {
			t.Fatalf("%s: expected: <nil>, got: %s", uploadID, err)
		}
	}
}
