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
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/dustin/go-humanize"

	"github.com/Azure/azure-storage-blob-go/azblob"
	minio "github.com/minio/minio/cmd"
)

func TestParseStorageEndpoint(t *testing.T) {
	testCases := []struct {
		host        string
		accountName string
		expectedURL string
		expectedErr error
	}{
		{
			"", "myaccount", "https://myaccount.blob.core.windows.net", nil,
		},
		{
			"myaccount.blob.core.usgovcloudapi.net", "myaccount", "https://myaccount.blob.core.usgovcloudapi.net", nil,
		},
		{
			"http://localhost:10000", "myaccount", "http://localhost:10000/myaccount", nil,
		},
	}
	for i, testCase := range testCases {
		endpointURL, err := parseStorageEndpoint(testCase.host, testCase.accountName)
		if err != testCase.expectedErr {
			t.Errorf("Test %d: Expected error %s, got %s", i+1, testCase.expectedErr, err)
		}
		if endpointURL.String() != testCase.expectedURL {
			t.Errorf("Test %d: Expected URL %s, got %s", i+1, testCase.expectedURL, endpointURL.String())
		}
	}
}

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
	meta, _, err := s3MetaToAzureProperties(minio.GlobalContext, headers)
	if err != nil {
		t.Fatalf("Test failed, with %s", err)
	}
	if !reflect.DeepEqual(map[string]string(meta), expectedHeaders) {
		t.Fatalf("Test failed, expected %#v, got %#v", expectedHeaders, meta)
	}
	headers = map[string]string{
		"invalid--meta": "value",
	}
	_, _, err = s3MetaToAzureProperties(minio.GlobalContext, headers)
	if err != nil {
		if _, ok := err.(minio.UnsupportedMetadata); !ok {
			t.Fatalf("Test failed with unexpected error %s, expected UnsupportedMetadata", err)
		}
	}

	headers = map[string]string{
		"content-md5": "Dce7bmCX61zvxzP5QmfelQ==",
	}
	_, props, err := s3MetaToAzureProperties(minio.GlobalContext, headers)
	if err != nil {
		t.Fatalf("Test failed, with %s", err)
	}
	if base64.StdEncoding.EncodeToString(props.ContentMD5) != headers["content-md5"] {
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
		"Content-MD5":              base64.StdEncoding.EncodeToString([]byte("base64-md5")),
		"Content-Type":             "application/javascript",
	}
	actualMeta := azurePropertiesToS3Meta(metadata, azblob.BlobHTTPHeaders{
		CacheControl:       "max-age: 3600",
		ContentDisposition: "dummy",
		ContentEncoding:    "gzip",
		ContentMD5:         []byte("base64-md5"),
		ContentType:        "application/javascript",
	}, 10)
	if !reflect.DeepEqual(actualMeta, expectedMeta) {
		t.Fatalf("Test failed, expected %#v, got %#v", expectedMeta, actualMeta)
	}
}

// Add tests for azure to object error (top level).
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
	}
	for i, testCase := range testCases {
		if err := azureToObjectError(testCase.actualErr, testCase.bucket, testCase.object); err != nil {
			if err.Error() != testCase.expectedErr.Error() {
				t.Errorf("Test %d: Expected error %s, got %s", i+1, testCase.expectedErr, err)
			}
		} else {
			if testCase.expectedErr != nil {
				t.Errorf("Test %d expected an error but one was not produced", i+1)
			}
		}
	}
}

// Add tests for azure to object error (internal).
func TestAzureCodesToObjectError(t *testing.T) {
	testCases := []struct {
		originalErr       error
		actualServiceCode string
		actualStatusCode  int
		expectedErr       error
		bucket, object    string
	}{
		{
			nil, "ContainerAlreadyExists", 0,
			minio.BucketExists{Bucket: "bucket"}, "bucket", "",
		},
		{
			nil, "InvalidResourceName", 0,
			minio.BucketNameInvalid{Bucket: "bucket."}, "bucket.", "",
		},
		{
			nil, "RequestBodyTooLarge", 0,
			minio.PartTooBig{}, "", "",
		},
		{
			nil, "InvalidMetadata", 0,
			minio.UnsupportedMetadata{}, "", "",
		},
		{
			nil, "", http.StatusNotFound,
			minio.ObjectNotFound{
				Bucket: "bucket",
				Object: "object",
			}, "bucket", "object",
		},
		{
			nil, "", http.StatusNotFound,
			minio.BucketNotFound{Bucket: "bucket"}, "bucket", "",
		},
		{
			nil, "", http.StatusBadRequest,
			minio.BucketNameInvalid{Bucket: "bucket."}, "bucket.", "",
		},
		{
			fmt.Errorf("unhandled azure error"), "", http.StatusForbidden,
			fmt.Errorf("unhandled azure error"), "", "",
		},
	}
	for i, testCase := range testCases {
		if err := azureCodesToObjectError(testCase.originalErr, testCase.actualServiceCode, testCase.actualStatusCode, testCase.bucket, testCase.object); err != nil {
			if err.Error() != testCase.expectedErr.Error() {
				t.Errorf("Test %d: Expected error %s, got %s", i+1, testCase.expectedErr, err)
			}
		} else {
			if testCase.expectedErr != nil {
				t.Errorf("Test %d expected an error but one was not produced", i+1)
			}
		}
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
		if err := checkAzureUploadID(minio.GlobalContext, uploadID); err == nil {
			t.Fatalf("%s: expected: <error>, got: <nil>", uploadID)
		}
	}

	validUploadIDs := []string{
		"1234567890abcdef",
		"1122334455667788",
	}

	for _, uploadID := range validUploadIDs {
		if err := checkAzureUploadID(minio.GlobalContext, uploadID); err != nil {
			t.Fatalf("%s: expected: <nil>, got: %s", uploadID, err)
		}
	}
}

func TestParsingUploadChunkSize(t *testing.T) {
	key := "MINIO_AZURE_CHUNK_SIZE_MB"
	invalidValues := []string{
		"",
		"0,3",
		"100.1",
		"-1",
	}

	for i, chunkValue := range invalidValues {
		os.Setenv(key, chunkValue)
		result := getUploadChunkSizeFromEnv(key, strconv.Itoa(azureDefaultUploadChunkSize/humanize.MiByte))
		if result != azureDefaultUploadChunkSize {
			t.Errorf("Test %d: expected: %d, got: %d", i+1, azureDefaultUploadChunkSize, result)
		}
	}

	validValues := []string{
		"1",
		"1.25",
		"50",
		"99",
	}
	for i, chunkValue := range validValues {
		os.Setenv(key, chunkValue)
		result := getUploadChunkSizeFromEnv(key, strconv.Itoa(azureDefaultUploadChunkSize/humanize.MiByte))
		if result == azureDefaultUploadChunkSize {
			t.Errorf("Test %d: expected: %d, got: %d", i+1, azureDefaultUploadChunkSize, result)
		}
	}

}
