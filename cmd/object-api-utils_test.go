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
	"context"
	"reflect"
	"testing"
)

// Tests getCompleteMultipartMD5
func TestGetCompleteMultipartMD5(t *testing.T) {
	testCases := []struct {
		parts          []CompletePart
		expectedResult string
		expectedErr    string
	}{
		// Wrong MD5 hash string
		{[]CompletePart{{ETag: "wrong-md5-hash-string"}}, "", "encoding/hex: invalid byte: U+0077 'w'"},

		// Single CompletePart with valid MD5 hash string.
		{[]CompletePart{{ETag: "cf1f738a5924e645913c984e0fe3d708"}}, "10dc1617fbcf0bd0858048cb96e6bd77-1", ""},

		// Multiple CompletePart with valid MD5 hash string.
		{[]CompletePart{{ETag: "cf1f738a5924e645913c984e0fe3d708"}, {ETag: "9ccbc9a80eee7fb6fdd22441db2aedbd"}}, "0239a86b5266bb624f0ac60ba2aed6c8-2", ""},
	}

	for i, test := range testCases {
		result, err := getCompleteMultipartMD5(context.Background(), test.parts)
		if result != test.expectedResult {
			t.Fatalf("test %d failed: expected: result=%v, got=%v", i+1, test.expectedResult, result)
		}
		errString := ""
		if err != nil {
			errString = err.Error()
		}
		if errString != test.expectedErr {
			t.Fatalf("test %d failed: expected: err=%v, got=%v", i+1, test.expectedErr, err)
		}
	}
}

// TestIsMinioBucketName - Tests isMinioBucketName helper function.
func TestIsMinioMetaBucketName(t *testing.T) {
	testCases := []struct {
		bucket string
		result bool
	}{
		// Minio meta bucket.
		{
			bucket: minioMetaBucket,
			result: true,
		},
		// Minio meta bucket.
		{
			bucket: minioMetaMultipartBucket,
			result: true,
		},
		// Minio meta bucket.
		{
			bucket: minioMetaTmpBucket,
			result: true,
		},
		// Normal bucket
		{
			bucket: "mybucket",
			result: false,
		},
	}

	for i, test := range testCases {
		actual := isMinioMetaBucketName(test.bucket)
		if actual != test.result {
			t.Errorf("Test %d - expected %v but received %v",
				i+1, test.result, actual)
		}
	}
}

// Tests RemoveStandardStorageClass method. Expectation is metadata map
// should be cleared of x-amz-storage-class, if it is set to STANDARD
func TestRemoveStandardStorageClass(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		want     map[string]string
	}{
		{
			name:     "1",
			metadata: map[string]string{"content-type": "application/octet-stream", "etag": "de75a98baf2c6aef435b57dd0fc33c86", "x-amz-storage-class": "STANDARD"},
			want:     map[string]string{"content-type": "application/octet-stream", "etag": "de75a98baf2c6aef435b57dd0fc33c86"},
		},
		{
			name:     "2",
			metadata: map[string]string{"content-type": "application/octet-stream", "etag": "de75a98baf2c6aef435b57dd0fc33c86", "x-amz-storage-class": "REDUCED_REDUNDANCY"},
			want:     map[string]string{"content-type": "application/octet-stream", "etag": "de75a98baf2c6aef435b57dd0fc33c86", "x-amz-storage-class": "REDUCED_REDUNDANCY"},
		},
		{
			name:     "3",
			metadata: map[string]string{"content-type": "application/octet-stream", "etag": "de75a98baf2c6aef435b57dd0fc33c86"},
			want:     map[string]string{"content-type": "application/octet-stream", "etag": "de75a98baf2c6aef435b57dd0fc33c86"},
		},
	}
	for _, tt := range tests {
		if got := removeStandardStorageClass(tt.metadata); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("Test %s failed, expected %v, got %v", tt.name, tt.want, got)
		}
	}
}

// Tests CleanMetadata method. Expectation is metadata map
// should be cleared of etag, md5Sum and x-amz-storage-class, if it is set to STANDARD
func TestCleanMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		want     map[string]string
	}{
		{
			name:     "1",
			metadata: map[string]string{"content-type": "application/octet-stream", "etag": "de75a98baf2c6aef435b57dd0fc33c86", "x-amz-storage-class": "STANDARD"},
			want:     map[string]string{"content-type": "application/octet-stream"},
		},
		{
			name:     "2",
			metadata: map[string]string{"content-type": "application/octet-stream", "etag": "de75a98baf2c6aef435b57dd0fc33c86", "x-amz-storage-class": "REDUCED_REDUNDANCY"},
			want:     map[string]string{"content-type": "application/octet-stream", "x-amz-storage-class": "REDUCED_REDUNDANCY"},
		},
		{
			name:     "3",
			metadata: map[string]string{"content-type": "application/octet-stream", "etag": "de75a98baf2c6aef435b57dd0fc33c86", "md5Sum": "abcde"},
			want:     map[string]string{"content-type": "application/octet-stream"},
		},
	}
	for _, tt := range tests {
		if got := cleanMetadata(tt.metadata); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("Test %s failed, expected %v, got %v", tt.name, tt.want, got)
		}
	}
}

// Tests CleanMetadataKeys method. Expectation is metadata map
// should be cleared of keys passed to CleanMetadataKeys method
func TestCleanMetadataKeys(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		keys     []string
		want     map[string]string
	}{
		{
			name:     "1",
			metadata: map[string]string{"content-type": "application/octet-stream", "etag": "de75a98baf2c6aef435b57dd0fc33c86", "x-amz-storage-class": "STANDARD", "md5": "abcde"},
			keys:     []string{"etag", "md5"},
			want:     map[string]string{"content-type": "application/octet-stream", "x-amz-storage-class": "STANDARD"},
		},
		{
			name:     "2",
			metadata: map[string]string{"content-type": "application/octet-stream", "etag": "de75a98baf2c6aef435b57dd0fc33c86", "x-amz-storage-class": "REDUCED_REDUNDANCY", "md5sum": "abcde"},
			keys:     []string{"etag", "md5sum"},
			want:     map[string]string{"content-type": "application/octet-stream", "x-amz-storage-class": "REDUCED_REDUNDANCY"},
		},
		{
			name:     "3",
			metadata: map[string]string{"content-type": "application/octet-stream", "etag": "de75a98baf2c6aef435b57dd0fc33c86", "xyz": "abcde"},
			keys:     []string{"etag", "xyz"},
			want:     map[string]string{"content-type": "application/octet-stream"},
		},
	}
	for _, tt := range tests {
		if got := cleanMetadataKeys(tt.metadata, tt.keys...); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("Test %s failed, expected %v, got %v", tt.name, tt.want, got)
		}
	}
}
