/*
 * MinIO Cloud Storage, (C) 2016-2019 MinIO, Inc.
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
	"bytes"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/minio/minio/cmd/config/compress"
	"github.com/minio/minio/cmd/crypto"
)

// Tests validate bucket name.
func TestIsValidBucketName(t *testing.T) {
	testCases := []struct {
		bucketName string
		shouldPass bool
	}{
		// cases which should pass the test.
		// passing in valid bucket names.
		{"lol", true},
		{"1-this-is-valid", true},
		{"1-this-too-is-valid-1", true},
		{"this.works.too.1", true},
		{"1234567", true},
		{"123", true},
		{"s3-eu-west-1.amazonaws.com", true},
		{"ideas-are-more-powerful-than-guns", true},
		{"testbucket", true},
		{"1bucket", true},
		{"bucket1", true},
		{"a.b", true},
		{"ab.a.bc", true},
		// cases for which test should fail.
		// passing invalid bucket names.
		{"------", false},
		{"my..bucket", false},
		{"192.168.1.1", false},
		{"$this-is-not-valid-too", false},
		{"contains-$-dollar", false},
		{"contains-^-carret", false},
		{"contains-$-dollar", false},
		{"contains-$-dollar", false},
		{"......", false},
		{"", false},
		{"a", false},
		{"ab", false},
		{".starts-with-a-dot", false},
		{"ends-with-a-dot.", false},
		{"ends-with-a-dash-", false},
		{"-starts-with-a-dash", false},
		{"THIS-BEGINS-WITH-UPPERCASe", false},
		{"tHIS-ENDS-WITH-UPPERCASE", false},
		{"ThisBeginsAndEndsWithUpperCasE", false},
		{"una ñina", false},
		{"dash-.may-not-appear-next-to-dot", false},
		{"dash.-may-not-appear-next-to-dot", false},
		{"dash-.-may-not-appear-next-to-dot", false},
		{"lalalallalallalalalallalallalala-thestring-size-is-greater-than-63", false},
	}

	for i, testCase := range testCases {
		isValidBucketName := IsValidBucketName(testCase.bucketName)
		if testCase.shouldPass && !isValidBucketName {
			t.Errorf("Test case %d: Expected \"%s\" to be a valid bucket name", i+1, testCase.bucketName)
		}
		if !testCase.shouldPass && isValidBucketName {
			t.Errorf("Test case %d: Expected bucket name \"%s\" to be invalid", i+1, testCase.bucketName)
		}
	}
}

// Tests for validate object name.
func TestIsValidObjectName(t *testing.T) {
	testCases := []struct {
		objectName string
		shouldPass bool
	}{
		// cases which should pass the test.
		// passing in valid object name.
		{"object", true},
		{"The Shining Script <v1>.pdf", true},
		{"Cost Benefit Analysis (2009-2010).pptx", true},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", true},
		{"SHØRT", true},
		{"f*le", true},
		{"contains-^-carret", true},
		{"contains-|-pipe", true},
		{"contains-`-tick", true},
		{"..test", true},
		{".. test", true},
		{". test", true},
		{".test", true},
		{"There are far too many object names, and far too few bucket names!", true},
		{"!\"#$%&'()*+,-.／:;<=>?@[\\]^_`{|}~/!\"#$%&'()*+,-.／:;<=>?@[\\]^_`{|}~)", true},
		{"!\"#$%&'()*+,-.／:;<=>?@[\\]^_`{|}~", true},
		{"␀␁␂␃␄␅␆␇␈␉␊␋␌␍␎␏␐␑␒␓␔␕␖␗␘␙␚␛␜␝␞␟␡", true},
		{"trailing VT␋/trailing VT␋", true},
		{"␋leading VT/␋leading VT", true},
		{"~leading tilde", true},
		{"\rleading CR", true},
		{"\nleading LF", true},
		{"\tleading HT", true},
		{"trailing CR\r", true},
		{"trailing LF\n", true},
		{"trailing HT\t", true},
		// cases for which test should fail.
		// passing invalid object names.
		{"", false},
		{"a/b/c/", false},
		{"../../etc", false},
		{"../../", false},
		{"/../../etc", false},
		{" ../etc", false},
		{"./././", false},
		{"./etc", false},
		{`contains//double/forwardslash`, false},
		{`//contains/double-forwardslash-prefix`, false},
		{string([]byte{0xff, 0xfe, 0xfd}), false},
	}

	for i, testCase := range testCases {
		isValidObjectName := IsValidObjectName(testCase.objectName)
		if testCase.shouldPass && !isValidObjectName {
			t.Errorf("Test case %d: Expected \"%s\" to be a valid object name", i+1, testCase.objectName)
		}
		if !testCase.shouldPass && isValidObjectName {
			t.Errorf("Test case %d: Expected object name \"%s\" to be invalid", i+1, testCase.objectName)
		}
	}
}

// Tests getCompleteMultipartMD5
func TestGetCompleteMultipartMD5(t *testing.T) {
	testCases := []struct {
		parts          []CompletePart
		expectedResult string
		expectedErr    string
	}{
		// Wrong MD5 hash string, returns md5um of hash
		{[]CompletePart{{ETag: "wrong-md5-hash-string"}}, "0deb8cb07527b4b2669c861cb9653607-1", ""},

		// Single CompletePart with valid MD5 hash string.
		{[]CompletePart{{ETag: "cf1f738a5924e645913c984e0fe3d708"}}, "10dc1617fbcf0bd0858048cb96e6bd77-1", ""},

		// Multiple CompletePart with valid MD5 hash string.
		{[]CompletePart{{ETag: "cf1f738a5924e645913c984e0fe3d708"}, {ETag: "9ccbc9a80eee7fb6fdd22441db2aedbd"}}, "0239a86b5266bb624f0ac60ba2aed6c8-2", ""},
	}

	for i, test := range testCases {
		result := getCompleteMultipartMD5(test.parts)
		if result != test.expectedResult {
			t.Fatalf("test %d failed: expected: result=%v, got=%v", i+1, test.expectedResult, result)
		}
	}
}

// TestIsMinioBucketName - Tests isMinioBucketName helper function.
func TestIsMinioMetaBucketName(t *testing.T) {
	testCases := []struct {
		bucket string
		result bool
	}{
		// MinIO meta bucket.
		{
			bucket: minioMetaBucket,
			result: true,
		},
		// MinIO meta bucket.
		{
			bucket: minioMetaMultipartBucket,
			result: true,
		},
		// MinIO meta bucket.
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

// Tests isCompressed method
func TestIsCompressed(t *testing.T) {
	testCases := []struct {
		objInfo ObjectInfo
		result  bool
		err     bool
	}{
		{
			objInfo: ObjectInfo{
				UserDefined: map[string]string{"X-Minio-Internal-compression": compressionAlgorithmV1,
					"content-type": "application/octet-stream",
					"etag":         "b3ff3ef3789147152fbfbc50efba4bfd-2"},
			},
			result: true,
		},
		{
			objInfo: ObjectInfo{
				UserDefined: map[string]string{"X-Minio-Internal-compression": compressionAlgorithmV2,
					"content-type": "application/octet-stream",
					"etag":         "b3ff3ef3789147152fbfbc50efba4bfd-2"},
			},
			result: true,
		},
		{
			objInfo: ObjectInfo{
				UserDefined: map[string]string{"X-Minio-Internal-compression": "unknown/compression/type",
					"content-type": "application/octet-stream",
					"etag":         "b3ff3ef3789147152fbfbc50efba4bfd-2"},
			},
			result: true,
			err:    true,
		},
		{
			objInfo: ObjectInfo{
				UserDefined: map[string]string{"X-Minio-Internal-compression": compressionAlgorithmV2,
					"content-type": "application/octet-stream",
					"etag":         "b3ff3ef3789147152fbfbc50efba4bfd-2",
					crypto.SSEIV:   "yes",
				},
			},
			result: true,
			err:    true,
		},
		{
			objInfo: ObjectInfo{
				UserDefined: map[string]string{"X-Minio-Internal-XYZ": "klauspost/compress/s2",
					"content-type": "application/octet-stream",
					"etag":         "b3ff3ef3789147152fbfbc50efba4bfd-2"},
			},
			result: false,
		},
		{
			objInfo: ObjectInfo{
				UserDefined: map[string]string{"content-type": "application/octet-stream",
					"etag": "b3ff3ef3789147152fbfbc50efba4bfd-2"},
			},
			result: false,
		},
	}
	for i, test := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			got := test.objInfo.IsCompressed()
			if got != test.result {
				t.Errorf("IsCompressed: Expected %v but received %v",
					test.result, got)
			}
			got, gErr := test.objInfo.IsCompressedOK()
			if got != test.result {
				t.Errorf("IsCompressedOK: Expected %v but received %v",
					test.result, got)
			}
			if gErr != nil != test.err {
				t.Errorf("IsCompressedOK: want error: %t, got error: %v", test.err, gErr)
			}
		})
	}
}

// Tests excludeForCompression.
func TestExcludeForCompression(t *testing.T) {
	testCases := []struct {
		object string
		header http.Header
		result bool
	}{
		{
			object: "object.txt",
			header: http.Header{
				"Content-Type": []string{"application/zip"},
			},
			result: true,
		},
		{
			object: "object.zip",
			header: http.Header{
				"Content-Type": []string{"application/XYZ"},
			},
			result: true,
		},
		{
			object: "object.json",
			header: http.Header{
				"Content-Type": []string{"application/json"},
			},
			result: false,
		},
		{
			object: "object.txt",
			header: http.Header{
				"Content-Type": []string{"text/plain"},
			},
			result: false,
		},
		{
			object: "object",
			header: http.Header{
				"Content-Type": []string{"text/something"},
			},
			result: false,
		},
	}
	for i, test := range testCases {
		got := excludeForCompression(test.header, test.object, compress.Config{
			Enabled: true,
		})
		if got != test.result {
			t.Errorf("Test %d - expected %v but received %v",
				i+1, test.result, got)
		}
	}
}

// Test getPartFile function.
func TestGetPartFile(t *testing.T) {
	testCases := []struct {
		entries    []string
		partNumber int
		etag       string
		result     string
	}{
		{
			entries:    []string{"00001.8a034f82cb9cb31140d87d3ce2a9ede3.67108864", "fs.json", "00002.d73d8ab724016dfb051e2d3584495c54.32891137"},
			partNumber: 1,
			etag:       "8a034f82cb9cb31140d87d3ce2a9ede3",
			result:     "00001.8a034f82cb9cb31140d87d3ce2a9ede3.67108864",
		},
		{
			entries:    []string{"00001.8a034f82cb9cb31140d87d3ce2a9ede3.67108864", "fs.json", "00002.d73d8ab724016dfb051e2d3584495c54.32891137"},
			partNumber: 2,
			etag:       "d73d8ab724016dfb051e2d3584495c54",
			result:     "00002.d73d8ab724016dfb051e2d3584495c54.32891137",
		},
		{
			entries:    []string{"00001.8a034f82cb9cb31140d87d3ce2a9ede3.67108864", "fs.json", "00002.d73d8ab724016dfb051e2d3584495c54.32891137"},
			partNumber: 1,
			etag:       "d73d8ab724016dfb051e2d3584495c54",
			result:     "",
		},
	}
	for i, test := range testCases {
		got := getPartFile(test.entries, test.partNumber, test.etag)
		if got != test.result {
			t.Errorf("Test %d - expected %s but received %s",
				i+1, test.result, got)
		}
	}
}

func TestGetActualSize(t *testing.T) {
	testCases := []struct {
		objInfo ObjectInfo
		result  int64
	}{
		{
			objInfo: ObjectInfo{
				UserDefined: map[string]string{"X-Minio-Internal-compression": "klauspost/compress/s2",
					"X-Minio-Internal-actual-size": "100000001",
					"content-type":                 "application/octet-stream",
					"etag":                         "b3ff3ef3789147152fbfbc50efba4bfd-2"},
				Parts: []ObjectPartInfo{
					{
						Size:       39235668,
						ActualSize: 67108864,
					},
					{
						Size:       19177372,
						ActualSize: 32891137,
					},
				},
			},
			result: 100000001,
		},
		{
			objInfo: ObjectInfo{
				UserDefined: map[string]string{"X-Minio-Internal-compression": "klauspost/compress/s2",
					"X-Minio-Internal-actual-size": "841",
					"content-type":                 "application/octet-stream",
					"etag":                         "b3ff3ef3789147152fbfbc50efba4bfd-2"},
				Parts: []ObjectPartInfo{},
			},
			result: 841,
		},
		{
			objInfo: ObjectInfo{
				UserDefined: map[string]string{"X-Minio-Internal-compression": "klauspost/compress/s2",
					"content-type": "application/octet-stream",
					"etag":         "b3ff3ef3789147152fbfbc50efba4bfd-2"},
				Parts: []ObjectPartInfo{},
			},
			result: -1,
		},
	}
	for i, test := range testCases {
		got, _ := test.objInfo.GetActualSize()
		if got != test.result {
			t.Errorf("Test %d - expected %d but received %d",
				i+1, test.result, got)
		}
	}
}

func TestGetCompressedOffsets(t *testing.T) {
	testCases := []struct {
		objInfo           ObjectInfo
		offset            int64
		startOffset       int64
		snappyStartOffset int64
	}{
		{
			objInfo: ObjectInfo{
				Parts: []ObjectPartInfo{
					{
						Size:       39235668,
						ActualSize: 67108864,
					},
					{
						Size:       19177372,
						ActualSize: 32891137,
					},
				},
			},
			offset:            79109865,
			startOffset:       39235668,
			snappyStartOffset: 12001001,
		},
		{
			objInfo: ObjectInfo{
				Parts: []ObjectPartInfo{
					{
						Size:       39235668,
						ActualSize: 67108864,
					},
					{
						Size:       19177372,
						ActualSize: 32891137,
					},
				},
			},
			offset:            19109865,
			startOffset:       0,
			snappyStartOffset: 19109865,
		},
		{
			objInfo: ObjectInfo{
				Parts: []ObjectPartInfo{
					{
						Size:       39235668,
						ActualSize: 67108864,
					},
					{
						Size:       19177372,
						ActualSize: 32891137,
					},
				},
			},
			offset:            0,
			startOffset:       0,
			snappyStartOffset: 0,
		},
	}
	for i, test := range testCases {
		startOffset, snappyStartOffset := getCompressedOffsets(test.objInfo, test.offset)
		if startOffset != test.startOffset {
			t.Errorf("Test %d - expected startOffset %d but received %d",
				i+1, test.startOffset, startOffset)
		}
		if snappyStartOffset != test.snappyStartOffset {
			t.Errorf("Test %d - expected snappyOffset %d but received %d",
				i+1, test.snappyStartOffset, snappyStartOffset)
		}
	}
}

func TestS2CompressReader(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "small", data: []byte("hello, world")},
		{name: "large", data: bytes.Repeat([]byte("hello, world"), 1000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 100) // make small buffer to ensure multiple reads are required for large case

			r := newS2CompressReader(bytes.NewReader(tt.data))
			defer r.Close()

			var rdrBuf bytes.Buffer
			_, err := io.CopyBuffer(&rdrBuf, r, buf)
			if err != nil {
				t.Fatal(err)
			}

			var stdBuf bytes.Buffer
			w := s2.NewWriter(&stdBuf)
			_, err = io.CopyBuffer(w, bytes.NewReader(tt.data), buf)
			if err != nil {
				t.Fatal(err)
			}
			err = w.Close()
			if err != nil {
				t.Fatal(err)
			}

			var (
				got  = rdrBuf.Bytes()
				want = stdBuf.Bytes()
			)
			if !bytes.Equal(got, want) {
				t.Errorf("encoded data does not match\n\t%q\n\t%q", got, want)
			}

			var decBuf bytes.Buffer
			decRdr := s2.NewReader(&rdrBuf)
			_, err = io.Copy(&decBuf, decRdr)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(tt.data, decBuf.Bytes()) {
				t.Errorf("roundtrip failed\n\t%q\n\t%q", tt.data, decBuf.Bytes())
			}
		})
	}
}
