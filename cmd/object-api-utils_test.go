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

package cmd

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config/compress"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/pkg/v3/trie"
)

func pathJoinOld(elem ...string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if hasSuffixByte(elem[len(elem)-1], SlashSeparatorChar) {
			trailingSlash = SlashSeparator
		}
	}
	return path.Join(elem...) + trailingSlash
}

func concatNaive(ss ...string) string {
	rs := ss[0]
	for i := 1; i < len(ss); i++ {
		rs += ss[i]
	}
	return rs
}

func benchmark(b *testing.B, data []string) {
	b.Run("concat naive", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			concatNaive(data...)
		}
	})
	b.Run("concat fast", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			concat(data...)
		}
	})
}

func BenchmarkConcatImplementation(b *testing.B) {
	data := make([]string, 2)
	rng := rand.New(rand.NewSource(0))
	for i := range 2 {
		var tmp [16]byte
		rng.Read(tmp[:])
		data[i] = hex.EncodeToString(tmp[:])
	}
	b.ResetTimer()
	benchmark(b, data)
}

func BenchmarkPathJoinOld(b *testing.B) {
	b.Run("PathJoin", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			pathJoinOld("volume", "path/path/path")
		}
	})
}

func BenchmarkPathJoin(b *testing.B) {
	b.Run("PathJoin", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			pathJoin("volume", "path/path/path")
		}
	})
}

// Wrapper
func TestPathTraversalExploit(t *testing.T) {
	if runtime.GOOS != globalWindowsOSName {
		t.Skip()
	}
	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testPathTraversalExploit, []string{"PutObject"})
}

// testPathTraversal exploit test, exploits path traversal on windows
// with following object names "\\../.minio.sys/config/iam/${username}/identity.json"
// #16852
func testPathTraversalExploit(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	if err := newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatalf("Initializing config.json failed")
	}

	objectName := `\../.minio.sys/config/hello.txt`

	// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
	rec := httptest.NewRecorder()
	// construct HTTP request for Get Object end point.
	req, err := newTestSignedRequestV4(http.MethodPut, getPutObjectURL("", bucketName, objectName),
		int64(5), bytes.NewReader([]byte("hello")), credentials.AccessKey, credentials.SecretKey, map[string]string{})
	if err != nil {
		t.Fatalf("failed to create HTTP request for Put Object: <ERROR> %v", err)
	}

	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
	// Call the ServeHTTP to execute the handler.
	apiRouter.ServeHTTP(rec, req)

	ctx, cancel := context.WithCancel(GlobalContext)
	defer cancel()

	// Now check if we actually wrote to backend (regardless of the response
	// returned by the server).
	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]
	erasureDisks := xl.getDisks()
	parts, errs := readAllFileInfo(ctx, erasureDisks, "", bucketName, objectName, "", false, false)
	for i := range parts {
		if errs[i] == nil {
			if parts[i].Name == objectName {
				t.Errorf("path traversal allowed to allow writing to minioMetaBucket: %s", instanceType)
			}
		}
	}
}

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
		{"contains-^-caret", false},
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
		{"contains-^-caret", true},
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
		0: {
			objInfo: ObjectInfo{
				UserDefined: map[string]string{
					"X-Minio-Internal-compression": compressionAlgorithmV1,
					"content-type":                 "application/octet-stream",
					"etag":                         "b3ff3ef3789147152fbfbc50efba4bfd-2",
				},
			},
			result: true,
		},
		1: {
			objInfo: ObjectInfo{
				UserDefined: map[string]string{
					"X-Minio-Internal-compression": compressionAlgorithmV2,
					"content-type":                 "application/octet-stream",
					"etag":                         "b3ff3ef3789147152fbfbc50efba4bfd-2",
				},
			},
			result: true,
		},
		2: {
			objInfo: ObjectInfo{
				UserDefined: map[string]string{
					"X-Minio-Internal-compression": "unknown/compression/type",
					"content-type":                 "application/octet-stream",
					"etag":                         "b3ff3ef3789147152fbfbc50efba4bfd-2",
				},
			},
			result: true,
			err:    true,
		},
		3: {
			objInfo: ObjectInfo{
				UserDefined: map[string]string{
					"X-Minio-Internal-compression": compressionAlgorithmV2,
					"content-type":                 "application/octet-stream",
					"etag":                         "b3ff3ef3789147152fbfbc50efba4bfd-2",
					crypto.MetaIV:                  "yes",
				},
			},
			result: true,
			err:    false,
		},
		4: {
			objInfo: ObjectInfo{
				UserDefined: map[string]string{
					"X-Minio-Internal-XYZ": "klauspost/compress/s2",
					"content-type":         "application/octet-stream",
					"etag":                 "b3ff3ef3789147152fbfbc50efba4bfd-2",
				},
			},
			result: false,
		},
		5: {
			objInfo: ObjectInfo{
				UserDefined: map[string]string{
					"content-type": "application/octet-stream",
					"etag":         "b3ff3ef3789147152fbfbc50efba4bfd-2",
				},
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

func BenchmarkGetPartFileWithTrie(b *testing.B) {
	b.ResetTimer()

	entriesTrie := trie.NewTrie()
	for i := 1; i <= 10000; i++ {
		entriesTrie.Insert(fmt.Sprintf("%.5d.8a034f82cb9cb31140d87d3ce2a9ede3.67108864", i))
	}

	for i := 1; i <= 10000; i++ {
		partFile := getPartFile(entriesTrie, i, "8a034f82cb9cb31140d87d3ce2a9ede3")
		if partFile == "" {
			b.Fatal("partFile returned is empty")
		}
	}

	b.ReportAllocs()
}

func TestGetActualSize(t *testing.T) {
	testCases := []struct {
		objInfo ObjectInfo
		result  int64
	}{
		{
			objInfo: ObjectInfo{
				UserDefined: map[string]string{
					"X-Minio-Internal-compression": "klauspost/compress/s2",
					"content-type":                 "application/octet-stream",
					"etag":                         "b3ff3ef3789147152fbfbc50efba4bfd-2",
				},
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
				Size: 100000001,
			},
			result: 100000001,
		},
		{
			objInfo: ObjectInfo{
				UserDefined: map[string]string{
					"X-Minio-Internal-compression": "klauspost/compress/s2",
					"X-Minio-Internal-actual-size": "841",
					"content-type":                 "application/octet-stream",
					"etag":                         "b3ff3ef3789147152fbfbc50efba4bfd-2",
				},
				Parts: []ObjectPartInfo{},
				Size:  841,
			},
			result: 841,
		},
		{
			objInfo: ObjectInfo{
				UserDefined: map[string]string{
					"X-Minio-Internal-compression": "klauspost/compress/s2",
					"content-type":                 "application/octet-stream",
					"etag":                         "b3ff3ef3789147152fbfbc50efba4bfd-2",
				},
				Parts: []ObjectPartInfo{},
				Size:  100,
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
		firstPart         int
	}{
		0: {
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
			firstPart:         1,
		},
		1: {
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
		2: {
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
		startOffset, snappyStartOffset, firstPart, _, _ := getCompressedOffsets(test.objInfo, test.offset, nil)
		if startOffset != test.startOffset {
			t.Errorf("Test %d - expected startOffset %d but received %d",
				i, test.startOffset, startOffset)
		}
		if snappyStartOffset != test.snappyStartOffset {
			t.Errorf("Test %d - expected snappyOffset %d but received %d",
				i, test.snappyStartOffset, snappyStartOffset)
		}
		if firstPart != test.firstPart {
			t.Errorf("Test %d - expected firstPart %d but received %d",
				i, test.firstPart, firstPart)
		}
	}
}

func TestS2CompressReader(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantIdx bool
	}{
		{name: "empty", data: nil},
		{name: "small", data: []byte("hello, world!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")},
		{name: "large", data: bytes.Repeat([]byte("hello, world"), 1000000), wantIdx: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 100) // make small buffer to ensure multiple reads are required for large case

			r, idxCB := newS2CompressReader(bytes.NewReader(tt.data), int64(len(tt.data)), false)
			defer r.Close()

			var rdrBuf bytes.Buffer
			_, err := io.CopyBuffer(&rdrBuf, r, buf)
			if err != nil {
				t.Fatal(err)
			}
			r.Close()
			idx := idxCB()
			if !tt.wantIdx && len(idx) > 0 {
				t.Errorf("index returned above threshold")
			}
			if tt.wantIdx {
				if idx == nil {
					t.Errorf("no index returned")
				}
				var index s2.Index
				_, err = index.Load(s2.RestoreIndexHeaders(idx))
				if err != nil {
					t.Errorf("error loading index: %v", err)
				}
				t.Log("size:", len(idx))
				t.Log(string(index.JSON()))
				if index.TotalUncompressed != int64(len(tt.data)) {
					t.Errorf("Expected size %d, got %d", len(tt.data), index.TotalUncompressed)
				}
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

func Test_pathNeedsClean(t *testing.T) {
	type pathTest struct {
		path, result string
	}

	cleantests := []pathTest{
		// Already clean
		{"", "."},
		{"abc", "abc"},
		{"abc/def", "abc/def"},
		{"a/b/c", "a/b/c"},
		{".", "."},
		{"..", ".."},
		{"../..", "../.."},
		{"../../abc", "../../abc"},
		{"/abc", "/abc"},
		{"/abc/def", "/abc/def"},
		{"/", "/"},

		// Remove trailing slash
		{"abc/", "abc"},
		{"abc/def/", "abc/def"},
		{"a/b/c/", "a/b/c"},
		{"./", "."},
		{"../", ".."},
		{"../../", "../.."},
		{"/abc/", "/abc"},

		// Remove doubled slash
		{"abc//def//ghi", "abc/def/ghi"},
		{"//abc", "/abc"},
		{"///abc", "/abc"},
		{"//abc//", "/abc"},
		{"abc//", "abc"},

		// Remove . elements
		{"abc/./def", "abc/def"},
		{"/./abc/def", "/abc/def"},
		{"abc/.", "abc"},

		// Remove .. elements
		{"abc/def/ghi/../jkl", "abc/def/jkl"},
		{"abc/def/../ghi/../jkl", "abc/jkl"},
		{"abc/def/..", "abc"},
		{"abc/def/../..", "."},
		{"/abc/def/../..", "/"},
		{"abc/def/../../..", ".."},
		{"/abc/def/../../..", "/"},
		{"abc/def/../../../ghi/jkl/../../../mno", "../../mno"},

		// Combinations
		{"abc/./../def", "def"},
		{"abc//./../def", "def"},
		{"abc/../../././../def", "../../def"},
	}
	for _, test := range cleantests {
		want := test.path != test.result
		got := pathNeedsClean([]byte(test.path))
		if !got {
			t.Logf("no clean: %q", test.path)
		}
		if want && !got {
			t.Errorf("input: %q, want %v, got %v", test.path, want, got)
		}
	}
}
