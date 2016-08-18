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
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

// md5Hex ignores error from Write method since it never returns one. Check
// crypto/md5 doc for more details.
func md5Hex(b []byte) string {
	md5Writer := md5.New()
	md5Writer.Write(b)
	return hex.EncodeToString(md5Writer.Sum(nil))
}

func md5Header(data []byte) map[string]string {
	return map[string]string{"md5Sum": md5Hex([]byte(data))}
}

// Wrapper for calling PutObject tests for both XL multiple disks and single node setup.
func TestObjectAPIPutObject(t *testing.T) {
	ExecObjectLayerTest(t, testObjectAPIPutObject)
}

// Tests validate correctness of PutObject.
func testObjectAPIPutObject(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Generating cases for which the PutObject fails.
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket.
	err := obj.MakeBucket(bucket)
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Creating a dummy bucket for tests.
	err = obj.MakeBucket("unused-bucket")
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	var (
		nilBytes    []byte
		data        = []byte("hello")
		fiveMBBytes = bytes.Repeat([]byte("a"), 5*1024*124)
	)
	invalidMD5 := md5Hex([]byte("meh"))
	invalidMD5Header := md5Header([]byte("meh"))

	testCases := []struct {
		bucketName     string
		objName        string
		inputData      []byte
		inputMeta      map[string]string
		intputDataSize int64
		// expected error output.
		expectedMd5   string
		expectedError error
	}{
		// Test case  1-4.
		// Cases with invalid bucket name.
		{".test", "obj", []byte(""), nil, 0, "", BucketNameInvalid{Bucket: ".test"}},
		{"------", "obj", []byte(""), nil, 0, "", BucketNameInvalid{Bucket: "------"}},
		{"$this-is-not-valid-too", "obj", []byte(""), nil, 0, "",
			BucketNameInvalid{Bucket: "$this-is-not-valid-too"}},
		{"a", "obj", []byte(""), nil, 0, "", BucketNameInvalid{Bucket: "a"}},

		// Test case - 5.
		// Case with invalid object names.
		{bucket, "", []byte(""), nil, 0, "", ObjectNameInvalid{Bucket: bucket, Object: ""}},

		// Test case - 6.
		// Valid object and bucket names but non-existent bucket.
		{"abc", "def", []byte(""), nil, 0, "", BucketNotFound{Bucket: "abc"}},

		// Test case - 7.
		// Input to replicate Md5 mismatch.
		{bucket, object, []byte(""), map[string]string{"md5Sum": "a35"}, 0, "",
			BadDigest{ExpectedMD5: "a35", CalculatedMD5: "d41d8cd98f00b204e9800998ecf8427e"}},

		// Test case - 8.
		// Input with size more than the size of actual data inside the reader.
		{bucket, object, []byte("abcd"), map[string]string{"md5Sum": "a35"}, int64(len("abcd") + 1), "",
			IncompleteBody{}},

		// Test case - 9.
		// Input with size less than the size of actual data inside the reader.
		{bucket, object, []byte("abcd"), map[string]string{"md5Sum": "a35"}, int64(len("abcd") - 1), "",
			BadDigest{ExpectedMD5: "a35", CalculatedMD5: "900150983cd24fb0d6963f7d28e17f72"}},

		// Test case - 10-13.
		// Validating for success cases.
		{bucket, object, []byte("abcd"), map[string]string{"md5Sum": "e2fc714c4727ee9395f324cd2e7f331f"}, int64(len("abcd")), "", nil},
		{bucket, object, []byte("efgh"), map[string]string{"md5Sum": "1f7690ebdd9b4caf8fab49ca1757bf27"}, int64(len("efgh")), "", nil},
		{bucket, object, []byte("ijkl"), map[string]string{"md5Sum": "09a0877d04abf8759f99adec02baf579"}, int64(len("ijkl")), "", nil},
		{bucket, object, []byte("mnop"), map[string]string{"md5Sum": "e132e96a5ddad6da8b07bba6f6131fef"}, int64(len("mnop")), "", nil},

		// Test case 14-16.
		// With no metadata
		{bucket, object, data, nil, int64(len(data)), md5Hex(data), nil},
		{bucket, object, nilBytes, nil, int64(len(nilBytes)), md5Hex(nilBytes), nil},
		{bucket, object, fiveMBBytes, nil, int64(len(fiveMBBytes)), md5Hex(fiveMBBytes), nil},

		// Test case 17-19.
		// With arbitrary metadata
		{bucket, object, data, map[string]string{"answer": "42"}, int64(len(data)), md5Hex(data), nil},
		{bucket, object, nilBytes, map[string]string{"answer": "42"}, int64(len(nilBytes)), md5Hex(nilBytes), nil},
		{bucket, object, fiveMBBytes, map[string]string{"answer": "42"}, int64(len(fiveMBBytes)), md5Hex(fiveMBBytes), nil},

		// Test case 20-22.
		// With valid md5sum in header
		{bucket, object, data, md5Header(data), int64(len(data)), md5Hex(data), nil},
		{bucket, object, nilBytes, md5Header(nilBytes), int64(len(nilBytes)), md5Hex(nilBytes), nil},
		{bucket, object, fiveMBBytes, md5Header(fiveMBBytes), int64(len(fiveMBBytes)), md5Hex(fiveMBBytes), nil},

		// Test case 23-25.
		// data with invalid md5sum in header
		{bucket, object, data, invalidMD5Header, int64(len(data)), md5Hex(data), BadDigest{invalidMD5, md5Hex(data)}},
		{bucket, object, nilBytes, invalidMD5Header, int64(len(nilBytes)), md5Hex(nilBytes), BadDigest{invalidMD5, md5Hex(nilBytes)}},
		{bucket, object, fiveMBBytes, invalidMD5Header, int64(len(fiveMBBytes)), md5Hex(fiveMBBytes), BadDigest{invalidMD5, md5Hex(fiveMBBytes)}},

		// Test case 26-28.
		// data with size different from the actual number of bytes available in the reader
		{bucket, object, data, nil, int64(len(data) - 1), md5Hex(data[:len(data)-1]), nil},
		{bucket, object, nilBytes, nil, int64(len(nilBytes) + 1), md5Hex(nilBytes), IncompleteBody{}},
		{bucket, object, fiveMBBytes, nil, int64(0), md5Hex(fiveMBBytes), nil},
	}

	for i, testCase := range testCases {
		actualMd5Hex, actualErr := obj.PutObject(testCase.bucketName, testCase.objName, testCase.intputDataSize, bytes.NewReader(testCase.inputData), testCase.inputMeta)
		if actualErr != nil && testCase.expectedError == nil {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: error %s.", i+1, instanceType, actualErr.Error())
		}
		if actualErr == nil && testCase.expectedError != nil {
			t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but passed instead.", i+1, instanceType, testCase.expectedError.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && testCase.expectedError != actualErr {
			t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead.", i+1, instanceType, testCase.expectedError.Error(), actualErr.Error())
		}
		// Test passes as expected, but the output values are verified for correctness here.
		if actualErr == nil {
			// Asserting whether the md5 output is correct.
			if expectedMD5, ok := testCase.inputMeta["md5Sum"]; ok && expectedMD5 != actualMd5Hex {
				t.Errorf("Test %d: %s: Calculated Md5 different from the actual one %s.", i+1, instanceType, actualMd5Hex)
			}
		}
	}
}

// Wrapper for calling PutObject tests for both XL multiple disks case
// when quorum is not available.
func TestObjectAPIPutObjectDiskNotFound(t *testing.T) {
	ExecObjectLayerDiskNotFoundTest(t, testObjectAPIPutObjectDiskNotFOund)
}

// Tests validate correctness of PutObject.
func testObjectAPIPutObjectDiskNotFOund(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	// Generating cases for which the PutObject fails.
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket.
	err := obj.MakeBucket(bucket)
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Creating a dummy bucket for tests.
	err = obj.MakeBucket("unused-bucket")
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Take 8 disks down, one more we loose quorum on 16 disk node.
	for _, disk := range disks[:7] {
		removeAll(disk)
	}

	testCases := []struct {
		bucketName     string
		objName        string
		inputData      []byte
		inputMeta      map[string]string
		intputDataSize int64
		// flag indicating whether the test should pass.
		shouldPass bool
		// expected error output.
		expectedMd5   string
		expectedError error
	}{
		// Validating for success cases.
		{bucket, object, []byte("abcd"), map[string]string{"md5Sum": "e2fc714c4727ee9395f324cd2e7f331f"}, int64(len("abcd")), true, "", nil},
		{bucket, object, []byte("efgh"), map[string]string{"md5Sum": "1f7690ebdd9b4caf8fab49ca1757bf27"}, int64(len("efgh")), true, "", nil},
		{bucket, object, []byte("ijkl"), map[string]string{"md5Sum": "09a0877d04abf8759f99adec02baf579"}, int64(len("ijkl")), true, "", nil},
		{bucket, object, []byte("mnop"), map[string]string{"md5Sum": "e132e96a5ddad6da8b07bba6f6131fef"}, int64(len("mnop")), true, "", nil},
	}

	for i, testCase := range testCases {
		actualMd5Hex, actualErr := obj.PutObject(testCase.bucketName, testCase.objName, testCase.intputDataSize, bytes.NewReader(testCase.inputData), testCase.inputMeta)
		if actualErr != nil && testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s.", i+1, instanceType, actualErr.Error())
		}
		if actualErr == nil && !testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead.", i+1, instanceType, testCase.expectedError.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && !testCase.shouldPass {
			if testCase.expectedError.Error() != actualErr.Error() {
				t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead.", i+1,
					instanceType, testCase.expectedError.Error(), actualErr.Error())
			}
		}
		// Test passes as expected, but the output values are verified for correctness here.
		if actualErr == nil && testCase.shouldPass {
			// Asserting whether the md5 output is correct.
			if testCase.inputMeta["md5Sum"] != actualMd5Hex {
				t.Errorf("Test %d: %s: Calculated Md5 different from the actual one %s.", i+1, instanceType, actualMd5Hex)
			}
		}
	}

	// This causes quorum failure verify.
	removeAll(disks[len(disks)-1])

	// Validate the last test.
	testCase := struct {
		bucketName     string
		objName        string
		inputData      []byte
		inputMeta      map[string]string
		intputDataSize int64
		// flag indicating whether the test should pass.
		shouldPass bool
		// expected error output.
		expectedMd5   string
		expectedError error
	}{
		bucket,
		object,
		[]byte("mnop"),
		map[string]string{"md5Sum": "e132e96a5ddad6da8b07bba6f6131fef"},
		int64(len("mnop")),
		false,
		"",
		InsufficientWriteQuorum{},
	}
	_, actualErr := obj.PutObject(testCase.bucketName, testCase.objName, testCase.intputDataSize, bytes.NewReader(testCase.inputData), testCase.inputMeta)
	if actualErr != nil && testCase.shouldPass {
		t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s.", len(testCases)+1, instanceType, actualErr.Error())
	}
	// Failed as expected, but does it fail for the expected reason.
	if actualErr != nil && !testCase.shouldPass {
		if testCase.expectedError.Error() != actualErr.Error() {
			t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead.", len(testCases)+1, instanceType, testCase.expectedError.Error(), actualErr.Error())
		}
	}
}

// Wrapper for calling PutObject tests for both XL multiple disks and single node setup.
func TestObjectAPIPutObjectStaleFiles(t *testing.T) {
	ExecObjectLayerStaleFilesTest(t, testObjectAPIPutObjectStaleFiles)
}

// Tests validate correctness of PutObject.
func testObjectAPIPutObjectStaleFiles(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	// Generating cases for which the PutObject fails.
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket.
	err := obj.MakeBucket(bucket)
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	data := []byte("hello, world")
	// Create object.
	_, err = obj.PutObject(bucket, object, int64(len(data)), bytes.NewReader(data), nil)
	if err != nil {
		// Failed to create object, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	for _, disk := range disks {
		tmpMetaDir := path.Join(disk, minioMetaBucket, tmpMetaPrefix)
		if !isDirEmpty(tmpMetaDir) {
			t.Fatalf("%s: expected: empty, got: non-empty", tmpMetaDir)
		}
	}
}

// Wrapper for calling Multipart PutObject tests for both XL multiple disks and single node setup.
func TestObjectAPIMultipartPutObjectStaleFiles(t *testing.T) {
	ExecObjectLayerStaleFilesTest(t, testObjectAPIMultipartPutObjectStaleFiles)
}

// Tests validate correctness of PutObject.
func testObjectAPIMultipartPutObjectStaleFiles(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	// Generating cases for which the PutObject fails.
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket.
	err := obj.MakeBucket(bucket)
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Initiate Multipart Upload on the above created bucket.
	uploadID, err := obj.NewMultipartUpload(bucket, object, nil)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Upload part1.
	fiveMBBytes := bytes.Repeat([]byte("a"), 5*1024*1024)
	md5Writer := md5.New()
	md5Writer.Write(fiveMBBytes)
	etag1 := hex.EncodeToString(md5Writer.Sum(nil))
	_, err = obj.PutObjectPart(bucket, object, uploadID, 1, int64(len(fiveMBBytes)), bytes.NewReader(fiveMBBytes), etag1)
	if err != nil {
		// Failed to upload object part, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Upload part2.
	data := []byte("hello, world")
	md5Writer = md5.New()
	md5Writer.Write(data)
	etag2 := hex.EncodeToString(md5Writer.Sum(nil))
	_, err = obj.PutObjectPart(bucket, object, uploadID, 2, int64(len(data)), bytes.NewReader(data), etag2)
	if err != nil {
		// Failed to upload object part, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Complete multipart.
	parts := []completePart{
		{ETag: etag1, PartNumber: 1},
		{ETag: etag2, PartNumber: 2},
	}
	_, err = obj.CompleteMultipartUpload(bucket, object, uploadID, parts)
	if err != nil {
		// Failed to complete multipart upload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	for _, disk := range disks {
		tmpMetaDir := path.Join(disk, minioMetaBucket, tmpMetaPrefix)
		files, err := ioutil.ReadDir(tmpMetaDir)
		if err != nil {
			// Its OK to have non-existen tmpMetaDir.
			if os.IsNotExist(err) {
				continue
			}

			// Print the error
			t.Errorf("%s", err)
		}

		if len(files) != 0 {
			t.Fatalf("%s: expected: empty, got: non-empty. content: %s", tmpMetaDir, files)
		}
	}
}

// Benchmarks for ObjectLayer.PutObject().
// The intent is to benchmark PutObject for various sizes ranging from few bytes to 100MB.
// Also each of these Benchmarks are run both XL and FS backends.

// BenchmarkPutObjectVerySmallFS - Benchmark FS.PutObject() for object size of 10 bytes.
func BenchmarkPutObjectVerySmallFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 10)
}

// BenchmarkPutObjectVerySmallXL - Benchmark XL.PutObject() for object size of 10 bytes.
func BenchmarkPutObjectVerySmallXL(b *testing.B) {
	benchmarkPutObject(b, "XL", 10)
}

// BenchmarkPutObject10KbFS - Benchmark FS.PutObject() for object size of 10KB.
func BenchmarkPutObject10KbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 10*1024)
}

// BenchmarkPutObject10KbXL - Benchmark XL.PutObject() for object size of 10KB.
func BenchmarkPutObject10KbXL(b *testing.B) {
	benchmarkPutObject(b, "XL", 10*1024)
}

// BenchmarkPutObject100KbFS - Benchmark FS.PutObject() for object size of 100KB.
func BenchmarkPutObject100KbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 100*1024)
}

// BenchmarkPutObject100KbXL - Benchmark XL.PutObject() for object size of 100KB.
func BenchmarkPutObject100KbXL(b *testing.B) {
	benchmarkPutObject(b, "XL", 100*1024)
}

// BenchmarkPutObject1MbFS - Benchmark FS.PutObject() for object size of 1MB.
func BenchmarkPutObject1MbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 1024*1024)
}

// BenchmarkPutObject1MbXL - Benchmark XL.PutObject() for object size of 1MB.
func BenchmarkPutObject1MbXL(b *testing.B) {
	benchmarkPutObject(b, "XL", 1024*1024)
}

// BenchmarkPutObject5MbFS - Benchmark FS.PutObject() for object size of 5MB.
func BenchmarkPutObject5MbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 5*1024*1024)
}

// BenchmarkPutObject5MbXL - Benchmark XL.PutObject() for object size of 5MB.
func BenchmarkPutObject5MbXL(b *testing.B) {
	benchmarkPutObject(b, "XL", 5*1024*1024)
}

// BenchmarkPutObject10MbFS - Benchmark FS.PutObject() for object size of 10MB.
func BenchmarkPutObject10MbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 10*1024*1024)
}

// BenchmarkPutObject10MbXL - Benchmark XL.PutObject() for object size of 10MB.
func BenchmarkPutObject10MbXL(b *testing.B) {
	benchmarkPutObject(b, "XL", 10*1024*1024)
}

// BenchmarkPutObject25MbFS - Benchmark FS.PutObject() for object size of 25MB.
func BenchmarkPutObject25MbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 25*1024*1024)

}

// BenchmarkPutObject25MbXL - Benchmark XL.PutObject() for object size of 25MB.
func BenchmarkPutObject25MbXL(b *testing.B) {
	benchmarkPutObject(b, "XL", 25*1024*1024)
}

// BenchmarkPutObject50MbFS - Benchmark FS.PutObject() for object size of 50MB.
func BenchmarkPutObject50MbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 50*1024*1024)
}

// BenchmarkPutObject50MbXL - Benchmark XL.PutObject() for object size of 50MB.
func BenchmarkPutObject50MbXL(b *testing.B) {
	benchmarkPutObject(b, "XL", 50*1024*1024)
}

// parallel benchmarks for ObjectLayer.PutObject() .

// BenchmarkParallelPutObjectVerySmallFS - BenchmarkParallel FS.PutObject() for object size of 10 bytes.
func BenchmarkParallelPutObjectVerySmallFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 10)
}

// BenchmarkParallelPutObjectVerySmallXL - BenchmarkParallel XL.PutObject() for object size of 10 bytes.
func BenchmarkParallelPutObjectVerySmallXL(b *testing.B) {
	benchmarkPutObjectParallel(b, "XL", 10)
}

// BenchmarkParallelPutObject10KbFS - BenchmarkParallel FS.PutObject() for object size of 10KB.
func BenchmarkParallelPutObject10KbFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 10*1024)
}

// BenchmarkParallelPutObject10KbXL - BenchmarkParallel XL.PutObject() for object size of 10KB.
func BenchmarkParallelPutObject10KbXL(b *testing.B) {
	benchmarkPutObjectParallel(b, "XL", 10*1024)
}

// BenchmarkParallelPutObject100KbFS - BenchmarkParallel FS.PutObject() for object size of 100KB.
func BenchmarkParallelPutObject100KbFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 100*1024)
}

// BenchmarkParallelPutObject100KbXL - BenchmarkParallel XL.PutObject() for object size of 100KB.
func BenchmarkParallelPutObject100KbXL(b *testing.B) {
	benchmarkPutObjectParallel(b, "XL", 100*1024)
}

// BenchmarkParallelPutObject1MbFS - BenchmarkParallel FS.PutObject() for object size of 1MB.
func BenchmarkParallelPutObject1MbFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 1024*1024)
}

// BenchmarkParallelPutObject1MbXL - BenchmarkParallel XL.PutObject() for object size of 1MB.
func BenchmarkParallelPutObject1MbXL(b *testing.B) {
	benchmarkPutObjectParallel(b, "XL", 1024*1024)
}

// BenchmarkParallelPutObject5MbFS - BenchmarkParallel FS.PutObject() for object size of 5MB.
func BenchmarkParallelPutObject5MbFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 5*1024*1024)
}

// BenchmarkParallelPutObject5MbXL - BenchmarkParallel XL.PutObject() for object size of 5MB.
func BenchmarkParallelPutObject5MbXL(b *testing.B) {
	benchmarkPutObjectParallel(b, "XL", 5*1024*1024)
}

// BenchmarkParallelPutObject10MbFS - BenchmarkParallel FS.PutObject() for object size of 10MB.
func BenchmarkParallelPutObject10MbFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 10*1024*1024)
}

// BenchmarkParallelPutObject10MbXL - BenchmarkParallel XL.PutObject() for object size of 10MB.
func BenchmarkParallelPutObject10MbXL(b *testing.B) {
	benchmarkPutObjectParallel(b, "XL", 10*1024*1024)
}

// BenchmarkParallelPutObject25MbFS - BenchmarkParallel FS.PutObject() for object size of 25MB.
func BenchmarkParallelPutObject25MbFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 25*1024*1024)

}

// BenchmarkParallelPutObject25MbXL - BenchmarkParallel XL.PutObject() for object size of 25MB.
func BenchmarkParallelPutObject25MbXL(b *testing.B) {
	benchmarkPutObjectParallel(b, "XL", 25*1024*1024)
}
