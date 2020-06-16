/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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
	"context"
	"crypto/md5"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path"
	"testing"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/hash"
)

func md5Header(data []byte) map[string]string {
	return map[string]string{"etag": getMD5Hash([]byte(data))}
}

// Wrapper for calling PutObject tests for both Erasure multiple disks and single node setup.
func TestObjectAPIPutObjectSingle(t *testing.T) {
	ExecObjectLayerTest(t, testObjectAPIPutObject)
}

// Tests validate correctness of PutObject.
func testObjectAPIPutObject(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Generating cases for which the PutObject fails.
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket.
	err := obj.MakeBucketWithLocation(context.Background(), bucket, BucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Creating a dummy bucket for tests.
	err = obj.MakeBucketWithLocation(context.Background(), "unused-bucket", BucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	var (
		nilBytes    []byte
		data        = []byte("hello")
		fiveMBBytes = bytes.Repeat([]byte("a"), 5*humanize.MiByte)
	)
	invalidMD5 := getMD5Hash([]byte("meh"))
	invalidMD5Header := md5Header([]byte("meh"))

	testCases := []struct {
		bucketName     string
		objName        string
		inputData      []byte
		inputMeta      map[string]string
		inputSHA256    string
		intputDataSize int64
		// expected error output.
		expectedMd5   string
		expectedError error
	}{
		// Test case  1-4.
		// Cases with invalid bucket name.
		{".test", "obj", []byte(""), nil, "", 0, "", BucketNotFound{Bucket: ".test"}},
		{"------", "obj", []byte(""), nil, "", 0, "", BucketNotFound{Bucket: "------"}},
		{"$this-is-not-valid-too", "obj", []byte(""), nil, "", 0, "",
			BucketNotFound{Bucket: "$this-is-not-valid-too"}},
		{"a", "obj", []byte(""), nil, "", 0, "", BucketNotFound{Bucket: "a"}},

		// Test case - 5.
		// Case with invalid object names.
		{bucket, "", []byte(""), nil, "", 0, "", ObjectNameInvalid{Bucket: bucket, Object: ""}},

		// Test case - 6.
		// Valid object and bucket names but non-existent bucket.
		{"abc", "def", []byte(""), nil, "", 0, "", BucketNotFound{Bucket: "abc"}},

		// Test case - 7.
		// Input to replicate Md5 mismatch.
		{bucket, object, []byte(""), map[string]string{"etag": "d41d8cd98f00b204e9800998ecf8427f"}, "", 0, "",
			hash.BadDigest{ExpectedMD5: "d41d8cd98f00b204e9800998ecf8427f", CalculatedMD5: "d41d8cd98f00b204e9800998ecf8427e"}},

		// Test case - 8.
		// With incorrect sha256.
		{bucket, object, []byte("abcd"), map[string]string{"etag": "e2fc714c4727ee9395f324cd2e7f331f"},
			"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031580", int64(len("abcd")),
			"", hash.SHA256Mismatch{ExpectedSHA256: "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031580",
				CalculatedSHA256: "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589"}},

		// Test case - 9.
		// Input with size more than the size of actual data inside the reader.
		{bucket, object, []byte("abcd"), map[string]string{"etag": "e2fc714c4727ee9395f324cd2e7f331e"}, "", int64(len("abcd") + 1), "",
			hash.BadDigest{ExpectedMD5: "e2fc714c4727ee9395f324cd2e7f331e", CalculatedMD5: "e2fc714c4727ee9395f324cd2e7f331f"}},

		// Test case - 10.
		// Input with size less than the size of actual data inside the reader.
		{bucket, object, []byte("abcd"), map[string]string{"etag": "900150983cd24fb0d6963f7d28e17f73"}, "", int64(len("abcd") - 1), "",
			hash.BadDigest{ExpectedMD5: "900150983cd24fb0d6963f7d28e17f73", CalculatedMD5: "900150983cd24fb0d6963f7d28e17f72"}},

		// Test case - 11-14.
		// Validating for success cases.
		{bucket, object, []byte("abcd"), map[string]string{"etag": "e2fc714c4727ee9395f324cd2e7f331f"}, "", int64(len("abcd")), "", nil},
		{bucket, object, []byte("efgh"), map[string]string{"etag": "1f7690ebdd9b4caf8fab49ca1757bf27"}, "", int64(len("efgh")), "", nil},
		{bucket, object, []byte("ijkl"), map[string]string{"etag": "09a0877d04abf8759f99adec02baf579"}, "", int64(len("ijkl")), "", nil},
		{bucket, object, []byte("mnop"), map[string]string{"etag": "e132e96a5ddad6da8b07bba6f6131fef"}, "", int64(len("mnop")), "", nil},

		// Test case 15-17.
		// With no metadata
		{bucket, object, data, nil, "", int64(len(data)), getMD5Hash(data), nil},
		{bucket, object, nilBytes, nil, "", int64(len(nilBytes)), getMD5Hash(nilBytes), nil},
		{bucket, object, fiveMBBytes, nil, "", int64(len(fiveMBBytes)), getMD5Hash(fiveMBBytes), nil},

		// Test case 18-20.
		// With arbitrary metadata
		{bucket, object, data, map[string]string{"answer": "42"}, "", int64(len(data)), getMD5Hash(data), nil},
		{bucket, object, nilBytes, map[string]string{"answer": "42"}, "", int64(len(nilBytes)), getMD5Hash(nilBytes), nil},
		{bucket, object, fiveMBBytes, map[string]string{"answer": "42"}, "", int64(len(fiveMBBytes)), getMD5Hash(fiveMBBytes), nil},

		// Test case 21-23.
		// With valid md5sum and sha256.
		{bucket, object, data, md5Header(data), getSHA256Hash(data), int64(len(data)), getMD5Hash(data), nil},
		{bucket, object, nilBytes, md5Header(nilBytes), getSHA256Hash(nilBytes), int64(len(nilBytes)), getMD5Hash(nilBytes), nil},
		{bucket, object, fiveMBBytes, md5Header(fiveMBBytes), getSHA256Hash(fiveMBBytes), int64(len(fiveMBBytes)), getMD5Hash(fiveMBBytes), nil},

		// Test case 24-26.
		// data with invalid md5sum in header
		{bucket, object, data, invalidMD5Header, "", int64(len(data)), getMD5Hash(data),
			hash.BadDigest{ExpectedMD5: invalidMD5, CalculatedMD5: getMD5Hash(data)}},
		{bucket, object, nilBytes, invalidMD5Header, "", int64(len(nilBytes)), getMD5Hash(nilBytes),
			hash.BadDigest{ExpectedMD5: invalidMD5, CalculatedMD5: getMD5Hash(nilBytes)}},
		{bucket, object, fiveMBBytes, invalidMD5Header, "", int64(len(fiveMBBytes)), getMD5Hash(fiveMBBytes),
			hash.BadDigest{ExpectedMD5: invalidMD5, CalculatedMD5: getMD5Hash(fiveMBBytes)}},

		// Test case 27-29.
		// data with size different from the actual number of bytes available in the reader
		{bucket, object, data, nil, "", int64(len(data) - 1), getMD5Hash(data[:len(data)-1]), nil},
		{bucket, object, nilBytes, nil, "", int64(len(nilBytes) + 1), getMD5Hash(nilBytes), IncompleteBody{}},
		{bucket, object, fiveMBBytes, nil, "", 0, getMD5Hash(fiveMBBytes), nil},

		// Test case 30
		// valid data with X-Amz-Meta- meta
		{bucket, object, data, map[string]string{"X-Amz-Meta-AppID": "a42"}, "", int64(len(data)), getMD5Hash(data), nil},

		// Test case 31
		// Put an empty object with a trailing slash
		{bucket, "emptydir/", []byte{}, nil, "", 0, getMD5Hash([]byte{}), nil},
		// Test case 32
		// Put an object inside the empty directory
		{bucket, "emptydir/" + object, data, nil, "", int64(len(data)), getMD5Hash(data), nil},
		// Test case 33
		// Put the empty object with a trailing slash again (refer to Test case 31), this needs to succeed
		{bucket, "emptydir/", []byte{}, nil, "", 0, getMD5Hash([]byte{}), nil},
	}

	for i, testCase := range testCases {
		in := mustGetPutObjReader(t, bytes.NewReader(testCase.inputData), testCase.intputDataSize, testCase.inputMeta["etag"], testCase.inputSHA256)
		objInfo, actualErr := obj.PutObject(context.Background(), testCase.bucketName, testCase.objName, in, ObjectOptions{UserDefined: testCase.inputMeta})
		if actualErr != nil && testCase.expectedError == nil {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: error %s.", i+1, instanceType, actualErr.Error())
			continue
		}
		if actualErr == nil && testCase.expectedError != nil {
			t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but passed instead.", i+1, instanceType, testCase.expectedError.Error())
			continue
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && actualErr != testCase.expectedError {
			t.Errorf("Test %d: %s: Expected to fail with error \"%v\", but instead failed with error \"%v\" instead.", i+1, instanceType, testCase.expectedError, actualErr)
			continue
		}
		// Test passes as expected, but the output values are verified for correctness here.
		if actualErr == nil {
			// Asserting whether the md5 output is correct.
			if expectedMD5, ok := testCase.inputMeta["etag"]; ok && expectedMD5 != objInfo.ETag {
				t.Errorf("Test %d: %s: Calculated Md5 different from the actual one %s.", i+1, instanceType, objInfo.ETag)
				continue
			}
		}
	}
}

// Wrapper for calling PutObject tests for both Erasure multiple disks case
// when quorum is not available.
func TestObjectAPIPutObjectDiskNotFound(t *testing.T) {
	ExecObjectLayerDiskAlteredTest(t, testObjectAPIPutObjectDiskNotFound)
}

// Tests validate correctness of PutObject.
func testObjectAPIPutObjectDiskNotFound(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	// Generating cases for which the PutObject fails.
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket.
	err := obj.MakeBucketWithLocation(context.Background(), bucket, BucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Creating a dummy bucket for tests.
	err = obj.MakeBucketWithLocation(context.Background(), "unused-bucket", BucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Take 8 disks down, one more we loose quorum on 16 disk node.
	for _, disk := range disks[:7] {
		os.RemoveAll(disk)
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
		{bucket, object, []byte("abcd"), map[string]string{"etag": "e2fc714c4727ee9395f324cd2e7f331f"}, int64(len("abcd")), true, "", nil},
		{bucket, object, []byte("efgh"), map[string]string{"etag": "1f7690ebdd9b4caf8fab49ca1757bf27"}, int64(len("efgh")), true, "", nil},
		{bucket, object, []byte("ijkl"), map[string]string{"etag": "09a0877d04abf8759f99adec02baf579"}, int64(len("ijkl")), true, "", nil},
		{bucket, object, []byte("mnop"), map[string]string{"etag": "e132e96a5ddad6da8b07bba6f6131fef"}, int64(len("mnop")), true, "", nil},
	}

	sha256sum := ""
	for i, testCase := range testCases {
		objInfo, actualErr := obj.PutObject(context.Background(), testCase.bucketName, testCase.objName, mustGetPutObjReader(t, bytes.NewReader(testCase.inputData), testCase.intputDataSize, testCase.inputMeta["etag"], sha256sum), ObjectOptions{UserDefined: testCase.inputMeta})
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
			if testCase.inputMeta["etag"] != objInfo.ETag {
				t.Errorf("Test %d: %s: Calculated Md5 different from the actual one %s.", i+1, instanceType, objInfo.ETag)
			}
		}
	}

	// This causes quorum failure verify.
	os.RemoveAll(disks[len(disks)-1])

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
		map[string]string{"etag": "e132e96a5ddad6da8b07bba6f6131fef"},
		int64(len("mnop")),
		false,
		"",
		InsufficientWriteQuorum{},
	}

	_, actualErr := obj.PutObject(context.Background(), testCase.bucketName, testCase.objName, mustGetPutObjReader(t, bytes.NewReader(testCase.inputData), testCase.intputDataSize, testCase.inputMeta["etag"], sha256sum), ObjectOptions{UserDefined: testCase.inputMeta})
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

// Wrapper for calling PutObject tests for both Erasure multiple disks and single node setup.
func TestObjectAPIPutObjectStaleFiles(t *testing.T) {
	ExecObjectLayerStaleFilesTest(t, testObjectAPIPutObjectStaleFiles)
}

// Tests validate correctness of PutObject.
func testObjectAPIPutObjectStaleFiles(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	// Generating cases for which the PutObject fails.
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket.
	err := obj.MakeBucketWithLocation(context.Background(), bucket, BucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	data := []byte("hello, world")
	// Create object.
	_, err = obj.PutObject(context.Background(), bucket, object, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
	if err != nil {
		// Failed to create object, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	for _, disk := range disks {
		tmpMetaDir := path.Join(disk, minioMetaTmpBucket)
		if !isDirEmpty(tmpMetaDir) {
			t.Fatalf("%s: expected: empty, got: non-empty", minioMetaTmpBucket)
		}
	}
}

// Wrapper for calling Multipart PutObject tests for both Erasure multiple disks and single node setup.
func TestObjectAPIMultipartPutObjectStaleFiles(t *testing.T) {
	ExecObjectLayerStaleFilesTest(t, testObjectAPIMultipartPutObjectStaleFiles)
}

// Tests validate correctness of PutObject.
func testObjectAPIMultipartPutObjectStaleFiles(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	// Generating cases for which the PutObject fails.
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket.
	err := obj.MakeBucketWithLocation(context.Background(), bucket, BucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	opts := ObjectOptions{}
	// Initiate Multipart Upload on the above created bucket.
	uploadID, err := obj.NewMultipartUpload(context.Background(), bucket, object, opts)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Upload part1.
	fiveMBBytes := bytes.Repeat([]byte("a"), 5*humanize.MiByte)
	md5Writer := md5.New()
	md5Writer.Write(fiveMBBytes)
	etag1 := hex.EncodeToString(md5Writer.Sum(nil))
	sha256sum := ""
	_, err = obj.PutObjectPart(context.Background(), bucket, object, uploadID, 1, mustGetPutObjReader(t, bytes.NewReader(fiveMBBytes), int64(len(fiveMBBytes)), etag1, sha256sum), opts)
	if err != nil {
		// Failed to upload object part, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Upload part2.
	data := []byte("hello, world")
	md5Writer = md5.New()
	md5Writer.Write(data)
	etag2 := hex.EncodeToString(md5Writer.Sum(nil))
	_, err = obj.PutObjectPart(context.Background(), bucket, object, uploadID, 2, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), etag2, sha256sum), opts)
	if err != nil {
		// Failed to upload object part, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Complete multipart.
	parts := []CompletePart{
		{ETag: etag1, PartNumber: 1},
		{ETag: etag2, PartNumber: 2},
	}
	_, err = obj.CompleteMultipartUpload(context.Background(), bucket, object, uploadID, parts, ObjectOptions{})
	if err != nil {
		// Failed to complete multipart upload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	for _, disk := range disks {
		tmpMetaDir := path.Join(disk, minioMetaTmpBucket)
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
// Also each of these Benchmarks are run both Erasure and FS backends.

// BenchmarkPutObjectVerySmallFS - Benchmark FS.PutObject() for object size of 10 bytes.
func BenchmarkPutObjectVerySmallFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 10)
}

// BenchmarkPutObjectVerySmallErasure - Benchmark Erasure.PutObject() for object size of 10 bytes.
func BenchmarkPutObjectVerySmallErasure(b *testing.B) {
	benchmarkPutObject(b, "Erasure", 10)
}

// BenchmarkPutObject10KbFS - Benchmark FS.PutObject() for object size of 10KB.
func BenchmarkPutObject10KbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 10*humanize.KiByte)
}

// BenchmarkPutObject10KbErasure - Benchmark Erasure.PutObject() for object size of 10KB.
func BenchmarkPutObject10KbErasure(b *testing.B) {
	benchmarkPutObject(b, "Erasure", 10*humanize.KiByte)
}

// BenchmarkPutObject100KbFS - Benchmark FS.PutObject() for object size of 100KB.
func BenchmarkPutObject100KbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 100*humanize.KiByte)
}

// BenchmarkPutObject100KbErasure - Benchmark Erasure.PutObject() for object size of 100KB.
func BenchmarkPutObject100KbErasure(b *testing.B) {
	benchmarkPutObject(b, "Erasure", 100*humanize.KiByte)
}

// BenchmarkPutObject1MbFS - Benchmark FS.PutObject() for object size of 1MB.
func BenchmarkPutObject1MbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 1*humanize.MiByte)
}

// BenchmarkPutObject1MbErasure - Benchmark Erasure.PutObject() for object size of 1MB.
func BenchmarkPutObject1MbErasure(b *testing.B) {
	benchmarkPutObject(b, "Erasure", 1*humanize.MiByte)
}

// BenchmarkPutObject5MbFS - Benchmark FS.PutObject() for object size of 5MB.
func BenchmarkPutObject5MbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 5*humanize.MiByte)
}

// BenchmarkPutObject5MbErasure - Benchmark Erasure.PutObject() for object size of 5MB.
func BenchmarkPutObject5MbErasure(b *testing.B) {
	benchmarkPutObject(b, "Erasure", 5*humanize.MiByte)
}

// BenchmarkPutObject10MbFS - Benchmark FS.PutObject() for object size of 10MB.
func BenchmarkPutObject10MbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 10*humanize.MiByte)
}

// BenchmarkPutObject10MbErasure - Benchmark Erasure.PutObject() for object size of 10MB.
func BenchmarkPutObject10MbErasure(b *testing.B) {
	benchmarkPutObject(b, "Erasure", 10*humanize.MiByte)
}

// BenchmarkPutObject25MbFS - Benchmark FS.PutObject() for object size of 25MB.
func BenchmarkPutObject25MbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 25*humanize.MiByte)

}

// BenchmarkPutObject25MbErasure - Benchmark Erasure.PutObject() for object size of 25MB.
func BenchmarkPutObject25MbErasure(b *testing.B) {
	benchmarkPutObject(b, "Erasure", 25*humanize.MiByte)
}

// BenchmarkPutObject50MbFS - Benchmark FS.PutObject() for object size of 50MB.
func BenchmarkPutObject50MbFS(b *testing.B) {
	benchmarkPutObject(b, "FS", 50*humanize.MiByte)
}

// BenchmarkPutObject50MbErasure - Benchmark Erasure.PutObject() for object size of 50MB.
func BenchmarkPutObject50MbErasure(b *testing.B) {
	benchmarkPutObject(b, "Erasure", 50*humanize.MiByte)
}

// parallel benchmarks for ObjectLayer.PutObject() .

// BenchmarkParallelPutObjectVerySmallFS - BenchmarkParallel FS.PutObject() for object size of 10 bytes.
func BenchmarkParallelPutObjectVerySmallFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 10)
}

// BenchmarkParallelPutObjectVerySmallErasure - BenchmarkParallel Erasure.PutObject() for object size of 10 bytes.
func BenchmarkParallelPutObjectVerySmallErasure(b *testing.B) {
	benchmarkPutObjectParallel(b, "Erasure", 10)
}

// BenchmarkParallelPutObject10KbFS - BenchmarkParallel FS.PutObject() for object size of 10KB.
func BenchmarkParallelPutObject10KbFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 10*humanize.KiByte)
}

// BenchmarkParallelPutObject10KbErasure - BenchmarkParallel Erasure.PutObject() for object size of 10KB.
func BenchmarkParallelPutObject10KbErasure(b *testing.B) {
	benchmarkPutObjectParallel(b, "Erasure", 10*humanize.KiByte)
}

// BenchmarkParallelPutObject100KbFS - BenchmarkParallel FS.PutObject() for object size of 100KB.
func BenchmarkParallelPutObject100KbFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 100*humanize.KiByte)
}

// BenchmarkParallelPutObject100KbErasure - BenchmarkParallel Erasure.PutObject() for object size of 100KB.
func BenchmarkParallelPutObject100KbErasure(b *testing.B) {
	benchmarkPutObjectParallel(b, "Erasure", 100*humanize.KiByte)
}

// BenchmarkParallelPutObject1MbFS - BenchmarkParallel FS.PutObject() for object size of 1MB.
func BenchmarkParallelPutObject1MbFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 1*humanize.MiByte)
}

// BenchmarkParallelPutObject1MbErasure - BenchmarkParallel Erasure.PutObject() for object size of 1MB.
func BenchmarkParallelPutObject1MbErasure(b *testing.B) {
	benchmarkPutObjectParallel(b, "Erasure", 1*humanize.MiByte)
}

// BenchmarkParallelPutObject5MbFS - BenchmarkParallel FS.PutObject() for object size of 5MB.
func BenchmarkParallelPutObject5MbFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 5*humanize.MiByte)
}

// BenchmarkParallelPutObject5MbErasure - BenchmarkParallel Erasure.PutObject() for object size of 5MB.
func BenchmarkParallelPutObject5MbErasure(b *testing.B) {
	benchmarkPutObjectParallel(b, "Erasure", 5*humanize.MiByte)
}

// BenchmarkParallelPutObject10MbFS - BenchmarkParallel FS.PutObject() for object size of 10MB.
func BenchmarkParallelPutObject10MbFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 10*humanize.MiByte)
}

// BenchmarkParallelPutObject10MbErasure - BenchmarkParallel Erasure.PutObject() for object size of 10MB.
func BenchmarkParallelPutObject10MbErasure(b *testing.B) {
	benchmarkPutObjectParallel(b, "Erasure", 10*humanize.MiByte)
}

// BenchmarkParallelPutObject25MbFS - BenchmarkParallel FS.PutObject() for object size of 25MB.
func BenchmarkParallelPutObject25MbFS(b *testing.B) {
	benchmarkPutObjectParallel(b, "FS", 25*humanize.MiByte)

}

// BenchmarkParallelPutObject25MbErasure - BenchmarkParallel Erasure.PutObject() for object size of 25MB.
func BenchmarkParallelPutObject25MbErasure(b *testing.B) {
	benchmarkPutObjectParallel(b, "Erasure", 25*humanize.MiByte)
}
