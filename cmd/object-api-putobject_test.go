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
	"crypto/md5"
	"encoding/hex"
	"errors"
	"os"
	"path"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/ioutil"
)

func md5Header(data []byte) map[string]string {
	return map[string]string{"etag": getMD5Hash(data)}
}

// Wrapper for calling PutObject tests for both Erasure multiple disks and single node setup.
func TestObjectAPIPutObjectSingle(t *testing.T) {
	ExecExtendedObjectLayerTest(t, testObjectAPIPutObject)
}

// Tests validate correctness of PutObject.
func testObjectAPIPutObject(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Generating cases for which the PutObject fails.
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket.
	err := obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Creating a dummy bucket for tests.
	err = obj.MakeBucket(context.Background(), "unused-bucket", MakeBucketOptions{})
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
		bucketName    string
		objName       string
		inputData     []byte
		inputMeta     map[string]string
		inputSHA256   string
		inputDataSize int64
		// expected error output.
		expectedMd5   string
		expectedError error
	}{
		// Cases with invalid bucket name.
		0: {bucketName: ".test", objName: "obj", inputData: []byte(""), expectedError: BucketNameInvalid{Bucket: ".test"}},
		1: {bucketName: "------", objName: "obj", inputData: []byte(""), expectedError: BucketNameInvalid{Bucket: "------"}},
		2: {
			bucketName: "$this-is-not-valid-too", objName: "obj", inputData: []byte(""),
			expectedError: BucketNameInvalid{Bucket: "$this-is-not-valid-too"},
		},
		3: {bucketName: "a", objName: "obj", inputData: []byte(""), expectedError: BucketNameInvalid{Bucket: "a"}},

		// Case with invalid object names.
		4: {bucketName: bucket, inputData: []byte(""), expectedError: ObjectNameInvalid{Bucket: bucket, Object: ""}},

		// Valid object and bucket names but non-existent bucket.
		5: {bucketName: "abc", objName: "def", inputData: []byte(""), expectedError: BucketNotFound{Bucket: "abc"}},

		// Input to replicate Md5 mismatch.
		6: {
			bucketName: bucket, objName: object, inputData: []byte(""),
			inputMeta:     map[string]string{"etag": "d41d8cd98f00b204e9800998ecf8427f"},
			expectedError: hash.BadDigest{ExpectedMD5: "d41d8cd98f00b204e9800998ecf8427f", CalculatedMD5: "d41d8cd98f00b204e9800998ecf8427e"},
		},

		// With incorrect sha256.
		7: {
			bucketName: bucket, objName: object, inputData: []byte("abcd"),
			inputMeta:   map[string]string{"etag": "e2fc714c4727ee9395f324cd2e7f331f"},
			inputSHA256: "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031580", inputDataSize: int64(len("abcd")),
			expectedError: hash.SHA256Mismatch{
				ExpectedSHA256:   "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031580",
				CalculatedSHA256: "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
			},
		},

		// Input with size more than the size of actual data inside the reader.
		8: {
			bucketName: bucket, objName: object, inputData: []byte("abcd"),
			inputMeta: map[string]string{"etag": "e2fc714c4727ee9395f324cd2e7f331e"}, inputDataSize: int64(len("abcd") + 1),
			expectedError: hash.BadDigest{ExpectedMD5: "e2fc714c4727ee9395f324cd2e7f331e", CalculatedMD5: "e2fc714c4727ee9395f324cd2e7f331f"},
		},

		// Input with size less than the size of actual data inside the reader.
		9: {
			bucketName: bucket, objName: object, inputData: []byte("abcd"),
			inputMeta: map[string]string{"etag": "900150983cd24fb0d6963f7d28e17f73"}, inputDataSize: int64(len("abcd") - 1),
			expectedError: ioutil.ErrOverread,
		},

		// Validating for success cases.
		10: {bucketName: bucket, objName: object, inputData: []byte("abcd"), inputMeta: map[string]string{"etag": "e2fc714c4727ee9395f324cd2e7f331f"}, inputDataSize: int64(len("abcd"))},
		11: {bucketName: bucket, objName: object, inputData: []byte("efgh"), inputMeta: map[string]string{"etag": "1f7690ebdd9b4caf8fab49ca1757bf27"}, inputDataSize: int64(len("efgh"))},
		12: {bucketName: bucket, objName: object, inputData: []byte("ijkl"), inputMeta: map[string]string{"etag": "09a0877d04abf8759f99adec02baf579"}, inputDataSize: int64(len("ijkl"))},
		13: {bucketName: bucket, objName: object, inputData: []byte("mnop"), inputMeta: map[string]string{"etag": "e132e96a5ddad6da8b07bba6f6131fef"}, inputDataSize: int64(len("mnop"))},

		// With no metadata
		14: {bucketName: bucket, objName: object, inputData: data, inputDataSize: int64(len(data)), expectedMd5: getMD5Hash(data)},
		15: {bucketName: bucket, objName: object, inputData: nilBytes, inputDataSize: int64(len(nilBytes)), expectedMd5: getMD5Hash(nilBytes)},
		16: {bucketName: bucket, objName: object, inputData: fiveMBBytes, inputDataSize: int64(len(fiveMBBytes)), expectedMd5: getMD5Hash(fiveMBBytes)},

		// With arbitrary metadata
		17: {bucketName: bucket, objName: object, inputData: data, inputMeta: map[string]string{"answer": "42"}, inputDataSize: int64(len(data)), expectedMd5: getMD5Hash(data)},
		18: {bucketName: bucket, objName: object, inputData: nilBytes, inputMeta: map[string]string{"answer": "42"}, inputDataSize: int64(len(nilBytes)), expectedMd5: getMD5Hash(nilBytes)},
		19: {bucketName: bucket, objName: object, inputData: fiveMBBytes, inputMeta: map[string]string{"answer": "42"}, inputDataSize: int64(len(fiveMBBytes)), expectedMd5: getMD5Hash(fiveMBBytes)},

		// With valid md5sum and sha256.
		20: {bucketName: bucket, objName: object, inputData: data, inputMeta: md5Header(data), inputSHA256: getSHA256Hash(data), inputDataSize: int64(len(data)), expectedMd5: getMD5Hash(data)},
		21: {bucketName: bucket, objName: object, inputData: nilBytes, inputMeta: md5Header(nilBytes), inputSHA256: getSHA256Hash(nilBytes), inputDataSize: int64(len(nilBytes)), expectedMd5: getMD5Hash(nilBytes)},
		22: {bucketName: bucket, objName: object, inputData: fiveMBBytes, inputMeta: md5Header(fiveMBBytes), inputSHA256: getSHA256Hash(fiveMBBytes), inputDataSize: int64(len(fiveMBBytes)), expectedMd5: getMD5Hash(fiveMBBytes)},

		// data with invalid md5sum in header
		23: {
			bucketName: bucket, objName: object, inputData: data, inputMeta: invalidMD5Header, inputDataSize: int64(len(data)), expectedMd5: getMD5Hash(data),
			expectedError: hash.BadDigest{ExpectedMD5: invalidMD5, CalculatedMD5: getMD5Hash(data)},
		},
		24: {
			bucketName: bucket, objName: object, inputData: nilBytes, inputMeta: invalidMD5Header, inputDataSize: int64(len(nilBytes)), expectedMd5: getMD5Hash(nilBytes),
			expectedError: hash.BadDigest{ExpectedMD5: invalidMD5, CalculatedMD5: getMD5Hash(nilBytes)},
		},
		25: {
			bucketName: bucket, objName: object, inputData: fiveMBBytes, inputMeta: invalidMD5Header, inputDataSize: int64(len(fiveMBBytes)), expectedMd5: getMD5Hash(fiveMBBytes),
			expectedError: hash.BadDigest{ExpectedMD5: invalidMD5, CalculatedMD5: getMD5Hash(fiveMBBytes)},
		},

		// data with size different from the actual number of bytes available in the reader
		26: {bucketName: bucket, objName: object, inputData: data, inputDataSize: int64(len(data) - 1), expectedMd5: getMD5Hash(data[:len(data)-1]), expectedError: ioutil.ErrOverread},
		27: {bucketName: bucket, objName: object, inputData: nilBytes, inputDataSize: int64(len(nilBytes) + 1), expectedMd5: getMD5Hash(nilBytes), expectedError: IncompleteBody{Bucket: bucket, Object: object}},
		28: {bucketName: bucket, objName: object, inputData: fiveMBBytes, expectedMd5: getMD5Hash(fiveMBBytes), expectedError: ioutil.ErrOverread},

		// valid data with X-Amz-Meta- meta
		29: {bucketName: bucket, objName: object, inputData: data, inputMeta: map[string]string{"X-Amz-Meta-AppID": "a42"}, inputDataSize: int64(len(data)), expectedMd5: getMD5Hash(data)},

		// Put an empty object with a trailing slash
		30: {bucketName: bucket, objName: "emptydir/", inputData: []byte{}, expectedMd5: getMD5Hash([]byte{})},
		// Put an object inside the empty directory
		31: {bucketName: bucket, objName: "emptydir/" + object, inputData: data, inputDataSize: int64(len(data)), expectedMd5: getMD5Hash(data)},
		// Put the empty object with a trailing slash again (refer to Test case 30), this needs to succeed
		32: {bucketName: bucket, objName: "emptydir/", inputData: []byte{}, expectedMd5: getMD5Hash([]byte{})},

		// With invalid crc32.
		33: {
			bucketName: bucket, objName: object, inputData: []byte("abcd"),
			inputMeta:     map[string]string{"etag": "e2fc714c4727ee9395f324cd2e7f331f", "x-amz-checksum-crc32": "abcd"},
			inputDataSize: int64(len("abcd")),
		},
	}
	for i, testCase := range testCases {
		in := mustGetPutObjReader(t, bytes.NewReader(testCase.inputData), testCase.inputDataSize, testCase.inputMeta["etag"], testCase.inputSHA256)
		objInfo, actualErr := obj.PutObject(context.Background(), testCase.bucketName, testCase.objName, in, ObjectOptions{UserDefined: testCase.inputMeta})
		if actualErr != nil && testCase.expectedError == nil {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: error %s.", i, instanceType, actualErr.Error())
			continue
		}
		if actualErr == nil && testCase.expectedError != nil {
			t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but passed instead.", i, instanceType, testCase.expectedError.Error())
			continue
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && actualErr != testCase.expectedError {
			t.Errorf("Test %d: %s: Expected to fail with error \"%v\", but instead failed with error \"%v\" instead.", i, instanceType, testCase.expectedError, actualErr)
			continue
		}
		// Test passes as expected, but the output values are verified for correctness here.
		if actualErr == nil {
			// Asserting whether the md5 output is correct.
			if expectedMD5, ok := testCase.inputMeta["etag"]; ok && expectedMD5 != objInfo.ETag {
				t.Errorf("Test %d: %s: Calculated Md5 different from the actual one %s.", i, instanceType, objInfo.ETag)
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
	err := obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Creating a dummy bucket for tests.
	err = obj.MakeBucket(context.Background(), "unused-bucket", MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Take 4 disks down, one more we loose quorum on 16 disk node.
	for _, disk := range disks[:4] {
		os.RemoveAll(disk)
	}

	testCases := []struct {
		bucketName    string
		objName       string
		inputData     []byte
		inputMeta     map[string]string
		inputDataSize int64
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
		objInfo, actualErr := obj.PutObject(context.Background(), testCase.bucketName, testCase.objName, mustGetPutObjReader(t, bytes.NewReader(testCase.inputData), testCase.inputDataSize, testCase.inputMeta["etag"], sha256sum), ObjectOptions{UserDefined: testCase.inputMeta})
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
		bucketName    string
		objName       string
		inputData     []byte
		inputMeta     map[string]string
		inputDataSize int64
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
		errErasureWriteQuorum,
	}

	_, actualErr := obj.PutObject(context.Background(), testCase.bucketName, testCase.objName, mustGetPutObjReader(t, bytes.NewReader(testCase.inputData), testCase.inputDataSize, testCase.inputMeta["etag"], sha256sum), ObjectOptions{UserDefined: testCase.inputMeta})
	if actualErr != nil && testCase.shouldPass {
		t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s.", len(testCases)+1, instanceType, actualErr.Error())
	}
	// Failed as expected, but does it fail for the expected reason.
	if actualErr != nil && !testCase.shouldPass {
		if !errors.Is(actualErr, testCase.expectedError) {
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
	err := obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{})
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
		files, err := os.ReadDir(tmpMetaDir)
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, fi := range files {
			if fi.Name() == ".trash" {
				continue
			}
			found = true
		}
		if found {
			t.Fatalf("%s: expected: empty, got: non-empty %#v", minioMetaTmpBucket, files)
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
	err := obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	opts := ObjectOptions{}
	// Initiate Multipart Upload on the above created bucket.
	res, err := obj.NewMultipartUpload(context.Background(), bucket, object, opts)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	uploadID := res.UploadID

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
		files, err := os.ReadDir(tmpMetaDir)
		if err != nil {
			// It's OK to have non-existing tmpMetaDir.
			if osIsNotExist(err) {
				continue
			}

			// Print the error
			t.Errorf("%s", err)
		}

		var found bool
		for _, fi := range files {
			if fi.Name() == ".trash" {
				continue
			}
			found = true
			break
		}

		if found {
			t.Fatalf("%s: expected: empty, got: non-empty. content: %#v", tmpMetaDir, files)
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
