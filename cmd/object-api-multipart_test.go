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
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/ioutil"
)

// Wrapper for calling NewMultipartUpload tests for both Erasure multiple disks and single node setup.
func TestObjectNewMultipartUpload(t *testing.T) {
	if runtime.GOOS == globalWindowsOSName {
		t.Skip()
	}
	ExecObjectLayerTest(t, testObjectNewMultipartUpload)
}

// Tests validate creation of new multipart upload instance.
func testObjectNewMultipartUpload(obj ObjectLayer, instanceType string, t TestErrHandler) {
	bucket := "minio-bucket"
	object := "minio-object"
	opts := ObjectOptions{}
	_, err := obj.NewMultipartUpload(context.Background(), "--", object, opts)
	if err == nil {
		t.Fatalf("%s: Expected to fail since bucket name is invalid.", instanceType)
	}

	errMsg := "Bucket not found: minio-bucket"
	// operation expected to fail since the bucket on which NewMultipartUpload is being initiated doesn't exist.
	_, err = obj.NewMultipartUpload(context.Background(), bucket, object, opts)
	if err == nil {
		t.Fatalf("%s: Expected to fail since the NewMultipartUpload is initialized on a non-existent bucket.", instanceType)
	}
	if errMsg != err.Error() {
		t.Errorf("%s, Expected to fail with Error \"%s\", but instead found \"%s\".", instanceType, errMsg, err.Error())
	}

	// Create bucket before initiating NewMultipartUpload.
	err = obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{})
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	res, err := obj.NewMultipartUpload(context.Background(), bucket, "\\", opts)
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	err = obj.AbortMultipartUpload(context.Background(), bucket, "\\", res.UploadID, opts)
	if err != nil {
		switch err.(type) {
		case InvalidUploadID:
			t.Fatalf("%s: New Multipart upload failed to create uuid file.", instanceType)
		default:
			t.Fatal(err.Error())
		}
	}
}

// Wrapper for calling AbortMultipartUpload tests for both Erasure multiple disks and single node setup.
func TestObjectAbortMultipartUpload(t *testing.T) {
	ExecObjectLayerTest(t, testObjectAbortMultipartUpload)
}

// Tests validate creation of abort multipart upload instance.
func testObjectAbortMultipartUpload(obj ObjectLayer, instanceType string, t TestErrHandler) {
	bucket := "minio-bucket"
	object := "minio-object"
	opts := ObjectOptions{}
	// Create bucket before initiating NewMultipartUpload.
	err := obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{})
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	res, err := obj.NewMultipartUpload(context.Background(), bucket, object, opts)
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	uploadID := res.UploadID

	abortTestCases := []struct {
		bucketName      string
		objName         string
		uploadID        string
		expectedErrType error
	}{
		{"--", object, uploadID, BucketNameInvalid{}},
		{"foo", object, uploadID, BucketNotFound{}},
		{bucket, object, "foo-foo", InvalidUploadID{}},
		{bucket, object, uploadID, nil},
	}

	if runtime.GOOS != globalWindowsOSName {
		abortTestCases = append(abortTestCases, struct {
			bucketName      string
			objName         string
			uploadID        string
			expectedErrType error
		}{bucket, "\\", uploadID, InvalidUploadID{}})
	}

	// Iterating over creatPartCases to generate multipart chunks.
	for i, testCase := range abortTestCases {
		err = obj.AbortMultipartUpload(context.Background(), testCase.bucketName, testCase.objName, testCase.uploadID, opts)
		if testCase.expectedErrType == nil && err != nil {
			t.Errorf("Test %d, unexpected err is received: %v, expected:%v\n", i+1, err, testCase.expectedErrType)
		}
		if testCase.expectedErrType != nil && !isSameType(err, testCase.expectedErrType) {
			t.Errorf("Test %d, unexpected err is received: %v, expected:%v\n", i+1, err, testCase.expectedErrType)
		}
	}
}

// Wrapper for calling isUploadIDExists tests for both Erasure multiple disks and single node setup.
func TestObjectAPIIsUploadIDExists(t *testing.T) {
	ExecObjectLayerTest(t, testObjectAPIIsUploadIDExists)
}

// Tests validates the validator for existence of uploadID.
func testObjectAPIIsUploadIDExists(obj ObjectLayer, instanceType string, t TestErrHandler) {
	bucket := "minio-bucket"
	object := "minio-object"

	// Create bucket before initiating NewMultipartUpload.
	err := obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	_, err = obj.NewMultipartUpload(context.Background(), bucket, object, ObjectOptions{})
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	opts := ObjectOptions{}
	err = obj.AbortMultipartUpload(context.Background(), bucket, object, "abc", opts)
	switch err.(type) {
	case InvalidUploadID:
	default:
		t.Fatalf("%s: Expected uploadIDPath to exist.", instanceType)
	}
}

// Wrapper for calling PutObjectPart tests for both Erasure multiple disks and single node setup.
func TestObjectAPIPutObjectPart(t *testing.T) {
	ExecExtendedObjectLayerTest(t, testObjectAPIPutObjectPart)
}

// Tests validate correctness of PutObjectPart.
func testObjectAPIPutObjectPart(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Generating cases for which the PutObjectPart fails.
	bucket := "minio-bucket"
	object := "minio-object"
	opts := ObjectOptions{}
	// Create bucket before initiating NewMultipartUpload.
	err := obj.MakeBucket(context.Background(), bucket, MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	// Initiate Multipart Upload on the above created bucket.
	res, err := obj.NewMultipartUpload(context.Background(), bucket, object, opts)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	err = obj.MakeBucket(context.Background(), "abc", MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	resN, err := obj.NewMultipartUpload(context.Background(), "abc", "def", opts)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	uploadID := res.UploadID
	// Creating a dummy bucket for tests.
	err = obj.MakeBucket(context.Background(), "unused-bucket", MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	obj.DeleteBucket(context.Background(), "abc", DeleteBucketOptions{})

	// Collection of non-exhaustive PutObjectPart test cases, valid errors
	// and success responses.
	testCases := []struct {
		bucketName      string
		objName         string
		uploadID        string
		PartID          int
		inputReaderData string
		inputMd5        string
		inputSHA256     string
		inputDataSize   int64
		// flag indicating whether the test should pass.
		shouldPass bool
		// expected error output.
		expectedMd5   string
		expectedError error
	}{
		// Test case  1-4.
		// Cases with invalid bucket name.
		{bucketName: ".test", objName: "obj", PartID: 1, expectedError: fmt.Errorf("%s", "Bucket name invalid: .test")},
		{bucketName: "------", objName: "obj", PartID: 1, expectedError: fmt.Errorf("%s", "Bucket name invalid: ------")},
		{
			bucketName: "$this-is-not-valid-too", objName: "obj", PartID: 1,
			expectedError: fmt.Errorf("%s", "Bucket name invalid: $this-is-not-valid-too"),
		},
		{bucketName: "a", objName: "obj", PartID: 1, expectedError: fmt.Errorf("%s", "Bucket name invalid: a")},
		// Test case - 5.
		// Case with invalid object names.
		{bucketName: bucket, PartID: 1, expectedError: fmt.Errorf("%s", "Object name invalid: minio-bucket/")},
		// Test case - 6.
		// Valid object and bucket names but non-existent bucket.
		{bucketName: "abc", objName: "def", uploadID: resN.UploadID, PartID: 1, expectedError: fmt.Errorf("%s", "Bucket not found: abc")},
		// Test Case - 7.
		// Existing bucket, but using a bucket on which NewMultipartUpload is not Initiated.
		{bucketName: "unused-bucket", objName: "def", uploadID: "xyz", PartID: 1, expectedError: fmt.Errorf("%s", "Invalid upload id xyz")},
		// Test Case - 8.
		// Existing bucket, object name different from which NewMultipartUpload is constructed from.
		// Expecting "Invalid upload id".
		{bucketName: bucket, objName: "def", uploadID: "xyz", PartID: 1, expectedError: fmt.Errorf("%s", "Invalid upload id xyz")},
		// Test Case - 9.
		// Existing bucket, bucket and object name are the ones from which NewMultipartUpload is constructed from.
		// But the uploadID is invalid.
		// Expecting "Invalid upload id".
		{bucketName: bucket, objName: object, uploadID: "xyz", PartID: 1, expectedError: fmt.Errorf("%s", "Invalid upload id xyz")},
		// Test Case - 10.
		// Case with valid UploadID, existing bucket name.
		// But using the bucket name from which NewMultipartUpload is not constructed from.
		{bucketName: "unused-bucket", objName: object, uploadID: uploadID, PartID: 1, expectedError: fmt.Errorf("%s", "Invalid upload id "+uploadID)},
		// Test Case - 11.
		// Case with valid UploadID, existing bucket name.
		// But using the object name from which NewMultipartUpload is not constructed from.
		{bucketName: bucket, objName: "none-object", uploadID: uploadID, PartID: 1, expectedError: fmt.Errorf("%s", "Invalid upload id "+uploadID)},
		// Test case - 12.
		// Input to replicate Md5 mismatch.
		{
			bucketName: bucket, objName: object, uploadID: uploadID, PartID: 1, inputMd5: "d41d8cd98f00b204e9800998ecf8427f",
			expectedError: hash.BadDigest{ExpectedMD5: "d41d8cd98f00b204e9800998ecf8427f", CalculatedMD5: "d41d8cd98f00b204e9800998ecf8427e"},
		},
		// Test case - 13.
		// When incorrect sha256 is provided.
		{
			bucketName: bucket, objName: object, uploadID: uploadID, PartID: 1, inputSHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b854",
			expectedError: hash.SHA256Mismatch{
				ExpectedSHA256:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b854",
				CalculatedSHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			},
		},
		// Test case - 14.
		// Input with size more than the size of actual data inside the reader.
		{
			bucketName: bucket, objName: object, uploadID: uploadID, PartID: 1, inputReaderData: "abcd", inputMd5: "e2fc714c4727ee9395f324cd2e7f3335", inputDataSize: int64(len("abcd") + 1),
			expectedError: hash.BadDigest{ExpectedMD5: "e2fc714c4727ee9395f324cd2e7f3335", CalculatedMD5: "e2fc714c4727ee9395f324cd2e7f331f"},
		},
		// Test case - 15.
		// Input with size less than the size of actual data inside the reader.
		{
			bucketName: bucket, objName: object, uploadID: uploadID, PartID: 1, inputReaderData: "abcd", inputMd5: "900150983cd24fb0d6963f7d28e17f73", inputDataSize: int64(len("abcd") - 1),
			expectedError: ioutil.ErrOverread,
		},

		// Test case - 16-19.
		// Validating for success cases.
		{bucketName: bucket, objName: object, uploadID: uploadID, PartID: 1, inputReaderData: "abcd", inputMd5: "e2fc714c4727ee9395f324cd2e7f331f", inputSHA256: "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589", inputDataSize: int64(len("abcd")), shouldPass: true},
		{bucketName: bucket, objName: object, uploadID: uploadID, PartID: 2, inputReaderData: "efgh", inputMd5: "1f7690ebdd9b4caf8fab49ca1757bf27", inputSHA256: "e5e088a0b66163a0a26a5e053d2a4496dc16ab6e0e3dd1adf2d16aa84a078c9d", inputDataSize: int64(len("efgh")), shouldPass: true},
		{bucketName: bucket, objName: object, uploadID: uploadID, PartID: 3, inputReaderData: "ijkl", inputMd5: "09a0877d04abf8759f99adec02baf579", inputSHA256: "005c19658919186b85618c5870463eec8d9b8c1a9d00208a5352891ba5bbe086", inputDataSize: int64(len("abcd")), shouldPass: true},
		{bucketName: bucket, objName: object, uploadID: uploadID, PartID: 4, inputReaderData: "mnop", inputMd5: "e132e96a5ddad6da8b07bba6f6131fef", inputSHA256: "f1afc31479522d6cff1ed068f93998f05a8cd3b22f5c37d7f307084f62d1d270", inputDataSize: int64(len("abcd")), shouldPass: true},
	}

	// Validate all the test cases.
	for i, testCase := range testCases {
		actualInfo, actualErr := obj.PutObjectPart(context.Background(), testCase.bucketName, testCase.objName, testCase.uploadID, testCase.PartID, mustGetPutObjReader(t, bytes.NewBufferString(testCase.inputReaderData), testCase.inputDataSize, testCase.inputMd5, testCase.inputSHA256), opts)
		// All are test cases above are expected to fail.
		if actualErr != nil && testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s.", i+1, instanceType, actualErr.Error())
		}
		if actualErr == nil && !testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead.", i+1, instanceType, testCase.expectedError.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && !testCase.shouldPass {
			if testCase.expectedError.Error() != actualErr.Error() {
				t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead.", i+1, instanceType, testCase.expectedError.Error(), actualErr.Error())
			}
		}
		// Test passes as expected, but the output values are verified for correctness here.
		if actualErr == nil && testCase.shouldPass {
			// Asserting whether the md5 output is correct.
			if testCase.inputMd5 != actualInfo.ETag {
				t.Errorf("Test %d: %s: Calculated Md5 different from the actual one %s.", i+1, instanceType, actualInfo.ETag)
			}
		}
	}
}

// Wrapper for calling TestListMultipartUploads tests for both Erasure multiple disks and single node setup.
func TestListMultipartUploads(t *testing.T) {
	ExecExtendedObjectLayerTest(t, testListMultipartUploads)
}

// testListMultipartUploads - Tests validate listing of multipart uploads.
func testListMultipartUploads(obj ObjectLayer, instanceType string, t TestErrHandler) {
	bucketNames := []string{"minio-bucket", "minio-2-bucket", "minio-3-bucket"}
	objectNames := []string{"minio-object-1.txt", "minio-object.txt", "neymar-1.jpeg", "neymar.jpeg", "parrot-1.png", "parrot.png"}
	uploadIDs := []string{}
	opts := ObjectOptions{}
	// bucketnames[0].
	// objectNames[0].
	// uploadIds [0].
	// Create bucket before initiating NewMultipartUpload.
	err := obj.MakeBucket(context.Background(), bucketNames[0], MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	// Initiate Multipart Upload on the above created bucket.
	res, err := obj.NewMultipartUpload(context.Background(), bucketNames[0], objectNames[0], opts)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	uploadIDs = append(uploadIDs, res.UploadID)

	// bucketnames[1].
	// objectNames[0].
	// uploadIds [1-3].
	// Bucket to test for multiple upload Id's for a given object.
	err = obj.MakeBucket(context.Background(), bucketNames[1], MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	for range 3 {
		// Initiate Multipart Upload on bucketNames[1] for the same object 3 times.
		//  Used to test the listing for the case of multiple uploadID's for a given object.
		res, err = obj.NewMultipartUpload(context.Background(), bucketNames[1], objectNames[0], opts)
		if err != nil {
			// Failed to create NewMultipartUpload, abort.
			t.Fatalf("%s : %s", instanceType, err.Error())
		}

		uploadIDs = append(uploadIDs, res.UploadID)
	}

	// Bucket to test for multiple objects, each with unique UUID.
	// bucketnames[2].
	// objectNames[0-2].
	// uploadIds [4-9].
	err = obj.MakeBucket(context.Background(), bucketNames[2], MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	// Initiate Multipart Upload on bucketNames[2].
	//  Used to test the listing for the case of multiple objects for a given bucket.
	for i := range 6 {
		res, err = obj.NewMultipartUpload(context.Background(), bucketNames[2], objectNames[i], opts)
		if err != nil {
			// Failed to create NewMultipartUpload, abort.
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
		// uploadIds [4-9].
		uploadIDs = append(uploadIDs, res.UploadID)
	}
	// Create multipart parts.
	// Need parts to be uploaded before MultipartLists can be called and tested.
	createPartCases := []struct {
		bucketName      string
		objName         string
		uploadID        string
		PartID          int
		inputReaderData string
		inputMd5        string
		inputDataSize   int64
		expectedMd5     string
	}{
		// Case 1-4.
		// Creating sequence of parts for same uploadID.
		// Used to ensure that the ListMultipartResult produces one output for the four parts uploaded below for the given upload ID.
		{bucketNames[0], objectNames[0], uploadIDs[0], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 2, "efgh", "1f7690ebdd9b4caf8fab49ca1757bf27", int64(len("efgh")), "1f7690ebdd9b4caf8fab49ca1757bf27"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 3, "ijkl", "09a0877d04abf8759f99adec02baf579", int64(len("abcd")), "09a0877d04abf8759f99adec02baf579"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 4, "mnop", "e132e96a5ddad6da8b07bba6f6131fef", int64(len("abcd")), "e132e96a5ddad6da8b07bba6f6131fef"},
		// Cases 5-7.
		// Create parts with 3 uploadID's for the same object.
		// Testing for listing of all the uploadID's for given object.
		// Insertion with 3 different uploadID's are done for same bucket and object.
		{bucketNames[1], objectNames[0], uploadIDs[1], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[1], objectNames[0], uploadIDs[2], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[1], objectNames[0], uploadIDs[3], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		// Case 8-13.
		// Generating parts for different objects.
		{bucketNames[2], objectNames[0], uploadIDs[4], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[2], objectNames[1], uploadIDs[5], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[2], objectNames[2], uploadIDs[6], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[2], objectNames[3], uploadIDs[7], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[2], objectNames[4], uploadIDs[8], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[2], objectNames[5], uploadIDs[9], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
	}
	sha256sum := ""
	// Iterating over creatPartCases to generate multipart chunks.
	for _, testCase := range createPartCases {
		_, err := obj.PutObjectPart(context.Background(), testCase.bucketName, testCase.objName, testCase.uploadID, testCase.PartID, mustGetPutObjReader(t, bytes.NewBufferString(testCase.inputReaderData), testCase.inputDataSize, testCase.inputMd5, sha256sum), opts)
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
	}

	// Expected Results set for asserting ListObjectMultipart test.
	listMultipartResults := []ListMultipartsInfo{
		// listMultipartResults - 1.
		// Used to check that the result produces only one output for the 4 parts uploaded in cases 1-4 of createPartCases above.
		// ListMultipartUploads doesn't list the parts.
		{
			MaxUploads: 100,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// listMultipartResults - 2.
		// Used to check that the result produces if keyMarker is set to the only available object.
		// `KeyMarker` is set.
		// ListMultipartUploads doesn't list the parts.
		{
			MaxUploads: 100,
			KeyMarker:  "minio-object-1.txt",
		},
		// listMultipartResults - 3.
		// `KeyMarker` is set, no MultipartInfo expected.
		// ListMultipartUploads doesn't list the parts.
		// `Maxupload` value is asserted.
		{
			MaxUploads: 100,
			KeyMarker:  "orange",
		},
		// listMultipartResults - 4.
		// `KeyMarker` is set, no MultipartInfo expected.
		// Maxupload value is asserted.
		{
			MaxUploads: 1,
			KeyMarker:  "orange",
		},
		// listMultipartResults - 5.
		// `KeyMarker` is set. It contains part of the objectname as `KeyPrefix`.
		// Expecting the result to contain one MultipartInfo entry and Istruncated to be false.
		{
			MaxUploads:  10,
			KeyMarker:   "min",
			IsTruncated: false,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// listMultipartResults - 6.
		// `KeyMarker` is set. It contains part of the objectname as `KeyPrefix`.
		// `MaxUploads` is set equal to the number of meta data entries in the result, the result contains only one entry.
		// Expecting the result to contain one MultipartInfo entry and IsTruncated to be false.
		{
			MaxUploads:  1,
			KeyMarker:   "min",
			IsTruncated: false,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// listMultipartResults - 7.
		// `KeyMarker` is set. It contains part of the objectname as `KeyPrefix`.
		// Testing for the case with `MaxUploads` set to 0.
		// Expecting the result to contain no MultipartInfo entry since `MaxUploads` is set to 0.
		// Expecting `IsTruncated` to be true.
		{
			MaxUploads:  0,
			KeyMarker:   "min",
			IsTruncated: true,
		},
		// listMultipartResults - 8.
		// `KeyMarker` is set. It contains part of the objectname as KeyPrefix.
		// Testing for the case with `MaxUploads` set to 0.
		// Expecting the result to contain no MultipartInfo entry since `MaxUploads` is set to 0.
		// Expecting `isTruncated` to be true.
		{
			MaxUploads:  0,
			KeyMarker:   "min",
			IsTruncated: true,
		},
		// listMultipartResults - 9.
		// `KeyMarker` is set. It contains part of the objectname as KeyPrefix.
		// `KeyMarker` is set equal to the object name in the result.
		// Expecting the result to contain one MultipartInfo entry and IsTruncated to be false.
		{
			MaxUploads:  2,
			KeyMarker:   "minio-object",
			IsTruncated: false,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// listMultipartResults - 10.
		// Prefix is set. It is set equal to the object name.
		// MaxUploads is set more than number of meta data entries in the result.
		// Expecting the result to contain one MultipartInfo entry and IsTruncated to be false.
		{
			MaxUploads:  2,
			Prefix:      "minio-object-1.txt",
			IsTruncated: false,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// listMultipartResults - 11.
		// Setting `Prefix` to contain the object name as its prefix.
		// MaxUploads is set more than number of meta data entries in the result.
		// Expecting the result to contain one MultipartInfo entry and IsTruncated to be false.
		{
			MaxUploads:  2,
			Prefix:      "min",
			IsTruncated: false,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// listMultipartResults - 12.
		// Setting `Prefix` to contain the object name as its prefix.
		// MaxUploads is set equal to number of meta data entries in the result.
		// Expecting the result to contain one MultipartInfo entry and IsTruncated to be false.
		{
			MaxUploads:  1,
			Prefix:      "min",
			IsTruncated: false,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// listMultipartResults - 13.
		// `Prefix` is set. It doesn't contain object name as its preifx.
		// MaxUploads is set more than number of meta data entries in the result.
		// Expecting no `Uploads` metadata.
		{
			MaxUploads:  2,
			Prefix:      "orange",
			IsTruncated: false,
		},
		// listMultipartResults - 14.
		// `Prefix` is set. It doesn't contain object name as its preifx.
		// MaxUploads is set more than number of meta data entries in the result.
		// Expecting the result to contain 0 uploads and isTruncated to false.
		{
			MaxUploads:  2,
			Prefix:      "Asia",
			IsTruncated: false,
		},
		// listMultipartResults - 15.
		// Setting `Delimiter`.
		// MaxUploads is set more than number of meta data entries in the result.
		// Expecting the result to contain one MultipartInfo entry and IsTruncated to be false.
		{
			MaxUploads:  2,
			Delimiter:   SlashSeparator,
			Prefix:      "",
			IsTruncated: false,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[0],
				},
			},
		},
		// listMultipartResults - 16.
		// Testing for listing of 3 uploadID's for a given object.
		// Will be used to list on bucketNames[1].
		{
			MaxUploads: 100,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[1],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[2],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[3],
				},
			},
		},
		// listMultipartResults - 17.
		// Testing for listing of 3 uploadID's (uploadIDs[1-3]) for a given object with uploadID Marker set.
		// uploadIDs[1] is set as UploadMarker, Expecting it to be skipped in the result.
		// uploadIDs[2] and uploadIDs[3] are expected to be in the result.
		// Istruncted is expected to be false.
		// Will be used to list on bucketNames[1].
		{
			MaxUploads:     100,
			KeyMarker:      "minio-object-1.txt",
			UploadIDMarker: uploadIDs[1],
			IsTruncated:    false,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[2],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[3],
				},
			},
		},
		// listMultipartResults - 18.
		// Testing for listing of 3 uploadID's (uploadIDs[1-3])  for a given object with uploadID Marker set.
		// uploadIDs[2] is set as UploadMarker, Expecting it to be skipped in the result.
		// Only uploadIDs[3] are expected to be in the result.
		// Istruncted is expected to be false.
		// Will be used to list on bucketNames[1].
		{
			MaxUploads:     100,
			KeyMarker:      "minio-object-1.txt",
			UploadIDMarker: uploadIDs[2],
			IsTruncated:    false,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[3],
				},
			},
		},
		// listMultipartResults - 19.
		// Testing for listing of 3 uploadID's for a given object, setting maxKeys to be 2.
		// There are 3 MultipartInfo in the result (uploadIDs[1-3]), it should be truncated to 2.
		// Since there is only single object for bucketNames[1], the NextKeyMarker is set to its name.
		// The last entry in the result, uploadIDs[2], that is should be set as NextUploadIDMarker.
		// Will be used to list on bucketNames[1].
		{
			MaxUploads:         2,
			IsTruncated:        true,
			NextKeyMarker:      objectNames[0],
			NextUploadIDMarker: uploadIDs[2],
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[1],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[2],
				},
			},
		},
		// listMultipartResults - 20.
		// Testing for listing of 3 uploadID's for a given object, setting maxKeys to be 1.
		// There are 3 MultipartInfo in the result (uploadIDs[1-3]), it should be truncated to 1.
		// The last entry in the result, uploadIDs[1], that is should be set as NextUploadIDMarker.
		// Will be used to list on bucketNames[1].
		{
			MaxUploads:         1,
			IsTruncated:        true,
			NextKeyMarker:      objectNames[0],
			NextUploadIDMarker: uploadIDs[1],
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[1],
				},
			},
		},
		// listMultipartResults - 21.
		// Testing for listing of 3 uploadID's for a given object, setting maxKeys to be 3.
		// There are 3 MultipartInfo in the result (uploadIDs[1-3]), hence no truncation is expected.
		// Since all the MultipartInfo is listed, expecting no values for NextUploadIDMarker and NextKeyMarker.
		// Will be used to list on bucketNames[1].
		{
			MaxUploads:  3,
			IsTruncated: false,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[1],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[2],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[3],
				},
			},
		},
		// listMultipartResults - 22.
		// Testing for listing of 3 uploadID's for a given object, setting `prefix` to be "min".
		// Will be used to list on bucketNames[1].
		{
			MaxUploads:  10,
			IsTruncated: false,
			Prefix:      "min",
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[1],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[2],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[3],
				},
			},
		},
		// listMultipartResults - 23.
		// Testing for listing of 3 uploadID's for a given object
		// setting `prefix` to be "orange".
		// Will be used to list on bucketNames[1].
		{
			MaxUploads:  10,
			IsTruncated: false,
			Prefix:      "orange",
		},
		// listMultipartResults - 24.
		// Testing for listing of 3 uploadID's for a given object.
		// setting `prefix` to be "Asia".
		// Will be used to list on bucketNames[1].
		{
			MaxUploads:  10,
			IsTruncated: false,
			Prefix:      "Asia",
		},
		// listMultipartResults - 25.
		// Testing for listing of 3 uploadID's for a given object.
		// setting `prefix` and uploadIDMarker.
		// Will be used to list on bucketNames[1].
		{
			MaxUploads:     10,
			KeyMarker:      "minio-object-1.txt",
			IsTruncated:    false,
			Prefix:         "min",
			UploadIDMarker: uploadIDs[1],
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[2],
				},
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[3],
				},
			},
		},

		// Operations on bucket 2.
		// listMultipartResults - 26.
		// checking listing everything.
		{
			MaxUploads:  100,
			IsTruncated: false,

			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[4],
				},
				{
					Object:   objectNames[1],
					UploadID: uploadIDs[5],
				},
				{
					Object:   objectNames[2],
					UploadID: uploadIDs[6],
				},
				{
					Object:   objectNames[3],
					UploadID: uploadIDs[7],
				},
				{
					Object:   objectNames[4],
					UploadID: uploadIDs[8],
				},
				{
					Object:   objectNames[5],
					UploadID: uploadIDs[9],
				},
			},
		},
		// listMultipartResults - 27.
		//  listing with `prefix` "min".
		{
			MaxUploads:  100,
			IsTruncated: false,
			Prefix:      "min",
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[4],
				},
				{
					Object:   objectNames[1],
					UploadID: uploadIDs[5],
				},
			},
		},
		// listMultipartResults - 28.
		//  listing with `prefix` "ney".
		{
			MaxUploads:  100,
			IsTruncated: false,
			Prefix:      "ney",
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[2],
					UploadID: uploadIDs[6],
				},
				{
					Object:   objectNames[3],
					UploadID: uploadIDs[7],
				},
			},
		},
		// listMultipartResults - 29.
		//  listing with `prefix` "parrot".
		{
			MaxUploads:  100,
			IsTruncated: false,
			Prefix:      "parrot",
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[4],
					UploadID: uploadIDs[8],
				},
				{
					Object:   objectNames[5],
					UploadID: uploadIDs[9],
				},
			},
		},
		// listMultipartResults - 30.
		//  listing with `prefix` "neymar.jpeg".
		// prefix set to object name.
		{
			MaxUploads:  100,
			IsTruncated: false,
			Prefix:      "neymar.jpeg",
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[3],
					UploadID: uploadIDs[7],
				},
			},
		},

		// listMultipartResults - 31.
		// checking listing with marker set to 3.
		// `NextUploadIDMarker` is expected to be set on last uploadID in the result.
		// `NextKeyMarker` is expected to be set on the last object key in the list.
		{
			MaxUploads:         3,
			IsTruncated:        true,
			NextUploadIDMarker: uploadIDs[6],
			NextKeyMarker:      objectNames[2],
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[4],
				},
				{
					Object:   objectNames[1],
					UploadID: uploadIDs[5],
				},
				{
					Object:   objectNames[2],
					UploadID: uploadIDs[6],
				},
			},
		},
		// listMultipartResults - 32.
		// checking listing with marker set to no of objects in the list.
		// `NextUploadIDMarker` is expected to be empty since all results are listed.
		// `NextKeyMarker` is expected to be empty since all results are listed.
		{
			MaxUploads:  6,
			IsTruncated: false,
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[0],
					UploadID: uploadIDs[4],
				},
				{
					Object:   objectNames[1],
					UploadID: uploadIDs[5],
				},
				{
					Object:   objectNames[2],
					UploadID: uploadIDs[6],
				},
				{
					Object:   objectNames[3],
					UploadID: uploadIDs[7],
				},
				{
					Object:   objectNames[4],
					UploadID: uploadIDs[8],
				},
				{
					Object:   objectNames[5],
					UploadID: uploadIDs[9],
				},
			},
		},
		// listMultipartResults - 33.
		// checking listing with `UploadIDMarker` set.
		{
			MaxUploads:     10,
			IsTruncated:    false,
			UploadIDMarker: uploadIDs[6],
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[3],
					UploadID: uploadIDs[7],
				},
				{
					Object:   objectNames[4],
					UploadID: uploadIDs[8],
				},
				{
					Object:   objectNames[5],
					UploadID: uploadIDs[9],
				},
			},
		},
		// listMultipartResults - 34.
		// checking listing with `KeyMarker` set.
		{
			MaxUploads:  10,
			IsTruncated: false,
			KeyMarker:   objectNames[3],
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[4],
					UploadID: uploadIDs[8],
				},
				{
					Object:   objectNames[5],
					UploadID: uploadIDs[9],
				},
			},
		},
		// listMultipartResults - 35.
		// Checking listing with `Prefix` and `KeyMarker`.
		// No upload MultipartInfo in the result expected since KeyMarker is set to last Key in the result.
		{
			MaxUploads:  10,
			IsTruncated: false,
			Prefix:      "minio-object",
			KeyMarker:   objectNames[1],
		},
		// listMultipartResults - 36.
		// checking listing with `Prefix` and `UploadIDMarker` set.
		{
			MaxUploads:     10,
			IsTruncated:    false,
			Prefix:         "minio",
			UploadIDMarker: uploadIDs[4],
			Uploads: []MultipartInfo{
				{
					Object:   objectNames[1],
					UploadID: uploadIDs[5],
				},
			},
		},
		// listMultipartResults - 37.
		// Checking listing with `KeyMarker` and `UploadIDMarker` set.
		{
			MaxUploads:     10,
			IsTruncated:    false,
			KeyMarker:      "minio-object.txt",
			UploadIDMarker: uploadIDs[5],
		},
	}

	// Collection of non-exhaustive ListMultipartUploads test cases, valid errors
	// and success responses.
	testCases := []struct {
		// Inputs to ListMultipartUploads.
		bucket         string
		prefix         string
		keyMarker      string
		uploadIDMarker string
		delimiter      string
		maxUploads     int
		// Expected output of ListMultipartUploads.
		expectedResult ListMultipartsInfo
		expectedErr    error
		// Flag indicating whether the test is expected to pass or not.
		shouldPass bool
	}{
		// Test cases with invalid bucket names ( Test number 1-4 ).
		{".test", "", "", "", "", 0, ListMultipartsInfo{}, BucketNameInvalid{Bucket: ".test"}, false},
		{"Test", "", "", "", "", 0, ListMultipartsInfo{}, BucketNameInvalid{Bucket: "Test"}, false},
		{"---", "", "", "", "", 0, ListMultipartsInfo{}, BucketNameInvalid{Bucket: "---"}, false},
		{"ad", "", "", "", "", 0, ListMultipartsInfo{}, BucketNameInvalid{Bucket: "ad"}, false},
		// Valid bucket names, but they do not exist (Test number 5-7).
		{"volatile-bucket-1", "", "", "", "", 0, ListMultipartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-1"}, false},
		{"volatile-bucket-2", "", "", "", "", 0, ListMultipartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-2"}, false},
		{"volatile-bucket-3", "", "", "", "", 0, ListMultipartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-3"}, false},
		// Valid, existing bucket, delimiter not supported, returns empty values (Test number 8-9).
		{bucketNames[0], "", "", "", "*", 0, ListMultipartsInfo{Delimiter: "*"}, nil, true},
		{bucketNames[0], "", "", "", "-", 0, ListMultipartsInfo{Delimiter: "-"}, nil, true},
		// If marker is *after* the last possible object from the prefix it should return an empty list.
		{
			bucketNames[0], "Asia", "europe-object", "", "", 0,
			ListMultipartsInfo{KeyMarker: "europe-object", Prefix: "Asia", IsTruncated: false},
			nil, true,
		},
		// Setting an invalid combination of uploadIDMarker and Marker (Test number 11-12).
		{
			bucketNames[0], "asia", "asia/europe/", "abc", "", 0,
			ListMultipartsInfo{},
			fmt.Errorf("Invalid combination of uploadID marker '%s' and marker '%s'", "abc", "asia/europe/"), false,
		},
		{
			// Contains a base64 padding character
			bucketNames[0], "asia", "asia/europe", "abc=", "", 0,
			ListMultipartsInfo{},
			fmt.Errorf("Malformed upload id %s", "abc="), false,
		},

		// Setting up valid case of ListMultiPartUploads.
		// Test case with multiple parts for a single uploadID (Test number 13).
		{bucketNames[0], "", "", "", "", 100, listMultipartResults[0], nil, true},
		// Test with a KeyMarker (Test number 14-17).
		{bucketNames[0], "", "minio-object-1.txt", "", "", 100, listMultipartResults[1], nil, true},
		{bucketNames[0], "", "orange", "", "", 100, listMultipartResults[2], nil, true},
		{bucketNames[0], "", "orange", "", "", 1, listMultipartResults[3], nil, true},
		{bucketNames[0], "", "min", "", "", 10, listMultipartResults[4], nil, true},
		// Test case with keyMarker set equal to number of parts in the result. (Test number 18).
		{bucketNames[0], "", "min", "", "", 1, listMultipartResults[5], nil, true},
		// Test case with keyMarker set to 0. (Test number 19).
		{bucketNames[0], "", "min", "", "", 0, listMultipartResults[6], nil, true},
		// Test case with keyMarker less than 0. (Test number 20).
		// {bucketNames[0], "", "min", "", "", -1, listMultipartResults[7], nil, true},
		// The result contains only one entry. The  KeyPrefix is set to the object name in the result.
		// Expecting the result to skip the KeyPrefix entry in the result (Test number 21).
		{bucketNames[0], "", "minio-object", "", "", 2, listMultipartResults[8], nil, true},
		// Test case containing prefix values.
		// Setting prefix to be equal to object name.(Test number 22).
		{bucketNames[0], "minio-object-1.txt", "", "", "", 2, listMultipartResults[9], nil, true},
		// Setting `prefix` to contain the object name as its prefix (Test number 23).
		{bucketNames[0], "min", "", "", "", 2, listMultipartResults[10], nil, true},
		// Setting `prefix` to contain the object name as its prefix (Test number 24).
		{bucketNames[0], "min", "", "", "", 1, listMultipartResults[11], nil, true},
		// Setting `prefix` to not to contain the object name as its prefix (Test number 25-26).
		{bucketNames[0], "orange", "", "", "", 2, listMultipartResults[12], nil, true},
		{bucketNames[0], "Asia", "", "", "", 2, listMultipartResults[13], nil, true},
		// setting delimiter (Test number 27).
		{bucketNames[0], "", "", "", SlashSeparator, 2, listMultipartResults[14], nil, true},
		// Test case with multiple uploadID listing for given object (Test number 28).
		{bucketNames[1], "", "", "", "", 100, listMultipartResults[15], nil, true},
		// Test case with multiple uploadID listing for given object, but uploadID marker set.
		// Testing whether the marker entry is skipped (Test number 29-30).
		{bucketNames[1], "", "minio-object-1.txt", uploadIDs[1], "", 100, listMultipartResults[16], nil, true},
		{bucketNames[1], "", "minio-object-1.txt", uploadIDs[2], "", 100, listMultipartResults[17], nil, true},
		// Test cases with multiple uploadID listing for a given object (Test number 31-32).
		// MaxKeys set to values lesser than the number of entries in the MultipartInfo.
		// IsTruncated is expected to be true.
		{bucketNames[1], "", "", "", "", 2, listMultipartResults[18], nil, true},
		{bucketNames[1], "", "", "", "", 1, listMultipartResults[19], nil, true},
		// MaxKeys set to the value which is equal to no of entries in the MultipartInfo (Test number 33).
		// In case of bucketNames[1], there are 3 entries.
		// Since all available entries are listed, IsTruncated is expected to be false
		// and NextMarkers are expected to empty.
		{bucketNames[1], "", "", "", "", 3, listMultipartResults[20], nil, true},
		// Adding  prefix (Test number 34-36).
		{bucketNames[1], "min", "", "", "", 10, listMultipartResults[21], nil, true},
		{bucketNames[1], "orange", "", "", "", 10, listMultipartResults[22], nil, true},
		{bucketNames[1], "Asia", "", "", "", 10, listMultipartResults[23], nil, true},
		// Test case with `Prefix` and `UploadIDMarker` (Test number 37).
		{bucketNames[1], "min", "minio-object-1.txt", uploadIDs[1], "", 10, listMultipartResults[24], nil, true},
		// Test case for bucket with multiple objects in it.
		// Bucket used : `bucketNames[2]`.
		// Objects used: `objectNames[1-5]`.
		// UploadId's used: uploadIds[4-8].
		// (Test number 39).
		{bucketNames[2], "", "", "", "", 100, listMultipartResults[25], nil, true},
		// Test cases with prefixes.
		// Testing listing with prefix set to "min" (Test number 40)	.
		{bucketNames[2], "min", "", "", "", 100, listMultipartResults[26], nil, true},
		// Testing listing with prefix set to "ney" (Test number 41).
		{bucketNames[2], "ney", "", "", "", 100, listMultipartResults[27], nil, true},
		// Testing listing with prefix set to "par" (Test number 42).
		{bucketNames[2], "parrot", "", "", "", 100, listMultipartResults[28], nil, true},
		// Testing listing with prefix set to object name "neymar.jpeg" (Test number 43).
		{bucketNames[2], "neymar.jpeg", "", "", "", 100, listMultipartResults[29], nil, true},
		// Testing listing with `MaxUploads` set to 3 (Test number 44).
		{bucketNames[2], "", "", "", "", 3, listMultipartResults[30], nil, true},
		// In case of bucketNames[2], there are 6 entries (Test number 45).
		// Since all available entries are listed, IsTruncated is expected to be false
		// and NextMarkers are expected to empty.
		{bucketNames[2], "", "", "", "", 6, listMultipartResults[31], nil, true},
		// Test case with `KeyMarker` (Test number 47).
		{bucketNames[2], "", objectNames[3], "", "", 10, listMultipartResults[33], nil, true},
		// Test case with `prefix` and `KeyMarker` (Test number 48).
		{bucketNames[2], "minio-object", objectNames[1], "", "", 10, listMultipartResults[34], nil, true},
	}

	for i, testCase := range testCases {
		// fmt.Println(i+1, testCase) // uncomment to peek into the test cases.
		actualResult, actualErr := obj.ListMultipartUploads(context.Background(), testCase.bucket, testCase.prefix, testCase.keyMarker, testCase.uploadIDMarker, testCase.delimiter, testCase.maxUploads)
		if actualErr != nil && testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s", i+1, instanceType, actualErr.Error())
		}
		if actualErr == nil && !testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead", i+1, instanceType, testCase.expectedErr.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && !testCase.shouldPass {
			if !strings.Contains(actualErr.Error(), testCase.expectedErr.Error()) {
				t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead", i+1, instanceType, testCase.expectedErr.Error(), actualErr.Error())
			}
		}
		// Passes as expected, but asserting the results.
		if actualErr == nil && testCase.shouldPass {
			expectedResult := testCase.expectedResult
			// Asserting the MaxUploads.
			if actualResult.MaxUploads != expectedResult.MaxUploads {
				t.Errorf("Test %d: %s: Expected the MaxUploads to be %d, but instead found it to be %d", i+1, instanceType, expectedResult.MaxUploads, actualResult.MaxUploads)
			}
			// Asserting Prefix.
			if actualResult.Prefix != expectedResult.Prefix {
				t.Errorf("Test %d: %s: Expected Prefix to be \"%s\", but instead found it to be \"%s\"", i+1, instanceType, expectedResult.Prefix, actualResult.Prefix)
			}
			// Asserting Delimiter.
			if actualResult.Delimiter != expectedResult.Delimiter {
				t.Errorf("Test %d: %s: Expected Delimiter to be \"%s\", but instead found it to be \"%s\"", i+1, instanceType, expectedResult.Delimiter, actualResult.Delimiter)
			}
			// Asserting the keyMarker.
			if actualResult.KeyMarker != expectedResult.KeyMarker {
				t.Errorf("Test %d: %s: Expected keyMarker to be \"%s\", but instead found it to be \"%s\"", i+1, instanceType, expectedResult.KeyMarker, actualResult.KeyMarker)
			}
		}
	}
}

// Wrapper for calling TestListObjectPartsStale tests for both Erasure multiple disks and single node setup.
func TestListObjectPartsStale(t *testing.T) {
	ExecObjectLayerDiskAlteredTest(t, testListObjectPartsStale)
}

// testListObjectPartsStale - Tests validate listing of object parts when parts are stale
func testListObjectPartsStale(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	bucketNames := []string{"minio-bucket", "minio-2-bucket"}
	objectNames := []string{"minio-object-1.txt"}
	uploadIDs := []string{}

	globalStorageClass.Update(storageclass.Config{
		RRS: storageclass.StorageClass{
			Parity: 2,
		},
		Standard: storageclass.StorageClass{
			Parity: 4,
		},
	})

	// bucketnames[0].
	// objectNames[0].
	// uploadIds [0].
	// Create bucket before initiating NewMultipartUpload.
	err := obj.MakeBucket(context.Background(), bucketNames[0], MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	opts := ObjectOptions{}
	// Initiate Multipart Upload on the above created bucket.
	res, err := obj.NewMultipartUpload(context.Background(), bucketNames[0], objectNames[0], opts)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	z := obj.(*erasureServerPools)
	er := z.serverPools[0].sets[0]

	uploadIDs = append(uploadIDs, res.UploadID)

	// Create multipart parts.
	// Need parts to be uploaded before MultipartLists can be called and tested.
	createPartCases := []struct {
		bucketName      string
		objName         string
		uploadID        string
		PartID          int
		inputReaderData string
		inputMd5        string
		inputDataSize   int64
		expectedMd5     string
	}{
		// Case 1-4.
		// Creating sequence of parts for same uploadID.
		// Used to ensure that the ListMultipartResult produces one output for the four parts uploaded below for the given upload ID.
		{bucketNames[0], objectNames[0], uploadIDs[0], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 2, "efgh", "1f7690ebdd9b4caf8fab49ca1757bf27", int64(len("efgh")), "1f7690ebdd9b4caf8fab49ca1757bf27"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 3, "ijkl", "09a0877d04abf8759f99adec02baf579", int64(len("abcd")), "09a0877d04abf8759f99adec02baf579"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 4, "mnop", "e132e96a5ddad6da8b07bba6f6131fef", int64(len("abcd")), "e132e96a5ddad6da8b07bba6f6131fef"},
	}
	sha256sum := ""
	// Iterating over creatPartCases to generate multipart chunks.
	for _, testCase := range createPartCases {
		_, err := obj.PutObjectPart(context.Background(), testCase.bucketName, testCase.objName, testCase.uploadID, testCase.PartID, mustGetPutObjReader(t, bytes.NewBufferString(testCase.inputReaderData), testCase.inputDataSize, testCase.inputMd5, sha256sum), opts)
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
	}

	erasureDisks := er.getDisks()
	uploadIDPath := er.getUploadIDDir(bucketNames[0], objectNames[0], uploadIDs[0])
	dataDirs, err := erasureDisks[0].ListDir(context.Background(), minioMetaMultipartBucket, minioMetaMultipartBucket, uploadIDPath, -1)
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	var dataDir string
	for _, folder := range dataDirs {
		if strings.HasSuffix(folder, SlashSeparator) {
			dataDir = folder
			break
		}
	}

	toDel := (len(erasureDisks) / 2) + 1
	for _, disk := range erasureDisks[:toDel] {
		disk.DeleteBulk(context.Background(), minioMetaMultipartBucket, []string{pathJoin(uploadIDPath, dataDir, "part.2")}...)
	}

	partInfos := []ListPartsInfo{
		// partinfos - 0.
		{
			Bucket:   bucketNames[0],
			Object:   objectNames[0],
			MaxParts: 10,
			UploadID: uploadIDs[0],
			Parts: []PartInfo{
				{
					PartNumber: 1,
					Size:       4,
					ETag:       "e2fc714c4727ee9395f324cd2e7f331f",
				},
				{
					PartNumber: 3,
					Size:       4,
					ETag:       "09a0877d04abf8759f99adec02baf579",
				},
				{
					PartNumber: 4,
					Size:       4,
					ETag:       "e132e96a5ddad6da8b07bba6f6131fef",
				},
			},
		},
		// partinfos - 1.
		{
			Bucket:   bucketNames[0],
			Object:   objectNames[0],
			MaxParts: 3,
			UploadID: uploadIDs[0],
			Parts: []PartInfo{
				{
					PartNumber: 1,
					Size:       4,
					ETag:       "e2fc714c4727ee9395f324cd2e7f331f",
				},
				{
					PartNumber: 3,
					Size:       4,
					ETag:       "09a0877d04abf8759f99adec02baf579",
				},
				{
					PartNumber: 4,
					Size:       4,
					ETag:       "e132e96a5ddad6da8b07bba6f6131fef",
				},
			},
		},
		// partinfos - 2.
		{
			Bucket:               bucketNames[0],
			Object:               objectNames[0],
			MaxParts:             2,
			NextPartNumberMarker: 3,
			IsTruncated:          true,
			UploadID:             uploadIDs[0],
			Parts: []PartInfo{
				{
					PartNumber: 1,
					Size:       4,
					ETag:       "e2fc714c4727ee9395f324cd2e7f331f",
				},
				{
					PartNumber: 3,
					Size:       4,
					ETag:       "09a0877d04abf8759f99adec02baf579",
				},
			},
		},
		// partinfos - 3.
		{
			Bucket:           bucketNames[0],
			Object:           objectNames[0],
			MaxParts:         2,
			IsTruncated:      false,
			UploadID:         uploadIDs[0],
			PartNumberMarker: 3,
			Parts: []PartInfo{
				{
					PartNumber: 4,
					Size:       4,
					ETag:       "e132e96a5ddad6da8b07bba6f6131fef",
				},
			},
		},
		// partinfos - 4.
		{
			Bucket:           bucketNames[0],
			Object:           objectNames[0],
			MaxParts:         2,
			IsTruncated:      false,
			UploadID:         uploadIDs[0],
			PartNumberMarker: 4,
		},
		// partinfos - 5.
		{
			Bucket:           bucketNames[0],
			Object:           objectNames[0],
			MaxParts:         2,
			IsTruncated:      false,
			UploadID:         uploadIDs[0],
			PartNumberMarker: 100,
		},
	}

	// Collection of non-exhaustive ListObjectParts test cases, valid errors
	// and success responses.
	testCases := []struct {
		bucket           string
		object           string
		uploadID         string
		partNumberMarker int
		maxParts         int
		// Expected output of ListPartsInfo.
		expectedResult ListPartsInfo
		expectedErr    error
		// Flag indicating whether the test is expected to pass or not.
		shouldPass bool
	}{
		// Test cases with invalid bucket names (Test number 1-4).
		{".test", "", "", 0, 0, ListPartsInfo{}, BucketNameInvalid{Bucket: ".test"}, false},
		{"Test", "", "", 0, 0, ListPartsInfo{}, BucketNameInvalid{Bucket: "Test"}, false},
		{"---", "", "", 0, 0, ListPartsInfo{}, BucketNameInvalid{Bucket: "---"}, false},
		{"ad", "", "", 0, 0, ListPartsInfo{}, BucketNameInvalid{Bucket: "ad"}, false},
		// Test cases for listing uploadID with single part.
		// Valid bucket names, but they do not exist (Test number 5-7).
		{"volatile-bucket-1", "test1", "", 0, 0, ListPartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-1"}, false},
		{"volatile-bucket-2", "test1", "", 0, 0, ListPartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-2"}, false},
		{"volatile-bucket-3", "test1", "", 0, 0, ListPartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-3"}, false},
		// Test case for Asserting for invalid objectName (Test number 8).
		{bucketNames[0], "", "", 0, 0, ListPartsInfo{}, ObjectNameInvalid{Bucket: bucketNames[0]}, false},
		// Asserting for Invalid UploadID (Test number 9).
		{bucketNames[0], objectNames[0], "abc", 0, 0, ListPartsInfo{}, InvalidUploadID{UploadID: "abc"}, false},
		// Test case for uploadID with multiple parts (Test number 12).
		{bucketNames[0], objectNames[0], uploadIDs[0], 0, 10, partInfos[0], nil, true},
		// Test case with maxParts set to less than number of parts (Test number 13).
		{bucketNames[0], objectNames[0], uploadIDs[0], 0, 3, partInfos[1], nil, true},
		// Test case with partNumberMarker set (Test number 14).
		{bucketNames[0], objectNames[0], uploadIDs[0], 0, 2, partInfos[2], nil, true},
		// Test case with partNumberMarker set (Test number 15).
		{bucketNames[0], objectNames[0], uploadIDs[0], 3, 2, partInfos[3], nil, true},
		// Test case with partNumberMarker set (Test number 16).
		{bucketNames[0], objectNames[0], uploadIDs[0], 4, 2, partInfos[4], nil, true},
		// Test case with partNumberMarker set (Test number 17).
		{bucketNames[0], objectNames[0], uploadIDs[0], 100, 2, partInfos[5], nil, true},
	}

	for i, testCase := range testCases {
		actualResult, actualErr := obj.ListObjectParts(context.Background(), testCase.bucket, testCase.object, testCase.uploadID, testCase.partNumberMarker, testCase.maxParts, ObjectOptions{})
		if actualErr != nil && testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s", i+1, instanceType, actualErr.Error())
		}
		if actualErr == nil && !testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead", i+1, instanceType, testCase.expectedErr.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && !testCase.shouldPass {
			if !strings.Contains(actualErr.Error(), testCase.expectedErr.Error()) {
				t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead", i+1, instanceType, testCase.expectedErr, actualErr)
			}
		}
		// Passes as expected, but asserting the results.
		if actualErr == nil && testCase.shouldPass {
			expectedResult := testCase.expectedResult
			// Asserting the MaxParts.
			if actualResult.MaxParts != expectedResult.MaxParts {
				t.Errorf("Test %d: %s: Expected the MaxParts to be %d, but instead found it to be %d", i+1, instanceType, expectedResult.MaxParts, actualResult.MaxParts)
			}
			// Asserting Object Name.
			if actualResult.Object != expectedResult.Object {
				t.Errorf("Test %d: %s: Expected Object name to be \"%s\", but instead found it to be \"%s\"", i+1, instanceType, expectedResult.Object, actualResult.Object)
			}
			// Asserting UploadID.
			if actualResult.UploadID != expectedResult.UploadID {
				t.Errorf("Test %d: %s: Expected UploadID to be \"%s\", but instead found it to be \"%s\"", i+1, instanceType, expectedResult.UploadID, actualResult.UploadID)
			}
			// Asserting NextPartNumberMarker.
			if actualResult.NextPartNumberMarker != expectedResult.NextPartNumberMarker {
				t.Errorf("Test %d: %s: Expected NextPartNumberMarker to be \"%d\", but instead found it to be \"%d\"", i+1, instanceType, expectedResult.NextPartNumberMarker, actualResult.NextPartNumberMarker)
			}
			// Asserting PartNumberMarker.
			if actualResult.PartNumberMarker != expectedResult.PartNumberMarker {
				t.Errorf("Test %d: %s: Expected PartNumberMarker to be \"%d\", but instead found it to be \"%d\"", i+1, instanceType, expectedResult.PartNumberMarker, actualResult.PartNumberMarker)
			}
			// Asserting the BucketName.
			if actualResult.Bucket != expectedResult.Bucket {
				t.Errorf("Test %d: %s: Expected Bucket to be \"%s\", but instead found it to be \"%s\"", i+1, instanceType, expectedResult.Bucket, actualResult.Bucket)
			}
			// Asserting IsTruncated.
			if actualResult.IsTruncated != testCase.expectedResult.IsTruncated {
				t.Errorf("Test %d: %s: Expected IsTruncated to be \"%v\", but found it to \"%v\"", i+1, instanceType, expectedResult.IsTruncated, actualResult.IsTruncated)
			}
			// Asserting the number of Parts.
			if len(expectedResult.Parts) != len(actualResult.Parts) {
				t.Errorf("Test %d: %s: Expected the result to contain info of %d Parts, but found %d instead", i+1, instanceType, len(expectedResult.Parts), len(actualResult.Parts))
			} else {
				// Iterating over the partInfos and asserting the fields.
				for j, actualMetaData := range actualResult.Parts {
					//  Asserting the PartNumber in the PartInfo.
					if actualMetaData.PartNumber != expectedResult.Parts[j].PartNumber {
						t.Errorf("Test %d: %s: Part %d: Expected PartNumber to be \"%d\", but instead found \"%d\"", i+1, instanceType, j+1, expectedResult.Parts[j].PartNumber, actualMetaData.PartNumber)
					}
					//  Asserting the Size in the PartInfo.
					if actualMetaData.Size != expectedResult.Parts[j].Size {
						t.Errorf("Test %d: %s: Part %d: Expected Part Size to be \"%d\", but instead found \"%d\"", i+1, instanceType, j+1, expectedResult.Parts[j].Size, actualMetaData.Size)
					}
					//  Asserting the ETag in the PartInfo.
					if actualMetaData.ETag != expectedResult.Parts[j].ETag {
						t.Errorf("Test %d: %s: Part %d: Expected Etag to be \"%s\", but instead found \"%s\"", i+1, instanceType, j+1, expectedResult.Parts[j].ETag, actualMetaData.ETag)
					}
				}
			}
		}
	}
}

// Wrapper for calling TestListObjectPartsDiskNotFound tests for both Erasure multiple disks and single node setup.
func TestListObjectPartsDiskNotFound(t *testing.T) {
	ExecObjectLayerDiskAlteredTest(t, testListObjectPartsDiskNotFound)
}

// testListObjectParts - Tests validate listing of object parts when disks go offline.
func testListObjectPartsDiskNotFound(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	bucketNames := []string{"minio-bucket", "minio-2-bucket"}
	objectNames := []string{"minio-object-1.txt"}
	uploadIDs := []string{}

	globalStorageClass.Update(storageclass.Config{
		RRS: storageclass.StorageClass{
			Parity: 2,
		},
		Standard: storageclass.StorageClass{
			Parity: 4,
		},
	})

	// bucketnames[0].
	// objectNames[0].
	// uploadIds [0].
	// Create bucket before initiating NewMultipartUpload.
	err := obj.MakeBucket(context.Background(), bucketNames[0], MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	opts := ObjectOptions{}
	// Initiate Multipart Upload on the above created bucket.
	res, err := obj.NewMultipartUpload(context.Background(), bucketNames[0], objectNames[0], opts)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	z := obj.(*erasureServerPools)
	er := z.serverPools[0].sets[0]

	erasureDisks := er.getDisks()
	ridx := rand.Intn(len(erasureDisks))

	z.serverPools[0].erasureDisksMu.Lock()
	er.getDisks = func() []StorageAPI {
		erasureDisks[ridx] = newNaughtyDisk(erasureDisks[ridx], nil, errFaultyDisk)
		return erasureDisks
	}
	z.serverPools[0].erasureDisksMu.Unlock()

	uploadIDs = append(uploadIDs, res.UploadID)

	// Create multipart parts.
	// Need parts to be uploaded before MultipartLists can be called and tested.
	createPartCases := []struct {
		bucketName      string
		objName         string
		uploadID        string
		PartID          int
		inputReaderData string
		inputMd5        string
		inputDataSize   int64
		expectedMd5     string
	}{
		// Case 1-4.
		// Creating sequence of parts for same uploadID.
		// Used to ensure that the ListMultipartResult produces one output for the four parts uploaded below for the given upload ID.
		{bucketNames[0], objectNames[0], uploadIDs[0], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 2, "efgh", "1f7690ebdd9b4caf8fab49ca1757bf27", int64(len("efgh")), "1f7690ebdd9b4caf8fab49ca1757bf27"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 3, "ijkl", "09a0877d04abf8759f99adec02baf579", int64(len("abcd")), "09a0877d04abf8759f99adec02baf579"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 4, "mnop", "e132e96a5ddad6da8b07bba6f6131fef", int64(len("abcd")), "e132e96a5ddad6da8b07bba6f6131fef"},
	}
	sha256sum := ""
	// Iterating over creatPartCases to generate multipart chunks.
	for _, testCase := range createPartCases {
		_, err := obj.PutObjectPart(context.Background(), testCase.bucketName, testCase.objName, testCase.uploadID, testCase.PartID, mustGetPutObjReader(t, bytes.NewBufferString(testCase.inputReaderData), testCase.inputDataSize, testCase.inputMd5, sha256sum), opts)
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
	}

	partInfos := []ListPartsInfo{
		// partinfos - 0.
		{
			Bucket:   bucketNames[0],
			Object:   objectNames[0],
			MaxParts: 10,
			UploadID: uploadIDs[0],
			Parts: []PartInfo{
				{
					PartNumber: 1,
					Size:       4,
					ETag:       "e2fc714c4727ee9395f324cd2e7f331f",
				},
				{
					PartNumber: 2,
					Size:       4,
					ETag:       "1f7690ebdd9b4caf8fab49ca1757bf27",
				},
				{
					PartNumber: 3,
					Size:       4,
					ETag:       "09a0877d04abf8759f99adec02baf579",
				},
				{
					PartNumber: 4,
					Size:       4,
					ETag:       "e132e96a5ddad6da8b07bba6f6131fef",
				},
			},
		},
		// partinfos - 1.
		{
			Bucket:               bucketNames[0],
			Object:               objectNames[0],
			MaxParts:             3,
			NextPartNumberMarker: 3,
			IsTruncated:          true,
			UploadID:             uploadIDs[0],
			Parts: []PartInfo{
				{
					PartNumber: 1,
					Size:       4,
					ETag:       "e2fc714c4727ee9395f324cd2e7f331f",
				},
				{
					PartNumber: 2,
					Size:       4,
					ETag:       "1f7690ebdd9b4caf8fab49ca1757bf27",
				},
				{
					PartNumber: 3,
					Size:       4,
					ETag:       "09a0877d04abf8759f99adec02baf579",
				},
			},
		},
		// partinfos - 2.
		{
			Bucket:           bucketNames[0],
			Object:           objectNames[0],
			MaxParts:         2,
			IsTruncated:      false,
			UploadID:         uploadIDs[0],
			PartNumberMarker: 3,
			Parts: []PartInfo{
				{
					PartNumber: 4,
					Size:       4,
					ETag:       "e132e96a5ddad6da8b07bba6f6131fef",
				},
			},
		},
	}

	// Collection of non-exhaustive ListObjectParts test cases, valid errors
	// and success responses.
	testCases := []struct {
		bucket           string
		object           string
		uploadID         string
		partNumberMarker int
		maxParts         int
		// Expected output of ListPartsInfo.
		expectedResult ListPartsInfo
		expectedErr    error
		// Flag indicating whether the test is expected to pass or not.
		shouldPass bool
	}{
		// Test cases with invalid bucket names (Test number 1-4).
		{".test", "", "", 0, 0, ListPartsInfo{}, BucketNameInvalid{Bucket: ".test"}, false},
		{"Test", "", "", 0, 0, ListPartsInfo{}, BucketNameInvalid{Bucket: "Test"}, false},
		{"---", "", "", 0, 0, ListPartsInfo{}, BucketNameInvalid{Bucket: "---"}, false},
		{"ad", "", "", 0, 0, ListPartsInfo{}, BucketNameInvalid{Bucket: "ad"}, false},
		// Test cases for listing uploadID with single part.
		// Valid bucket names, but they do not exist (Test number 5-7).
		{"volatile-bucket-1", "test1", "", 0, 0, ListPartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-1"}, false},
		{"volatile-bucket-2", "test1", "", 0, 0, ListPartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-2"}, false},
		{"volatile-bucket-3", "test1", "", 0, 0, ListPartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-3"}, false},
		// Test case for Asserting for invalid objectName (Test number 8).
		{bucketNames[0], "", "", 0, 0, ListPartsInfo{}, ObjectNameInvalid{Bucket: bucketNames[0]}, false},
		// Asserting for Invalid UploadID (Test number 9).
		{bucketNames[0], objectNames[0], "abc", 0, 0, ListPartsInfo{}, InvalidUploadID{UploadID: "abc"}, false},
		// Test case for uploadID with multiple parts (Test number 12).
		{bucketNames[0], objectNames[0], uploadIDs[0], 0, 10, partInfos[0], nil, true},
		// Test case with maxParts set to less than number of parts (Test number 13).
		{bucketNames[0], objectNames[0], uploadIDs[0], 0, 3, partInfos[1], nil, true},
		// Test case with partNumberMarker set (Test number 14)-.
		{bucketNames[0], objectNames[0], uploadIDs[0], 3, 2, partInfos[2], nil, true},
	}

	for i, testCase := range testCases {
		actualResult, actualErr := obj.ListObjectParts(context.Background(), testCase.bucket, testCase.object, testCase.uploadID, testCase.partNumberMarker, testCase.maxParts, ObjectOptions{})
		if actualErr != nil && testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s", i+1, instanceType, actualErr.Error())
		}
		if actualErr == nil && !testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead", i+1, instanceType, testCase.expectedErr.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && !testCase.shouldPass {
			if !strings.Contains(actualErr.Error(), testCase.expectedErr.Error()) {
				t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead", i+1, instanceType, testCase.expectedErr, actualErr)
			}
		}
		// Passes as expected, but asserting the results.
		if actualErr == nil && testCase.shouldPass {
			expectedResult := testCase.expectedResult
			// Asserting the MaxParts.
			if actualResult.MaxParts != expectedResult.MaxParts {
				t.Errorf("Test %d: %s: Expected the MaxParts to be %d, but instead found it to be %d", i+1, instanceType, expectedResult.MaxParts, actualResult.MaxParts)
			}
			// Asserting Object Name.
			if actualResult.Object != expectedResult.Object {
				t.Errorf("Test %d: %s: Expected Object name to be \"%s\", but instead found it to be \"%s\"", i+1, instanceType, expectedResult.Object, actualResult.Object)
			}
			// Asserting UploadID.
			if actualResult.UploadID != expectedResult.UploadID {
				t.Errorf("Test %d: %s: Expected UploadID to be \"%s\", but instead found it to be \"%s\"", i+1, instanceType, expectedResult.UploadID, actualResult.UploadID)
			}
			// Asserting NextPartNumberMarker.
			if actualResult.NextPartNumberMarker != expectedResult.NextPartNumberMarker {
				t.Errorf("Test %d: %s: Expected NextPartNumberMarker to be \"%d\", but instead found it to be \"%d\"", i+1, instanceType, expectedResult.NextPartNumberMarker, actualResult.NextPartNumberMarker)
			}
			// Asserting PartNumberMarker.
			if actualResult.PartNumberMarker != expectedResult.PartNumberMarker {
				t.Errorf("Test %d: %s: Expected PartNumberMarker to be \"%d\", but instead found it to be \"%d\"", i+1, instanceType, expectedResult.PartNumberMarker, actualResult.PartNumberMarker)
			}
			// Asserting the BucketName.
			if actualResult.Bucket != expectedResult.Bucket {
				t.Errorf("Test %d: %s: Expected Bucket to be \"%s\", but instead found it to be \"%s\"", i+1, instanceType, expectedResult.Bucket, actualResult.Bucket)
			}
			// Asserting IsTruncated.
			if actualResult.IsTruncated != testCase.expectedResult.IsTruncated {
				t.Errorf("Test %d: %s: Expected IsTruncated to be \"%v\", but found it to \"%v\"", i+1, instanceType, expectedResult.IsTruncated, actualResult.IsTruncated)
			}
			// Asserting the number of Parts.
			if len(expectedResult.Parts) != len(actualResult.Parts) {
				t.Errorf("Test %d: %s: Expected the result to contain info of %d Parts, but found %d instead", i+1, instanceType, len(expectedResult.Parts), len(actualResult.Parts))
			} else {
				// Iterating over the partInfos and asserting the fields.
				for j, actualMetaData := range actualResult.Parts {
					//  Asserting the PartNumber in the PartInfo.
					if actualMetaData.PartNumber != expectedResult.Parts[j].PartNumber {
						t.Errorf("Test %d: %s: Part %d: Expected PartNumber to be \"%d\", but instead found \"%d\"", i+1, instanceType, j+1, expectedResult.Parts[j].PartNumber, actualMetaData.PartNumber)
					}
					//  Asserting the Size in the PartInfo.
					if actualMetaData.Size != expectedResult.Parts[j].Size {
						t.Errorf("Test %d: %s: Part %d: Expected Part Size to be \"%d\", but instead found \"%d\"", i+1, instanceType, j+1, expectedResult.Parts[j].Size, actualMetaData.Size)
					}
					//  Asserting the ETag in the PartInfo.
					if actualMetaData.ETag != expectedResult.Parts[j].ETag {
						t.Errorf("Test %d: %s: Part %d: Expected Etag to be \"%s\", but instead found \"%s\"", i+1, instanceType, j+1, expectedResult.Parts[j].ETag, actualMetaData.ETag)
					}
				}
			}
		}
	}
}

// Wrapper for calling TestListObjectParts tests for both Erasure multiple disks and single node setup.
func TestListObjectParts(t *testing.T) {
	ExecObjectLayerTest(t, testListObjectParts)
}

// testListObjectParts - test validate listing of object parts.
func testListObjectParts(obj ObjectLayer, instanceType string, t TestErrHandler) {
	bucketNames := []string{"minio-bucket", "minio-2-bucket"}
	objectNames := []string{"minio-object-1.txt"}
	uploadIDs := []string{}
	opts := ObjectOptions{}
	// bucketnames[0].
	// objectNames[0].
	// uploadIds [0].
	// Create bucket before initiating NewMultipartUpload.
	err := obj.MakeBucket(context.Background(), bucketNames[0], MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	// Initiate Multipart Upload on the above created bucket.
	res, err := obj.NewMultipartUpload(context.Background(), bucketNames[0], objectNames[0], opts)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	uploadIDs = append(uploadIDs, res.UploadID)

	// Create multipart parts.
	// Need parts to be uploaded before MultipartLists can be called and tested.
	createPartCases := []struct {
		bucketName      string
		objName         string
		uploadID        string
		PartID          int
		inputReaderData string
		inputMd5        string
		inputDataSize   int64
		expectedMd5     string
	}{
		// Case 1-4.
		// Creating sequence of parts for same uploadID.
		// Used to ensure that the ListMultipartResult produces one output for the four parts uploaded below for the given upload ID.
		{bucketNames[0], objectNames[0], uploadIDs[0], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd")), "e2fc714c4727ee9395f324cd2e7f331f"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 2, "efgh", "1f7690ebdd9b4caf8fab49ca1757bf27", int64(len("efgh")), "1f7690ebdd9b4caf8fab49ca1757bf27"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 3, "ijkl", "09a0877d04abf8759f99adec02baf579", int64(len("abcd")), "09a0877d04abf8759f99adec02baf579"},
		{bucketNames[0], objectNames[0], uploadIDs[0], 4, "mnop", "e132e96a5ddad6da8b07bba6f6131fef", int64(len("abcd")), "e132e96a5ddad6da8b07bba6f6131fef"},
	}
	sha256sum := ""
	// Iterating over creatPartCases to generate multipart chunks.
	for _, testCase := range createPartCases {
		_, err := obj.PutObjectPart(context.Background(), testCase.bucketName, testCase.objName, testCase.uploadID, testCase.PartID, mustGetPutObjReader(t, bytes.NewBufferString(testCase.inputReaderData), testCase.inputDataSize, testCase.inputMd5, sha256sum), opts)
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
	}

	partInfos := []ListPartsInfo{
		// partinfos - 0.
		{
			Bucket:   bucketNames[0],
			Object:   objectNames[0],
			MaxParts: 10,
			UploadID: uploadIDs[0],
			Parts: []PartInfo{
				{
					PartNumber: 1,
					Size:       4,
					ETag:       "e2fc714c4727ee9395f324cd2e7f331f",
				},
				{
					PartNumber: 2,
					Size:       4,
					ETag:       "1f7690ebdd9b4caf8fab49ca1757bf27",
				},
				{
					PartNumber: 3,
					Size:       4,
					ETag:       "09a0877d04abf8759f99adec02baf579",
				},
				{
					PartNumber: 4,
					Size:       4,
					ETag:       "e132e96a5ddad6da8b07bba6f6131fef",
				},
			},
		},
		// partinfos - 1.
		{
			Bucket:               bucketNames[0],
			Object:               objectNames[0],
			MaxParts:             3,
			NextPartNumberMarker: 3,
			IsTruncated:          true,
			UploadID:             uploadIDs[0],
			Parts: []PartInfo{
				{
					PartNumber: 1,
					Size:       4,
					ETag:       "e2fc714c4727ee9395f324cd2e7f331f",
				},
				{
					PartNumber: 2,
					Size:       4,
					ETag:       "1f7690ebdd9b4caf8fab49ca1757bf27",
				},
				{
					PartNumber: 3,
					Size:       4,
					ETag:       "09a0877d04abf8759f99adec02baf579",
				},
			},
		},
		// partinfos - 2.
		{
			Bucket:           bucketNames[0],
			Object:           objectNames[0],
			MaxParts:         2,
			IsTruncated:      false,
			UploadID:         uploadIDs[0],
			PartNumberMarker: 3,
			Parts: []PartInfo{
				{
					PartNumber: 4,
					Size:       4,
					ETag:       "e132e96a5ddad6da8b07bba6f6131fef",
				},
			},
		},
	}

	// Collection of non-exhaustive ListObjectParts test cases, valid errors
	// and success responses.
	testCases := []struct {
		bucket           string
		object           string
		uploadID         string
		partNumberMarker int
		maxParts         int
		// Expected output of ListPartsInfo.
		expectedResult ListPartsInfo
		expectedErr    error
		// Flag indicating whether the test is expected to pass or not.
		shouldPass bool
	}{
		// Test cases with invalid bucket names (Test number 1-4).
		{".test", "", "", 0, 0, ListPartsInfo{}, BucketNameInvalid{Bucket: ".test"}, false},
		{"Test", "", "", 0, 0, ListPartsInfo{}, BucketNameInvalid{Bucket: "Test"}, false},
		{"---", "", "", 0, 0, ListPartsInfo{}, BucketNameInvalid{Bucket: "---"}, false},
		{"ad", "", "", 0, 0, ListPartsInfo{}, BucketNameInvalid{Bucket: "ad"}, false},
		// Test cases for listing uploadID with single part.
		// Valid bucket names, but they do not exist (Test number 5-7).
		{"volatile-bucket-1", "test1", "", 0, 0, ListPartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-1"}, false},
		{"volatile-bucket-2", "test1", "", 0, 0, ListPartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-2"}, false},
		{"volatile-bucket-3", "test1", "", 0, 0, ListPartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-3"}, false},
		// Test case for Asserting for invalid objectName (Test number 8).
		{bucketNames[0], "", "", 0, 0, ListPartsInfo{}, ObjectNameInvalid{Bucket: bucketNames[0]}, false},
		// Asserting for Invalid UploadID (Test number 9).
		{bucketNames[0], objectNames[0], "abc", 0, 0, ListPartsInfo{}, InvalidUploadID{UploadID: "abc"}, false},
		// Test case for uploadID with multiple parts (Test number 12).
		{bucketNames[0], objectNames[0], uploadIDs[0], 0, 10, partInfos[0], nil, true},
		// Test case with maxParts set to less than number of parts (Test number 13).
		{bucketNames[0], objectNames[0], uploadIDs[0], 0, 3, partInfos[1], nil, true},
		// Test case with partNumberMarker set (Test number 14)-.
		{bucketNames[0], objectNames[0], uploadIDs[0], 3, 2, partInfos[2], nil, true},
	}

	for i, testCase := range testCases {
		actualResult, actualErr := obj.ListObjectParts(context.Background(), testCase.bucket, testCase.object, testCase.uploadID, testCase.partNumberMarker, testCase.maxParts, opts)
		if actualErr != nil && testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to pass, but failed with: <ERROR> %s", i+1, instanceType, actualErr.Error())
		}
		if actualErr == nil && !testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected to fail with <ERROR> \"%s\", but passed instead", i+1, instanceType, testCase.expectedErr.Error())
		}
		// Failed as expected, but does it fail for the expected reason.
		if actualErr != nil && !testCase.shouldPass {
			if !strings.Contains(actualErr.Error(), testCase.expectedErr.Error()) {
				t.Errorf("Test %d: %s: Expected to fail with error \"%s\", but instead failed with error \"%s\" instead", i+1, instanceType, testCase.expectedErr.Error(), actualErr.Error())
			}
		}
		// Passes as expected, but asserting the results.
		if actualErr == nil && testCase.shouldPass {
			expectedResult := testCase.expectedResult
			// Asserting the MaxParts.
			if actualResult.MaxParts != expectedResult.MaxParts {
				t.Errorf("Test %d: %s: Expected the MaxParts to be %d, but instead found it to be %d", i+1, instanceType, expectedResult.MaxParts, actualResult.MaxParts)
			}
			// Asserting Object Name.
			if actualResult.Object != expectedResult.Object {
				t.Errorf("Test %d: %s: Expected Object name to be \"%s\", but instead found it to be \"%s\"", i+1, instanceType, expectedResult.Object, actualResult.Object)
			}
			// Asserting UploadID.
			if actualResult.UploadID != expectedResult.UploadID {
				t.Errorf("Test %d: %s: Expected UploadID to be \"%s\", but instead found it to be \"%s\"", i+1, instanceType, expectedResult.UploadID, actualResult.UploadID)
			}
			// Asserting PartNumberMarker.
			if actualResult.PartNumberMarker != expectedResult.PartNumberMarker {
				t.Errorf("Test %d: %s: Expected PartNumberMarker to be \"%d\", but instead found it to be \"%d\"", i+1, instanceType, expectedResult.PartNumberMarker, actualResult.PartNumberMarker)
			}
			// Asserting the BucketName.
			if actualResult.Bucket != expectedResult.Bucket {
				t.Errorf("Test %d: %s: Expected Bucket to be \"%s\", but instead found it to be \"%s\"", i+1, instanceType, expectedResult.Bucket, actualResult.Bucket)
			}

			// Asserting IsTruncated.
			if actualResult.IsTruncated != testCase.expectedResult.IsTruncated {
				t.Errorf("Test %d: %s: Expected IsTruncated to be \"%v\", but found it to \"%v\"", i+1, instanceType, expectedResult.IsTruncated, actualResult.IsTruncated)
				continue
			}
			// Asserting NextPartNumberMarker.
			if actualResult.NextPartNumberMarker != expectedResult.NextPartNumberMarker {
				t.Errorf("Test %d: %s: Expected NextPartNumberMarker to be \"%d\", but instead found it to be \"%d\"", i+1, instanceType, expectedResult.NextPartNumberMarker, actualResult.NextPartNumberMarker)
				continue
			}
			// Asserting the number of Parts.
			if len(expectedResult.Parts) != len(actualResult.Parts) {
				t.Errorf("Test %d: %s: Expected the result to contain info of %d Parts, but found %d instead", i+1, instanceType, len(expectedResult.Parts), len(actualResult.Parts))
				continue
			}
			// Iterating over the partInfos and asserting the fields.
			for j, actualMetaData := range actualResult.Parts {
				//  Asserting the PartNumber in the PartInfo.
				if actualMetaData.PartNumber != expectedResult.Parts[j].PartNumber {
					t.Errorf("Test %d: %s: Part %d: Expected PartNumber to be \"%d\", but instead found \"%d\"", i+1, instanceType, j+1, expectedResult.Parts[j].PartNumber, actualMetaData.PartNumber)
				}
				//  Asserting the Size in the PartInfo.
				if actualMetaData.Size != expectedResult.Parts[j].Size {
					t.Errorf("Test %d: %s: Part %d: Expected Part Size to be \"%d\", but instead found \"%d\"", i+1, instanceType, j+1, expectedResult.Parts[j].Size, actualMetaData.Size)
				}
				//  Asserting the ETag in the PartInfo.
				if actualMetaData.ETag != expectedResult.Parts[j].ETag {
					t.Errorf("Test %d: %s: Part %d: Expected Etag to be \"%s\", but instead found \"%s\"", i+1, instanceType, j+1, expectedResult.Parts[j].ETag, actualMetaData.ETag)
				}
			}
		}
	}
}

// Test for validating complete Multipart upload.
func TestObjectCompleteMultipartUpload(t *testing.T) {
	ExecExtendedObjectLayerTest(t, testObjectCompleteMultipartUpload)
}

// Tests validate CompleteMultipart functionality.
func testObjectCompleteMultipartUpload(obj ObjectLayer, instanceType string, t TestErrHandler) {
	var err error
	bucketNames := []string{"minio-bucket", "minio-2-bucket"}
	objectNames := []string{"minio-object-1.txt"}
	uploadIDs := []string{}

	// bucketnames[0].
	// objectNames[0].
	// uploadIds [0].
	// Create bucket before initiating NewMultipartUpload.
	err = obj.MakeBucket(context.Background(), bucketNames[0], MakeBucketOptions{})
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}
	// Initiate Multipart Upload on the above created bucket.
	res, err := obj.NewMultipartUpload(context.Background(), bucketNames[0], objectNames[0], ObjectOptions{UserDefined: map[string]string{"X-Amz-Meta-Id": "id"}})
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	uploadIDs = append(uploadIDs, res.UploadID)
	// Parts with size greater than 5 MiB.
	// Generating a 6MiB byte array.
	validPart := bytes.Repeat([]byte("abcdef"), 1*humanize.MiByte)
	validPartMD5 := getMD5Hash(validPart)
	// Create multipart parts.
	// Need parts to be uploaded before CompleteMultiPartUpload can be called tested.
	parts := []struct {
		bucketName      string
		objName         string
		uploadID        string
		PartID          int
		inputReaderData string
		inputMd5        string
		inputDataSize   int64
	}{
		// Case 1-4.
		// Creating sequence of parts for same uploadID.
		{bucketNames[0], objectNames[0], uploadIDs[0], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd"))},
		{bucketNames[0], objectNames[0], uploadIDs[0], 2, "efgh", "1f7690ebdd9b4caf8fab49ca1757bf27", int64(len("efgh"))},
		{bucketNames[0], objectNames[0], uploadIDs[0], 3, "ijkl", "09a0877d04abf8759f99adec02baf579", int64(len("abcd"))},
		{bucketNames[0], objectNames[0], uploadIDs[0], 4, "mnop", "e132e96a5ddad6da8b07bba6f6131fef", int64(len("abcd"))},
		// Part with size larger than 5Mb.
		{bucketNames[0], objectNames[0], uploadIDs[0], 5, string(validPart), validPartMD5, int64(len(validPart))},
		{bucketNames[0], objectNames[0], uploadIDs[0], 6, string(validPart), validPartMD5, int64(len(validPart))},
		{bucketNames[0], objectNames[0], uploadIDs[0], 7, string(validPart), validPartMD5, int64(len(validPart))},
	}
	sha256sum := ""
	var opts ObjectOptions
	// Iterating over creatPartCases to generate multipart chunks.
	for _, part := range parts {
		_, err = obj.PutObjectPart(context.Background(), part.bucketName, part.objName, part.uploadID, part.PartID, mustGetPutObjReader(t, bytes.NewBufferString(part.inputReaderData), part.inputDataSize, part.inputMd5, sha256sum), opts)
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err)
		}
	}
	// Parts to be sent as input for CompleteMultipartUpload.
	inputParts := []struct {
		parts []CompletePart
	}{
		// inputParts - 0.
		// Case for replicating ETag mismatch.
		{
			[]CompletePart{
				{ETag: "abcd", PartNumber: 1},
			},
		},
		// inputParts - 1.
		// should error out with part too small.
		{
			[]CompletePart{
				{ETag: "e2fc714c4727ee9395f324cd2e7f331f", PartNumber: 1},
				{ETag: "1f7690ebdd9b4caf8fab49ca1757bf27", PartNumber: 2},
			},
		},
		// inputParts - 2.
		// Case with invalid Part number.
		{
			[]CompletePart{
				{ETag: "e2fc714c4727ee9395f324cd2e7f331f", PartNumber: 10},
			},
		},
		// inputParts - 3.
		// Case with valid part.
		// Part size greater than 5MB.
		{
			[]CompletePart{
				{ETag: fmt.Sprintf("\"\"\"\"\"%s\"\"\"", validPartMD5), PartNumber: 5},
			},
		},
		// inputParts - 4.
		// Used to verify that the other remaining parts are deleted after
		// a successful call to CompleteMultipartUpload.
		{
			[]CompletePart{
				{ETag: validPartMD5, PartNumber: 6},
			},
		},
	}
	s3MD5 := getCompleteMultipartMD5(inputParts[3].parts)

	// Test cases with sample input values for CompleteMultipartUpload.
	testCases := []struct {
		bucket   string
		object   string
		uploadID string
		parts    []CompletePart
		// Expected output of CompleteMultipartUpload.
		expectedS3MD5 string
		expectedErr   error
		// Flag indicating whether the test is expected to pass or not.
		shouldPass bool
	}{
		// Test cases with invalid bucket names (Test number 1-4).
		{".test", "", "", []CompletePart{}, "", BucketNameInvalid{Bucket: ".test"}, false},
		{"Test", "", "", []CompletePart{}, "", BucketNameInvalid{Bucket: "Test"}, false},
		{"---", "", "", []CompletePart{}, "", BucketNameInvalid{Bucket: "---"}, false},
		{"ad", "", "", []CompletePart{}, "", BucketNameInvalid{Bucket: "ad"}, false},
		// Test cases for listing uploadID with single part.
		// Valid bucket names, but they do not exist (Test number 5-7).
		{"volatile-bucket-1", "test1", "", []CompletePart{}, "", BucketNotFound{Bucket: "volatile-bucket-1"}, false},
		{"volatile-bucket-2", "test1", "", []CompletePart{}, "", BucketNotFound{Bucket: "volatile-bucket-2"}, false},
		{"volatile-bucket-3", "test1", "", []CompletePart{}, "", BucketNotFound{Bucket: "volatile-bucket-3"}, false},
		// Test case for Asserting for invalid objectName (Test number 8).
		{bucketNames[0], "", "", []CompletePart{}, "", ObjectNameInvalid{Bucket: bucketNames[0]}, false},
		// Asserting for Invalid UploadID (Test number 9).
		{bucketNames[0], objectNames[0], "abc", []CompletePart{}, "", InvalidUploadID{UploadID: "abc"}, false},
		// Test case with invalid Part Etag (Test number 10-11).
		{bucketNames[0], objectNames[0], uploadIDs[0], []CompletePart{{ETag: "abc"}}, "", InvalidPart{}, false},
		{bucketNames[0], objectNames[0], uploadIDs[0], []CompletePart{{ETag: "abcz"}}, "", InvalidPart{}, false},
		// Part number 0 doesn't exist, expecting InvalidPart error (Test number 12).
		{bucketNames[0], objectNames[0], uploadIDs[0], []CompletePart{{ETag: "abcd", PartNumber: 0}}, "", InvalidPart{}, false},
		// // Upload and PartNumber exists, But a deliberate ETag mismatch is introduced (Test number 13).
		{bucketNames[0], objectNames[0], uploadIDs[0], inputParts[0].parts, "", InvalidPart{}, false},
		// Test case with non existent object name (Test number 14).
		{bucketNames[0], "my-object", uploadIDs[0], []CompletePart{{ETag: "abcd", PartNumber: 1}}, "", InvalidUploadID{UploadID: uploadIDs[0]}, false},
		// Testing for Part being too small (Test number 15).
		{bucketNames[0], objectNames[0], uploadIDs[0], inputParts[1].parts, "", PartTooSmall{PartNumber: 1}, false},
		// TestCase with invalid Part Number (Test number 16).
		// Should error with Invalid Part .
		{bucketNames[0], objectNames[0], uploadIDs[0], inputParts[2].parts, "", InvalidPart{}, false},
		// Test case with unsorted parts (Test number 17).
		{bucketNames[0], objectNames[0], uploadIDs[0], inputParts[3].parts, s3MD5, nil, true},
		// The other parts will be flushed after a successful CompletePart (Test number 18).
		// the case above successfully completes CompleteMultipartUpload, the remaining Parts will be flushed.
		// Expecting to fail with Invalid UploadID.
		{bucketNames[0], objectNames[0], uploadIDs[0], inputParts[4].parts, "", InvalidUploadID{UploadID: uploadIDs[0]}, false},
	}

	for _, testCase := range testCases {
		t.(*testing.T).Run("", func(t *testing.T) {
			opts = ObjectOptions{}
			actualResult, actualErr := obj.CompleteMultipartUpload(t.Context(), testCase.bucket, testCase.object, testCase.uploadID, testCase.parts, ObjectOptions{})
			if actualErr != nil && testCase.shouldPass {
				t.Errorf("%s: Expected to pass, but failed with: <ERROR> %s", instanceType, actualErr)
			}
			if actualErr == nil && !testCase.shouldPass {
				t.Errorf("%s: Expected to fail with <ERROR> \"%s\", but passed instead", instanceType, testCase.expectedErr)
			}
			// Failed as expected, but does it fail for the expected reason.
			if actualErr != nil && !testCase.shouldPass {
				if reflect.TypeOf(actualErr) != reflect.TypeOf(testCase.expectedErr) {
					t.Errorf("%s: Expected to fail with error \"%s\", but instead failed with error \"%s\"", instanceType, testCase.expectedErr, actualErr)
				}
			}
			// Passes as expected, but asserting the results.
			if actualErr == nil && testCase.shouldPass {
				// Asserting IsTruncated.
				if actualResult.ETag != testCase.expectedS3MD5 {
					t.Errorf("%s: Expected the result to be \"%v\", but found it to \"%v\"", instanceType, testCase.expectedS3MD5, actualResult)
				}
			}
		})
	}
}

// Benchmarks for ObjectLayer.PutObjectPart().
// The intent is to benchmark PutObjectPart for various sizes ranging from few bytes to 100MB.
// Also each of these Benchmarks are run both Erasure and FS backends.

// BenchmarkPutObjectPart5MbFS - Benchmark FS.PutObjectPart() for object size of 5MB.
func BenchmarkPutObjectPart5MbFS(b *testing.B) {
	benchmarkPutObjectPart(b, "FS", 5*humanize.MiByte)
}

// BenchmarkPutObjectPart5MbErasure - Benchmark Erasure.PutObjectPart() for object size of 5MB.
func BenchmarkPutObjectPart5MbErasure(b *testing.B) {
	benchmarkPutObjectPart(b, "Erasure", 5*humanize.MiByte)
}

// BenchmarkPutObjectPart10MbFS - Benchmark FS.PutObjectPart() for object size of 10MB.
func BenchmarkPutObjectPart10MbFS(b *testing.B) {
	benchmarkPutObjectPart(b, "FS", 10*humanize.MiByte)
}

// BenchmarkPutObjectPart10MbErasure - Benchmark Erasure.PutObjectPart() for object size of 10MB.
func BenchmarkPutObjectPart10MbErasure(b *testing.B) {
	benchmarkPutObjectPart(b, "Erasure", 10*humanize.MiByte)
}

// BenchmarkPutObjectPart25MbFS - Benchmark FS.PutObjectPart() for object size of 25MB.
func BenchmarkPutObjectPart25MbFS(b *testing.B) {
	benchmarkPutObjectPart(b, "FS", 25*humanize.MiByte)
}

// BenchmarkPutObjectPart25MbErasure - Benchmark Erasure.PutObjectPart() for object size of 25MB.
func BenchmarkPutObjectPart25MbErasure(b *testing.B) {
	benchmarkPutObjectPart(b, "Erasure", 25*humanize.MiByte)
}

// BenchmarkPutObjectPart50MbFS - Benchmark FS.PutObjectPart() for object size of 50MB.
func BenchmarkPutObjectPart50MbFS(b *testing.B) {
	benchmarkPutObjectPart(b, "FS", 50*humanize.MiByte)
}

// BenchmarkPutObjectPart50MbErasure - Benchmark Erasure.PutObjectPart() for object size of 50MB.
func BenchmarkPutObjectPart50MbErasure(b *testing.B) {
	benchmarkPutObjectPart(b, "Erasure", 50*humanize.MiByte)
}
