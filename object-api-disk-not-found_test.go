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

package main

import (
	"bytes"
	"strings"
	"testing"
)

// Wrapper for calling TestListObjectPartsDiskNotFound tests for both XL multiple disks and single node setup.
func TestListObjectPartsDiskNotFound(t *testing.T) {
	ExecObjectLayerDiskNotFoundTest(t, testListObjectPartsDiskNotFound)
}

// testListObjectParts - Tests validate listing of object parts when disks go offline.
func testListObjectPartsDiskNotFound(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {

	bucketNames := []string{"minio-bucket", "minio-2-bucket"}
	objectNames := []string{"minio-object-1.txt"}
	uploadIDs := []string{}

	// bucketnames[0].
	// objectNames[0].
	// uploadIds [0].
	// Create bucket before intiating NewMultipartUpload.
	err := obj.MakeBucket(bucketNames[0])
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	// Initiate Multipart Upload on the above created bucket.
	uploadID, err := obj.NewMultipartUpload(bucketNames[0], objectNames[0], nil)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Remove some random disk.
	removeRandomDisk(disks, 1)

	uploadIDs = append(uploadIDs, uploadID)

	// Create multipart parts.
	// Need parts to be uploaded before MultipartLists can be called and tested.
	createPartCases := []struct {
		bucketName      string
		objName         string
		uploadID        string
		PartID          int
		inputReaderData string
		inputMd5        string
		intputDataSize  int64
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
	// Iterating over creatPartCases to generate multipart chunks.
	for _, testCase := range createPartCases {
		_, err := obj.PutObjectPart(testCase.bucketName, testCase.objName, testCase.uploadID, testCase.PartID, testCase.intputDataSize,
			bytes.NewBufferString(testCase.inputReaderData), testCase.inputMd5)
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}
	}

	// Remove one more random disk.
	removeRandomDisk(disks, 1)

	partInfos := []ListPartsInfo{
		// partinfos - 0.
		{
			Bucket:   bucketNames[0],
			Object:   objectNames[0],
			MaxParts: 10,
			UploadID: uploadIDs[0],
			Parts: []partInfo{
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
			Parts: []partInfo{
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
			Bucket:      bucketNames[0],
			Object:      objectNames[0],
			MaxParts:    2,
			IsTruncated: false,
			UploadID:    uploadIDs[0],
			Parts: []partInfo{
				{
					PartNumber: 4,
					Size:       4,
					ETag:       "e132e96a5ddad6da8b07bba6f6131fef",
				},
			},
		},
	}

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
		// Valid bucket names, but they donot exist (Test number 5-7).
		{"volatile-bucket-1", "", "", 0, 0, ListPartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-1"}, false},
		{"volatile-bucket-2", "", "", 0, 0, ListPartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-2"}, false},
		{"volatile-bucket-3", "", "", 0, 0, ListPartsInfo{}, BucketNotFound{Bucket: "volatile-bucket-3"}, false},
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
		actualResult, actualErr := obj.ListObjectParts(testCase.bucket, testCase.object, testCase.uploadID, testCase.partNumberMarker, testCase.maxParts)
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
