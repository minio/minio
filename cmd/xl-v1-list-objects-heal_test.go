/*
 * Minio Cloud Storage (C) 2016, 2017 Minio, Inc.
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
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"
)

// TestListObjectsHeal - Tests ListObjectsHeal API for XL
func TestListObjectsHeal(t *testing.T) {

	initNSLock(false)

	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	// Create an instance of xl backend
	xl, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup backend directories
	defer removeRoots(fsDirs)

	bucketName := "bucket"
	objName := "obj"

	// Create test bucket
	err = xl.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		t.Fatal(err)
	}

	// Put 5 objects under sane dir
	for i := 0; i < 5; i++ {
		_, err = xl.PutObject(bucketName, "sane/"+objName+strconv.Itoa(i), mustGetHashReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), nil)
		if err != nil {
			t.Fatalf("XL Object upload failed: <ERROR> %s", err)
		}
	}
	// Put 500 objects under unsane/subdir dir
	for i := 0; i < 5; i++ {
		_, err = xl.PutObject(bucketName, "unsane/subdir/"+objName+strconv.Itoa(i), mustGetHashReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), nil)
		if err != nil {
			t.Fatalf("XL Object upload failed: <ERROR> %s", err)
		}
	}

	// Structure for testing
	type testData struct {
		bucket      string
		object      string
		marker      string
		delimiter   string
		maxKeys     int
		expectedErr error
		foundObjs   int
	}

	// Generic function for testing ListObjectsHeal, needs testData as a parameter
	testFunc := func(testCase testData, testRank int) {
		objectsNeedHeal, foundErr := xl.ListObjectsHeal(testCase.bucket, testCase.object, testCase.marker, testCase.delimiter, testCase.maxKeys)
		if testCase.expectedErr == nil && foundErr != nil {
			t.Fatalf("Test %d: Expected nil error, found: %v", testRank, foundErr)
		}
		if testCase.expectedErr != nil && foundErr.Error() != testCase.expectedErr.Error() {
			t.Fatalf("Test %d: Found unexpected error: %v, expected: %v", testRank, foundErr, testCase.expectedErr)

		}
		if len(objectsNeedHeal.Objects) != testCase.foundObjs {
			t.Fatalf("Test %d: Found unexpected number of objects: %d, expected: %v", testRank, len(objectsNeedHeal.Objects), testCase.foundObjs)
		}
	}

	// Start tests

	testCases := []testData{
		// Wrong bucket name
		{"foobucket", "", "", "", 1000, BucketNotFound{Bucket: "foobucket"}, 0},
		// Inexistent object
		{bucketName, "inexistentObj", "", "", 1000, nil, 0},
		// Test ListObjectsHeal when all objects are sane
		{bucketName, "", "", "", 1000, nil, 0},
	}
	for i, testCase := range testCases {
		testFunc(testCase, i+1)
	}

	// Test ListObjectsHeal when all objects under unsane need healing
	xlObj := xl.(*xlObjects)
	for i := 0; i < 5; i++ {
		if err = xlObj.storageDisks[0].DeleteFile(bucketName, "unsane/subdir/"+objName+strconv.Itoa(i)+"/xl.json"); err != nil {
			t.Fatal(err)
		}
	}

	// Start tests again with some objects that need healing

	testCases = []testData{
		// Test ListObjectsHeal when all objects under unsane/ need to be healed
		{bucketName, "", "", "", 1000, nil, 5},
		// List objects heal under unsane/, should return all elements
		{bucketName, "unsane/", "", "", 1000, nil, 5},
		// List healing objects under sane/, should return 0
		{bucketName, "sane/", "", "", 1000, nil, 0},
		// Max Keys == 200
		{bucketName, "unsane/", "", "", 2, nil, 2},
		// Max key > 1000
		{bucketName, "unsane/", "", "", 5000, nil, 5},
		// Prefix == Delimiter == "/"
		{bucketName, "/", "", "/", 1000, nil, 0},
		// Max Keys == 0
		{bucketName, "", "", "", 0, nil, 0},
		// Testing with marker parameter
		{bucketName, "", "unsane/subdir/" + objName + "0", "", 1000, nil, 4},
	}
	for i, testCase := range testCases {
		testFunc(testCase, i+1)
	}

}

// Test for ListUploadsHeal API for XL.
func TestListUploadsHeal(t *testing.T) {
	initNSLock(false)

	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// Remove config directory after the test ends.
	defer os.RemoveAll(rootPath)

	// Create an instance of XL backend.
	xl, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup backend directories on function return.
	defer removeRoots(fsDirs)

	bucketName := "bucket"
	prefix := "prefix"
	objName := path.Join(prefix, "obj")

	// Create test bucket.
	err = xl.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		t.Fatal(err)
	}

	// Create a new multipart upload.
	uploadID, err := xl.NewMultipartUpload(bucketName, objName, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Upload a part.
	data := bytes.Repeat([]byte("a"), 1024)
	_, err = xl.PutObjectPart(bucketName, objName, uploadID, 1,
		mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), "", ""))
	if err != nil {
		t.Fatal(err)
	}

	// Check if list uploads heal returns any uploads to be healed
	// incorrectly.
	listUploadsInfo, err := xl.ListUploadsHeal(bucketName, prefix, "", "", "", 1000)
	if err != nil {
		t.Fatal(err)
	}

	// All uploads intact nothing to heal.
	if len(listUploadsInfo.Uploads) != 0 {
		t.Errorf("Expected no uploads but received %d", len(listUploadsInfo.Uploads))
	}

	// Delete the part from the first disk to make the upload (and
	// its part) to appear in upload heal listing.
	firstDisk := xl.(*xlObjects).storageDisks[0]
	err = firstDisk.DeleteFile(minioMetaMultipartBucket,
		filepath.Join(bucketName, objName, uploadID, xlMetaJSONFile))
	if err != nil {
		t.Fatal(err)
	}

	listUploadsInfo, err = xl.ListUploadsHeal(bucketName, prefix, "", "", "", 1000)
	if err != nil {
		t.Fatal(err)
	}

	// One upload with missing xl.json on first disk.
	if len(listUploadsInfo.Uploads) != 1 {
		t.Errorf("Expected 1 upload but received %d", len(listUploadsInfo.Uploads))
	}
}
