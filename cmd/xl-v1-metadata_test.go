/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"errors"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
)

// Tests for reading XL object info.
func TestXLReadStat(t *testing.T) {
	ExecObjectLayerDiskAlteredTest(t, testXLReadStat)
}

func testXLReadStat(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	// Setup for the tests.
	bucketName := getRandomBucketName()
	objectName := "test-object"
	// create bucket.
	err := obj.MakeBucketWithLocation(context.Background(), bucketName, "")
	// Stop the test if creation of the bucket fails.
	if err != nil {
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// set of byte data for PutObject.
	// object has to be created before running tests for GetObject.
	// this is required even to assert the GetObject data,
	// since dataInserted === dataFetched back is a primary criteria for any object storage this assertion is critical.
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * humanize.MiByte)},
	}
	// set of inputs for uploading the objects before tests for downloading is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		// case - 1.
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upkoad the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err = obj.PutObject(context.Background(), input.bucketName, input.objectName, mustGetHashReader(t, bytes.NewBuffer(input.textData), input.contentLength, input.metaData["etag"], ""), input.metaData)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	_, _, err = obj.(*xlObjects).readXLMetaStat(context.Background(), bucketName, objectName)
	if err != nil {
		t.Fatal(err)
	}

	// Remove one disk.
	removeDiskN(disks, 7)

	// Removing disk shouldn't affect reading object info.
	_, _, err = obj.(*xlObjects).readXLMetaStat(context.Background(), bucketName, objectName)
	if err != nil {
		t.Fatal(err)
	}

	for _, disk := range disks {
		os.RemoveAll(path.Join(disk, bucketName))
	}

	_, _, err = obj.(*xlObjects).readXLMetaStat(context.Background(), bucketName, objectName)
	if err != errVolumeNotFound {
		t.Fatal(err)
	}
}

// Tests for reading XL meta parts.
func TestXLReadMetaParts(t *testing.T) {
	ExecObjectLayerDiskAlteredTest(t, testXLReadMetaParts)
}

// testListObjectParts - Tests validate listing of object parts when disks go offline.
func testXLReadMetaParts(obj ObjectLayer, instanceType string, disks []string, t *testing.T) {
	bucketNames := []string{"minio-bucket", "minio-2-bucket"}
	objectNames := []string{"minio-object-1.txt"}
	uploadIDs := []string{}

	// bucketnames[0].
	// objectNames[0].
	// uploadIds [0].
	// Create bucket before intiating NewMultipartUpload.
	err := obj.MakeBucketWithLocation(context.Background(), bucketNames[0], "")
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}
	// Initiate Multipart Upload on the above created bucket.
	uploadID, err := obj.NewMultipartUpload(context.Background(), bucketNames[0], objectNames[0], nil)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

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
	sha256sum := ""
	// Iterating over creatPartCases to generate multipart chunks.
	for _, testCase := range createPartCases {
		_, perr := obj.PutObjectPart(context.Background(), testCase.bucketName, testCase.objName, testCase.uploadID, testCase.PartID, mustGetHashReader(t, bytes.NewBufferString(testCase.inputReaderData), testCase.intputDataSize, testCase.inputMd5, sha256sum))
		if perr != nil {
			t.Fatalf("%s : %s", instanceType, perr)
		}
	}

	uploadIDPath := obj.(*xlObjects).getUploadIDDir(bucketNames[0], objectNames[0], uploadIDs[0])

	_, _, err = obj.(*xlObjects).readXLMetaParts(context.Background(), minioMetaMultipartBucket, uploadIDPath)
	if err != nil {
		t.Fatal(err)
	}

	// Remove one disk.
	removeDiskN(disks, 7)

	// Removing disk shouldn't affect reading object parts info.
	_, _, err = obj.(*xlObjects).readXLMetaParts(context.Background(), minioMetaMultipartBucket, uploadIDPath)
	if err != nil {
		t.Fatal(err)
	}

	for _, disk := range disks {
		os.RemoveAll(path.Join(disk, bucketNames[0]))
		os.RemoveAll(path.Join(disk, minioMetaMultipartBucket, obj.(*xlObjects).getMultipartSHADir(bucketNames[0], objectNames[0])))
	}

	_, _, err = obj.(*xlObjects).readXLMetaParts(context.Background(), minioMetaMultipartBucket, uploadIDPath)
	if err != errFileNotFound {
		t.Fatal(err)
	}
}

// Test xlMetaV1.AddObjectPart()
func TestAddObjectPart(t *testing.T) {
	testCases := []struct {
		partNum       int
		expectedIndex int
	}{
		{1, 0},
		{2, 1},
		{4, 2},
		{5, 3},
		{7, 4},
		// Insert part.
		{3, 2},
		// Replace existing part.
		{4, 3},
		// Missing part.
		{6, -1},
	}

	// Setup.
	xlMeta := newXLMetaV1("test-object", 8, 8)
	if !xlMeta.IsValid() {
		t.Fatalf("unable to get xl meta")
	}

	// Test them.
	for _, testCase := range testCases {
		if testCase.expectedIndex > -1 {
			partNumString := strconv.Itoa(testCase.partNum)
			xlMeta.AddObjectPart(testCase.partNum, "part."+partNumString, "etag."+partNumString, int64(testCase.partNum+humanize.MiByte))
		}

		if index := objectPartIndex(xlMeta.Parts, testCase.partNum); index != testCase.expectedIndex {
			t.Fatalf("%+v: expected = %d, got: %d", testCase, testCase.expectedIndex, index)
		}
	}
}

// Test objectPartIndex().
// generates a sample xlMeta data and asserts the output of objectPartIndex() with the expected value.
func TestObjectPartIndex(t *testing.T) {
	testCases := []struct {
		partNum       int
		expectedIndex int
	}{
		{2, 1},
		{1, 0},
		{5, 3},
		{4, 2},
		{7, 4},
	}

	// Setup.
	xlMeta := newXLMetaV1("test-object", 8, 8)
	if !xlMeta.IsValid() {
		t.Fatalf("unable to get xl meta")
	}

	// Add some parts for testing.
	for _, testCase := range testCases {
		partNumString := strconv.Itoa(testCase.partNum)
		xlMeta.AddObjectPart(testCase.partNum, "part."+partNumString, "etag."+partNumString, int64(testCase.partNum+humanize.MiByte))
	}

	// Add failure test case.
	testCases = append(testCases, struct {
		partNum       int
		expectedIndex int
	}{6, -1})

	// Test them.
	for _, testCase := range testCases {
		if index := objectPartIndex(xlMeta.Parts, testCase.partNum); index != testCase.expectedIndex {
			t.Fatalf("%+v: expected = %d, got: %d", testCase, testCase.expectedIndex, index)
		}
	}
}

// Test xlMetaV1.ObjectToPartOffset().
func TestObjectToPartOffset(t *testing.T) {
	// Setup.
	xlMeta := newXLMetaV1("test-object", 8, 8)
	if !xlMeta.IsValid() {
		t.Fatalf("unable to get xl meta")
	}

	// Add some parts for testing.
	// Total size of all parts is 5,242,899 bytes.
	for _, partNum := range []int{1, 2, 4, 5, 7} {
		partNumString := strconv.Itoa(partNum)
		xlMeta.AddObjectPart(partNum, "part."+partNumString, "etag."+partNumString, int64(partNum+humanize.MiByte))
	}

	testCases := []struct {
		offset         int64
		expectedIndex  int
		expectedOffset int64
		expectedErr    error
	}{
		{0, 0, 0, nil},
		{1 * humanize.MiByte, 0, 1 * humanize.MiByte, nil},
		{1 + humanize.MiByte, 1, 0, nil},
		{2 + humanize.MiByte, 1, 1, nil},
		// Its valid for zero sized object.
		{-1, 0, -1, nil},
		// Max fffset is always (size - 1).
		{(1 + 2 + 4 + 5 + 7) + (5 * humanize.MiByte) - 1, 4, 1048582, nil},
		// Error if offset is size.
		{(1 + 2 + 4 + 5 + 7) + (5 * humanize.MiByte), 0, 0, InvalidRange{}},
	}

	// Test them.
	for _, testCase := range testCases {
		index, offset, err := xlMeta.ObjectToPartOffset(context.Background(), testCase.offset)
		if err != testCase.expectedErr {
			t.Fatalf("%+v: expected = %s, got: %s", testCase, testCase.expectedErr, err)
		}
		if index != testCase.expectedIndex {
			t.Fatalf("%+v: index: expected = %d, got: %d", testCase, testCase.expectedIndex, index)
		}
		if offset != testCase.expectedOffset {
			t.Fatalf("%+v: offset: expected = %d, got: %d", testCase, testCase.expectedOffset, offset)
		}
	}
}

// Helper function to check if two xlMetaV1 values are similar.
func isXLMetaSimilar(m, n xlMetaV1) bool {
	if m.Version != n.Version {
		return false
	}
	if m.Format != n.Format {
		return false
	}
	if len(m.Parts) != len(n.Parts) {
		return false
	}
	return true
}

func TestPickValidXLMeta(t *testing.T) {
	obj := "object"
	x1 := newXLMetaV1(obj, 4, 4)
	now := UTCNow()
	x1.Stat.ModTime = now
	invalidX1 := x1
	invalidX1.Version = "invalid-version"
	xs := []xlMetaV1{x1, x1, x1, x1}
	invalidXS := []xlMetaV1{invalidX1, invalidX1, invalidX1, invalidX1}
	testCases := []struct {
		metaArr     []xlMetaV1
		modTime     time.Time
		xlMeta      xlMetaV1
		expectedErr error
	}{
		{
			metaArr:     xs,
			modTime:     now,
			xlMeta:      x1,
			expectedErr: nil,
		},
		{
			metaArr:     invalidXS,
			modTime:     now,
			xlMeta:      invalidX1,
			expectedErr: errors.New("No valid xl.json present"),
		},
	}
	for i, test := range testCases {
		xlMeta, err := pickValidXLMeta(context.Background(), test.metaArr, test.modTime)
		if test.expectedErr != nil {
			if err.Error() != test.expectedErr.Error() {
				t.Errorf("Test %d: Expected to fail with %v but received %v",
					i+1, test.expectedErr, err)
			}
		} else {
			if !isXLMetaSimilar(xlMeta, test.xlMeta) {
				t.Errorf("Test %d: Expected %v but received %v",
					i+1, test.xlMeta, xlMeta)
			}
		}
	}
}

func TestIsXLMetaFormatValid(t *testing.T) {
	tests := []struct {
		name    int
		version string
		format  string
		want    bool
	}{
		{1, "123", "fs", false},
		{2, "123", xlMetaFormat, false},
		{3, xlMetaVersion, "test", false},
		{4, xlMetaVersion100, "hello", false},
		{5, xlMetaVersion, xlMetaFormat, true},
		{6, xlMetaVersion100, xlMetaFormat, true},
	}
	for _, tt := range tests {
		if got := isXLMetaFormatValid(tt.version, tt.format); got != tt.want {
			t.Errorf("Test %d: Expected %v but received %v", tt.name, got, tt.want)
		}
	}
}

func TestIsXLMetaErasureInfoValid(t *testing.T) {
	tests := []struct {
		name   int
		data   int
		parity int
		want   bool
	}{
		{1, 5, 6, false},
		{2, 5, 5, true},
		{3, 0, 5, false},
		{4, 5, 0, false},
		{5, 5, 0, false},
		{6, 5, 4, true},
	}
	for _, tt := range tests {
		if got := isXLMetaErasureInfoValid(tt.data, tt.parity); got != tt.want {
			t.Errorf("Test %d: Expected %v but received %v", tt.name, got, tt.want)
		}
	}
}
