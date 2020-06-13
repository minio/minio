/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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
	"io/ioutil"
	"os"
	"testing"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/config/storageclass"
)

func TestRepeatPutObjectPart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var objLayer ObjectLayer
	var disks []string
	var err error
	var opts ObjectOptions

	objLayer, disks, err = prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// cleaning up of temporary test directories
	defer removeRoots(disks)

	err = objLayer.MakeBucketWithLocation(ctx, "bucket1", BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	uploadID, err := objLayer.NewMultipartUpload(ctx, "bucket1", "mpartObj1", opts)
	if err != nil {
		t.Fatal(err)
	}
	fiveMBBytes := bytes.Repeat([]byte("a"), 5*humanize.MiByte)
	md5Hex := getMD5Hash(fiveMBBytes)
	_, err = objLayer.PutObjectPart(ctx, "bucket1", "mpartObj1", uploadID, 1, mustGetPutObjReader(t, bytes.NewReader(fiveMBBytes), 5*humanize.MiByte, md5Hex, ""), opts)
	if err != nil {
		t.Fatal(err)
	}
	// PutObjectPart should succeed even if part already exists. ref: https://github.com/minio/minio/issues/1930
	_, err = objLayer.PutObjectPart(ctx, "bucket1", "mpartObj1", uploadID, 1, mustGetPutObjReader(t, bytes.NewReader(fiveMBBytes), 5*humanize.MiByte, md5Hex, ""), opts)
	if err != nil {
		t.Fatal(err)
	}
}

func TestErasureDeleteObjectBasic(t *testing.T) {
	testCases := []struct {
		bucket      string
		object      string
		expectedErr error
	}{
		{".test", "dir/obj", BucketNameInvalid{Bucket: ".test"}},
		{"----", "dir/obj", BucketNameInvalid{Bucket: "----"}},
		{"bucket", "", ObjectNameInvalid{Bucket: "bucket", Object: ""}},
		{"bucket", "doesnotexist", ObjectNotFound{Bucket: "bucket", Object: "doesnotexist"}},
		{"bucket", "dir/doesnotexist", ObjectNotFound{Bucket: "bucket", Object: "dir/doesnotexist"}},
		{"bucket", "dir", ObjectNotFound{Bucket: "bucket", Object: "dir"}},
		{"bucket", "dir/", ObjectNotFound{Bucket: "bucket", Object: "dir/"}},
		{"bucket", "dir/obj", nil},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend
	xl, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = xl.MakeBucketWithLocation(ctx, "bucket", BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Create object "dir/obj" under bucket "bucket" for Test 7 to pass
	_, err = xl.PutObject(ctx, "bucket", "dir/obj", mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatalf("Erasure Object upload failed: <ERROR> %s", err)
	}
	for _, test := range testCases {
		test := test
		t.Run("", func(t *testing.T) {
			_, actualErr := xl.DeleteObject(ctx, test.bucket, test.object, ObjectOptions{})
			if test.expectedErr != nil && actualErr != test.expectedErr {
				t.Errorf("Expected to fail with %s, but failed with %s", test.expectedErr, actualErr)
			}
			if test.expectedErr == nil && actualErr != nil {
				t.Errorf("Expected to pass, but failed with %s", actualErr)
			}
		})
	}
	// Cleanup backend directories
	removeRoots(fsDirs)
}

func TestErasureDeleteObjectsErasureSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var objs []*erasureObjects
	for i := 0; i < 32; i++ {
		obj, fsDirs, err := prepareErasure(ctx, 16)
		if err != nil {
			t.Fatal("Unable to initialize 'Erasure' object layer.", err)
		}
		// Remove all dirs.
		for _, dir := range fsDirs {
			defer os.RemoveAll(dir)
		}
		z := obj.(*erasureZones)
		xl := z.zones[0].sets[0]
		objs = append(objs, xl)
	}

	erasureSets := &erasureSets{sets: objs, distributionAlgo: "CRCMOD"}

	type testCaseType struct {
		bucket string
		object string
	}

	bucketName := "bucket"
	testCases := []testCaseType{
		{bucketName, "dir/obj1"},
		{bucketName, "dir/obj2"},
		{bucketName, "obj3"},
		{bucketName, "obj_4"},
	}

	err := erasureSets.MakeBucketWithLocation(ctx, bucketName, BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	for _, testCase := range testCases {
		_, err = erasureSets.PutObject(ctx, testCase.bucket, testCase.object,
			mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{})
		if err != nil {
			t.Fatalf("Erasure Object upload failed: <ERROR> %s", err)
		}
	}

	toObjectNames := func(testCases []testCaseType) []ObjectToDelete {
		names := make([]ObjectToDelete, len(testCases))
		for i := range testCases {
			names[i] = ObjectToDelete{ObjectName: testCases[i].object}
		}
		return names
	}

	objectNames := toObjectNames(testCases)
	_, delErrs := erasureSets.DeleteObjects(ctx, bucketName, objectNames, ObjectOptions{})

	for i := range delErrs {
		if delErrs[i] != nil {
			t.Errorf("Failed to remove object `%v` with the error: `%v`", objectNames[i], delErrs[i])
		}
	}

	for _, test := range testCases {
		_, statErr := erasureSets.GetObjectInfo(ctx, test.bucket, test.object, ObjectOptions{})
		switch statErr.(type) {
		case ObjectNotFound:
		default:
			t.Fatalf("Object %s is not removed", test.bucket+SlashSeparator+test.object)
		}
	}
}

func TestErasureDeleteObjectDiskNotFound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend.
	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup backend directories
	defer removeRoots(fsDirs)

	z := obj.(*erasureZones)
	xl := z.zones[0].sets[0]

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, "bucket", BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	opts := ObjectOptions{}
	// Create object "obj" under bucket "bucket".
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}
	// for a 16 disk setup, quorum is 9. To simulate disks not found yet
	// quorum is available, we remove disks leaving quorum disks behind.
	erasureDisks := xl.getDisks()
	z.zones[0].erasureDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		for i := range erasureDisks[:7] {
			erasureDisks[i] = newNaughtyDisk(erasureDisks[i], nil, errFaultyDisk)
		}
		return erasureDisks
	}

	z.zones[0].erasureDisksMu.Unlock()
	_, err = obj.DeleteObject(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Create "obj" under "bucket".
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	// Remove one more disk to 'lose' quorum, by setting it to nil.
	erasureDisks = xl.getDisks()
	z.zones[0].erasureDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		erasureDisks[7] = nil
		erasureDisks[8] = nil
		return erasureDisks
	}

	z.zones[0].erasureDisksMu.Unlock()
	_, err = obj.DeleteObject(ctx, bucket, object, ObjectOptions{})
	// since majority of disks are not available, metaquorum is not achieved and hence errErasureWriteQuorum error
	if err != toObjectErr(errErasureWriteQuorum, bucket, object) {
		t.Errorf("Expected deleteObject to fail with %v, but failed with %v", toObjectErr(errErasureWriteQuorum, bucket, object), err)
	}
}

func TestGetObjectNoQuorum(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend.
	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup backend directories.
	defer removeRoots(fsDirs)

	z := obj.(*erasureZones)
	xl := z.zones[0].sets[0]

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, "bucket", BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	opts := ObjectOptions{}
	// Create "object" under "bucket".
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	// Make 9 disks offline, which leaves less than quorum number of disks
	// in a 16 disk Erasure setup. The original disks are 'replaced' with
	// naughtyDisks that fail after 'f' successful StorageAPI method
	// invocations, where f - [0,2)
	for f := 0; f < 2; f++ {
		diskErrors := make(map[int]error)
		for i := 0; i <= f; i++ {
			diskErrors[i] = nil
		}
		erasureDisks := xl.getDisks()
		for i := range erasureDisks[:9] {
			switch diskType := erasureDisks[i].(type) {
			case *naughtyDisk:
				erasureDisks[i] = newNaughtyDisk(diskType.disk, diskErrors, errFaultyDisk)
			default:
				erasureDisks[i] = newNaughtyDisk(erasureDisks[i], diskErrors, errFaultyDisk)
			}
		}
		z.zones[0].erasureDisksMu.Lock()
		xl.getDisks = func() []StorageAPI {
			return erasureDisks
		}
		z.zones[0].erasureDisksMu.Unlock()
		// Fetch object from store.
		err = xl.GetObject(ctx, bucket, object, 0, int64(len("abcd")), ioutil.Discard, "", opts)
		if err != toObjectErr(errErasureReadQuorum, bucket, object) {
			t.Errorf("Expected putObject to fail with %v, but failed with %v", toObjectErr(errErasureWriteQuorum, bucket, object), err)
		}
	}
}

func TestPutObjectNoQuorum(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend.
	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Cleanup backend directories.
	defer removeRoots(fsDirs)

	z := obj.(*erasureZones)
	xl := z.zones[0].sets[0]

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, "bucket", BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	opts := ObjectOptions{}
	// Create "object" under "bucket".
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	// Make 9 disks offline, which leaves less than quorum number of disks
	// in a 16 disk Erasure setup. The original disks are 'replaced' with
	// naughtyDisks that fail after 'f' successful StorageAPI method
	// invocations, where f - [0,3)
	for f := 0; f < 3; f++ {
		diskErrors := make(map[int]error)
		for i := 0; i <= f; i++ {
			diskErrors[i] = nil
		}
		erasureDisks := xl.getDisks()
		for i := range erasureDisks[:9] {
			switch diskType := erasureDisks[i].(type) {
			case *naughtyDisk:
				erasureDisks[i] = newNaughtyDisk(diskType.disk, diskErrors, errFaultyDisk)
			default:
				erasureDisks[i] = newNaughtyDisk(erasureDisks[i], diskErrors, errFaultyDisk)
			}
		}
		z.zones[0].erasureDisksMu.Lock()
		xl.getDisks = func() []StorageAPI {
			return erasureDisks
		}
		z.zones[0].erasureDisksMu.Unlock()
		// Upload new content to same object "object"
		_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), opts)
		if err != toObjectErr(errErasureWriteQuorum, bucket, object) {
			t.Errorf("Expected putObject to fail with %v, but failed with %v", toObjectErr(errErasureWriteQuorum, bucket, object), err)
		}
	}
}

func TestObjectQuorumFromMeta(t *testing.T) {
	ExecObjectLayerTestWithDirs(t, testObjectQuorumFromMeta)
}

func testObjectQuorumFromMeta(obj ObjectLayer, instanceType string, dirs []string, t TestErrHandler) {
	restoreGlobalStorageClass := globalStorageClass
	defer func() {
		globalStorageClass = restoreGlobalStorageClass
	}()

	bucket := getRandomBucketName()

	var opts ObjectOptions
	// make data with more than one part
	partCount := 3
	data := bytes.Repeat([]byte("a"), 6*1024*1024*partCount)

	z := obj.(*erasureZones)
	xl := z.zones[0].sets[0]
	erasureDisks := xl.getDisks()

	ctx, cancel := context.WithCancel(GlobalContext)
	defer cancel()

	err := obj.MakeBucketWithLocation(ctx, bucket, BucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket %v", err)
	}

	// Object for test case 1 - No StorageClass defined, no MetaData in PutObject
	object1 := "object1"
	_, err = obj.PutObject(ctx, bucket, object1, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), opts)
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts1, errs1 := readAllFileInfo(ctx, erasureDisks, bucket, object1, "")

	// Object for test case 2 - No StorageClass defined, MetaData in PutObject requesting RRS Class
	object2 := "object2"
	metadata2 := make(map[string]string)
	metadata2["x-amz-storage-class"] = storageclass.RRS
	_, err = obj.PutObject(ctx, bucket, object2, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata2})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts2, errs2 := readAllFileInfo(ctx, erasureDisks, bucket, object2, "")

	// Object for test case 3 - No StorageClass defined, MetaData in PutObject requesting Standard Storage Class
	object3 := "object3"
	metadata3 := make(map[string]string)
	metadata3["x-amz-storage-class"] = storageclass.STANDARD
	_, err = obj.PutObject(ctx, bucket, object3, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata3})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts3, errs3 := readAllFileInfo(ctx, erasureDisks, bucket, object3, "")

	// Object for test case 4 - Standard StorageClass defined as Parity 6, MetaData in PutObject requesting Standard Storage Class
	object4 := "object4"
	metadata4 := make(map[string]string)
	metadata4["x-amz-storage-class"] = storageclass.STANDARD
	globalStorageClass = storageclass.Config{
		Standard: storageclass.StorageClass{
			Parity: 6,
		},
	}

	_, err = obj.PutObject(ctx, bucket, object4, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata4})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts4, errs4 := readAllFileInfo(ctx, erasureDisks, bucket, object4, "")

	// Object for test case 5 - RRS StorageClass defined as Parity 2, MetaData in PutObject requesting RRS Class
	// Reset global storage class flags
	object5 := "object5"
	metadata5 := make(map[string]string)
	metadata5["x-amz-storage-class"] = storageclass.RRS
	globalStorageClass = storageclass.Config{
		RRS: storageclass.StorageClass{
			Parity: 2,
		},
	}

	_, err = obj.PutObject(ctx, bucket, object5, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata5})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts5, errs5 := readAllFileInfo(ctx, erasureDisks, bucket, object5, "")

	// Object for test case 6 - RRS StorageClass defined as Parity 2, MetaData in PutObject requesting Standard Storage Class
	object6 := "object6"
	metadata6 := make(map[string]string)
	metadata6["x-amz-storage-class"] = storageclass.STANDARD
	globalStorageClass = storageclass.Config{
		RRS: storageclass.StorageClass{
			Parity: 2,
		},
	}

	_, err = obj.PutObject(ctx, bucket, object6, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata6})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts6, errs6 := readAllFileInfo(ctx, erasureDisks, bucket, object6, "")

	// Object for test case 7 - Standard StorageClass defined as Parity 5, MetaData in PutObject requesting RRS Class
	// Reset global storage class flags
	object7 := "object7"
	metadata7 := make(map[string]string)
	metadata7["x-amz-storage-class"] = storageclass.RRS
	globalStorageClass = storageclass.Config{
		Standard: storageclass.StorageClass{
			Parity: 5,
		},
	}

	_, err = obj.PutObject(ctx, bucket, object7, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata7})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts7, errs7 := readAllFileInfo(ctx, erasureDisks, bucket, object7, "")

	tests := []struct {
		parts               []FileInfo
		errs                []error
		expectedReadQuorum  int
		expectedWriteQuorum int
		expectedError       error
	}{
		{parts1, errs1, 8, 9, nil},
		{parts2, errs2, 14, 15, nil},
		{parts3, errs3, 8, 9, nil},
		{parts4, errs4, 10, 11, nil},
		{parts5, errs5, 14, 15, nil},
		{parts6, errs6, 8, 9, nil},
		{parts7, errs7, 14, 15, nil},
	}
	for _, tt := range tests {
		tt := tt
		t.(*testing.T).Run("", func(t *testing.T) {
			actualReadQuorum, actualWriteQuorum, err := objectQuorumFromMeta(ctx, *xl, tt.parts, tt.errs)
			if tt.expectedError != nil && err == nil {
				t.Errorf("Expected %s, got %s", tt.expectedError, err)
			}
			if tt.expectedError == nil && err != nil {
				t.Errorf("Expected %s, got %s", tt.expectedError, err)
			}
			if tt.expectedReadQuorum != actualReadQuorum {
				t.Errorf("Expected Read Quorum %d, got %d", tt.expectedReadQuorum, actualReadQuorum)
			}
			if tt.expectedWriteQuorum != actualWriteQuorum {
				t.Errorf("Expected Write Quorum %d, got %d", tt.expectedWriteQuorum, actualWriteQuorum)
			}
		})
	}
}
