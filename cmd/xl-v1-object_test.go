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
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/config/storageclass"
	"github.com/minio/minio/pkg/madmin"
)

func TestRepeatPutObjectPart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var objLayer ObjectLayer
	var disks []string
	var err error
	var opts ObjectOptions

	objLayer, disks, err = prepareXL16(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// cleaning up of temporary test directories
	defer removeRoots(disks)

	err = objLayer.MakeBucketWithLocation(ctx, "bucket1", "", false)
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

func TestXLDeleteObjectBasic(t *testing.T) {
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
	xl, fsDirs, err := prepareXL16(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = xl.MakeBucketWithLocation(ctx, "bucket", "", false)
	if err != nil {
		t.Fatal(err)
	}

	// Create object "dir/obj" under bucket "bucket" for Test 7 to pass
	_, err = xl.PutObject(ctx, "bucket", "dir/obj", mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatalf("XL Object upload failed: <ERROR> %s", err)
	}
	for i, test := range testCases {
		actualErr := xl.DeleteObject(ctx, test.bucket, test.object)
		if test.expectedErr != nil && actualErr != test.expectedErr {
			t.Errorf("Test %d: Expected to fail with %s, but failed with %s", i+1, test.expectedErr, actualErr)
		}
		if test.expectedErr == nil && actualErr != nil {
			t.Errorf("Test %d: Expected to pass, but failed with %s", i+1, actualErr)
		}
	}
	// Cleanup backend directories
	removeRoots(fsDirs)
}

func TestXLDeleteObjectsXLSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var objs []*xlObjects
	for i := 0; i < 32; i++ {
		obj, fsDirs, err := prepareXL(ctx, 16)
		if err != nil {
			t.Fatal("Unable to initialize 'XL' object layer.", err)
		}
		// Remove all dirs.
		for _, dir := range fsDirs {
			defer os.RemoveAll(dir)
		}
		z := obj.(*xlZones)
		xl := z.zones[0].sets[0]
		objs = append(objs, xl)
	}

	xlSets := &xlSets{sets: objs, distributionAlgo: "CRCMOD"}

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

	err := xlSets.MakeBucketWithLocation(GlobalContext, bucketName, "", false)
	if err != nil {
		t.Fatal(err)
	}

	for _, testCase := range testCases {
		_, err = xlSets.PutObject(GlobalContext, testCase.bucket, testCase.object,
			mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{})
		if err != nil {
			t.Fatalf("XL Object upload failed: <ERROR> %s", err)
		}
	}

	toObjectNames := func(testCases []testCaseType) []string {
		names := make([]string, len(testCases))
		for i := range testCases {
			names[i] = testCases[i].object
		}
		return names
	}

	objectNames := toObjectNames(testCases)
	delErrs, err := xlSets.DeleteObjects(GlobalContext, bucketName, objectNames)
	if err != nil {
		t.Errorf("Failed to call DeleteObjects with the error: `%v`", err)
	}

	for i := range delErrs {
		if delErrs[i] != nil {
			t.Errorf("Failed to remove object `%v` with the error: `%v`", objectNames[i], delErrs[i])
		}
	}

	for _, test := range testCases {
		_, statErr := xlSets.GetObjectInfo(GlobalContext, test.bucket, test.object, ObjectOptions{})
		switch statErr.(type) {
		case ObjectNotFound:
		default:
			t.Fatalf("Object %s is not removed", test.bucket+SlashSeparator+test.object)
		}
	}
}

func TestXLDeleteObjectDiskNotFound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend.
	obj, fsDirs, err := prepareXL16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup backend directories
	defer removeRoots(fsDirs)

	z := obj.(*xlZones)
	xl := z.zones[0].sets[0]

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, "bucket", "", false)
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
	xlDisks := xl.getDisks()
	z.zones[0].xlDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		for i := range xlDisks[:7] {
			xlDisks[i] = newNaughtyDisk(xlDisks[i], nil, errFaultyDisk)
		}
		return xlDisks
	}
	z.zones[0].xlDisksMu.Unlock()
	err = obj.DeleteObject(ctx, bucket, object)
	if err != nil {
		t.Fatal(err)
	}

	// Create "obj" under "bucket".
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	// Remove one more disk to 'lose' quorum, by setting it to nil.
	xlDisks = xl.getDisks()
	z.zones[0].xlDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		xlDisks[7] = nil
		xlDisks[8] = nil
		return xlDisks
	}
	z.zones[0].xlDisksMu.Unlock()
	err = obj.DeleteObject(ctx, bucket, object)
	// since majority of disks are not available, metaquorum is not achieved and hence errXLReadQuorum error
	if err != toObjectErr(errXLReadQuorum, bucket, object) {
		t.Errorf("Expected deleteObject to fail with %v, but failed with %v", toObjectErr(errXLReadQuorum, bucket, object), err)
	}
}

func TestGetObjectNoQuorum(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend.
	obj, fsDirs, err := prepareXL16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup backend directories.
	defer removeRoots(fsDirs)

	z := obj.(*xlZones)
	xl := z.zones[0].sets[0]

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, "bucket", "", false)
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
	// in a 16 disk XL setup. The original disks are 'replaced' with
	// naughtyDisks that fail after 'f' successful StorageAPI method
	// invocations, where f - [0,2)
	for f := 0; f < 2; f++ {
		diskErrors := make(map[int]error)
		for i := 0; i <= f; i++ {
			diskErrors[i] = nil
		}
		xlDisks := xl.getDisks()
		for i := range xlDisks[:9] {
			switch diskType := xlDisks[i].(type) {
			case *naughtyDisk:
				xlDisks[i] = newNaughtyDisk(diskType.disk, diskErrors, errFaultyDisk)
			default:
				xlDisks[i] = newNaughtyDisk(xlDisks[i], diskErrors, errFaultyDisk)
			}
		}
		z.zones[0].xlDisksMu.Lock()
		xl.getDisks = func() []StorageAPI {
			return xlDisks
		}
		z.zones[0].xlDisksMu.Unlock()
		// Fetch object from store.
		err = xl.GetObject(ctx, bucket, object, 0, int64(len("abcd")), ioutil.Discard, "", opts)
		if err != toObjectErr(errXLReadQuorum, bucket, object) {
			t.Errorf("Expected putObject to fail with %v, but failed with %v", toObjectErr(errXLWriteQuorum, bucket, object), err)
		}
	}
}

func TestPutObjectNoQuorum(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend.
	obj, fsDirs, err := prepareXL16(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Cleanup backend directories.
	defer removeRoots(fsDirs)

	z := obj.(*xlZones)
	xl := z.zones[0].sets[0]

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, "bucket", "", false)
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
	// in a 16 disk XL setup. The original disks are 'replaced' with
	// naughtyDisks that fail after 'f' successful StorageAPI method
	// invocations, where f - [0,3)
	for f := 0; f < 3; f++ {
		diskErrors := make(map[int]error)
		for i := 0; i <= f; i++ {
			diskErrors[i] = nil
		}
		xlDisks := xl.getDisks()
		for i := range xlDisks[:9] {
			switch diskType := xlDisks[i].(type) {
			case *naughtyDisk:
				xlDisks[i] = newNaughtyDisk(diskType.disk, diskErrors, errFaultyDisk)
			default:
				xlDisks[i] = newNaughtyDisk(xlDisks[i], diskErrors, errFaultyDisk)
			}
		}
		z.zones[0].xlDisksMu.Lock()
		xl.getDisks = func() []StorageAPI {
			return xlDisks
		}
		z.zones[0].xlDisksMu.Unlock()
		// Upload new content to same object "object"
		_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), opts)
		if err != toObjectErr(errXLWriteQuorum, bucket, object) {
			t.Errorf("Expected putObject to fail with %v, but failed with %v", toObjectErr(errXLWriteQuorum, bucket, object), err)
		}
	}
}

// Tests both object and bucket healing.
func TestHealing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, fsDirs, err := prepareXL16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	z := obj.(*xlZones)
	xl := z.zones[0].sets[0]

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, "bucket", "", false)
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"

	data := make([]byte, 1*humanize.MiByte)
	length := int64(len(data))
	_, err = rand.Read(data)
	if err != nil {
		t.Fatal(err)
	}

	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(data), length, "", ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	disk := xl.getDisks()[0]
	xlMetaPreHeal, err := readXLMeta(ctx, disk, bucket, object)
	if err != nil {
		t.Fatal(err)
	}

	// Remove the object - to simulate the case where the disk was down when the object
	// was created.
	err = os.RemoveAll(path.Join(fsDirs[0], bucket, object))
	if err != nil {
		t.Fatal(err)
	}

	_, err = xl.HealObject(ctx, bucket, object, madmin.HealOpts{ScanMode: madmin.HealNormalScan})
	if err != nil {
		t.Fatal(err)
	}

	xlMetaPostHeal, err := readXLMeta(ctx, disk, bucket, object)
	if err != nil {
		t.Fatal(err)
	}

	// After heal the meta file should be as expected.
	if !reflect.DeepEqual(xlMetaPreHeal, xlMetaPostHeal) {
		t.Fatal("HealObject failed")
	}

	err = os.RemoveAll(path.Join(fsDirs[0], bucket, object, "xl.json"))
	if err != nil {
		t.Fatal(err)
	}

	// Write xl.json with different modtime to simulate the case where a disk had
	// gone down when an object was replaced by a new object.
	xlMetaOutDated := xlMetaPreHeal
	xlMetaOutDated.Stat.ModTime = time.Now()
	err = writeXLMetadata(ctx, disk, bucket, object, xlMetaOutDated)
	if err != nil {
		t.Fatal(err)
	}

	_, err = xl.HealObject(ctx, bucket, object, madmin.HealOpts{ScanMode: madmin.HealDeepScan})
	if err != nil {
		t.Fatal(err)
	}

	xlMetaPostHeal, err = readXLMeta(ctx, disk, bucket, object)
	if err != nil {
		t.Fatal(err)
	}

	// After heal the meta file should be as expected.
	if !reflect.DeepEqual(xlMetaPreHeal, xlMetaPostHeal) {
		t.Fatal("HealObject failed")
	}

	// Remove the bucket - to simulate the case where bucket was
	// created when the disk was down.
	err = os.RemoveAll(path.Join(fsDirs[0], bucket))
	if err != nil {
		t.Fatal(err)
	}
	// This would create the bucket.
	_, err = xl.HealBucket(ctx, bucket, false, false)
	if err != nil {
		t.Fatal(err)
	}
	// Stat the bucket to make sure that it was created.
	_, err = xl.getDisks()[0].StatVol(bucket)
	if err != nil {
		t.Fatal(err)
	}
}

func TestObjectQuorumFromMeta(t *testing.T) {
	ExecObjectLayerTestWithDirs(t, testObjectQuorumFromMeta)
}

func testObjectQuorumFromMeta(obj ObjectLayer, instanceType string, dirs []string, t TestErrHandler) {
	bucket := getRandomBucketName()

	var opts ObjectOptions
	// make data with more than one part
	partCount := 3
	data := bytes.Repeat([]byte("a"), 6*1024*1024*partCount)

	z := obj.(*xlZones)
	xl := z.zones[0].sets[0]
	xlDisks := xl.getDisks()

	err := obj.MakeBucketWithLocation(GlobalContext, bucket, globalMinioDefaultRegion, false)
	if err != nil {
		t.Fatalf("Failed to make a bucket %v", err)
	}

	// Object for test case 1 - No StorageClass defined, no MetaData in PutObject
	object1 := "object1"
	_, err = obj.PutObject(GlobalContext, bucket, object1, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), opts)
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts1, errs1 := readAllXLMetadata(GlobalContext, xlDisks, bucket, object1)

	// Object for test case 2 - No StorageClass defined, MetaData in PutObject requesting RRS Class
	object2 := "object2"
	metadata2 := make(map[string]string)
	metadata2["x-amz-storage-class"] = storageclass.RRS
	_, err = obj.PutObject(GlobalContext, bucket, object2, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata2})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts2, errs2 := readAllXLMetadata(GlobalContext, xlDisks, bucket, object2)

	// Object for test case 3 - No StorageClass defined, MetaData in PutObject requesting Standard Storage Class
	object3 := "object3"
	metadata3 := make(map[string]string)
	metadata3["x-amz-storage-class"] = storageclass.STANDARD
	_, err = obj.PutObject(GlobalContext, bucket, object3, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata3})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts3, errs3 := readAllXLMetadata(GlobalContext, xlDisks, bucket, object3)

	// Object for test case 4 - Standard StorageClass defined as Parity 6, MetaData in PutObject requesting Standard Storage Class
	object4 := "object4"
	metadata4 := make(map[string]string)
	metadata4["x-amz-storage-class"] = storageclass.STANDARD
	globalStorageClass = storageclass.Config{
		Standard: storageclass.StorageClass{
			Parity: 6,
		},
	}

	_, err = obj.PutObject(GlobalContext, bucket, object4, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata4})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts4, errs4 := readAllXLMetadata(GlobalContext, xlDisks, bucket, object4)

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

	_, err = obj.PutObject(GlobalContext, bucket, object5, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata5})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts5, errs5 := readAllXLMetadata(GlobalContext, xlDisks, bucket, object5)

	// Object for test case 6 - RRS StorageClass defined as Parity 2, MetaData in PutObject requesting Standard Storage Class
	object6 := "object6"
	metadata6 := make(map[string]string)
	metadata6["x-amz-storage-class"] = storageclass.STANDARD
	globalStorageClass = storageclass.Config{
		RRS: storageclass.StorageClass{
			Parity: 2,
		},
	}

	_, err = obj.PutObject(GlobalContext, bucket, object6, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata6})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts6, errs6 := readAllXLMetadata(GlobalContext, xlDisks, bucket, object6)

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

	_, err = obj.PutObject(GlobalContext, bucket, object7, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata7})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts7, errs7 := readAllXLMetadata(GlobalContext, xlDisks, bucket, object7)

	tests := []struct {
		parts               []xlMetaV1
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
	for i, tt := range tests {
		actualReadQuorum, actualWriteQuorum, err := objectQuorumFromMeta(GlobalContext, *xl, tt.parts, tt.errs)
		if tt.expectedError != nil && err == nil {
			t.Errorf("Test %d, Expected %s, got %s", i+1, tt.expectedError, err)
			return
		}
		if tt.expectedError == nil && err != nil {
			t.Errorf("Test %d, Expected %s, got %s", i+1, tt.expectedError, err)
			return
		}
		if tt.expectedReadQuorum != actualReadQuorum {
			t.Errorf("Test %d, Expected Read Quorum %d, got %d", i+1, tt.expectedReadQuorum, actualReadQuorum)
			return
		}
		if tt.expectedWriteQuorum != actualWriteQuorum {
			t.Errorf("Test %d, Expected Write Quorum %d, got %d", i+1, tt.expectedWriteQuorum, actualWriteQuorum)
			return
		}
	}
}
