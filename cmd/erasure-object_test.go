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
	crand "crypto/rand"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/config/storageclass"
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
	defer objLayer.Shutdown(context.Background())
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
	defer xl.Shutdown(context.Background())

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
			_, err := xl.GetObjectInfo(ctx, "bucket", "dir/obj", ObjectOptions{})
			if err != nil {
				t.Fatal("dir/obj not found before last test")
			}
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
		z := obj.(*erasureServerPools)
		xl := z.serverPools[0].sets[0]
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
	restoreGlobalStorageClass := globalStorageClass
	defer func() {
		globalStorageClass = restoreGlobalStorageClass
	}()

	globalStorageClass = storageclass.Config{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend.
	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup backend directories
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]

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

	erasureDisks := xl.getDisks()
	z.serverPools[0].erasureDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		for i := range erasureDisks[:6] {
			erasureDisks[i] = newNaughtyDisk(erasureDisks[i], nil, errFaultyDisk)
		}
		return erasureDisks
	}

	z.serverPools[0].erasureDisksMu.Unlock()
	_, err = obj.DeleteObject(ctx, bucket, object, ObjectOptions{})
	if !errors.Is(err, errErasureWriteQuorum) {
		t.Fatal(err)
	}

	// Create "obj" under "bucket".
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	// Remove one more disk to 'lose' quorum, by taking 2 more drives offline.
	erasureDisks = xl.getDisks()
	z.serverPools[0].erasureDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		erasureDisks[7] = nil
		erasureDisks[8] = nil
		return erasureDisks
	}

	z.serverPools[0].erasureDisksMu.Unlock()
	_, err = obj.DeleteObject(ctx, bucket, object, ObjectOptions{})
	// since majority of disks are not available, metaquorum is not achieved and hence errErasureWriteQuorum error
	if !errors.Is(err, errErasureWriteQuorum) {
		t.Errorf("Expected deleteObject to fail with %v, but failed with %v", toObjectErr(errErasureWriteQuorum, bucket, object), err)
	}
}

func TestErasureDeleteObjectDiskNotFoundErasure4(t *testing.T) {
	restoreGlobalStorageClass := globalStorageClass
	defer func() {
		globalStorageClass = restoreGlobalStorageClass
	}()

	globalStorageClass = storageclass.Config{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend.
	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup backend directories
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]

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
	// Upload a good object
	_, err = obj.DeleteObject(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Create "obj" under "bucket".
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	// Remove disks to 'lose' quorum for object, by setting 5 to nil.
	erasureDisks := xl.getDisks()
	z.serverPools[0].erasureDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		for i := range erasureDisks[:5] {
			erasureDisks[i] = newNaughtyDisk(erasureDisks[i], nil, errFaultyDisk)
		}
		return erasureDisks
	}

	z.serverPools[0].erasureDisksMu.Unlock()
	_, err = obj.DeleteObject(ctx, bucket, object, ObjectOptions{})
	// since majority of disks are not available, metaquorum is not achieved and hence errErasureWriteQuorum error
	if !errors.Is(err, errErasureWriteQuorum) {
		t.Errorf("Expected deleteObject to fail with %v, but failed with %v", toObjectErr(errErasureWriteQuorum, bucket, object), err)
	}
}

func TestErasureDeleteObjectDiskNotFoundErr(t *testing.T) {
	restoreGlobalStorageClass := globalStorageClass
	defer func() {
		globalStorageClass = restoreGlobalStorageClass
	}()

	globalStorageClass = storageclass.Config{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend.
	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup backend directories
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]

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
	// for a 16 disk setup, EC is 4, but will be upgraded up to 8.
	// Remove 4 disks.
	erasureDisks := xl.getDisks()
	z.serverPools[0].erasureDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		for i := range erasureDisks[:4] {
			erasureDisks[i] = newNaughtyDisk(erasureDisks[i], nil, errFaultyDisk)
		}
		return erasureDisks
	}

	z.serverPools[0].erasureDisksMu.Unlock()
	_, err = obj.DeleteObject(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Create "obj" under "bucket".
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	// Object was uploaded with 4 known bad drives, so we should still be able to lose 3 drives and still write to the object.
	erasureDisks = xl.getDisks()
	z.serverPools[0].erasureDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		erasureDisks[7] = nil
		erasureDisks[8] = nil
		erasureDisks[9] = nil
		return erasureDisks
	}

	z.serverPools[0].erasureDisksMu.Unlock()
	_, err = obj.DeleteObject(ctx, bucket, object, ObjectOptions{})
	// since majority of disks are available, metaquorum achieved.
	if err != nil {
		t.Errorf("Expected deleteObject to not fail, but failed with %v", err)
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
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, "bucket", BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	opts := ObjectOptions{}
	buf := make([]byte, smallFileThreshold*16)
	if _, err = io.ReadFull(crand.Reader, buf); err != nil {
		t.Fatal(err)
	}

	// Test use case 1: All disks are online, xl.meta are present, but data are missing
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(buf), int64(len(buf)), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	for _, disk := range xl.getDisks() {
		files, _ := disk.ListDir(ctx, bucket, object, -1)
		for _, file := range files {
			if file != "xl.meta" {
				disk.Delete(ctx, bucket, pathJoin(object, file), true)
			}
		}
	}

	gr, err := xl.GetObjectNInfo(ctx, bucket, object, nil, nil, readLock, opts)
	if err != nil {
		if err != toObjectErr(errErasureReadQuorum, bucket, object) {
			t.Errorf("Expected GetObject to fail with %v, but failed with %v", toObjectErr(errErasureReadQuorum, bucket, object), err)
		}
	}
	if gr != nil {
		_, err = io.Copy(ioutil.Discard, gr)
		if err != toObjectErr(errErasureReadQuorum, bucket, object) {
			t.Errorf("Expected GetObject to fail with %v, but failed with %v", toObjectErr(errErasureReadQuorum, bucket, object), err)
		}
		gr.Close()
	}

	// Test use case 2: Make 9 disks offline, which leaves less than quorum number of disks
	// in a 16 disk Erasure setup. The original disks are 'replaced' with
	// naughtyDisks that fail after 'f' successful StorageAPI method
	// invocations, where f - [0,2)

	// Create "object" under "bucket".
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(buf), int64(len(buf)), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

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
		z.serverPools[0].erasureDisksMu.Lock()
		xl.getDisks = func() []StorageAPI {
			return erasureDisks
		}
		z.serverPools[0].erasureDisksMu.Unlock()
		// Fetch object from store.
		gr, err := xl.GetObjectNInfo(ctx, bucket, object, nil, nil, readLock, opts)
		if err != nil {
			if err != toObjectErr(errErasureReadQuorum, bucket, object) {
				t.Errorf("Expected GetObject to fail with %v, but failed with %v", toObjectErr(errErasureReadQuorum, bucket, object), err)
			}
		}
		if gr != nil {
			_, err = io.Copy(ioutil.Discard, gr)
			if err != toObjectErr(errErasureReadQuorum, bucket, object) {
				t.Errorf("Expected GetObject to fail with %v, but failed with %v", toObjectErr(errErasureReadQuorum, bucket, object), err)
			}
			gr.Close()
		}
	}

}

func TestHeadObjectNoQuorum(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend.
	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup backend directories.
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, "bucket", BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	opts := ObjectOptions{}

	// Test use case 1: All disks are online, xl.meta are present, but data are missing
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, disk := range xl.getDisks() {
		files, _ := disk.ListDir(ctx, bucket, object, -1)
		for _, file := range files {
			if file != "xl.meta" {
				disk.Delete(ctx, bucket, pathJoin(object, file), true)
			}
		}
	}

	_, err = xl.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		t.Errorf("Expected StatObject to succeed if data dir are not found, but failed with %v", err)
	}

	// Test use case 2: Make 9 disks offline, which leaves less than quorum number of disks
	// in a 16 disk Erasure setup. The original disks are 'replaced' with
	// naughtyDisks that fail after 'f' successful StorageAPI method
	// invocations, where f - [0,2)

	// Create "object" under "bucket".
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	erasureDisks := xl.getDisks()
	for i := range erasureDisks[:10] {
		erasureDisks[i] = nil
	}

	z.serverPools[0].erasureDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		return erasureDisks
	}
	z.serverPools[0].erasureDisksMu.Unlock()

	// Fetch object from store.
	_, err = xl.GetObjectInfo(ctx, bucket, object, opts)
	if err != toObjectErr(errErasureReadQuorum, bucket, object) {
		t.Errorf("Expected getObjectInfo to fail with %v, but failed with %v", toObjectErr(errErasureWriteQuorum, bucket, object), err)
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
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, "bucket", BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	opts := ObjectOptions{}
	// Create "object" under "bucket".
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(bytes.Repeat([]byte{'a'}, smallFileThreshold*16)), smallFileThreshold*16, "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	// Make 9 disks offline, which leaves less than quorum number of disks
	// in a 16 disk Erasure setup. The original disks are 'replaced' with
	// naughtyDisks that fail after 'f' successful StorageAPI method
	// invocations, where f - [0,4)
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
		z.serverPools[0].erasureDisksMu.Lock()
		xl.getDisks = func() []StorageAPI {
			return erasureDisks
		}
		z.serverPools[0].erasureDisksMu.Unlock()
		// Upload new content to same object "object"
		_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(bytes.Repeat([]byte{byte(f)}, smallFileThreshold*16)), smallFileThreshold*16, "", ""), opts)
		if !errors.Is(err, errErasureWriteQuorum) {
			t.Errorf("Expected putObject to fail with %v, but failed with %v", toObjectErr(errErasureWriteQuorum, bucket, object), err)
		}
	}
}

func TestPutObjectNoQuorumSmall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend.
	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Cleanup backend directories.
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, "bucket", BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	opts := ObjectOptions{}
	// Create "object" under "bucket".
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(bytes.Repeat([]byte{'a'}, smallFileThreshold/2)), smallFileThreshold/2, "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	// Make 9 disks offline, which leaves less than quorum number of disks
	// in a 16 disk Erasure setup. The original disks are 'replaced' with
	// naughtyDisks that fail after 'f' successful StorageAPI method
	// invocations, where f - [0,2)
	for f := 0; f < 2; f++ {
		t.Run("exec-"+strconv.Itoa(f), func(t *testing.T) {
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
			z.serverPools[0].erasureDisksMu.Lock()
			xl.getDisks = func() []StorageAPI {
				return erasureDisks
			}
			z.serverPools[0].erasureDisksMu.Unlock()
			// Upload new content to same object "object"
			_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(bytes.Repeat([]byte{byte(f)}, smallFileThreshold/2)), smallFileThreshold/2, "", ""), opts)
			if !errors.Is(err, errErasureWriteQuorum) {
				t.Errorf("Expected putObject to fail with %v, but failed with %v", toObjectErr(errErasureWriteQuorum, bucket, object), err)
			}
		})
	}
}

// Test PutObject twice, one small and another bigger
// than small data thresold and checks reading them again
func TestPutObjectSmallInlineData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numberOfDisks = 4

	// Create an instance of xl backend.
	obj, fsDirs, err := prepareErasure(ctx, numberOfDisks)
	if err != nil {
		t.Fatal(err)
	}

	// Cleanup backend directories.
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	bucket := "bucket"
	object := "object"

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, bucket, BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Test: Upload a small file and read it.
	smallData := []byte{'a'}
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(smallData), int64(len(smallData)), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gr, err := obj.GetObjectNInfo(ctx, bucket, object, nil, nil, readLock, ObjectOptions{})
	if err != nil {
		t.Fatalf("Expected GetObject to succeed, but failed with %v", err)
	}
	output := bytes.NewBuffer([]byte{})
	_, err = io.Copy(output, gr)
	if err != nil {
		t.Fatalf("Expected GetObject reading data to succeed, but failed with %v", err)
	}
	gr.Close()
	if !bytes.Equal(output.Bytes(), smallData) {
		t.Fatalf("Corrupted data is found")
	}

	// Test: Upload a file bigger than the small file threshold
	// under the same bucket & key name and try to read it again.

	output.Reset()
	bigData := bytes.Repeat([]byte{'b'}, smallFileThreshold*numberOfDisks/2)

	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(bigData), int64(len(bigData)), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gr, err = obj.GetObjectNInfo(ctx, bucket, object, nil, nil, readLock, ObjectOptions{})
	if err != nil {
		t.Fatalf("Expected GetObject to succeed, but failed with %v", err)
	}
	_, err = io.Copy(output, gr)
	if err != nil {
		t.Fatalf("Expected GetObject reading data to succeed, but failed with %v", err)
	}
	gr.Close()
	if !bytes.Equal(output.Bytes(), bigData) {
		t.Fatalf("Corrupted data found")
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

	globalStorageClass = storageclass.Config{}

	bucket := getRandomBucketName()

	var opts ObjectOptions
	// make data with more than one part
	partCount := 3
	data := bytes.Repeat([]byte("a"), 6*1024*1024*partCount)

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]
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

	parts1, errs1 := readAllFileInfo(ctx, erasureDisks, bucket, object1, "", false)
	parts1SC := globalStorageClass

	// Object for test case 2 - No StorageClass defined, MetaData in PutObject requesting RRS Class
	object2 := "object2"
	metadata2 := make(map[string]string)
	metadata2["x-amz-storage-class"] = storageclass.RRS
	_, err = obj.PutObject(ctx, bucket, object2, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata2})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts2, errs2 := readAllFileInfo(ctx, erasureDisks, bucket, object2, "", false)
	parts2SC := globalStorageClass

	// Object for test case 3 - No StorageClass defined, MetaData in PutObject requesting Standard Storage Class
	object3 := "object3"
	metadata3 := make(map[string]string)
	metadata3["x-amz-storage-class"] = storageclass.STANDARD
	_, err = obj.PutObject(ctx, bucket, object3, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata3})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts3, errs3 := readAllFileInfo(ctx, erasureDisks, bucket, object3, "", false)
	parts3SC := globalStorageClass

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

	parts4, errs4 := readAllFileInfo(ctx, erasureDisks, bucket, object4, "", false)
	parts4SC := storageclass.Config{
		Standard: storageclass.StorageClass{
			Parity: 6,
		},
	}

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

	parts5, errs5 := readAllFileInfo(ctx, erasureDisks, bucket, object5, "", false)
	parts5SC := storageclass.Config{
		RRS: storageclass.StorageClass{
			Parity: 2,
		},
	}

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

	parts6, errs6 := readAllFileInfo(ctx, erasureDisks, bucket, object6, "", false)
	parts6SC := storageclass.Config{
		RRS: storageclass.StorageClass{
			Parity: 2,
		},
	}

	// Object for test case 7 - Standard StorageClass defined as Parity 5, MetaData in PutObject requesting RRS Class
	// Reset global storage class flags
	object7 := "object7"
	metadata7 := make(map[string]string)
	metadata7["x-amz-storage-class"] = storageclass.STANDARD
	globalStorageClass = storageclass.Config{
		Standard: storageclass.StorageClass{
			Parity: 5,
		},
	}

	_, err = obj.PutObject(ctx, bucket, object7, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{UserDefined: metadata7})
	if err != nil {
		t.Fatalf("Failed to putObject %v", err)
	}

	parts7, errs7 := readAllFileInfo(ctx, erasureDisks, bucket, object7, "", false)
	parts7SC := storageclass.Config{
		Standard: storageclass.StorageClass{
			Parity: 5,
		},
	}

	tests := []struct {
		parts               []FileInfo
		errs                []error
		expectedReadQuorum  int
		expectedWriteQuorum int
		storageClassCfg     storageclass.Config
		expectedError       error
	}{
		{parts1, errs1, 12, 12, parts1SC, nil},
		{parts2, errs2, 14, 14, parts2SC, nil},
		{parts3, errs3, 12, 12, parts3SC, nil},
		{parts4, errs4, 10, 10, parts4SC, nil},
		{parts5, errs5, 14, 14, parts5SC, nil},
		{parts6, errs6, 12, 12, parts6SC, nil},
		{parts7, errs7, 11, 11, parts7SC, nil},
	}
	for _, tt := range tests {
		tt := tt
		t.(*testing.T).Run("", func(t *testing.T) {
			globalStorageClass = tt.storageClassCfg
			actualReadQuorum, actualWriteQuorum, err := objectQuorumFromMeta(ctx, tt.parts, tt.errs, getDefaultParityBlocks(len(erasureDisks)))
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
