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
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go"
)

// Tests isObjectDangling function
func TestIsObjectDangling(t *testing.T) {
	fi := newFileInfo("test-object", 2, 2)
	fi.Erasure.Index = 1

	testCases := []struct {
		name             string
		metaArr          []FileInfo
		errs             []error
		dataErrs         []error
		expectedMeta     FileInfo
		expectedDangling bool
	}{
		{
			name: "FileInfoExists-case1",
			metaArr: []FileInfo{
				{},
				{},
				fi,
				fi,
			},
			errs: []error{
				errFileNotFound,
				errDiskNotFound,
				nil,
				nil,
			},
			dataErrs:         nil,
			expectedMeta:     fi,
			expectedDangling: false,
		},
		{
			name: "FileInfoExists-case2",
			metaArr: []FileInfo{
				{},
				{},
				fi,
				fi,
			},
			errs: []error{
				errFileNotFound,
				errFileNotFound,
				nil,
				nil,
			},
			dataErrs:         nil,
			expectedMeta:     fi,
			expectedDangling: false,
		},
		{
			name: "FileInfoUndecided-case1",
			metaArr: []FileInfo{
				{},
				{},
				{},
				fi,
			},
			errs: []error{
				errFileNotFound,
				errDiskNotFound,
				errDiskNotFound,
				nil,
			},
			dataErrs:         nil,
			expectedMeta:     fi,
			expectedDangling: false,
		},
		{
			name:    "FileInfoUndecided-case2",
			metaArr: []FileInfo{},
			errs: []error{
				errFileNotFound,
				errDiskNotFound,
				errDiskNotFound,
				errFileNotFound,
			},
			dataErrs:         nil,
			expectedMeta:     FileInfo{},
			expectedDangling: false,
		},
		{
			name:    "FileInfoUndecided-case3(file deleted)",
			metaArr: []FileInfo{},
			errs: []error{
				errFileNotFound,
				errFileNotFound,
				errFileNotFound,
				errFileNotFound,
			},
			dataErrs:         nil,
			expectedMeta:     FileInfo{},
			expectedDangling: false,
		},
		{
			name: "FileInfoDecided-case1",
			metaArr: []FileInfo{
				{},
				{},
				{},
				fi,
			},
			errs: []error{
				errFileNotFound,
				errFileNotFound,
				errFileNotFound,
				nil,
			},
			dataErrs:         nil,
			expectedMeta:     fi,
			expectedDangling: true,
		},
		{
			name: "FileInfoDecided-case2",
			metaArr: []FileInfo{
				{},
				{},
				{},
				fi,
			},
			errs: []error{
				errFileNotFound,
				errFileCorrupt,
				errFileCorrupt,
				nil,
			},
			dataErrs:         nil,
			expectedMeta:     fi,
			expectedDangling: true,
		},
		{
			name: "FileInfoDecided-case2-delete-marker",
			metaArr: []FileInfo{
				{},
				{},
				{},
				{Deleted: true},
			},
			errs: []error{
				errFileNotFound,
				errFileCorrupt,
				errFileCorrupt,
				nil,
			},
			dataErrs:         nil,
			expectedMeta:     FileInfo{Deleted: true},
			expectedDangling: true,
		},
		{
			name: "FileInfoDecided-case2-(duplicate data errors)",
			metaArr: []FileInfo{
				{},
				{},
				{},
				{Deleted: true},
			},
			errs: []error{
				errFileNotFound,
				errFileCorrupt,
				errFileCorrupt,
				nil,
			},
			dataErrs: []error{
				errFileNotFound,
				errFileCorrupt,
				nil,
				nil,
			},
			expectedMeta:     FileInfo{Deleted: true},
			expectedDangling: true,
		},
		// Add new cases as seen
	}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			gotMeta, dangling := isObjectDangling(testCase.metaArr, testCase.errs, testCase.dataErrs)
			if !gotMeta.Equals(testCase.expectedMeta) {
				t.Errorf("Expected %#v, got %#v", testCase.expectedMeta, gotMeta)
			}
			if dangling != testCase.expectedDangling {
				t.Errorf("Expected dangling %t, got %t", testCase.expectedDangling, dangling)
			}
		})
	}
}

// Tests both object and bucket healing.
func TestHealing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Shutdown(context.Background())

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}

	defer removeRoots(fsDirs)

	z := obj.(*erasureServerPools)
	er := z.serverPools[0].sets[0]

	// Create "bucket"
	err = obj.MakeBucketWithLocation(ctx, "bucket", MakeBucketOptions{})
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

	disk := er.getDisks()[0]
	fileInfoPreHeal, err := disk.ReadVersion(context.Background(), bucket, object, "", false)
	if err != nil {
		t.Fatal(err)
	}

	// Remove the object - to simulate the case where the disk was down when the object
	// was created.
	err = removeAll(pathJoin(disk.String(), bucket, object))
	if err != nil {
		t.Fatal(err)
	}

	_, err = er.HealObject(ctx, bucket, object, "", madmin.HealOpts{ScanMode: madmin.HealNormalScan})
	if err != nil {
		t.Fatal(err)
	}

	fileInfoPostHeal, err := disk.ReadVersion(context.Background(), bucket, object, "", false)
	if err != nil {
		t.Fatal(err)
	}

	// After heal the meta file should be as expected.
	if !fileInfoPreHeal.Equals(fileInfoPostHeal) {
		t.Fatal("HealObject failed")
	}

	err = os.RemoveAll(path.Join(fsDirs[0], bucket, object, "xl.meta"))
	if err != nil {
		t.Fatal(err)
	}

	// Write xl.meta with different modtime to simulate the case where a disk had
	// gone down when an object was replaced by a new object.
	fileInfoOutDated := fileInfoPreHeal
	fileInfoOutDated.ModTime = time.Now()
	err = disk.WriteMetadata(context.Background(), bucket, object, fileInfoOutDated)
	if err != nil {
		t.Fatal(err)
	}

	_, err = er.HealObject(ctx, bucket, object, "", madmin.HealOpts{ScanMode: madmin.HealDeepScan})
	if err != nil {
		t.Fatal(err)
	}

	fileInfoPostHeal, err = disk.ReadVersion(context.Background(), bucket, object, "", false)
	if err != nil {
		t.Fatal(err)
	}

	// After heal the meta file should be as expected.
	if !fileInfoPreHeal.Equals(fileInfoPostHeal) {
		t.Fatal("HealObject failed")
	}

	// Remove the bucket - to simulate the case where bucket was
	// created when the disk was down.
	err = os.RemoveAll(path.Join(fsDirs[0], bucket))
	if err != nil {
		t.Fatal(err)
	}
	// This would create the bucket.
	_, err = er.HealBucket(ctx, bucket, madmin.HealOpts{
		DryRun: false,
		Remove: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Stat the bucket to make sure that it was created.
	_, err = er.getDisks()[0].StatVol(context.Background(), bucket)
	if err != nil {
		t.Fatal(err)
	}
}

func TestHealingDanglingObject(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resetGlobalHealState()
	defer resetGlobalHealState()

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	defer removeRoots(fsDirs)

	// Everything is fine, should return nil
	objLayer, disks, err := initObjectLayer(ctx, mustGetPoolEndpoints(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	bucket := getRandomBucketName()
	object := getRandomObjectName()
	data := bytes.Repeat([]byte("a"), 128*1024)

	err = objLayer.MakeBucketWithLocation(ctx, bucket, MakeBucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket - %v", err)
	}

	disks = objLayer.(*erasureServerPools).serverPools[0].erasureDisks[0]
	orgDisks := append([]StorageAPI{}, disks...)

	// Enable versioning.
	globalBucketMetadataSys.Update(ctx, bucket, bucketVersioningConfig, []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))

	_, err = objLayer.PutObject(ctx, bucket, object, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{
		Versioned: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	setDisks := func(newDisks ...StorageAPI) {
		objLayer.(*erasureServerPools).serverPools[0].erasureDisksMu.Lock()
		copy(disks, newDisks)
		objLayer.(*erasureServerPools).serverPools[0].erasureDisksMu.Unlock()
	}
	getDisk := func(n int) StorageAPI {
		objLayer.(*erasureServerPools).serverPools[0].erasureDisksMu.Lock()
		disk := disks[n]
		objLayer.(*erasureServerPools).serverPools[0].erasureDisksMu.Unlock()
		return disk
	}

	// Remove 4 disks.
	setDisks(nil, nil, nil, nil)

	// Create delete marker under quorum.
	objInfo, err := objLayer.DeleteObject(ctx, bucket, object, ObjectOptions{Versioned: true})
	if err != nil {
		t.Fatal(err)
	}

	// Restore...
	setDisks(orgDisks[:4]...)

	fileInfoPreHeal, err := disks[0].ReadVersion(context.Background(), bucket, object, "", false)
	if err != nil {
		t.Fatal(err)
	}

	if fileInfoPreHeal.NumVersions != 1 {
		t.Fatalf("Expected versions 1, got %d", fileInfoPreHeal.NumVersions)
	}

	if err = objLayer.HealObjects(ctx, bucket, "", madmin.HealOpts{Remove: true},
		func(bucket, object, vid string) error {
			_, err := objLayer.HealObject(ctx, bucket, object, vid, madmin.HealOpts{Remove: true})
			return err
		}); err != nil {
		t.Fatal(err)
	}

	fileInfoPostHeal, err := disks[0].ReadVersion(context.Background(), bucket, object, "", false)
	if err != nil {
		t.Fatal(err)
	}

	if fileInfoPostHeal.NumVersions != 2 {
		t.Fatalf("Expected versions 2, got %d", fileInfoPreHeal.NumVersions)
	}

	if objInfo.DeleteMarker {
		if _, err = objLayer.DeleteObject(ctx, bucket, object, ObjectOptions{
			Versioned: true,
			VersionID: objInfo.VersionID,
		}); err != nil {
			t.Fatal(err)
		}
	}

	setDisks(nil, nil, nil, nil)

	rd := mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", "")
	_, err = objLayer.PutObject(ctx, bucket, object, rd, ObjectOptions{
		Versioned: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	setDisks(orgDisks[:4]...)
	disk := getDisk(0)
	fileInfoPreHeal, err = disk.ReadVersion(context.Background(), bucket, object, "", false)
	if err != nil {
		t.Fatal(err)
	}

	if fileInfoPreHeal.NumVersions != 1 {
		t.Fatalf("Expected versions 1, got %d", fileInfoPreHeal.NumVersions)
	}

	if err = objLayer.HealObjects(ctx, bucket, "", madmin.HealOpts{Remove: true},
		func(bucket, object, vid string) error {
			_, err := objLayer.HealObject(ctx, bucket, object, vid, madmin.HealOpts{Remove: true})
			return err
		}); err != nil {
		t.Fatal(err)
	}

	disk = getDisk(0)
	fileInfoPostHeal, err = disk.ReadVersion(context.Background(), bucket, object, "", false)
	if err != nil {
		t.Fatal(err)
	}

	if fileInfoPostHeal.NumVersions != 2 {
		t.Fatalf("Expected versions 2, got %d", fileInfoPreHeal.NumVersions)
	}

	rd = mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", "")
	objInfo, err = objLayer.PutObject(ctx, bucket, object, rd, ObjectOptions{
		Versioned: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	setDisks(nil, nil, nil, nil)

	// Create delete marker under quorum.
	_, err = objLayer.DeleteObject(ctx, bucket, object, ObjectOptions{
		Versioned: true,
		VersionID: objInfo.VersionID,
	})
	if err != nil {
		t.Fatal(err)
	}

	setDisks(orgDisks[:4]...)

	disk = getDisk(0)
	fileInfoPreHeal, err = disk.ReadVersion(context.Background(), bucket, object, "", false)
	if err != nil {
		t.Fatal(err)
	}

	if fileInfoPreHeal.NumVersions != 3 {
		t.Fatalf("Expected versions 3, got %d", fileInfoPreHeal.NumVersions)
	}

	if err = objLayer.HealObjects(ctx, bucket, "", madmin.HealOpts{Remove: true},
		func(bucket, object, vid string) error {
			_, err := objLayer.HealObject(ctx, bucket, object, vid, madmin.HealOpts{Remove: true})
			return err
		}); err != nil {
		t.Fatal(err)
	}

	disk = getDisk(0)
	fileInfoPostHeal, err = disk.ReadVersion(context.Background(), bucket, object, "", false)
	if err != nil {
		t.Fatal(err)
	}

	if fileInfoPostHeal.NumVersions != 2 {
		t.Fatalf("Expected versions 2, got %d", fileInfoPreHeal.NumVersions)
	}
}

func TestHealCorrectQuorum(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resetGlobalHealState()
	defer resetGlobalHealState()

	nDisks := 32
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	defer removeRoots(fsDirs)

	pools := mustGetPoolEndpoints(fsDirs[:16]...)
	pools = append(pools, mustGetPoolEndpoints(fsDirs[16:]...)...)

	// Everything is fine, should return nil
	objLayer, _, err := initObjectLayer(ctx, pools)
	if err != nil {
		t.Fatal(err)
	}

	bucket := getRandomBucketName()
	object := getRandomObjectName()
	data := bytes.Repeat([]byte("a"), 5*1024*1024)
	var opts ObjectOptions

	err = objLayer.MakeBucketWithLocation(ctx, bucket, MakeBucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket - %v", err)
	}

	// Create an object with multiple parts uploaded in decreasing
	// part number.
	res, err := objLayer.NewMultipartUpload(ctx, bucket, object, opts)
	if err != nil {
		t.Fatalf("Failed to create a multipart upload - %v", err)
	}

	var uploadedParts []CompletePart
	for _, partID := range []int{2, 1} {
		pInfo, err1 := objLayer.PutObjectPart(ctx, bucket, object, res.UploadID, partID, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), opts)
		if err1 != nil {
			t.Fatalf("Failed to upload a part - %v", err1)
		}
		uploadedParts = append(uploadedParts, CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}

	_, err = objLayer.CompleteMultipartUpload(ctx, bucket, object, res.UploadID, uploadedParts, ObjectOptions{})
	if err != nil {
		t.Fatalf("Failed to complete multipart upload - got: %v", err)
	}

	cfgFile := pathJoin(bucketMetaPrefix, bucket, ".test.bin")
	if err = saveConfig(ctx, objLayer, cfgFile, data); err != nil {
		t.Fatal(err)
	}

	hopts := madmin.HealOpts{
		DryRun:   false,
		Remove:   true,
		ScanMode: madmin.HealNormalScan,
	}

	// Test 1: Remove the object backend files from the first disk.
	z := objLayer.(*erasureServerPools)
	for _, set := range z.serverPools {
		er := set.sets[0]
		erasureDisks := er.getDisks()

		fileInfos, errs := readAllFileInfo(ctx, erasureDisks, bucket, object, "", false)
		nfi, err := getLatestFileInfo(ctx, fileInfos, er.defaultParityCount, errs)
		if errors.Is(err, errFileNotFound) {
			continue
		}
		if err != nil {
			t.Fatalf("Failed to getLatestFileInfo - %v", err)
		}

		for i := 0; i < nfi.Erasure.ParityBlocks; i++ {
			erasureDisks[i].Delete(context.Background(), bucket, pathJoin(object, xlStorageFormatFile), DeleteOptions{
				Recursive: false,
				Force:     false,
			})
		}

		// Try healing now, it should heal the content properly.
		_, err = objLayer.HealObject(ctx, bucket, object, "", hopts)
		if err != nil {
			t.Fatal(err)
		}

		fileInfos, errs = readAllFileInfo(ctx, erasureDisks, bucket, object, "", false)
		if countErrs(errs, nil) != len(fileInfos) {
			t.Fatal("Expected all xl.meta healed, but partial heal detected")
		}

		fileInfos, errs = readAllFileInfo(ctx, erasureDisks, minioMetaBucket, cfgFile, "", false)
		nfi, err = getLatestFileInfo(ctx, fileInfos, er.defaultParityCount, errs)
		if errors.Is(err, errFileNotFound) {
			continue
		}
		if err != nil {
			t.Fatalf("Failed to getLatestFileInfo - %v", err)
		}

		for i := 0; i < nfi.Erasure.ParityBlocks; i++ {
			erasureDisks[i].Delete(context.Background(), minioMetaBucket, pathJoin(cfgFile, xlStorageFormatFile), DeleteOptions{
				Recursive: false,
				Force:     false,
			})
		}

		// Try healing now, it should heal the content properly.
		_, err = objLayer.HealObject(ctx, minioMetaBucket, cfgFile, "", hopts)
		if err != nil {
			t.Fatal(err)
		}

		fileInfos, errs = readAllFileInfo(ctx, erasureDisks, minioMetaBucket, cfgFile, "", false)
		if countErrs(errs, nil) != len(fileInfos) {
			t.Fatal("Expected all xl.meta healed, but partial heal detected")
		}
	}
}

func TestHealObjectCorruptedPools(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resetGlobalHealState()
	defer resetGlobalHealState()

	nDisks := 32
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	defer removeRoots(fsDirs)

	pools := mustGetPoolEndpoints(fsDirs[:16]...)
	pools = append(pools, mustGetPoolEndpoints(fsDirs[16:]...)...)

	// Everything is fine, should return nil
	objLayer, _, err := initObjectLayer(ctx, pools)
	if err != nil {
		t.Fatal(err)
	}

	bucket := getRandomBucketName()
	object := getRandomObjectName()
	data := bytes.Repeat([]byte("a"), 5*1024*1024)
	var opts ObjectOptions

	err = objLayer.MakeBucketWithLocation(ctx, bucket, MakeBucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket - %v", err)
	}

	// Upload a multipart object in the second pool
	z := objLayer.(*erasureServerPools)
	set := z.serverPools[1]

	res, err := set.NewMultipartUpload(ctx, bucket, object, opts)
	if err != nil {
		t.Fatalf("Failed to create a multipart upload - %v", err)
	}
	uploadID := res.UploadID

	var uploadedParts []CompletePart
	for _, partID := range []int{2, 1} {
		pInfo, err1 := set.PutObjectPart(ctx, bucket, object, uploadID, partID, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), opts)
		if err1 != nil {
			t.Fatalf("Failed to upload a part - %v", err1)
		}
		uploadedParts = append(uploadedParts, CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}

	_, err = set.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, ObjectOptions{})
	if err != nil {
		t.Fatalf("Failed to complete multipart upload - %v", err)
	}

	// Test 1: Remove the object backend files from the first disk.
	er := set.sets[0]
	erasureDisks := er.getDisks()
	firstDisk := erasureDisks[0]
	err = firstDisk.Delete(context.Background(), bucket, pathJoin(object, xlStorageFormatFile), DeleteOptions{
		Recursive: false,
		Force:     false,
	})
	if err != nil {
		t.Fatalf("Failed to delete a file - %v", err)
	}

	_, err = objLayer.HealObject(ctx, bucket, object, "", madmin.HealOpts{ScanMode: madmin.HealNormalScan})
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	fileInfos, errs := readAllFileInfo(ctx, erasureDisks, bucket, object, "", false)
	fi, err := getLatestFileInfo(ctx, fileInfos, er.defaultParityCount, errs)
	if err != nil {
		t.Fatalf("Failed to getLatestFileInfo - %v", err)
	}

	if _, err = firstDisk.StatInfoFile(context.Background(), bucket, object+"/"+xlStorageFormatFile, false); err != nil {
		t.Errorf("Expected xl.meta file to be present but stat failed - %v", err)
	}

	err = firstDisk.Delete(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"), DeleteOptions{
		Recursive: false,
		Force:     false,
	})
	if err != nil {
		t.Errorf("Failure during deleting part.1 - %v", err)
	}

	err = firstDisk.WriteAll(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"), []byte{})
	if err != nil {
		t.Errorf("Failure during creating part.1 - %v", err)
	}

	_, err = objLayer.HealObject(ctx, bucket, object, "", madmin.HealOpts{DryRun: false, Remove: true, ScanMode: madmin.HealDeepScan})
	if err != nil {
		t.Errorf("Expected nil but received %v", err)
	}

	fileInfos, errs = readAllFileInfo(ctx, erasureDisks, bucket, object, "", false)
	nfi, err := getLatestFileInfo(ctx, fileInfos, er.defaultParityCount, errs)
	if err != nil {
		t.Fatalf("Failed to getLatestFileInfo - %v", err)
	}

	fi.DiskMTime = time.Time{}
	nfi.DiskMTime = time.Time{}
	if !reflect.DeepEqual(fi, nfi) {
		t.Fatalf("FileInfo not equal after healing: %v != %v", fi, nfi)
	}

	err = firstDisk.Delete(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"), DeleteOptions{
		Recursive: false,
		Force:     false,
	})
	if err != nil {
		t.Errorf("Failure during deleting part.1 - %v", err)
	}

	bdata := bytes.Repeat([]byte("b"), int(nfi.Size))
	err = firstDisk.WriteAll(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"), bdata)
	if err != nil {
		t.Errorf("Failure during creating part.1 - %v", err)
	}

	_, err = objLayer.HealObject(ctx, bucket, object, "", madmin.HealOpts{DryRun: false, Remove: true, ScanMode: madmin.HealDeepScan})
	if err != nil {
		t.Errorf("Expected nil but received %v", err)
	}

	fileInfos, errs = readAllFileInfo(ctx, erasureDisks, bucket, object, "", false)
	nfi, err = getLatestFileInfo(ctx, fileInfos, er.defaultParityCount, errs)
	if err != nil {
		t.Fatalf("Failed to getLatestFileInfo - %v", err)
	}

	fi.DiskMTime = time.Time{}
	nfi.DiskMTime = time.Time{}
	if !reflect.DeepEqual(fi, nfi) {
		t.Fatalf("FileInfo not equal after healing: %v != %v", fi, nfi)
	}

	// Test 4: checks if HealObject returns an error when xl.meta is not found
	// in more than read quorum number of disks, to create a corrupted situation.
	for i := 0; i <= nfi.Erasure.DataBlocks; i++ {
		erasureDisks[i].Delete(context.Background(), bucket, pathJoin(object, xlStorageFormatFile), DeleteOptions{
			Recursive: false,
			Force:     false,
		})
	}

	// Try healing now, expect to receive errFileNotFound.
	_, err = objLayer.HealObject(ctx, bucket, object, "", madmin.HealOpts{DryRun: false, Remove: true, ScanMode: madmin.HealDeepScan})
	if err != nil {
		if _, ok := err.(ObjectNotFound); !ok {
			t.Errorf("Expect %v but received %v", ObjectNotFound{Bucket: bucket, Object: object}, err)
		}
	}

	// since majority of xl.meta's are not available, object should be successfully deleted.
	_, err = objLayer.GetObjectInfo(ctx, bucket, object, ObjectOptions{})
	if _, ok := err.(ObjectNotFound); !ok {
		t.Errorf("Expect %v but received %v", ObjectNotFound{Bucket: bucket, Object: object}, err)
	}

	for i := 0; i < (nfi.Erasure.DataBlocks + nfi.Erasure.ParityBlocks); i++ {
		_, err = erasureDisks[i].StatInfoFile(context.Background(), bucket, pathJoin(object, xlStorageFormatFile), false)
		if err == nil {
			t.Errorf("Expected xl.meta file to be not present, but succeeeded")
		}
	}
}

func TestHealObjectCorruptedXLMeta(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resetGlobalHealState()
	defer resetGlobalHealState()

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	defer removeRoots(fsDirs)

	// Everything is fine, should return nil
	objLayer, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	bucket := getRandomBucketName()
	object := getRandomObjectName()
	data := bytes.Repeat([]byte("a"), 5*1024*1024)
	var opts ObjectOptions

	err = objLayer.MakeBucketWithLocation(ctx, bucket, MakeBucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket - %v", err)
	}

	// Create an object with multiple parts uploaded in decreasing
	// part number.
	res, err := objLayer.NewMultipartUpload(ctx, bucket, object, opts)
	if err != nil {
		t.Fatalf("Failed to create a multipart upload - %v", err)
	}

	var uploadedParts []CompletePart
	for _, partID := range []int{2, 1} {
		pInfo, err1 := objLayer.PutObjectPart(ctx, bucket, object, res.UploadID, partID, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), opts)
		if err1 != nil {
			t.Fatalf("Failed to upload a part - %v", err1)
		}
		uploadedParts = append(uploadedParts, CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}

	_, err = objLayer.CompleteMultipartUpload(ctx, bucket, object, res.UploadID, uploadedParts, ObjectOptions{})
	if err != nil {
		t.Fatalf("Failed to complete multipart upload - %v", err)
	}

	z := objLayer.(*erasureServerPools)
	er := z.serverPools[0].sets[0]
	erasureDisks := er.getDisks()
	firstDisk := erasureDisks[0]

	// Test 1: Remove the object backend files from the first disk.
	fileInfos, errs := readAllFileInfo(ctx, erasureDisks, bucket, object, "", false)
	fi, err := getLatestFileInfo(ctx, fileInfos, er.defaultParityCount, errs)
	if err != nil {
		t.Fatalf("Failed to getLatestFileInfo - %v", err)
	}

	err = firstDisk.Delete(context.Background(), bucket, pathJoin(object, xlStorageFormatFile), DeleteOptions{
		Recursive: false,
		Force:     false,
	})
	if err != nil {
		t.Fatalf("Failed to delete a file - %v", err)
	}

	_, err = objLayer.HealObject(ctx, bucket, object, "", madmin.HealOpts{ScanMode: madmin.HealNormalScan})
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	if _, err = firstDisk.StatInfoFile(context.Background(), bucket, object+"/"+xlStorageFormatFile, false); err != nil {
		t.Errorf("Expected xl.meta file to be present but stat failed - %v", err)
	}

	fileInfos, errs = readAllFileInfo(ctx, erasureDisks, bucket, object, "", false)
	nfi1, err := getLatestFileInfo(ctx, fileInfos, er.defaultParityCount, errs)
	if err != nil {
		t.Fatalf("Failed to getLatestFileInfo - %v", err)
	}

	fi.DiskMTime = time.Time{}
	nfi1.DiskMTime = time.Time{}
	if !reflect.DeepEqual(fi, nfi1) {
		t.Fatalf("FileInfo not equal after healing")
	}

	// Test 2: Test with a corrupted xl.meta
	err = firstDisk.WriteAll(context.Background(), bucket, pathJoin(object, xlStorageFormatFile), []byte("abcd"))
	if err != nil {
		t.Errorf("Failure during creating part.1 - %v", err)
	}

	_, err = objLayer.HealObject(ctx, bucket, object, "", madmin.HealOpts{ScanMode: madmin.HealNormalScan})
	if err != nil {
		t.Errorf("Expected nil but received %v", err)
	}

	fileInfos, errs = readAllFileInfo(ctx, erasureDisks, bucket, object, "", false)
	nfi2, err := getLatestFileInfo(ctx, fileInfos, er.defaultParityCount, errs)
	if err != nil {
		t.Fatalf("Failed to getLatestFileInfo - %v", err)
	}

	fi.DiskMTime = time.Time{}
	nfi2.DiskMTime = time.Time{}
	if !reflect.DeepEqual(fi, nfi2) {
		t.Fatalf("FileInfo not equal after healing")
	}

	// Test 3: checks if HealObject returns an error when xl.meta is not found
	// in more than read quorum number of disks, to create a corrupted situation.
	for i := 0; i <= nfi2.Erasure.DataBlocks; i++ {
		erasureDisks[i].Delete(context.Background(), bucket, pathJoin(object, xlStorageFormatFile), DeleteOptions{
			Recursive: false,
			Force:     false,
		})
	}

	// Try healing now, expect to receive errFileNotFound.
	_, err = objLayer.HealObject(ctx, bucket, object, "", madmin.HealOpts{DryRun: false, Remove: true, ScanMode: madmin.HealDeepScan})
	if err != nil {
		if _, ok := err.(ObjectNotFound); !ok {
			t.Errorf("Expect %v but received %v", ObjectNotFound{Bucket: bucket, Object: object}, err)
		}
	}

	// since majority of xl.meta's are not available, object should be successfully deleted.
	_, err = objLayer.GetObjectInfo(ctx, bucket, object, ObjectOptions{})
	if _, ok := err.(ObjectNotFound); !ok {
		t.Errorf("Expect %v but received %v", ObjectNotFound{Bucket: bucket, Object: object}, err)
	}
}

func TestHealObjectCorruptedParts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resetGlobalHealState()
	defer resetGlobalHealState()

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	defer removeRoots(fsDirs)

	// Everything is fine, should return nil
	objLayer, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	bucket := getRandomBucketName()
	object := getRandomObjectName()
	data := bytes.Repeat([]byte("a"), 5*1024*1024)
	var opts ObjectOptions

	err = objLayer.MakeBucketWithLocation(ctx, bucket, MakeBucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket - %v", err)
	}

	// Create an object with multiple parts uploaded in decreasing
	// part number.
	res, err := objLayer.NewMultipartUpload(ctx, bucket, object, opts)
	if err != nil {
		t.Fatalf("Failed to create a multipart upload - %v", err)
	}

	var uploadedParts []CompletePart
	for _, partID := range []int{2, 1} {
		pInfo, err1 := objLayer.PutObjectPart(ctx, bucket, object, res.UploadID, partID, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), opts)
		if err1 != nil {
			t.Fatalf("Failed to upload a part - %v", err1)
		}
		uploadedParts = append(uploadedParts, CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}

	_, err = objLayer.CompleteMultipartUpload(ctx, bucket, object, res.UploadID, uploadedParts, ObjectOptions{})
	if err != nil {
		t.Fatalf("Failed to complete multipart upload - %v", err)
	}

	// Test 1: Remove the object backend files from the first disk.
	z := objLayer.(*erasureServerPools)
	er := z.serverPools[0].sets[0]
	erasureDisks := er.getDisks()
	firstDisk := erasureDisks[0]
	secondDisk := erasureDisks[1]

	fileInfos, errs := readAllFileInfo(ctx, erasureDisks, bucket, object, "", false)
	fi, err := getLatestFileInfo(ctx, fileInfos, er.defaultParityCount, errs)
	if err != nil {
		t.Fatalf("Failed to getLatestFileInfo - %v", err)
	}

	part1Disk1Origin, err := firstDisk.ReadAll(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"))
	if err != nil {
		t.Fatalf("Failed to read a file - %v", err)
	}

	part1Disk2Origin, err := secondDisk.ReadAll(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"))
	if err != nil {
		t.Fatalf("Failed to read a file - %v", err)
	}

	// Test 1, remove part.1
	err = firstDisk.Delete(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"), DeleteOptions{
		Recursive: false,
		Force:     false,
	})
	if err != nil {
		t.Fatalf("Failed to delete a file - %v", err)
	}

	_, err = objLayer.HealObject(ctx, bucket, object, "", madmin.HealOpts{ScanMode: madmin.HealNormalScan})
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	part1Replaced, err := firstDisk.ReadAll(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"))
	if err != nil {
		t.Fatalf("Failed to read a file - %v", err)
	}

	if !reflect.DeepEqual(part1Disk1Origin, part1Replaced) {
		t.Fatalf("part.1 not healed correctly")
	}

	// Test 2, Corrupt part.1
	err = firstDisk.WriteAll(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"), []byte("foobytes"))
	if err != nil {
		t.Fatalf("Failed to write a file - %v", err)
	}

	_, err = objLayer.HealObject(ctx, bucket, object, "", madmin.HealOpts{ScanMode: madmin.HealNormalScan})
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	part1Replaced, err = firstDisk.ReadAll(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"))
	if err != nil {
		t.Fatalf("Failed to read a file - %v", err)
	}

	if !reflect.DeepEqual(part1Disk1Origin, part1Replaced) {
		t.Fatalf("part.1 not healed correctly")
	}

	// Test 3, Corrupt one part and remove data in another disk
	err = firstDisk.WriteAll(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"), []byte("foobytes"))
	if err != nil {
		t.Fatalf("Failed to write a file - %v", err)
	}

	err = secondDisk.Delete(context.Background(), bucket, object, DeleteOptions{
		Recursive: true,
		Force:     false,
	})
	if err != nil {
		t.Fatalf("Failed to delete a file - %v", err)
	}

	_, err = objLayer.HealObject(ctx, bucket, object, "", madmin.HealOpts{ScanMode: madmin.HealNormalScan})
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	partReconstructed, err := firstDisk.ReadAll(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"))
	if err != nil {
		t.Fatalf("Failed to read a file - %v", err)
	}

	if !reflect.DeepEqual(part1Disk1Origin, partReconstructed) {
		t.Fatalf("part.1 not healed correctly")
	}

	partReconstructed, err = secondDisk.ReadAll(context.Background(), bucket, pathJoin(object, fi.DataDir, "part.1"))
	if err != nil {
		t.Fatalf("Failed to read a file - %v", err)
	}

	if !reflect.DeepEqual(part1Disk2Origin, partReconstructed) {
		t.Fatalf("part.1 not healed correctly")
	}
}

// Tests healing of object.
func TestHealObjectErasure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}

	defer removeRoots(fsDirs)

	// Everything is fine, should return nil
	obj, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"
	data := bytes.Repeat([]byte("a"), 5*1024*1024)
	var opts ObjectOptions

	err = obj.MakeBucketWithLocation(ctx, bucket, MakeBucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket - %v", err)
	}

	// Create an object with multiple parts uploaded in decreasing
	// part number.
	res, err := obj.NewMultipartUpload(ctx, bucket, object, opts)
	if err != nil {
		t.Fatalf("Failed to create a multipart upload - %v", err)
	}

	var uploadedParts []CompletePart
	for _, partID := range []int{2, 1} {
		pInfo, err1 := obj.PutObjectPart(ctx, bucket, object, res.UploadID, partID, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), opts)
		if err1 != nil {
			t.Fatalf("Failed to upload a part - %v", err1)
		}
		uploadedParts = append(uploadedParts, CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}

	// Remove the object backend files from the first disk.
	z := obj.(*erasureServerPools)
	er := z.serverPools[0].sets[0]
	firstDisk := er.getDisks()[0]

	_, err = obj.CompleteMultipartUpload(ctx, bucket, object, res.UploadID, uploadedParts, ObjectOptions{})
	if err != nil {
		t.Fatalf("Failed to complete multipart upload - %v", err)
	}

	// Delete the whole object folder
	err = firstDisk.Delete(context.Background(), bucket, object, DeleteOptions{
		Recursive: true,
		Force:     false,
	})
	if err != nil {
		t.Fatalf("Failed to delete a file - %v", err)
	}

	_, err = obj.HealObject(ctx, bucket, object, "", madmin.HealOpts{ScanMode: madmin.HealNormalScan})
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	if _, err = firstDisk.StatInfoFile(context.Background(), bucket, object+"/"+xlStorageFormatFile, false); err != nil {
		t.Errorf("Expected xl.meta file to be present but stat failed - %v", err)
	}

	erasureDisks := er.getDisks()
	z.serverPools[0].erasureDisksMu.Lock()
	er.getDisks = func() []StorageAPI {
		// Nil more than half the disks, to remove write quorum.
		for i := 0; i <= len(erasureDisks)/2; i++ {
			erasureDisks[i] = nil
		}
		return erasureDisks
	}
	z.serverPools[0].erasureDisksMu.Unlock()

	// Try healing now, expect to receive errDiskNotFound.
	_, err = obj.HealObject(ctx, bucket, object, "", madmin.HealOpts{
		ScanMode: madmin.HealDeepScan,
	})
	// since majority of xl.meta's are not available, object quorum
	// can't be read properly will be deleted automatically and
	// err is nil
	if !isErrObjectNotFound(err) {
		t.Fatal(err)
	}
}

// Tests healing of empty directories
func TestHealEmptyDirectoryErasure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDisks := 16
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	// Everything is fine, should return nil
	obj, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(fsDirs...))
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "empty-dir/"
	var opts ObjectOptions

	err = obj.MakeBucketWithLocation(ctx, bucket, MakeBucketOptions{})
	if err != nil {
		t.Fatalf("Failed to make a bucket - %v", err)
	}

	// Upload an empty directory
	_, err = obj.PutObject(ctx, bucket, object, mustGetPutObjReader(t,
		bytes.NewReader([]byte{}), 0, "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}

	// Remove the object backend files from the first disk.
	z := obj.(*erasureServerPools)
	er := z.serverPools[0].sets[0]
	firstDisk := er.getDisks()[0]
	err = firstDisk.DeleteVol(context.Background(), pathJoin(bucket, encodeDirObject(object)), true)
	if err != nil {
		t.Fatalf("Failed to delete a file - %v", err)
	}

	// Heal the object
	hr, err := obj.HealObject(ctx, bucket, object, "", madmin.HealOpts{ScanMode: madmin.HealNormalScan})
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	// Check if the empty directory is restored in the first disk
	_, err = firstDisk.StatVol(context.Background(), pathJoin(bucket, encodeDirObject(object)))
	if err != nil {
		t.Fatalf("Expected object to be present but stat failed - %v", err)
	}

	// Check the state of the object in the first disk (should be missing)
	if hr.Before.Drives[0].State != madmin.DriveStateMissing {
		t.Fatalf("Unexpected drive state: %v", hr.Before.Drives[0].State)
	}

	// Check the state of all other disks (should be ok)
	for i, h := range append(hr.Before.Drives[1:], hr.After.Drives...) {
		if h.State != madmin.DriveStateOk {
			t.Fatalf("Unexpected drive state (%d): %v", i+1, h.State)
		}
	}

	// Heal the same object again
	hr, err = obj.HealObject(ctx, bucket, object, "", madmin.HealOpts{ScanMode: madmin.HealNormalScan})
	if err != nil {
		t.Fatalf("Failed to heal object - %v", err)
	}

	// Check that Before & After states are all okay
	for i, h := range append(hr.Before.Drives, hr.After.Drives...) {
		if h.State != madmin.DriveStateOk {
			t.Fatalf("Unexpected drive state (%d): %v", i+1, h.State)
		}
	}
}

func TestHealLastDataShard(t *testing.T) {
	tests := []struct {
		name     string
		dataSize int64
	}{
		{"4KiB", 4 * humanize.KiByte},
		{"64KiB", 64 * humanize.KiByte},
		{"128KiB", 128 * humanize.KiByte},
		{"1MiB", 1 * humanize.MiByte},
		{"5MiB", 5 * humanize.MiByte},
		{"10MiB", 10 * humanize.MiByte},
		{"5MiB-1KiB", 5*humanize.MiByte - 1*humanize.KiByte},
		{"10MiB-1Kib", 10*humanize.MiByte - 1*humanize.KiByte},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			nDisks := 16
			fsDirs, err := getRandomDisks(nDisks)
			if err != nil {
				t.Fatal(err)
			}

			defer removeRoots(fsDirs)

			obj, _, err := initObjectLayer(ctx, mustGetPoolEndpoints(fsDirs...))
			if err != nil {
				t.Fatal(err)
			}
			bucket := "bucket"
			object := "object"

			data := make([]byte, test.dataSize)
			_, err = rand.Read(data)
			if err != nil {
				t.Fatal(err)
			}
			var opts ObjectOptions

			err = obj.MakeBucketWithLocation(ctx, bucket, MakeBucketOptions{})
			if err != nil {
				t.Fatalf("Failed to make a bucket - %v", err)
			}

			_, err = obj.PutObject(ctx, bucket, object,
				mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), opts)

			if err != nil {
				t.Fatal(err)
			}

			actualH := sha256.New()
			_, err = io.Copy(actualH, bytes.NewReader(data))
			if err != nil {
				return
			}
			actualSha256 := actualH.Sum(nil)

			z := obj.(*erasureServerPools)
			er := z.serverPools[0].getHashedSet(object)

			disks := er.getDisks()
			distribution := hashOrder(pathJoin(bucket, object), nDisks)
			shuffledDisks := shuffleDisks(disks, distribution)

			// remove last data shard
			err = removeAll(pathJoin(shuffledDisks[11].String(), bucket, object))
			if err != nil {
				t.Fatalf("Failed to delete a file - %v", err)
			}
			_, err = obj.HealObject(ctx, bucket, object, "", madmin.HealOpts{
				ScanMode: madmin.HealNormalScan,
			})
			if err != nil {
				t.Fatal(err)
			}

			firstGr, err := obj.GetObjectNInfo(ctx, bucket, object, nil, nil, noLock, ObjectOptions{})
			defer firstGr.Close()
			if err != nil {
				t.Fatal(err)
			}

			firstHealedH := sha256.New()
			_, err = io.Copy(firstHealedH, firstGr)
			if err != nil {
				t.Fatal(err)
			}
			firstHealedDataSha256 := firstHealedH.Sum(nil)

			if !bytes.Equal(actualSha256, firstHealedDataSha256) {
				t.Fatalf("object healed wrong, expected %x, got %x",
					actualSha256, firstHealedDataSha256)
			}

			// remove another data shard
			if err = removeAll(pathJoin(shuffledDisks[1].String(), bucket, object)); err != nil {
				t.Fatalf("Failed to delete a file - %v", err)
			}

			_, err = obj.HealObject(ctx, bucket, object, "", madmin.HealOpts{
				ScanMode: madmin.HealNormalScan,
			})
			if err != nil {
				t.Fatal(err)
			}

			secondGr, err := obj.GetObjectNInfo(ctx, bucket, object, nil, nil, noLock, ObjectOptions{})
			defer secondGr.Close()
			if err != nil {
				t.Fatal(err)
			}

			secondHealedH := sha256.New()
			_, err = io.Copy(secondHealedH, secondGr)
			if err != nil {
				t.Fatal(err)
			}
			secondHealedDataSha256 := secondHealedH.Sum(nil)

			if !bytes.Equal(actualSha256, secondHealedDataSha256) {
				t.Fatalf("object healed wrong, expected %x, got %x",
					actualSha256, secondHealedDataSha256)
			}
		})
	}
}
