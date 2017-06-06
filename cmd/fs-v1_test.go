/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/minio/minio/pkg/lock"
)

// TestNewFS - tests initialization of all input disks
// and constructs a valid `FS` object layer.
func TestNewFS(t *testing.T) {
	// Do not attempt to create this path, the test validates
	// so that newFSObjectLayer initializes non existing paths
	// and successfully returns initialized object layer.
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	_, err := newFSObjectLayer("")
	if err != errInvalidArgument {
		t.Errorf("Expecting error invalid argument, got %s", err)
	}
	_, err = newFSObjectLayer(disk)
	if err != nil {
		errMsg := "Unable to recognize backend format, Disk is not in FS format."
		if err.Error() == errMsg {
			t.Errorf("Expecting %s, got %s", errMsg, err)
		}
	}
}

// TestFSShutdown - initialize a new FS object layer then calls
// Shutdown to check returned results
func TestFSShutdown(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(rootPath)

	bucketName := "testbucket"
	objectName := "object"
	// Create and return an fsObject with its path in the disk
	prepareTest := func() (*fsObjects, string) {
		disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		obj := initFSObjects(disk, t)
		fs := obj.(*fsObjects)
		objectContent := "12345"
		obj.MakeBucketWithLocation(bucketName, "")
		sha256sum := ""
		obj.PutObject(bucketName, objectName, int64(len(objectContent)), bytes.NewReader([]byte(objectContent)), nil, sha256sum)
		return fs, disk
	}

	// Test Shutdown with regular conditions
	fs, disk := prepareTest()
	if err := fs.Shutdown(); err != nil {
		t.Fatal("Cannot shutdown the FS object: ", err)
	}
	removeAll(disk)

	// Test Shutdown with faulty disk
	fs, disk = prepareTest()
	fs.DeleteObject(bucketName, objectName)
	removeAll(disk)
	if err := fs.Shutdown(); err != nil {
		t.Fatal("Got unexpected fs shutdown error: ", err)
	}
}

// Tests migrating FS format without .minio.sys/buckets.
func TestFSMigrateObjectWithoutObjects(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	// Assign a new UUID.
	uuid := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolumeFS(disk, uuid); err != nil {
		t.Fatal(err)
	}

	fsFormatPath := pathJoin(disk, minioMetaBucket, fsFormatJSONFile)
	formatCfg := &formatConfigV1{
		Version: "1",
		Format:  "fs",
		FS: &fsFormat{
			Version: "1",
		},
	}

	lk, err := lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = formatCfg.WriteTo(lk)
	lk.Close()
	if err != nil {
		t.Fatal("Should not fail here", err)
	}

	if err = initFormatFS(disk, uuid); err != nil {
		t.Fatal("Should not fail with unexpected", err)
	}

	formatCfg = &formatConfigV1{}
	lk, err = lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = formatCfg.ReadFrom(lk)
	lk.Close()
	if err != nil {
		t.Fatal("Should not fail here", err)
	}
	if formatCfg.FS.Version != fsFormatV2 {
		t.Fatalf("Unexpected version detected expected \"%s\", got %s", fsFormatV2, formatCfg.FS.Version)
	}
}

// Tests migrating FS format without .minio.sys/buckets.
func TestFSMigrateObjectWithErr(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	// Assign a new UUID.
	uuid := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolumeFS(disk, uuid); err != nil {
		t.Fatal(err)
	}

	fsFormatPath := pathJoin(disk, minioMetaBucket, fsFormatJSONFile)
	formatCfg := &formatConfigV1{
		Version: "1",
		Format:  "fs",
		FS: &fsFormat{
			Version: "10",
		},
	}

	lk, err := lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = formatCfg.WriteTo(lk)
	lk.Close()
	if err != nil {
		t.Fatal("Should not fail here", err)
	}

	if err = initFormatFS(disk, uuid); err != nil {
		if !strings.Contains(errorCause(err).Error(), "Unable to validate 'format.json', corrupted backend format") {
			t.Fatal("Should not fail with unexpected", err)
		}
	}

	fsFormatPath = pathJoin(disk, minioMetaBucket, fsFormatJSONFile)
	formatCfg = &formatConfigV1{
		Version: "1",
		Format:  "garbage",
		FS: &fsFormat{
			Version: "1",
		},
	}

	lk, err = lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = formatCfg.WriteTo(lk)
	lk.Close()
	if err != nil {
		t.Fatal("Should not fail here", err)
	}

	if err = initFormatFS(disk, uuid); err != nil {
		if errorCause(err).Error() !=
			"Unable to validate 'format.json', Unable to recognize backend format, Disk is not in FS format. garbage" {
			t.Fatal("Should not fail with unexpected", err)
		}
	}

}

// Tests migrating FS format with .minio.sys/buckets filled with
// objects such as policy.json/fs.json, notification.xml/fs.json
// listener.json/fs.json.
func TestFSMigrateObjectWithBucketConfigObjects(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	// Assign a new UUID.
	uuid := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolumeFS(disk, uuid); err != nil {
		t.Fatal(err)
	}

	fsFormatPath := pathJoin(disk, minioMetaBucket, fsFormatJSONFile)
	formatCfg := &formatConfigV1{
		Version: "1",
		Format:  "fs",
		FS: &fsFormat{
			Version: "1",
		},
	}
	lk, err := lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = formatCfg.WriteTo(lk)
	lk.Close()
	if err != nil {
		t.Fatal("Should not fail here", err)
	}

	// Construct the full path of fs.json
	fsPath1 := pathJoin(bucketMetaPrefix, "testvolume1", bucketPolicyConfig, fsMetaJSONFile)
	fsPath1 = pathJoin(disk, minioMetaBucket, fsPath1)

	fsMetaJSON := `{"version":"1.0.0","format":"fs","minio":{"release":"DEVELOPMENT.2017-03-27T02-26-33Z"},"meta":{"etag":"467886be95c8ecfd71a2900e3f461b4f"}`
	if _, err = fsCreateFile(fsPath1, bytes.NewReader([]byte(fsMetaJSON)), nil, 0); err != nil {
		t.Fatal(err)
	}

	// Construct the full path of fs.json
	fsPath2 := pathJoin(bucketMetaPrefix, "testvolume2", bucketNotificationConfig, fsMetaJSONFile)
	fsPath2 = pathJoin(disk, minioMetaBucket, fsPath2)

	fsMetaJSON = `{"version":"1.0.0","format":"fs","minio":{"release":"DEVELOPMENT.2017-03-27T02-26-33Z"},"meta":{"etag":"467886be95c8ecfd71a2900eff461b4d"}`
	if _, err = fsCreateFile(fsPath2, bytes.NewReader([]byte(fsMetaJSON)), nil, 0); err != nil {
		t.Fatal(err)
	}

	// Construct the full path of fs.json
	fsPath3 := pathJoin(bucketMetaPrefix, "testvolume3", bucketListenerConfig, fsMetaJSONFile)
	fsPath3 = pathJoin(disk, minioMetaBucket, fsPath3)

	fsMetaJSON = `{"version":"1.0.0","format":"fs","minio":{"release":"DEVELOPMENT.2017-03-27T02-26-33Z"},"meta":{"etag":"467886be95c8ecfd71a2900eff461b4d"}`
	if _, err = fsCreateFile(fsPath3, bytes.NewReader([]byte(fsMetaJSON)), nil, 0); err != nil {
		t.Fatal(err)
	}

	if err = initFormatFS(disk, mustGetUUID()); err != nil {
		t.Fatal("Should not fail here", err)
	}

	fsPath1 = pathJoin(bucketMetaPrefix, "testvolume1", objectMetaPrefix, bucketPolicyConfig, fsMetaJSONFile)
	fsPath1 = pathJoin(disk, minioMetaBucket, fsPath1)
	fi, err := fsStatFile(fsPath1)
	if err != nil {
		t.Fatal("Path should exist and accessible after migration", err)
	}
	if fi.IsDir() {
		t.Fatalf("Unexpected path %s should be a file", fsPath1)
	}

	fsPath2 = pathJoin(bucketMetaPrefix, "testvolume2", objectMetaPrefix, bucketNotificationConfig, fsMetaJSONFile)
	fsPath2 = pathJoin(disk, minioMetaBucket, fsPath2)
	fi, err = fsStatFile(fsPath2)
	if err != nil {
		t.Fatal("Path should exist and accessible after migration", err)
	}
	if fi.IsDir() {
		t.Fatalf("Unexpected path %s should be a file", fsPath2)
	}

	fsPath3 = pathJoin(bucketMetaPrefix, "testvolume3", objectMetaPrefix, bucketListenerConfig, fsMetaJSONFile)
	fsPath3 = pathJoin(disk, minioMetaBucket, fsPath3)
	fi, err = fsStatFile(fsPath3)
	if err != nil {
		t.Fatal("Path should exist and accessible after migration", err)
	}
	if fi.IsDir() {
		t.Fatalf("Unexpected path %s should be a file", fsPath3)
	}

	formatCfg = &formatConfigV1{}
	lk, err = lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = formatCfg.ReadFrom(lk)
	lk.Close()
	if err != nil {
		t.Fatal("Should not fail here", err)
	}
	if formatCfg.FS.Version != fsFormatV2 {
		t.Fatalf("Unexpected version detected expected \"%s\", got %s", fsFormatV2, formatCfg.FS.Version)
	}
}

// Tests migrating FS format with .minio.sys/buckets filled with
// object metadata.
func TestFSMigrateObjectWithObjects(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	// Assign a new UUID.
	uuid := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolumeFS(disk, uuid); err != nil {
		t.Fatal(err)
	}

	fsFormatPath := pathJoin(disk, minioMetaBucket, fsFormatJSONFile)
	formatCfg := &formatConfigV1{
		Version: "1",
		Format:  "fs",
		FS: &fsFormat{
			Version: "1",
		},
	}
	lk, err := lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = formatCfg.WriteTo(lk)
	lk.Close()
	if err != nil {
		t.Fatal("Should not fail here", err)
	}

	// Construct the full path of fs.json
	fsPath1 := pathJoin(bucketMetaPrefix, "testvolume1", "my-object1", fsMetaJSONFile)
	fsPath1 = pathJoin(disk, minioMetaBucket, fsPath1)

	fsMetaJSON := `{"version":"1.0.0","format":"fs","minio":{"release":"DEVELOPMENT.2017-03-27T02-26-33Z"},"meta":{"etag":"467886be95c8ecfd71a2900e3f461b4f"}`
	if _, err = fsCreateFile(fsPath1, bytes.NewReader([]byte(fsMetaJSON)), nil, 0); err != nil {
		t.Fatal(err)
	}

	// Construct the full path of fs.json
	fsPath2 := pathJoin(bucketMetaPrefix, "testvolume2", "my-object2", fsMetaJSONFile)
	fsPath2 = pathJoin(disk, minioMetaBucket, fsPath2)

	fsMetaJSON = `{"version":"1.0.0","format":"fs","minio":{"release":"DEVELOPMENT.2017-03-27T02-26-33Z"},"meta":{"etag":"467886be95c8ecfd71a2900eff461b4d"}`
	if _, err = fsCreateFile(fsPath2, bytes.NewReader([]byte(fsMetaJSON)), nil, 0); err != nil {
		t.Fatal(err)
	}

	// Construct the full path of policy.json
	ppath := pathJoin(bucketMetaPrefix, "testvolume2", bucketPolicyConfig)
	ppath = pathJoin(disk, minioMetaBucket, ppath)

	policyJSON := `{"Version":"2012-10-17","Statement":[{"Action":["s3:GetBucketLocation","s3:ListBucket"],"Effect":"Allow","Principal":{"AWS":["*"]},"Resource":["arn:aws:s3:::testbucket"],"Sid":""},{"Action":["s3:GetObject"],"Effect":"Allow","Principal":{"AWS":["*"]},"Resource":["arn:aws:s3:::testbucket/*"],"Sid":""}]}`
	if _, err = fsCreateFile(ppath, bytes.NewReader([]byte(policyJSON)), nil, 0); err != nil {
		t.Fatal(err)
	}

	if err = initFormatFS(disk, mustGetUUID()); err != nil {
		t.Fatal("Should not fail here", err)
	}

	fsPath2 = pathJoin(bucketMetaPrefix, "testvolume2", objectMetaPrefix, "my-object2", fsMetaJSONFile)
	fsPath2 = pathJoin(disk, minioMetaBucket, fsPath2)
	fi, err := fsStatFile(fsPath2)
	if err != nil {
		t.Fatal("Path should exist and accessible after migration", err)
	}
	if fi.IsDir() {
		t.Fatalf("Unexpected path %s should be a file", fsPath2)
	}

	formatCfg = &formatConfigV1{}
	lk, err = lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = formatCfg.ReadFrom(lk)
	lk.Close()
	if err != nil {
		t.Fatal("Should not fail here", err)
	}
	if formatCfg.FS.Version != fsFormatV2 {
		t.Fatalf("Unexpected version detected expected \"%s\", got %s", fsFormatV2, formatCfg.FS.Version)
	}

	ppath = pathJoin(bucketMetaPrefix, "testvolume2", "acl.json")
	ppath = pathJoin(disk, minioMetaBucket, ppath)

	if _, err = fsCreateFile(ppath, bytes.NewReader([]byte("")), nil, 0); err != nil {
		t.Fatal(err)
	}

	if err = initFormatFS(disk, mustGetUUID()); errorCause(err) != errCorruptedFormat {
		t.Fatal("Should not fail here", err)
	}
}

// TestFSCheckFormatFSErr - test loadFormatFS loading older format.
func TestFSCheckFormatFSErr(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	// Assign a new UUID.
	uuid := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolumeFS(disk, uuid); err != nil {
		t.Fatal(err)
	}

	fsFormatPath := pathJoin(disk, minioMetaBucket, fsFormatJSONFile)
	formatCfg := &formatConfigV1{
		Version: "1",
		Format:  "fs",
		FS: &fsFormat{
			Version: "1",
		},
	}

	lk, err := lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}

	_, err = formatCfg.WriteTo(lk)
	lk.Close()
	if err != nil {
		t.Fatal(err)
	}

	formatCfg = &formatConfigV1{}
	lk, err = lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = formatCfg.ReadFrom(lk)
	lk.Close()
	if err != nil {
		t.Fatal(err)
	}

	if err = checkFormatFS(formatCfg, fsFormatVersion); errorCause(err) != errFSFormatOld {
		t.Fatal("Should not fail with unexpected", err)
	}

	formatCfg = &formatConfigV1{
		Version: "1",
		Format:  "fs",
		FS: &fsFormat{
			Version: "10",
		},
	}

	lk, err = lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}

	_, err = formatCfg.WriteTo(lk)
	lk.Close()
	if err != nil {
		t.Fatal(err)
	}

	if err = checkFormatFS(formatCfg, fsFormatVersion); errorCause(err) != errCorruptedFormat {
		t.Fatal("Should not fail with unexpected", err)
	}

	formatCfg = &formatConfigV1{
		Version: "1",
		Format:  "garbage",
		FS: &fsFormat{
			Version: "1",
		},
	}

	lk, err = lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}

	_, err = formatCfg.WriteTo(lk)
	lk.Close()
	if err != nil {
		t.Fatal(err)
	}

	if err = checkFormatFS(formatCfg, fsFormatVersion); err != nil {
		if errorCause(err).Error() != "Unable to recognize backend format, Disk is not in FS format. garbage" {
			t.Fatal("Should not fail with unexpected", err)
		}
	}

	if err = checkFormatFS(nil, fsFormatVersion); errorCause(err) != errUnexpected {
		t.Fatal("Should fail with errUnexpected, but found", err)
	}

	formatCfg = &formatConfigV1{
		Version: "1",
		Format:  "fs",
		FS: &fsFormat{
			Version: "2",
		},
	}

	lk, err = lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}

	_, err = formatCfg.WriteTo(lk)
	lk.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Should not fail.
	if err = checkFormatFS(formatCfg, fsFormatVersion); err != nil {
		t.Fatal(err)
	}
}

// TestFSCheckFormatFS - test loadFormatFS with healty and faulty disks
func TestFSCheckFormatFS(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	// Assign a new UUID.
	uuid := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err := initMetaVolumeFS(disk, uuid); err != nil {
		t.Fatal(err)
	}

	fsFormatPath := pathJoin(disk, minioMetaBucket, fsFormatJSONFile)
	lk, err := lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}

	format := newFSFormatV2()
	_, err = format.WriteTo(lk)
	lk.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Loading corrupted format file
	file, err := os.OpenFile(preparePath(fsFormatPath), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		t.Fatal("Should not fail here", err)
	}
	file.Write([]byte{'b'})
	file.Close()

	lk, err = lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}

	format = &formatConfigV1{}
	_, err = format.ReadFrom(lk)
	lk.Close()
	if err == nil {
		t.Fatal("Should return an error here")
	}

	// Loading format file from disk not found.
	removeAll(disk)
	_, err = lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDONLY, 0600)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal("Should return 'format.json' does not exist, but got", err)
	}
}

// TestFSGetBucketInfo - test GetBucketInfo with healty and faulty disks
func TestFSGetBucketInfo(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*fsObjects)
	bucketName := "bucket"

	obj.MakeBucketWithLocation(bucketName, "")

	// Test with valid parameters
	info, err := fs.GetBucketInfo(bucketName)
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != bucketName {
		t.Fatalf("wrong bucket name, expected: %s, found: %s", bucketName, info.Name)
	}

	// Test with inexistant bucket
	_, err = fs.GetBucketInfo("a")
	if !isSameType(errorCause(err), BucketNameInvalid{}) {
		t.Fatal("BucketNameInvalid error not returned")
	}

	// Check for buckets and should get disk not found.
	removeAll(disk)
	_, err = fs.GetBucketInfo(bucketName)
	if !isSameType(errorCause(err), BucketNotFound{}) {
		t.Fatal("BucketNotFound error not returned")
	}
}

// Tests FS backend put object behavior.
func TestFSPutObject(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	bucketName := "bucket"
	objectName := "1/2/3/4/object"

	if err := obj.MakeBucketWithLocation(bucketName, ""); err != nil {
		t.Fatal(err)
	}

	sha256sum := ""

	// With a regular object.
	_, err := obj.PutObject(bucketName+"non-existent", objectName, int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil, sha256sum)
	if err == nil {
		t.Fatal("Unexpected should fail here, bucket doesn't exist")
	}
	if _, ok := errorCause(err).(BucketNotFound); !ok {
		t.Fatalf("Expected error type BucketNotFound, got %#v", err)
	}

	// With a directory object.
	_, err = obj.PutObject(bucketName+"non-existent", objectName+"/", int64(0), bytes.NewReader([]byte("")), nil, sha256sum)
	if err == nil {
		t.Fatal("Unexpected should fail here, bucket doesn't exist")
	}
	if _, ok := errorCause(err).(BucketNotFound); !ok {
		t.Fatalf("Expected error type BucketNotFound, got %#v", err)
	}

	_, err = obj.PutObject(bucketName, objectName, int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil, sha256sum)
	if err != nil {
		t.Fatal(err)
	}
	_, err = obj.PutObject(bucketName, objectName+"/1", int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil, sha256sum)
	if err == nil {
		t.Fatal("Unexpected should fail here, backend corruption occurred")
	}
	if nerr, ok := errorCause(err).(PrefixAccessDenied); !ok {
		t.Fatalf("Expected PrefixAccessDenied, got %#v", err)
	} else {
		if nerr.Bucket != "bucket" {
			t.Fatalf("Expected 'bucket', got %s", nerr.Bucket)
		}
		if nerr.Object != "1/2/3/4/object/1" {
			t.Fatalf("Expected '1/2/3/4/object/1', got %s", nerr.Object)
		}
	}

	_, err = obj.PutObject(bucketName, objectName+"/1/", 0, bytes.NewReader([]byte("")), nil, sha256sum)
	if err == nil {
		t.Fatal("Unexpected should fail here, backned corruption occurred")
	}
	if nerr, ok := errorCause(err).(PrefixAccessDenied); !ok {
		t.Fatalf("Expected PrefixAccessDenied, got %#v", err)
	} else {
		if nerr.Bucket != "bucket" {
			t.Fatalf("Expected 'bucket', got %s", nerr.Bucket)
		}
		if nerr.Object != "1/2/3/4/object/1/" {
			t.Fatalf("Expected '1/2/3/4/object/1/', got %s", nerr.Object)
		}
	}
}

// TestFSDeleteObject - test fs.DeleteObject() with healthy and corrupted disks
func TestFSDeleteObject(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*fsObjects)
	bucketName := "bucket"
	objectName := "object"

	obj.MakeBucketWithLocation(bucketName, "")
	sha256sum := ""
	obj.PutObject(bucketName, objectName, int64(len("abcd")), bytes.NewReader([]byte("abcd")), nil, sha256sum)

	// Test with invalid bucket name
	if err := fs.DeleteObject("fo", objectName); !isSameType(errorCause(err), BucketNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with bucket does not exist
	if err := fs.DeleteObject("foobucket", "fooobject"); !isSameType(errorCause(err), BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with invalid object name
	if err := fs.DeleteObject(bucketName, "\\"); !isSameType(errorCause(err), ObjectNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with object does not exist.
	if err := fs.DeleteObject(bucketName, "foooobject"); !isSameType(errorCause(err), ObjectNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with valid condition
	if err := fs.DeleteObject(bucketName, objectName); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Delete object should err disk not found.
	removeAll(disk)
	if err := fs.DeleteObject(bucketName, objectName); err != nil {
		if !isSameType(errorCause(err), BucketNotFound{}) {
			t.Fatal("Unexpected error: ", err)
		}
	}

}

// TestFSDeleteBucket - tests for fs DeleteBucket
func TestFSDeleteBucket(t *testing.T) {
	// Prepare for testing
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*fsObjects)
	bucketName := "bucket"

	err := obj.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Test with an invalid bucket name
	if err = fs.DeleteBucket("fo"); !isSameType(errorCause(err), BucketNameInvalid{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with an inexistant bucket
	if err = fs.DeleteBucket("foobucket"); !isSameType(errorCause(err), BucketNotFound{}) {
		t.Fatal("Unexpected error: ", err)
	}
	// Test with a valid case
	if err = fs.DeleteBucket(bucketName); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	obj.MakeBucketWithLocation(bucketName, "")

	// Delete bucker should get error disk not found.
	removeAll(disk)
	if err = fs.DeleteBucket(bucketName); err != nil {
		if !isSameType(errorCause(err), BucketNotFound{}) {
			t.Fatal("Unexpected error: ", err)
		}
	}
}

// TestFSListBuckets - tests for fs ListBuckets
func TestFSListBuckets(t *testing.T) {
	// Prepare for tests
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*fsObjects)

	bucketName := "bucket"
	if err := obj.MakeBucketWithLocation(bucketName, ""); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Create a bucket with invalid name
	if err := mkdirAll(pathJoin(fs.fsPath, "vo^"), 0777); err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	f, err := os.Create(pathJoin(fs.fsPath, "test"))
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	f.Close()

	// Test list buckets to have only one entry.
	buckets, err := fs.ListBuckets()
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}
	if len(buckets) != 1 {
		t.Fatal("ListBuckets not working properly", buckets)
	}

	// Test ListBuckets with disk not found.
	removeAll(disk)

	if _, err := fs.ListBuckets(); err != nil {
		if errorCause(err) != errDiskNotFound {
			t.Fatal("Unexpected error: ", err)
		}
	}

	longPath := fmt.Sprintf("%0256d", 1)
	fs.fsPath = longPath
	if _, err := fs.ListBuckets(); err != nil {
		if errorCause(err) != errFileNameTooLong {
			t.Fatal("Unexpected error: ", err)
		}
	}
}

// TestFSHealObject - tests for fs HealObject
func TestFSHealObject(t *testing.T) {
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	_, _, err := obj.HealObject("bucket", "object")
	if err == nil || !isSameType(errorCause(err), NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}

// TestFSListObjectHeal - tests for fs ListObjectHeals
func TestFSListObjectsHeal(t *testing.T) {
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer removeAll(disk)

	obj := initFSObjects(disk, t)
	_, err := obj.ListObjectsHeal("bucket", "prefix", "marker", "delimiter", 1000)
	if err == nil || !isSameType(errorCause(err), NotImplemented{}) {
		t.Fatalf("Heal Object should return NotImplemented error ")
	}
}
