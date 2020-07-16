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
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Tests ToObjectInfo function.
func TestFSV1MetadataObjInfo(t *testing.T) {
	fsMeta := newFSMetaV1()
	objInfo := fsMeta.ToObjectInfo("testbucket", "testobject", nil)
	if objInfo.Size != 0 {
		t.Fatal("Unexpected object info value for Size", objInfo.Size)
	}
	if !objInfo.ModTime.Equal(timeSentinel) {
		t.Fatal("Unexpected object info value for ModTime ", objInfo.ModTime)
	}
	if objInfo.IsDir {
		t.Fatal("Unexpected object info value for IsDir", objInfo.IsDir)
	}
	if !objInfo.Expires.IsZero() {
		t.Fatal("Unexpected object info value for Expires ", objInfo.Expires)
	}
}

// TestReadFSMetadata - readFSMetadata testing with a healthy and faulty disk
func TestReadFSMetadata(t *testing.T) {
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*FSObjects)

	bucketName := "bucket"
	objectName := "object"

	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{}); err != nil {
		t.Fatal("Unexpected err: ", err)
	}
	if _, err := obj.PutObject(GlobalContext, bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{}); err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	// Construct the full path of fs.json
	fsPath := pathJoin(bucketMetaPrefix, bucketName, objectName, "fs.json")
	fsPath = pathJoin(fs.fsPath, minioMetaBucket, fsPath)

	rlk, err := fs.rwPool.Open(fsPath)
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}
	defer rlk.Close()

	// Regular fs metadata reading, no errors expected
	fsMeta := fsMetaV1{}
	if _, err = fsMeta.ReadFrom(GlobalContext, rlk.LockedFile); err != nil {
		t.Fatal("Unexpected error ", err)
	}
}

// TestWriteFSMetadata - tests of writeFSMetadata with healthy disk.
func TestWriteFSMetadata(t *testing.T) {
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*FSObjects)

	bucketName := "bucket"
	objectName := "object"

	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{}); err != nil {
		t.Fatal("Unexpected err: ", err)
	}
	if _, err := obj.PutObject(GlobalContext, bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{}); err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	// Construct the full path of fs.json
	fsPath := pathJoin(bucketMetaPrefix, bucketName, objectName, "fs.json")
	fsPath = pathJoin(fs.fsPath, minioMetaBucket, fsPath)

	rlk, err := fs.rwPool.Open(fsPath)
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}
	defer rlk.Close()

	// FS metadata reading, no errors expected (healthy disk)
	fsMeta := fsMetaV1{}
	_, err = fsMeta.ReadFrom(GlobalContext, rlk.LockedFile)
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}
	if fsMeta.Version != fsMetaVersion {
		t.Fatalf("Unexpected version %s", fsMeta.Version)
	}
}

func TestFSChecksumV1MarshalJSON(t *testing.T) {
	var cs FSChecksumInfoV1

	testCases := []struct {
		checksum       FSChecksumInfoV1
		expectedResult string
	}{
		{cs, `{"algorithm":"","blocksize":0,"hashes":null}`},
		{FSChecksumInfoV1{Algorithm: "highwayhash", Blocksize: 500}, `{"algorithm":"highwayhash","blocksize":500,"hashes":null}`},
		{FSChecksumInfoV1{Algorithm: "highwayhash", Blocksize: 10, Hashes: [][]byte{[]byte("hello")}}, `{"algorithm":"highwayhash","blocksize":10,"hashes":["68656c6c6f"]}`},
	}

	for _, testCase := range testCases {
		data, _ := testCase.checksum.MarshalJSON()
		if testCase.expectedResult != string(data) {
			t.Fatalf("expected: %v, got: %v", testCase.expectedResult, string(data))
		}
	}
}

func TestFSChecksumV1UnMarshalJSON(t *testing.T) {
	var cs FSChecksumInfoV1

	testCases := []struct {
		data           []byte
		expectedResult FSChecksumInfoV1
	}{
		{[]byte(`{"algorithm":"","blocksize":0,"hashes":null}`), cs},
		{[]byte(`{"algorithm":"highwayhash","blocksize":500,"hashes":null}`), FSChecksumInfoV1{Algorithm: "highwayhash", Blocksize: 500}},
		{[]byte(`{"algorithm":"highwayhash","blocksize":10,"hashes":["68656c6c6f"]}`), FSChecksumInfoV1{Algorithm: "highwayhash", Blocksize: 10, Hashes: [][]byte{[]byte("hello")}}},
	}

	for _, testCase := range testCases {
		err := (&cs).UnmarshalJSON(testCase.data)
		if err != nil {
			t.Fatal("Unexpected error during checksum unmarshalling ", err)
		}
		if !reflect.DeepEqual(testCase.expectedResult, cs) {
			t.Fatalf("expected: %v, got: %v", testCase.expectedResult, cs)
		}
	}
}
