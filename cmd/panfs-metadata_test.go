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
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Tests ToObjectInfo function.
func TestPANFSV1MetadataObjInfo(t *testing.T) {
	fsMeta := newPANFSMeta()
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

// TestReadPANFSMetadata - readPANFSMetadata testing with a healthy and faulty disk
func TestReadPANFSMetadata(t *testing.T) {
	t.Skip()

	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*PANFSObjects)

	bucketName := "bucket"
	objectName := "object"

	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, MakeBucketOptions{}); err != nil {
		t.Fatal("Unexpected err: ", err)
	}
	if _, err := obj.PutObject(GlobalContext, bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{}); err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	// Construct the full path of fs.json
	fsPath := pathJoin(bucketMetaPrefix, bucketName, objectName, "panfs.json")
	fsPath = pathJoin(fs.fsPath, minioMetaBucket, fsPath)

	rlk, err := fs.rwPool.Open(fsPath)
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}
	defer rlk.Close()

	// Regular fs metadata reading, no errors expected
	fsMeta := panfsMeta{}
	if _, err = fsMeta.ReadFrom(GlobalContext, rlk.LockedFile); err != nil {
		t.Fatal("Unexpected error ", err)
	}
}

// TestWritePANFSMetadata - tests of writePANFSMetadata with healthy disk.
func TestWritePANFSMetadata(t *testing.T) {
	t.Skip()
	disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
	defer os.RemoveAll(disk)

	obj := initFSObjects(disk, t)
	fs := obj.(*PANFSObjects)

	bucketName := "bucket"
	objectName := "object"

	if err := obj.MakeBucketWithLocation(GlobalContext, bucketName, MakeBucketOptions{}); err != nil {
		t.Fatal("Unexpected err: ", err)
	}
	if _, err := obj.PutObject(GlobalContext, bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader([]byte("abcd")), int64(len("abcd")), "", ""), ObjectOptions{}); err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	// Construct the full path of panfs.json
	fsPath := pathJoin(bucketMetaPrefix, bucketName, objectName, "panfs.json")
	fsPath = pathJoin(fs.fsPath, minioMetaBucket, fsPath)

	rlk, err := fs.rwPool.Open(fsPath)
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}
	defer rlk.Close()

	// PANFS metadata reading, no errors expected (healthy disk)
	fsMeta := panfsMeta{}
	_, err = fsMeta.ReadFrom(GlobalContext, rlk.LockedFile)
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}
	if fsMeta.Version != panfsMetaVersion {
		t.Fatalf("Unexpected version %s", fsMeta.Version)
	}
}

func TestPANFSChecksumV1MarshalJSON(t *testing.T) {
	var cs PANFSChecksumInfoV1

	testCases := []struct {
		checksum       PANFSChecksumInfoV1
		expectedResult string
	}{
		{cs, `{"algorithm":"","blocksize":0,"hashes":null}`},
		{PANFSChecksumInfoV1{Algorithm: "highwayhash", Blocksize: 500}, `{"algorithm":"highwayhash","blocksize":500,"hashes":null}`},
		{PANFSChecksumInfoV1{Algorithm: "highwayhash", Blocksize: 10, Hashes: [][]byte{[]byte("hello")}}, `{"algorithm":"highwayhash","blocksize":10,"hashes":["68656c6c6f"]}`},
	}

	for _, testCase := range testCases {
		data, _ := testCase.checksum.MarshalJSON()
		if testCase.expectedResult != string(data) {
			t.Fatalf("expected: %v, got: %v", testCase.expectedResult, string(data))
		}
	}
}

func TestPANFSChecksumV1UnMarshalJSON(t *testing.T) {
	var cs PANFSChecksumInfoV1

	testCases := []struct {
		data           []byte
		expectedResult PANFSChecksumInfoV1
	}{
		{[]byte(`{"algorithm":"","blocksize":0,"hashes":null}`), cs},
		{[]byte(`{"algorithm":"highwayhash","blocksize":500,"hashes":null}`), PANFSChecksumInfoV1{Algorithm: "highwayhash", Blocksize: 500}},
		{[]byte(`{"algorithm":"highwayhash","blocksize":10,"hashes":["68656c6c6f"]}`), PANFSChecksumInfoV1{Algorithm: "highwayhash", Blocksize: 10, Hashes: [][]byte{[]byte("hello")}}},
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
