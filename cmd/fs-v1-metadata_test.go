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

package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Tests scenarios which can occur for hasExtendedHeader function.
func TestHasExtendedHeader(t *testing.T) {
	// All test cases concerning hasExtendedHeader function.
	testCases := []struct {
		metadata map[string]string
		has      bool
	}{
		// Verifies if X-Amz-Meta is present.
		{
			metadata: map[string]string{
				"X-Amz-Meta-1": "value",
			},
			has: true || os.Getenv("MINIO_ENABLE_FSMETA") == "1",
		},
		// Verifies if X-Minio-Meta is present.
		{
			metadata: map[string]string{
				"X-Minio-Meta-1": "value",
			},
			has: true || os.Getenv("MINIO_ENABLE_FSMETA") == "1",
		},
		// Verifies if extended header is not present.
		{
			metadata: map[string]string{
				"md5Sum": "value",
			},
			has: false || os.Getenv("MINIO_ENABLE_FSMETA") == "1",
		},
		// Verifies if extended header is not present, but with an empty input.
		{
			metadata: nil,
			has:      false || os.Getenv("MINIO_ENABLE_FSMETA") == "1",
		},
	}

	// Validate all test cases.
	for i, testCase := range testCases {
		has := hasExtendedHeader(testCase.metadata)
		if has != testCase.has {
			t.Fatalf("Test case %d: Expected \"%#v\", but got \"%#v\"", i+1, testCase.has, has)
		}
	}
}

// TestReadFsMetadata - readFSMetadata testing with a healthy and faulty disk
func TestReadFSMetadata(t *testing.T) {
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)
	obj, err := newFSObjects(disk)
	if err != nil {
		t.Fatal("Unexpected err: ", err)
	}
	fs := obj.(fsObjects)

	bucketName := "bucket"
	objectName := "object"

	if err = obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Unexpected err: ", err)
	}
	if _, err = obj.PutObject(bucketName, objectName, int64(len("abcd")), bytes.NewReader([]byte("abcd")),
		map[string]string{"X-Amz-Meta-AppId": "a"}); err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	// Construct the full path of fs.json
	fsPath := "buckets/" + bucketName + "/" + objectName + "/fs.json"

	// Regular fs metadata reading, no errors expected
	if _, err = readFSMetadata(fs.storage, ".minio.sys", fsPath); err != nil {
		t.Fatal("Unexpected error ", err)
	}

	// Corrupted fs.json
	if err = fs.storage.AppendFile(".minio.sys", fsPath, []byte{'a'}); err != nil {
		t.Fatal("Unexpected error ", err)
	}
	if _, err = readFSMetadata(fs.storage, ".minio.sys", fsPath); err == nil {
		t.Fatal("Should fail", err)
	}

	// Test with corrupted disk
	fsStorage := fs.storage.(*posix)
	naughty := newNaughtyDisk(fsStorage, nil, errFaultyDisk)
	fs.storage = naughty
	if _, err = readFSMetadata(fs.storage, ".minio.sys", fsPath); errorCause(err) != errFaultyDisk {
		t.Fatal("Should fail", err)
	}

}

// TestWriteFsMetadata - tests of writeFSMetadata with healthy and faulty disks
func TestWriteFSMetadata(t *testing.T) {
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)
	obj, err := newFSObjects(disk)
	if err != nil {
		t.Fatal("Unexpected err: ", err)
	}
	fs := obj.(fsObjects)

	bucketName := "bucket"
	objectName := "object"

	if err = obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Unexpected err: ", err)
	}
	if _, err = obj.PutObject(bucketName, objectName, int64(len("abcd")), bytes.NewReader([]byte("abcd")),
		map[string]string{"X-Amz-Meta-AppId": "a"}); err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	// Construct the complete path of fs.json
	fsPath := "buckets/" + bucketName + "/" + objectName + "/fs.json"

	// Fs metadata reading, no errors expected (healthy disk)
	fsMeta, err := readFSMetadata(fs.storage, ".minio.sys", fsPath)
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	// Reading metadata with a corrupted disk
	fsStorage := fs.storage.(*posix)
	for i := 1; i <= 2; i++ {
		naughty := newNaughtyDisk(fsStorage, map[int]error{i: errFaultyDisk, i + 1: errFaultyDisk}, nil)
		fs.storage = naughty
		if err = writeFSMetadata(fs.storage, ".minio.sys", fsPath, fsMeta); errorCause(err) != errFaultyDisk {
			t.Fatal("Unexpected error", i, err)

		}
	}

}
