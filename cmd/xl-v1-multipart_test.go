/*
 * Minio Cloud Storage, (C) 2014, 2015, 2016, 2017 Minio, Inc.
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
	"os"
	"testing"
	"time"

	"github.com/minio/minio/pkg/errors"
)

// Tests cleanup multipart uploads for erasure coded backend.
func TestXLCleanupMultipartUploadsInRoutine(t *testing.T) {
	// Initialize configuration
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer os.RemoveAll(root)

	// Create an instance of xl backend
	obj, fsDirs, err := prepareXL16()
	if err != nil {
		t.Fatal(err)
	}
	// Defer cleanup of backend directories
	defer removeRoots(fsDirs)

	xl := obj.(*xlObjects)

	// Close the go-routine, we are going to
	// manually start it and test in this test case.
	globalServiceDoneCh <- struct{}{}

	bucketName := "bucket"
	objectName := "object"

	obj.MakeBucketWithLocation(bucketName, "")
	uploadID, err := obj.NewMultipartUpload(bucketName, objectName, nil)
	if err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	go cleanupStaleMultipartUploads(20*time.Millisecond, 0, obj, xl.listMultipartUploadsCleanup, globalServiceDoneCh)

	// Wait for 40ms such that - we have given enough time for
	// cleanup routine to kick in.
	time.Sleep(40 * time.Millisecond)

	// Close the routine we do not need it anymore.
	globalServiceDoneCh <- struct{}{}

	// Check if upload id was already purged.
	if err = obj.AbortMultipartUpload(bucketName, objectName, uploadID); err != nil {
		err = errors.Cause(err)
		if _, ok := err.(InvalidUploadID); !ok {
			t.Fatal("Unexpected err: ", err)
		}
	}
}

// Tests cleanup of stale upload ids.
func TestXLCleanupMultipartUpload(t *testing.T) {
	// Initialize configuration
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer os.RemoveAll(root)

	// Create an instance of xl backend
	obj, fsDirs, err := prepareXL16()
	if err != nil {
		t.Fatal(err)
	}
	// Defer cleanup of backend directories
	defer removeRoots(fsDirs)

	xl := obj.(*xlObjects)

	// Close the go-routine, we are going to
	// manually start it and test in this test case.
	globalServiceDoneCh <- struct{}{}

	bucketName := "bucket"
	objectName := "object"

	obj.MakeBucketWithLocation(bucketName, "")
	uploadID, err := obj.NewMultipartUpload(bucketName, objectName, nil)
	if err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	if err = cleanupStaleMultipartUpload(bucketName, 0, obj, xl.listMultipartUploadsCleanup); err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	// Check if upload id was already purged.
	if err = obj.AbortMultipartUpload(bucketName, objectName, uploadID); err != nil {
		err = errors.Cause(err)
		if _, ok := err.(InvalidUploadID); !ok {
			t.Fatal("Unexpected err: ", err)
		}
	}
}

func TestUpdateUploadJSON(t *testing.T) {
	// Initialize configuration
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer os.RemoveAll(root)

	// Create an instance of xl backend
	obj, fsDirs, err := prepareXL16()
	if err != nil {
		t.Fatal(err)
	}
	// Defer cleanup of backend directories
	defer removeRoots(fsDirs)

	bucket, object := "bucket", "object"
	err = obj.MakeBucketWithLocation(bucket, "")
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		uploadID    string
		initiated   time.Time
		writeQuorum int
		isRemove    bool
		errVal      error
	}{
		{"111abc", UTCNow(), 9, false, nil},
		{"222abc", UTCNow(), 10, false, nil},
		{"111abc", time.Time{}, 11, true, nil},
	}

	xl := obj.(*xlObjects)
	for i, test := range testCases {
		testErrVal := xl.updateUploadJSON(bucket, object, test.uploadID, test.initiated, test.writeQuorum, test.isRemove)
		if testErrVal != test.errVal {
			t.Errorf("Test %d: Expected error value %v, but got %v",
				i+1, test.errVal, testErrVal)
		}
	}

	// make some disks faulty to simulate a failure.
	for i := range xl.storageDisks[:9] {
		xl.storageDisks[i] = newNaughtyDisk(xl.storageDisks[i].(*retryStorage), nil, errFaultyDisk)
	}

	testErrVal := xl.updateUploadJSON(bucket, object, "222abc", UTCNow(), 10, false)
	if testErrVal == nil || testErrVal.Error() != errXLWriteQuorum.Error() {
		t.Errorf("Expected write quorum error, but got: %v", testErrVal)
	}
}
