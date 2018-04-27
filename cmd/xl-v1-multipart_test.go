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
	"context"
	"os"
	"testing"
	"time"
)

// Tests cleanup multipart uploads for erasure coded backend.
func TestXLCleanupStaleMultipartUploads(t *testing.T) {
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

	obj.MakeBucketWithLocation(context.Background(), bucketName, "")
	uploadID, err := obj.NewMultipartUpload(context.Background(), bucketName, objectName, nil)
	if err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	go xl.cleanupStaleMultipartUploads(context.Background(), 20*time.Millisecond, 0, globalServiceDoneCh)

	// Wait for 40ms such that - we have given enough time for
	// cleanup routine to kick in.
	time.Sleep(40 * time.Millisecond)

	// Close the routine we do not need it anymore.
	globalServiceDoneCh <- struct{}{}

	// Check if upload id was already purged.
	if err = obj.AbortMultipartUpload(context.Background(), bucketName, objectName, uploadID); err != nil {
		if _, ok := err.(InvalidUploadID); !ok {
			t.Fatal("Unexpected err: ", err)
		}
	}
}
