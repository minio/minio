/*
 * MinIO Cloud Storage, (C) 2014, 2015, 2016, 2017 MinIO, Inc.
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
	"sync"
	"testing"
	"time"
)

// Tests cleanup multipart uploads for erasure coded backend.
func TestXLCleanupStaleMultipartUploads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an instance of xl backend
	obj, fsDirs, err := prepareXL16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Defer cleanup of backend directories
	defer removeRoots(fsDirs)

	z := obj.(*xlZones)
	xl := z.zones[0].sets[0]

	bucketName := "bucket"
	objectName := "object"
	var opts ObjectOptions

	obj.MakeBucketWithLocation(ctx, bucketName, "", false)
	uploadID, err := obj.NewMultipartUpload(GlobalContext, bucketName, objectName, opts)
	if err != nil {
		t.Fatal("Unexpected err: ", err)
	}

	var cleanupWg sync.WaitGroup
	cleanupWg.Add(1)
	go func() {
		defer cleanupWg.Done()
		xl.cleanupStaleMultipartUploads(GlobalContext, time.Millisecond, 0, ctx.Done())
	}()

	// Wait for 100ms such that - we have given enough time for cleanup routine to kick in.
	// Flaky on slow systems :/
	time.Sleep(100 * time.Millisecond)

	// Exit cleanup..
	cancel()
	cleanupWg.Wait()

	// Check if upload id was already purged.
	if err = obj.AbortMultipartUpload(context.Background(), bucketName, objectName, uploadID); err != nil {
		if _, ok := err.(InvalidUploadID); !ok {
			t.Fatal("Unexpected err: ", err)
		}
	} else {
		t.Error("Item was not cleaned up.")
	}
}
