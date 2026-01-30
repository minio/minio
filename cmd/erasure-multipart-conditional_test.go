// Copyright (c) 2015-2025 MinIO, Inc.
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
	"testing"

	"github.com/dustin/go-humanize"
	xhttp "github.com/minio/minio/internal/http"
)

// TestNewMultipartUploadConditionalWithReadQuorumFailure tests that conditional
// multipart uploads (with if-match/if-none-match) behave correctly when read quorum
// cannot be reached.
//
// Related to: https://github.com/minio/minio/issues/21603
//
// Should return an error when read quorum cannot
// be reached, as we cannot reliably determine if the precondition is met.
func TestNewMultipartUploadConditionalWithReadQuorumFailure(t *testing.T) {
	ctx := context.Background()

	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]

	bucket := "test-bucket"
	object := "test-object"

	err = obj.MakeBucket(ctx, bucket, MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Put an initial object so it exists
	_, err = obj.PutObject(ctx, bucket, object,
		mustGetPutObjReader(t, bytes.NewReader([]byte("initial-value")),
			int64(len("initial-value")), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Get object info to capture the ETag
	objInfo, err := obj.GetObjectInfo(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	existingETag := objInfo.ETag

	// Simulate read quorum failure by taking enough disks offline
	// With 16 disks (EC 8+8), read quorum is 9. Taking 8 disks offline leaves only 8,
	// which is below read quorum.
	erasureDisks := xl.getDisks()
	z.serverPools[0].erasureDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		for i := range erasureDisks[:8] {
			erasureDisks[i] = nil
		}
		return erasureDisks
	}
	z.serverPools[0].erasureDisksMu.Unlock()

	t.Run("if-none-match with read quorum failure", func(t *testing.T) {
		// Test Case 1: if-none-match (create only if doesn't exist)
		// With if-none-match: *, this should only succeed if object doesn't exist.
		// Since read quorum fails, we can't determine if object exists.
		opts := ObjectOptions{
			UserDefined: map[string]string{
				xhttp.IfNoneMatch: "*",
			},
			CheckPrecondFn: func(oi ObjectInfo) bool {
				// Precondition fails if object exists (ETag is not empty)
				return oi.ETag != ""
			},
		}

		_, err := obj.NewMultipartUpload(ctx, bucket, object, opts)
		if !isErrReadQuorum(err) {
			t.Errorf("Expected read quorum error when if-none-match is used with quorum failure, got: %v", err)
		}
	})

	t.Run("if-match with wrong ETag and read quorum failure", func(t *testing.T) {
		// Test Case 2: if-match with WRONG ETag
		// This should fail even without quorum issues, but with quorum failure
		// we can't verify the ETag at all.
		opts := ObjectOptions{
			UserDefined: map[string]string{
				xhttp.IfMatch: "wrong-etag-12345",
			},
			HasIfMatch: true,
			CheckPrecondFn: func(oi ObjectInfo) bool {
				// Precondition fails if ETags don't match
				return oi.ETag != "wrong-etag-12345"
			},
		}

		_, err := obj.NewMultipartUpload(ctx, bucket, object, opts)
		if !isErrReadQuorum(err) {
			t.Logf("Got error (as expected): %v", err)
			t.Logf("But expected read quorum error, not object-not-found error")
		}
	})

	t.Run("if-match with correct ETag and read quorum failure", func(t *testing.T) {
		// Test Case 3: if-match with CORRECT ETag but read quorum failure
		// Even with the correct ETag, we shouldn't proceed if we can't verify it.
		opts := ObjectOptions{
			UserDefined: map[string]string{
				xhttp.IfMatch: existingETag,
			},
			HasIfMatch: true,
			CheckPrecondFn: func(oi ObjectInfo) bool {
				// Precondition fails if ETags don't match
				return oi.ETag != existingETag
			},
		}

		_, err := obj.NewMultipartUpload(ctx, bucket, object, opts)
		if !isErrReadQuorum(err) {
			t.Errorf("Expected read quorum error when if-match is used with quorum failure, got: %v", err)
		}
	})
}

// TestCompleteMultipartUploadConditionalWithReadQuorumFailure tests that conditional
// complete multipart upload operations behave correctly when read quorum cannot be reached.
func TestCompleteMultipartUploadConditionalWithReadQuorumFailure(t *testing.T) {
	ctx := context.Background()

	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Shutdown(context.Background())
	defer removeRoots(fsDirs)

	z := obj.(*erasureServerPools)
	xl := z.serverPools[0].sets[0]

	bucket := "test-bucket"
	object := "test-object"

	err = obj.MakeBucket(ctx, bucket, MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Put an initial object
	_, err = obj.PutObject(ctx, bucket, object,
		mustGetPutObjReader(t, bytes.NewReader([]byte("initial-value")),
			int64(len("initial-value")), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Start a multipart upload WITHOUT conditional checks (this should work)
	res, err := obj.NewMultipartUpload(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Upload a part
	partData := bytes.Repeat([]byte("a"), 5*humanize.MiByte)
	md5Hex := getMD5Hash(partData)
	_, err = obj.PutObjectPart(ctx, bucket, object, res.UploadID, 1,
		mustGetPutObjReader(t, bytes.NewReader(partData), int64(len(partData)), md5Hex, ""),
		ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Now simulate read quorum failure
	erasureDisks := xl.getDisks()
	z.serverPools[0].erasureDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		for i := range erasureDisks[:8] {
			erasureDisks[i] = nil
		}
		return erasureDisks
	}
	z.serverPools[0].erasureDisksMu.Unlock()

	t.Run("complete multipart with if-none-match and read quorum failure", func(t *testing.T) {
		// Try to complete the multipart upload with if-none-match
		// This should fail because we can't verify the condition due to read quorum failure
		opts := ObjectOptions{
			UserDefined: map[string]string{
				xhttp.IfNoneMatch: "*",
			},
			CheckPrecondFn: func(oi ObjectInfo) bool {
				return oi.ETag != ""
			},
		}

		parts := []CompletePart{{PartNumber: 1, ETag: md5Hex}}
		_, err := obj.CompleteMultipartUpload(ctx, bucket, object, res.UploadID, parts, opts)
		if !isErrReadQuorum(err) {
			t.Errorf("Expected read quorum error, got: %v", err)
		}
	})
}
