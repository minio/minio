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

	xhttp "github.com/minio/minio/internal/http"
)

// TestPutObjectConditionalWithReadQuorumFailure tests that conditional
// PutObject operations (with if-match/if-none-match) behave correctly when read quorum
// cannot be reached.
//
// Related to: https://github.com/minio/minio/issues/21603
//
// Should return an error when read quorum cannot
// be reached, as we cannot reliably determine if the precondition is met.
func TestPutObjectConditionalWithReadQuorumFailure(t *testing.T) {
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

		_, err := obj.PutObject(ctx, bucket, object,
			mustGetPutObjReader(t, bytes.NewReader([]byte("new-value")),
				int64(len("new-value")), "", ""), opts)
		if !isErrReadQuorum(err) {
			t.Errorf("Expected read quorum error when if-none-match is used with quorum failure, got: %v", err)
		}
	})

	t.Run("if-match with read quorum failure", func(t *testing.T) {
		// Test Case 2: if-match (update only if ETag matches)
		// With if-match: <etag>, this should only succeed if object exists with matching ETag.
		// Since read quorum fails, we can't determine if object exists or ETag matches.
		opts := ObjectOptions{
			UserDefined: map[string]string{
				xhttp.IfMatch: existingETag,
			},
			CheckPrecondFn: func(oi ObjectInfo) bool {
				// Precondition fails if ETag doesn't match
				return oi.ETag != existingETag
			},
		}

		_, err := obj.PutObject(ctx, bucket, object,
			mustGetPutObjReader(t, bytes.NewReader([]byte("updated-value")),
				int64(len("updated-value")), "", ""), opts)
		if !isErrReadQuorum(err) {
			t.Errorf("Expected read quorum error when if-match is used with quorum failure, got: %v", err)
		}
	})

	t.Run("if-match wrong etag with read quorum failure", func(t *testing.T) {
		// Test Case 3: if-match with wrong ETag
		// Even if the ETag doesn't match, we should still get read quorum error
		// because we can't read the object to check the condition.
		opts := ObjectOptions{
			UserDefined: map[string]string{
				xhttp.IfMatch: "wrong-etag",
			},
			CheckPrecondFn: func(oi ObjectInfo) bool {
				// Precondition fails if ETag doesn't match
				return oi.ETag != "wrong-etag"
			},
		}

		_, err := obj.PutObject(ctx, bucket, object,
			mustGetPutObjReader(t, bytes.NewReader([]byte("should-fail")),
				int64(len("should-fail")), "", ""), opts)
		if !isErrReadQuorum(err) {
			t.Errorf("Expected read quorum error when if-match is used with quorum failure (even with wrong ETag), got: %v", err)
		}
	})
}
