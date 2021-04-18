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
	"os"
	"testing"
)

// Tests for if parent directory is object
func TestErasureParentDirIsObject(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, fsDisks, err := prepareErasureSets32(ctx)
	if err != nil {
		t.Fatalf("Unable to initialize 'Erasure' object layer.")
	}
	defer obj.Shutdown(context.Background())

	// Remove all disks.
	for _, disk := range fsDisks {
		defer os.RemoveAll(disk)
	}

	bucketName := "testbucket"
	objectName := "object"

	if err = obj.MakeBucketWithLocation(GlobalContext, bucketName, BucketOptions{}); err != nil {
		t.Fatal(err)
	}

	objectContent := "12345"
	_, err = obj.PutObject(GlobalContext, bucketName, objectName,
		mustGetPutObjReader(t, bytes.NewReader([]byte(objectContent)), int64(len(objectContent)), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		expectedErr bool
		objectName  string
	}{
		{
			expectedErr: true,
			objectName:  pathJoin(objectName, "parent-is-object"),
		},
		{
			expectedErr: false,
			objectName:  pathJoin("no-parent", "object"),
		},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			_, err = obj.PutObject(GlobalContext, bucketName, testCase.objectName,
				mustGetPutObjReader(t, bytes.NewReader([]byte(objectContent)), int64(len(objectContent)), "", ""), ObjectOptions{})
			if testCase.expectedErr && err == nil {
				t.Error("Expected error but got nil")
			}
			if !testCase.expectedErr && err != nil {
				t.Errorf("Expected nil but got %v", err)
			}
		})
	}
}
