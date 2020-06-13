/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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
	"context"
	"os"
	"testing"
)

// Tests for if parent directory is object
func TestErasureParentDirIsObject(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, fsDisks, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatalf("Unable to initialize 'Erasure' object layer.")
	}

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
	objInfo, err := obj.PutObject(GlobalContext, bucketName, objectName,
		mustGetPutObjReader(t, bytes.NewReader([]byte(objectContent)), int64(len(objectContent)), "", ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if objInfo.Name != objectName {
		t.Fatalf("Unexpected object name returned got %s, expected %s", objInfo.Name, objectName)
	}

	z := obj.(*erasureZones)
	xl := z.zones[0].sets[0]
	testCases := []struct {
		parentIsObject bool
		objectName     string
	}{
		// parentIsObject is true if object is available.
		{
			parentIsObject: true,
			objectName:     objectName,
		},
		{
			parentIsObject: false,
			objectName:     "",
		},
		{
			parentIsObject: false,
			objectName:     ".",
		},
		// Should not cause infinite loop.
		{
			parentIsObject: false,
			objectName:     SlashSeparator,
		},
		{
			parentIsObject: false,
			objectName:     "\\",
		},
		// Should not cause infinite loop with double forward slash.
		{
			parentIsObject: false,
			objectName:     "//",
		},
	}

	for i, testCase := range testCases {
		gotValue := xl.parentDirIsObject(GlobalContext, bucketName, testCase.objectName)
		if testCase.parentIsObject != gotValue {
			t.Errorf("Test %d: Unexpected value returned got %t, expected %t", i+1, gotValue, testCase.parentIsObject)
		}
	}
}
