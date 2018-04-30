/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
func TestXLParentDirIsObject(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)

	obj, fsDisks, err := prepareXL16()
	if err != nil {
		t.Fatalf("Unable to initialize 'XL' object layer.")
	}

	// Remove all disks.
	for _, disk := range fsDisks {
		defer os.RemoveAll(disk)
	}

	bucketName := "testbucket"
	objectName := "object"

	if err = obj.MakeBucketWithLocation(context.Background(), bucketName, ""); err != nil {
		t.Fatal(err)
	}
	objectContent := "12345"
	objInfo, err := obj.PutObject(context.Background(), bucketName, objectName,
		mustGetHashReader(t, bytes.NewReader([]byte(objectContent)), int64(len(objectContent)), "", ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	if objInfo.Name != objectName {
		t.Fatalf("Unexpected object name returned got %s, expected %s", objInfo.Name, objectName)
	}

	fs := obj.(*xlObjects)
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
			objectName:     "/",
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
		gotValue := fs.parentDirIsObject(context.Background(), bucketName, testCase.objectName)
		if testCase.parentIsObject != gotValue {
			t.Errorf("Test %d: Unexpected value returned got %t, expected %t", i+1, gotValue, testCase.parentIsObject)
		}
	}
}
