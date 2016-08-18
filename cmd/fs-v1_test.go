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
	"os"
	"path/filepath"
	"testing"
)

// TestNewFS - tests initialization of all input disks
// and constructs a valid `FS` object layer.
func TestNewFS(t *testing.T) {
	// Do not attempt to create this path, the test validates
	// so that newFSObjects initializes non existing paths
	// and successfully returns initialized object layer.
	disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
	defer removeAll(disk)

	// Setup to test errFSDiskFormat.
	disks := []string{}
	for i := 0; i < 6; i++ {
		xlDisk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
		defer removeAll(xlDisk)
		disks = append(disks, xlDisk)
	}

	// Initializes all disks with XL
	_, err := newXLObjects(disks, nil)
	if err != nil {
		t.Fatalf("Unable to initialize XL object, %s", err)
	}

	testCases := []struct {
		disk        string
		expectedErr error
	}{
		{disk, nil},
		{disks[0], errFSDiskFormat},
	}

	for _, testCase := range testCases {
		if _, err := newFSObjects(testCase.disk); err != testCase.expectedErr {
			t.Fatalf("expected: %s, got: %s", testCase.expectedErr, err)
		}
	}
}
