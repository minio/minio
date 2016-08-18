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

// Collection of disks verbatim used for tests.
var disks = []string{
	"/mnt/backend1",
	"/mnt/backend2",
	"/mnt/backend3",
	"/mnt/backend4",
	"/mnt/backend5",
	"/mnt/backend6",
	"/mnt/backend7",
	"/mnt/backend8",
	"/mnt/backend9",
	"/mnt/backend10",
	"/mnt/backend11",
	"/mnt/backend12",
	"/mnt/backend13",
	"/mnt/backend14",
	"/mnt/backend15",
	"/mnt/backend16",
}

// Tests all the expected input disks for function checkSufficientDisks.
func TestCheckSufficientDisks(t *testing.T) {
	// List of test cases fo sufficient disk verification.
	testCases := []struct {
		disks       []string
		expectedErr error
	}{
		// Even number of disks '6'.
		{
			disks[0:6],
			nil,
		},
		// Even number of disks '12'.
		{
			disks[0:12],
			nil,
		},
		// Even number of disks '16'.
		{

			disks[0:16],
			nil,
		},
		// Larger than maximum number of disks > 16.
		{
			append(disks[0:16], "/mnt/unsupported"),
			errXLMaxDisks,
		},
		// Lesser than minimum number of disks < 6.
		{
			disks[0:3],
			errXLMinDisks,
		},
		// Odd number of disks, not divisible by '2'.
		{
			append(disks[0:10], disks[11]),
			errXLNumDisks,
		},
	}

	// Validates different variations of input disks.
	for i, testCase := range testCases {
		if checkSufficientDisks(testCase.disks) != testCase.expectedErr {
			t.Errorf("Test %d expected to pass for disks %s", i+1, testCase.disks)
		}
	}
}

// TestStorageInfo - tests storage info.
func TestStorageInfo(t *testing.T) {
	objLayer, fsDirs, err := getXLObjectLayer()
	if err != nil {
		t.Fatalf("Unable to initialize 'XL' object layer.")
	}

	// Remove all dirs.
	for _, dir := range fsDirs {
		defer removeAll(dir)
	}

	// Get storage info first attempt.
	disks16Info := objLayer.StorageInfo()

	// This test assumes homogenity between all disks,
	// i.e if we loose one disk the effective storage
	// usage values is assumed to decrease. If we have
	// heterogenous environment this is not true all the time.
	if disks16Info.Free <= 0 {
		t.Fatalf("Diskinfo total free values should be greater 0")
	}
	if disks16Info.Total <= 0 {
		t.Fatalf("Diskinfo total values should be greater 0")
	}
}

// TestNewXL - tests initialization of all input disks
// and constructs a valid `XL` object
func TestNewXL(t *testing.T) {
	var nDisks = 16 // Maximum disks.
	var erasureDisks []string
	for i := 0; i < nDisks; i++ {
		// Do not attempt to create this path, the test validates
		// so that newFSObjects initializes non existing paths
		// and successfully returns initialized object layer.
		disk := filepath.Join(os.TempDir(), "minio-"+nextSuffix())
		erasureDisks = append(erasureDisks, disk)
		defer removeAll(disk)
	}
	// Initializes all erasure disks
	_, err := newXLObjects(erasureDisks, nil)
	if err != nil {
		t.Fatalf("Unable to initialize erasure, %s", err)
	}
}
