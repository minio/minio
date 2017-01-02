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
	"path/filepath"
	"reflect"
	"testing"

	"github.com/minio/minio/pkg/disk"
)

// TestStorageInfo - tests storage info.
func TestStorageInfo(t *testing.T) {
	objLayer, fsDirs, err := prepareXL()
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

	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatalf("Unexpected error %s", err)
	}

	storageDisks, err := initStorageDisks(endpoints, defaultRetryConfig)
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	objLayer, err = newXLObjects(storageDisks)
	if err != nil {
		t.Fatalf("Unable to initialize 'XL' object layer with ignored disks %s. error %s", fsDirs[:4], err)
	}

	// Get storage info first attempt.
	disks16Info = objLayer.StorageInfo()

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

// Sort valid disks info.
func TestSortingValidDisks(t *testing.T) {
	testCases := []struct {
		disksInfo      []disk.Info
		validDisksInfo []disk.Info
	}{
		// One of the disks is offline.
		{
			disksInfo: []disk.Info{
				{Total: 150, Free: 10},
				{Total: 0, Free: 0},
				{Total: 200, Free: 10},
				{Total: 100, Free: 10},
			},
			validDisksInfo: []disk.Info{
				{Total: 100, Free: 10},
				{Total: 150, Free: 10},
				{Total: 200, Free: 10},
			},
		},
		// All disks are online.
		{
			disksInfo: []disk.Info{
				{Total: 150, Free: 10},
				{Total: 200, Free: 10},
				{Total: 100, Free: 10},
				{Total: 115, Free: 10},
			},
			validDisksInfo: []disk.Info{
				{Total: 100, Free: 10},
				{Total: 115, Free: 10},
				{Total: 150, Free: 10},
				{Total: 200, Free: 10},
			},
		},
	}

	for i, testCase := range testCases {
		validDisksInfo := sortValidDisksInfo(testCase.disksInfo)
		if !reflect.DeepEqual(validDisksInfo, testCase.validDisksInfo) {
			t.Errorf("Test %d: Expected %#v, Got %#v", i+1, testCase.validDisksInfo, validDisksInfo)
		}
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
		disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		erasureDisks = append(erasureDisks, disk)
		defer removeAll(disk)
	}

	// No disks input.
	_, err := newXLObjects(nil)
	if err != errInvalidArgument {
		t.Fatalf("Unable to initialize erasure, %s", err)
	}

	endpoints, err := parseStorageEndpoints(erasureDisks)
	if err != nil {
		t.Fatalf("Unable to initialize erasure, %s", err)
	}

	_, err = waitForFormatDisks(true, nil)
	if err != errInvalidArgument {
		t.Fatalf("Expecting error, got %s", err)
	}

	// Initializes all erasure disks
	formattedDisks, err := waitForFormatDisks(true, endpoints)
	if err != nil {
		t.Fatalf("Unable to format disks for erasure, %s", err)
	}
	_, err = newXLObjects(formattedDisks)
	if err != nil {
		t.Fatalf("Unable to initialize erasure, %s", err)
	}
}
