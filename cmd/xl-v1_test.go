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
	"encoding/json"
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

	storageDisks, err := initStorageDisks(mustGetNewEndpointList(fsDirs...))
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
		// so that newXLObjects initializes non existing paths
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

	endpoints := mustGetNewEndpointList(erasureDisks...)
	storageDisks, err := initStorageDisks(endpoints)
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	_, err = waitForFormatXLDisks(true, endpoints, nil)
	if err != errInvalidArgument {
		t.Fatalf("Expecting error, got %s", err)
	}

	_, err = waitForFormatXLDisks(true, nil, storageDisks)
	if err != errInvalidArgument {
		t.Fatalf("Expecting error, got %s", err)
	}

	// Initializes all erasure disks
	formattedDisks, err := waitForFormatXLDisks(true, endpoints, storageDisks)
	if err != nil {
		t.Fatalf("Unable to format disks for erasure, %s", err)
	}
	_, err = newXLObjects(formattedDisks)
	if err != nil {
		t.Fatalf("Unable to initialize erasure, %s", err)
	}
}

// saveXLToDisk - helper function to save an XL format to a specific
// disk. This is intended only to be used in tests.
func saveXLToDisk(disk StorageAPI, format *formatConfigV1) (err error) {
	// Marshal and write to disk.
	formatBytes, err := json.Marshal(format)
	if err != nil {
		return err
	}

	// Purge any existing temporary file, okay to ignore errors here.
	disk.DeleteFile(minioMetaBucket, formatConfigFileTmp)

	// Append file `format.json.tmp`.
	if err = disk.AppendFile(minioMetaBucket, formatConfigFileTmp, formatBytes); err != nil {
		return err
	}

	// Rename file `format.json.tmp` --> `format.json`.
	if err = disk.RenameFile(minioMetaBucket, formatConfigFileTmp, minioMetaBucket, formatConfigFile); err != nil {
		return err
	}

	return nil
}

// TestNewXLFormatErrors - tests that XL's format.json validation
// errors stop the initialization of the object layer.
func TestNewXLFormatErrors(t *testing.T) {
	var nDisks = 16 // Maximum disks.
	var erasureDisks []string
	for i := 0; i < nDisks; i++ {
		// Do not attempt to create this path, the test validates
		// so that newXLObjects initializes non existing paths
		// and successfully returns initialized object layer.
		disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		erasureDisks = append(erasureDisks, disk)
		defer removeAll(disk)
	}

	endpoints := mustGetNewEndpointList(erasureDisks...)
	storageDisks, err := initStorageDisks(endpoints)
	if err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	// Initializes all erasure disks
	formattedDisks, err := waitForFormatXLDisks(true, endpoints, storageDisks)
	if err != nil {
		t.Fatalf("Unable to format disks for erasure, %s", err)
	}

	// Disks are formatted - let's corrupt format.json on the last
	// disk to cause errors.
	disk := formattedDisks[15]
	format, err := loadFormat(disk)
	if err != nil {
		t.Fatal("unexpected error: ", err)
	}

	// corrupt format file version
	format.Version = "2"
	err = saveXLToDisk(disk, format)
	if err != nil {
		t.Fatal("unexpected error: ", err)
	}
	_, err = newXLObjects(formattedDisks)
	if err == nil || err.Error() != "Unable to recognize backend format - error on disk 16: unknown version of backend format `2` found; please upgrade Minio server; if you are running the latest version, you may have a corrupt file - please join https://slack.minio.io for assistance" {
		t.Fatal("Got unexpected error value:", err)
	}

	// corrupt format name
	format.Version = "1"
	format.Format = "garbage"
	err = saveXLToDisk(disk, format)
	if err != nil {
		t.Fatal("unexpected error: ", err)
	}
	_, err = newXLObjects(formattedDisks)
	if err == nil || err.Error() != "Unable to recognize backend format - error on disk 16: unknown backend format `garbage` found" {
		t.Fatal("Got unexpected error value:", err)
	}

	// corrupt xl disk format version
	format.Format = "xl"
	format.XL.Version = "2"
	err = saveXLToDisk(disk, format)
	if err != nil {
		t.Fatal("unexpected error: ", err)
	}
	_, err = newXLObjects(formattedDisks)
	if err == nil || err.Error() != "Unable to recognize backend format - error on disk 16: unknown XL backend format version `2` found; please upgrade Minio server; if you are running the latest version, you may have a corrupt file - please join https://slack.minio.io for assistance" {
		t.Fatal("Got unexpected error value:", err)
	}

	// fix the file and check that the error stops
	format.XL.Version = "1"
	err = saveXLToDisk(disk, format)
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	_, err = newXLObjects(formattedDisks)
	if err != nil {
		t.Fatal("Unable to initialize erasure:", err)
	}
}
