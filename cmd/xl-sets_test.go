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
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestCrcHashMod - test crc hash.
func TestCrcHashMod(t *testing.T) {
	testCases := []struct {
		objectName string
		crcHash    int
	}{
		// cases which should pass the test.
		// passing in valid object name.
		{"object", 12},
		{"The Shining Script <v1>.pdf", 14},
		{"Cost Benefit Analysis (2009-2010).pptx", 13},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", 1},
		{"SHØRT", 9},
		{"There are far too many object names, and far too few bucket names!", 13},
		{"a/b/c/", 1},
		{"/a/b/c", 4},
		{string([]byte{0xff, 0xfe, 0xfd}), 13},
	}

	// Tests hashing order to be consistent.
	for i, testCase := range testCases {
		if crcHashElement := hashKey("CRCMOD", testCase.objectName, 16); crcHashElement != testCase.crcHash {
			t.Errorf("Test case %d: Expected \"%v\" but failed \"%v\"", i+1, testCase.crcHash, crcHashElement)
		}
	}

	if crcHashElement := hashKey("CRCMOD", "This will fail", -1); crcHashElement != -1 {
		t.Errorf("Test: Expected \"-1\" but got \"%v\"", crcHashElement)
	}

	if crcHashElement := hashKey("CRCMOD", "This will fail", 0); crcHashElement != -1 {
		t.Errorf("Test: Expected \"-1\" but got \"%v\"", crcHashElement)
	}

	if crcHashElement := hashKey("UNKNOWN", "This will fail", 0); crcHashElement != -1 {
		t.Errorf("Test: Expected \"-1\" but got \"%v\"", crcHashElement)
	}
}

// TestNewXL - tests initialization of all input disks
// and constructs a valid `XL` object
func TestNewXLSets(t *testing.T) {
	var nDisks = 16 // Maximum disks.
	var erasureDisks []string
	for i := 0; i < nDisks; i++ {
		// Do not attempt to create this path, the test validates
		// so that newXLSets initializes non existing paths
		// and successfully returns initialized object layer.
		disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		erasureDisks = append(erasureDisks, disk)
		defer os.RemoveAll(disk)
	}

	endpoints := mustGetNewEndpointList(erasureDisks...)
	_, err := waitForFormatXL(context.Background(), true, endpoints, 0, 16)
	if err != errInvalidArgument {
		t.Fatalf("Expecting error, got %s", err)
	}

	_, err = waitForFormatXL(context.Background(), true, nil, 1, 16)
	if err != errInvalidArgument {
		t.Fatalf("Expecting error, got %s", err)
	}

	// Initializes all erasure disks
	format, err := waitForFormatXL(context.Background(), true, endpoints, 1, 16)
	if err != nil {
		t.Fatalf("Unable to format disks for erasure, %s", err)
	}

	if _, err := newXLSets(endpoints, format, 1, 16); err != nil {
		t.Fatalf("Unable to initialize erasure")
	}
}

// TestStorageInfoSets - tests storage info for erasure coded sets of disks.
func TestStorageInfoSets(t *testing.T) {
	var nDisks = 16 // Maximum disks.
	var erasureDisks []string
	for i := 0; i < nDisks; i++ {
		// Do not attempt to create this path, the test validates
		// so that newXLSets initializes non existing paths
		// and successfully returns initialized object layer.
		disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		erasureDisks = append(erasureDisks, disk)
		defer os.RemoveAll(disk)
	}

	endpoints := mustGetNewEndpointList(erasureDisks...)
	// Initializes all erasure disks
	format, err := waitForFormatXL(context.Background(), true, endpoints, 1, 16)
	if err != nil {
		t.Fatalf("Unable to format disks for erasure, %s", err)
	}

	objLayer, err := newXLSets(endpoints, format, 1, 16)
	if err != nil {
		t.Fatal(err)
	}

	// Get storage info first attempt.
	disks16Info := objLayer.StorageInfo(context.Background())

	// This test assumes homogeneity between all disks,
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

// TestHashedLayer - tests the hashed layer which will be returned
// consistently for a given object name.
func TestHashedLayer(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)

	var objs []*xlObjects

	for i := 0; i < 16; i++ {
		obj, fsDirs, err := prepareXL16()
		if err != nil {
			t.Fatal("Unable to initialize 'XL' object layer.", err)
		}

		// Remove all dirs.
		for _, dir := range fsDirs {
			defer os.RemoveAll(dir)
		}

		objs = append(objs, obj.(*xlObjects))
	}

	sets := &xlSets{sets: objs, distributionAlgo: "CRCMOD"}

	testCases := []struct {
		objectName  string
		expectedObj *xlObjects
	}{
		// cases which should pass the test.
		// passing in valid object name.
		{"object", objs[12]},
		{"The Shining Script <v1>.pdf", objs[14]},
		{"Cost Benefit Analysis (2009-2010).pptx", objs[13]},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", objs[1]},
		{"SHØRT", objs[9]},
		{"There are far too many object names, and far too few bucket names!", objs[13]},
		{"a/b/c/", objs[1]},
		{"/a/b/c", objs[4]},
		{string([]byte{0xff, 0xfe, 0xfd}), objs[13]},
	}

	// Tests hashing order to be consistent.
	for i, testCase := range testCases {
		gotObj := sets.getHashedSet(testCase.objectName)
		if gotObj != testCase.expectedObj {
			t.Errorf("Test case %d: Expected \"%#v\" but failed \"%#v\"", i+1, testCase.expectedObj, gotObj)
		}
	}
}
