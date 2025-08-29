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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

var testUUID = uuid.MustParse("f5c58c61-7175-4018-ab5e-a94fe9c2de4e")

func BenchmarkCrcHash(b *testing.B) {
	cases := []struct {
		key int
	}{
		{16},
		{64},
		{128},
		{256},
		{512},
		{1024},
	}
	for _, testCase := range cases {
		key := randString(testCase.key)
		b.Run("", func(b *testing.B) {
			b.SetBytes(1024)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				crcHashMod(key, 16)
			}
		})
	}
}

func BenchmarkSipHash(b *testing.B) {
	cases := []struct {
		key int
	}{
		{16},
		{64},
		{128},
		{256},
		{512},
		{1024},
	}
	for _, testCase := range cases {
		key := randString(testCase.key)
		b.Run("", func(b *testing.B) {
			b.SetBytes(1024)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				sipHashMod(key, 16, testUUID)
			}
		})
	}
}

// TestSipHashMod - test sip hash.
func TestSipHashMod(t *testing.T) {
	testCases := []struct {
		objectName string
		sipHash    int
	}{
		// cases which should pass the test.
		// passing in valid object name.
		{"object", 37},
		{"The Shining Script <v1>.pdf", 38},
		{"Cost Benefit Analysis (2009-2010).pptx", 59},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", 35},
		{"SHØRT", 49},
		{"There are far too many object names, and far too few bucket names!", 8},
		{"a/b/c/", 159},
		{"/a/b/c", 96},
		{string([]byte{0xff, 0xfe, 0xfd}), 147},
	}

	// Tests hashing order to be consistent.
	for i, testCase := range testCases {
		if sipHashElement := hashKey("SIPMOD", testCase.objectName, 200, testUUID); sipHashElement != testCase.sipHash {
			t.Errorf("Test case %d: Expected \"%v\" but failed \"%v\"", i+1, testCase.sipHash, sipHashElement)
		}
	}

	if sipHashElement := hashKey("SIPMOD", "This will fail", -1, testUUID); sipHashElement != -1 {
		t.Errorf("Test: Expected \"-1\" but got \"%v\"", sipHashElement)
	}

	if sipHashElement := hashKey("SIPMOD", "This will fail", 0, testUUID); sipHashElement != -1 {
		t.Errorf("Test: Expected \"-1\" but got \"%v\"", sipHashElement)
	}

	if sipHashElement := hashKey("UNKNOWN", "This will fail", 0, testUUID); sipHashElement != -1 {
		t.Errorf("Test: Expected \"-1\" but got \"%v\"", sipHashElement)
	}
}

// TestCrcHashMod - test crc hash.
func TestCrcHashMod(t *testing.T) {
	testCases := []struct {
		objectName string
		crcHash    int
	}{
		// cases which should pass the test.
		// passing in valid object name.
		{"object", 28},
		{"The Shining Script <v1>.pdf", 142},
		{"Cost Benefit Analysis (2009-2010).pptx", 133},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", 185},
		{"SHØRT", 97},
		{"There are far too many object names, and far too few bucket names!", 101},
		{"a/b/c/", 193},
		{"/a/b/c", 116},
		{string([]byte{0xff, 0xfe, 0xfd}), 61},
	}

	// Tests hashing order to be consistent.
	for i, testCase := range testCases {
		if crcHashElement := hashKey("CRCMOD", testCase.objectName, 200, testUUID); crcHashElement != testCase.crcHash {
			t.Errorf("Test case %d: Expected \"%v\" but failed \"%v\"", i+1, testCase.crcHash, crcHashElement)
		}
	}

	if crcHashElement := hashKey("CRCMOD", "This will fail", -1, testUUID); crcHashElement != -1 {
		t.Errorf("Test: Expected \"-1\" but got \"%v\"", crcHashElement)
	}

	if crcHashElement := hashKey("CRCMOD", "This will fail", 0, testUUID); crcHashElement != -1 {
		t.Errorf("Test: Expected \"-1\" but got \"%v\"", crcHashElement)
	}

	if crcHashElement := hashKey("UNKNOWN", "This will fail", 0, testUUID); crcHashElement != -1 {
		t.Errorf("Test: Expected \"-1\" but got \"%v\"", crcHashElement)
	}
}

// TestNewErasure - tests initialization of all input disks
// and constructs a valid `Erasure` object
func TestNewErasureSets(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	nDisks := 16 // Maximum disks.
	var erasureDisks []string
	for range nDisks {
		// Do not attempt to create this path, the test validates
		// so that newErasureSets initializes non existing paths
		// and successfully returns initialized object layer.
		disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		erasureDisks = append(erasureDisks, disk)
		defer os.RemoveAll(disk)
	}

	endpoints := mustGetNewEndpoints(0, 16, erasureDisks...)
	_, _, err := waitForFormatErasure(true, endpoints, 1, 0, 16, "")
	if err != errInvalidArgument {
		t.Fatalf("Expecting error, got %s", err)
	}

	_, _, err = waitForFormatErasure(true, nil, 1, 1, 16, "")
	if err != errInvalidArgument {
		t.Fatalf("Expecting error, got %s", err)
	}

	// Initializes all erasure disks
	storageDisks, format, err := waitForFormatErasure(true, endpoints, 1, 1, 16, "")
	if err != nil {
		t.Fatalf("Unable to format drives for erasure, %s", err)
	}

	ep := PoolEndpoints{Endpoints: endpoints}

	parity, err := ecDrivesNoConfig(16)
	if err != nil {
		t.Fatalf("Unexpected error during EC drive config: %v", err)
	}
	if _, err := newErasureSets(ctx, ep, storageDisks, format, parity, 0); err != nil {
		t.Fatalf("Unable to initialize erasure")
	}
}

// TestHashedLayer - tests the hashed layer which will be returned
// consistently for a given object name.
func TestHashedLayer(t *testing.T) {
	// Test distribution with 16 sets.
	var objs [16]*erasureObjects
	for i := range objs {
		objs[i] = &erasureObjects{}
	}

	sets := &erasureSets{sets: objs[:], distributionAlgo: "CRCMOD"}

	testCases := []struct {
		objectName  string
		expectedObj *erasureObjects
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
