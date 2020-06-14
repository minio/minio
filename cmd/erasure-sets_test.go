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
		testCase := testCase
		key := randString(testCase.key)
		b.Run("", func(b *testing.B) {
			b.SetBytes(1024)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
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
		testCase := testCase
		key := randString(testCase.key)
		b.Run("", func(b *testing.B) {
			b.SetBytes(1024)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var nDisks = 16 // Maximum disks.
	var erasureDisks []string
	for i := 0; i < nDisks; i++ {
		// Do not attempt to create this path, the test validates
		// so that newErasureSets initializes non existing paths
		// and successfully returns initialized object layer.
		disk := filepath.Join(globalTestTmpDir, "minio-"+nextSuffix())
		erasureDisks = append(erasureDisks, disk)
		defer os.RemoveAll(disk)
	}

	endpoints := mustGetNewEndpoints(erasureDisks...)
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
		t.Fatalf("Unable to format disks for erasure, %s", err)
	}

	if _, err := newErasureSets(ctx, endpoints, storageDisks, format); err != nil {
		t.Fatalf("Unable to initialize erasure")
	}
}

// TestHashedLayer - tests the hashed layer which will be returned
// consistently for a given object name.
func TestHashedLayer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var objs []*erasureObjects
	for i := 0; i < 16; i++ {
		obj, fsDirs, err := prepareErasure16(ctx)
		if err != nil {
			t.Fatal("Unable to initialize 'Erasure' object layer.", err)
		}

		// Remove all dirs.
		for _, dir := range fsDirs {
			defer os.RemoveAll(dir)
		}

		z := obj.(*erasureZones)
		objs = append(objs, z.zones[0].sets[0])
	}

	sets := &erasureSets{sets: objs, distributionAlgo: "CRCMOD"}

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
