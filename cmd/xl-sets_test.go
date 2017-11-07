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
	"os"
	"testing"
)

// TestStorageInfoSets - tests storage info for erasure coded sets of disks.
func TestStorageInfoSets(t *testing.T) {
	var objs []*xlObjects

	for i := 0; i < 4; i++ {
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

	objLayer := newXLSets(objs)

	// Get storage info first attempt.
	disks16Info := objLayer.StorageInfo()

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

	sets := newXLSets(objs).(*xlSets)

	testCases := []struct {
		objectName  string
		expectedObj *xlObjects
	}{
		// cases which should pass the test.
		// passing in valid object name.
		{"object", objs[13]},
		{"The Shining Script <v1>.pdf", objs[15]},
		{"Cost Benefit Analysis (2009-2010).pptx", objs[14]},
		{"117Gn8rfHL2ACARPAhaFd0AGzic9pUbIA/5OCn5A", objs[2]},
		{"SHØRT", objs[10]},
		{"There are far too many object names, and far too few bucket names!", objs[14]},
		{"a/b/c/", objs[2]},
		{"/a/b/c", objs[5]},
		{string([]byte{0xff, 0xfe, 0xfd}), objs[14]},
	}

	// Tests hashing order to be consistent.
	for i, testCase := range testCases {
		gotObj := sets.getHashedSet(testCase.objectName)
		if gotObj != testCase.expectedObj {
			t.Errorf("Test case %d: Expected \"%#v\" but failed \"%#v\"", i+1, testCase.expectedObj, gotObj)
		}
	}
}
