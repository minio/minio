/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

package main

import (
	"io/ioutil"
	"os"
	"testing"
)

const (
	// singleNodeTestStr is the string which is used as notation for Single node ObjectLayer in the unit tests.
	singleNodeTestStr string = "SingleNode"
	// xLTestStr is the string which is used as notation for XL ObjectLayer in the unit tests.
	xLTestStr string = "XL"
)

// getXLObjectLayer - Instantiates XL object layer and returns it.
func getXLObjectLayer() (ObjectLayer, []string, error) {
	var nDisks = 16 // Maximum disks.
	var erasureDisks []string
	for i := 0; i < nDisks; i++ {
		path, err := ioutil.TempDir(os.TempDir(), "minio-")
		if err != nil {
			return nil, nil, err
		}
		erasureDisks = append(erasureDisks, path)
	}

	// Initialize name space lock.
	initNSLock()

	objLayer, err := newXLObjects(erasureDisks)
	if err != nil {
		return nil, nil, err
	}
	return objLayer, erasureDisks, nil
}

// getSingleNodeObjectLayer - Instantiates single node object layer and returns it.
func getSingleNodeObjectLayer() (ObjectLayer, string, error) {
	// Make a temporary directory to use as the obj.
	fsDir, err := ioutil.TempDir("", "minio-")
	if err != nil {
		return nil, "", err
	}

	// Initialize name space lock.
	initNSLock()

	// Create the obj.
	objLayer, err := newFSObjects(fsDir)
	if err != nil {
		return nil, "", err
	}
	return objLayer, fsDir, nil
}

// removeRoots - Cleans up initialized directories during tests.
func removeRoots(roots []string) {
	for _, root := range roots {
		removeAll(root)
	}
}

// removeRandomDisk - removes a count of random disks from a disk slice.
func removeRandomDisk(disks []string, removeCount int) {
	ints := randInts(len(disks))
	for _, i := range ints[:removeCount] {
		removeAll(disks[i-1])
	}
}

// Regular object test type.
type objTestType func(obj ObjectLayer, instanceType string, t *testing.T)

// Special object test type for disk not found situations.
type objTestDiskNotFoundType func(obj ObjectLayer, instanceType string, dirs []string, t *testing.T)

// ExecObjectLayerTest - executes object layer tests.
// Creates single node and XL ObjectLayer instance and runs test for both the layers.
func ExecObjectLayerTest(t *testing.T, objTest objTestType) {
	objLayer, fsDir, err := getSingleNodeObjectLayer()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for single node setup: %s", err.Error())
	}
	// Executing the object layer tests for single node setup.
	objTest(objLayer, singleNodeTestStr, t)

	objLayer, fsDirs, err := getXLObjectLayer()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err.Error())
	}
	// Executing the object layer tests for XL.
	objTest(objLayer, xLTestStr, t)
	defer removeRoots(append(fsDirs, fsDir))
}

// ExecObjectLayerDiskNotFoundTest - executes object layer tests while deleting
// disks in between tests. Creates XL ObjectLayer instance and runs test for XL layer.
func ExecObjectLayerDiskNotFoundTest(t *testing.T, objTest objTestDiskNotFoundType) {
	objLayer, fsDirs, err := getXLObjectLayer()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err.Error())
	}
	// Executing the object layer tests for XL.
	objTest(objLayer, xLTestStr, fsDirs, t)
	defer removeRoots(fsDirs)
}
