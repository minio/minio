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

package main

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"
)

// Sample entries for the namespace.
var volume = "testvolume"
var files = []string{
	"d/e",
	"d/f",
	"d/g/h",
	"i/j/k",
	"lmn",
}

// Helper function that creates a volume and files in it.
func createNamespace(disk StorageAPI, volume string, files []string) error {
	// Make a volume.
	err := disk.MakeVol(volume)
	if err != nil {
		return err
	}

	// Create files.
	for _, file := range files {
		err = disk.AppendFile(volume, file, []byte{})
		if err != nil {
			return err
		}
	}
	return err
}

// Test if tree walker returns entries matching prefix alone are received
// when a non empty prefix is supplied.
func testTreeWalkPrefix(t *testing.T, listDir listDirFunc) {
	// Start the tree walk go-routine.
	prefix := "d/"
	endWalkCh := make(chan struct{})
	twResultCh := startTreeWalk(volume, prefix, "", true, listDir, endWalkCh)

	// Check if all entries received on the channel match the prefix.
	for res := range twResultCh {
		if !strings.HasPrefix(res.entry, prefix) {
			t.Errorf("Entry %s doesn't match prefix %s", res.entry, prefix)
		}
	}
}

// Test if entries received on tree walk's channel appear after the supplied marker.
func testTreeWalkMarker(t *testing.T, listDir listDirFunc) {
	// Start the tree walk go-routine.
	prefix := ""
	endWalkCh := make(chan struct{})
	twResultCh := startTreeWalk(volume, prefix, "d/g", true, listDir, endWalkCh)

	// Check if only 3 entries, namely d/g/h, i/j/k, lmn are received on the channel.
	expectedCount := 3
	actualCount := 0
	for range twResultCh {
		actualCount++
	}
	if expectedCount != actualCount {
		t.Errorf("Expected %d entries, actual no. of entries were %d", expectedCount, actualCount)
	}
}

// Test tree-walk.
func TestTreeWalk(t *testing.T) {
	fsDir, err := ioutil.TempDir("", "minio-")
	if err != nil {
		t.Errorf("Unable to create tmp directory: %s", err)
	}
	disk, err := newStorageAPI(fsDir)
	if err != nil {
		t.Errorf("Unable to create StorageAPI: %s", err)
	}

	err = createNamespace(disk, volume, files)
	if err != nil {
		t.Fatal(err)
	}

	listDir := listDirFactory(func(volume, prefix string) bool {
		return !strings.HasSuffix(prefix, slashSeparator)
	}, disk)
	// Simple test for prefix based walk.
	testTreeWalkPrefix(t, listDir)
	// Simple test when marker is set.
	testTreeWalkMarker(t, listDir)
	err = removeAll(fsDir)
	if err != nil {
		t.Fatal(err)
	}
}

// Test if tree walk go-routine exits cleanly if tree walk is aborted because of timeout.
func TestTreeWalkTimeout(t *testing.T) {
	fsDir, err := ioutil.TempDir("", "minio-")
	if err != nil {
		t.Errorf("Unable to create tmp directory: %s", err)
	}
	disk, err := newStorageAPI(fsDir)
	if err != nil {
		t.Errorf("Unable to create StorageAPI: %s", err)
	}
	var files []string
	// Create maxObjectsList+1 number of entries.
	for i := 0; i < maxObjectList+1; i++ {
		files = append(files, fmt.Sprintf("file.%d", i))
	}
	err = createNamespace(disk, volume, files)
	if err != nil {
		t.Fatal(err)
	}

	listDir := listDirFactory(func(volume, prefix string) bool {
		return !strings.HasSuffix(prefix, slashSeparator)
	}, disk)

	// TreeWalk pool with 2 seconds timeout for tree-walk go routines.
	pool := newTreeWalkPool(2 * time.Second)

	endWalkCh := make(chan struct{})
	prefix := ""
	marker := ""
	recursive := true
	resultCh := startTreeWalk(volume, prefix, marker, recursive, listDir, endWalkCh)

	params := listParams{
		bucket:    volume,
		recursive: recursive,
	}
	// Add Treewalk to the pool.
	pool.Set(params, resultCh, endWalkCh)

	// Wait for the Treewalk to timeout.
	<-time.After(3 * time.Second)

	// Read maxObjectList number of entries from the channel.
	// maxObjectsList number of entries would have been filled into the resultCh
	// buffered channel. After the timeout resultCh would get closed and hence the
	// maxObjectsList+1 entry would not be sent in the channel.
	i := 0
	for range resultCh {
		i++
		if i == maxObjectList {
			break
		}
	}

	// The last entry will not be received as the Treewalk goroutine would have exited.
	_, ok := <-resultCh
	if ok {
		t.Error("Tree-walk go routine has not exited after timeout.")
	}
	err = removeAll(fsDir)
	if err != nil {
		t.Error(err)
	}
}

// Test ListDir - listDir should list entries from the first disk, if the first disk is down,
// it should list from the next disk.
func TestListDir(t *testing.T) {
	file1 := "file1"
	file2 := "file2"
	// Create two backend directories fsDir1 and fsDir2.
	fsDir1, err := ioutil.TempDir("", "minio-")
	if err != nil {
		t.Errorf("Unable to create tmp directory: %s", err)
	}
	fsDir2, err := ioutil.TempDir("", "minio-")
	if err != nil {
		t.Errorf("Unable to create tmp directory: %s", err)
	}

	// Create two StorageAPIs disk1 and disk2.
	disk1, err := newStorageAPI(fsDir1)
	if err != nil {
		t.Errorf("Unable to create StorageAPI: %s", err)
	}
	disk2, err := newStorageAPI(fsDir2)
	if err != nil {
		t.Errorf("Unable to create StorageAPI: %s", err)
	}

	// create listDir function.
	listDir := listDirFactory(func(volume, prefix string) bool {
		return !strings.HasSuffix(prefix, slashSeparator)
	}, disk1, disk2)

	// Create file1 in fsDir1 and file2 in fsDir2.
	disks := []StorageAPI{disk1, disk2}
	for i, disk := range disks {
		err = createNamespace(disk, volume, []string{fmt.Sprintf("file%d", i+1)})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Should list "file1" from fsDir1.
	entries, err := listDir(volume, "", "")
	if err != nil {
		t.Error(err)
	}
	if len(entries) != 1 {
		t.Fatal("Expected the number of entries to be 1")
	}
	if entries[0] != file1 {
		t.Fatal("Expected the entry to be file1")
	}

	// Remove fsDir1 to test failover.
	err = removeAll(fsDir1)
	if err != nil {
		t.Error(err)
	}

	// Should list "file2" from fsDir2.
	entries, err = listDir(volume, "", "")
	if err != nil {
		t.Error(err)
	}
	if len(entries) != 1 {
		t.Fatal("Expected the number of entries to be 1")
	}
	if entries[0] != file2 {
		t.Fatal("Expected the entry to be file2")
	}
	err = removeAll(fsDir2)
	if err != nil {
		t.Error(err)
	}
	// None of the disks are available, should get errDiskNotFound.
	entries, err = listDir(volume, "", "")
	if err != errDiskNotFound {
		t.Error("expected errDiskNotFound error.")
	}
}
