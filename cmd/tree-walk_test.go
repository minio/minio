/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"
)

// Fixed volume name that could be used across tests
const volume = "testvolume"

// Test for filterMatchingPrefix.
func TestFilterMatchingPrefix(t *testing.T) {
	entries := []string{"a", "aab", "ab", "abbbb", "zzz"}
	testCases := []struct {
		prefixEntry string
		result      []string
	}{
		{
			// Empty prefix should match all entries.
			"",
			[]string{"a", "aab", "ab", "abbbb", "zzz"},
		},
		{
			"a",
			[]string{"a", "aab", "ab", "abbbb"},
		},
		{
			"aa",
			[]string{"aab"},
		},
		{
			// Does not match any of the entries.
			"c",
			[]string{},
		},
	}
	for i, testCase := range testCases {
		expected := testCase.result
		got := filterMatchingPrefix(entries, testCase.prefixEntry)
		if !reflect.DeepEqual(expected, got) {
			t.Errorf("Test %d : expected %v, got %v", i+1, expected, got)
		}
	}
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

// Returns function "listDir" of the type listDirFunc.
// disks - used for doing disk.ListDir()
func listDirFactory(ctx context.Context, disk StorageAPI) ListDirFunc {
	return func(volume, dirPath, dirEntry string) (emptyDir bool, entries []string) {
		entries, err := disk.ListDir(volume, dirPath, -1)
		if err != nil {
			return false, nil
		}
		if len(entries) == 0 {
			return true, nil
		}
		sort.Strings(entries)
		return false, filterMatchingPrefix(entries, dirEntry)
	}
}

// Test if tree walker returns entries matching prefix alone are received
// when a non empty prefix is supplied.
func testTreeWalkPrefix(t *testing.T, listDir ListDirFunc) {
	// Start the tree walk go-routine.
	prefix := "d/"
	endWalkCh := make(chan struct{})
	twResultCh := startTreeWalk(context.Background(), volume, prefix, "", true, listDir, endWalkCh)

	// Check if all entries received on the channel match the prefix.
	for res := range twResultCh {
		if !HasPrefix(res.entry, prefix) {
			t.Errorf("Entry %s doesn't match prefix %s", res.entry, prefix)
		}
	}
}

// Test if entries received on tree walk's channel appear after the supplied marker.
func testTreeWalkMarker(t *testing.T, listDir ListDirFunc) {
	// Start the tree walk go-routine.
	prefix := ""
	endWalkCh := make(chan struct{})
	twResultCh := startTreeWalk(context.Background(), volume, prefix, "d/g", true, listDir, endWalkCh)

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
	fsDir, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		t.Fatalf("Unable to create tmp directory: %s", err)
	}
	endpoints := mustGetNewEndpoints(fsDir)
	disk, err := newStorageAPI(endpoints[0])
	if err != nil {
		t.Fatalf("Unable to create StorageAPI: %s", err)
	}

	var files = []string{
		"d/e",
		"d/f",
		"d/g/h",
		"i/j/k",
		"lmn",
	}
	err = createNamespace(disk, volume, files)
	if err != nil {
		t.Fatal(err)
	}

	listDir := listDirFactory(context.Background(), disk)

	// Simple test for prefix based walk.
	testTreeWalkPrefix(t, listDir)
	// Simple test when marker is set.
	testTreeWalkMarker(t, listDir)

	err = os.RemoveAll(fsDir)
	if err != nil {
		t.Fatal(err)
	}
}

// Test if tree walk go-routine exits cleanly if tree walk is aborted because of timeout.
func TestTreeWalkTimeout(t *testing.T) {
	fsDir, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		t.Fatalf("Unable to create tmp directory: %s", err)
	}
	endpoints := mustGetNewEndpoints(fsDir)
	disk, err := newStorageAPI(endpoints[0])
	if err != nil {
		t.Fatalf("Unable to create StorageAPI: %s", err)
	}
	var myfiles []string
	// Create maxObjectsList+1 number of entries.
	for i := 0; i < maxObjectList+1; i++ {
		myfiles = append(myfiles, fmt.Sprintf("file.%d", i))
	}
	err = createNamespace(disk, volume, myfiles)
	if err != nil {
		t.Fatal(err)
	}

	listDir := listDirFactory(context.Background(), disk)

	// TreeWalk pool with 2 seconds timeout for tree-walk go routines.
	pool := NewTreeWalkPool(2 * time.Second)

	endWalkCh := make(chan struct{})
	prefix := ""
	marker := ""
	recursive := true
	resultCh := startTreeWalk(context.Background(), volume, prefix, marker, recursive, listDir, endWalkCh)

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
	err = os.RemoveAll(fsDir)
	if err != nil {
		t.Error(err)
	}
}

// TestRecursiveWalk - tests if treeWalk returns entries correctly with and
// without recursively traversing prefixes.
func TestRecursiveTreeWalk(t *testing.T) {
	// Create a backend directories fsDir1.
	fsDir1, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		t.Fatalf("Unable to create tmp directory: %s", err)
	}

	endpoints := mustGetNewEndpoints(fsDir1)
	disk1, err := newStorageAPI(endpoints[0])
	if err != nil {
		t.Fatalf("Unable to create StorageAPI: %s", err)
	}

	// Create listDir function.
	listDir := listDirFactory(context.Background(), disk1)

	// Create the namespace.
	var files = []string{
		"d/e",
		"d/f",
		"d/g/h",
		"i/j/k",
		"lmn",
	}
	err = createNamespace(disk1, volume, files)
	if err != nil {
		t.Fatal(err)
	}

	endWalkCh := make(chan struct{})
	testCases := []struct {
		prefix    string
		marker    string
		recursive bool
		expected  map[string]struct{}
	}{
		// with no prefix, no marker and no recursive traversal
		{"", "", false, map[string]struct{}{
			"d/":  {},
			"i/":  {},
			"lmn": {},
		}},
		// with no prefix, no marker and recursive traversal
		{"", "", true, map[string]struct{}{
			"d/f":   {},
			"d/g/h": {},
			"d/e":   {},
			"i/j/k": {},
			"lmn":   {},
		}},
		// with no prefix, marker and no recursive traversal
		{"", "d/e", false, map[string]struct{}{
			"d/f":  {},
			"d/g/": {},
			"i/":   {},
			"lmn":  {},
		}},
		// with no prefix, marker and recursive traversal
		{"", "d/e", true, map[string]struct{}{
			"d/f":   {},
			"d/g/h": {},
			"i/j/k": {},
			"lmn":   {},
		}},
		// with prefix, no marker and no recursive traversal
		{"d/", "", false, map[string]struct{}{
			"d/e":  {},
			"d/f":  {},
			"d/g/": {},
		}},
		// with prefix, no marker and no recursive traversal
		{"d/", "", true, map[string]struct{}{
			"d/e":   {},
			"d/f":   {},
			"d/g/h": {},
		}},
		// with prefix, marker and no recursive traversal
		{"d/", "d/e", false, map[string]struct{}{
			"d/f":  {},
			"d/g/": {},
		}},
		// with prefix, marker and recursive traversal
		{"d/", "d/e", true, map[string]struct{}{
			"d/f":   {},
			"d/g/h": {},
		}},
	}
	for i, testCase := range testCases {
		testCase := testCase
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			for entry := range startTreeWalk(context.Background(), volume,
				testCase.prefix, testCase.marker, testCase.recursive,
				listDir, endWalkCh) {
				if _, found := testCase.expected[entry.entry]; !found {
					t.Errorf("Expected %s, but couldn't find", entry.entry)
				}
			}
		})
	}
	err = os.RemoveAll(fsDir1)
	if err != nil {
		t.Error(err)
	}
}

func TestSortedness(t *testing.T) {
	// Create a backend directories fsDir1.
	fsDir1, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		t.Errorf("Unable to create tmp directory: %s", err)
	}

	endpoints := mustGetNewEndpoints(fsDir1)
	disk1, err := newStorageAPI(endpoints[0])
	if err != nil {
		t.Fatalf("Unable to create StorageAPI: %s", err)
	}

	// Create listDir function.
	listDir := listDirFactory(context.Background(), disk1)

	// Create the namespace.
	var files = []string{
		"d/e",
		"d/f",
		"d/g/h",
		"i/j/k",
		"lmn",
	}
	err = createNamespace(disk1, volume, files)
	if err != nil {
		t.Fatal(err)
	}

	endWalkCh := make(chan struct{})
	testCases := []struct {
		prefix    string
		marker    string
		recursive bool
	}{
		// with no prefix, no marker and no recursive traversal
		{"", "", false},
		// with no prefix, no marker and recursive traversal
		{"", "", true},
		// with no prefix, marker and no recursive traversal
		{"", "d/e", false},
		// with no prefix, marker and recursive traversal
		{"", "d/e", true},
		// with prefix, no marker and no recursive traversal
		{"d/", "", false},
		// with prefix, no marker and no recursive traversal
		{"d/", "", true},
		// with prefix, marker and no recursive traversal
		{"d/", "d/e", false},
		// with prefix, marker and recursive traversal
		{"d/", "d/e", true},
	}
	for i, test := range testCases {
		var actualEntries []string
		for entry := range startTreeWalk(context.Background(), volume,
			test.prefix, test.marker, test.recursive,
			listDir, endWalkCh) {
			actualEntries = append(actualEntries, entry.entry)
		}
		if !sort.IsSorted(sort.StringSlice(actualEntries)) {
			t.Error(i+1, "Expected entries to be sort, but it wasn't")
		}
	}

	// Remove directory created for testing
	err = os.RemoveAll(fsDir1)
	if err != nil {
		t.Error(err)
	}
}

func TestTreeWalkIsEnd(t *testing.T) {
	// Create a backend directories fsDir1.
	fsDir1, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		t.Errorf("Unable to create tmp directory: %s", err)
	}

	endpoints := mustGetNewEndpoints(fsDir1)
	disk1, err := newStorageAPI(endpoints[0])
	if err != nil {
		t.Fatalf("Unable to create StorageAPI: %s", err)
	}

	// Create listDir function.
	listDir := listDirFactory(context.Background(), disk1)

	// Create the namespace.
	var files = []string{
		"d/e",
		"d/f",
		"d/g/h",
		"i/j/k",
		"lmn",
	}
	err = createNamespace(disk1, volume, files)
	if err != nil {
		t.Fatal(err)
	}

	endWalkCh := make(chan struct{})
	testCases := []struct {
		prefix        string
		marker        string
		recursive     bool
		expectedEntry string
	}{
		// with no prefix, no marker and no recursive traversal
		{"", "", false, "lmn"},
		// with no prefix, no marker and recursive traversal
		{"", "", true, "lmn"},
		// with no prefix, marker and no recursive traversal
		{"", "d/e", false, "lmn"},
		// with no prefix, marker and recursive traversal
		{"", "d/e", true, "lmn"},
		// with prefix, no marker and no recursive traversal
		{"d/", "", false, "d/g/"},
		// with prefix, no marker and no recursive traversal
		{"d/", "", true, "d/g/h"},
		// with prefix, marker and no recursive traversal
		{"d/", "d/e", false, "d/g/"},
		// with prefix, marker and recursive traversal
		{"d/", "d/e", true, "d/g/h"},
	}
	for i, test := range testCases {
		var entry TreeWalkResult
		for entry = range startTreeWalk(context.Background(), volume, test.prefix,
			test.marker, test.recursive, listDir, endWalkCh) {
		}
		if entry.entry != test.expectedEntry {
			t.Errorf("Test %d: Expected entry %s, but received %s with the EOF marker", i, test.expectedEntry, entry.entry)
		}
		if !entry.end {
			t.Errorf("Test %d: Last entry %s, doesn't have EOF marker set", i, entry.entry)
		}
	}

	// Remove directory created for testing
	err = os.RemoveAll(fsDir1)
	if err != nil {
		t.Error(err)
	}
}
