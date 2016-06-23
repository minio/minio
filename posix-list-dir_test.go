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
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// Test to check for different input arguments.
func TestReadDir0(t *testing.T) {
	// Check non existent directory.
	if _, err := readDir("/tmp/non-existent-directory"); err != errFileNotFound {
		t.Fatalf("expected = %s, got: %s", errFileNotFound, err)
	}

	// Check if file is given.
	if _, err := readDir("/etc/issue/mydir"); err != errFileNotFound {
		t.Fatalf("expected = %s, got: %s", errFileNotFound, err)
	}

	// Check if permission denied.
	if _, err := readDir("/proc/1/fd"); err == nil {
		t.Fatalf("expected = an error, got: nil")
	}
}

type result struct {
	dir     string
	entries []string
}

var testResults = []result{}

// Test to read empty directory.
func setupTestReadDir1(t *testing.T) {
	// Create unique test directory.
	if dir, err := ioutil.TempDir("", "minio-posix-list-dir"); err != nil {
		t.Fatal("failed to setup.", err)
	} else {
		// Add empty entry slice for this test directory.
		testResults = append(testResults, result{dir, []string{}})
	}
}

// Test to read empty directory with reserved names.
func setupTestReadDir2(t *testing.T) {
	// Create unique test directory.
	if dir, err := ioutil.TempDir("", "minio-posix-list-dir"); err != nil {
		t.Fatal("failed to setup.", err)
	} else {
		entries := []string{}
		// Create a file with reserved name.
		for _, reservedName := range posixReservedPrefix {
			if err := ioutil.WriteFile(filepath.Join(dir, reservedName), []byte{}, os.ModePerm); err != nil {
				// For cleanup, its required to add these entries into test results.
				sort.Strings(entries)
				testResults = append(testResults, result{dir, entries})
				t.Fatal("failed to setup.", err)
			}
		}

		sort.Strings(entries)

		// Add entries slice for this test directory.
		testResults = append(testResults, result{dir, entries})
	}
}

// Test to read non-empty directory.
func setupTestReadDir3(t *testing.T) {
	if dir, err := ioutil.TempDir("", "minio-posix-list-dir"); err != nil {
		t.Fatal("failed to setup.", err)
	} else {
		entries := []string{}
		for i := 0; i < 10; i++ {
			name := fmt.Sprintf("file-%d", i)
			entries = append(entries, name)
			if err := ioutil.WriteFile(filepath.Join(dir, name), []byte{}, os.ModePerm); err != nil {
				// Keep entries sorted for easier comparison.
				sort.Strings(entries)
				// For cleanup, its required to add these entries into test results.
				testResults = append(testResults, result{dir, entries})
				t.Fatal("failed to setup.", err)
			}
		}

		// Keep entries sorted for easier comparison.
		sort.Strings(entries)

		// Add entries slice for this test directory.
		testResults = append(testResults, result{dir, entries})
	}
}

// teardown - cleans up test directories.
func teardown() {
	for _, r := range testResults {
		os.RemoveAll(r.dir)
	}
}

// checkResult - checks whether entries are got are same as expected entries.
func checkResult(expected []string, got []string) bool {
	// If length of expected and got slice are different, the test actually failed.
	if len(expected) != len(got) {
		return false
	}

	for i := range expected {
		// If entry in expected is not same as entry it got, the test is failed.
		if expected[i] != got[i] {
			return false
		}
	}

	// expected and got have same entries.
	return true
}

// TestReadDir - test function to run various readDir() tests.
func TestReadDir(t *testing.T) {
	defer teardown()
	setupTestReadDir1(t)
	setupTestReadDir2(t)
	setupTestReadDir3(t)

	for _, r := range testResults {
		if entries, err := readDir(r.dir); err != nil {
			t.Fatal("failed to run test.", err)
		} else {
			// Keep entries sorted for easier comparison.
			sort.Strings(entries)
			if !checkResult(r.entries, entries) {
				t.Fatalf("expected = %s, got: %s", r.entries, entries)
			}
		}
	}
}
