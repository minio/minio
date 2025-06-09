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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
)

// Test to check for different input arguments.
func TestReadDirFail(t *testing.T) {
	// Check non existent directory.
	if _, err := readDir("/tmp/non-existent-directory"); err != errFileNotFound {
		t.Fatalf("expected = %s, got: %s", errFileNotFound, err)
	}

	file := path.Join(os.TempDir(), "issue")
	if err := os.WriteFile(file, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(file)

	// Check if file is given.
	if _, err := readDir(path.Join(file, "mydir")); err != errFileNotFound {
		t.Fatalf("expected = %s, got: %s", errFileNotFound, err)
	}

	// Only valid for linux.
	if runtime.GOOS == "linux" {
		permDir := path.Join(os.TempDir(), "perm-dir")
		if err := os.MkdirAll(permDir, os.FileMode(0o200)); err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(permDir)

		// Check if permission denied.
		if _, err := readDir(permDir); err == nil {
			t.Fatalf("expected = an error, got: nil")
		}
	}
}

// Represents data type for all the test results.
type result struct {
	dir     string
	entries []string
}

// Test to read empty directory.
func setupTestReadDirEmpty(t *testing.T) (testResults []result) {
	// Add empty entry slice for this test directory.
	testResults = append(testResults, result{t.TempDir(), []string{}})
	return testResults
}

// Test to read non-empty directory with only files.
func setupTestReadDirFiles(t *testing.T) (testResults []result) {
	dir := t.TempDir()
	entries := []string{}
	for i := range 10 {
		name := fmt.Sprintf("file-%d", i)
		if err := os.WriteFile(filepath.Join(dir, name), []byte{}, os.ModePerm); err != nil {
			// For cleanup, its required to add these entries into test results.
			testResults = append(testResults, result{dir, entries})
			t.Fatalf("Unable to create file, %s", err)
		}
		entries = append(entries, name)
	}

	// Keep entries sorted for easier comparison.
	sort.Strings(entries)

	// Add entries slice for this test directory.
	testResults = append(testResults, result{dir, entries})
	return testResults
}

// Test to read non-empty directory with directories and files.
func setupTestReadDirGeneric(t *testing.T) (testResults []result) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "mydir"), 0o777); err != nil {
		t.Fatalf("Unable to create prefix directory \"mydir\", %s", err)
	}
	entries := []string{"mydir/"}
	for i := range 10 {
		name := fmt.Sprintf("file-%d", i)
		if err := os.WriteFile(filepath.Join(dir, "mydir", name), []byte{}, os.ModePerm); err != nil {
			// For cleanup, its required to add these entries into test results.
			testResults = append(testResults, result{dir, entries})
			t.Fatalf("Unable to write file, %s", err)
		}
	}
	// Keep entries sorted for easier comparison.
	sort.Strings(entries)

	// Add entries slice for this test directory.
	testResults = append(testResults, result{dir, entries})
	return testResults
}

// Test to read non-empty directory with symlinks.
func setupTestReadDirSymlink(t *testing.T) (testResults []result) {
	if runtime.GOOS == globalWindowsOSName {
		t.Skip("symlinks not available on windows")
		return nil
	}
	dir := t.TempDir()
	entries := []string{}
	for i := range 10 {
		name1 := fmt.Sprintf("file-%d", i)
		name2 := fmt.Sprintf("file-%d", i+10)
		if err := os.WriteFile(filepath.Join(dir, name1), []byte{}, os.ModePerm); err != nil {
			// For cleanup, its required to add these entries into test results.
			testResults = append(testResults, result{dir, entries})
			t.Fatalf("Unable to create a file, %s", err)
		}
		// Symlink will not be added to entries.
		if err := os.Symlink(filepath.Join(dir, name1), filepath.Join(dir, name2)); err != nil {
			t.Fatalf("Unable to create a symlink, %s", err)
		}
		// Add to entries.
		entries = append(entries, name1)
		// Symlinks are preserved for regular files
		entries = append(entries, name2)
	}
	if err := os.MkdirAll(filepath.Join(dir, "mydir"), 0o777); err != nil {
		t.Fatalf("Unable to create \"mydir\", %s", err)
	}
	entries = append(entries, "mydir/")

	// Keep entries sorted for easier comparison.
	sort.Strings(entries)

	// Add entries slice for this test directory.
	testResults = append(testResults, result{dir, entries})
	return testResults
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

// teardown - cleans up test directories.
func teardown(testResults []result) {
	for _, r := range testResults {
		os.RemoveAll(r.dir)
	}
}

// TestReadDir - test function to run various readDir() tests.
func TestReadDir(t *testing.T) {
	var testResults []result

	// Setup and capture test results for empty directory.
	testResults = append(testResults, setupTestReadDirEmpty(t)...)
	// Setup and capture test results for directory with only files.
	testResults = append(testResults, setupTestReadDirFiles(t)...)
	// Setup and capture test results for directory with files and directories.
	testResults = append(testResults, setupTestReadDirGeneric(t)...)
	// Setup and capture test results for directory with files and symlink.
	testResults = append(testResults, setupTestReadDirSymlink(t)...)

	// Remove all dirs once tests are over.
	defer teardown(testResults)

	// Validate all the results.
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

func TestReadDirN(t *testing.T) {
	testCases := []struct {
		numFiles    int
		n           int
		expectedNum int
	}{
		{0, 0, 0},
		{0, 1, 0},
		{1, 0, 0},
		{0, -1, 0},
		{1, -1, 1},
		{10, -1, 10},
		{1, 1, 1},
		{2, 1, 1},
		{10, 9, 9},
		{10, 10, 10},
		{10, 11, 10},
	}

	for i, testCase := range testCases {
		dir := t.TempDir()

		for c := 1; c <= testCase.numFiles; c++ {
			err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d", c)), []byte{}, os.ModePerm)
			if err != nil {
				os.RemoveAll(dir)
				t.Fatalf("Unable to create a file, %s", err)
			}
		}
		entries, err := readDirN(dir, testCase.n)
		if err != nil {
			os.RemoveAll(dir)
			t.Fatalf("Unable to read entries, %s", err)
		}
		if len(entries) != testCase.expectedNum {
			os.RemoveAll(dir)
			t.Fatalf("Test %d: unexpected number of entries, waiting for %d, but found %d",
				i+1, testCase.expectedNum, len(entries))
		}
		os.RemoveAll(dir)
	}
}
