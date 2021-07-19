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
	"strconv"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
)

const ActualSize = 1000

// Test FileInfo.AddObjectPart()
func TestAddObjectPart(t *testing.T) {
	testCases := []struct {
		partNum       int
		expectedIndex int
	}{
		{1, 0},
		{2, 1},
		{4, 2},
		{5, 3},
		{7, 4},
		// Insert part.
		{3, 2},
		// Replace existing part.
		{4, 3},
		// Missing part.
		{6, -1},
	}

	// Setup.
	fi := newFileInfo("test-object", 8, 8)
	fi.Erasure.Index = 1
	if !fi.IsValid() {
		t.Fatalf("unable to get xl meta")
	}

	// Test them.
	for _, testCase := range testCases {
		if testCase.expectedIndex > -1 {
			partNumString := strconv.Itoa(testCase.partNum)
			fi.AddObjectPart(testCase.partNum, "etag."+partNumString, int64(testCase.partNum+humanize.MiByte), ActualSize)
		}

		if index := objectPartIndex(fi.Parts, testCase.partNum); index != testCase.expectedIndex {
			t.Fatalf("%+v: expected = %d, got: %d", testCase, testCase.expectedIndex, index)
		}
	}
}

// Test objectPartIndex(). generates a sample FileInfo data and asserts
// the output of objectPartIndex() with the expected value.
func TestObjectPartIndex(t *testing.T) {
	testCases := []struct {
		partNum       int
		expectedIndex int
	}{
		{2, 1},
		{1, 0},
		{5, 3},
		{4, 2},
		{7, 4},
	}

	// Setup.
	fi := newFileInfo("test-object", 8, 8)
	fi.Erasure.Index = 1
	if !fi.IsValid() {
		t.Fatalf("unable to get xl meta")
	}

	// Add some parts for testing.
	for _, testCase := range testCases {
		partNumString := strconv.Itoa(testCase.partNum)
		fi.AddObjectPart(testCase.partNum, "etag."+partNumString, int64(testCase.partNum+humanize.MiByte), ActualSize)
	}

	// Add failure test case.
	testCases = append(testCases, struct {
		partNum       int
		expectedIndex int
	}{6, -1})

	// Test them.
	for _, testCase := range testCases {
		if index := objectPartIndex(fi.Parts, testCase.partNum); index != testCase.expectedIndex {
			t.Fatalf("%+v: expected = %d, got: %d", testCase, testCase.expectedIndex, index)
		}
	}
}

// Test FileInfo.ObjectToPartOffset().
func TestObjectToPartOffset(t *testing.T) {
	// Setup.
	fi := newFileInfo("test-object", 8, 8)
	fi.Erasure.Index = 1
	if !fi.IsValid() {
		t.Fatalf("unable to get xl meta")
	}

	// Add some parts for testing.
	// Total size of all parts is 5,242,899 bytes.
	for _, partNum := range []int{1, 2, 4, 5, 7} {
		partNumString := strconv.Itoa(partNum)
		fi.AddObjectPart(partNum, "etag."+partNumString, int64(partNum+humanize.MiByte), ActualSize)
	}

	testCases := []struct {
		offset         int64
		expectedIndex  int
		expectedOffset int64
		expectedErr    error
	}{
		{0, 0, 0, nil},
		{1 * humanize.MiByte, 0, 1 * humanize.MiByte, nil},
		{1 + humanize.MiByte, 1, 0, nil},
		{2 + humanize.MiByte, 1, 1, nil},
		// Its valid for zero sized object.
		{-1, 0, -1, nil},
		// Max fffset is always (size - 1).
		{(1 + 2 + 4 + 5 + 7) + (5 * humanize.MiByte) - 1, 4, 1048582, nil},
		// Error if offset is size.
		{(1 + 2 + 4 + 5 + 7) + (5 * humanize.MiByte), 0, 0, InvalidRange{}},
	}

	// Test them.
	for _, testCase := range testCases {
		index, offset, err := fi.ObjectToPartOffset(context.Background(), testCase.offset)
		if err != testCase.expectedErr {
			t.Fatalf("%+v: expected = %s, got: %s", testCase, testCase.expectedErr, err)
		}
		if index != testCase.expectedIndex {
			t.Fatalf("%+v: index: expected = %d, got: %d", testCase, testCase.expectedIndex, index)
		}
		if offset != testCase.expectedOffset {
			t.Fatalf("%+v: offset: expected = %d, got: %d", testCase, testCase.expectedOffset, offset)
		}
	}
}

func TestFindFileInfoInQuorum(t *testing.T) {
	getNFInfo := func(n int, quorum int, t int64, dataDir string) []FileInfo {
		fi := newFileInfo("test", 8, 8)
		fi.AddObjectPart(1, "etag", 100, 100)
		fi.ModTime = time.Unix(t, 0)
		fi.DataDir = dataDir
		fis := make([]FileInfo, n)
		for i := range fis {
			fis[i] = fi
			fis[i].Erasure.Index = i + 1
			quorum--
			if quorum == 0 {
				break
			}
		}
		return fis
	}

	tests := []struct {
		fis            []FileInfo
		modTime        time.Time
		dataDir        string
		expectedErr    error
		expectedQuorum int
	}{
		{
			fis:            getNFInfo(16, 16, 1603863445, "36a21454-a2ca-11eb-bbaa-93a81c686f21"),
			modTime:        time.Unix(1603863445, 0),
			dataDir:        "36a21454-a2ca-11eb-bbaa-93a81c686f21",
			expectedErr:    nil,
			expectedQuorum: 8,
		},
		{
			fis:            getNFInfo(16, 7, 1603863445, "36a21454-a2ca-11eb-bbaa-93a81c686f21"),
			modTime:        time.Unix(1603863445, 0),
			dataDir:        "36a21454-a2ca-11eb-bbaa-93a81c686f21",
			expectedErr:    errErasureReadQuorum,
			expectedQuorum: 8,
		},
		{
			fis:            getNFInfo(16, 16, 1603863445, "36a21454-a2ca-11eb-bbaa-93a81c686f21"),
			modTime:        time.Unix(1603863445, 0),
			dataDir:        "36a21454-a2ca-11eb-bbaa-93a81c686f21",
			expectedErr:    errErasureReadQuorum,
			expectedQuorum: 0,
		},
	}

	for _, test := range tests {
		test := test
		t.Run("", func(t *testing.T) {
			_, err := findFileInfoInQuorum(context.Background(), test.fis, test.modTime, test.dataDir, test.expectedQuorum)
			if err != test.expectedErr {
				t.Errorf("Expected %s, got %s", test.expectedErr, err)
			}
		})
	}
}
