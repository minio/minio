/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017 MinIO, Inc.
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
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
)

const ActualSize = 1000

// Test xlMetaV1.AddObjectPart()
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
	xlMeta := newXLMetaV1("test-object", 8, 8)
	if !xlMeta.IsValid() {
		t.Fatalf("unable to get xl meta")
	}

	// Test them.
	for _, testCase := range testCases {
		if testCase.expectedIndex > -1 {
			xlMeta.AddObjectPart(testCase.partNum, "", int64(testCase.partNum+humanize.MiByte), ActualSize)
		}

		if index := objectPartIndex(xlMeta.Parts, testCase.partNum); index != testCase.expectedIndex {
			t.Fatalf("%+v: expected = %d, got: %d", testCase, testCase.expectedIndex, index)
		}
	}
}

// Test objectPartIndex().
// generates a sample xlMeta data and asserts the output of objectPartIndex() with the expected value.
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
	xlMeta := newXLMetaV1("test-object", 8, 8)
	if !xlMeta.IsValid() {
		t.Fatalf("unable to get xl meta")
	}

	// Add some parts for testing.
	for _, testCase := range testCases {
		xlMeta.AddObjectPart(testCase.partNum, "", int64(testCase.partNum+humanize.MiByte), ActualSize)
	}

	// Add failure test case.
	testCases = append(testCases, struct {
		partNum       int
		expectedIndex int
	}{6, -1})

	// Test them.
	for _, testCase := range testCases {
		if index := objectPartIndex(xlMeta.Parts, testCase.partNum); index != testCase.expectedIndex {
			t.Fatalf("%+v: expected = %d, got: %d", testCase, testCase.expectedIndex, index)
		}
	}
}

// Test xlMetaV1.ObjectToPartOffset().
func TestObjectToPartOffset(t *testing.T) {
	// Setup.
	xlMeta := newXLMetaV1("test-object", 8, 8)
	if !xlMeta.IsValid() {
		t.Fatalf("unable to get xl meta")
	}

	// Add some parts for testing.
	// Total size of all parts is 5,242,899 bytes.
	for _, partNum := range []int{1, 2, 4, 5, 7} {
		xlMeta.AddObjectPart(partNum, "", int64(partNum+humanize.MiByte), ActualSize)
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
		index, offset, err := xlMeta.ObjectToPartOffset(GlobalContext, testCase.offset)
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

// Helper function to check if two xlMetaV1 values are similar.
func isXLMetaSimilar(m, n xlMetaV1) bool {
	if m.Version != n.Version {
		return false
	}
	if m.Format != n.Format {
		return false
	}
	if len(m.Parts) != len(n.Parts) {
		return false
	}
	return true
}

func TestPickValidXLMeta(t *testing.T) {
	obj := "object"
	x1 := newXLMetaV1(obj, 4, 4)
	now := UTCNow()
	x1.Stat.ModTime = now
	invalidX1 := x1
	invalidX1.Version = "invalid-version"
	xs := []xlMetaV1{x1, x1, x1, x1}
	invalidXS := []xlMetaV1{invalidX1, invalidX1, invalidX1, invalidX1}
	testCases := []struct {
		metaArr     []xlMetaV1
		modTime     time.Time
		xlMeta      xlMetaV1
		expectedErr error
	}{
		{
			metaArr:     xs,
			modTime:     now,
			xlMeta:      x1,
			expectedErr: nil,
		},
		{
			metaArr:     invalidXS,
			modTime:     now,
			xlMeta:      invalidX1,
			expectedErr: errXLReadQuorum,
		},
	}
	for i, test := range testCases {
		xlMeta, err := pickValidXLMeta(GlobalContext, test.metaArr, test.modTime, len(test.metaArr)/2)
		if test.expectedErr != nil {
			if err.Error() != test.expectedErr.Error() {
				t.Errorf("Test %d: Expected to fail with %v but received %v",
					i+1, test.expectedErr, err)
			}
		} else {
			if !isXLMetaSimilar(xlMeta, test.xlMeta) {
				t.Errorf("Test %d: Expected %v but received %v",
					i+1, test.xlMeta, xlMeta)
			}
		}
	}
}

func TestIsXLMetaFormatValid(t *testing.T) {
	tests := []struct {
		name    int
		version string
		format  string
		want    bool
	}{
		{1, "123", "fs", false},
		{2, "123", xlMetaFormat, false},
		{3, xlMetaVersion, "test", false},
		{4, xlMetaVersion100, "hello", false},
		{5, xlMetaVersion, xlMetaFormat, true},
		{6, xlMetaVersion100, xlMetaFormat, true},
	}
	for _, tt := range tests {
		if got := isXLMetaFormatValid(tt.version, tt.format); got != tt.want {
			t.Errorf("Test %d: Expected %v but received %v", tt.name, got, tt.want)
		}
	}
}

func TestIsXLMetaErasureInfoValid(t *testing.T) {
	tests := []struct {
		name   int
		data   int
		parity int
		want   bool
	}{
		{1, 5, 6, false},
		{2, 5, 5, true},
		{3, 0, 5, false},
		{4, 5, 0, false},
		{5, 5, 0, false},
		{6, 5, 4, true},
	}
	for _, tt := range tests {
		if got := isXLMetaErasureInfoValid(tt.data, tt.parity); got != tt.want {
			t.Errorf("Test %d: Expected %v but received %v", tt.name, got, tt.want)
		}
	}
}
