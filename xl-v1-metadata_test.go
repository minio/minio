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
	"testing"
)

// Test cases for xlMetaV1{}
func TestXLMetaV1(t *testing.T) {
	fiveMB := int64(5 * 1024 * 1024)

	testCases := []struct {
		partNum  int
		partName string
		etag     string
		size     int64
		index    int
	}{
		{5, "part.5", "etag5", fiveMB + 5, 3},
		{4, "part.4", "etag4", fiveMB + 4, 2},
		{7, "part.7", "etag7", fiveMB + 7, 4},
		{2, "part.2", "etag2", fiveMB + 2, 1},
		{1, "part.1", "etag1", fiveMB + 1, 0},
	}

	// Create a XLMetaV1 structure to test on.
	meta := newXLMetaV1(8, 8)

	// Add 5 parts.
	for _, test := range testCases {
		meta.AddObjectPart(test.partNum, test.partName, test.etag, test.size)
	}

	// Test for ObjectPartIndex()
	for i, test := range testCases {
		expected := test.index
		got := meta.ObjectPartIndex(test.partNum)
		if expected != got {
			t.Errorf("Test %d: Expected %d, obtained %d", i+1, expected, got)
		}
	}

	offsetCases := []struct {
		offset     int64
		partIndex  int
		partOffset int64
	}{
		{4 * 1024 * 1024, 0, 4 * 1024 * 1024},
		{8 * 1024 * 1024, 1, 3145727},
		{12 * 1024 * 1024, 2, 2097149},
		{16 * 1024 * 1024, 3, 1048569},
		{20 * 1024 * 1024, 3, 5242873},
		{24 * 1024 * 1024, 4, 4194292},
	}

	// Test for ObjectToPartOffset()
	for i, test := range offsetCases {
		expectedIndex := test.partIndex
		expectedOffset := test.partOffset
		gotIndex, gotOffset, err := meta.ObjectToPartOffset(test.offset)
		if err != nil {
			t.Errorf("Test %d: %s", i+1, err)
		}
		if gotIndex != expectedIndex {
			t.Errorf("Test %d: Expected %v got %v", i+1, expectedIndex, gotIndex)
		}
		if gotOffset != expectedOffset {
			t.Errorf("Test %d: Expected %v got %v", i+1, expectedOffset, gotOffset)
		}
	}
}
