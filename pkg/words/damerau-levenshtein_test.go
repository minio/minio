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

package words

import (
	"math"
	"testing"
)

// Test minimum function which calculates the minimal value in a list of integers
func TestMinimum(t *testing.T) {
	type testCase struct {
		listval  []int
		expected int
	}
	testCases := []testCase{
		{listval: []int{3, 4, 15}, expected: 3},
		{listval: []int{}, expected: math.MaxInt32},
	}
	// Validate all the test cases.
	for i, tt := range testCases {
		val := minimum(tt.listval)
		if val != tt.expected {
			t.Errorf("Test %d:, Expected %d, got %d", i+1, tt.expected, val)
		}
	}
}

// Test DamerauLevenshtein which calculates the difference distance between two words
func TestDamerauLevenshtein(t *testing.T) {
	type testCase struct {
		word1    string
		word2    string
		distance int
	}
	testCases := []testCase{
		{word1: "", word2: "", distance: 0},
		{word1: "a", word2: "a", distance: 0},
		{word1: "a", word2: "b", distance: 1},
		{word1: "rm", word2: "tm", distance: 1},
		{word1: "version", word2: "evrsion", distance: 1},
		{word1: "version", word2: "bersio", distance: 2},
	}
	// Validate all the test cases.
	for i, tt := range testCases {
		d := DamerauLevenshteinDistance(tt.word1, tt.word2)
		if d != tt.distance {
			t.Errorf("Test %d:, Expected %d, got %d", i+1, tt.distance, d)
		}
	}
}
