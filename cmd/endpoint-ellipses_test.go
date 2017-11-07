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
	"fmt"
	"reflect"
	"testing"
)

// Test tests calculating set indexes.
func TestGetSetIndexes(t *testing.T) {
	testCases := []struct {
		totalSize uint64
		indexes   []uint64
		success   bool
	}{
		// Invalid inputs.
		{
			27,
			nil,
			false,
		},
		// Valid inputs.
		{
			64,
			[]uint64{16, 16, 16, 16},
			true,
		},
		{
			24,
			[]uint64{12, 12},
			true,
		},
		{
			88,
			[]uint64{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8},
			true,
		},
		{
			4,
			[]uint64{4},
			true,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			gotIndexes, err := getSetIndexes(testCase.totalSize)
			if err != nil && testCase.success {
				t.Errorf("Expected success but failed instead %s", err)
			}
			if err == nil && !testCase.success {
				t.Errorf("Expected failure but passed instead")
			}
			if !reflect.DeepEqual(testCase.indexes, gotIndexes) {
				t.Errorf("Expected %v, got %v", testCase.indexes, gotIndexes)
			}
		})
	}
}

// Test tests parses endpoint ellipses input pattern.
func TestParseEndpointSet(t *testing.T) {
	testCases := []struct {
		arg     string
		es      endpointSet
		success bool
	}{
		// Tests invalid inputs.
		{
			"...",
			endpointSet{},
			false,
		},
		// Indivisible range.
		{
			"{1...27}",
			endpointSet{},
			false,
		},
		// No range specified.
		{
			"{...}",
			endpointSet{},
			false,
		},
		// Invalid range.
		{
			"http://minio{2...3}/export/set{1...0}",
			endpointSet{},
			false,
		},
		// More than 2 ellipses per argument not supported.
		{
			"http://minio{2...3}/export/set{1...64}/test{1...2}",
			endpointSet{},
			false,
		},
		// Tests valid inputs.
		{
			"/export/set{1...64}",
			endpointSet{[]ellipsesSubPattern{
				{
					"/export/set",
					"{1...64}",
					1, 64,
				},
			},
				64,
				[]uint64{16, 16, 16, 16},
			},
			true,
		},
		// Valid input for distributed setup.
		{
			"http://minio{2...3}/export/set{1...64}",
			endpointSet{[]ellipsesSubPattern{
				{
					"http://minio",
					"{2...3}",
					2, 3,
				},
				{
					"/export/set",
					"{1...64}",
					1, 64,
				},
			},
				128,
				[]uint64{16, 16, 16, 16, 16, 16, 16, 16},
			},
			true,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			gotEs, err := parseEndpointSet(testCase.arg)
			if err != nil && testCase.success {
				t.Errorf("Expected success but failed instead %s", err)
			}
			if err == nil && !testCase.success {
				t.Errorf("Expected failure but passed instead")
			}
			if !reflect.DeepEqual(testCase.es, gotEs) {
				t.Errorf("Expected %v, got %v", testCase.es, gotEs)
			}
		})
	}
}

// Test tests args with ellipses.
func TestHasArgsWithEllipses(t *testing.T) {
	testCases := []struct {
		args       []string
		expectedOk bool
	}{
		// Tests for all invalid inputs
		{
			[]string{"{1..64}"},
			false,
		},
		{
			[]string{"1...64"},
			false,
		},
		{
			[]string{"..."},
			false,
		},
		{
			[]string{"{-1...1}"},
			false,
		},
		{
			[]string{"{0...-1}"},
			false,
		},
		{
			[]string{"{1....4}"},
			false,
		},
		// Test for valid input.
		{
			[]string{"{1...64}"},
			true,
		},
		{
			[]string{"{...}"},
			true,
		},
		{
			[]string{"{1...64}", "{65...128}"},
			true,
		},
		{
			[]string{"http://minio{2...3}/export/set{1...64}"},
			true,
		},
		{
			[]string{
				"http://minio{2...3}/export/set{1...64}",
				"http://minio{2...3}/export/set{65...128}",
			},
			true,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			gotOk := hasArgsWithEllipses(testCase.args...)
			if gotOk != testCase.expectedOk {
				t.Errorf("Expected %t, got %t", testCase.expectedOk, gotOk)
			}
		})
	}
}

// Test tests parsing ellipses range.
func TestParseEllipsesRange(t *testing.T) {
	testCases := []struct {
		pattern string
		success bool
	}{
		// Tests for all invalid inputs
		{
			"{1..64}",
			false,
		},
		{
			"{1...64} {65...128}",
			false,
		},
		{
			"1...64",
			false,
		},
		{
			"...",
			false,
		},
		{
			"{1...",
			false,
		},
		{
			"...64}",
			false,
		},
		{
			"{...}",
			false,
		},
		{
			"{-1...1}",
			false,
		},
		{
			"{0...-1}",
			false,
		},
		{
			"{64...1}",
			false,
		},
		{
			"{1....4}",
			false,
		},
		// Test for valid input.
		{
			"{1...64}",
			true,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			_, _, err := parseEllipsesRange(testCase.pattern)
			if err != nil && testCase.success {
				t.Errorf("Expected success but failed instead %s", err)
			}
			if err == nil && !testCase.success {
				t.Errorf("Expected failure but passed instead")
			}
		})
	}
}
