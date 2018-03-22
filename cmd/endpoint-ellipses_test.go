/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

	"github.com/minio/minio/pkg/ellipses"
)

// Tests create endpoints with ellipses and without.
func TestCreateServerEndpoints(t *testing.T) {
	testCases := []struct {
		serverAddr string
		args       []string
		success    bool
	}{
		// Invalid input.
		{"", []string{}, false},
		// Range cannot be negative.
		{":9000", []string{"/export1{-1...1}"}, false},
		// Range cannot start bigger than end.
		{":9000", []string{"/export1{64...1}"}, false},
		// Range can only be numeric.
		{":9000", []string{"/export1{a...z}"}, false},
		// Duplicate disks not allowed.
		{":9000", []string{"/export1{1...32}", "/export1{1...32}"}, false},
		// Same host cannot export same disk on two ports - special case localhost.
		{":9001", []string{"http://localhost:900{1...2}/export{1...64}"}, false},

		// Valid inputs.
		{":9000", []string{"/export1"}, true},
		{":9000", []string{"/export1", "/export2", "/export3", "/export4"}, true},
		{":9000", []string{"/export1{1...64}"}, true},
		{":9000", []string{"/export1{01...64}"}, true},
		{":9000", []string{"/export1{1...32}", "/export1{33...64}"}, true},
		{":9001", []string{"http://localhost:9001/export{1...64}"}, true},
		{":9001", []string{"http://localhost:9001/export{01...64}"}, true},
	}

	for i, testCase := range testCases {
		_, _, _, _, _, err := createServerEndpoints(testCase.serverAddr, testCase.args...)
		if err != nil && testCase.success {
			t.Errorf("Test %d: Expected success but failed instead %s", i+1, err)
		}
		if err == nil && !testCase.success {
			t.Errorf("Test %d: Expected failure but passed instead", i+1)
		}
	}
}

func TestGetDivisibleSize(t *testing.T) {
	testCases := []struct {
		totalSizes []uint64
		result     uint64
	}{{[]uint64{24, 32, 16}, 8},
		{[]uint64{32, 8, 4}, 4},
		{[]uint64{8, 8, 8}, 8},
		{[]uint64{24}, 24},
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			gotGCD := getDivisibleSize(testCase.totalSizes)
			if testCase.result != gotGCD {
				t.Errorf("Expected %v, got %v", testCase.result, gotGCD)
			}
		})
	}
}

// Test tests calculating set indexes.
func TestGetSetIndexes(t *testing.T) {
	testCases := []struct {
		args       []string
		totalSizes []uint64
		indexes    [][]uint64
		success    bool
	}{
		// Invalid inputs.
		{
			[]string{"data{1...27}"},
			[]uint64{27},
			nil,
			false,
		},
		{
			[]string{"data{1...3}"},
			[]uint64{3},
			nil,
			false,
		},
		{
			[]string{"data/controller1/export{1...2}, data/controller2/export{1...4}, data/controller3/export{1...8}"},
			[]uint64{2, 4, 8},
			nil,
			false,
		},
		// Valid inputs.
		{
			[]string{"data/controller1/export{1...4}, data/controller2/export{1...8}, data/controller3/export{1...12}"},
			[]uint64{4, 8, 12},
			[][]uint64{{4}, {4, 4}, {4, 4, 4}},
			true,
		},
		{
			[]string{"data{1...64}"},
			[]uint64{64},
			[][]uint64{{16, 16, 16, 16}},
			true,
		},
		{
			[]string{"data{1...24}"},
			[]uint64{24},
			[][]uint64{{12, 12}},
			true,
		},
		{
			[]string{"data/controller{1...11}/export{1...8}"},
			[]uint64{88},
			[][]uint64{{8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8}},
			true,
		},
		{
			[]string{"data{1...4}"},
			[]uint64{4},
			[][]uint64{{4}},
			true,
		},
		{
			[]string{"data/controller1/export{1...10}, data/controller2/export{1...10}, data/controller3/export{1...10}"},
			[]uint64{10, 10, 10},
			[][]uint64{{10}, {10}, {10}},
			true,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			gotIndexes, err := getSetIndexes(testCase.args, testCase.totalSizes)
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

func getSequences(start int, number int, paddinglen int) (seq []string) {
	for i := start; i <= number; i++ {
		if paddinglen == 0 {
			seq = append(seq, fmt.Sprintf("%d", i))
		} else {
			seq = append(seq, fmt.Sprintf(fmt.Sprintf("%%0%dd", paddinglen), i))
		}
	}
	return seq
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
		// Range cannot be smaller than 4 minimum.
		{
			"/export{1..2}",
			endpointSet{},
			false,
		},
		// Unsupported characters.
		{
			"/export/test{1...2O}",
			endpointSet{},
			false,
		},
		// Tests valid inputs.
		{
			"/export/set{1...64}",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							"/export/set",
							"",
							getSequences(1, 64, 0),
						},
					},
				},
				nil,
				[][]uint64{{16, 16, 16, 16}},
			},
			true,
		},
		// Valid input for distributed setup.
		{
			"http://minio{2...3}/export/set{1...64}",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							"",
							"",
							getSequences(1, 64, 0),
						},
						{
							"http://minio",
							"/export/set",
							getSequences(2, 3, 0),
						},
					},
				},
				nil,
				[][]uint64{{16, 16, 16, 16, 16, 16, 16, 16}},
			},
			true,
		},
		// Supporting some advanced cases.
		{
			"http://minio{1...64}.mydomain.net/data",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							"http://minio",
							".mydomain.net/data",
							getSequences(1, 64, 0),
						},
					},
				},
				nil,
				[][]uint64{{16, 16, 16, 16}},
			},
			true,
		},
		{
			"http://rack{1...4}.mydomain.minio{1...16}/data",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							"",
							"/data",
							getSequences(1, 16, 0),
						},
						{
							"http://rack",
							".mydomain.minio",
							getSequences(1, 4, 0),
						},
					},
				},
				nil,
				[][]uint64{{16, 16, 16, 16}},
			},
			true,
		},
		// Supporting kubernetes cases.
		{
			"http://minio{0...15}.mydomain.net/data{0...1}",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							"",
							"",
							getSequences(0, 1, 0),
						},
						{
							"http://minio",
							".mydomain.net/data",
							getSequences(0, 15, 0),
						},
					},
				},
				nil,
				[][]uint64{{16, 16}},
			},
			true,
		},
		// No host regex, just disks.
		{
			"http://server1/data{1...32}",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							"http://server1/data",
							"",
							getSequences(1, 32, 0),
						},
					},
				},
				nil,
				[][]uint64{{16, 16}},
			},
			true,
		},
		// No host regex, just disks with two position numerics.
		{
			"http://server1/data{01...32}",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							"http://server1/data",
							"",
							getSequences(1, 32, 2),
						},
					},
				},
				nil,
				[][]uint64{{16, 16}},
			},
			true,
		},
		// More than 2 ellipses are supported as well.
		{
			"http://minio{2...3}/export/set{1...64}/test{1...2}",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							"",
							"",
							getSequences(1, 2, 0),
						},
						{
							"",
							"/test",
							getSequences(1, 64, 0),
						},
						{
							"http://minio",
							"/export/set",
							getSequences(2, 3, 0),
						},
					},
				},
				nil,
				[][]uint64{{16, 16, 16, 16, 16, 16, 16, 16,
					16, 16, 16, 16, 16, 16, 16, 16}},
			},
			true,
		},
		// More than 1 ellipses per argument for standalone setup.
		{
			"/export{1...10}/disk{1...10}",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							"",
							"",
							getSequences(1, 10, 0),
						},
						{
							"/export",
							"/disk",
							getSequences(1, 10, 0),
						},
					},
				},
				nil,
				[][]uint64{{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}},
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
