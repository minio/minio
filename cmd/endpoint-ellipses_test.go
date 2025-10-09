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
	"reflect"
	"testing"

	"github.com/minio/pkg/v3/ellipses"
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
		t.Run("", func(t *testing.T) {
			srvCtxt := serverCtxt{}
			err := mergeDisksLayoutFromArgs(testCase.args, &srvCtxt)
			if err != nil && testCase.success {
				t.Fatalf("Test %d: unexpected error: %v", i+1, err)
			}
			_, _, err = createServerEndpoints(testCase.serverAddr, srvCtxt.Layout.pools, srvCtxt.Layout.legacy)
			if err != nil && testCase.success {
				t.Errorf("Test %d: Expected success but failed instead %s", i+1, err)
			}
			if err == nil && !testCase.success {
				t.Errorf("Test %d: Expected failure but passed instead", i+1)
			}
		})
	}
}

func TestGetDivisibleSize(t *testing.T) {
	testCases := []struct {
		totalSizes []uint64
		result     uint64
	}{
		{[]uint64{24, 32, 16}, 8},
		{[]uint64{32, 8, 4}, 4},
		{[]uint64{8, 8, 8}, 8},
		{[]uint64{24}, 24},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			gotGCD := getDivisibleSize(testCase.totalSizes)
			if testCase.result != gotGCD {
				t.Errorf("Expected %v, got %v", testCase.result, gotGCD)
			}
		})
	}
}

// Test tests calculating set indexes with ENV override for drive count.
func TestGetSetIndexesEnvOverride(t *testing.T) {
	testCases := []struct {
		args        []string
		totalSizes  []uint64
		indexes     [][]uint64
		envOverride uint64
		success     bool
	}{
		{
			[]string{"data{1...64}"},
			[]uint64{64},
			[][]uint64{{8, 8, 8, 8, 8, 8, 8, 8}},
			8,
			true,
		},
		{
			[]string{"http://host{1...2}/data{1...180}"},
			[]uint64{360},
			[][]uint64{{15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15}},
			15,
			true,
		},
		{
			[]string{"http://host{1...12}/data{1...12}"},
			[]uint64{144},
			[][]uint64{{12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12}},
			12,
			true,
		},
		{
			[]string{"http://host{0...5}/data{1...28}"},
			[]uint64{168},
			[][]uint64{{12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12}},
			12,
			true,
		},
		// Incorrect custom set drive count.
		{
			[]string{"http://host{0...5}/data{1...28}"},
			[]uint64{168},
			nil,
			10,
			false,
		},
		// Failure not divisible number of disks.
		{
			[]string{"http://host{1...11}/data{1...11}"},
			[]uint64{121},
			[][]uint64{{11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11}},
			11,
			true,
		},
		{
			[]string{"data{1...60}"},
			nil,
			nil,
			8,
			false,
		},
		{
			[]string{"data{1...64}"},
			nil,
			nil,
			64,
			false,
		},
		{
			[]string{"data{1...64}"},
			nil,
			nil,
			2,
			false,
		},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			argPatterns := make([]ellipses.ArgPattern, len(testCase.args))
			for i, arg := range testCase.args {
				patterns, err := ellipses.FindEllipsesPatterns(arg)
				if err != nil {
					t.Fatalf("Unexpected failure %s", err)
				}
				argPatterns[i] = patterns
			}

			gotIndexes, err := getSetIndexes(testCase.args, testCase.totalSizes, testCase.envOverride, argPatterns)
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
			[]string{"data{1...17}/export{1...52}"},
			[]uint64{14144},
			nil,
			false,
		},
		// Valid inputs.
		{
			[]string{"data{1...3}"},
			[]uint64{3},
			[][]uint64{{3}},
			true,
		},
		{
			[]string{"data/controller1/export{1...2}, data/controller2/export{1...4}, data/controller3/export{1...8}"},
			[]uint64{2, 4, 8},
			[][]uint64{{2}, {2, 2}, {2, 2, 2, 2}},
			true,
		},
		{
			[]string{"data{1...27}"},
			[]uint64{27},
			[][]uint64{{9, 9, 9}},
			true,
		},
		{
			[]string{"http://host{1...3}/data{1...180}"},
			[]uint64{540},
			[][]uint64{{15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15}},
			true,
		},
		{
			[]string{"http://host{1...2}.rack{1...4}/data{1...180}"},
			[]uint64{1440},
			[][]uint64{{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16}},
			true,
		},
		{
			[]string{"http://host{1...2}/data{1...180}"},
			[]uint64{360},
			[][]uint64{{12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12}},
			true,
		},
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
			[][]uint64{{11, 11, 11, 11, 11, 11, 11, 11}},
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
		{
			[]string{"data{1...16}/export{1...52}"},
			[]uint64{832},
			[][]uint64{{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16}},
			true,
		},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			argPatterns := make([]ellipses.ArgPattern, len(testCase.args))
			for i, arg := range testCase.args {
				patterns, err := ellipses.FindEllipsesPatterns(arg)
				if err != nil {
					t.Fatalf("Unexpected failure %s", err)
				}
				argPatterns[i] = patterns
			}
			gotIndexes, err := getSetIndexes(testCase.args, testCase.totalSizes, 0, argPatterns)
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

func getHexSequences(start int, number int, paddinglen int) (seq []string) {
	for i := start; i <= number; i++ {
		if paddinglen == 0 {
			seq = append(seq, fmt.Sprintf("%x", i))
		} else {
			seq = append(seq, fmt.Sprintf(fmt.Sprintf("%%0%dx", paddinglen), i))
		}
	}
	return seq
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
			"{1...27}",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							Prefix: "",
							Suffix: "",
							Seq:    getSequences(1, 27, 0),
						},
					},
				},
				nil,
				[][]uint64{{9, 9, 9}},
			},
			true,
		},
		{
			"/export/set{1...64}",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							Prefix: "/export/set",
							Suffix: "",
							Seq:    getSequences(1, 64, 0),
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
							Prefix: "",
							Suffix: "",
							Seq:    getSequences(1, 64, 0),
						},
						{
							Prefix: "http://minio",
							Suffix: "/export/set",
							Seq:    getSequences(2, 3, 0),
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
							Prefix: "http://minio",
							Suffix: ".mydomain.net/data",
							Seq:    getSequences(1, 64, 0),
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
							Prefix: "",
							Suffix: "/data",
							Seq:    getSequences(1, 16, 0),
						},
						{
							Prefix: "http://rack",
							Suffix: ".mydomain.minio",
							Seq:    getSequences(1, 4, 0),
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
							Prefix: "",
							Suffix: "",
							Seq:    getSequences(0, 1, 0),
						},
						{
							Prefix: "http://minio",
							Suffix: ".mydomain.net/data",
							Seq:    getSequences(0, 15, 0),
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
							Prefix: "http://server1/data",
							Suffix: "",
							Seq:    getSequences(1, 32, 0),
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
							Prefix: "http://server1/data",
							Suffix: "",
							Seq:    getSequences(1, 32, 2),
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
							Prefix: "",
							Suffix: "",
							Seq:    getSequences(1, 2, 0),
						},
						{
							Prefix: "",
							Suffix: "/test",
							Seq:    getSequences(1, 64, 0),
						},
						{
							Prefix: "http://minio",
							Suffix: "/export/set",
							Seq:    getSequences(2, 3, 0),
						},
					},
				},
				nil,
				[][]uint64{{
					16, 16, 16, 16, 16, 16, 16, 16,
					16, 16, 16, 16, 16, 16, 16, 16,
				}},
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
							Prefix: "",
							Suffix: "",
							Seq:    getSequences(1, 10, 0),
						},
						{
							Prefix: "/export",
							Suffix: "/disk",
							Seq:    getSequences(1, 10, 0),
						},
					},
				},
				nil,
				[][]uint64{{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}},
			},
			true,
		},
		// IPv6 ellipses with hexadecimal expansion
		{
			"http://[2001:3984:3989::{1...a}]/disk{1...10}",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							Prefix: "",
							Suffix: "",
							Seq:    getSequences(1, 10, 0),
						},
						{
							Prefix: "http://[2001:3984:3989::",
							Suffix: "]/disk",
							Seq:    getHexSequences(1, 10, 0),
						},
					},
				},
				nil,
				[][]uint64{{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}},
			},
			true,
		},
		// IPv6 ellipses with hexadecimal expansion with 3 position numerics.
		{
			"http://[2001:3984:3989::{001...00a}]/disk{1...10}",
			endpointSet{
				[]ellipses.ArgPattern{
					[]ellipses.Pattern{
						{
							Prefix: "",
							Suffix: "",
							Seq:    getSequences(1, 10, 0),
						},
						{
							Prefix: "http://[2001:3984:3989::",
							Suffix: "]/disk",
							Seq:    getHexSequences(1, 10, 3),
						},
					},
				},
				nil,
				[][]uint64{{10, 10, 10, 10, 10, 10, 10, 10, 10, 10}},
			},
			true,
		},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			gotEs, err := parseEndpointSet(0, testCase.arg)
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
