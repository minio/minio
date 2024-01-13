// Copyright (c) 2015-2023 MinIO, Inc.
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
	"testing"

	"github.com/dustin/go-humanize"
)

func TestSizeHistogramToMap(t *testing.T) {
	tests := []struct {
		sizes []int64
		want  map[string]uint64
	}{
		{
			sizes: []int64{100, 1000, 72_000, 100_000},
			want: map[string]uint64{
				"LESS_THAN_1024_B":         2,
				"BETWEEN_64_KB_AND_256_KB": 2,
				"BETWEEN_1024B_AND_1_MB":   2,
			},
		},
		{
			sizes: []int64{100, 1000, 2000, 100_000, 13 * humanize.MiByte},
			want: map[string]uint64{
				"LESS_THAN_1024_B":         2,
				"BETWEEN_1024_B_AND_64_KB": 1,
				"BETWEEN_64_KB_AND_256_KB": 1,
				"BETWEEN_1024B_AND_1_MB":   2,
				"BETWEEN_10_MB_AND_64_MB":  1,
			},
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("Test-%d", i), func(t *testing.T) {
			var h sizeHistogram
			for _, sz := range test.sizes {
				h.add(sz)
			}
			got := h.toMap()
			exp := test.want
			// what is in exp is in got
			for k := range exp {
				if exp[k] != got[k] {
					t.Fatalf("interval %s: Expected %d values but got %d values\n", k, exp[k], got[k])
				}
			}
			// what is absent in exp is absent in got too
			for k := range got {
				if _, ok := exp[k]; !ok && got[k] > 0 {
					t.Fatalf("Unexpected interval: %s has value %d\n", k, got[k])
				}
			}
		})
	}
}

func TestMigrateSizeHistogramFromV1(t *testing.T) {
	tests := []struct {
		v    sizeHistogramV1
		want sizeHistogram
	}{
		{
			v:    sizeHistogramV1{0: 10, 1: 20, 2: 3},
			want: sizeHistogram{0: 10, 5: 20, 6: 3},
		},
		{
			v:    sizeHistogramV1{0: 10, 1: 20, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7},
			want: sizeHistogram{0: 10, 5: 20, 6: 3, 7: 4, 8: 5, 9: 6, 10: 7},
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			var got sizeHistogram
			got.mergeV1(test.v)
			if got != test.want {
				t.Fatalf("Expected %v but got %v", test.want, got)
			}
		})
	}
}
