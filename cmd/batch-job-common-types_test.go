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
)

func TestBatchJobSizeInRange(t *testing.T) {
	tests := []struct {
		objSize    int64
		sizeFilter BatchJobSizeFilter
		want       bool
	}{
		{
			// 1Mib < 2Mib < 10MiB -> in range
			objSize: 2 << 20,
			sizeFilter: BatchJobSizeFilter{
				UpperBound: 10 << 20,
				LowerBound: 1 << 20,
			},
			want: true,
		},
		{
			// 2KiB < 1 MiB -> out of range from left
			objSize: 2 << 10,
			sizeFilter: BatchJobSizeFilter{
				UpperBound: 10 << 20,
				LowerBound: 1 << 20,
			},
			want: false,
		},
		{
			// 11MiB > 10 MiB -> out of range from right
			objSize: 11 << 20,
			sizeFilter: BatchJobSizeFilter{
				UpperBound: 10 << 20,
				LowerBound: 1 << 20,
			},
			want: false,
		},
		{
			//  2MiB < 10MiB -> in range
			objSize: 2 << 20,
			sizeFilter: BatchJobSizeFilter{
				UpperBound: 10 << 20,
			},
			want: true,
		},
		{
			//  2MiB > 1MiB -> in range
			objSize: 2 << 20,
			sizeFilter: BatchJobSizeFilter{
				LowerBound: 1 << 20,
			},
			want: true,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i+1), func(t *testing.T) {
			if got := test.sizeFilter.InRange(test.objSize); got != test.want {
				t.Fatalf("Expected %v but got %v", test.want, got)
			}
		})
	}
}

func TestBatchJobSizeValidate(t *testing.T) {
	errInvalidBatchJobSizeFilter := BatchJobYamlErr{
		msg: "invalid batch-job size filter",
	}

	tests := []struct {
		sizeFilter BatchJobSizeFilter
		err        error
	}{
		{
			// Unspecified size filter is a valid filter
			sizeFilter: BatchJobSizeFilter{
				UpperBound: 0,
				LowerBound: 0,
			},
			err: nil,
		},
		{
			sizeFilter: BatchJobSizeFilter{
				UpperBound: 0,
				LowerBound: 1 << 20,
			},
			err: nil,
		},
		{
			sizeFilter: BatchJobSizeFilter{
				UpperBound: 10 << 20,
				LowerBound: 0,
			},
			err: nil,
		},
		{
			// LowerBound > UpperBound -> empty range
			sizeFilter: BatchJobSizeFilter{
				UpperBound: 1 << 20,
				LowerBound: 10 << 20,
			},
			err: errInvalidBatchJobSizeFilter,
		},
		{
			// LowerBound == UpperBound -> empty range
			sizeFilter: BatchJobSizeFilter{
				UpperBound: 1 << 20,
				LowerBound: 1 << 20,
			},
			err: errInvalidBatchJobSizeFilter,
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i+1), func(t *testing.T) {
			err := test.sizeFilter.Validate()
			if err != nil {
				gotErr := err.(BatchJobYamlErr)
				testErr := test.err.(BatchJobYamlErr)
				if gotErr.message() != testErr.message() {
					t.Fatalf("Expected %v but got %v", test.err, err)
				}
			}
			if err == nil && test.err != nil {
				t.Fatalf("Expected %v but got nil", test.err)
			}
		})
	}
}
