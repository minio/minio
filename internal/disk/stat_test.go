//go:build linux && !s390x && !arm && !386
// +build linux,!s390x,!arm,!386

// Copyright (c) 2015-2024 MinIO, Inc.
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

package disk

import (
	"os"
	"reflect"
	"testing"
)

func TestReadDriveStats(t *testing.T) {
	testCases := []struct {
		stat            string
		expectedIOStats IOStats
		expectErr       bool
	}{
		{
			stat: "1432553   420084 66247626  2398227  7077314  8720147 157049224  7469810        0  7580552  9869354    46037        0 41695120     1315        0        0",
			expectedIOStats: IOStats{
				ReadIOs:        1432553,
				ReadMerges:     420084,
				ReadSectors:    66247626,
				ReadTicks:      2398227,
				WriteIOs:       7077314,
				WriteMerges:    8720147,
				WriteSectors:   157049224,
				WriteTicks:     7469810,
				CurrentIOs:     0,
				TotalTicks:     7580552,
				ReqTicks:       9869354,
				DiscardIOs:     46037,
				DiscardMerges:  0,
				DiscardSectors: 41695120,
				DiscardTicks:   1315,
				FlushIOs:       0,
				FlushTicks:     0,
			},
			expectErr: false,
		},
		{
			stat: "1432553   420084 66247626  2398227  7077314  8720147 157049224  7469810        0  7580552  9869354    46037        0 41695120     1315",
			expectedIOStats: IOStats{
				ReadIOs:        1432553,
				ReadMerges:     420084,
				ReadSectors:    66247626,
				ReadTicks:      2398227,
				WriteIOs:       7077314,
				WriteMerges:    8720147,
				WriteSectors:   157049224,
				WriteTicks:     7469810,
				CurrentIOs:     0,
				TotalTicks:     7580552,
				ReqTicks:       9869354,
				DiscardIOs:     46037,
				DiscardMerges:  0,
				DiscardSectors: 41695120,
				DiscardTicks:   1315,
			},
			expectErr: false,
		},
		{
			stat: "1432553   420084 66247626  2398227  7077314  8720147 157049224  7469810        0  7580552  9869354",
			expectedIOStats: IOStats{
				ReadIOs:      1432553,
				ReadMerges:   420084,
				ReadSectors:  66247626,
				ReadTicks:    2398227,
				WriteIOs:     7077314,
				WriteMerges:  8720147,
				WriteSectors: 157049224,
				WriteTicks:   7469810,
				CurrentIOs:   0,
				TotalTicks:   7580552,
				ReqTicks:     9869354,
			},
			expectErr: false,
		},
		{
			stat:            "1432553   420084 66247626  2398227",
			expectedIOStats: IOStats{},
			expectErr:       true,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run("", func(t *testing.T) {
			tmpfile, err := os.CreateTemp(t.TempDir(), "testfile")
			if err != nil {
				t.Error(err)
			}
			tmpfile.WriteString(testCase.stat)
			tmpfile.Sync()
			tmpfile.Close()

			iostats, err := readDriveStats(tmpfile.Name())
			if err != nil && !testCase.expectErr {
				t.Fatalf("unexpected err; %v", err)
			}
			if testCase.expectErr && err == nil {
				t.Fatal("expected to fail but err is nil")
			}
			if !reflect.DeepEqual(iostats, testCase.expectedIOStats) {
				t.Fatalf("expected iostats: %v but got %v", testCase.expectedIOStats, iostats)
			}
		})
	}
}
