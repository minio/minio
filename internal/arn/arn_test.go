// Copyright (c) 2015-2023 MinIO, Inc.
//
// # This file is part of MinIO Object Storage stack
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

package arn

import (
	"fmt"
	"testing"
)

func TestNewIAMRoleARN(t *testing.T) {
	testCases := []struct {
		resourceID    string
		serverRegion  string
		expectedARN   string
		isErrExpected bool
	}{
		{
			resourceID:    "myrole",
			serverRegion:  "us-east-1",
			expectedARN:   "arn:minio:iam:us-east-1::role/myrole",
			isErrExpected: false,
		},
		{
			resourceID:    "myrole",
			serverRegion:  "",
			expectedARN:   "arn:minio:iam:::role/myrole",
			isErrExpected: false,
		},
		{
			// Resource ID can start with a hyphen
			resourceID:    "-myrole",
			serverRegion:  "",
			expectedARN:   "arn:minio:iam:::role/-myrole",
			isErrExpected: false,
		},
		{
			resourceID:    "",
			serverRegion:  "",
			expectedARN:   "",
			isErrExpected: true,
		},
	}
	for i, testCase := range testCases {
		arn, err := NewIAMRoleARN(testCase.resourceID, testCase.serverRegion)
		fmt.Println(arn, err)
		if err != nil {
			if !testCase.isErrExpected {
				t.Errorf("Test %d: Unexpected error: %v", i+1, err)
			}
			continue
		}

		if testCase.isErrExpected {
			t.Errorf("Test %d: Expected error but got none", i+1)
		}
		if arn.String() != testCase.expectedARN {
			t.Errorf("Test %d: Expected ARN %s but got %s", i+1, testCase.expectedARN, arn.String())
		}

	}
}
