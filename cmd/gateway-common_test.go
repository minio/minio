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
	"reflect"
	"testing"
)

// Tests cache exclude parsing.
func TestParseGatewaySSE(t *testing.T) {
	testCases := []struct {
		gwSSEStr string
		expected gatewaySSE
		success  bool
	}{
		// valid input
		{"c;S3", []string{"C", "S3"}, true},
		{"S3", []string{"S3"}, true},
		{"c,S3", []string{}, false},
		{"c;S3;KMS", []string{}, false},
		{"C;s3", []string{"C", "S3"}, true},
	}

	for i, testCase := range testCases {
		gwSSE, err := parseGatewaySSE(testCase.gwSSEStr)
		if err != nil && testCase.success {
			t.Errorf("Test %d: Expected success but failed instead %s", i+1, err)
		}
		if err == nil && !testCase.success {
			t.Errorf("Test %d: Expected failure but passed instead", i+1)
		}
		if err == nil {
			if !reflect.DeepEqual(gwSSE, testCase.expected) {
				t.Errorf("Test %d: Expected %v, got %v", i+1, testCase.expected, gwSSE)
			}
		}
	}
}
