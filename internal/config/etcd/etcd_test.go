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

package etcd

import (
	"reflect"
	"testing"
)

// TestParseEndpoints - tests parseEndpoints function with valid and invalid inputs.
func TestParseEndpoints(t *testing.T) {
	testCases := []struct {
		s         string
		endpoints []string
		secure    bool
		success   bool
	}{
		// Invalid inputs
		{"https://localhost:2379,http://localhost:2380", nil, false, false},
		{",,,", nil, false, false},
		{"", nil, false, false},
		{"ftp://localhost:2379", nil, false, false},
		{"http://localhost:2379000", nil, false, false},

		// Valid inputs
		{
			"https://localhost:2379,https://localhost:2380",
			[]string{
				"https://localhost:2379", "https://localhost:2380",
			},
			true, true,
		},
		{"http://localhost:2379", []string{"http://localhost:2379"}, false, true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.s, func(t *testing.T) {
			endpoints, secure, err := parseEndpoints(testCase.s)
			if err != nil && testCase.success {
				t.Errorf("expected to succeed but failed with %s", err)
			}
			if !testCase.success && err == nil {
				t.Error("expected failure but succeeded instead")
			}
			if testCase.success {
				if !reflect.DeepEqual(endpoints, testCase.endpoints) {
					t.Errorf("expected %s, got %s", testCase.endpoints, endpoints)
				}
				if secure != testCase.secure {
					t.Errorf("expected %t, got %t", testCase.secure, secure)
				}
			}
		})
	}
}
