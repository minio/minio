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

package net

import (
	"testing"
)

func TestPortString(t *testing.T) {
	testCases := []struct {
		port        Port
		expectedStr string
	}{
		{Port(0), "0"},
		{Port(9000), "9000"},
		{Port(65535), "65535"},
		{Port(1024), "1024"},
	}

	for i, testCase := range testCases {
		str := testCase.port.String()

		if str != testCase.expectedStr {
			t.Fatalf("test %v: error: port: %v, got: %v", i+1, testCase.expectedStr, str)
		}
	}
}

func TestParsePort(t *testing.T) {
	testCases := []struct {
		s            string
		expectedPort Port
		expectErr    bool
	}{
		{"0", Port(0), false},
		{"9000", Port(9000), false},
		{"65535", Port(65535), false},
		{"http", Port(80), false},
		{"https", Port(443), false},
		{"90000", Port(0), true},
		{"-10", Port(0), true},
		{"", Port(0), true},
		{" 1024", Port(0), true},
	}

	for i, testCase := range testCases {
		port, err := ParsePort(testCase.s)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if port != testCase.expectedPort {
				t.Fatalf("test %v: error: port: %v, got: %v", i+1, testCase.expectedPort, port)
			}
		}
	}
}
