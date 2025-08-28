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

package compress

import (
	"reflect"
	"testing"
)

func TestParseCompressIncludes(t *testing.T) {
	testCases := []struct {
		str              string
		expectedPatterns []string
		success          bool
	}{
		// invalid input
		{",,,", []string{}, false},
		{"", []string{}, false},
		{",", []string{}, false},
		{"/", []string{}, false},
		{"text/*,/", []string{}, false},

		// valid input
		{".txt,.log", []string{".txt", ".log"}, true},
		{"text/*,application/json", []string{"text/*", "application/json"}, true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.str, func(t *testing.T) {
			gotPatterns, err := parseCompressIncludes(testCase.str)
			if !testCase.success && err == nil {
				t.Error("expected failure but success instead")
			}
			if testCase.success && err != nil {
				t.Errorf("expected success but failed instead %s", err)
			}
			if testCase.success && !reflect.DeepEqual(testCase.expectedPatterns, gotPatterns) {
				t.Errorf("expected patterns %s but got %s", testCase.expectedPatterns, gotPatterns)
			}
		})
	}
}
