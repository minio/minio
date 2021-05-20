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

package cache

import (
	"reflect"
	"runtime"
	"testing"
)

// Tests cache drive parsing.
func TestParseCacheDrives(t *testing.T) {
	testCases := []struct {
		driveStr         string
		expectedPatterns []string
		success          bool
	}{
		// Invalid input

		{"bucket1/*;*.png;images/trip/barcelona/*", []string{}, false},
		{"bucket1", []string{}, false},
		{";;;", []string{}, false},
		{",;,;,;", []string{}, false},
	}

	// Valid inputs
	if runtime.GOOS == "windows" {
		testCases = append(testCases, struct {
			driveStr         string
			expectedPatterns []string
			success          bool
		}{"C:/home/drive1;C:/home/drive2;C:/home/drive3", []string{"C:/home/drive1", "C:/home/drive2", "C:/home/drive3"}, true})
		testCases = append(testCases, struct {
			driveStr         string
			expectedPatterns []string
			success          bool
		}{"C:/home/drive{1...3}", []string{"C:/home/drive1", "C:/home/drive2", "C:/home/drive3"}, true})
		testCases = append(testCases, struct {
			driveStr         string
			expectedPatterns []string
			success          bool
		}{"C:/home/drive{1..3}", []string{}, false})
	} else {
		testCases = append(testCases, struct {
			driveStr         string
			expectedPatterns []string
			success          bool
		}{"/home/drive1;/home/drive2;/home/drive3", []string{"/home/drive1", "/home/drive2", "/home/drive3"}, true})
		testCases = append(testCases, struct {
			driveStr         string
			expectedPatterns []string
			success          bool
		}{"/home/drive1,/home/drive2,/home/drive3", []string{"/home/drive1", "/home/drive2", "/home/drive3"}, true})
		testCases = append(testCases, struct {
			driveStr         string
			expectedPatterns []string
			success          bool
		}{"/home/drive{1...3}", []string{"/home/drive1", "/home/drive2", "/home/drive3"}, true})
		testCases = append(testCases, struct {
			driveStr         string
			expectedPatterns []string
			success          bool
		}{"/home/drive{1..3}", []string{}, false})
	}
	for i, testCase := range testCases {
		drives, err := parseCacheDrives(testCase.driveStr)
		if err != nil && testCase.success {
			t.Errorf("Test %d: Expected success but failed instead %s", i+1, err)
		}
		if err == nil && !testCase.success {
			t.Errorf("Test %d: Expected failure but passed instead", i+1)
		}
		if err == nil {
			if !reflect.DeepEqual(drives, testCase.expectedPatterns) {
				t.Errorf("Test %d: Expected %v, got %v", i+1, testCase.expectedPatterns, drives)
			}
		}
	}
}

// Tests cache exclude parsing.
func TestParseCacheExclude(t *testing.T) {
	testCases := []struct {
		excludeStr       string
		expectedPatterns []string
		success          bool
	}{
		// Invalid input
		{"/home/drive1;/home/drive2;/home/drive3", []string{}, false},
		{"/", []string{}, false},
		{";;;", []string{}, false},

		// valid input
		{"bucket1/*;*.png;images/trip/barcelona/*", []string{"bucket1/*", "*.png", "images/trip/barcelona/*"}, true},
		{"bucket1/*,*.png,images/trip/barcelona/*", []string{"bucket1/*", "*.png", "images/trip/barcelona/*"}, true},
		{"bucket1", []string{"bucket1"}, true},
	}

	for i, testCase := range testCases {
		excludes, err := parseCacheExcludes(testCase.excludeStr)
		if err != nil && testCase.success {
			t.Errorf("Test %d: Expected success but failed instead %s", i+1, err)
		}
		if err == nil && !testCase.success {
			t.Errorf("Test %d: Expected failure but passed instead", i+1)
		}
		if err == nil {
			if !reflect.DeepEqual(excludes, testCase.expectedPatterns) {
				t.Errorf("Test %d: Expected %v, got %v", i+1, testCase.expectedPatterns, excludes)
			}
		}
	}
}
