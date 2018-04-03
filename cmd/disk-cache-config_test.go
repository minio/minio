/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// Tests cache drive parsing.
func TestParseCacheDrives(t *testing.T) {
	testCases := []struct {
		driveStr         string
		expectedPatterns []string
		success          bool
	}{
		// valid input

		{"bucket1/*;*.png;images/trip/barcelona/*", []string{}, false},
		{"bucket1", []string{}, false},
	}
	if runtime.GOOS == globalWindowsOSName {
		testCases = append(testCases, struct {
			driveStr         string
			expectedPatterns []string
			success          bool
		}{"C:/home/drive1;C:/home/drive2;C:/home/drive3", []string{"C:/home/drive1", "C:/home/drive2", "C:/home/drive3"}, true})
	} else {
		testCases = append(testCases, struct {
			driveStr         string
			expectedPatterns []string
			success          bool
		}{"/home/drive1;/home/drive2;/home/drive3", []string{"/home/drive1", "/home/drive2", "/home/drive3"}, true})
	}
	for i, testCase := range testCases {
		drives, err := parseCacheDrives(strings.Split(testCase.driveStr, cacheEnvDelimiter))
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
		// valid input
		{"/home/drive1;/home/drive2;/home/drive3", []string{}, false},
		{"bucket1/*;*.png;images/trip/barcelona/*", []string{"bucket1/*", "*.png", "images/trip/barcelona/*"}, true},
		{"bucket1", []string{"bucket1"}, true},
	}

	for i, testCase := range testCases {
		excludes, err := parseCacheExcludes(strings.Split(testCase.excludeStr, cacheEnvDelimiter))
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
