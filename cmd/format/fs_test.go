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

package format

import (
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestParseFS(t *testing.T) {
	case1Reader := strings.NewReader(`
{
    "version": "1",
    "format": "fs",
    "fs": {
        "version": "2"
    }
}`)

	case2Reader := strings.NewReader(`
{
    "version": "1",
    "format": "fs",
    "fs": {
        "version": "1"
    }
}`)

	case3Reader := strings.NewReader("foo")

	case4Reader := strings.NewReader(`
{
    "version": "2",
    "format": "fs",
    "fs": {
        "version": "2"
    }
}`)

	case5Reader := strings.NewReader(`
{
    "version": "1",
    "format": "fs",
    "fs": {
        "version": "3"
    }
}`)

	testCases := []struct {
		reader           io.Reader
		expectedResult   *FSV2
		expectedMigrated bool
		expectErr        bool
	}{
		{case1Reader, NewFSV2(), false, false},
		{case2Reader, NewFSV2(), true, false},
		{case3Reader, nil, false, true},
		{case4Reader, nil, false, true},
		{case5Reader, nil, false, true},
	}

	for i, testCase := range testCases {
		result, migrated, err := ParseFS(testCase.reader)
		expectErr := err != nil

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %+v, got: %+v", i+1, testCase.expectedResult, result)
			}
			if migrated != testCase.expectedMigrated {
				t.Fatalf("case %v: migrated: expected: %+v, got: %+v", i+1, testCase.expectedMigrated, migrated)
			}
		}
	}
}
