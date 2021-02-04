/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
		{"https://localhost:2379,https://localhost:2380", []string{
			"https://localhost:2379", "https://localhost:2380"},
			true, true},
		{"http://localhost:2379", []string{"http://localhost:2379"}, false, true},
	}

	for _, testCase := range testCases {
		testCase := testCase
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
