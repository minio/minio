/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"net/url"
	"reflect"
	"sort"
	"testing"
)

func sortIsEndpointsSorted(endpoints []*url.URL) bool {
	return sort.SliceIsSorted(endpoints, func(i, j int) bool {
		return (endpoints[i].Host + endpoints[i].Path) <
			(endpoints[j].Host + endpoints[j].Path)
	})
}

// TestSortEndpoints - tests if ordering of urls are based on
// host+path concatenated.
func TestSortEndpoints(t *testing.T) {
	testCases := []struct {
		given    []string
		expected []*url.URL
	}{
		{
			given: []string{
				"http://abcd.com/a/b/d",
				"http://abcd.com/a/b/c",
				"http://abcd.com/a/b/e",
			},
			expected: []*url.URL{
				{
					Scheme: httpScheme,
					Host:   "abcd.com:9000",
					Path:   "/a/b/c",
				},
				{
					Scheme: httpScheme,
					Host:   "abcd.com:9000",
					Path:   "/a/b/d",
				},
				{
					Scheme: httpScheme,
					Host:   "abcd.com:9000",
					Path:   "/a/b/e",
				},
			},
		},
		{
			given: []string{
				"http://defg.com/a/b/c",
				"http://abcd.com/a/b/c",
				"http://hijk.com/a/b/c",
			},
			expected: []*url.URL{
				{
					Scheme: httpScheme,
					Host:   "abcd.com:9000",
					Path:   "/a/b/c",
				},
				{
					Scheme: httpScheme,
					Host:   "defg.com:9000",
					Path:   "/a/b/c",
				},
				{
					Scheme: httpScheme,
					Host:   "hijk.com:9000",
					Path:   "/a/b/c",
				},
			},
		},
	}

	saveGlobalPort := globalMinioPort
	globalMinioPort = "9000"
	for i, test := range testCases {
		eps, err := parseStorageEndpoints(test.given)
		if err != nil {
			t.Fatalf("Test %d - Failed to parse storage endpoint %v", i+1, err)
		}
		sortEndpoints(eps)
		if !sortIsEndpointsSorted(eps) {
			t.Errorf("Test %d - Expected order %v but got %v", i+1, test.expected, eps)
		}
		if !reflect.DeepEqual(eps, test.expected) {
			t.Errorf("Test %d - Expected order %v but got %v", i+1, test.expected, eps)
		}
	}
	globalMinioPort = saveGlobalPort
}
