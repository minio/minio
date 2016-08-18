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

import "testing"

// Tests sorted list generated from hosts to ip.
func TestHostToIP(t *testing.T) {
	// Collection of test cases to validate last octet sorting.
	testCases := []struct {
		hosts       []string
		sortedHosts []string
	}{
		{
			// List of ip addresses that need to be sorted.
			[]string{
				"129.95.30.40",
				"5.24.69.2",
				"19.20.203.5",
				"1.2.3.4",
				"127.0.0.1",
				"19.20.21.22",
				"5.220.100.50",
			},
			// Numerical sorting result based on the last octet.
			[]string{
				"5.220.100.50",
				"129.95.30.40",
				"19.20.21.22",
				"19.20.203.5",
				"1.2.3.4",
				"5.24.69.2",
				"127.0.0.1",
			},
		},
	}

	// Tests the correct sorting behavior of getIPsFromHosts.
	for j, testCase := range testCases {
		ips := getIPsFromHosts(testCase.hosts)
		for i, ip := range ips {
			if ip.String() != testCase.sortedHosts[i] {
				t.Fatalf("Test %d expected to pass but failed. Wanted ip %s, but got %s", j+1, testCase.sortedHosts[i], ip.String())
			}
		}
	}
}
