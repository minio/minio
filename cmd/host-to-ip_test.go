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
	"fmt"
	"testing"
)

// Tests sorted list generated from hosts to ip.
func TestHostToIP(t *testing.T) {
	// Collection of test cases to validate last octet sorting.
	testCases := []struct {
		ips        []string
		sortedIPs  []string
		err        error
		shouldPass bool
	}{
		{
			// List of ip addresses that need to be sorted.
			ips: []string{
				"129.95.30.40/24",
				"5.24.69.2/24",
				"19.20.203.5/24",
				"1.2.3.4/24",
				"127.0.0.1/24",
				"19.20.21.22/24",
				"5.220.100.50/24",
			},
			// Numerical sorting result based on the last octet.
			sortedIPs: []string{
				"5.220.100.50",
				"129.95.30.40",
				"19.20.21.22",
				"19.20.203.5",
				"1.2.3.4",
				"5.24.69.2",
				"127.0.0.1",
			},
			err:        nil,
			shouldPass: true,
		},
		{
			ips: []string{
				"localhost",
			},
			sortedIPs:  []string{},
			err:        fmt.Errorf("Unable to parse invalid ip localhost"),
			shouldPass: false,
		},
	}

	// Tests the correct sorting behavior of getIPsFromHosts.
	for j, testCase := range testCases {
		err := sortIPsByOctet(testCase.ips)
		if !testCase.shouldPass && testCase.err.Error() != err.Error() {
			t.Fatalf("Test %d: Expected error %s, got %s", j+1, testCase.err, err)
		}
		if testCase.shouldPass && err != nil {
			t.Fatalf("Test %d: Expected error %s", j+1, err)
		}
		if testCase.shouldPass {
			for i, ip := range testCase.ips {
				if ip == testCase.sortedIPs[i] {
					continue
				}
				t.Errorf("Test %d expected to pass but failed. Wanted ip %s, but got %s", j+1, testCase.sortedIPs[i], ip)
			}
		}
	}
}
