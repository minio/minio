// +build windows

/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"net"
	"testing"
)

// Test for splitNetPath
func TestSplitNetPath(t *testing.T) {
	testCases := []struct {
		networkPath string
		netAddr     string
		netPath     string
		err         error
	}{
		// Invalid cases 1-8.
		{":C:", "", "", &net.AddrError{Err: "Missing address in network path", Addr: ":C:"}},
		{"10.1.10.1:", "", "", &net.AddrError{Err: "Missing path in network path", Addr: "10.1.10.1:"}},
		{"10.1.10.1:C", "", "", &net.AddrError{Err: "Network path should be absolute", Addr: "10.1.10.1:C"}},
		{"10.1.10.1:C:", "", "", &net.AddrError{Err: "Network path should be absolute", Addr: "10.1.10.1:C:"}},
		{"10.1.10.1:C:../path", "", "", &net.AddrError{Err: "Network path should be absolute", Addr: "10.1.10.1:C:../path"}},
		{"10.1.10.1:C:tmp/1", "", "", &net.AddrError{Err: "Network path should be absolute", Addr: "10.1.10.1:C:tmp/1"}},
		{"10.1.10.1::C:\\path\\test", "", "", &net.AddrError{
			Err:  "Network path should be absolute",
			Addr: "10.1.10.1::C:\\path\\test",
		}},
		{"10.1.10.1:\\path\\test", "", "", &net.AddrError{
			Err:  "Network path should be absolute",
			Addr: "10.1.10.1:\\path\\test",
		}},

		// Valid cases 9-11.
		{"10.1.10.1:C:\\path\\test", "10.1.10.1", "C:\\path\\test", nil},
		{"C:\\path\\test", "", "C:\\path\\test", nil},
		{`10.1.10.1:\\?\UNC\path\test`, "10.1.10.1", `\\?\UNC\path\test`, nil},
	}

	for i, test := range testCases {
		receivedAddr, receivedPath, receivedErr := splitNetPath(test.networkPath)
		if receivedAddr != test.netAddr {
			t.Errorf("Test case %d: Expected: %s, Received: %s", i+1, test.netAddr, receivedAddr)
		}
		if receivedPath != test.netPath {
			t.Errorf("Test case %d: Expected: %s, Received: %s", i+1, test.netPath, receivedPath)
		}
		if test.err != nil {
			if receivedErr == nil || receivedErr.Error() != test.err.Error() {
				t.Errorf("Test case %d: Expected: %v, Received: %v", i+1, test.err, receivedErr)
			}
		}
	}
}
