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

package main

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
		{"10.1.10.1:", "", "", &net.AddrError{Err: "missing path in network path", Addr: "10.1.10.1:"}},
		{"10.1.10.1", "", "10.1.10.1", nil},
		{"10.1.10.1://", "10.1.10.1", "//", nil},
		{"10.1.10.1:/disk/1", "10.1.10.1", "/disk/1", nil},
		{"10.1.10.1:\\path\\test", "10.1.10.1", "\\path\\test", nil},
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

// Tests maximum object size.
func TestMaxObjectSize(t *testing.T) {
	sizes := []struct {
		isMax bool
		size  int64
	}{
		// Test - 1 - maximum object size.
		{
			true,
			maxObjectSize + 1,
		},
		// Test - 2 - not maximum object size.
		{
			false,
			maxObjectSize - 1,
		},
	}
	for i, s := range sizes {
		isMax := isMaxObjectSize(s.size)
		if isMax != s.isMax {
			t.Errorf("Test %d: Expected %t, got %t", i+1, s.isMax, isMax)
		}
	}
}

// Tests minimum allowed part size.
func TestMinAllowedPartSize(t *testing.T) {
	sizes := []struct {
		isMin bool
		size  int64
	}{
		// Test - 1 - within minimum part size.
		{
			true,
			minPartSize + 1,
		},
		// Test - 2 - smaller than minimum part size.
		{
			false,
			minPartSize - 1,
		},
	}

	for i, s := range sizes {
		isMin := isMinAllowedPartSize(s.size)
		if isMin != s.isMin {
			t.Errorf("Test %d: Expected %t, got %t", i+1, s.isMin, isMin)
		}
	}
}

// Tests maximum allowed part number.
func TestMaxPartID(t *testing.T) {
	sizes := []struct {
		isMax bool
		partN int
	}{
		// Test - 1 part number within max part number.
		{
			false,
			maxPartID - 1,
		},
		// Test - 2 part number bigger than max part number.
		{
			true,
			maxPartID + 1,
		},
	}

	for i, s := range sizes {
		isMax := isMaxPartID(s.partN)
		if isMax != s.isMax {
			t.Errorf("Test %d: Expected %t, got %t", i+1, s.isMax, isMax)
		}
	}
}
