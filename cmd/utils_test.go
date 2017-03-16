/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"net"
	"net/http"
	"net/url"
	"reflect"
	"runtime"
	"testing"
)

// Tests http.Header clone.
func TestCloneHeader(t *testing.T) {
	headers := []http.Header{
		{
			"Content-Type":   {"text/html; charset=UTF-8"},
			"Content-Length": {"0"},
		},
		{
			"Content-Length": {"0", "1", "2"},
		},
		{
			"Expires":          {"-1"},
			"Content-Length":   {"0"},
			"Content-Encoding": {"gzip"},
		},
	}
	for i, header := range headers {
		clonedHeader := cloneHeader(header)
		if !reflect.DeepEqual(header, clonedHeader) {
			t.Errorf("Test %d failed", i+1)
		}
	}
}

// Tests check duplicates function.
func TestCheckDuplicates(t *testing.T) {
	tests := []struct {
		list       []string
		err        error
		shouldPass bool
	}{
		// Test 1 - for '/tmp/1' repeated twice.
		{
			list:       []string{"/tmp/1", "/tmp/1", "/tmp/2", "/tmp/3"},
			err:        fmt.Errorf("Duplicate key: \"/tmp/1\" found of count: \"2\""),
			shouldPass: false,
		},
		// Test 2 - for '/tmp/1' repeated thrice.
		{
			list:       []string{"/tmp/1", "/tmp/1", "/tmp/1", "/tmp/3"},
			err:        fmt.Errorf("Duplicate key: \"/tmp/1\" found of count: \"3\""),
			shouldPass: false,
		},
		// Test 3 - empty string.
		{
			list:       []string{""},
			err:        errInvalidArgument,
			shouldPass: false,
		},
		// Test 4 - empty string.
		{
			list:       nil,
			err:        errInvalidArgument,
			shouldPass: false,
		},
		// Test 5 - non repeated strings.
		{
			list:       []string{"/tmp/1", "/tmp/2", "/tmp/3"},
			err:        nil,
			shouldPass: true,
		},
	}

	// Validate if function runs as expected.
	for i, test := range tests {
		err := checkDuplicateStrings(test.list)
		if test.shouldPass && err != test.err {
			t.Errorf("Test: %d, Expected %s got %s", i+1, test.err, err)
		}
		if !test.shouldPass && err.Error() != test.err.Error() {
			t.Errorf("Test: %d, Expected %s got %s", i+1, test.err, err)
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
			globalMaxObjectSize + 1,
		},
		// Test - 2 - not maximum object size.
		{
			false,
			globalMaxObjectSize - 1,
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
			globalMinPartSize + 1,
		},
		// Test - 2 - smaller than minimum part size.
		{
			false,
			globalMinPartSize - 1,
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
			globalMaxPartID - 1,
		},
		// Test - 2 part number bigger than max part number.
		{
			true,
			globalMaxPartID + 1,
		},
	}

	for i, s := range sizes {
		isMax := isMaxPartID(s.partN)
		if isMax != s.isMax {
			t.Errorf("Test %d: Expected %t, got %t", i+1, s.isMax, isMax)
		}
	}
}

// Tests extracting bucket and objectname from various types of URL paths.
func TestURL2BucketObjectName(t *testing.T) {
	testCases := []struct {
		u              *url.URL
		bucket, object string
	}{
		// Test case 1 normal case.
		{
			u: &url.URL{
				Path: "/bucket/object",
			},
			bucket: "bucket",
			object: "object",
		},
		// Test case 2 where url only has separator.
		{
			u: &url.URL{
				Path: "/",
			},
			bucket: "",
			object: "",
		},
		// Test case 3 only bucket is present.
		{
			u: &url.URL{
				Path: "/bucket",
			},
			bucket: "bucket",
			object: "",
		},
		// Test case 4 many separators and object is a directory.
		{
			u: &url.URL{
				Path: "/bucket/object/1/",
			},
			bucket: "bucket",
			object: "object/1/",
		},
		// Test case 5 object has many trailing separators.
		{
			u: &url.URL{
				Path: "/bucket/object/1///",
			},
			bucket: "bucket",
			object: "object/1///",
		},
		// Test case 6 object has only trailing separators.
		{
			u: &url.URL{
				Path: "/bucket/object///////",
			},
			bucket: "bucket",
			object: "object///////",
		},
		// Test case 7 object has preceding separators.
		{
			u: &url.URL{
				Path: "/bucket////object////",
			},
			bucket: "bucket",
			object: "///object////",
		},
		// Test case 8 url is not allocated.
		{
			u:      nil,
			bucket: "",
			object: "",
		},
		// Test case 9 url path is empty.
		{
			u:      &url.URL{},
			bucket: "",
			object: "",
		},
	}

	// Validate all test cases.
	for i, testCase := range testCases {
		bucketName, objectName := urlPath2BucketObjectName(testCase.u)
		if bucketName != testCase.bucket {
			t.Errorf("Test %d: failed expected bucket name \"%s\", got \"%s\"", i+1, testCase.bucket, bucketName)
		}
		if objectName != testCase.object {
			t.Errorf("Test %d: failed expected bucket name \"%s\", got \"%s\"", i+1, testCase.object, objectName)
		}
	}
}

// Add tests for starting and stopping different profilers.
func TestStartProfiler(t *testing.T) {
	if startProfiler("") != nil {
		t.Fatal("Expected nil, but non-nil value returned for invalid profiler.")
	}
}

// Tests fetch local address.
func TestLocalAddress(t *testing.T) {
	if runtime.GOOS == globalWindowsOSName {
		return
	}

	currentIsDistXL := globalIsDistXL
	defer func() {
		globalIsDistXL = currentIsDistXL
	}()

	// need to set this to avoid stale values from other tests.
	globalMinioPort = "9000"
	globalMinioHost = ""
	testCases := []struct {
		isDistXL     bool
		srvCmdConfig serverCmdConfig
		localAddr    string
	}{
		// Test 1 - local address is found.
		{
			isDistXL: true,
			srvCmdConfig: serverCmdConfig{
				endpoints: []*url.URL{{
					Scheme: httpScheme,
					Host:   "localhost:9000",
					Path:   "/mnt/disk1",
				}, {
					Scheme: httpScheme,
					Host:   "1.1.1.2:9000",
					Path:   "/mnt/disk2",
				}, {
					Scheme: httpScheme,
					Host:   "1.1.2.1:9000",
					Path:   "/mnt/disk3",
				}, {
					Scheme: httpScheme,
					Host:   "1.1.2.2:9000",
					Path:   "/mnt/disk4",
				}},
			},
			localAddr: net.JoinHostPort("localhost", globalMinioPort),
		},
		// Test 2 - local address is everything.
		{
			isDistXL: false,
			srvCmdConfig: serverCmdConfig{
				serverAddr: net.JoinHostPort("", globalMinioPort),
				endpoints: []*url.URL{{
					Path: "/mnt/disk1",
				}, {
					Path: "/mnt/disk2",
				}, {
					Path: "/mnt/disk3",
				}, {
					Path: "/mnt/disk4",
				}},
			},
			localAddr: net.JoinHostPort("", globalMinioPort),
		},
		// Test 3 - local address is not found.
		{
			isDistXL: true,
			srvCmdConfig: serverCmdConfig{
				endpoints: []*url.URL{{
					Scheme: httpScheme,
					Host:   "1.1.1.1:9000",
					Path:   "/mnt/disk2",
				}, {
					Scheme: httpScheme,
					Host:   "1.1.1.2:9000",
					Path:   "/mnt/disk2",
				}, {
					Scheme: httpScheme,
					Host:   "1.1.2.1:9000",
					Path:   "/mnt/disk3",
				}, {
					Scheme: httpScheme,
					Host:   "1.1.2.2:9000",
					Path:   "/mnt/disk4",
				}},
			},
			localAddr: "",
		},
		// Test 4 - in case of FS mode, with SSL, the host
		// name is specified in the --address option on the
		// server command line.
		{
			isDistXL: false,
			srvCmdConfig: serverCmdConfig{
				serverAddr: "play.minio.io:9000",
				endpoints: []*url.URL{{
					Path: "/mnt/disk1",
				}, {
					Path: "/mnt/disk2",
				}, {
					Path: "/mnt/disk3",
				}, {
					Path: "/mnt/disk4",
				}},
			},
			localAddr: "play.minio.io:9000",
		},
	}

	// Validates fetching local address.
	for i, testCase := range testCases {
		globalIsDistXL = testCase.isDistXL
		localAddr := getLocalAddress(testCase.srvCmdConfig)
		if localAddr != testCase.localAddr {
			t.Fatalf("Test %d: Expected %s, got %s", i+1, testCase.localAddr, localAddr)
		}
	}

}

// TestCheckURL tests valid address
func TestCheckURL(t *testing.T) {
	testCases := []struct {
		addr       string
		shouldPass bool
	}{
		{"", false},
		{":", false},
		{"localhost", true},
		{"127.0.0.1", true},
		{"http://localhost/", true},
		{"http://127.0.0.1/", true},
		{"proto://myhostname/path", true},
	}

	// Validates fetching local address.
	for i, testCase := range testCases {
		_, err := checkURL(testCase.addr)
		if testCase.shouldPass && err != nil {
			t.Errorf("Test %d: expected to pass but got an error: %v\n", i+1, err)
		}
		if !testCase.shouldPass && err == nil {
			t.Errorf("Test %d: expected to fail but passed.", i+1)
		}
	}
}
