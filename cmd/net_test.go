/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"runtime"
	"testing"

	"github.com/minio/minio-go/pkg/set"
)

func TestMustSplitHostPort(t *testing.T) {
	testCases := []struct {
		hostPort     string
		expectedHost string
		expectedPort string
	}{
		{":54321", "", "54321"},
		{"server:54321", "server", "54321"},
		{":", "", ""},
		{":0", "", "0"},
		{":-10", "", "-10"},
		{"server:100000000", "server", "100000000"},
		{"server:https", "server", "https"},
	}

	for _, testCase := range testCases {
		host, port := mustSplitHostPort(testCase.hostPort)
		if testCase.expectedHost != host {
			t.Fatalf("host: expected = %v, got = %v", testCase.expectedHost, host)
		}

		if testCase.expectedPort != port {
			t.Fatalf("port: expected = %v, got = %v", testCase.expectedPort, port)
		}
	}
}

func TestMustGetLocalIP4(t *testing.T) {
	testCases := []struct {
		expectedIPList set.StringSet
	}{
		{set.CreateStringSet("127.0.0.1")},
	}

	for _, testCase := range testCases {
		ipList := mustGetLocalIP4()
		if testCase.expectedIPList != nil && testCase.expectedIPList.Intersection(ipList).IsEmpty() {
			t.Fatalf("host: expected = %v, got = %v", testCase.expectedIPList, ipList)
		}
	}
}

func TestGetHostIP(t *testing.T) {
	_, err := getHostIP4("myserver")
	testCases := []struct {
		host           string
		expectedIPList set.StringSet
		expectedErr    error
	}{
		{"localhost", set.CreateStringSet("127.0.0.1"), nil},
		{"example.org", set.CreateStringSet("93.184.216.34"), nil},
		{"myserver", nil, err},
	}

	for _, testCase := range testCases {
		ipList, err := getHostIP4(testCase.host)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}

		if testCase.expectedIPList != nil && testCase.expectedIPList.Intersection(ipList).IsEmpty() {
			t.Fatalf("host: expected = %v, got = %v", testCase.expectedIPList, ipList)
		}
	}
}

// Tests finalize api endpoints.
func TestGetAPIEndpoints(t *testing.T) {
	testCases := []struct {
		serverAddr     string
		expectedResult string
	}{
		{":80", "http://127.0.0.1:80"},
		{"127.0.0.1:80", "http://127.0.0.1:80"},
		{"localhost:80", "http://localhost:80"},
	}

	for i, testCase := range testCases {
		apiEndpoints := getAPIEndpoints(testCase.serverAddr)
		apiEndpointSet := set.CreateStringSet(apiEndpoints...)
		if !apiEndpointSet.Contains(testCase.expectedResult) {
			t.Fatalf("test %d: expected: Found, got: Not Found", i+1)
		}
	}
}

// Tests for port availability logic written for server startup sequence.
func TestCheckPortAvailability(t *testing.T) {
	// Make a port is not available.
	port := getFreePort()
	listener, err := net.Listen("tcp", net.JoinHostPort("", port))
	if err != nil {
		t.Fatalf("Unable to listen on port %v", port)
	}
	defer listener.Close()

	testCases := []struct {
		port        string
		expectedErr error
	}{
		{port, fmt.Errorf("listen tcp :%v: bind: address already in use", port)},
		{getFreePort(), nil},
	}

	for _, testCase := range testCases {
		// On MS Windows, skip checking error case due to https://github.com/golang/go/issues/7598
		if runtime.GOOS == globalWindowsOSName && testCase.expectedErr != nil {
			continue
		}

		err := checkPortAvailability(testCase.port)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}
	}
}

func TestCheckLocalServerAddr(t *testing.T) {
	testCases := []struct {
		serverAddr  string
		expectedErr error
	}{
		{":54321", nil},
		{"localhost:54321", nil},
		{"", fmt.Errorf("missing port in address")},
		{"localhost", fmt.Errorf("missing port in address localhost")},
		{"example.org:54321", fmt.Errorf("host in server address should be this server")},
		{":0", fmt.Errorf("port number must be between 1 to 65535")},
		{":-10", fmt.Errorf("port number must be between 1 to 65535")},
	}

	for _, testCase := range testCases {
		err := CheckLocalServerAddr(testCase.serverAddr)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}
	}
}
