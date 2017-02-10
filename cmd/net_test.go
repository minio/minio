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
	"testing"

	"github.com/minio/minio-go/pkg/set"
)

func TestSplitHostPort(t *testing.T) {
	testCases := []struct {
		hostPort     string
		expectedHost string
		expectedPort string
		expectedErr  error
	}{
		{"", "", defaultPort, nil},
		{":54321", "", "54321", nil},
		{"server:54321", "server", "54321", nil},
		{"server", "server", defaultPort, nil},
		{"server:100000000", "server", "100000000", nil},
		{":", "", "", fmt.Errorf("Invalid port number")},
		{":0", "", "", fmt.Errorf("Port number should be greater than zero")},
		{":-10", "", "", fmt.Errorf("Port number should be greater than zero")},
	}

	for _, testCase := range testCases {
		host, port, err := splitHostPort(testCase.hostPort)
		if testCase.expectedErr == nil {
			if testCase.expectedErr != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}

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
			if testCase.expectedErr != nil {
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
