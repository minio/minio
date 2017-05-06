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
	"errors"
	"fmt"
	"net"
	"reflect"
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

func TestSortIPs(t *testing.T) {
	testCases := []struct {
		ipList       []string
		sortedIPList []string
	}{
		// Default case of two ips one with higher octet moves
		// to the beginning of the list.
		{
			ipList:       []string{"127.0.0.1", "10.0.0.13"},
			sortedIPList: []string{"10.0.0.13", "127.0.0.1"},
		},
		// With multiple types of octet, chooses a higher octet.
		{
			ipList:       []string{"127.0.0.1", "172.0.21.1", "192.168.1.106"},
			sortedIPList: []string{"192.168.1.106", "172.0.21.1", "127.0.0.1"},
		},
		// With different ip along with localhost.
		{
			ipList:       []string{"127.0.0.1", "192.168.1.106"},
			sortedIPList: []string{"192.168.1.106", "127.0.0.1"},
		},
		// With a list of only one element nothing to sort.
		{
			ipList:       []string{"hostname"},
			sortedIPList: []string{"hostname"},
		},
		// With a list of only one element nothing to sort.
		{
			ipList:       []string{"127.0.0.1"},
			sortedIPList: []string{"127.0.0.1"},
		},
		// Non parsable ip is assumed to be hostame and gets preserved
		// as the left most elements, regardless of IP based sorting.
		{
			ipList:       []string{"hostname", "127.0.0.1", "192.168.1.106"},
			sortedIPList: []string{"hostname", "192.168.1.106", "127.0.0.1"},
		},
		// Non parsable ip is assumed to be hostname, with a mixed input of ip and hostname.
		// gets preserved and moved into left most elements, regardless of
		// IP based sorting.
		{
			ipList:       []string{"hostname1", "10.0.0.13", "hostname2", "127.0.0.1", "192.168.1.106"},
			sortedIPList: []string{"hostname1", "hostname2", "192.168.1.106", "10.0.0.13", "127.0.0.1"},
		},
		// With same higher octets, preferentially move the localhost.
		{
			ipList:       []string{"127.0.0.1", "10.0.0.1", "192.168.0.1"},
			sortedIPList: []string{"10.0.0.1", "192.168.0.1", "127.0.0.1"},
		},
	}
	for i, testCase := range testCases {
		gotIPList := sortIPs(testCase.ipList)
		if !reflect.DeepEqual(testCase.sortedIPList, gotIPList) {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.sortedIPList, gotIPList)
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
	testCases := []struct {
		host           string
		expectedIPList set.StringSet
		expectedErr    error
	}{
		{"localhost", set.CreateStringSet("127.0.0.1"), nil},
		{"example.org", set.CreateStringSet("93.184.216.34"), nil},
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

// Ask the kernel for a free open port.
func getFreePort() string {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return fmt.Sprintf("%d", l.Addr().(*net.TCPAddr).Port)
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
		{"0.0.0.0:9000", nil},
		{"", fmt.Errorf("missing port in address")},
		{"localhost", fmt.Errorf("address localhost: missing port in address")},
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

func TestExtractHostPort(t *testing.T) {
	testCases := []struct {
		addr        string
		host        string
		port        string
		expectedErr error
	}{
		{"", "", "", errors.New("unable to process empty address")},
		{"localhost:9000", "localhost", "9000", nil},
		{"http://:9000/", "", "9000", nil},
		{"http://8.8.8.8:9000/", "8.8.8.8", "9000", nil},
		{"https://facebook.com:9000/", "facebook.com", "9000", nil},
	}

	for i, testCase := range testCases {
		host, port, err := extractHostPort(testCase.addr)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("Test %d: should succeed but failed with err: %v", i+1, err)
			}
			if host != testCase.host {
				t.Fatalf("Test %d: expected: %v, found: %v", i+1, testCase.host, host)
			}
			if port != testCase.port {
				t.Fatalf("Test %d: expected: %v, found: %v", i+1, testCase.port, port)
			}

		}
		if testCase.expectedErr != nil {
			if err == nil {
				t.Fatalf("Test %d:, should fail but succeeded.", i+1)
			}
			if testCase.expectedErr.Error() != err.Error() {
				t.Fatalf("Test %d: failed with different error, expected: '%v', found:'%v'.", i+1, testCase.expectedErr, err)
			}
		}
	}
}

func TestSameLocalAddrs(t *testing.T) {
	testCases := []struct {
		addr1       string
		addr2       string
		sameAddr    bool
		expectedErr error
	}{
		{"", "", false, errors.New("unable to process empty address")},
		{":9000", ":9000", true, nil},
		{"localhost:9000", ":9000", true, nil},
		{"localhost:9000", "http://localhost:9000", true, nil},
		{"http://localhost:9000", ":9000", true, nil},
		{"http://localhost:9000", "http://localhost:9000", true, nil},
		{"http://8.8.8.8:9000", "http://localhost:9000", false, nil},
	}

	for i, testCase := range testCases {
		sameAddr, err := sameLocalAddrs(testCase.addr1, testCase.addr2)
		if testCase.expectedErr != nil && err == nil {
			t.Fatalf("Test %d: should fail but succeeded", i+1)
		}
		if testCase.expectedErr == nil && err != nil {
			t.Fatalf("Test %d: should succeed but failed with %v", i+1, err)
		}
		if err == nil {
			if sameAddr != testCase.sameAddr {
				t.Fatalf("Test %d: expected: %v, found: %v", i+1, testCase.sameAddr, sameAddr)
			}
		} else {
			if err.Error() != testCase.expectedErr.Error() {
				t.Fatalf("Test %d: failed with different error, expected: '%v', found:'%v'.", i+1, testCase.expectedErr, err)
			}
		}
	}
}
func TestIsHostIPv4(t *testing.T) {
	testCases := []struct {
		args           string
		expectedResult bool
	}{
		{"localhost", false},
		{"localhost:9000", false},
		{"example.com", false},
		{"http://192.168.1.0", false},
		{"http://192.168.1.0:9000", false},
		{"192.168.1.0", true},
	}

	for _, testCase := range testCases {
		ret := isHostIPv4(testCase.args)
		if testCase.expectedResult != ret {
			t.Fatalf("expected: %v , got: %v", testCase.expectedResult, ret)
		}
	}
}
