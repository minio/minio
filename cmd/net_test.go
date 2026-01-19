// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/minio/minio-go/v7/pkg/set"
)

func TestMustSplitHostPort(t *testing.T) {
	testCases := []struct {
		hostPort     string
		expectedHost string
		expectedPort string
	}{
		{":54321", "", "54321"},
		{"server:54321", "server", "54321"},
		{":0", "", "0"},
		{"server:https", "server", "443"},
		{"server:http", "server", "80"},
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
		// Non parsable ip is assumed to be hostname and gets preserved
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
	}

	for _, testCase := range testCases {
		ipList, err := getHostIP(testCase.host)
		switch {
		case testCase.expectedErr == nil:
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		case err == nil:
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		case testCase.expectedErr.Error() != err.Error():
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}

		if testCase.expectedIPList != nil {
			var found bool
			for _, ip := range ipList.ToSlice() {
				if testCase.expectedIPList.Contains(ip) {
					found = true
				}
			}
			if !found {
				t.Fatalf("host: expected = %v, got = %v", testCase.expectedIPList, ipList)
			}
		}
	}
}

// Tests finalize api endpoints.
func TestGetAPIEndpoints(t *testing.T) {
	host, port := globalMinioHost, globalMinioPort
	defer func() {
		globalMinioHost, globalMinioPort = host, port
	}()
	testCases := []struct {
		host, port     string
		expectedResult string
	}{
		{"", "80", "http://127.0.0.1:80"},
		{"127.0.0.1", "80", "http://127.0.0.1:80"},
		{"localhost", "80", "http://localhost:80"},
	}

	for i, testCase := range testCases {
		globalMinioHost, globalMinioPort = testCase.host, testCase.port
		apiEndpoints := getAPIEndpoints()
		apiEndpointSet := set.CreateStringSet(apiEndpoints...)
		if !apiEndpointSet.Contains(testCase.expectedResult) {
			t.Fatalf("test %d: expected: Found, got: Not Found", i+1)
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
		{":0", nil},
		{"localhost", nil},
		{"", fmt.Errorf("invalid argument")},
		{"example.org:54321", fmt.Errorf("host in server address should be this server")},
		{":-10", fmt.Errorf("port must be between 0 to 65535")},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			err := CheckLocalServerAddr(testCase.serverAddr)
			switch {
			case testCase.expectedErr == nil:
				if err != nil {
					t.Errorf("error: expected = <nil>, got = %v", err)
				}
			case err == nil:
				t.Errorf("error: expected = %v, got = <nil>", testCase.expectedErr)
			case testCase.expectedErr.Error() != err.Error():
				t.Errorf("error: expected = %v, got = %v", testCase.expectedErr, err)
			}
		})
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
		if testCase.expectedErr == nil && err != nil {
			t.Fatalf("Test %d: should succeed but failed with err: %v", i+1, err)
		}
		if testCase.expectedErr != nil && err == nil {
			t.Fatalf("Test %d:, should fail but succeeded.", i+1)
		}
		if err == nil {
			if host != testCase.host {
				t.Fatalf("Test %d: expected: %v, found: %v", i+1, testCase.host, host)
			}
			if port != testCase.port {
				t.Fatalf("Test %d: expected: %v, found: %v", i+1, testCase.port, port)
			}
		}
		if testCase.expectedErr != nil && err != nil {
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

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			sameAddr, err := sameLocalAddrs(testCase.addr1, testCase.addr2)
			if testCase.expectedErr != nil && err == nil {
				t.Errorf("should fail but succeeded")
			}
			if testCase.expectedErr == nil && err != nil {
				t.Errorf("should succeed but failed with %v", err)
			}
			if err == nil {
				if sameAddr != testCase.sameAddr {
					t.Errorf("expected: %v, found: %v", testCase.sameAddr, sameAddr)
				}
			} else {
				if err.Error() != testCase.expectedErr.Error() {
					t.Errorf("failed with different error, expected: '%v', found:'%v'.",
						testCase.expectedErr, err)
				}
			}
		})
	}
}

func TestIsHostIP(t *testing.T) {
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
		{"[2001:3984:3989::20%eth0]:9000", true},
	}

	for _, testCase := range testCases {
		ret := isHostIP(testCase.args)
		if testCase.expectedResult != ret {
			t.Fatalf("expected: %v , got: %v", testCase.expectedResult, ret)
		}
	}
}
