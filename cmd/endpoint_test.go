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
	"net/url"
	"reflect"
	"testing"
)

func TestIsValidDistribution(t *testing.T) {
	testCases := []struct {
		count          int
		expectedResult bool
	}{
		{-4, false},
		{0, false},
		{1, false},
		{7, false},
		{18, false},
		{4, true},
		{6, true},
		{8, true},
		{10, true},
		{12, true},
		{14, true},
		{16, true},
	}

	for _, testCase := range testCases {
		result := IsValidDistribution(testCase.count)
		if testCase.expectedResult != result {
			t.Fatalf("expected = %v, got = %v", testCase.expectedResult, result)
		}
	}
}

func TestCheckLocalServerAddr(t *testing.T) {
	testCases := []struct {
		serverAddr  string
		expectedErr error
	}{
		{"", nil},
		{":54321", nil},
		{"localhost:54321", nil},
		{"localhost", fmt.Errorf("missing port in address localhost")},
		{"example.org:54321", fmt.Errorf("host in server address should be this server")},
		{":0", fmt.Errorf("port number should be greater than zero in server address")},
		{":-10", fmt.Errorf("port number should be greater than zero in server address")},
	}

	for _, testCase := range testCases {
		err := CheckLocalServerAddr(testCase.serverAddr)
		if testCase.expectedErr == nil {
			if testCase.expectedErr != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}
	}
}

func TestIsEmptyPath(t *testing.T) {
	testCases := []struct {
		path           string
		expectedResult bool
	}{
		{"", true},
		{".", true},
		{"/", true},
		{`\`, true},
		{"foo", false},
		{"/foo", false},
		{`\foo`, false},
		{`C:\foo`, false},
		{"C:/foo", false},
	}

	for _, testCase := range testCases {
		result := IsEmptyPath(testCase.path)
		if testCase.expectedResult != result {
			t.Fatalf("expected = %v, got = %v", testCase.expectedResult, result)
		}
	}
}

func TestNewEndpoint(t *testing.T) {
	u1, _ := url.Parse("http://server:" + defaultPort + "/path")
	u2, _ := url.Parse("https://server:" + defaultPort + "/path")
	u3, _ := url.Parse("http://server:8080/path")
	u4, _ := url.Parse("https://server:808080/path")

	testCases := []struct {
		arg              string
		expectedEndpoint Endpoint
		expectedType     EndpointType
		expectedErr      error
	}{
		{"foo", Endpoint{Value: "foo", URL: nil}, PathEndpointType, nil},
		{"/foo", Endpoint{Value: "/foo", URL: nil}, PathEndpointType, nil},
		{`\foo`, Endpoint{Value: `\foo`, URL: nil}, PathEndpointType, nil},
		{`C:\foo`, Endpoint{Value: `C:\foo`, URL: nil}, PathEndpointType, nil},
		{"C:/foo", Endpoint{Value: "C:/foo", URL: nil}, PathEndpointType, nil},
		{`C:\\foo`, Endpoint{Value: `C:\\foo`, URL: nil}, PathEndpointType, nil},
		{"http:path", Endpoint{Value: "http:path", URL: nil}, PathEndpointType, nil},
		{"http:/path", Endpoint{Value: "http:/path", URL: nil}, PathEndpointType, nil},
		{"http:///path", Endpoint{Value: "http:///path", URL: nil}, PathEndpointType, nil},
		{"http://server/path", Endpoint{Value: "http://server:" + defaultPort + "/path", URL: u1}, URLEndpointType, nil},
		{"https://server/path", Endpoint{Value: "https://server:" + defaultPort + "/path", URL: u2}, URLEndpointType, nil},
		{"http://server:8080/path", Endpoint{Value: "http://server:8080/path", URL: u3}, URLEndpointType, nil},
		{"https://server:808080/path", Endpoint{Value: "https://server:808080/path", URL: u4}, URLEndpointType, nil},
		{"/", Endpoint{}, UnknownEndpointType, fmt.Errorf("Empty or root endpoint is not supported")},
		{"c://foo", Endpoint{}, UnknownEndpointType, fmt.Errorf("Unknown endpoint format")},
		{"ftp://foo", Endpoint{}, UnknownEndpointType, fmt.Errorf("Unknown endpoint format")},
		{"http://server/path?location", Endpoint{}, UnknownEndpointType, fmt.Errorf("Unknown endpoint format")},
		{"http://:/path", Endpoint{}, UnknownEndpointType, fmt.Errorf("Invalid host in endpoint format")},
		{"http://:8080/path", Endpoint{}, UnknownEndpointType, fmt.Errorf("Invalid host in endpoint format")},
		{"http://server:/path", Endpoint{}, UnknownEndpointType, fmt.Errorf("Invalid host in endpoint format")},
		{"http://server:8080/", Endpoint{}, UnknownEndpointType, fmt.Errorf("Empty or root path is not supported in URL endpoint")},
	}

	for _, testCase := range testCases {
		endpoint, err := NewEndpoint(testCase.arg)
		if testCase.expectedErr == nil {
			if testCase.expectedErr != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}

		if !reflect.DeepEqual(testCase.expectedEndpoint, endpoint) {
			t.Fatalf("endpoint: expected = %+v, got = %+v", testCase.expectedEndpoint, endpoint)
		}

		if testCase.expectedType != endpoint.Type() {
			t.Fatalf("type: expected = %+v, got = %+v", testCase.expectedType, endpoint.Type())
		}
	}
}

func TestNewEndpointList(t *testing.T) {
	testCases := []struct {
		args        []string
		expectedErr error
	}{
		{[]string{"d1", "d2", "d3", "d4"}, nil},
		{[]string{"/d1", "/d2", "/d3", "/d4"}, nil},
		{[]string{"http://server/d1", "http://server/d2", "http://server/d3", "http://server/d4"}, nil},
		{[]string{"http://server1/d1", "http://server2/d1", "http://server3/d1", "http://server4/d1"}, nil},
		{[]string{"http://server1/d1", "http://server1/d2", "http://server2/d1", "http://server2/d2"}, nil},
		{[]string{"https://server:9000/d1", "https://server:9001/d2", "https://server:9002/d3", "https://server:9003/d4"}, nil},
		// It is valid that same path is expected with different port on same server.
		{[]string{"https://server:9000/d1", "https://server:9001/d1", "https://server:9002/d1", "https://server:9003/d1"}, nil},
		{[]string{"d1", "d2", "d3", "d1"}, fmt.Errorf("duplicate endpoints found")},
		{[]string{"d1", "d2", "d3", "d4", "d5"}, fmt.Errorf("total endpoints 5 found. For XL/Distribute, it should be 4, 6, 8, 10, 12, 14 or 16")},
		{[]string{"ftp://server/d1", "http://server/d2", "http://server/d3", "http://server/d4"}, fmt.Errorf("unknown endpoint format ftp://server/d1")},
		{[]string{"d1", "http://server/d2", "d3", "d4"}, fmt.Errorf("mixed style endpoints are not supported")},
		{[]string{"http://server1/d1", "https://server2/d1", "http://server3/d1", "https://server4/d1"}, fmt.Errorf("mixed scheme is not supported")},
	}

	for _, testCase := range testCases {
		_, err := NewEndpointList(testCase.args...)
		if testCase.expectedErr == nil {
			if testCase.expectedErr != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}
	}
}
