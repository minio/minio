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
	"runtime"
	"strings"
	"testing"
)

func TestNewEndpoint(t *testing.T) {
	u1, _ := url.Parse("http://localhost:" + defaultPort + "/path")
	u2, _ := url.Parse("https://example.org:" + defaultPort + "/path")
	u3, _ := url.Parse("http://127.0.0.1:8080/path")
	u4, _ := url.Parse("http://192.168.253.200:9000/path")

	errMsg := ": no such host"
	if runtime.GOOS == "windows" {
		errMsg = ": No such host is known."
	}

	testCases := []struct {
		arg              string
		expectedEndpoint Endpoint
		expectedType     EndpointType
		expectedErr      error
	}{
		{"foo", Endpoint{URL: &url.URL{Path: "foo"}, IsLocal: true}, PathEndpointType, nil},
		{"/foo", Endpoint{URL: &url.URL{Path: "/foo"}, IsLocal: true}, PathEndpointType, nil},
		{`\foo`, Endpoint{URL: &url.URL{Path: `\foo`}, IsLocal: true}, PathEndpointType, nil},
		{`C:\foo`, Endpoint{URL: &url.URL{Path: `C:\foo`}, IsLocal: true}, PathEndpointType, nil},
		{"C:/foo", Endpoint{URL: &url.URL{Path: "C:/foo"}, IsLocal: true}, PathEndpointType, nil},
		{`C:\\foo`, Endpoint{URL: &url.URL{Path: `C:\\foo`}, IsLocal: true}, PathEndpointType, nil},
		{"http:path", Endpoint{URL: &url.URL{Path: "http:path"}, IsLocal: true}, PathEndpointType, nil},
		{"http:/path", Endpoint{URL: &url.URL{Path: "http:/path"}, IsLocal: true}, PathEndpointType, nil},
		{"http:///path", Endpoint{URL: &url.URL{Path: "http:/path"}, IsLocal: true}, PathEndpointType, nil},
		{"http://localhost/path", Endpoint{URL: u1, IsLocal: true}, URLEndpointType, nil},
		{"http://localhost/path//", Endpoint{URL: u1, IsLocal: true}, URLEndpointType, nil},
		{"https://example.org/path", Endpoint{URL: u2}, URLEndpointType, nil},
		{"http://127.0.0.1:8080/path", Endpoint{URL: u3, IsLocal: true}, URLEndpointType, nil},
		{"http://192.168.253.200/path", Endpoint{URL: u4}, URLEndpointType, nil},
		{"", Endpoint{}, -1, fmt.Errorf("empty or root endpoint is not supported")},
		{".", Endpoint{}, -1, fmt.Errorf("empty or root endpoint is not supported")},
		{"/", Endpoint{}, -1, fmt.Errorf("empty or root endpoint is not supported")},
		{`\`, Endpoint{}, -1, fmt.Errorf("empty or root endpoint is not supported")},
		{"c://foo", Endpoint{}, -1, fmt.Errorf("invalid URL endpoint format")},
		{"ftp://foo", Endpoint{}, -1, fmt.Errorf("invalid URL endpoint format")},
		{"http://server/path?location", Endpoint{}, -1, fmt.Errorf("invalid URL endpoint format")},
		{"http://:/path", Endpoint{}, -1, fmt.Errorf("invalid URL endpoint format: invalid port number")},
		{"http://:8080/path", Endpoint{}, -1, fmt.Errorf("invalid URL endpoint format: empty host name")},
		{"http://server:/path", Endpoint{}, -1, fmt.Errorf("invalid URL endpoint format: invalid port number")},
		{"https://93.184.216.34:808080/path", Endpoint{}, -1, fmt.Errorf("invalid URL endpoint format: port number must be between 1 to 65536")},
		{"http://server:8080//", Endpoint{}, -1, fmt.Errorf("empty or root path is not supported in URL endpoint")},
		{"http://server:8080/", Endpoint{}, -1, fmt.Errorf("empty or root path is not supported in URL endpoint")},
		{"http://server/path", Endpoint{}, -1, fmt.Errorf("lookup server" + errMsg)},
	}

	for _, testCase := range testCases {
		endpoint, err := NewEndpoint(testCase.arg)
		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else {
			match := false
			if strings.HasSuffix(testCase.expectedErr.Error(), errMsg) {
				match = strings.HasSuffix(err.Error(), errMsg)
			} else {
				match = (testCase.expectedErr.Error() == err.Error())
			}
			if !match {
				t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
			}
		}

		if err == nil && !reflect.DeepEqual(testCase.expectedEndpoint, endpoint) {
			t.Fatalf("endpoint: expected = %+v, got = %+v", testCase.expectedEndpoint, endpoint)
		}

		if err == nil && testCase.expectedType != endpoint.Type() {
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
		{[]string{"http://localhost/d1", "http://localhost/d2", "http://localhost/d3", "http://localhost/d4"}, nil},
		{[]string{"http://example.org/d1", "http://example.com/d1", "http://example.net/d1", "http://example.edu/d1"}, nil},
		{[]string{"http://localhost/d1", "http://localhost/d2", "http://example.org/d1", "http://example.org/d2"}, nil},
		{[]string{"https://localhost:9000/d1", "https://localhost:9001/d2", "https://localhost:9002/d3", "https://localhost:9003/d4"}, nil},
		// // It is valid WRT endpoint list that same path is expected with different port on same server.
		{[]string{"https://127.0.0.1:9000/d1", "https://127.0.0.1:9001/d1", "https://127.0.0.1:9002/d1", "https://127.0.0.1:9003/d1"}, nil},
		{[]string{"d1", "d2", "d3", "d1"}, fmt.Errorf("duplicate endpoints found")},
		{[]string{"http://localhost/d1", "http://localhost/d2", "http://localhost/d1", "http://localhost/d4"}, fmt.Errorf("duplicate endpoints found")},
		{[]string{"d1", "d2", "d3", "d4", "d5"}, fmt.Errorf("total endpoints 5 found. For XL/Distribute, it should be 4, 6, 8, 10, 12, 14 or 16")},
		{[]string{"ftp://server/d1", "http://server/d2", "http://server/d3", "http://server/d4"}, fmt.Errorf("'ftp://server/d1': invalid URL endpoint format")},
		{[]string{"d1", "http://localhost/d2", "d3", "d4"}, fmt.Errorf("mixed style endpoints are not supported")},
		{[]string{"http://example.org/d1", "https://example.com/d1", "http://example.net/d1", "https://example.edut/d1"}, fmt.Errorf("mixed scheme is not supported")},
	}

	for _, testCase := range testCases {
		_, err := NewEndpointList(testCase.args...)
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

func TestCreateEndpoints(t *testing.T) {
	_, err := getHostIP4("myserver")
	testCases := []struct {
		serverAddr  string
		args        []string
		expectedErr error
	}{
		// FS setup.
		{"", []string{"d1"}, nil},
		{"localhost:10000", []string{"/d1"}, nil},
		// XL setup.
		{"", []string{"/d1", "/d2", "d3", "d4"}, nil},
		{"", []string{"http://localhost/d1", "http://localhost/d2", "http://localhost/d3", "http://localhost/d4"}, nil},
		{":10000", []string{"http://localhost/d1", "http://localhost/d2", "http://localhost/d3", "http://localhost/d4"}, nil},
		{":10000", []string{"http://localhost/d1", "http://localhost/d2", "http://127.0.0.1/d3", "http://localhost/d4"}, nil},
		// Distribute setup.
		{"", []string{"http://localhost/d1", "http://example.com/d1", "http://example.net/d1", "http://example.org/d1"}, nil},
		{"", []string{"http://localhost:20000/d1", "http://example.com/d1", "http://example.net/d1", "http://example.org/d1"}, nil},
		{"", []string{"http://localhost/d1", "http://localhost/d2", "http://example.org/d1", "http://example.org/d2"}, nil},
		{"", []string{"http://localhost:10000/d1", "http://localhost:10000/d2", "http://example.org/d1", "http://example.org/d2"}, nil},
		{":10000", []string{"http://localhost:10000/d1", "http://localhost:20000/d2", "http://example.org/d1", "http://example.org/d2"}, nil},
		{":9000", []string{"http://localhost:9000/d1", "http://localhost:9001/d2", "http://localhost:9003/d3", "http://localhost:9004/d4"}, nil},
		// Error setup.
		{"myserver:10000", []string{"d1"}, err},
		{"example.org:10000", []string{"d1"}, fmt.Errorf("host in server address should be this server")},
		{"", []string{"http://localhost/d1"}, fmt.Errorf("use path style endpoint for FS setup")},
		{"", []string{"https://example.org:9000/d1", "https://example.org:9001/d1", "https://example.com/d1", "https://example.com/d2"}, fmt.Errorf("same path can not be served from different port")},
		{"localhost:10000", []string{"http://example.org/d1", "http://example.org/d2", "http://example.org/d3", "http://example.org/d4"}, fmt.Errorf("no endpoint found for this host")},
		{"", []string{"https://example.org/d1", "https://example.com/d2", "https://example.net/d3", "https://example.edu/d4"}, fmt.Errorf("no endpoint found for this host")},
		{":20000", []string{"http://localhost:10000/d1", "http://localhost:10000/d2", "http://example.org/d1", "http://example.org/d2"}, fmt.Errorf("server address and endpoint have different ports")},
		{"", []string{"http://localhost:10000/d1", "http://localhost:20000/d2", "http://example.org/d1", "http://example.org/d2"}, fmt.Errorf("for more than one endpoints for local host with different port, server address must be provided")},
		{":30000", []string{"http://localhost:10000/d1", "http://localhost:20000/d2", "http://example.org/d1", "http://example.org/d2"}, fmt.Errorf("port in server address does not match with local endpoints")},
	}

	for _, testCase := range testCases {
		_, _, _, err := CreateEndpoints(testCase.serverAddr, testCase.args...)
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
