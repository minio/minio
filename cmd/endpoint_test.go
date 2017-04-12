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
	u1, _ := url.Parse("http://localhost/path")
	u2, _ := url.Parse("https://example.org/path")
	u3, _ := url.Parse("http://127.0.0.1:8080/path")
	u4, _ := url.Parse("http://192.168.253.200/path")

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
		{"C", Endpoint{URL: &url.URL{Path: `C`}, IsLocal: true}, PathEndpointType, nil},
		{"C:", Endpoint{URL: &url.URL{Path: `C:`}, IsLocal: true}, PathEndpointType, nil},
		{"C:/", Endpoint{URL: &url.URL{Path: "C:"}, IsLocal: true}, PathEndpointType, nil},
		{`C:\`, Endpoint{URL: &url.URL{Path: `C:\`}, IsLocal: true}, PathEndpointType, nil},
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
		{"https://93.184.216.34:808080/path", Endpoint{}, -1, fmt.Errorf("invalid URL endpoint format: port number must be between 1 to 65535")},
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
		{[]string{"d1", "d2", "d3", "./d1"}, fmt.Errorf("duplicate endpoints found")},
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
	case1u1, _ := url.Parse("http://example.com:10000/d4")
	case1u2, _ := url.Parse("http://example.org:10000/d3")
	case1u3, _ := url.Parse("http://localhost:10000/d1")
	case1u4, _ := url.Parse("http://localhost:10000/d2")

	case2u1, _ := url.Parse("http://example.com:10000/d4")
	case2u2, _ := url.Parse("http://example.org:10000/d3")
	case2u3, _ := url.Parse("http://localhost:10000/d1")
	case2u4, _ := url.Parse("http://localhost:9000/d2")

	case3u1, _ := url.Parse("http://example.com:80/d3")
	case3u2, _ := url.Parse("http://example.net:80/d4")
	case3u3, _ := url.Parse("http://example.org:9000/d2")
	case3u4, _ := url.Parse("http://localhost:80/d1")

	case4u1, _ := url.Parse("http://example.com:9000/d3")
	case4u2, _ := url.Parse("http://example.net:9000/d4")
	case4u3, _ := url.Parse("http://example.org:9000/d2")
	case4u4, _ := url.Parse("http://localhost:9000/d1")

	case5u1, _ := url.Parse("http://localhost:9000/d1")
	case5u2, _ := url.Parse("http://localhost:9001/d2")
	case5u3, _ := url.Parse("http://localhost:9002/d3")
	case5u4, _ := url.Parse("http://localhost:9003/d4")

	case6u1, _ := url.Parse("http://10.0.0.1:9000/export")
	case6u2, _ := url.Parse("http://10.0.0.2:9000/export")
	case6u3, _ := url.Parse("http://10.0.0.3:9000/export")
	case6u4, _ := url.Parse("http://localhost:9001/export")

	testCases := []struct {
		serverAddr         string
		args               []string
		expectedServerAddr string
		expectedEndpoints  EndpointList
		expectedSetupType  SetupType
		expectedErr        error
	}{
		{"localhost", []string{}, "", EndpointList{}, -1, fmt.Errorf("missing port in address localhost")},

		// FS Setup
		{"localhost:9000", []string{"http://localhost/d1"}, "", EndpointList{}, -1, fmt.Errorf("use path style endpoint for FS setup")},
		{":443", []string{"d1"}, ":443", EndpointList{Endpoint{URL: &url.URL{Path: "d1"}, IsLocal: true}}, FSSetupType, nil},
		{"localhost:10000", []string{"/d1"}, "localhost:10000", EndpointList{Endpoint{URL: &url.URL{Path: "/d1"}, IsLocal: true}}, FSSetupType, nil},
		{"localhost:10000", []string{"./d1"}, "localhost:10000", EndpointList{Endpoint{URL: &url.URL{Path: "d1"}, IsLocal: true}}, FSSetupType, nil},
		{"localhost:10000", []string{`\d1`}, "localhost:10000", EndpointList{Endpoint{URL: &url.URL{Path: `\d1`}, IsLocal: true}}, FSSetupType, nil},
		{"localhost:10000", []string{`.\d1`}, "localhost:10000", EndpointList{Endpoint{URL: &url.URL{Path: `.\d1`}, IsLocal: true}}, FSSetupType, nil},
		{":8080", []string{"https://example.org/d1", "https://example.org/d2", "https://example.org/d3", "https://example.org/d4"}, "", EndpointList{}, -1, fmt.Errorf("no endpoint found for this host")},
		{":8080", []string{"https://example.org/d1", "https://example.com/d2", "https://example.net:8000/d3", "https://example.edu/d1"}, "", EndpointList{}, -1, fmt.Errorf("no endpoint found for this host")},
		{"localhost:9000", []string{"https://127.0.0.1:9000/d1", "https://localhost:9001/d1", "https://example.com/d1", "https://example.com/d2"}, "", EndpointList{}, -1, fmt.Errorf("path '/d1' can not be served from different address/port")},
		{"localhost:9000", []string{"https://127.0.0.1:8000/d1", "https://localhost:9001/d2", "https://example.com/d1", "https://example.com/d2"}, "", EndpointList{}, -1, fmt.Errorf("port number in server address must match with one of the port in local endpoints")},
		{"localhost:10000", []string{"https://127.0.0.1:8000/d1", "https://localhost:8000/d2", "https://example.com/d1", "https://example.com/d2"}, "", EndpointList{}, -1, fmt.Errorf("server address and local endpoint have different ports")},

		// XL Setup with PathEndpointType
		{":1234", []string{"/d1", "/d2", "d3", "d4"}, ":1234",
			EndpointList{
				Endpoint{URL: &url.URL{Path: "/d1"}, IsLocal: true},
				Endpoint{URL: &url.URL{Path: "/d2"}, IsLocal: true},
				Endpoint{URL: &url.URL{Path: "d3"}, IsLocal: true},
				Endpoint{URL: &url.URL{Path: "d4"}, IsLocal: true},
			}, XLSetupType, nil},
		// XL Setup with URLEndpointType
		{":9000", []string{"http://localhost/d1", "http://localhost/d2", "http://localhost/d3", "http://localhost/d4"}, ":9000", EndpointList{
			Endpoint{URL: &url.URL{Path: "/d1"}, IsLocal: true},
			Endpoint{URL: &url.URL{Path: "/d2"}, IsLocal: true},
			Endpoint{URL: &url.URL{Path: "/d3"}, IsLocal: true},
			Endpoint{URL: &url.URL{Path: "/d4"}, IsLocal: true},
		}, XLSetupType, nil},
		// XL Setup with URLEndpointType having mixed naming to local host.
		{"127.0.0.1:10000", []string{"http://localhost/d1", "http://localhost/d2", "http://127.0.0.1/d3", "http://127.0.0.1/d4"}, ":10000", EndpointList{
			Endpoint{URL: &url.URL{Path: "/d1"}, IsLocal: true},
			Endpoint{URL: &url.URL{Path: "/d2"}, IsLocal: true},
			Endpoint{URL: &url.URL{Path: "/d3"}, IsLocal: true},
			Endpoint{URL: &url.URL{Path: "/d4"}, IsLocal: true},
		}, XLSetupType, nil},

		// DistXL type
		{"127.0.0.1:10000", []string{"http://localhost/d1", "http://localhost/d2", "http://example.org/d3", "http://example.com/d4"}, "127.0.0.1:10000", EndpointList{
			Endpoint{URL: case1u1, IsLocal: false},
			Endpoint{URL: case1u2, IsLocal: false},
			Endpoint{URL: case1u3, IsLocal: true},
			Endpoint{URL: case1u4, IsLocal: true},
		}, DistXLSetupType, nil},
		{"127.0.0.1:10000", []string{"http://localhost/d1", "http://localhost:9000/d2", "http://example.org/d3", "http://example.com/d4"}, "127.0.0.1:10000", EndpointList{
			Endpoint{URL: case2u1, IsLocal: false},
			Endpoint{URL: case2u2, IsLocal: false},
			Endpoint{URL: case2u3, IsLocal: true},
			Endpoint{URL: case2u4, IsLocal: false},
		}, DistXLSetupType, nil},
		{":80", []string{"http://localhost/d1", "http://example.org:9000/d2", "http://example.com/d3", "http://example.net/d4"}, ":80", EndpointList{
			Endpoint{URL: case3u1, IsLocal: false},
			Endpoint{URL: case3u2, IsLocal: false},
			Endpoint{URL: case3u3, IsLocal: false},
			Endpoint{URL: case3u4, IsLocal: true},
		}, DistXLSetupType, nil},
		{":9000", []string{"http://localhost/d1", "http://example.org/d2", "http://example.com/d3", "http://example.net/d4"}, ":9000", EndpointList{
			Endpoint{URL: case4u1, IsLocal: false},
			Endpoint{URL: case4u2, IsLocal: false},
			Endpoint{URL: case4u3, IsLocal: false},
			Endpoint{URL: case4u4, IsLocal: true},
		}, DistXLSetupType, nil},
		{":9000", []string{"http://localhost:9000/d1", "http://localhost:9001/d2", "http://localhost:9002/d3", "http://localhost:9003/d4"}, ":9000", EndpointList{
			Endpoint{URL: case5u1, IsLocal: true},
			Endpoint{URL: case5u2, IsLocal: false},
			Endpoint{URL: case5u3, IsLocal: false},
			Endpoint{URL: case5u4, IsLocal: false},
		}, DistXLSetupType, nil},

		{":9001", []string{"http://10.0.0.1:9000/export", "http://10.0.0.2:9000/export", "http://localhost:9001/export", "http://10.0.0.3:9000/export"}, ":9001", EndpointList{
			Endpoint{URL: case6u1, IsLocal: false},
			Endpoint{URL: case6u2, IsLocal: false},
			Endpoint{URL: case6u3, IsLocal: false},
			Endpoint{URL: case6u4, IsLocal: true},
		}, DistXLSetupType, nil},
	}

	for _, testCase := range testCases {
		serverAddr, endpoints, setupType, err := CreateEndpoints(testCase.serverAddr, testCase.args...)

		if err == nil {
			if testCase.expectedErr != nil {
				t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
			} else {
				if serverAddr != testCase.expectedServerAddr {
					t.Fatalf("serverAddr: expected = %v, got = %v", testCase.expectedServerAddr, serverAddr)
				}
				if !reflect.DeepEqual(endpoints, testCase.expectedEndpoints) {
					t.Fatalf("endpoints: expected = %v, got = %v", testCase.expectedEndpoints, endpoints)
				}
				if setupType != testCase.expectedSetupType {
					t.Fatalf("setupType: expected = %v, got = %v", testCase.expectedSetupType, setupType)
				}
			}
		} else if testCase.expectedErr == nil {
			t.Fatalf("error: expected = <nil>, got = %v", err)
		} else if err.Error() != testCase.expectedErr.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}
	}
}

func TestGetRemotePeers(t *testing.T) {
	tempGlobalMinioPort := globalMinioPort
	defer func() {
		globalMinioPort = tempGlobalMinioPort
	}()
	globalMinioPort = "9000"

	testCases := []struct {
		endpointArgs   []string
		expectedResult []string
	}{
		{[]string{"/d1", "/d2", "d3", "d4"}, []string{}},
		{[]string{"http://localhost:9000/d1", "http://localhost:9000/d2", "http://example.org:9000/d3", "http://example.com:9000/d4"}, []string{"example.com:9000", "example.org:9000"}},
		{[]string{"http://localhost:9000/d1", "http://localhost:10000/d2", "http://example.org:9000/d3", "http://example.com:9000/d4"}, []string{"example.com:9000", "example.org:9000", "localhost:10000"}},
		{[]string{"http://localhost:9000/d1", "http://example.org:9000/d2", "http://example.com:9000/d3", "http://example.net:9000/d4"}, []string{"example.com:9000", "example.net:9000", "example.org:9000"}},
		{[]string{"http://localhost:9000/d1", "http://localhost:9001/d2", "http://localhost:9002/d3", "http://localhost:9003/d4"}, []string{"localhost:9001", "localhost:9002", "localhost:9003"}},
	}

	for _, testCase := range testCases {
		endpoints, _ := NewEndpointList(testCase.endpointArgs...)
		remotePeers := GetRemotePeers(endpoints)
		if !reflect.DeepEqual(remotePeers, testCase.expectedResult) {
			t.Fatalf("expected: %v, got: %v", testCase.expectedResult, remotePeers)
		}
	}
}
