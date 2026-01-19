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
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNewEndpoint(t *testing.T) {
	u2, _ := url.Parse("https://example.org/path")
	u4, _ := url.Parse("http://192.168.253.200/path")
	rootSlashFoo, _ := filepath.Abs("/foo")
	testCases := []struct {
		arg              string
		expectedEndpoint Endpoint
		expectedType     EndpointType
		expectedErr      error
	}{
		{"/foo", Endpoint{&url.URL{Path: rootSlashFoo}, true, -1, -1, -1}, PathEndpointType, nil},
		{"https://example.org/path", Endpoint{u2, false, -1, -1, -1}, URLEndpointType, nil},
		{"http://192.168.253.200/path", Endpoint{u4, false, -1, -1, -1}, URLEndpointType, nil},
		{"", Endpoint{}, -1, fmt.Errorf("empty or root endpoint is not supported")},
		{SlashSeparator, Endpoint{}, -1, fmt.Errorf("empty or root endpoint is not supported")},
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
		{"192.168.1.210:9000", Endpoint{}, -1, fmt.Errorf("invalid URL endpoint format: missing scheme http or https")},
	}

	for i, test := range testCases {
		t.Run(fmt.Sprint("case-", i), func(t *testing.T) {
			endpoint, err := NewEndpoint(test.arg)
			if err == nil {
				err = endpoint.UpdateIsLocal()
			}

			switch {
			case test.expectedErr == nil:
				if err != nil {
					t.Errorf("error: expected = <nil>, got = %v", err)
				}
			case err == nil:
				t.Errorf("error: expected = %v, got = <nil>", test.expectedErr)
			case test.expectedErr.Error() != err.Error():
				t.Errorf("error: expected = %v, got = %v", test.expectedErr, err)
			}

			if err == nil {
				if (test.expectedEndpoint.URL == nil) != (endpoint.URL == nil) {
					t.Errorf("endpoint url: expected = %#v, got = %#v", test.expectedEndpoint.URL, endpoint.URL)
					return
				} else if test.expectedEndpoint.URL.String() != endpoint.URL.String() {
					t.Errorf("endpoint url: expected = %#v, got = %#v", test.expectedEndpoint.URL.String(), endpoint.URL.String())
					return
				}
				if !reflect.DeepEqual(test.expectedEndpoint, endpoint) {
					t.Errorf("endpoint: expected = %#v, got = %#v", test.expectedEndpoint, endpoint)
				}
			}

			if err == nil && test.expectedType != endpoint.Type() {
				t.Errorf("type: expected = %+v, got = %+v", test.expectedType, endpoint.Type())
			}
		})
	}
}

func TestNewEndpoints(t *testing.T) {
	testCases := []struct {
		args        []string
		expectedErr error
	}{
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
		{[]string{"ftp://server/d1", "http://server/d2", "http://server/d3", "http://server/d4"}, fmt.Errorf("'ftp://server/d1': invalid URL endpoint format")},
		{[]string{"d1", "http://localhost/d2", "d3", "d4"}, fmt.Errorf("mixed style endpoints are not supported")},
		{[]string{"http://example.org/d1", "https://example.com/d1", "http://example.net/d1", "https://example.edut/d1"}, fmt.Errorf("mixed scheme is not supported")},
		{[]string{"192.168.1.210:9000/tmp/dir0", "192.168.1.210:9000/tmp/dir1", "192.168.1.210:9000/tmp/dir2", "192.168.110:9000/tmp/dir3"}, fmt.Errorf("'192.168.1.210:9000/tmp/dir0': invalid URL endpoint format: missing scheme http or https")},
	}

	for _, testCase := range testCases {
		_, err := NewEndpoints(testCase.args...)
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
	}
}

func TestCreateEndpoints(t *testing.T) {
	tempGlobalMinioPort := globalMinioPort
	defer func() {
		globalMinioPort = tempGlobalMinioPort
	}()
	globalMinioPort = "9000"

	// Filter ipList by IPs those do not start with '127.'.
	nonLoopBackIPs := localIP4.FuncMatch(func(ip string, matchString string) bool {
		return !net.ParseIP(ip).IsLoopback()
	}, "")
	if len(nonLoopBackIPs) == 0 {
		t.Fatalf("No non-loop back IP address found for this host")
	}
	nonLoopBackIP := nonLoopBackIPs.ToSlice()[0]

	mustAbs := func(s string) string {
		s, err := filepath.Abs(s)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	getExpectedEndpoints := func(args []string, prefix string) ([]*url.URL, []bool) {
		var URLs []*url.URL
		var localFlags []bool
		for _, arg := range args {
			u, _ := url.Parse(arg)
			URLs = append(URLs, u)
			localFlags = append(localFlags, strings.HasPrefix(arg, prefix))
		}

		return URLs, localFlags
	}

	case1Endpoint1 := "http://" + nonLoopBackIP + "/d1"
	case1Endpoint2 := "http://" + nonLoopBackIP + "/d2"
	args := []string{
		"http://" + nonLoopBackIP + ":10000/d1",
		"http://" + nonLoopBackIP + ":10000/d2",
		"http://example.org:10000/d3",
		"http://example.com:10000/d4",
	}
	case1URLs, case1LocalFlags := getExpectedEndpoints(args, "http://"+nonLoopBackIP+":10000/")

	case2Endpoint1 := "http://" + nonLoopBackIP + "/d1"
	case2Endpoint2 := "http://" + nonLoopBackIP + ":9000/d2"
	args = []string{
		"http://" + nonLoopBackIP + ":10000/d1",
		"http://" + nonLoopBackIP + ":9000/d2",
		"http://example.org:10000/d3",
		"http://example.com:10000/d4",
	}
	case2URLs, case2LocalFlags := getExpectedEndpoints(args, "http://"+nonLoopBackIP+":10000/")

	case3Endpoint1 := "http://" + nonLoopBackIP + "/d1"
	args = []string{
		"http://" + nonLoopBackIP + ":80/d1",
		"http://example.org:9000/d2",
		"http://example.com:80/d3",
		"http://example.net:80/d4",
	}
	case3URLs, case3LocalFlags := getExpectedEndpoints(args, "http://"+nonLoopBackIP+":80/")

	case4Endpoint1 := "http://" + nonLoopBackIP + "/d1"
	args = []string{
		"http://" + nonLoopBackIP + ":9000/d1",
		"http://example.org:9000/d2",
		"http://example.com:9000/d3",
		"http://example.net:9000/d4",
	}
	case4URLs, case4LocalFlags := getExpectedEndpoints(args, "http://"+nonLoopBackIP+":9000/")

	case5Endpoint1 := "http://" + nonLoopBackIP + ":9000/d1"
	case5Endpoint2 := "http://" + nonLoopBackIP + ":9001/d2"
	case5Endpoint3 := "http://" + nonLoopBackIP + ":9002/d3"
	case5Endpoint4 := "http://" + nonLoopBackIP + ":9003/d4"
	args = []string{
		case5Endpoint1,
		case5Endpoint2,
		case5Endpoint3,
		case5Endpoint4,
	}
	case5URLs, case5LocalFlags := getExpectedEndpoints(args, "http://"+nonLoopBackIP+":9000/")

	case6Endpoint := "http://" + nonLoopBackIP + ":9003/d4"
	args = []string{
		"http://localhost:9000/d1",
		"http://localhost:9001/d2",
		"http://127.0.0.1:9002/d3",
		case6Endpoint,
	}
	case6URLs, case6LocalFlags := getExpectedEndpoints(args, "http://"+nonLoopBackIP+":9003/")

	testCases := []struct {
		serverAddr         string
		args               []string
		expectedServerAddr string
		expectedEndpoints  Endpoints
		expectedSetupType  SetupType
		expectedErr        error
	}{
		{"localhost", []string{}, "", Endpoints{}, -1, fmt.Errorf("address localhost: missing port in address")},

		// Erasure Single Drive
		{"localhost:9000", []string{"http://localhost/d1"}, "", Endpoints{}, -1, fmt.Errorf("use path style endpoint for SD setup")},
		{":443", []string{"/d1"}, ":443", Endpoints{Endpoint{URL: &url.URL{Path: mustAbs("/d1")}, IsLocal: true}}, ErasureSDSetupType, nil},
		{"localhost:10000", []string{"/d1"}, "localhost:10000", Endpoints{Endpoint{URL: &url.URL{Path: mustAbs("/d1")}, IsLocal: true}}, ErasureSDSetupType, nil},
		{"localhost:9000", []string{"https://127.0.0.1:9000/d1", "https://localhost:9001/d1", "https://example.com/d1", "https://example.com/d2"}, "", Endpoints{}, -1, fmt.Errorf("path '/d1' can not be served by different port on same address")},

		// Erasure Setup with PathEndpointType
		{
			":1234",
			[]string{"/d1", "/d2", "/d3", "/d4"},
			":1234",
			Endpoints{
				Endpoint{URL: &url.URL{Path: mustAbs("/d1")}, IsLocal: true},
				Endpoint{URL: &url.URL{Path: mustAbs("/d2")}, IsLocal: true},
				Endpoint{URL: &url.URL{Path: mustAbs("/d3")}, IsLocal: true},
				Endpoint{URL: &url.URL{Path: mustAbs("/d4")}, IsLocal: true},
			},
			ErasureSetupType, nil,
		},
		// DistErasure Setup with URLEndpointType
		{":9000", []string{"http://localhost/d1", "http://localhost/d2", "http://localhost/d3", "http://localhost/d4"}, ":9000", Endpoints{
			Endpoint{URL: &url.URL{Scheme: "http", Host: "localhost:9000", Path: "/d1"}, IsLocal: true},
			Endpoint{URL: &url.URL{Scheme: "http", Host: "localhost:9000", Path: "/d2"}, IsLocal: true},
			Endpoint{URL: &url.URL{Scheme: "http", Host: "localhost:9000", Path: "/d3"}, IsLocal: true},
			Endpoint{URL: &url.URL{Scheme: "http", Host: "localhost:9000", Path: "/d4"}, IsLocal: true},
		}, ErasureSetupType, nil},
		// DistErasure Setup with URLEndpointType having mixed naming to local host.
		{"127.0.0.1:10000", []string{"http://localhost/d1", "http://localhost/d2", "http://127.0.0.1/d3", "http://127.0.0.1/d4"}, "", Endpoints{}, -1, fmt.Errorf("all local endpoints should not have different hostnames/ips")},

		{":9001", []string{"http://10.0.0.1:9000/export", "http://10.0.0.2:9000/export", "http://" + nonLoopBackIP + ":9001/export", "http://10.0.0.2:9001/export"}, "", Endpoints{}, -1, fmt.Errorf("path '/export' can not be served by different port on same address")},

		{":9000", []string{"http://127.0.0.1:9000/export", "http://" + nonLoopBackIP + ":9000/export", "http://10.0.0.1:9000/export", "http://10.0.0.2:9000/export"}, "", Endpoints{}, -1, fmt.Errorf("path '/export' cannot be served by different address on same server")},

		// DistErasure type
		{"127.0.0.1:10000", []string{case1Endpoint1, case1Endpoint2, "http://example.org/d3", "http://example.com/d4"}, "127.0.0.1:10000", Endpoints{
			Endpoint{URL: case1URLs[0], IsLocal: case1LocalFlags[0]},
			Endpoint{URL: case1URLs[1], IsLocal: case1LocalFlags[1]},
			Endpoint{URL: case1URLs[2], IsLocal: case1LocalFlags[2]},
			Endpoint{URL: case1URLs[3], IsLocal: case1LocalFlags[3]},
		}, DistErasureSetupType, nil},

		{"127.0.0.1:10000", []string{case2Endpoint1, case2Endpoint2, "http://example.org/d3", "http://example.com/d4"}, "127.0.0.1:10000", Endpoints{
			Endpoint{URL: case2URLs[0], IsLocal: case2LocalFlags[0]},
			Endpoint{URL: case2URLs[1], IsLocal: case2LocalFlags[1]},
			Endpoint{URL: case2URLs[2], IsLocal: case2LocalFlags[2]},
			Endpoint{URL: case2URLs[3], IsLocal: case2LocalFlags[3]},
		}, DistErasureSetupType, nil},

		{":80", []string{case3Endpoint1, "http://example.org:9000/d2", "http://example.com/d3", "http://example.net/d4"}, ":80", Endpoints{
			Endpoint{URL: case3URLs[0], IsLocal: case3LocalFlags[0]},
			Endpoint{URL: case3URLs[1], IsLocal: case3LocalFlags[1]},
			Endpoint{URL: case3URLs[2], IsLocal: case3LocalFlags[2]},
			Endpoint{URL: case3URLs[3], IsLocal: case3LocalFlags[3]},
		}, DistErasureSetupType, nil},

		{":9000", []string{case4Endpoint1, "http://example.org/d2", "http://example.com/d3", "http://example.net/d4"}, ":9000", Endpoints{
			Endpoint{URL: case4URLs[0], IsLocal: case4LocalFlags[0]},
			Endpoint{URL: case4URLs[1], IsLocal: case4LocalFlags[1]},
			Endpoint{URL: case4URLs[2], IsLocal: case4LocalFlags[2]},
			Endpoint{URL: case4URLs[3], IsLocal: case4LocalFlags[3]},
		}, DistErasureSetupType, nil},

		{":9000", []string{case5Endpoint1, case5Endpoint2, case5Endpoint3, case5Endpoint4}, ":9000", Endpoints{
			Endpoint{URL: case5URLs[0], IsLocal: case5LocalFlags[0]},
			Endpoint{URL: case5URLs[1], IsLocal: case5LocalFlags[1]},
			Endpoint{URL: case5URLs[2], IsLocal: case5LocalFlags[2]},
			Endpoint{URL: case5URLs[3], IsLocal: case5LocalFlags[3]},
		}, DistErasureSetupType, nil},

		// DistErasure Setup using only local host.
		{":9003", []string{"http://localhost:9000/d1", "http://localhost:9001/d2", "http://127.0.0.1:9002/d3", case6Endpoint}, ":9003", Endpoints{
			Endpoint{URL: case6URLs[0], IsLocal: case6LocalFlags[0]},
			Endpoint{URL: case6URLs[1], IsLocal: case6LocalFlags[1]},
			Endpoint{URL: case6URLs[2], IsLocal: case6LocalFlags[2]},
			Endpoint{URL: case6URLs[3], IsLocal: case6LocalFlags[3]},
		}, DistErasureSetupType, nil},
	}

	for i, testCase := range testCases {
		testCase := testCase
		t.Run("", func(t *testing.T) {
			var srvCtxt serverCtxt
			err := mergeDisksLayoutFromArgs(testCase.args, &srvCtxt)
			if err != nil && testCase.expectedErr == nil {
				t.Errorf("Test %d: unexpected error: %v", i+1, err)
			}
			pools, setupType, err := CreatePoolEndpoints(testCase.serverAddr, srvCtxt.Layout.pools...)
			if err == nil && testCase.expectedErr != nil {
				t.Errorf("Test %d: expected = %v, got = <nil>", i+1, testCase.expectedErr)
			}
			if err == nil {
				if setupType != testCase.expectedSetupType {
					t.Errorf("Test %d: setupType: expected = %v, got = %v", i+1, testCase.expectedSetupType, setupType)
				}
				endpoints := pools[0]
				if len(endpoints) != len(testCase.expectedEndpoints) {
					t.Errorf("Test %d: endpoints: expected = %d, got = %d", i+1, len(testCase.expectedEndpoints),
						len(endpoints))
				} else {
					for i, endpoint := range endpoints {
						if testCase.expectedEndpoints[i].String() != endpoint.String() {
							t.Errorf("Test %d: endpoints: expected = %s, got = %s",
								i+1,
								testCase.expectedEndpoints[i],
								endpoint)
						}
					}
				}
			}
			if err != nil && testCase.expectedErr == nil {
				t.Errorf("Test %d: error: expected = <nil>, got = %v, testCase: %v", i+1, err, testCase)
			}
		})
	}
}

// Tests get local peer functionality, local peer is supposed to only return one entry per minio service.
// So it means that if you have say localhost:9000 and localhost:9001 as endpointArgs then localhost:9001
// is considered a remote service from localhost:9000 perspective.
func TestGetLocalPeer(t *testing.T) {
	tempGlobalMinioPort := globalMinioPort
	defer func() {
		globalMinioPort = tempGlobalMinioPort
	}()
	globalMinioPort = "9000"

	testCases := []struct {
		endpointArgs   []string
		expectedResult string
	}{
		{[]string{"/d1", "/d2", "d3", "d4"}, "127.0.0.1:9000"},
		{
			[]string{"http://localhost:9000/d1", "http://localhost:9000/d2", "http://example.org:9000/d3", "http://example.com:9000/d4"},
			"localhost:9000",
		},
		{
			[]string{"http://localhost:9000/d1", "http://example.org:9000/d2", "http://example.com:9000/d3", "http://example.net:9000/d4"},
			"localhost:9000",
		},
		{
			[]string{"http://localhost:9000/d1", "http://localhost:9001/d2", "http://localhost:9002/d3", "http://localhost:9003/d4"},
			"localhost:9000",
		},
	}

	for i, testCase := range testCases {
		zendpoints := mustGetPoolEndpoints(0, testCase.endpointArgs...)
		if !zendpoints[0].Endpoints[0].IsLocal {
			if err := zendpoints[0].Endpoints.UpdateIsLocal(); err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		}
		localPeer := GetLocalPeer(zendpoints, "", "9000")
		if localPeer != testCase.expectedResult {
			t.Fatalf("Test %d: expected: %v, got: %v", i+1, testCase.expectedResult, localPeer)
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
		expectedLocal  string
	}{
		{[]string{"/d1", "/d2", "d3", "d4"}, []string{}, ""},
		{[]string{"http://localhost:9000/d1", "http://localhost:9000/d2", "http://example.org:9000/d3", "http://example.com:9000/d4"}, []string{"example.com:9000", "example.org:9000", "localhost:9000"}, "localhost:9000"},
		{[]string{"http://localhost:9000/d1", "http://localhost:10000/d2", "http://example.org:9000/d3", "http://example.com:9000/d4"}, []string{"example.com:9000", "example.org:9000", "localhost:10000", "localhost:9000"}, "localhost:9000"},
		{[]string{"http://localhost:9000/d1", "http://example.org:9000/d2", "http://example.com:9000/d3", "http://example.net:9000/d4"}, []string{"example.com:9000", "example.net:9000", "example.org:9000", "localhost:9000"}, "localhost:9000"},
		{[]string{"http://localhost:9000/d1", "http://localhost:9001/d2", "http://localhost:9002/d3", "http://localhost:9003/d4"}, []string{"localhost:9000", "localhost:9001", "localhost:9002", "localhost:9003"}, "localhost:9000"},
	}

	for _, testCase := range testCases {
		zendpoints := mustGetPoolEndpoints(0, testCase.endpointArgs...)
		if !zendpoints[0].Endpoints[0].IsLocal {
			if err := zendpoints[0].Endpoints.UpdateIsLocal(); err != nil {
				t.Errorf("error: expected = <nil>, got = %v", err)
			}
		}
		remotePeers, local := zendpoints.peers()
		if !reflect.DeepEqual(remotePeers, testCase.expectedResult) {
			t.Errorf("expected: %v, got: %v", testCase.expectedResult, remotePeers)
		}
		if local != testCase.expectedLocal {
			t.Errorf("expected: %v, got: %v", testCase.expectedLocal, local)
		}
	}
}
