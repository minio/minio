/*
 * MinIO Cloud Storage, (C) 2017,2018,2019 MinIO, Inc.
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
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/minio/minio-go/v6/pkg/set"
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
		{"/foo", Endpoint{URL: &url.URL{Path: rootSlashFoo}, IsLocal: true}, PathEndpointType, nil},
		{"https://example.org/path", Endpoint{URL: u2, IsLocal: false}, URLEndpointType, nil},
		{"http://192.168.253.200/path", Endpoint{URL: u4, IsLocal: false}, URLEndpointType, nil},
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

			if test.expectedErr == nil {
				if err != nil {
					t.Errorf("error: expected = <nil>, got = %v", err)
				}
			} else if err == nil {
				t.Errorf("error: expected = %v, got = <nil>", test.expectedErr)
			} else if test.expectedErr.Error() != err.Error() {
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
		args               [][]string
		expectedServerAddr string
		expectedEndpoints  Endpoints
		expectedSetupType  SetupType
		expectedErr        error
	}{
		{"localhost", [][]string{}, "", Endpoints{}, -1, fmt.Errorf("address localhost: missing port in address")},

		// FS Setup
		{"localhost:9000", [][]string{{"http://localhost/d1"}}, "", Endpoints{}, -1, fmt.Errorf("use path style endpoint for FS setup")},
		{":443", [][]string{{"/d1"}}, ":443", Endpoints{Endpoint{URL: &url.URL{Path: mustAbs("/d1")}, IsLocal: true}}, FSSetupType, nil},
		{"localhost:10000", [][]string{{"/d1"}}, "localhost:10000", Endpoints{Endpoint{URL: &url.URL{Path: mustAbs("/d1")}, IsLocal: true}}, FSSetupType, nil},
		{"localhost:9000", [][]string{{"https://127.0.0.1:9000/d1", "https://localhost:9001/d1", "https://example.com/d1", "https://example.com/d2"}}, "", Endpoints{}, -1, fmt.Errorf("path '/d1' can not be served by different port on same address")},

		// Erasure Setup with PathEndpointType
		{":1234", [][]string{{"/d1", "/d2", "/d3", "/d4"}}, ":1234",
			Endpoints{
				Endpoint{URL: &url.URL{Path: mustAbs("/d1")}, IsLocal: true},
				Endpoint{URL: &url.URL{Path: mustAbs("/d2")}, IsLocal: true},
				Endpoint{URL: &url.URL{Path: mustAbs("/d3")}, IsLocal: true},
				Endpoint{URL: &url.URL{Path: mustAbs("/d4")}, IsLocal: true},
			}, ErasureSetupType, nil},
		// DistErasure Setup with URLEndpointType
		{":9000", [][]string{{"http://localhost/d1", "http://localhost/d2", "http://localhost/d3", "http://localhost/d4"}}, ":9000", Endpoints{
			Endpoint{URL: &url.URL{Scheme: "http", Host: "localhost", Path: "/d1"}, IsLocal: true},
			Endpoint{URL: &url.URL{Scheme: "http", Host: "localhost", Path: "/d2"}, IsLocal: true},
			Endpoint{URL: &url.URL{Scheme: "http", Host: "localhost", Path: "/d3"}, IsLocal: true},
			Endpoint{URL: &url.URL{Scheme: "http", Host: "localhost", Path: "/d4"}, IsLocal: true},
		}, DistErasureSetupType, nil},
		// DistErasure Setup with URLEndpointType having mixed naming to local host.
		{"127.0.0.1:10000", [][]string{{"http://localhost/d1", "http://localhost/d2", "http://127.0.0.1/d3", "http://127.0.0.1/d4"}}, "", Endpoints{}, -1, fmt.Errorf("all local endpoints should not have different hostnames/ips")},

		{":9001", [][]string{{"http://10.0.0.1:9000/export", "http://10.0.0.2:9000/export", "http://" + nonLoopBackIP + ":9001/export", "http://10.0.0.2:9001/export"}}, "", Endpoints{}, -1, fmt.Errorf("path '/export' can not be served by different port on same address")},

		{":9000", [][]string{{"http://127.0.0.1:9000/export", "http://" + nonLoopBackIP + ":9000/export", "http://10.0.0.1:9000/export", "http://10.0.0.2:9000/export"}}, "", Endpoints{}, -1, fmt.Errorf("path '/export' cannot be served by different address on same server")},

		// DistErasure type
		{"127.0.0.1:10000", [][]string{{case1Endpoint1, case1Endpoint2, "http://example.org/d3", "http://example.com/d4"}}, "127.0.0.1:10000", Endpoints{
			Endpoint{URL: case1URLs[0], IsLocal: case1LocalFlags[0]},
			Endpoint{URL: case1URLs[1], IsLocal: case1LocalFlags[1]},
			Endpoint{URL: case1URLs[2], IsLocal: case1LocalFlags[2]},
			Endpoint{URL: case1URLs[3], IsLocal: case1LocalFlags[3]},
		}, DistErasureSetupType, nil},

		{"127.0.0.1:10000", [][]string{{case2Endpoint1, case2Endpoint2, "http://example.org/d3", "http://example.com/d4"}}, "127.0.0.1:10000", Endpoints{
			Endpoint{URL: case2URLs[0], IsLocal: case2LocalFlags[0]},
			Endpoint{URL: case2URLs[1], IsLocal: case2LocalFlags[1]},
			Endpoint{URL: case2URLs[2], IsLocal: case2LocalFlags[2]},
			Endpoint{URL: case2URLs[3], IsLocal: case2LocalFlags[3]},
		}, DistErasureSetupType, nil},

		{":80", [][]string{{case3Endpoint1, "http://example.org:9000/d2", "http://example.com/d3", "http://example.net/d4"}}, ":80", Endpoints{
			Endpoint{URL: case3URLs[0], IsLocal: case3LocalFlags[0]},
			Endpoint{URL: case3URLs[1], IsLocal: case3LocalFlags[1]},
			Endpoint{URL: case3URLs[2], IsLocal: case3LocalFlags[2]},
			Endpoint{URL: case3URLs[3], IsLocal: case3LocalFlags[3]},
		}, DistErasureSetupType, nil},

		{":9000", [][]string{{case4Endpoint1, "http://example.org/d2", "http://example.com/d3", "http://example.net/d4"}}, ":9000", Endpoints{
			Endpoint{URL: case4URLs[0], IsLocal: case4LocalFlags[0]},
			Endpoint{URL: case4URLs[1], IsLocal: case4LocalFlags[1]},
			Endpoint{URL: case4URLs[2], IsLocal: case4LocalFlags[2]},
			Endpoint{URL: case4URLs[3], IsLocal: case4LocalFlags[3]},
		}, DistErasureSetupType, nil},

		{":9000", [][]string{{case5Endpoint1, case5Endpoint2, case5Endpoint3, case5Endpoint4}}, ":9000", Endpoints{
			Endpoint{URL: case5URLs[0], IsLocal: case5LocalFlags[0]},
			Endpoint{URL: case5URLs[1], IsLocal: case5LocalFlags[1]},
			Endpoint{URL: case5URLs[2], IsLocal: case5LocalFlags[2]},
			Endpoint{URL: case5URLs[3], IsLocal: case5LocalFlags[3]},
		}, DistErasureSetupType, nil},

		// DistErasure Setup using only local host.
		{":9003", [][]string{{"http://localhost:9000/d1", "http://localhost:9001/d2", "http://127.0.0.1:9002/d3", case6Endpoint}}, ":9003", Endpoints{
			Endpoint{URL: case6URLs[0], IsLocal: case6LocalFlags[0]},
			Endpoint{URL: case6URLs[1], IsLocal: case6LocalFlags[1]},
			Endpoint{URL: case6URLs[2], IsLocal: case6LocalFlags[2]},
			Endpoint{URL: case6URLs[3], IsLocal: case6LocalFlags[3]},
		}, DistErasureSetupType, nil},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run("", func(t *testing.T) {
			endpoints, setupType, err := CreateEndpoints(testCase.serverAddr, false, testCase.args...)
			if err == nil && testCase.expectedErr != nil {
				t.Errorf("error: expected = %v, got = <nil>", testCase.expectedErr)
			}
			if err == nil {
				if setupType != testCase.expectedSetupType {
					t.Errorf("setupType: expected = %v, got = %v", testCase.expectedSetupType, setupType)
				}
				if len(endpoints) != len(testCase.expectedEndpoints) {
					t.Errorf("endpoints: expected = %d, got = %d", len(testCase.expectedEndpoints),
						len(endpoints))
				} else {
					for i, endpoint := range endpoints {
						if testCase.expectedEndpoints[i].String() != endpoint.String() {
							t.Errorf("endpoints: expected = %s, got = %s",
								testCase.expectedEndpoints[i],
								endpoint)
						}
					}
				}
			}
			if err != nil && testCase.expectedErr == nil {
				t.Errorf("error: expected = <nil>, got = %v, testCase: %v", err, testCase)
			}
		})
	}
}

// Tests get local peer functionality, local peer is supposed to only return one entry per minio service.
// So it means that if you have say localhost:9000 and localhost:9001 as endpointArgs then localhost:9001
// is considered a remote service from localhost:9000 perspective.
func TestGetLocalPeer(t *testing.T) {
	tempGlobalMinioAddr := globalMinioAddr
	tempGlobalMinioPort := globalMinioPort
	defer func() {
		globalMinioAddr = tempGlobalMinioAddr
		globalMinioPort = tempGlobalMinioPort
	}()
	globalMinioAddr = ":9000"
	globalMinioPort = "9000"

	testCases := []struct {
		endpointArgs   []string
		expectedResult string
	}{
		{[]string{"/d1", "/d2", "d3", "d4"}, "127.0.0.1:9000"},
		{[]string{"http://localhost:9000/d1", "http://localhost:9000/d2", "http://example.org:9000/d3", "http://example.com:9000/d4"},
			"localhost:9000"},
		{[]string{"http://localhost:9000/d1", "http://example.org:9000/d2", "http://example.com:9000/d3", "http://example.net:9000/d4"},
			"localhost:9000"},
		{[]string{"http://localhost:9000/d1", "http://localhost:9001/d2", "http://localhost:9002/d3", "http://localhost:9003/d4"},
			"localhost:9000"},
	}

	for i, testCase := range testCases {
		zendpoints := mustGetZoneEndpoints(testCase.endpointArgs...)
		if !zendpoints[0].Endpoints[0].IsLocal {
			if err := zendpoints[0].Endpoints.UpdateIsLocal(false); err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		}
		remotePeer := GetLocalPeer(zendpoints)
		if remotePeer != testCase.expectedResult {
			t.Fatalf("Test %d: expected: %v, got: %v", i+1, testCase.expectedResult, remotePeer)
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
		zendpoints := mustGetZoneEndpoints(testCase.endpointArgs...)
		if !zendpoints[0].Endpoints[0].IsLocal {
			if err := zendpoints[0].Endpoints.UpdateIsLocal(false); err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		}
		remotePeers := GetRemotePeers(zendpoints)
		if !reflect.DeepEqual(remotePeers, testCase.expectedResult) {
			t.Fatalf("expected: %v, got: %v", testCase.expectedResult, remotePeers)
		}
	}
}

func TestUpdateDomainIPs(t *testing.T) {
	tempGlobalMinioPort := globalMinioPort
	defer func() {
		globalMinioPort = tempGlobalMinioPort
	}()
	globalMinioPort = "9000"

	tempGlobalDomainIPs := globalDomainIPs
	defer func() {
		globalDomainIPs = tempGlobalDomainIPs
	}()

	ipv4TestCases := []struct {
		endPoints      set.StringSet
		expectedResult set.StringSet
	}{
		{set.NewStringSet(), set.NewStringSet()},
		{set.CreateStringSet("localhost"), set.NewStringSet()},
		{set.CreateStringSet("localhost", "10.0.0.1"), set.CreateStringSet("10.0.0.1:9000")},
		{set.CreateStringSet("localhost:9001", "10.0.0.1"), set.CreateStringSet("10.0.0.1:9000")},
		{set.CreateStringSet("localhost", "10.0.0.1:9001"), set.CreateStringSet("10.0.0.1:9001")},
		{set.CreateStringSet("localhost:9000", "10.0.0.1:9001"), set.CreateStringSet("10.0.0.1:9001")},

		{set.CreateStringSet("10.0.0.1", "10.0.0.2"), set.CreateStringSet("10.0.0.1:9000", "10.0.0.2:9000")},
		{set.CreateStringSet("10.0.0.1:9001", "10.0.0.2"), set.CreateStringSet("10.0.0.1:9001", "10.0.0.2:9000")},
		{set.CreateStringSet("10.0.0.1", "10.0.0.2:9002"), set.CreateStringSet("10.0.0.1:9000", "10.0.0.2:9002")},
		{set.CreateStringSet("10.0.0.1:9001", "10.0.0.2:9002"), set.CreateStringSet("10.0.0.1:9001", "10.0.0.2:9002")},
	}

	for _, testCase := range ipv4TestCases {
		globalDomainIPs = nil

		updateDomainIPs(testCase.endPoints)

		if !testCase.expectedResult.Equals(globalDomainIPs) {
			t.Fatalf("error: expected = %s, got = %s", testCase.expectedResult, globalDomainIPs)
		}
	}
}
