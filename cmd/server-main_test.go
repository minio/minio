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
	"errors"
	"flag"
	"os"
	"reflect"
	"runtime"
	"testing"

	"github.com/minio/cli"
)

func TestGetListenIPs(t *testing.T) {
	testCases := []struct {
		addr       string
		port       string
		shouldPass bool
	}{
		{"127.0.0.1", "9000", true},
		{"", "9000", true},
		{"", "", false},
	}
	for _, test := range testCases {
		var addr string
		// Please keep this we need to do this because
		// of odd https://play.golang.org/p/4dMPtM6Wdd
		// implementation issue.
		if test.port != "" {
			addr = test.addr + ":" + test.port
		}
		hosts, port, err := getListenIPs(addr)
		if !test.shouldPass && err == nil {
			t.Fatalf("Test should fail but succeeded %s", err)
		}
		if test.shouldPass && err != nil {
			t.Fatalf("Test should succeed but failed %s", err)
		}
		if test.shouldPass {
			if port != test.port {
				t.Errorf("Test expected %s, got %s", test.port, port)
			}
			if len(hosts) == 0 {
				t.Errorf("Test unexpected value hosts cannot be empty %#v", test)
			}
		}
	}
}

// Tests get host port.
func TestGetHostPort(t *testing.T) {
	testCases := []struct {
		addr string
		err  error
	}{
		// Test 1 - successful.
		{
			addr: ":" + getFreePort(),
			err:  nil,
		},
		// Test 2 port empty.
		{
			addr: ":0",
			err:  errEmptyPort,
		},
		// Test 3 port empty.
		{
			addr: ":",
			err:  errEmptyPort,
		},
		// Test 4 invalid port.
		{
			addr: "linux:linux",
			err:  errors.New("strconv.Atoi: parsing \"linux\": invalid syntax"),
		},
		// Test 5 port not present.
		{
			addr: "hostname",
			err:  errors.New("address hostname: missing port in address"),
		},
	}

	// Validate all tests.
	for i, testCase := range testCases {
		_, _, err := getHostPort(testCase.addr)
		if err != nil {
			if err.Error() != testCase.err.Error() {
				t.Fatalf("Test %d: Error: %s", i+1, err)
			}
		}
	}
}

// Tests finalize api endpoints.
func TestFinalizeAPIEndpoints(t *testing.T) {
	testCases := []struct {
		addr string
	}{
		{":80"},
		{":80"},
		{"127.0.0.1:80"},
		{"127.0.0.1:80"},
	}

	for i, test := range testCases {
		endPoints, err := finalizeAPIEndpoints(test.addr)
		if err != nil && len(endPoints) <= 0 {
			t.Errorf("Test case %d returned with no API end points for %s",
				i+1, test.addr)
		}
	}
}

// Tests all the expected input disks for function checkSufficientDisks.
func TestCheckSufficientDisks(t *testing.T) {
	var xlDisks []string
	if runtime.GOOS == globalWindowsOSName {
		xlDisks = []string{
			"C:\\mnt\\backend1",
			"C:\\mnt\\backend2",
			"C:\\mnt\\backend3",
			"C:\\mnt\\backend4",
			"C:\\mnt\\backend5",
			"C:\\mnt\\backend6",
			"C:\\mnt\\backend7",
			"C:\\mnt\\backend8",
			"C:\\mnt\\backend9",
			"C:\\mnt\\backend10",
			"C:\\mnt\\backend11",
			"C:\\mnt\\backend12",
			"C:\\mnt\\backend13",
			"C:\\mnt\\backend14",
			"C:\\mnt\\backend15",
			"C:\\mnt\\backend16",
			"C:\\mnt\\backend17",
		}
	} else {
		xlDisks = []string{
			"/mnt/backend1",
			"/mnt/backend2",
			"/mnt/backend3",
			"/mnt/backend4",
			"/mnt/backend5",
			"/mnt/backend6",
			"/mnt/backend7",
			"/mnt/backend8",
			"/mnt/backend9",
			"/mnt/backend10",
			"/mnt/backend11",
			"/mnt/backend12",
			"/mnt/backend13",
			"/mnt/backend14",
			"/mnt/backend15",
			"/mnt/backend16",
			"/mnt/backend17",
		}
	}
	// List of test cases fo sufficient disk verification.
	testCases := []struct {
		disks       []string
		expectedErr error
	}{
		// Even number of disks '6'.
		{
			xlDisks[0:6],
			nil,
		},
		// Even number of disks '12'.
		{
			xlDisks[0:12],
			nil,
		},
		// Even number of disks '16'.
		{
			xlDisks[0:16],
			nil,
		},
		// Larger than maximum number of disks > 16.
		{
			xlDisks,
			errXLMaxDisks,
		},
		// Lesser than minimum number of disks < 6.
		{
			xlDisks[0:3],
			errXLMinDisks,
		},
		// Odd number of disks, not divisible by '2'.
		{
			append(xlDisks[0:10], xlDisks[11]),
			errXLNumDisks,
		},
	}

	// Validates different variations of input disks.
	for i, testCase := range testCases {
		endpoints, err := parseStorageEndpoints(testCase.disks)
		if err != nil {
			t.Fatalf("Unexpected error %s", err)
		}
		if checkSufficientDisks(endpoints) != testCase.expectedErr {
			t.Errorf("Test %d expected to pass for disks %s", i+1, testCase.disks)
		}
	}
}

// Tests initializing new object layer.
func TestNewObjectLayer(t *testing.T) {
	// Tests for FS object layer.
	nDisks := 1
	disks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal("Failed to create disks for the backend")
	}
	defer removeRoots(disks)

	endpoints, err := parseStorageEndpoints(disks)
	if err != nil {
		t.Fatal("Unexpected parse error", err)
	}

	obj, err := newObjectLayer(serverCmdConfig{
		serverAddr: ":9000",
		endpoints:  endpoints,
	})
	if err != nil {
		t.Fatal("Unexpected object layer initialization error", err)
	}
	_, ok := obj.(*fsObjects)
	if !ok {
		t.Fatal("Unexpected object layer detected", reflect.TypeOf(obj))
	}

	// Tests for XL object layer initialization.

	// Create temporary backend for the test server.
	nDisks = 16
	disks, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal("Failed to create disks for the backend")
	}
	defer removeRoots(disks)

	endpoints, err = parseStorageEndpoints(disks)
	if err != nil {
		t.Fatal("Unexpected parse error", err)
	}

	obj, err = newObjectLayer(serverCmdConfig{
		serverAddr: ":9000",
		endpoints:  endpoints,
	})
	if err != nil {
		t.Fatal("Unexpected object layer initialization error", err)
	}

	_, ok = obj.(*xlObjects)
	if !ok {
		t.Fatal("Unexpected object layer detected", reflect.TypeOf(obj))
	}
}

// Tests parsing various types of input endpoints and paths.
func TestParseStorageEndpoints(t *testing.T) {
	testCases := []struct {
		globalMinioHost string
		host            string
		expectedErr     error
	}{
		{"", "http://127.0.0.1/export", nil},
		{
			"testhost",
			"http://127.0.0.1/export",
			errors.New("Invalid Argument 127.0.0.1, port mandatory when --address <host>:<port> is used"),
		},
		{
			"",
			"http://127.0.0.1:9000/export",
			errors.New("Invalid Argument 127.0.0.1:9000, port configurable using --address :<port>"),
		},
		{"testhost", "http://127.0.0.1:9000/export", nil},
	}
	for i, test := range testCases {
		globalMinioHost = test.globalMinioHost
		_, err := parseStorageEndpoints([]string{test.host})
		if err != nil {
			if err.Error() != test.expectedErr.Error() {
				t.Errorf("Test %d : got %v, expected %v", i+1, err, test.expectedErr)
			}
		}
	}
	// Should be reset back to "" so that we don't affect other tests.
	globalMinioHost = ""
}

// Test check endpoints syntax function for syntax verification
// across various scenarios of inputs.
func TestCheckEndpointsSyntax(t *testing.T) {
	successCases := []string{
		"export",
		"/export",
		"http://127.0.0.1/export",
		"https://127.0.0.1/export",
	}

	failureCases := []string{
		"/",
		"http://127.0.0.1",
		"http://127.0.0.1/",
		"ftp://127.0.0.1/export",
		"server:/export",
	}

	if runtime.GOOS == globalWindowsOSName {
		successCases = append(successCases,
			`\export`,
			`D:\export`,
		)

		failureCases = append(failureCases,
			"D:",
			`D:\`,
			`\`,
		)
	}

	for _, disk := range successCases {
		eps, err := parseStorageEndpoints([]string{disk})
		if err != nil {
			t.Fatalf("Unable to parse %s, error %s", disk, err)
		}
		if err = checkEndpointsSyntax(eps, []string{disk}); err != nil {
			t.Errorf("expected: <nil>, got: %s", err)
		}
	}

	for _, disk := range failureCases {
		eps, err := parseStorageEndpoints([]string{disk})
		if err != nil {
			t.Fatalf("Unable to parse %s, error %s", disk, err)
		}
		if err = checkEndpointsSyntax(eps, []string{disk}); err == nil {
			t.Errorf("expected: <error>, got: <nil>")
		}
	}
}

// Tests check server syntax.
func TestCheckServerSyntax(t *testing.T) {
	app := cli.NewApp()
	app.Commands = []cli.Command{serverCmd}
	serverFlagSet := flag.NewFlagSet("server", 0)
	serverFlagSet.String("address", ":9000", "")
	ctx := cli.NewContext(app, serverFlagSet, nil)

	disksGen := func(n int) []string {
		disks, err := getRandomDisks(n)
		if err != nil {
			t.Fatalf("Unable to initialie disks %s", err)
		}
		return disks
	}
	testCases := [][]string{
		disksGen(1),
		disksGen(4),
		disksGen(8),
		disksGen(16),
	}

	for i, disks := range testCases {
		err := serverFlagSet.Parse(disks)
		if err != nil {
			t.Errorf("Test %d failed to parse arguments %s", i+1, disks)
		}
		defer removeRoots(disks)
		checkServerSyntax(ctx)
	}
}

func TestIsDistributedSetup(t *testing.T) {
	var testCases []struct {
		disks  []string
		result bool
	}
	if runtime.GOOS == globalWindowsOSName {
		testCases = []struct {
			disks  []string
			result bool
		}{
			{[]string{`http://4.4.4.4/c:\mnt\disk1`, `http://4.4.4.4/c:\mnt\disk2`}, true},
			{[]string{`http://4.4.4.4/c:\mnt\disk1`, `http://127.0.0.1/c:\mnt\disk2`}, true},
			{[]string{`c:\mnt\disk1`, `c:\mnt\disk2`}, false},
		}
	} else {
		testCases = []struct {
			disks  []string
			result bool
		}{
			{[]string{"http://4.4.4.4/mnt/disk1", "http://4.4.4.4/mnt/disk2"}, true},
			{[]string{"http://4.4.4.4/mnt/disk1", "http://127.0.0.1/mnt/disk2"}, true},
			{[]string{"/mnt/disk1", "/mnt/disk2"}, false},
		}
	}
	for i, test := range testCases {
		endpoints, err := parseStorageEndpoints(test.disks)
		if err != nil {
			t.Fatalf("Test %d: Unexpected error: %s", i+1, err)
		}
		res := isDistributedSetup(endpoints)
		if res != test.result {
			t.Errorf("Test %d: expected result %t but received %t", i+1, test.result, res)
		}
	}

	// Test cases when globalMinioHost is set
	globalMinioHost = "testhost"
	testCases = []struct {
		disks  []string
		result bool
	}{
		{[]string{"http://127.0.0.1:9001/mnt/disk1", "http://127.0.0.1:9002/mnt/disk2", "http://127.0.0.1:9003/mnt/disk3", "http://127.0.0.1:9004/mnt/disk4"}, true},
		{[]string{"/mnt/disk1", "/mnt/disk2"}, false},
	}

	for i, test := range testCases {
		endpoints, err := parseStorageEndpoints(test.disks)
		if err != nil {
			t.Fatalf("Test %d: Unexpected error: %s", i+1, err)
		}
		res := isDistributedSetup(endpoints)
		if res != test.result {
			t.Errorf("Test %d: expected result %t but received %t", i+1, test.result, res)
		}
	}
	// Reset so that we don't affect other tests.
	globalMinioHost = ""
}

// Tests init server.
func TestInitServer(t *testing.T) {
	app := cli.NewApp()
	app.Commands = []cli.Command{serverCmd}
	serverFlagSet := flag.NewFlagSet("server", 0)
	serverFlagSet.String("address", ":9000", "")
	ctx := cli.NewContext(app, serverFlagSet, nil)

	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal("Failed to set up test config")
	}
	defer removeAll(root)

	testCases := []struct {
		envVar string
		val    string
	}{
		{"MINIO_ACCESS_KEY", "abcd1"},
		{"MINIO_SECRET_KEY", "abcd12345"},
	}
	for i, test := range testCases {
		tErr := os.Setenv(test.envVar, test.val)
		if tErr != nil {
			t.Fatalf("Test %d failed with %v", i+1, tErr)
		}
		initServerConfig(ctx)
		os.Unsetenv(test.envVar)
	}
}

// Tests isAnyEndpointLocal function with inputs such that it returns true and false respectively.
func TestIsAnyEndpointLocal(t *testing.T) {
	testCases := []struct {
		disks  []string
		result bool
	}{
		{
			disks: []string{"http://4.4.4.4/mnt/disk1",
				"http://4.4.4.4/mnt/disk1"},
			result: false,
		},
		{
			disks: []string{"http://127.0.0.1/mnt/disk1",
				"http://127.0.0.1/mnt/disk1"},
			result: true,
		},
	}
	for i, test := range testCases {
		endpoints, err := parseStorageEndpoints(test.disks)
		if err != nil {
			t.Fatalf("Test %d - Failed to parse storage endpoints %v", i+1, err)
		}
		actual := isAnyEndpointLocal(endpoints)
		if actual != test.result {
			t.Errorf("Test %d - Expected %v but received %v", i+1, test.result, actual)
		}
	}
}
