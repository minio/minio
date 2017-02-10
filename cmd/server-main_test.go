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
	"net/url"
	"os"
	"reflect"
	"runtime"
	"testing"

	"github.com/minio/cli"
)

// Returns if slice of disks is a distributed setup.
func isDistributedSetup(eps []*url.URL) bool {
	// Validate if one the disks is not local.
	for _, ep := range eps {
		if !isLocalStorage(ep) {
			// One or more disks supplied as arguments are
			// not attached to the local node.
			return true
		}
	}
	return false
}

func TestGetListenIPs(t *testing.T) {
	testCases := []struct {
		addr       string
		port       string
		shouldPass bool
	}{
		{"localhost", "9000", true},
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

// Tests finalize api endpoints.
func TestFinalizeAPIEndpoints(t *testing.T) {
	testCases := []struct {
		addr string
	}{
		{":80"},
		{":80"},
		{"localhost:80"},
		{"localhost:80"},
	}

	for i, test := range testCases {
		endPoints, err := finalizeAPIEndpoints(test.addr)
		if err != nil && len(endPoints) <= 0 {
			t.Errorf("Test case %d returned with no API end points for %s",
				i+1, test.addr)
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
		{"", "http://localhost/export", nil},
		{
			"testhost",
			"http://localhost/export",
			errors.New("Invalid Argument localhost, port mandatory when --address <host>:<port> is used"),
		},
		{
			"",
			"http://localhost:9000/export",
			errors.New("Invalid Argument localhost:9000, port configurable using --address :<port>"),
		},
		{"testhost", "http://localhost:9000/export", nil},
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
	ctx := cli.NewContext(app, serverFlagSet, serverFlagSet)

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
