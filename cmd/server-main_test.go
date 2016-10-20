/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/minio/cli"
)

func TestGetListenIPs(t *testing.T) {
	testCases := []struct {
		addr string
		port string
	}{
		{"localhost", ":80"},
		{"", ":80"},
	}
	for _, test := range testCases {
		ts := &http.Server{Addr: test.addr + test.port}
		getListenIPs(ts)
	}
}

func TestFinalizeEndpoints(t *testing.T) {
	testCases := []struct {
		tls  bool
		addr string
	}{
		{false, ":80"},
		{true, ":80"},
		{false, "localhost:80"},
		{true, "localhost:80"},
	}

	for i, test := range testCases {
		endPoints := finalizeEndpoints(test.tls, &http.Server{Addr: test.addr})
		if len(endPoints) <= 0 {
			t.Errorf("Test case %d returned with no API end points for %s",
				i+1, test.addr)
		}
	}
}

// Tests all the expected input disks for function checkSufficientDisks.
func TestCheckSufficientDisks(t *testing.T) {
	xlDisks := []string{
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
			append(xlDisks[0:16], "/mnt/unsupported"),
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
		if checkSufficientDisks(parseStorageEndPoints(testCase.disks, 0)) != testCase.expectedErr {
			t.Errorf("Test %d expected to pass for disks %s", i+1, testCase.disks)
		}
	}
}

func TestCheckServerSyntax(t *testing.T) {
	app := cli.NewApp()
	app.Commands = []cli.Command{serverCmd}
	serverFlagSet := flag.NewFlagSet("server", 0)
	cli.NewContext(app, serverFlagSet, nil)
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
		_ = validateDisks(parseStorageEndPoints(disks, 0), nil)
	}
}

func TestGetPort(t *testing.T) {
	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal("failed to create test config")
	}
	defer removeAll(root)

	testCases := []struct {
		addr         string
		ssl          bool
		expectedPort int
	}{
		{"localhost:1234", true, 1234},
		{"localhost:1234", false, 1234},
		{"localhost", true, 443},
		{"localhost", false, 80},
	}

	certFile := filepath.Join(mustGetCertsPath(), globalMinioCertFile)
	keyFile := filepath.Join(mustGetCertsPath(), globalMinioKeyFile)
	err = os.MkdirAll(filepath.Dir(certFile), 0755)
	if err != nil {
		t.Fatalf("Couldn't create certs directory.")
	}
	for i, test := range testCases {
		if test.ssl {
			cFile, cErr := os.Create(certFile)
			if cErr != nil {
				t.Fatalf("Failed to create cert file %s", certFile)
			}
			cFile.Close()

			tFile, tErr := os.Create(keyFile)
			if tErr != nil {
				t.Fatalf("Failed to create key file %s", keyFile)
			}
			tFile.Close()
		}
		port := getPort(test.addr)
		if port != test.expectedPort {
			t.Errorf("Test %d expected port %d but received %d", i+1, test.expectedPort, port)
		}
		if test.ssl {
			os.Remove(certFile)
			os.Remove(keyFile)
		}
	}
}

func TestIsDistributedSetup(t *testing.T) {
	var testCases []struct {
		disks  []string
		result bool
	}
	if runtime.GOOS == "windows" {
		testCases = []struct {
			disks  []string
			result bool
		}{
			{[]string{`4.4.4.4:c:\mnt\disk1`, `4.4.4.4:c:\mnt\disk2`}, true},
			{[]string{`4.4.4.4:c:\mnt\disk1`, `localhost:c:\mnt\disk2`}, true},
			{[]string{`localhost:c:\mnt\disk1`, `localhost:c:\mnt\disk2`}, false},
			{[]string{`c:\mnt\disk1`, `c:\mnt\disk2`}, false},
		}

	} else {
		testCases = []struct {
			disks  []string
			result bool
		}{
			{[]string{"4.4.4.4:/mnt/disk1", "4.4.4.4:/mnt/disk2"}, true},
			{[]string{"4.4.4.4:/mnt/disk1", "localhost:/mnt/disk2"}, true},
			{[]string{"localhost:/mnt/disk1", "localhost:/mnt/disk2"}, false},
			{[]string{"/mnt/disk1", "/mnt/disk2"}, false},
		}

	}
	for i, test := range testCases {
		res := isDistributedSetup(parseStorageEndPoints(test.disks, 0))
		if res != test.result {
			t.Errorf("Test %d: expected result %t but received %t", i+1, test.result, res)
		}
	}
}

func TestInitServerConfig(t *testing.T) {
	ctx := &cli.Context{}
	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal("Failed to set up test config")
	}
	defer removeAll(root)

	testCases := []struct {
		envVar string
		val    string
	}{
		{"MINIO_MAXCONN", "10"},
		{"MINIO_CACHE_SIZE", "42MB"},
		{"MINIO_CACHE_EXPIRY", "2h45m"},
		{"MINIO_ACCESS_KEY", "abcd1"},
		{"MINIO_SECRET_KEY", "abcd12345"},
	}
	for i, test := range testCases {
		tErr := os.Setenv(test.envVar, test.val)
		if tErr != nil {
			t.Fatalf("Test %d failed with %v", i+1, tErr)
		}
		initServerConfig(ctx)
	}
}
