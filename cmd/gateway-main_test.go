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
	"strings"
	"testing"

	"github.com/minio/cli"
)

// Test RegisterGatewayCommand
func TestRegisterGatewayCommand(t *testing.T) {
	var err error

	cmd := cli.Command{Name: "test"}
	err = RegisterGatewayCommand(cmd)
	if err != nil {
		t.Errorf("RegisterGatewayCommand got unexpected error: %s", err)
	}
}

// Test parseGatewayEndpoint
func TestParseGatewayEndpoint(t *testing.T) {
	testCases := []struct {
		arg         string
		endPoint    string
		secure      bool
		errReturned bool
	}{
		{"http://127.0.0.1:9000", "127.0.0.1:9000", false, false},
		{"https://127.0.0.1:9000", "127.0.0.1:9000", true, false},
		{"http://play.minio.io:9000", "play.minio.io:9000", false, false},
		{"https://play.minio.io:9000", "play.minio.io:9000", true, false},
		{"ftp://127.0.0.1:9000", "", false, true},
		{"ftp://play.minio.io:9000", "", false, true},
		{"play.minio.io:9000", "play.minio.io:9000", true, false},
	}

	for i, test := range testCases {
		endPoint, secure, err := ParseGatewayEndpoint(test.arg)
		errReturned := err != nil

		if endPoint != test.endPoint ||
			secure != test.secure ||
			errReturned != test.errReturned {
			t.Errorf("Test %d: expected %s,%t,%t got %s,%t,%t",
				i+1, test.endPoint, test.secure, test.errReturned,
				endPoint, secure, errReturned)
		}
	}
}

// Test validateGatewayArguments
func TestValidateGatewayArguments(t *testing.T) {
	nonLoopBackIPs := localIP4.FuncMatch(func(ip string, matchString string) bool {
		return !strings.HasPrefix(ip, "127.")
	}, "")
	if len(nonLoopBackIPs) == 0 {
		t.Fatalf("No non-loop back IP address found for this host")
	}
	nonLoopBackIP := nonLoopBackIPs.ToSlice()[0]

	testCases := []struct {
		serverAddr   string
		endpointAddr string
		valid        bool
	}{
		{":9000", "http://localhost:9001", true},
		{":9000", "http://google.com", true},
		{"123.123.123.123:9000", "http://localhost:9000", false},
		{":9000", "http://localhost:9000", false},
		{":9000", nonLoopBackIP + ":9000", false},
	}
	for i, test := range testCases {
		err := ValidateGatewayArguments(test.serverAddr, test.endpointAddr)
		if test.valid && err != nil {
			t.Errorf("Test %d expected not to return error but got %s", i+1, err)
		}
		if !test.valid && err == nil {
			t.Errorf("Test %d expected to fail but it did not", i+1)
		}
	}
}
