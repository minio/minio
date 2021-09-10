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

// Test running a registered gateway command with a flag
func TestRunRegisteredGatewayCommand(t *testing.T) {
	var err error

	flagName := "test-flag"
	flagValue := "foo"

	cmd := cli.Command{
		Name: "test-run-with-flag",
		Flags: []cli.Flag{
			cli.StringFlag{Name: flagName},
		},
		Action: func(ctx *cli.Context) {
			if actual := ctx.String(flagName); actual != flagValue {
				t.Errorf("value of %s expects %s, but got %s", flagName, flagValue, actual)
			}
		},
	}

	err = RegisterGatewayCommand(cmd)
	if err != nil {
		t.Errorf("RegisterGatewayCommand got unexpected error: %s", err)
	}

	if err = newApp("minio").Run(
		[]string{"minio", "gateway", cmd.Name, fmt.Sprintf("--%s", flagName), flagValue}); err != nil {
		t.Errorf("running registered gateway command got unexpected error: %s", err)
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
		{"http://play.min.io:9000", "play.min.io:9000", false, false},
		{"https://play.min.io:9000", "play.min.io:9000", true, false},
		{"ftp://127.0.0.1:9000", "", false, true},
		{"ftp://play.min.io:9000", "", false, true},
		{"play.min.io:9000", "play.min.io:9000", true, false},
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
