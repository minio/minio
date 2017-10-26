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
	"testing"

	"github.com/minio/cli"
	xnet "github.com/minio/minio/pkg/net"
)

// Test RegisterGatewayCommand
func TestRegisterGatewayCommand(t *testing.T) {
	var err error

	cmd := cli.Command{Name: "test"}
	err = RegisterGatewayCommand(cmd)
	if err != nil {
		t.Errorf("RegisterGatewayCommand got unexpected error: %s", err)
	}

	// Should returns 'duplicated' error
	err = RegisterGatewayCommand(cmd)
	if err == nil {
		t.Errorf("RegisterGatewayCommand twice with same name should return error")
	} else {
		if err.Error() != "duplicate gateway: test" {
			t.Errorf("RegisterGatewayCommand got unexpected error: %s", err)
		}
	}
}

// Test parseGatewayEndpoint
func TestParseGatewayEndpoint(t *testing.T) {
	tmpGlobalServerHost := globalServerHost
	defer func() {
		globalServerHost = tmpGlobalServerHost
	}()
	globalServerHost = xnet.MustParseHost("0.0.0.0:10000")

	testCases := []struct {
		arg       string
		endpoint  string
		secure    bool
		expectErr bool
	}{
		{"s3.amazonaws.com", "s3.amazonaws.com", true, false},
		{"play.minio.io:9000", "play.minio.io:9000", true, false},
		{"http://127.0.0.1:9000", "127.0.0.1:9000", false, false},
		{"https://127.0.0.1:9000", "127.0.0.1:9000", true, false},
		{"http://play.minio.io:9000", "play.minio.io:9000", false, false},
		{"https://play.minio.io:9000", "play.minio.io:9000", true, false},
		{"http://127.0.0.1:10000", "", false, true},
		{"ftp://127.0.0.1:9000", "", false, true},
		{"ftp://play.minio.io:9000", "", false, true},
	}

	for i, testCase := range testCases {
		endpoint, secure, err := parseGatewayEndpoint(testCase.arg)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if endpoint != testCase.endpoint {
				t.Fatalf("test %v: endpoint: expected: %v, got: %v", i+1, testCase.endpoint, endpoint)
			}
			if secure != testCase.secure {
				t.Fatalf("test %v: secure: expected: %v, got: %v", i+1, testCase.secure, secure)
			}
		}
	}
}
