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
	"fmt"
	"net"
	"runtime"
	"testing"
)

// Tests for port availability logic written for server startup sequence.
func TestCheckPortAvailability(t *testing.T) {
	tests := []struct {
		port int
	}{
		{getFreePort()},
		{getFreePort()},
	}
	for _, test := range tests {
		// This test should pass if the ports are available
		err := checkPortAvailability(test.port)
		if err != nil {
			t.Fatalf("checkPortAvailability test failed for port: %d. Error: %v", test.port, err)
		}

		// Now use the ports and check again
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", test.port))
		if err != nil {
			t.Fail()
		}
		defer ln.Close()

		err = checkPortAvailability(test.port)

		// Skip if the os is windows due to https://github.com/golang/go/issues/7598
		if err == nil && runtime.GOOS != "windows" {
			t.Fatalf("checkPortAvailability should fail for port: %d. Error: %v", test.port, err)
		}
	}
}
