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
	"runtime"
	"testing"
)

// Tests fetch local address.
func TestLocalAddress(t *testing.T) {
	if runtime.GOOS == "windows" {
		return
	}
	testCases := []struct {
		srvCmdConfig serverCmdConfig
		localAddr    string
	}{
		// Test 1 - local address is found.
		{
			srvCmdConfig: serverCmdConfig{
				isDistXL: true,
				disks: []string{
					"localhost:/mnt/disk1",
					"1.1.1.2:/mnt/disk2",
					"1.1.2.1:/mnt/disk3",
					"1.1.2.2:/mnt/disk4",
				},
			},
			localAddr: "localhost:9000",
		},
		// Test 2 - local address is everything.
		{
			srvCmdConfig: serverCmdConfig{
				isDistXL: false,
				disks: []string{
					"/mnt/disk1",
					"/mnt/disk2",
					"/mnt/disk3",
					"/mnt/disk4",
				},
			},
			localAddr: ":9000",
		},
		// Test 3 - local address is not found.
		{
			srvCmdConfig: serverCmdConfig{
				isDistXL: true,
				disks: []string{
					"1.1.1.1:/mnt/disk1",
					"1.1.1.2:/mnt/disk2",
					"1.1.2.1:/mnt/disk3",
					"1.1.2.2:/mnt/disk4",
				},
			},
			localAddr: "",
		},
	}

	// Validates fetching local address.
	for i, testCase := range testCases {
		localAddr := getLocalAddress(testCase.srvCmdConfig)
		if localAddr != testCase.localAddr {
			t.Fatalf("Test %d: Expected %s, got %s", i+1, testCase.localAddr, localAddr)
		}
	}

}

// Tests initialization of remote controller clients.
func TestInitRemoteControlClients(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal("Unable to initialize config", err)
	}
	defer removeAll(rootPath)

	testCases := []struct {
		srvCmdConfig serverCmdConfig
		totalClients int
	}{
		// Test - 1 no allocation if server config is not distributed XL.
		{
			srvCmdConfig: serverCmdConfig{
				isDistXL: false,
			},
			totalClients: 0,
		},
		// Test - 2 two clients allocated with 4 disks with 2 disks on same node each.
		{
			srvCmdConfig: serverCmdConfig{
				isDistXL: true,
				disks: []string{
					"10.1.10.1:/mnt/disk1",
					"10.1.10.1:/mnt/disk2",
					"10.1.10.2:/mnt/disk3",
					"10.1.10.2:/mnt/disk4",
				},
			},
			totalClients: 2,
		},
		// Test - 3 4 clients allocated with 4 disks with 1 disk on each node.
		{
			srvCmdConfig: serverCmdConfig{
				isDistXL: true,
				disks: []string{
					"10.1.10.1:/mnt/disk1",
					"10.1.10.2:/mnt/disk2",
					"10.1.10.3:/mnt/disk3",
					"10.1.10.4:/mnt/disk4",
				},
			},
			totalClients: 4,
		},
	}

	// Evaluate and validate all test cases.
	for i, testCase := range testCases {
		rclients := initRemoteControlClients(testCase.srvCmdConfig)
		if len(rclients) != testCase.totalClients {
			t.Errorf("Test %d, Expected %d, got %d RPC clients.", i+1, testCase.totalClients, len(rclients))
		}
	}
}
