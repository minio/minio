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
	"reflect"
	"testing"
)

// Validates getAllPeers, fetches all peers based on list of storage endpoints.
func TestGetAllPeers(t *testing.T) {
	testCases := []struct {
		eps   []storageEndPoint
		peers []string
	}{
		{nil, nil},
		{[]storageEndPoint{{path: "/mnt/disk1"}}, []string{globalMinioAddr}},
		{[]storageEndPoint{{
			host: "localhost",
			port: 9001,
		}}, []string{
			globalMinioAddr, "localhost:9001",
		}},
	}

	for i, testCase := range testCases {
		peers := getAllPeers(testCase.eps)
		if !reflect.DeepEqual(testCase.peers, peers) {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.peers, peers)
		}
	}
}
