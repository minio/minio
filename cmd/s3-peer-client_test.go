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
	"net/url"
	"reflect"
	"testing"
)

// Validates makeS3Peers, fetches all peers based on list of storage
// endpoints.
func TestMakeS3Peers(t *testing.T) {
	// Initialize configuration
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer removeAll(root)

	// test cases
	testCases := []struct {
		gMinioAddr string
		eps        []*url.URL
		peers      []string
	}{
		{":9000", []*url.URL{{Path: "/mnt/disk1"}}, []string{":9000"}},
		{":9000", []*url.URL{{Host: "localhost:9001"}}, []string{":9000", "localhost:9001"}},
		{"m1:9000", []*url.URL{{Host: "m1:9000"}, {Host: "m2:9000"}, {Host: "m3:9000"}}, []string{"m1:9000", "m2:9000", "m3:9000"}},
	}

	getPeersHelper := func(s3p s3Peers) []string {
		r := []string{}
		for _, p := range s3p {
			r = append(r, p.addr)
		}
		return r
	}

	// execute tests
	for i, testCase := range testCases {
		globalMinioAddr = testCase.gMinioAddr
		s3peers := makeS3Peers(testCase.eps)
		referencePeers := getPeersHelper(s3peers)
		if !reflect.DeepEqual(testCase.peers, referencePeers) {
			t.Errorf("Test %d: Expected %v, got %v", i+1, testCase.peers, referencePeers)
		}
	}
}
