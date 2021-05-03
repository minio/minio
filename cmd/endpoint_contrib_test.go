/*
 * MinIO Object Storage (c) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/minio/minio-go/v7/pkg/set"
)

func TestUpdateDomainIPs(t *testing.T) {
	tempGlobalMinioPort := globalMinioPort
	defer func() {
		globalMinioPort = tempGlobalMinioPort
	}()
	globalMinioPort = "9000"

	tempGlobalDomainIPs := globalDomainIPs
	defer func() {
		globalDomainIPs = tempGlobalDomainIPs
	}()

	ipv4TestCases := []struct {
		endPoints      set.StringSet
		expectedResult set.StringSet
	}{
		{set.NewStringSet(), set.NewStringSet()},
		{set.CreateStringSet("localhost"), set.NewStringSet()},
		{set.CreateStringSet("localhost", "10.0.0.1"), set.CreateStringSet("10.0.0.1:9000")},
		{set.CreateStringSet("localhost:9001", "10.0.0.1"), set.CreateStringSet("10.0.0.1:9000")},
		{set.CreateStringSet("localhost", "10.0.0.1:9001"), set.CreateStringSet("10.0.0.1:9001")},
		{set.CreateStringSet("localhost:9000", "10.0.0.1:9001"), set.CreateStringSet("10.0.0.1:9001")},

		{set.CreateStringSet("10.0.0.1", "10.0.0.2"), set.CreateStringSet("10.0.0.1:9000", "10.0.0.2:9000")},
		{set.CreateStringSet("10.0.0.1:9001", "10.0.0.2"), set.CreateStringSet("10.0.0.1:9001", "10.0.0.2:9000")},
		{set.CreateStringSet("10.0.0.1", "10.0.0.2:9002"), set.CreateStringSet("10.0.0.1:9000", "10.0.0.2:9002")},
		{set.CreateStringSet("10.0.0.1:9001", "10.0.0.2:9002"), set.CreateStringSet("10.0.0.1:9001", "10.0.0.2:9002")},
	}

	for _, testCase := range ipv4TestCases {
		globalDomainIPs = nil

		updateDomainIPs(testCase.endPoints)

		if !testCase.expectedResult.Equals(globalDomainIPs) {
			t.Fatalf("error: expected = %s, got = %s", testCase.expectedResult, globalDomainIPs)
		}
	}
}
