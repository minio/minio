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
	"time"
)

// Tests function returning long lived locks.
func TestGetLongLivedLocks(t *testing.T) {
	ut := time.Now().UTC()
	// Collection of test cases for verifying returning valid long lived locks.
	testCases := []struct {
		lockMap      map[string][]lockRequesterInfo
		lockInterval time.Duration
		expectedNSLR []nameLockRequesterInfoPair
	}{
		// Testcase - 1 validates long lived locks, returns empty list.
		{
			lockMap: map[string][]lockRequesterInfo{
				"test": {{
					writer:        true,
					node:          "10.1.10.21",
					rpcPath:       "/lock/mnt/disk1",
					uid:           "10000112",
					timestamp:     ut,
					timeLastCheck: ut,
				}},
			},
			lockInterval: 1 * time.Minute,
			expectedNSLR: []nameLockRequesterInfoPair{},
		},
		// Testcase - 2 validates long lived locks, returns at least one list.
		{
			lockMap: map[string][]lockRequesterInfo{
				"test": {{
					writer:        true,
					node:          "10.1.10.21",
					rpcPath:       "/lock/mnt/disk1",
					uid:           "10000112",
					timestamp:     ut,
					timeLastCheck: ut.Add(-2 * time.Minute),
				}},
			},
			lockInterval: 1 * time.Minute,
			expectedNSLR: []nameLockRequesterInfoPair{
				{
					name: "test",
					lri: lockRequesterInfo{
						writer:        true,
						node:          "10.1.10.21",
						rpcPath:       "/lock/mnt/disk1",
						uid:           "10000112",
						timestamp:     ut,
						timeLastCheck: ut.Add(-2 * time.Minute),
					},
				},
			},
		},
	}
	// Validates all test cases here.
	for i, testCase := range testCases {
		nsLR := getLongLivedLocks(testCase.lockMap, testCase.lockInterval)
		if !reflect.DeepEqual(testCase.expectedNSLR, nsLR) {
			t.Errorf("Test %d: Expected %#v, got %#v", i+1, testCase.expectedNSLR, nsLR)
		}
	}
}
