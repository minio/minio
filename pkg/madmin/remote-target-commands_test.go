/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
 *
 */
package madmin

import (
	"net/url"
	"testing"
)

func isOpsEqual(op1 []TargetUpdateType, op2 []TargetUpdateType) bool {
	if len(op1) != len(op2) {
		return false
	}
	for _, o1 := range op1 {
		found := false
		for _, o2 := range op2 {
			if o2 == o1 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// TestGetTargetUpdateOps tests GetTargetUpdateOps
func TestGetTargetUpdateOps(t *testing.T) {
	testCases := []struct {
		values      url.Values
		expectedOps []TargetUpdateType
	}{
		{values: url.Values{
			"update": []string{"true"}},
			expectedOps: []TargetUpdateType{},
		},
		{values: url.Values{
			"update": []string{"false"},
			"path":   []string{"true"},
		},
			expectedOps: []TargetUpdateType{},
		},
		{values: url.Values{
			"update": []string{"true"},
			"path":   []string{""},
		},
			expectedOps: []TargetUpdateType{},
		},
		{values: url.Values{
			"update": []string{"true"},
			"path":   []string{"true"},
			"bzzzz":  []string{"true"},
		},
			expectedOps: []TargetUpdateType{PathUpdateType},
		},

		{values: url.Values{
			"update":      []string{"true"},
			"path":        []string{"true"},
			"creds":       []string{"true"},
			"sync":        []string{"true"},
			"proxy":       []string{"true"},
			"bandwidth":   []string{"true"},
			"healthcheck": []string{"true"},
		},
			expectedOps: []TargetUpdateType{
				PathUpdateType, CredentialsUpdateType, SyncUpdateType, ProxyUpdateType, BandwidthLimitUpdateType, HealthCheckDurationUpdateType},
		},
	}
	for i, test := range testCases {
		gotOps := GetTargetUpdateOps(test.values)
		if !isOpsEqual(gotOps, test.expectedOps) {
			t.Fatalf("test %d: expected %v got %v", i+1, test.expectedOps, gotOps)
		}
	}
}
