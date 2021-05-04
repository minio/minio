// Copyright (c) 2021 MinIO, Inc.
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
