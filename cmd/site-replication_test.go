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
	"testing"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
)

// TestGetMissingSiteNames
func TestGetMissingSiteNames(t *testing.T) {
	testCases := []struct {
		currSites []madmin.PeerInfo
		oldDepIDs set.StringSet
		newDepIDs set.StringSet
		expNames  []string
	}{
		// Test1: missing some sites in replicated setup
		{
			[]madmin.PeerInfo{
				{Endpoint: "minio1:9000", Name: "minio1", DeploymentID: "dep1"},
				{Endpoint: "minio2:9000", Name: "minio2", DeploymentID: "dep2"},
				{Endpoint: "minio3:9000", Name: "minio3", DeploymentID: "dep3"},
			},
			set.CreateStringSet("dep1", "dep2", "dep3"),
			set.CreateStringSet("dep1"),
			[]string{"minio2", "minio3"},
		},
		// Test2: new site added that is not in replicated setup
		{
			[]madmin.PeerInfo{{Endpoint: "minio1:9000", Name: "minio1", DeploymentID: "dep1"}, {Endpoint: "minio2:9000", Name: "minio2", DeploymentID: "dep2"}, {Endpoint: "minio3:9000", Name: "minio3", DeploymentID: "dep3"}},
			set.CreateStringSet("dep1", "dep2", "dep3"),
			set.CreateStringSet("dep1", "dep2", "dep3", "dep4"),
			[]string{},
		},
		// Test3: not currently under site replication.
		{
			[]madmin.PeerInfo{},
			set.CreateStringSet(),
			set.CreateStringSet("dep1", "dep2", "dep3", "dep4"),
			[]string{},
		},
	}

	for i, tc := range testCases {
		names := getMissingSiteNames(tc.oldDepIDs, tc.newDepIDs, tc.currSites)
		if len(names) != len(tc.expNames) {
			t.Errorf("Test %d: Expected `%v`, got `%v`", i+1, tc.expNames, names)
		}
	}
}
