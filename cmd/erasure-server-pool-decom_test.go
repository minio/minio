// Copyright (c) 2015-2022 MinIO, Inc.
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
	"context"
	"testing"
)

func prepareErasurePools() (ObjectLayer, []string, error) {
	nDisks := 32
	fsDirs, err := getRandomDisks(nDisks)
	if err != nil {
		return nil, nil, err
	}

	pools := mustGetPoolEndpoints(0, fsDirs[:16]...)
	pools = append(pools, mustGetPoolEndpoints(1, fsDirs[16:]...)...)

	objLayer, _, err := initObjectLayer(context.Background(), pools)
	if err != nil {
		removeRoots(fsDirs)
		return nil, nil, err
	}
	return objLayer, fsDirs, nil
}

func TestPoolMetaValidate(t *testing.T) {
	objLayer1, fsDirs, err := prepareErasurePools()
	if err != nil {
		t.Fatal(err)
	}
	defer removeRoots(fsDirs)

	meta := objLayer1.(*erasureServerPools).poolMeta
	pools := objLayer1.(*erasureServerPools).serverPools

	objLayer2, fsDirs, err := prepareErasurePools()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for Erasure setup: %s", err)
	}
	defer removeRoots(fsDirs)

	newPools := objLayer2.(*erasureServerPools).serverPools
	reducedPools := pools[1:]
	orderChangePools := []*erasureSets{
		pools[1],
		pools[0],
	}

	var nmeta1 poolMeta
	nmeta1.Version = poolMetaVersion
	nmeta1.Pools = append(nmeta1.Pools, meta.Pools...)
	for i, pool := range nmeta1.Pools {
		if i == 0 {
			nmeta1.Pools[i] = PoolStatus{
				CmdLine:    pool.CmdLine,
				ID:         i,
				LastUpdate: UTCNow(),
				Decommission: &PoolDecommissionInfo{
					Complete: true,
				},
			}
		}
	}

	var nmeta2 poolMeta
	nmeta2.Version = poolMetaVersion
	nmeta2.Pools = append(nmeta2.Pools, meta.Pools...)
	for i, pool := range nmeta2.Pools {
		if i == 0 {
			nmeta2.Pools[i] = PoolStatus{
				CmdLine:    pool.CmdLine,
				ID:         i,
				LastUpdate: UTCNow(),
				Decommission: &PoolDecommissionInfo{
					Complete: false,
				},
			}
		}
	}

	testCases := []struct {
		meta           poolMeta
		pools          []*erasureSets
		expectedUpdate bool
		expectedErr    bool
		name           string
	}{
		{
			meta:           meta,
			pools:          pools,
			name:           "Correct",
			expectedErr:    false,
			expectedUpdate: false,
		},
		{
			meta:           meta,
			pools:          newPools,
			name:           "Correct-Update",
			expectedErr:    false,
			expectedUpdate: true,
		},
		{
			meta:           meta,
			pools:          reducedPools,
			name:           "Correct-Update",
			expectedErr:    false,
			expectedUpdate: true,
		},
		{
			meta:           meta,
			pools:          orderChangePools,
			name:           "Invalid-Orderchange",
			expectedErr:    false,
			expectedUpdate: true,
		},
		{
			meta:           nmeta1,
			pools:          pools,
			name:           "Invalid-Completed-Pool-Not-Removed",
			expectedErr:    false,
			expectedUpdate: false,
		},
		{
			meta:           nmeta2,
			pools:          pools,
			name:           "Correct-Decom-Pending",
			expectedErr:    false,
			expectedUpdate: false,
		},
		{
			meta:           nmeta2,
			pools:          reducedPools,
			name:           "Invalid-Decom-Pending-Pool-Removal",
			expectedErr:    false,
			expectedUpdate: true,
		},
		{
			meta:           nmeta1,
			pools:          reducedPools,
			name:           "Correct-Decom-Pool-Removed",
			expectedErr:    false,
			expectedUpdate: true,
		},
		{
			meta:           poolMeta{}, // no-pool info available fresh setup.
			pools:          pools,
			name:           "Correct-Fresh-Setup",
			expectedErr:    false,
			expectedUpdate: true,
		},
		{
			meta:           nmeta2,
			pools:          orderChangePools,
			name:           "Invalid-Orderchange-Decom",
			expectedErr:    false,
			expectedUpdate: true,
		},
	}

	t.Parallel()
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			update, err := testCase.meta.validate(testCase.pools)
			if testCase.expectedErr {
				t.Log(err)
			}
			if err != nil && !testCase.expectedErr {
				t.Errorf("Expected success, but found %s", err)
			}
			if err == nil && testCase.expectedErr {
				t.Error("Expected error, but got `nil`")
			}
			if update != testCase.expectedUpdate {
				t.Errorf("Expected %t, got %t", testCase.expectedUpdate, update)
			}
		})
	}
}
