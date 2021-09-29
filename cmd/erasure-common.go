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
	"context"
	"sync"
)

func (er erasureObjects) getLoadBalancedLocalDisks() (newDisks []StorageAPI) {
	disks := er.getDisks()
	// Based on the random shuffling return back randomized disks.
	for _, i := range hashOrder(UTCNow().String(), len(disks)) {
		if disks[i-1] != nil && disks[i-1].IsLocal() {
			if disks[i-1].Healing() == nil && disks[i-1].IsOnline() {
				newDisks = append(newDisks, disks[i-1])
			}
		}
	}
	return newDisks
}

// getLoadBalancedDisks - fetches load balanced (sufficiently randomized) disk slice.
// ensures to skip disks if they are not healing and online.
func (er erasureObjects) getLoadBalancedDisks(optimized bool) []StorageAPI {
	disks := er.getDisks()

	if !optimized {
		var newDisks []StorageAPI
		for _, i := range hashOrder(UTCNow().String(), len(disks)) {
			newDisks = append(newDisks, disks[i-1])
		}
		return newDisks
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var newDisks = map[uint64][]StorageAPI{}
	// Based on the random shuffling return back randomized disks.
	for _, i := range hashOrder(UTCNow().String(), len(disks)) {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if disks[i-1] == nil {
				return
			}
			di, err := disks[i-1].DiskInfo(context.Background())
			if err != nil || di.Healing {
				// - Do not consume disks which are not reachable
				//   unformatted or simply not accessible for some reason.
				//
				// - Do not consume disks which are being healed
				//
				// - Future: skip busy disks
				return
			}

			mu.Lock()
			// Capture disks usage wise upto resolution of MiB
			newDisks[di.Used/1024/1024] = append(newDisks[di.Used/1024/1024], disks[i-1])
			mu.Unlock()
		}()
	}
	wg.Wait()

	var max uint64
	for k := range newDisks {
		if k > max {
			max = k
		}
	}

	// Return disks which have maximum disk usage common.
	return newDisks[max]
}
