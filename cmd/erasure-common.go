/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/minio/minio/pkg/sync/errgroup"
)

func (er erasureObjects) getLoadBalancedLocalDisks() (newDisks []StorageAPI) {
	disks := er.getDisks()
	// Based on the random shuffling return back randomized disks.
	for _, i := range hashOrder(UTCNow().String(), len(disks)) {
		if disks[i-1] != nil && disks[i-1].IsLocal() {
			if !disks[i-1].Healing() && disks[i-1].IsOnline() {
				newDisks = append(newDisks, disks[i-1])
			}
		}
	}
	return newDisks
}

type sortSlices struct {
	disks []StorageAPI
	infos []DiskInfo
}

type sortByOther sortSlices

func (sbo sortByOther) Len() int {
	return len(sbo.disks)
}

func (sbo sortByOther) Swap(i, j int) {
	sbo.disks[i], sbo.disks[j] = sbo.disks[j], sbo.disks[i]
	sbo.infos[i], sbo.infos[j] = sbo.infos[j], sbo.infos[i]
}

func (sbo sortByOther) Less(i, j int) bool {
	return sbo.infos[i].UsedInodes < sbo.infos[j].UsedInodes
}

func (er erasureObjects) getOnlineDisksSortedByUsedInodes() (newDisks []StorageAPI) {
	disks := er.getDisks()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var infos []DiskInfo
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
			newDisks = append(newDisks, disks[i-1])
			infos = append(infos, di)
			mu.Unlock()
		}()
	}
	wg.Wait()

	slices := sortSlices{newDisks, infos}
	sort.Sort(sortByOther(slices))

	return slices.disks
}

func (er erasureObjects) getOnlineDisks() (newDisks []StorageAPI) {
	disks := er.getDisks()
	var wg sync.WaitGroup
	var mu sync.Mutex
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
			newDisks = append(newDisks, disks[i-1])
			mu.Unlock()
		}()
	}
	wg.Wait()
	return newDisks
}

// getLoadBalancedNDisks - fetches load balanced (sufficiently randomized) disk slice
// with N disks online. If ndisks is zero or negative, then it will returns all disks,
// same if ndisks is greater than the number of all disks.
func (er erasureObjects) getLoadBalancedNDisks(ndisks int) (newDisks []StorageAPI) {
	disks := er.getLoadBalancedDisks(ndisks != -1)
	for _, disk := range disks {
		if disk == nil {
			continue
		}
		newDisks = append(newDisks, disk)
		ndisks--
		if ndisks == 0 {
			break
		}
	}
	return
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

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (er erasureObjects) parentDirIsObject(ctx context.Context, bucket, parent string) bool {
	path := ""
	segments := strings.Split(parent, slashSeparator)
	for _, s := range segments {
		if s == "" {
			break
		}
		path += s
		isObject, pathNotExist := er.isObject(ctx, bucket, path)
		if pathNotExist {
			return false
		}
		if isObject {
			// If there is already a file at prefix "p", return true.
			return true
		}
		path += slashSeparator
	}
	return false
}

// isObject - returns `true` if the prefix is an object i.e if
// `xl.meta` exists at the leaf, false otherwise.
func (er erasureObjects) isObject(ctx context.Context, bucket, prefix string) (ok, pathDoesNotExist bool) {
	storageDisks := er.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))

	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] == nil {
				return errDiskNotFound
			}
			// Check if 'prefix' is an object on this 'disk', else continue the check the next disk
			return storageDisks[index].CheckFile(ctx, bucket, prefix)
		}, index)
	}

	// NOTE: Observe we are not trying to read `xl.meta` and figure out the actual
	// quorum intentionally, but rely on the default case scenario. Actual quorum
	// verification will happen by top layer by using getObjectInfo() and will be
	// ignored if necessary.
	readQuorum := getReadQuorum(len(storageDisks))

	err := reduceReadQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, readQuorum)
	return err == nil, err == errPathNotFound
}
