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
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v2/sync/errgroup"
)

func (er erasureObjects) getOnlineDisks() (newDisks []StorageAPI) {
	disks := er.getDisks()
	var wg sync.WaitGroup
	var mu sync.Mutex
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, i := range r.Perm(len(disks)) {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if disks[i] == nil {
				return
			}
			di, err := disks[i].DiskInfo(context.Background(), false)
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
			newDisks = append(newDisks, disks[i])
			mu.Unlock()
		}()
	}
	wg.Wait()
	return newDisks
}

func (er erasureObjects) getLoadBalancedLocalDisks() (newDisks []StorageAPI) {
	disks := er.getDisks()
	// Based on the random shuffling return back randomized disks.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, i := range r.Perm(len(disks)) {
		if disks[i] != nil && disks[i].IsLocal() {
			newDisks = append(newDisks, disks[i])
		}
	}
	return newDisks
}

// getLoadBalancedDisks - fetches load balanced (sufficiently randomized) disk slice.
// ensures to skip disks if they are not healing and online.
func (er erasureObjects) getLoadBalancedDisks(optimized bool) []StorageAPI {
	disks := er.getDisks()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	if !optimized {
		var newDisks []StorageAPI
		for _, i := range r.Perm(len(disks)) {
			newDisks = append(newDisks, disks[i])
		}
		return newDisks
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	newDisks := map[uint64][]StorageAPI{}
	// Based on the random shuffling return back randomized disks.
	for _, i := range r.Perm(len(disks)) {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if disks[i] == nil {
				return
			}
			di, err := disks[i].DiskInfo(context.Background(), false)
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
			newDisks[di.Used/1024/1024] = append(newDisks[di.Used/1024/1024], disks[i])
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

// readMultipleFiles Reads raw data from all specified files from all disks.
func readMultipleFiles(ctx context.Context, disks []StorageAPI, req ReadMultipleReq, readQuorum int) ([]ReadMultipleResp, error) {
	resps := make([]chan ReadMultipleResp, len(disks))
	for i := range resps {
		resps[i] = make(chan ReadMultipleResp, len(req.Files))
	}
	g := errgroup.WithNErrs(len(disks))
	// Read files in parallel across disks.
	for index := range disks {
		index := index
		g.Go(func() (err error) {
			if disks[index] == nil {
				return errDiskNotFound
			}
			return disks[index].ReadMultiple(ctx, req, resps[index])
		}, index)
	}

	dataArray := make([]ReadMultipleResp, 0, len(req.Files))
	// Merge results. They should come in order from each.
	for _, wantFile := range req.Files {
		quorum := 0
		toAdd := ReadMultipleResp{
			Bucket: req.Bucket,
			Prefix: req.Prefix,
			File:   wantFile,
		}
		for i := range resps {
			if disks[i] == nil {
				continue
			}
			select {
			case <-ctx.Done():
			case gotFile, ok := <-resps[i]:
				if !ok {
					continue
				}
				if gotFile.Error != "" || !gotFile.Exists {
					continue
				}
				if gotFile.File != wantFile || gotFile.Bucket != req.Bucket || gotFile.Prefix != req.Prefix {
					continue
				}
				quorum++
				if toAdd.Modtime.After(gotFile.Modtime) || len(gotFile.Data) < len(toAdd.Data) {
					// Pick latest, or largest to avoid possible truncated entries.
					continue
				}
				toAdd = gotFile
			}
		}
		if quorum < readQuorum {
			toAdd.Exists = false
			toAdd.Error = errErasureReadQuorum.Error()
			toAdd.Data = nil
		}
		dataArray = append(dataArray, toAdd)
	}

	ignoredErrs := []error{
		errFileNotFound,
		errVolumeNotFound,
		errFileVersionNotFound,
		io.ErrUnexpectedEOF, // some times we would read without locks, ignore these errors
		io.EOF,              // some times we would read without locks, ignore these errors
	}
	ignoredErrs = append(ignoredErrs, objectOpIgnoredErrs...)

	errs := g.Wait()
	for index, err := range errs {
		if err == nil {
			continue
		}
		if !IsErr(err, ignoredErrs...) {
			logger.LogOnceIf(ctx, fmt.Errorf("Drive %s, path (%s/%s) returned an error (%w)",
				disks[index], req.Bucket, req.Prefix, err),
				disks[index].String())
		}
	}

	// Return all the metadata.
	return dataArray, nil
}
