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
	"runtime"

	"github.com/minio/madmin-go"
)

// healTask represents what to heal along with options
//   path: '/' =>  Heal disk formats along with metadata
//   path: 'bucket/' or '/bucket/' => Heal bucket
//   path: 'bucket/object' => Heal object
type healTask struct {
	bucket    string
	object    string
	versionID string
	opts      madmin.HealOpts
	// Healing response will be sent here
	respCh chan healResult
}

// healResult represents a healing result with a possible error
type healResult struct {
	result madmin.HealResultItem
	err    error
}

// healRoutine receives heal tasks, to heal buckets, objects and format.json
type healRoutine struct {
	tasks   chan healTask
	workers int
}

func systemIO() int {
	// Bucket notification and http trace are not costly, it is okay to ignore them
	// while counting the number of concurrent connections
	return int(globalHTTPListen.NumSubscribers()) + int(globalTrace.NumSubscribers())
}

func waitForLowHTTPReq() {
	var currentIO func() int
	if httpServer := newHTTPServerFn(); httpServer != nil {
		currentIO = httpServer.GetRequestCount
	}

	globalHealConfig.Wait(currentIO, systemIO)
}

func initBackgroundHealing(ctx context.Context, objAPI ObjectLayer) {
	// Run the background healer
	globalBackgroundHealRoutine = newHealRoutine()
	for i := 0; i < globalBackgroundHealRoutine.workers; i++ {
		go globalBackgroundHealRoutine.AddWorker(ctx, objAPI)
	}

	globalBackgroundHealState.LaunchNewHealSequence(newBgHealSequence(), objAPI)
}

// Wait for heal requests and process them
func (h *healRoutine) AddWorker(ctx context.Context, objAPI ObjectLayer) {
	for {
		select {
		case task, ok := <-h.tasks:
			if !ok {
				return
			}

			var res madmin.HealResultItem
			var err error
			switch task.bucket {
			case nopHeal:
				task.respCh <- healResult{err: errSkipFile}
				continue
			case SlashSeparator:
				res, err = healDiskFormat(ctx, objAPI, task.opts)
			default:
				if task.object == "" {
					res, err = objAPI.HealBucket(ctx, task.bucket, task.opts)
				} else {
					res, err = objAPI.HealObject(ctx, task.bucket, task.object, task.versionID, task.opts)
				}
			}

			task.respCh <- healResult{result: res, err: err}
		case <-ctx.Done():
			return
		}
	}
}

func newHealRoutine() *healRoutine {
	workers := runtime.GOMAXPROCS(0) / 2
	if workers == 0 {
		workers = 4
	}
	return &healRoutine{
		tasks:   make(chan healTask),
		workers: workers,
	}

}

// healDiskFormat - heals format.json, return value indicates if a
// failure error occurred.
func healDiskFormat(ctx context.Context, objAPI ObjectLayer, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	res, err := objAPI.HealFormat(ctx, opts.DryRun)

	// return any error, ignore error returned when disks have
	// already healed.
	if err != nil && err != errNoHealRequired {
		return madmin.HealResultItem{}, err
	}

	return res, nil
}
