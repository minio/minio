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
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/logger"
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
	responseCh chan healResult
}

// healResult represents a healing result with a possible error
type healResult struct {
	result madmin.HealResultItem
	err    error
}

// healRoutine receives heal tasks, to heal buckets, objects and format.json
type healRoutine struct {
	tasks  chan healTask
	doneCh chan struct{}
}

// Add a new task in the tasks queue
func (h *healRoutine) queueHealTask(task healTask) {
	h.tasks <- task
}

func waitForLowHTTPReq(maxIO int, maxWait time.Duration) {
	// No need to wait run at full speed.
	if maxIO <= 0 {
		return
	}

	// At max 10 attempts to wait with 100 millisecond interval before proceeding
	waitTick := 100 * time.Millisecond

	// Bucket notification and http trace are not costly, it is okay to ignore them
	// while counting the number of concurrent connections
	maxIOFn := func() int {
		return maxIO + int(globalHTTPListen.NumSubscribers()) + int(globalTrace.NumSubscribers())
	}

	tmpMaxWait := maxWait
	if httpServer := newHTTPServerFn(); httpServer != nil {
		// Any requests in progress, delay the heal.
		for httpServer.GetRequestCount() >= maxIOFn() {
			if tmpMaxWait > 0 {
				if tmpMaxWait < waitTick {
					time.Sleep(tmpMaxWait)
				} else {
					time.Sleep(waitTick)
				}
				tmpMaxWait = tmpMaxWait - waitTick
			}
			if tmpMaxWait <= 0 {
				if intDataUpdateTracker.debug {
					logger.Info("waitForLowHTTPReq: waited max %s, resuming", maxWait)
				}
				break
			}
		}
	}
}

// Wait for heal requests and process them
func (h *healRoutine) run(ctx context.Context, objAPI ObjectLayer) {
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
			task.responseCh <- healResult{result: res, err: err}

		case <-h.doneCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func newHealRoutine() *healRoutine {
	return &healRoutine{
		tasks:  make(chan healTask),
		doneCh: make(chan struct{}),
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
