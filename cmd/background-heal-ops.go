/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"path"
	"time"

	"github.com/minio/minio/pkg/madmin"
)

// healTask represents what to heal along with options
//   path: '/' =>  Heal disk formats along with metadata
//   path: 'bucket/' or '/bucket/' => Heal bucket
//   path: 'bucket/object' => Heal object
type healTask struct {
	bucket        string
	object        string
	versionID     string
	sleepDuration time.Duration
	sleepForIO    int
	opts          madmin.HealOpts
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

	waitTick := 100 * time.Millisecond

	// Bucket notification and http trace are not costly, it is okay to ignore them
	// while counting the number of concurrent connections
	maxIOFn := func() int {
		return maxIO + int(globalHTTPListen.NumSubscribers()) + int(globalHTTPTrace.NumSubscribers())
	}

	if httpServer := newHTTPServerFn(); httpServer != nil {
		// Any requests in progress, delay the heal.
		for httpServer.GetRequestCount() >= int32(maxIOFn()) {
			if maxWait < waitTick {
				time.Sleep(maxWait)
			} else {
				time.Sleep(waitTick)
			}
			maxWait = maxWait - waitTick
			if maxWait <= 0 {
				return
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
				break
			}

			// Wait and proceed if there are active requests
			waitForLowHTTPReq(task.sleepForIO, task.sleepDuration)

			var res madmin.HealResultItem
			var err error
			switch {
			case task.bucket == nopHeal:
				continue
			case task.bucket == SlashSeparator:
				res, err = healDiskFormat(ctx, objAPI, task.opts)
			case task.bucket != "" && task.object == "":
				res, err = objAPI.HealBucket(ctx, task.bucket, task.opts.DryRun, task.opts.Remove)
			case task.bucket != "" && task.object != "":
				res, err = objAPI.HealObject(ctx, task.bucket, task.object, task.versionID, task.opts)
			}
			if task.bucket != "" && task.object != "" {
				ObjectPathUpdated(path.Join(task.bucket, task.object))
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
