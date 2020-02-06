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

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
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

func waitForLowHTTPReq(tolerance int32) {
	if httpServer := newHTTPServerFn(); httpServer != nil {
		// Wait at max 10 minute for an inprogress request before proceeding to heal
		waitCount := 600
		// Any requests in progress, delay the heal.
		for (httpServer.GetRequestCount() >= tolerance) &&
			waitCount > 0 {
			waitCount--
			time.Sleep(1 * time.Second)
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
			waitForLowHTTPReq(int32(globalEndpoints.NEndpoints()))

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

func initHealRoutine() *healRoutine {
	return &healRoutine{
		tasks:  make(chan healTask),
		doneCh: make(chan struct{}),
	}

}

func startBackgroundHealing(ctx context.Context, objAPI ObjectLayer) {
	// Run the background healer
	globalBackgroundHealRoutine = initHealRoutine()
	go globalBackgroundHealRoutine.run(ctx, objAPI)

	// Launch the background healer sequence to track
	// background healing operations, ignore errors
	// errors are handled into offline disks already.
	info, _ := objAPI.StorageInfo(ctx, false)
	numDisks := info.Backend.OnlineDisks.Sum() + info.Backend.OfflineDisks.Sum()
	nh := newBgHealSequence(numDisks)
	globalBackgroundHealState.LaunchNewHealSequence(nh)
}

func initBackgroundHealing(ctx context.Context, objAPI ObjectLayer) {
	go startBackgroundHealing(ctx, objAPI)
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

	// Healing succeeded notify the peers to reload format and re-initialize disks.
	// We will not notify peers if healing is not required.
	if err == nil {
		for _, nerr := range globalNotificationSys.ReloadFormat(opts.DryRun) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
			}
		}
	}

	return res, nil
}
