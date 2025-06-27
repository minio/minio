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
	"runtime"
	"strconv"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/pkg/v3/env"
)

// healTask represents what to heal along with options
//
//	path: '/' =>  Heal disk formats along with metadata
//	path: 'bucket/' or '/bucket/' => Heal bucket
//	path: 'bucket/object' => Heal object
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

func activeListeners() int {
	// Bucket notification and http trace are not costly, it is okay to ignore them
	// while counting the number of concurrent connections
	return int(globalHTTPListen.Subscribers()) + int(globalTrace.Subscribers())
}

func waitForLowIO(maxIO int, maxWait time.Duration, currentIO func() int) {
	// No need to wait run at full speed.
	if maxIO <= 0 {
		return
	}

	const waitTick = 100 * time.Millisecond

	tmpMaxWait := maxWait

	for currentIO() >= maxIO {
		if tmpMaxWait > 0 {
			if tmpMaxWait < waitTick {
				time.Sleep(tmpMaxWait)
				return
			}
			time.Sleep(waitTick)
			tmpMaxWait -= waitTick
		}
		if tmpMaxWait <= 0 {
			return
		}
	}
}

func currentHTTPIO() int {
	httpServer := newHTTPServerFn()
	if httpServer == nil {
		return 0
	}

	return httpServer.GetRequestCount() - activeListeners()
}

func waitForLowHTTPReq() {
	maxIO, maxWait, _ := globalHealConfig.Clone()
	waitForLowIO(maxIO, maxWait, currentHTTPIO)
}

func initBackgroundHealing(ctx context.Context, objAPI ObjectLayer) {
	bgSeq := newBgHealSequence()
	// Run the background healer
	for range globalBackgroundHealRoutine.workers {
		go globalBackgroundHealRoutine.AddWorker(ctx, objAPI, bgSeq)
	}

	globalBackgroundHealState.LaunchNewHealSequence(bgSeq, objAPI)
}

// Wait for heal requests and process them
func (h *healRoutine) AddWorker(ctx context.Context, objAPI ObjectLayer, bgSeq *healSequence) {
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
				err = errSkipFile
			case SlashSeparator:
				res, err = healDiskFormat(ctx, objAPI, task.opts)
			default:
				if task.object == "" {
					res, err = objAPI.HealBucket(ctx, task.bucket, task.opts)
				} else {
					res, err = objAPI.HealObject(ctx, task.bucket, task.object, task.versionID, task.opts)
				}
			}

			if task.respCh != nil {
				task.respCh <- healResult{result: res, err: err}
				continue
			}

			// when respCh is not set caller is not waiting but we
			// update the relevant metrics for them
			if bgSeq != nil {
				if err == nil {
					bgSeq.countHealed(res.Type)
				} else {
					bgSeq.countFailed(res.Type)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func newHealRoutine() *healRoutine {
	workers := runtime.GOMAXPROCS(0) / 2

	if envHealWorkers := env.Get("_MINIO_HEAL_WORKERS", ""); envHealWorkers != "" {
		if numHealers, err := strconv.Atoi(envHealWorkers); err != nil {
			bugLogIf(context.Background(), fmt.Errorf("invalid _MINIO_HEAL_WORKERS value: %w", err))
		} else {
			workers = numHealers
		}
	}

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
