// Copyright (c) 2015-2024 MinIO, Inc.
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
)

// PurgeTask - cleanup task
type PurgeTask struct {
	Bucket       string
	Object       string
	VersionID    string
	DeleteMarker bool
	Size         int64
}

// cleanupMetrics - collect metrics on entries marked for purge that were removed from namespace
type cleanupMetrics struct {
	TotPurgeTasks      int64
	TotVersionsCount   int64
	TotDMVersionsCount int64
	TotSizeCleared     int64
	TotalErrs          int64 // total errors encountered - not definitive
	TotalBacklogSz     int64 // total backlog size - not definitive, could have duplicates across nodes
}

func (m *cleanupMetrics) inc(t PurgeTask, err error) {
	if err != nil {
		m.TotalErrs++
		m.TotalBacklogSz += t.Size
		return
	}
	m.TotPurgeTasks++
	if t.DeleteMarker {
		m.TotDMVersionsCount++
	} else {
		m.TotVersionsCount++
	}
	m.TotSizeCleared += t.Size
}

// backgroundCleanupSys - manages cleanup of deleted objects that are marked for purge
type backgroundCleanupSys struct {
	ctx     context.Context
	objAPI  ObjectLayer
	metrics cleanupMetrics
	purgeCh chan PurgeTask
}

func initBackgroundCleanup(ctx context.Context, objAPI ObjectLayer) {
	globalBackgroundCleanup.init(ctx, objAPI)
}

func (c *backgroundCleanupSys) init(ctx context.Context, objAPI ObjectLayer) {
	c.ctx = ctx
	c.objAPI = objAPI
	c.purgeCh = make(chan PurgeTask, 1000000)
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case task := <-c.purgeCh:
				_, err := c.objAPI.DeleteObject(ctx, task.Bucket, task.Object, ObjectOptions{
					VersionID:        task.VersionID,
					Versioned:        globalBucketVersioningSys.PrefixEnabled(task.Bucket, task.Object),
					VersionSuspended: globalBucketVersioningSys.Suspended(task.Bucket),
					PurgeCleanup:     true,
				})
				c.metrics.inc(task, err)
			}
		}
	}()
}

func (c *backgroundCleanupSys) enqueue(task PurgeTask) {
	select {
	case c.purgeCh <- task:
	case <-c.ctx.Done():
	default:
	}
}
