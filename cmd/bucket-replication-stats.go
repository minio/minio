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
	"time"

	"github.com/minio/minio/internal/bucket/replication"
)

func (b *BucketReplicationStats) hasReplicationUsage() bool {
	for _, s := range b.Stats {
		if s.hasReplicationUsage() {
			return true
		}
	}
	return false
}

// ReplicationStats holds the global in-memory replication stats
type ReplicationStats struct {
	Cache      map[string]*BucketReplicationStats
	UsageCache map[string]*BucketReplicationStats
	sync.RWMutex
	ulock sync.RWMutex
}

// Delete deletes in-memory replication statistics for a bucket.
func (r *ReplicationStats) Delete(bucket string) {
	if r == nil {
		return
	}

	r.Lock()
	defer r.Unlock()
	delete(r.Cache, bucket)

	r.ulock.Lock()
	defer r.ulock.Unlock()
	delete(r.UsageCache, bucket)
}

// UpdateReplicaStat updates in-memory replica statistics with new values.
func (r *ReplicationStats) UpdateReplicaStat(bucket string, n int64) {
	if r == nil {
		return
	}

	r.Lock()
	defer r.Unlock()
	bs, ok := r.Cache[bucket]
	if !ok {
		bs = &BucketReplicationStats{Stats: make(map[string]*BucketReplicationStat)}
	}
	bs.ReplicaSize += n
	r.Cache[bucket] = bs
}

// Update updates in-memory replication statistics with new values.
func (r *ReplicationStats) Update(bucket string, arn string, n int64, duration time.Duration, status, prevStatus replication.StatusType, opType replication.Type) {
	if r == nil {
		return
	}
	r.Lock()
	defer r.Unlock()

	bs, ok := r.Cache[bucket]
	if !ok {
		bs = &BucketReplicationStats{Stats: make(map[string]*BucketReplicationStat)}
		r.Cache[bucket] = bs
	}
	b, ok := bs.Stats[arn]
	if !ok {
		b = &BucketReplicationStat{}
		bs.Stats[arn] = b
	}
	switch status {
	case replication.Completed:
		switch prevStatus { // adjust counters based on previous state
		case replication.Failed:
			b.FailedCount--
		}
		if opType == replication.ObjectReplicationType {
			b.ReplicatedSize += n
			switch prevStatus {
			case replication.Failed:
				b.FailedSize -= n
			}
			if duration > 0 {
				b.Latency.update(n, duration)
			}
		}
	case replication.Failed:
		if opType == replication.ObjectReplicationType {
			if prevStatus == replication.Pending {
				b.FailedSize += n
				b.FailedCount++
			}
		}
	case replication.Replica:
		if opType == replication.ObjectReplicationType {
			b.ReplicaSize += n
		}
	}
}

// GetInitialUsage get replication metrics available at the time of cluster initialization
func (r *ReplicationStats) GetInitialUsage(bucket string) BucketReplicationStats {
	if r == nil {
		return BucketReplicationStats{}
	}
	r.ulock.RLock()
	defer r.ulock.RUnlock()
	st, ok := r.UsageCache[bucket]
	if !ok {
		return BucketReplicationStats{}
	}
	return st.Clone()
}

// GetAll returns replication metrics for all buckets at once.
func (r *ReplicationStats) GetAll() map[string]BucketReplicationStats {
	if r == nil {
		return map[string]BucketReplicationStats{}
	}

	r.RLock()
	defer r.RUnlock()

	bucketReplicationStats := make(map[string]BucketReplicationStats, len(r.Cache))
	for k, v := range r.Cache {
		bucketReplicationStats[k] = v.Clone()
	}

	return bucketReplicationStats
}

// Get replication metrics for a bucket from this node since this node came up.
func (r *ReplicationStats) Get(bucket string) BucketReplicationStats {
	if r == nil {
		return BucketReplicationStats{Stats: make(map[string]*BucketReplicationStat)}
	}

	r.RLock()
	defer r.RUnlock()

	st, ok := r.Cache[bucket]
	if !ok {
		return BucketReplicationStats{}
	}
	return st.Clone()
}

// NewReplicationStats initialize in-memory replication statistics
func NewReplicationStats(ctx context.Context, objectAPI ObjectLayer) *ReplicationStats {
	return &ReplicationStats{
		Cache:      make(map[string]*BucketReplicationStats),
		UsageCache: make(map[string]*BucketReplicationStats),
	}
}

// load replication metrics at cluster start from initial data usage
func (r *ReplicationStats) loadInitialReplicationMetrics(ctx context.Context) {
	rTimer := time.NewTimer(time.Minute)
	defer rTimer.Stop()
	var (
		dui DataUsageInfo
		err error
	)
outer:
	for {
		select {
		case <-ctx.Done():
			return
		case <-rTimer.C:
			dui, err = loadDataUsageFromBackend(GlobalContext, newObjectLayerFn())
			// If LastUpdate is set, data usage is available.
			if err == nil && !dui.LastUpdate.IsZero() {
				break outer
			}

			rTimer.Reset(time.Minute)
		}
	}

	m := make(map[string]*BucketReplicationStats)
	for bucket, usage := range dui.BucketsUsage {
		b := &BucketReplicationStats{
			Stats: make(map[string]*BucketReplicationStat, len(usage.ReplicationInfo)),
		}
		for arn, uinfo := range usage.ReplicationInfo {
			b.Stats[arn] = &BucketReplicationStat{
				FailedSize:     int64(uinfo.ReplicationFailedSize),
				ReplicatedSize: int64(uinfo.ReplicatedSize),
				ReplicaSize:    int64(uinfo.ReplicaSize),
				FailedCount:    int64(uinfo.ReplicationFailedCount),
			}
		}
		b.ReplicaSize += int64(usage.ReplicaSize)
		if b.hasReplicationUsage() {
			m[bucket] = b
		}
	}
	r.ulock.Lock()
	defer r.ulock.Unlock()
	r.UsageCache = m
}
