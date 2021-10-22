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
	"sync/atomic"
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
	atomic.AddInt64(&bs.ReplicaSize, n)
	r.Cache[bucket] = bs
}

// Update updates in-memory replication statistics with new values.
func (r *ReplicationStats) Update(bucket string, arn string, n int64, status, prevStatus replication.StatusType, opType replication.Type) {
	if r == nil {
		return
	}
	r.RLock()
	bs, ok := r.Cache[bucket]
	if !ok {
		bs = &BucketReplicationStats{Stats: make(map[string]*BucketReplicationStat)}
	}
	b, ok := bs.Stats[arn]
	if !ok {
		b = &BucketReplicationStat{}
	}
	r.RUnlock()
	switch status {
	case replication.Completed:
		switch prevStatus { // adjust counters based on previous state
		case replication.Failed:
			atomic.AddInt64(&b.FailedCount, -1)
		}
		if opType == replication.ObjectReplicationType {
			atomic.AddInt64(&b.ReplicatedSize, n)
			switch prevStatus {
			case replication.Failed:
				atomic.AddInt64(&b.FailedSize, -1*n)
			}
		}
	case replication.Failed:
		if opType == replication.ObjectReplicationType {
			if prevStatus == replication.Pending {
				atomic.AddInt64(&b.FailedSize, n)
				atomic.AddInt64(&b.FailedCount, 1)
			}
		}
	case replication.Replica:
		if opType == replication.ObjectReplicationType {
			atomic.AddInt64(&b.ReplicaSize, n)
		}
	}
	r.Lock()
	bs.Stats[arn] = b
	r.Cache[bucket] = bs
	r.Unlock()
}

// GetInitialUsage get replication metrics available at the time of cluster initialization
func (r *ReplicationStats) GetInitialUsage(bucket string) BucketReplicationStats {
	if r == nil {
		return BucketReplicationStats{}
	}
	r.ulock.RLock()
	defer r.ulock.RUnlock()
	st, ok := r.UsageCache[bucket]
	if ok {
		return st.Clone()
	}
	return BucketReplicationStats{Stats: make(map[string]*BucketReplicationStat)}
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
		return BucketReplicationStats{Stats: make(map[string]*BucketReplicationStat)}
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
	rTimer := time.NewTimer(time.Minute * 1)
	defer rTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-rTimer.C:
			dui, err := loadDataUsageFromBackend(GlobalContext, newObjectLayerFn())
			if err != nil {
				continue
			}
			// data usage has not captured any data yet.
			if dui.LastUpdate.IsZero() {
				continue
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
			return
		}
	}
}
