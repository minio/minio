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

	"github.com/minio/minio/internal/bucket/replication"
)

func (b *BucketReplicationStats) hasReplicationUsage() bool {
	return b.FailedSize > 0 ||
		b.ReplicatedSize > 0 ||
		b.ReplicaSize > 0 ||
		b.FailedCount > 0
}

// ReplicationStats holds the global in-memory replication stats
type ReplicationStats struct {
	Cache      map[string]*BucketReplicationStats
	UsageCache map[string]*BucketReplicationStats
	sync.RWMutex
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

// Update updates in-memory replication statistics with new values.
func (r *ReplicationStats) Update(bucket string, n int64, status, prevStatus replication.StatusType, opType replication.Type) {
	if r == nil {
		return
	}

	r.RLock()
	b, ok := r.Cache[bucket]
	if !ok {
		b = &BucketReplicationStats{}
	}
	r.RUnlock()
	switch status {
	case replication.Completed:
		switch prevStatus { // adjust counters based on previous state
		case replication.Failed:
			atomic.AddUint64(&b.FailedCount, ^uint64(0))
		}
		if opType == replication.ObjectReplicationType {
			atomic.AddUint64(&b.ReplicatedSize, uint64(n))
			switch prevStatus {
			case replication.Failed:
				atomic.AddUint64(&b.FailedSize, ^uint64(n-1))
			}
		}
	case replication.Failed:
		if opType == replication.ObjectReplicationType {
			if prevStatus == replication.Pending {
				atomic.AddUint64(&b.FailedSize, uint64(n))
				atomic.AddUint64(&b.FailedCount, 1)
			}
		}
	case replication.Replica:
		if opType == replication.ObjectReplicationType {
			atomic.AddUint64(&b.ReplicaSize, uint64(n))
		}
	}
	r.Lock()
	r.Cache[bucket] = b
	r.Unlock()
}

// GetInitialUsage get replication metrics available at the time of cluster initialization
func (r *ReplicationStats) GetInitialUsage(bucket string) BucketReplicationStats {
	if r == nil {
		return BucketReplicationStats{}
	}

	r.RLock()
	defer r.RUnlock()

	st, ok := r.UsageCache[bucket]
	if !ok {
		return BucketReplicationStats{}
	}
	return BucketReplicationStats{
		FailedSize:     atomic.LoadUint64(&st.FailedSize),
		ReplicatedSize: atomic.LoadUint64(&st.ReplicatedSize),
		ReplicaSize:    atomic.LoadUint64(&st.ReplicaSize),
		FailedCount:    atomic.LoadUint64(&st.FailedCount),
	}
}

// Get replication metrics for a bucket from this node since this node came up.
func (r *ReplicationStats) Get(bucket string) BucketReplicationStats {
	if r == nil {
		return BucketReplicationStats{}
	}

	r.RLock()
	defer r.RUnlock()

	st, ok := r.Cache[bucket]
	if !ok {
		return BucketReplicationStats{}
	}

	return BucketReplicationStats{
		FailedSize:     atomic.LoadUint64(&st.FailedSize),
		ReplicatedSize: atomic.LoadUint64(&st.ReplicatedSize),
		ReplicaSize:    atomic.LoadUint64(&st.ReplicaSize),
		FailedCount:    atomic.LoadUint64(&st.FailedCount),
	}
}

// NewReplicationStats initialize in-memory replication statistics
func NewReplicationStats(ctx context.Context, objectAPI ObjectLayer) *ReplicationStats {
	st := &ReplicationStats{
		Cache:      make(map[string]*BucketReplicationStats),
		UsageCache: make(map[string]*BucketReplicationStats),
	}

	dataUsageInfo, err := loadDataUsageFromBackend(ctx, objectAPI)
	if err != nil {
		return st
	}

	// data usage has not captured any data yet.
	if dataUsageInfo.LastUpdate.IsZero() {
		return st
	}

	for bucket, usage := range dataUsageInfo.BucketsUsage {
		b := &BucketReplicationStats{
			FailedSize:     usage.ReplicationFailedSize,
			ReplicatedSize: usage.ReplicatedSize,
			ReplicaSize:    usage.ReplicaSize,
			FailedCount:    usage.ReplicationFailedCount,
		}
		if b.hasReplicationUsage() {
			st.UsageCache[bucket] = b
		}
	}

	return st
}
