/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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
	"sync"
	"sync/atomic"

	"github.com/minio/minio/pkg/bucket/replication"
)

// BucketReplicationStats represents inline replication statistics
// such as pending, failed and completed bytes in total for a bucket
type BucketReplicationStats struct {
	// Pending size in bytes
	PendingSize uint64 `json:"pendingReplicationSize"`
	// Completed size in bytes
	ReplicatedSize uint64 `json:"completedReplicationSize"`
	// Total Replica size in bytes
	ReplicaSize uint64 `json:"replicaSize"`
	// Failed size in bytes
	FailedSize uint64 `json:"failedReplicationSize"`
	// Total number of pending operations including metadata updates
	PendingCount uint64 `json:"pendingReplicationCount"`
	// Total number of failed operations including metadata updates
	FailedCount uint64 `json:"failedReplicationCount"`
}

func (b *BucketReplicationStats) hasReplicationUsage() bool {
	return b.PendingSize > 0 ||
		b.FailedSize > 0 ||
		b.ReplicatedSize > 0 ||
		b.ReplicaSize > 0 ||
		b.PendingCount > 0 ||
		b.FailedCount > 0
}

// ReplicationStats holds the global in-memory replication stats
type ReplicationStats struct {
	sync.RWMutex
	Cache map[string]*BucketReplicationStats
}

// Update updates in-memory replication statistics with new values.
func (r *ReplicationStats) Update(ctx context.Context, bucket string, n int64, status, prevStatus replication.StatusType, opType replication.Type) {
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
	case replication.Pending:
		if opType == replication.ObjectReplicationType {
			atomic.AddUint64(&b.PendingSize, uint64(n))
		}
		atomic.AddUint64(&b.PendingCount, 1)
	case replication.Completed:
		switch prevStatus { // adjust counters based on previous state
		case replication.Pending:
			atomic.AddUint64(&b.PendingCount, ^uint64(0))
		case replication.Failed:
			atomic.AddUint64(&b.FailedCount, ^uint64(0))
		}
		if opType == replication.ObjectReplicationType {
			atomic.AddUint64(&b.ReplicatedSize, uint64(n))
			switch prevStatus {
			case replication.Pending:
				atomic.AddUint64(&b.PendingSize, ^uint64(n-1))
			case replication.Failed:
				atomic.AddUint64(&b.FailedSize, ^uint64(n-1))
			}
		}
	case replication.Failed:
		// count failures only once - not on every retry
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

// Get total bytes pending replication for a bucket
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
		PendingSize:    atomic.LoadUint64(&st.PendingSize),
		FailedSize:     atomic.LoadUint64(&st.FailedSize),
		ReplicatedSize: atomic.LoadUint64(&st.ReplicatedSize),
		ReplicaSize:    atomic.LoadUint64(&st.ReplicaSize),
		PendingCount:   atomic.LoadUint64(&st.PendingCount),
		FailedCount:    atomic.LoadUint64(&st.FailedCount),
	}
}

// NewReplicationStats initialize in-memory replication statistics
func NewReplicationStats(ctx context.Context, objectAPI ObjectLayer) *ReplicationStats {
	st := &ReplicationStats{
		Cache: make(map[string]*BucketReplicationStats),
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
			PendingSize:    usage.ReplicationPendingSize,
			FailedSize:     usage.ReplicationFailedSize,
			ReplicatedSize: usage.ReplicatedSize,
			ReplicaSize:    usage.ReplicaSize,
			PendingCount:   usage.ReplicationPendingCount,
			FailedCount:    usage.ReplicationFailedCount,
		}
		if b.hasReplicationUsage() {
			st.Cache[bucket] = b
		}
	}

	return st
}
