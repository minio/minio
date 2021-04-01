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
	PendingSize uint64
	// Completed size in bytes
	ReplicatedSize uint64
	// Total Replica size in bytes
	ReplicaSize uint64
	// Failed size in bytes
	FailedSize uint64
	// Total number of pending operations including metadata updates
	OperationsPendingCount uint64
}

func (b *BucketReplicationStats) hasReplicationUsage() bool {
	return b.PendingSize > 0 ||
		b.FailedSize > 0 ||
		b.ReplicatedSize > 0 ||
		b.ReplicaSize > 0 ||
		b.OperationsPendingCount > 0
}

type ReplicationStats struct {
	Cache map[string]*BucketReplicationStats
	sync.RWMutex
}

// Update replication stats from incoming queue
func (r *ReplicationStats) Update(bucket string, n int64, status replication.StatusType, opType replication.Type) {
	if r == nil {
		return
	}
	r.Lock()
	defer r.Unlock()
	if _, ok := r.Cache[bucket]; !ok {
		r.Cache[bucket] = &BucketReplicationStats{}
	}
	switch status {
	case replication.Pending:
		if opType == replication.ObjectReplicationType {
			atomic.AddUint64(&r.Cache[bucket].PendingSize, uint64(n))
		}
		atomic.AddUint64(&r.Cache[bucket].OperationsPendingCount, 1)
	case replication.Completed:
		atomic.AddUint64(&r.Cache[bucket].OperationsPendingCount, ^uint64(0))
		if opType == replication.ObjectReplicationType {
			atomic.AddUint64(&r.Cache[bucket].ReplicatedSize, uint64(n))
			atomic.AddUint64(&r.Cache[bucket].PendingSize, ^uint64(n-1))
		}
	case replication.Failed:
		if opType == replication.ObjectReplicationType {
			atomic.AddUint64(&r.Cache[bucket].FailedSize, uint64(n))
		}
	case replication.Replica:
		if opType == replication.ObjectReplicationType {
			atomic.AddUint64(&r.Cache[bucket].ReplicaSize, uint64(n))
		}
	}
}

// Get total bytes pending replication for a bucket
func (r *ReplicationStats) get(bucket string) BucketReplicationStats {
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
		PendingSize:            atomic.LoadUint64(&st.PendingSize),
		FailedSize:             atomic.LoadUint64(&st.FailedSize),
		ReplicatedSize:         atomic.LoadUint64(&st.ReplicatedSize),
		OperationsPendingCount: atomic.LoadUint64(&st.OperationsPendingCount),
	}
}

// Prepare new ReplicationStats structure
func newReplicationStats(ctx context.Context) *ReplicationStats {
	st := &ReplicationStats{
		Cache: make(map[string]*BucketReplicationStats),
	}
	go func(st *ReplicationStats) {
		objLayer := newObjectLayerFn()

		dataUsageInfo, err := loadDataUsageFromBackend(ctx, objLayer)
		if err != nil {
			return
		}

		// data usage has not captured any data yet.
		if dataUsageInfo.LastUpdate.IsZero() {
			return
		}
		st.Lock()
		defer st.Unlock()

		for bucket, usage := range dataUsageInfo.BucketsUsage {
			b := &BucketReplicationStats{
				PendingSize:            usage.ReplicationPendingSize,
				FailedSize:             usage.ReplicationFailedSize,
				ReplicatedSize:         usage.ReplicatedSize,
				ReplicaSize:            usage.ReplicaSize,
				OperationsPendingCount: usage.ReplicationPendingCount,
			}
			if b.hasReplicationUsage() {
				st.Cache[bucket] = b
			}
		}
	}(st)
	return st
}
