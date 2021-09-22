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
	"sync/atomic"
)

//go:generate msgp -file $GOFILE

// BucketStats bucket statistics
type BucketStats struct {
	ReplicationStats BucketReplicationStats
}

// BucketReplicationStats represents inline replication statistics
// such as pending, failed and completed bytes in total for a bucket
type BucketReplicationStats struct {
	Stats map[string]*BucketReplicationStat
	// Pending size in bytes
	PendingSize int64 `json:"pendingReplicationSize"`
	// Completed size in bytes
	ReplicatedSize int64 `json:"completedReplicationSize"`
	// Total Replica size in bytes
	ReplicaSize int64 `json:"replicaSize"`
	// Failed size in bytes
	FailedSize int64 `json:"failedReplicationSize"`
	// Total number of pending operations including metadata updates
	PendingCount int64 `json:"pendingReplicationCount"`
	// Total number of failed operations including metadata updates
	FailedCount int64 `json:"failedReplicationCount"`
}

// Empty returns true if there are no target stats
func (brs *BucketReplicationStats) Empty() bool {
	return len(brs.Stats) == 0 && brs.ReplicaSize == 0
}

// Clone creates a new BucketReplicationStats copy
func (brs BucketReplicationStats) Clone() BucketReplicationStats {
	c := BucketReplicationStats{
		Stats: make(map[string]*BucketReplicationStat, len(brs.Stats)),
	}
	//this is called only by replicationStats cache and already holds a read lock before calling Clone()
	for arn, st := range brs.Stats {
		c.Stats[arn] = &BucketReplicationStat{
			FailedSize:     atomic.LoadInt64(&st.FailedSize),
			ReplicatedSize: atomic.LoadInt64(&st.ReplicatedSize),
			ReplicaSize:    atomic.LoadInt64(&st.ReplicaSize),
			FailedCount:    atomic.LoadInt64(&st.FailedCount),
			PendingSize:    atomic.LoadInt64(&st.PendingSize),
			PendingCount:   atomic.LoadInt64(&st.PendingCount),
		}
	}
	// update total counts across targets
	c.FailedSize = atomic.LoadInt64(&brs.FailedSize)
	c.FailedCount = atomic.LoadInt64(&brs.FailedCount)
	c.PendingCount = atomic.LoadInt64(&brs.PendingCount)
	c.PendingSize = atomic.LoadInt64(&brs.PendingSize)
	c.ReplicaSize = atomic.LoadInt64(&brs.ReplicaSize)
	c.ReplicatedSize = atomic.LoadInt64(&brs.ReplicatedSize)
	return c
}

// BucketReplicationStat represents inline replication statistics
// such as pending, failed and completed bytes in total for a bucket
// remote target
type BucketReplicationStat struct {
	// Pending size in bytes
	PendingSize int64 `json:"pendingReplicationSize"`
	// Completed size in bytes
	ReplicatedSize int64 `json:"completedReplicationSize"`
	// Total Replica size in bytes
	ReplicaSize int64 `json:"replicaSize"`
	// Failed size in bytes
	FailedSize int64 `json:"failedReplicationSize"`
	// Total number of pending operations including metadata updates
	PendingCount int64 `json:"pendingReplicationCount"`
	// Total number of failed operations including metadata updates
	FailedCount int64 `json:"failedReplicationCount"`
}

func (bs *BucketReplicationStat) hasReplicationUsage() bool {
	return bs.FailedSize > 0 ||
		bs.ReplicatedSize > 0 ||
		bs.ReplicaSize > 0 ||
		bs.FailedCount > 0 ||
		bs.PendingCount > 0 ||
		bs.PendingSize > 0
}
