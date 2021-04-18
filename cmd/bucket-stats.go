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

//go:generate msgp -file $GOFILE

// BucketStats bucket statistics
type BucketStats struct {
	ReplicationStats BucketReplicationStats
}

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
