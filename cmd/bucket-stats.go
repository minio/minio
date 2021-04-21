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
