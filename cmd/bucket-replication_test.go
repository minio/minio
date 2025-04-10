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
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/bucket/replication"
	xhttp "github.com/minio/minio/internal/http"
)

var configs = []replication.Config{
	{ // Config0 - Replication config has no filters, existing object replication enabled
		Rules: []replication.Rule{
			{
				Status:                    replication.Enabled,
				Priority:                  1,
				DeleteMarkerReplication:   replication.DeleteMarkerReplication{Status: replication.Enabled},
				DeleteReplication:         replication.DeleteReplication{Status: replication.Enabled},
				Filter:                    replication.Filter{},
				ExistingObjectReplication: replication.ExistingObjectReplication{Status: replication.Enabled},
				SourceSelectionCriteria: replication.SourceSelectionCriteria{
					ReplicaModifications: replication.ReplicaModifications{Status: replication.Enabled},
				},
			},
		},
	},
}

var replicationConfigTests = []struct {
	info         ObjectInfo
	name         string
	rcfg         replicationConfig
	dsc          ReplicateDecision
	tgtStatuses  map[string]replication.StatusType
	expectedSync bool
}{
	{ // 1. no replication config
		name:         "no replication config",
		info:         ObjectInfo{Size: 100},
		rcfg:         replicationConfig{Config: nil},
		expectedSync: false,
	},
	{ // 2. existing object replication config enabled, no versioning
		name:         "existing object replication config enabled, no versioning",
		info:         ObjectInfo{Size: 100},
		rcfg:         replicationConfig{Config: &configs[0]},
		expectedSync: false,
	},
	{ // 3. existing object replication config enabled, versioning suspended
		name:         "existing object replication config enabled, versioning suspended",
		info:         ObjectInfo{Size: 100, VersionID: nullVersionID},
		rcfg:         replicationConfig{Config: &configs[0]},
		expectedSync: false,
	},
	{ // 4. existing object replication enabled, versioning enabled; no reset in progress
		name: "existing object replication enabled, versioning enabled; no reset in progress",
		info: ObjectInfo{
			Size:              100,
			ReplicationStatus: replication.Completed,
			VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
		},
		rcfg:         replicationConfig{Config: &configs[0]},
		expectedSync: false,
	},
}

func TestReplicationResync(t *testing.T) {
	ctx := t.Context()
	for i, test := range replicationConfigTests {
		if sync := test.rcfg.Resync(ctx, test.info, test.dsc, test.tgtStatuses); sync.mustResync() != test.expectedSync {
			t.Errorf("Test%d (%s): Resync  got %t , want %t", i+1, test.name, sync.mustResync(), test.expectedSync)
		}
	}
}

var (
	start                   = UTCNow().AddDate(0, 0, -1)
	replicationConfigTests2 = []struct {
		info         ObjectInfo
		name         string
		rcfg         replicationConfig
		dsc          ReplicateDecision
		tgtStatuses  map[string]replication.StatusType
		expectedSync bool
	}{
		{ // Cases 1-4: existing object replication enabled, versioning enabled, no reset - replication status varies
			// 1: Pending replication
			name: "existing object replication on object in Pending replication status",
			info: ObjectInfo{
				Size:                      100,
				ReplicationStatusInternal: "arn1:PENDING;",
				ReplicationStatus:         replication.Pending,
				VersionID:                 "a3348c34-c352-4498-82f0-1098e8b34df9",
			},
			rcfg: replicationConfig{remotes: &madmin.BucketTargets{Targets: []madmin.BucketTarget{{
				Arn: "arn1",
			}}}},
			dsc:          ReplicateDecision{targetsMap: map[string]replicateTargetDecision{"arn1": newReplicateTargetDecision("arn1", true, false)}},
			expectedSync: true,
		},

		{ // 2. replication status Failed
			name: "existing object replication on object in Failed replication status",
			info: ObjectInfo{
				Size:                      100,
				ReplicationStatusInternal: "arn1:FAILED",
				ReplicationStatus:         replication.Failed,
				VersionID:                 "a3348c34-c352-4498-82f0-1098e8b34df9",
			},
			dsc: ReplicateDecision{targetsMap: map[string]replicateTargetDecision{"arn1": newReplicateTargetDecision("arn1", true, false)}},
			rcfg: replicationConfig{remotes: &madmin.BucketTargets{Targets: []madmin.BucketTarget{{
				Arn: "arn1",
			}}}},
			expectedSync: true,
		},
		{ // 3. replication status unset
			name: "existing object replication on pre-existing unreplicated object",
			info: ObjectInfo{
				Size:              100,
				ReplicationStatus: replication.StatusType(""),
				VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
			},
			rcfg: replicationConfig{remotes: &madmin.BucketTargets{Targets: []madmin.BucketTarget{{
				Arn: "arn1",
			}}}},
			dsc:          ReplicateDecision{targetsMap: map[string]replicateTargetDecision{"arn1": newReplicateTargetDecision("arn1", true, false)}},
			expectedSync: true,
		},
		{ // 4. replication status Complete
			name: "existing object replication on object in Completed replication status",
			info: ObjectInfo{
				Size:                      100,
				ReplicationStatusInternal: "arn1:COMPLETED",
				ReplicationStatus:         replication.Completed,
				VersionID:                 "a3348c34-c352-4498-82f0-1098e8b34df9",
			},
			dsc: ReplicateDecision{targetsMap: map[string]replicateTargetDecision{"arn1": newReplicateTargetDecision("arn1", false, false)}},
			rcfg: replicationConfig{remotes: &madmin.BucketTargets{Targets: []madmin.BucketTarget{{
				Arn: "arn1",
			}}}},
			expectedSync: false,
		},
		{ // 5. existing object replication enabled, versioning enabled, replication status Pending & reset ID present
			name: "existing object replication with reset in progress and object in Pending status",
			info: ObjectInfo{
				Size:                      100,
				ReplicationStatusInternal: "arn1:PENDING;",
				ReplicationStatus:         replication.Pending,
				VersionID:                 "a3348c34-c352-4498-82f0-1098e8b34df9",
				UserDefined:               map[string]string{xhttp.MinIOReplicationResetStatus: fmt.Sprintf("%s;abc", UTCNow().AddDate(0, -1, 0).String())},
			},
			expectedSync: true,
			dsc:          ReplicateDecision{targetsMap: map[string]replicateTargetDecision{"arn1": newReplicateTargetDecision("arn1", true, false)}},
			rcfg: replicationConfig{
				remotes: &madmin.BucketTargets{Targets: []madmin.BucketTarget{{
					Arn:             "arn1",
					ResetID:         "xyz",
					ResetBeforeDate: UTCNow(),
				}}},
			},
		},
		{ // 6. existing object replication enabled, versioning enabled, replication status Failed & reset ID present
			name: "existing object replication with reset in progress and object in Failed status",
			info: ObjectInfo{
				Size:                      100,
				ReplicationStatusInternal: "arn1:FAILED;",
				ReplicationStatus:         replication.Failed,
				VersionID:                 "a3348c34-c352-4498-82f0-1098e8b34df9",
				UserDefined:               map[string]string{xhttp.MinIOReplicationResetStatus: fmt.Sprintf("%s;abc", UTCNow().AddDate(0, -1, 0).String())},
			},
			dsc: ReplicateDecision{targetsMap: map[string]replicateTargetDecision{"arn1": newReplicateTargetDecision("arn1", true, false)}},
			rcfg: replicationConfig{
				remotes: &madmin.BucketTargets{Targets: []madmin.BucketTarget{{
					Arn:             "arn1",
					ResetID:         "xyz",
					ResetBeforeDate: UTCNow(),
				}}},
			},
			expectedSync: true,
		},
		{ // 7. existing object replication enabled, versioning enabled, replication status unset & reset ID present
			name: "existing object replication with reset in progress and object never replicated before",
			info: ObjectInfo{
				Size:              100,
				ReplicationStatus: replication.StatusType(""),
				VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
				UserDefined:       map[string]string{xhttp.MinIOReplicationResetStatus: fmt.Sprintf("%s;abc", UTCNow().AddDate(0, -1, 0).String())},
			},
			dsc: ReplicateDecision{targetsMap: map[string]replicateTargetDecision{"arn1": newReplicateTargetDecision("arn1", true, false)}},
			rcfg: replicationConfig{
				remotes: &madmin.BucketTargets{Targets: []madmin.BucketTarget{{
					Arn:             "arn1",
					ResetID:         "xyz",
					ResetBeforeDate: UTCNow(),
				}}},
			},

			expectedSync: true,
		},

		{ // 8. existing object replication enabled, versioning enabled, replication status Complete & reset ID present
			name: "existing object replication enabled - reset in progress for an object in Completed status",
			info: ObjectInfo{
				Size:                      100,
				ReplicationStatusInternal: "arn1:COMPLETED;",
				ReplicationStatus:         replication.Completed,
				VersionID:                 "a3348c34-c352-4498-82f0-1098e8b34df8",
				UserDefined:               map[string]string{xhttp.MinIOReplicationResetStatus: fmt.Sprintf("%s;abc", UTCNow().AddDate(0, -1, 0).String())},
			},
			expectedSync: true,
			dsc:          ReplicateDecision{targetsMap: map[string]replicateTargetDecision{"arn1": newReplicateTargetDecision("arn1", true, false)}},
			rcfg: replicationConfig{
				remotes: &madmin.BucketTargets{Targets: []madmin.BucketTarget{{
					Arn:             "arn1",
					ResetID:         "xyz",
					ResetBeforeDate: UTCNow(),
				}}},
			},
		},
		{ // 9. existing object replication enabled, versioning enabled, replication status Pending & reset ID different
			name: "existing object replication enabled, newer reset in progress on object in Pending replication status",
			info: ObjectInfo{
				Size:                      100,
				ReplicationStatusInternal: "arn1:PENDING;",

				ReplicationStatus: replication.Pending,
				VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
				UserDefined:       map[string]string{xhttp.MinIOReplicationResetStatus: fmt.Sprintf("%s;%s", UTCNow().AddDate(0, 0, -1).Format(http.TimeFormat), "abc")},
				ModTime:           UTCNow().AddDate(0, 0, -2),
			},
			expectedSync: true,
			dsc:          ReplicateDecision{targetsMap: map[string]replicateTargetDecision{"arn1": newReplicateTargetDecision("arn1", true, false)}},
			rcfg: replicationConfig{
				remotes: &madmin.BucketTargets{Targets: []madmin.BucketTarget{{
					Arn:             "arn1",
					ResetID:         "xyz",
					ResetBeforeDate: UTCNow(),
				}}},
			},
		},
		{ // 10. existing object replication enabled, versioning enabled, replication status Complete & reset done
			name: "reset done on object in Completed Status - ineligbile for re-replication",
			info: ObjectInfo{
				Size:                      100,
				ReplicationStatusInternal: "arn1:COMPLETED;",
				ReplicationStatus:         replication.Completed,
				VersionID:                 "a3348c34-c352-4498-82f0-1098e8b34df9",
				UserDefined:               map[string]string{xhttp.MinIOReplicationResetStatus: fmt.Sprintf("%s;%s", start.Format(http.TimeFormat), "xyz")},
			},
			expectedSync: false,
			dsc:          ReplicateDecision{targetsMap: map[string]replicateTargetDecision{"arn1": newReplicateTargetDecision("arn1", true, false)}},
			rcfg: replicationConfig{
				remotes: &madmin.BucketTargets{Targets: []madmin.BucketTarget{{
					Arn:             "arn1",
					ResetID:         "xyz",
					ResetBeforeDate: start,
				}}},
			},
		},
	}
)

func TestReplicationResyncwrapper(t *testing.T) {
	for i, test := range replicationConfigTests2 {
		if sync := test.rcfg.resync(test.info, test.dsc, test.tgtStatuses); sync.mustResync() != test.expectedSync {
			t.Errorf("%s (%s): Replicationresync  got %t , want %t", fmt.Sprintf("Test%d - %s", i+1, time.Now().Format(http.TimeFormat)), test.name, sync.mustResync(), test.expectedSync)
		}
	}
}
