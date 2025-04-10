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
	"testing"

	"github.com/minio/minio/internal/bucket/replication"
)

var replicatedInfosTests = []struct {
	name                              string
	tgtInfos                          []replicatedTargetInfo
	expectedCompletedSize             int64
	expectedReplicationStatusInternal string
	expectedReplicationStatus         replication.StatusType
	expectedOpType                    replication.Type
	expectedAction                    replicationAction
}{
	{ // 1. empty tgtInfos slice
		name:                              "no replicated targets",
		tgtInfos:                          []replicatedTargetInfo{},
		expectedCompletedSize:             0,
		expectedReplicationStatusInternal: "",
		expectedReplicationStatus:         replication.StatusType(""),
		expectedOpType:                    replication.UnsetReplicationType,
		expectedAction:                    replicateNone,
	},
	{ // 2. replication completed to single target
		name: "replication completed to single target",
		tgtInfos: []replicatedTargetInfo{
			{
				Arn:                   "arn1",
				Size:                  249,
				PrevReplicationStatus: replication.Pending,
				ReplicationStatus:     replication.Completed,
				OpType:                replication.ObjectReplicationType,
				ReplicationAction:     replicateAll,
			},
		},
		expectedCompletedSize:             249,
		expectedReplicationStatusInternal: "arn1=COMPLETED;",
		expectedReplicationStatus:         replication.Completed,
		expectedOpType:                    replication.ObjectReplicationType,
		expectedAction:                    replicateAll,
	},
	{ // 3. replication completed to single target; failed to another
		name: "replication completed to single target",
		tgtInfos: []replicatedTargetInfo{
			{
				Arn:                   "arn1",
				Size:                  249,
				PrevReplicationStatus: replication.Pending,
				ReplicationStatus:     replication.Completed,
				OpType:                replication.ObjectReplicationType,
				ReplicationAction:     replicateAll,
			},
			{
				Arn:                   "arn2",
				Size:                  249,
				PrevReplicationStatus: replication.Pending,
				ReplicationStatus:     replication.Failed,
				OpType:                replication.ObjectReplicationType,
				ReplicationAction:     replicateAll,
			},
		},
		expectedCompletedSize:             249,
		expectedReplicationStatusInternal: "arn1=COMPLETED;arn2=FAILED;",
		expectedReplicationStatus:         replication.Failed,
		expectedOpType:                    replication.ObjectReplicationType,
		expectedAction:                    replicateAll,
	},
	{ // 4. replication pending on one target; failed to another
		name: "replication completed to single target",
		tgtInfos: []replicatedTargetInfo{
			{
				Arn:                   "arn1",
				Size:                  249,
				PrevReplicationStatus: replication.Pending,
				ReplicationStatus:     replication.Pending,
				OpType:                replication.ObjectReplicationType,
				ReplicationAction:     replicateAll,
			},
			{
				Arn:                   "arn2",
				Size:                  249,
				PrevReplicationStatus: replication.Pending,
				ReplicationStatus:     replication.Failed,
				OpType:                replication.ObjectReplicationType,
				ReplicationAction:     replicateAll,
			},
		},
		expectedCompletedSize:             0,
		expectedReplicationStatusInternal: "arn1=PENDING;arn2=FAILED;",
		expectedReplicationStatus:         replication.Failed,
		expectedOpType:                    replication.ObjectReplicationType,
		expectedAction:                    replicateAll,
	},
}

func TestReplicatedInfos(t *testing.T) {
	for i, test := range replicatedInfosTests {
		rinfos := replicatedInfos{
			Targets: test.tgtInfos,
		}
		if actualSize := rinfos.CompletedSize(); actualSize != test.expectedCompletedSize {
			t.Errorf("Test%d (%s): Size  got %d , want %d", i+1, test.name, actualSize, test.expectedCompletedSize)
		}
		if repStatusStr := rinfos.ReplicationStatusInternal(); repStatusStr != test.expectedReplicationStatusInternal {
			t.Errorf("Test%d (%s): Internal replication status  got %s , want %s", i+1, test.name, repStatusStr, test.expectedReplicationStatusInternal)
		}
		if repStatus := rinfos.ReplicationStatus(); repStatus != test.expectedReplicationStatus {
			t.Errorf("Test%d (%s): ReplicationStatus  got %s , want %s", i+1, test.name, repStatus, test.expectedReplicationStatus)
		}
		if action := rinfos.Action(); action != test.expectedAction {
			t.Errorf("Test%d (%s): Action  got %s , want %s", i+1, test.name, action, test.expectedAction)
		}
	}
}

var parseReplicationDecisionTest = []struct {
	name   string
	dsc    string
	expDsc ReplicateDecision
	expErr error
}{
	{ // 1.
		name: "empty string",
		dsc:  "",
		expDsc: ReplicateDecision{
			targetsMap: map[string]replicateTargetDecision{},
		},
		expErr: nil,
	},

	{ // 2.
		name:   "replicate decision for one target",
		dsc:    "arn:minio:replication::id:bucket=true;false;arn:minio:replication::id:bucket;id",
		expErr: nil,
		expDsc: ReplicateDecision{
			targetsMap: map[string]replicateTargetDecision{
				"arn:minio:replication::id:bucket": newReplicateTargetDecision("arn:minio:replication::id:bucket", true, false),
			},
		},
	},
	{ // 3.
		name:   "replicate decision for multiple targets",
		dsc:    "arn:minio:replication::id:bucket=true;false;arn:minio:replication::id:bucket;id,arn:minio:replication::id2:bucket=false;true;arn:minio:replication::id2:bucket;id2",
		expErr: nil,
		expDsc: ReplicateDecision{
			targetsMap: map[string]replicateTargetDecision{
				"arn:minio:replication::id:bucket":  newReplicateTargetDecision("arn:minio:replication::id:bucket", true, false),
				"arn:minio:replication::id2:bucket": newReplicateTargetDecision("arn:minio:replication::id2:bucket", false, true),
			},
		},
	},
	{ // 4.
		name:   "invalid format replicate decision for one target",
		dsc:    "arn:minio:replication::id:bucket:true;false;arn:minio:replication::id:bucket;id",
		expErr: errInvalidReplicateDecisionFormat,
		expDsc: ReplicateDecision{
			targetsMap: map[string]replicateTargetDecision{
				"arn:minio:replication::id:bucket": newReplicateTargetDecision("arn:minio:replication::id:bucket", true, false),
			},
		},
	},
}

func TestParseReplicateDecision(t *testing.T) {
	for i, test := range parseReplicationDecisionTest {
		dsc, err := parseReplicateDecision(t.Context(), "bucket", test.expDsc.String())
		if err != nil {
			if test.expErr != err {
				t.Errorf("Test%d (%s): Expected parse error got %t , want %t", i+1, test.name, err, test.expErr)
			}
			continue
		}
		if len(dsc.targetsMap) != len(test.expDsc.targetsMap) {
			t.Errorf("Test%d (%s): Invalid number of entries in targetsMap  got %d , want %d", i+1, test.name, len(dsc.targetsMap), len(test.expDsc.targetsMap))
		}
		for arn, tdsc := range dsc.targetsMap {
			expDsc, ok := test.expDsc.targetsMap[arn]
			if !ok || expDsc != tdsc {
				t.Errorf("Test%d (%s): Invalid  target replicate decision: got %+v, want %+v", i+1, test.name, tdsc, expDsc)
			}
		}
	}
}

var replicationStateTest = []struct {
	name      string
	rs        ReplicationState
	arn       string
	expStatus replication.StatusType
}{
	{ // 1. no replication status header
		name:      "no replicated targets",
		rs:        ReplicationState{},
		expStatus: replication.StatusType(""),
	},
	{ // 2. replication status for one target
		name:      "replication status for one target",
		rs:        ReplicationState{ReplicationStatusInternal: "arn1=PENDING;", Targets: map[string]replication.StatusType{"arn1": "PENDING"}},
		expStatus: replication.Pending,
	},
	{ // 3. replication status for one target - incorrect format
		name:      "replication status for one target",
		rs:        ReplicationState{ReplicationStatusInternal: "arn1=PENDING"},
		expStatus: replication.StatusType(""),
	},
	{ // 4. replication status for 3 targets, one of them failed
		name: "replication status for 3 targets - one failed",
		rs: ReplicationState{
			ReplicationStatusInternal: "arn1=COMPLETED;arn2=COMPLETED;arn3=FAILED;",
			Targets:                   map[string]replication.StatusType{"arn1": "COMPLETED", "arn2": "COMPLETED", "arn3": "FAILED"},
		},
		expStatus: replication.Failed,
	},
	{ // 5. replication status for replica version
		name:      "replication status for replica version",
		rs:        ReplicationState{ReplicationStatusInternal: string(replication.Replica)},
		expStatus: replication.Replica,
	},
}

func TestCompositeReplicationStatus(t *testing.T) {
	for i, test := range replicationStateTest {
		if rstatus := test.rs.CompositeReplicationStatus(); rstatus != test.expStatus {
			t.Errorf("Test%d (%s): Overall replication status  got %s , want %s", i+1, test.name, rstatus, test.expStatus)
		}
	}
}
