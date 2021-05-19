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
	"fmt"
	"net/http"
	"testing"

	xhttp "github.com/minio/minio/cmd/http"

	"github.com/minio/minio/pkg/bucket/replication"
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
	rcfg         replicationConfig
	expectedSync bool
}{
	{ //1. no replication config
		info:         ObjectInfo{Size: 100},
		rcfg:         replicationConfig{Config: nil},
		expectedSync: false,
	},
	{ //2. existing object replication config enabled, no versioning
		info:         ObjectInfo{Size: 100},
		rcfg:         replicationConfig{Config: &configs[0]},
		expectedSync: false,
	},
	{ //3. existing object replication config enabled, versioning suspended
		info:         ObjectInfo{Size: 100, VersionID: nullVersionID},
		rcfg:         replicationConfig{Config: &configs[0]},
		expectedSync: false,
	},
	{ //4. existing object replication enabled, versioning enabled; no reset in progress
		info: ObjectInfo{Size: 100,
			ReplicationStatus: replication.Completed,
			VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
		},
		rcfg:         replicationConfig{Config: &configs[0]},
		expectedSync: false,
	},
}

func TestReplicationResync(t *testing.T) {
	ctx := context.Background()
	for i, test := range replicationConfigTests {
		if sync := test.rcfg.Resync(ctx, test.info); sync != test.expectedSync {
			t.Errorf("Test %d: Resync  got %t , want %t", i+1, sync, test.expectedSync)
		}
	}
}

var start = UTCNow().AddDate(0, 0, -1)
var replicationConfigTests2 = []struct {
	info         ObjectInfo
	replicate    bool
	rcfg         replicationConfig
	expectedSync bool
}{
	{ // Cases 1-4: existing object replication enabled, versioning enabled, no reset - replication status varies
		// 1: Pending replication
		info: ObjectInfo{Size: 100,
			ReplicationStatus: replication.Pending,
			VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
		},
		replicate:    true,
		expectedSync: true,
	},
	{ // 2. replication status Failed
		info: ObjectInfo{Size: 100,
			ReplicationStatus: replication.Failed,
			VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
		},
		replicate:    true,
		expectedSync: true,
	},
	{ //3. replication status unset
		info: ObjectInfo{Size: 100,
			ReplicationStatus: replication.StatusType(""),
			VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
		},
		replicate:    true,
		expectedSync: true,
	},
	{ //4. replication status Complete
		info: ObjectInfo{Size: 100,
			ReplicationStatus: replication.Completed,
			VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
		},
		replicate:    true,
		expectedSync: false,
	},
	{ //5. existing object replication enabled, versioning enabled, replication status Pending & reset ID present
		info: ObjectInfo{Size: 100,
			ReplicationStatus: replication.Pending,
			VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
		},
		replicate:    true,
		expectedSync: true,
		rcfg:         replicationConfig{ResetID: "xyz", ResetBeforeDate: UTCNow()},
	},
	{ //6. existing object replication enabled, versioning enabled, replication status Failed & reset ID present
		info: ObjectInfo{Size: 100,
			ReplicationStatus: replication.Failed,
			VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
		},
		replicate:    true,
		expectedSync: true,
		rcfg:         replicationConfig{ResetID: "xyz", ResetBeforeDate: UTCNow()},
	},
	{ //7. existing object replication enabled, versioning enabled, replication status unset & reset ID present
		info: ObjectInfo{Size: 100,
			ReplicationStatus: replication.StatusType(""),
			VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
		},
		replicate:    true,
		expectedSync: true,
		rcfg:         replicationConfig{ResetID: "xyz", ResetBeforeDate: UTCNow()},
	},
	{ //8. existing object replication enabled, versioning enabled, replication status Complete & reset ID present
		info: ObjectInfo{Size: 100,
			ReplicationStatus: replication.Completed,
			VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
		},
		replicate:    true,
		expectedSync: true,
		rcfg:         replicationConfig{ResetID: "xyz", ResetBeforeDate: UTCNow()},
	},
	{ //9. existing object replication enabled, versioning enabled, replication status Pending & reset ID different
		info: ObjectInfo{Size: 100,
			ReplicationStatus: replication.Pending,
			VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
			UserDefined:       map[string]string{xhttp.MinIOReplicationResetStatus: fmt.Sprintf("%s;%s", UTCNow().Format(http.TimeFormat), "xyz")},
			ModTime:           UTCNow().AddDate(0, 0, -1),
		},
		replicate:    true,
		expectedSync: true,
		rcfg:         replicationConfig{ResetID: "abc", ResetBeforeDate: UTCNow()},
	},
	{ //10. existing object replication enabled, versioning enabled, replication status Complete & reset done
		info: ObjectInfo{Size: 100,
			ReplicationStatus: replication.Completed,
			VersionID:         "a3348c34-c352-4498-82f0-1098e8b34df9",
			UserDefined:       map[string]string{xhttp.MinIOReplicationResetStatus: fmt.Sprintf("%s;%s", start.Format(http.TimeFormat), "xyz")},
		},
		replicate:    true,
		expectedSync: false,
		rcfg:         replicationConfig{ResetID: "xyz", ResetBeforeDate: UTCNow()},
	},
}

func TestReplicationResyncwrapper(t *testing.T) {
	for i, test := range replicationConfigTests2 {
		if sync := test.rcfg.resync(test.info, test.replicate); sync != test.expectedSync {
			t.Errorf("Test %d: Replicationresync  got %t , want %t", i+1, sync, test.expectedSync)
		}
	}
}
