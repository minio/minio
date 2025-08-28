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

package replication

import (
	"bytes"
	"fmt"
	"testing"
)

func TestMetadataReplicate(t *testing.T) {
	testCases := []struct {
		inputConfig    string
		opts           ObjectOpts
		expectedResult bool
	}{
		// case 1 - rule with replica modification enabled; not a replica
		{
			inputConfig:    `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination><SourceSelectionCriteria><ReplicaModifications><Status>Enabled</Status></ReplicaModifications></SourceSelectionCriteria></Rule></ReplicationConfiguration>`,
			opts:           ObjectOpts{Name: "c1test", DeleteMarker: false, OpType: ObjectReplicationType, Replica: false}, // 1. Replica mod sync enabled; not a replica
			expectedResult: true,
		},
		// case 2 - rule with replica modification disabled; a replica
		{
			inputConfig:    `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination><SourceSelectionCriteria><ReplicaModifications><Status>Disabled</Status></ReplicaModifications></SourceSelectionCriteria></Rule></ReplicationConfiguration>`,
			opts:           ObjectOpts{Name: "c2test", DeleteMarker: false, OpType: ObjectReplicationType, Replica: true}, // 1. Replica mod sync enabled; a replica
			expectedResult: false,
		},
		// case 3 - rule with replica modification disabled; not a replica
		{
			inputConfig:    `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination><SourceSelectionCriteria><ReplicaModifications><Status>Disabled</Status></ReplicaModifications></SourceSelectionCriteria></Rule></ReplicationConfiguration>`,
			opts:           ObjectOpts{Name: "c2test", DeleteMarker: false, OpType: ObjectReplicationType, Replica: false}, // 1. Replica mod sync disabled; not a replica
			expectedResult: true,
		},

		// case 4 - rule with replica modification enabled; a replica
		{
			inputConfig:    `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination><SourceSelectionCriteria><ReplicaModifications><Status>Enabled</Status></ReplicaModifications></SourceSelectionCriteria></Rule></ReplicationConfiguration>`,
			opts:           ObjectOpts{Name: "c2test", DeleteMarker: false, OpType: MetadataReplicationType, Replica: true}, // 1. Replica mod sync enabled;  a replica
			expectedResult: true,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test_%d", i+1), func(t *testing.T) {
			cfg, err := ParseConfig(bytes.NewReader([]byte(tc.inputConfig)))
			if err != nil {
				t.Fatalf("Got unexpected error: %v", err)
			}
			if got := cfg.Rules[0].MetadataReplicate(tc.opts); got != tc.expectedResult {
				t.Fatalf("Expected result with recursive set to false: `%v`, got: `%v`", tc.expectedResult, got)
			}
		})
	}
}
