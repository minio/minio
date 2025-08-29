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

func TestParseAndValidateReplicationConfig(t *testing.T) {
	testCases := []struct {
		inputConfig           string
		expectedParsingErr    error
		expectedValidationErr error
		destBucket            string
		sameTarget            bool
	}{
		{ // 1 Invalid delete marker status in replication config
			inputConfig:           `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>string</Status></DeleteMarkerReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    nil,
			expectedValidationErr: errInvalidDeleteMarkerReplicationStatus,
		},
		// 2 No delete replication status in replication config
		{
			inputConfig:           `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    nil,
			expectedValidationErr: nil,
		},
		// 3 valid replication config
		{
			inputConfig:           `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    nil,
			expectedValidationErr: nil,
		},
		// 4 missing role in config and destination ARN is in legacy format
		{
			inputConfig: `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			// destination bucket in config different from bucket specified
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    nil,
			expectedValidationErr: errDestinationArnMissing,
		},
		// 5 replication destination in different rules not identical
		{
			inputConfig:           `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role></Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:minio:replication:::destinationbucket</Bucket></Destination></Rule><Rule><Status>Enabled</Status><Priority>3</Priority><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:minio:replication:::destinationbucket2</Bucket></Destination></Rule></ReplicationConfiguration>`,
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    nil,
			expectedValidationErr: nil,
		},
		// 6 missing rule status in replication config
		{
			inputConfig:           `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    nil,
			expectedValidationErr: errEmptyRuleStatus,
		},
		// 7 invalid rule status in replication config
		{
			inputConfig:           `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enssabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    nil,
			expectedValidationErr: errInvalidRuleStatus,
		},
		// 8 invalid rule id exceeds length allowed in replication config
		{
			inputConfig:           `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><ID>vsUVERgOc8zZYagLSzSa5lE8qeI6nh1lyLNS4R9W052yfecrhhepGboswSWMMNO8CPcXM4GM3nKyQ72EadlMzzZBFoYWKn7ju5GoE5w9c57a0piHR1vexpdd9FrMquiruvAJ0MTGVupm0EegMVxoIOdjx7VgZhGrmi2XDvpVEFT7WmYMA9fSK297XkTHWyECaNHBySJ1Qp4vwX8tPNauKpfHx4kzUpnKe1PZbptGMWbY5qTcwlNuMhVSmgFffShq</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    nil,
			expectedValidationErr: errInvalidRuleID,
		},
		// 9 invalid priority status in replication config
		{
			inputConfig:           `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    nil,
			expectedValidationErr: errReplicationUniquePriority,
		},
		// 10 no rule in replication config
		{
			inputConfig:           `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role></ReplicationConfiguration>`,
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    nil,
			expectedValidationErr: errReplicationNoRule,
		},
		// 11 no destination in replication config
		{
			inputConfig:           `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination></Destination></Rule></ReplicationConfiguration>`,
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    Errorf("invalid destination '%v'", ""),
			expectedValidationErr: nil,
		},
		// 12 destination not matching ARN in replication config
		{
			inputConfig:           `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>destinationbucket2</Bucket></Destination></Rule></ReplicationConfiguration>`,
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    fmt.Errorf("invalid destination '%v'", "destinationbucket2"),
			expectedValidationErr: nil,
		},
		// 13 missing role in config and destination ARN has target ARN
		{
			inputConfig: `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:minio:replication::8320b6d18f9032b4700f1f03b50d8d1853de8f22cab86931ee794e12f190852c:destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			// destination bucket in config different from bucket specified
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    nil,
			expectedValidationErr: nil,
		},
		// 14 role absent in config and destination ARN has target ARN in invalid format
		{
			inputConfig: `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:xx:replication::8320b6d18f9032b4700f1f03b50d8d1853de8f22cab86931ee794e12f190852c:destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			// destination bucket in config different from bucket specified
			destBucket:            "destinationbucket",
			sameTarget:            false,
			expectedParsingErr:    fmt.Errorf("invalid destination '%v'", "arn:xx:replication::8320b6d18f9032b4700f1f03b50d8d1853de8f22cab86931ee794e12f190852c:destinationbucket"),
			expectedValidationErr: nil,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			cfg, err := ParseConfig(bytes.NewReader([]byte(tc.inputConfig)))
			if err != nil && tc.expectedParsingErr != nil && err.Error() != tc.expectedParsingErr.Error() {
				t.Fatalf("%d: Expected '%v' during parsing but got '%v'", i+1, tc.expectedParsingErr, err)
			}
			if err == nil && tc.expectedParsingErr != nil {
				t.Fatalf("%d: Expected '%v' during parsing but got '%v'", i+1, tc.expectedParsingErr, err)
			}
			if tc.expectedParsingErr != nil {
				// We already expect a parsing error,
				// no need to continue this test.
				return
			}
			err = cfg.Validate(tc.destBucket, tc.sameTarget)
			if err != tc.expectedValidationErr {
				t.Fatalf("%d: Expected %v during parsing but got %v", i+1, tc.expectedValidationErr, err)
			}
		})
	}
}

func TestReplicate(t *testing.T) {
	cfgs := []Config{
		{ // Config0 - Replication config has no filters, all replication enabled
			Rules: []Rule{
				{
					Status:                  Enabled,
					Priority:                3,
					DeleteMarkerReplication: DeleteMarkerReplication{Status: Enabled},
					DeleteReplication:       DeleteReplication{Status: Enabled},
					Filter:                  Filter{},
				},
			},
		},
		{ // Config1 - Replication config has no filters, delete,delete-marker replication disabled
			Rules: []Rule{
				{
					Status:                  Enabled,
					Priority:                3,
					DeleteMarkerReplication: DeleteMarkerReplication{Status: Disabled},
					DeleteReplication:       DeleteReplication{Status: Disabled},
					Filter:                  Filter{},
				},
			},
		},
		{ // Config2 - Replication config has filters and more than 1 matching rule, delete,delete-marker replication disabled
			Rules: []Rule{
				{
					Status:                  Enabled,
					Priority:                2,
					DeleteMarkerReplication: DeleteMarkerReplication{Status: Disabled},
					DeleteReplication:       DeleteReplication{Status: Enabled},
					Filter:                  Filter{Prefix: "xy", And: And{}, Tag: Tag{Key: "k1", Value: "v1"}},
				},
				{
					Status:                  Enabled,
					Priority:                1,
					DeleteMarkerReplication: DeleteMarkerReplication{Status: Enabled},
					DeleteReplication:       DeleteReplication{Status: Disabled},
					Filter:                  Filter{Prefix: "xyz"},
				},
			},
		},
		{ // Config3 - Replication config has filters and no overlapping rules
			Rules: []Rule{
				{
					Status:                  Enabled,
					Priority:                2,
					DeleteMarkerReplication: DeleteMarkerReplication{Status: Disabled},
					DeleteReplication:       DeleteReplication{Status: Enabled},
					Filter:                  Filter{Prefix: "xy", And: And{}, Tag: Tag{Key: "k1", Value: "v1"}},
				},
				{
					Status:                  Enabled,
					Priority:                1,
					DeleteMarkerReplication: DeleteMarkerReplication{Status: Enabled},
					DeleteReplication:       DeleteReplication{Status: Disabled},
					Filter:                  Filter{Prefix: "abc"},
				},
			},
		},
		{ // Config4 - Replication config has filters and SourceSelectionCriteria Disabled
			Rules: []Rule{
				{
					Status:                  Enabled,
					Priority:                2,
					DeleteMarkerReplication: DeleteMarkerReplication{Status: Enabled},
					DeleteReplication:       DeleteReplication{Status: Enabled},
					SourceSelectionCriteria: SourceSelectionCriteria{ReplicaModifications: ReplicaModifications{Status: Disabled}},
				},
			},
		},
	}
	testCases := []struct {
		opts           ObjectOpts
		c              Config
		expectedResult bool
	}{
		// using config 1 - no filters, all replication enabled
		{ObjectOpts{}, cfgs[0], false},                                // 1. invalid ObjectOpts missing object name
		{ObjectOpts{Name: "c1test"}, cfgs[0], true},                   // 2. valid ObjectOpts passing empty Filter
		{ObjectOpts{Name: "c1test", VersionID: "vid"}, cfgs[0], true}, // 3. valid ObjectOpts passing empty Filter

		{ObjectOpts{Name: "c1test", DeleteMarker: true, OpType: DeleteReplicationType}, cfgs[0], true},                               // 4. DeleteMarker version replication valid case - matches DeleteMarkerReplication status
		{ObjectOpts{Name: "c1test", VersionID: "vid", OpType: DeleteReplicationType}, cfgs[0], true},                                 // 5. permanent delete of version, matches DeleteReplication status - valid case
		{ObjectOpts{Name: "c1test", VersionID: "vid", DeleteMarker: true, OpType: DeleteReplicationType}, cfgs[0], true},             // 6. permanent delete of version, matches DeleteReplication status
		{ObjectOpts{Name: "c1test", VersionID: "vid", DeleteMarker: true, SSEC: true, OpType: DeleteReplicationType}, cfgs[0], true}, // 7. permanent delete of version
		{ObjectOpts{Name: "c1test", DeleteMarker: true, SSEC: true, OpType: DeleteReplicationType}, cfgs[0], true},                   // 8. setting DeleteMarker on SSE-C encrypted object
		{ObjectOpts{Name: "c1test", SSEC: true}, cfgs[0], true},                                                                      // 9. replication of SSE-C encrypted object

		//  using config 2 - no filters, only replication of object, metadata enabled
		{ObjectOpts{Name: "c2test"}, cfgs[1], true},                                                                                   // 10. valid ObjectOpts passing empty Filter
		{ObjectOpts{Name: "c2test", DeleteMarker: true, OpType: DeleteReplicationType}, cfgs[1], false},                               // 11. DeleteMarker version replication  not allowed due to DeleteMarkerReplication status
		{ObjectOpts{Name: "c2test", VersionID: "vid", OpType: DeleteReplicationType}, cfgs[1], false},                                 // 12. permanent delete of version, disallowed by DeleteReplication status
		{ObjectOpts{Name: "c2test", VersionID: "vid", DeleteMarker: true, OpType: DeleteReplicationType}, cfgs[1], false},             // 13. permanent delete of DeleteMarker version, disallowed by DeleteReplication status
		{ObjectOpts{Name: "c2test", VersionID: "vid", DeleteMarker: true, SSEC: true, OpType: DeleteReplicationType}, cfgs[1], false}, // 14. permanent delete of version, disqualified by SSE-C & DeleteReplication status
		{ObjectOpts{Name: "c2test", DeleteMarker: true, SSEC: true, OpType: DeleteReplicationType}, cfgs[1], false},                   // 15. setting DeleteMarker on SSE-C encrypted object, disqualified by SSE-C & DeleteMarkerReplication status
		{ObjectOpts{Name: "c2test", SSEC: true}, cfgs[1], true},                                                                       // 16. replication of SSE-C encrypted object
		// using config 2 - has more than one rule with overlapping prefixes
		{ObjectOpts{Name: "xy/c3test", UserTags: "k1=v1"}, cfgs[2], true},                                                                       // 17. matches rule 1 for replication of content/metadata
		{ObjectOpts{Name: "xyz/c3test", UserTags: "k1=v1"}, cfgs[2], true},                                                                      // 18. matches rule 1 for replication of content/metadata
		{ObjectOpts{Name: "xyz/c3test", UserTags: "k1=v1", DeleteMarker: true, OpType: DeleteReplicationType}, cfgs[2], false},                  // 19. matches rule 1 - DeleteMarker replication disallowed by rule
		{ObjectOpts{Name: "xyz/c3test", UserTags: "k1=v1", DeleteMarker: true, VersionID: "vid", OpType: DeleteReplicationType}, cfgs[2], true}, // 20. matches rule 1 - DeleteReplication allowed by rule for permanent delete of DeleteMarker
		{ObjectOpts{Name: "xyz/c3test", UserTags: "k1=v1", VersionID: "vid", OpType: DeleteReplicationType}, cfgs[2], true},                     // 21. matches rule 1 - DeleteReplication allowed by rule for permanent delete of version
		{ObjectOpts{Name: "xyz/c3test"}, cfgs[2], true},                                                                                         // 22. matches rule 2 for replication of content/metadata
		{ObjectOpts{Name: "xy/c3test", UserTags: "k1=v2"}, cfgs[2], false},                                                                      // 23. does not match rule1 because tag value does not pass filter
		{ObjectOpts{Name: "xyz/c3test", DeleteMarker: true, OpType: DeleteReplicationType}, cfgs[2], true},                                      // 24. matches rule 2 - DeleteMarker replication allowed by rule
		{ObjectOpts{Name: "xyz/c3test", DeleteMarker: true, VersionID: "vid", OpType: DeleteReplicationType}, cfgs[2], false},                   // 25. matches rule 2 - DeleteReplication disallowed by rule for permanent delete of DeleteMarker
		{ObjectOpts{Name: "xyz/c3test", VersionID: "vid", OpType: DeleteReplicationType}, cfgs[2], false},                                       // 26. matches rule 1 - DeleteReplication disallowed by rule for permanent delete of version
		{ObjectOpts{Name: "abc/c3test"}, cfgs[2], false},                                                                                        // 27. matches no rule because object prefix does not match

		// using config 3 - has no overlapping rules
		{ObjectOpts{Name: "xy/c4test", UserTags: "k1=v1"}, cfgs[3], true},                                                                       // 28. matches rule 1 for replication of content/metadata
		{ObjectOpts{Name: "xa/c4test", UserTags: "k1=v1"}, cfgs[3], false},                                                                      // 29. no rule match object prefix not in rules
		{ObjectOpts{Name: "xyz/c4test", DeleteMarker: true, OpType: DeleteReplicationType}, cfgs[3], false},                                     // 30. rule 1 not matched because of tags filter
		{ObjectOpts{Name: "xyz/c4test", UserTags: "k1=v1", DeleteMarker: true, OpType: DeleteReplicationType}, cfgs[3], false},                  // 31. matches rule 1 - DeleteMarker replication disallowed by rule
		{ObjectOpts{Name: "xyz/c4test", UserTags: "k1=v1", DeleteMarker: true, VersionID: "vid", OpType: DeleteReplicationType}, cfgs[3], true}, // 32. matches rule 1 - DeleteReplication allowed by rule for permanent delete of DeleteMarker
		{ObjectOpts{Name: "xyz/c4test", UserTags: "k1=v1", VersionID: "vid", OpType: DeleteReplicationType}, cfgs[3], true},                     // 33. matches rule 1 - DeleteReplication allowed by rule for permanent delete of version
		{ObjectOpts{Name: "abc/c4test"}, cfgs[3], true},                                                                                         // 34. matches rule 2 for replication of content/metadata
		{ObjectOpts{Name: "abc/c4test", UserTags: "k1=v2"}, cfgs[3], true},                                                                      // 35. matches rule 2 for replication of content/metadata
		{ObjectOpts{Name: "abc/c4test", DeleteMarker: true, OpType: DeleteReplicationType}, cfgs[3], true},                                      // 36. matches rule 2 - DeleteMarker replication allowed by rule
		{ObjectOpts{Name: "abc/c4test", DeleteMarker: true, VersionID: "vid", OpType: DeleteReplicationType}, cfgs[3], false},                   // 37. matches rule 2 - DeleteReplication disallowed by rule for permanent delete of DeleteMarker
		{ObjectOpts{Name: "abc/c4test", VersionID: "vid", OpType: DeleteReplicationType}, cfgs[3], false},                                       // 38. matches rule 2 - DeleteReplication disallowed by rule for permanent delete of version
		//  using config 4 - with replica modification sync disabled.
		{ObjectOpts{Name: "xy/c5test", UserTags: "k1=v1", Replica: true}, cfgs[4], false}, // 39. replica syncing disabled, this object is a replica
		{ObjectOpts{Name: "xa/c5test", UserTags: "k1=v1", Replica: false}, cfgs[4], true}, // 40. replica syncing disabled, this object is NOT a replica
	}

	for _, testCase := range testCases {
		t.Run(testCase.opts.Name, func(t *testing.T) {
			result := testCase.c.Replicate(testCase.opts)
			if result != testCase.expectedResult {
				t.Errorf("expected: %v, got: %v", testCase.expectedResult, result)
			}
		})
	}
}

func TestHasActiveRules(t *testing.T) {
	testCases := []struct {
		inputConfig    string
		prefix         string
		expectedNonRec bool
		expectedRec    bool
	}{
		// case 1 - only one rule which is in Disabled status
		{
			inputConfig:    `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Disabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			prefix:         "miss/prefix",
			expectedNonRec: false,
			expectedRec:    false,
		},
		// case 2 - only one rule which matches prefix filter
		{
			inputConfig:    `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Filter><Prefix>key/prefix</Prefix></Filter><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			prefix:         "key/prefix1",
			expectedNonRec: true,
			expectedRec:    true,
		},
		// case 3 - empty prefix
		{
			inputConfig:    `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			prefix:         "key-prefix",
			expectedNonRec: true,
			expectedRec:    true,
		},
		// case 4 - has Filter based on prefix
		{
			inputConfig:    `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Filter><Prefix>testdir/dir1/</Prefix></Filter><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			prefix:         "testdir/",
			expectedNonRec: false,
			expectedRec:    true,
		},
		// case 5 - has filter with prefix and tags, here we are not matching on tags
		{
			inputConfig: `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Filter>
		<And><Prefix>key-prefix</Prefix><Tag><Key>key1</Key><Value>value1</Value></Tag><Tag><Key>key2</Key><Value>value2</Value></Tag></And></Filter><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			prefix:         "testdir/",
			expectedNonRec: true,
			expectedRec:    true,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test_%d", i+1), func(t *testing.T) {
			cfg, err := ParseConfig(bytes.NewReader([]byte(tc.inputConfig)))
			if err != nil {
				t.Fatalf("Got unexpected error: %v", err)
			}
			if got := cfg.HasActiveRules(tc.prefix, false); got != tc.expectedNonRec {
				t.Fatalf("Expected result with recursive set to false: `%v`, got: `%v`", tc.expectedNonRec, got)
			}
			if got := cfg.HasActiveRules(tc.prefix, true); got != tc.expectedRec {
				t.Fatalf("Expected result with recursive set to true: `%v`, got: `%v`", tc.expectedRec, got)
			}
		})
	}
}

func TestFilterActionableRules(t *testing.T) {
	testCases := []struct {
		inputConfig   string
		prefix        string
		ExpectedRules []Rule
	}{
		// case 1 - only one rule
		{
			inputConfig:   `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>prefix</Prefix><Priority>1</Priority><Destination><Bucket>arn:minio:replication:xxx::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			prefix:        "prefix",
			ExpectedRules: []Rule{{Status: Enabled, Priority: 1, DeleteMarkerReplication: DeleteMarkerReplication{Status: Enabled}, DeleteReplication: DeleteReplication{Status: Disabled}, Destination: Destination{Bucket: "destinationbucket", ARN: "arn:minio:replication:xxx::destinationbucket"}}},
		},
		// case 2 - multiple rules for same target, overlapping rules with different priority
		{
			inputConfig: `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>prefix</Prefix><Priority>3</Priority><Destination><Bucket>arn:minio:replication:xxx::destinationbucket</Bucket></Destination></Rule><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>prefix</Prefix><Priority>1</Priority><Destination><Bucket>arn:minio:replication:xxx::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			prefix:      "prefix",
			ExpectedRules: []Rule{
				{Status: Enabled, Priority: 3, DeleteMarkerReplication: DeleteMarkerReplication{Status: Enabled}, DeleteReplication: DeleteReplication{Status: Disabled}, Destination: Destination{Bucket: "destinationbucket", ARN: "arn:minio:replication:xxx::destinationbucket"}},
				{Status: Enabled, Priority: 1, DeleteMarkerReplication: DeleteMarkerReplication{Status: Enabled}, DeleteReplication: DeleteReplication{Status: Disabled}, Destination: Destination{Bucket: "destinationbucket", ARN: "arn:minio:replication:xxx::destinationbucket"}},
			},
		},
		// case 3 - multiple rules for different target, overlapping rules on a target
		{
			inputConfig: `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>prefix</Prefix><Priority>2</Priority><Destination><Bucket>arn:minio:replication:xxx::destinationbucket2</Bucket></Destination></Rule><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>prefix</Prefix><Priority>4</Priority><Destination><Bucket>arn:minio:replication:xxx::destinationbucket2</Bucket></Destination></Rule><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>prefix</Prefix><Priority>3</Priority><Destination><Bucket>arn:minio:replication:xxx::destinationbucket</Bucket></Destination></Rule><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>prefix</Prefix><Priority>1</Priority><Destination><Bucket>arn:minio:replication:xxx::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>`,
			prefix:      "prefix",
			ExpectedRules: []Rule{
				{Status: Enabled, Priority: 4, DeleteMarkerReplication: DeleteMarkerReplication{Status: Enabled}, DeleteReplication: DeleteReplication{Status: Disabled}, Destination: Destination{Bucket: "destinationbucket2", ARN: "arn:minio:replication:xxx::destinationbucket2"}},
				{Status: Enabled, Priority: 2, DeleteMarkerReplication: DeleteMarkerReplication{Status: Enabled}, DeleteReplication: DeleteReplication{Status: Disabled}, Destination: Destination{Bucket: "destinationbucket2", ARN: "arn:minio:replication:xxx::destinationbucket2"}},
				{Status: Enabled, Priority: 3, DeleteMarkerReplication: DeleteMarkerReplication{Status: Enabled}, DeleteReplication: DeleteReplication{Status: Disabled}, Destination: Destination{Bucket: "destinationbucket", ARN: "arn:minio:replication:xxx::destinationbucket"}},
				{Status: Enabled, Priority: 1, DeleteMarkerReplication: DeleteMarkerReplication{Status: Enabled}, DeleteReplication: DeleteReplication{Status: Disabled}, Destination: Destination{Bucket: "destinationbucket", ARN: "arn:minio:replication:xxx::destinationbucket"}},
			},
		},
	}
	for _, tc := range testCases {
		cfg, err := ParseConfig(bytes.NewReader([]byte(tc.inputConfig)))
		if err != nil {
			t.Fatalf("Got unexpected error: %v", err)
		}
		got := cfg.FilterActionableRules(ObjectOpts{Name: tc.prefix})
		if len(got) != len(tc.ExpectedRules) {
			t.Fatalf("Expected matching number of actionable rules: `%v`, got: `%v`", tc.ExpectedRules, got)
		}
		for i := range got {
			if got[i].Destination.ARN != tc.ExpectedRules[i].Destination.ARN || got[i].Priority != tc.ExpectedRules[i].Priority {
				t.Fatalf("Expected order of filtered rules to be identical: `%v`, got: `%v`", tc.ExpectedRules, got)
			}
		}
	}
}
