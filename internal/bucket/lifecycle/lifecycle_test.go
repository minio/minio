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

package lifecycle

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/minio/minio/internal/bucket/object/lock"
	xhttp "github.com/minio/minio/internal/http"
)

func TestParseAndValidateLifecycleConfig(t *testing.T) {
	testCases := []struct {
		inputConfig           string
		expectedParsingErr    error
		expectedValidationErr error
		lr                    lock.Retention
	}{
		{ // Valid lifecycle config
			inputConfig: `<LifecycleConfiguration>
								  <Rule>
								  <ID>testRule1</ID>
		                          <Filter>
		                             <Prefix>prefix</Prefix>
		                          </Filter>
		                          <Status>Enabled</Status>
		                          <Expiration><Days>3</Days></Expiration>
		                          </Rule>
		                              <Rule>
								  <ID>testRule2</ID>
		                          <Filter>
		                             <Prefix>another-prefix</Prefix>
		                          </Filter>
		                          <Status>Enabled</Status>
		                          <Expiration><Days>3</Days></Expiration>
		                          </Rule>
		                          </LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: nil,
		},
		{ // Using ExpiredObjectAllVersions element with an object locked bucket
			inputConfig: `<LifecycleConfiguration>
                                        <Rule>
                                          <ID>ExpiredObjectAllVersions with object locking</ID>
		                          <Filter>
		                             <Prefix>prefix</Prefix>
		                          </Filter>
		                          <Status>Enabled</Status>
		                          <Expiration>
			                    <Days>3</Days>
				            <ExpiredObjectAllVersions>true</ExpiredObjectAllVersions>
			                  </Expiration>
		                        </Rule>
		                      </LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: errLifecycleBucketLocked,
			lr: lock.Retention{
				LockEnabled: true,
			},
		},
		{ // Using DelMarkerExpiration action with an object locked bucket
			inputConfig: `<LifecycleConfiguration>
                                        <Rule>
                                          <ID>DeleteMarkerExpiration with object locking</ID>
		                          <Filter>
		                             <Prefix>prefix</Prefix>
		                          </Filter>
		                          <Status>Enabled</Status>
		                          <DelMarkerExpiration>
			                    <Days>3</Days>
			                  </DelMarkerExpiration>
		                        </Rule>
		                      </LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: errLifecycleBucketLocked,
			lr: lock.Retention{
				LockEnabled: true,
			},
		},
		{ // lifecycle config with no rules
			inputConfig: `<LifecycleConfiguration>
		                          </LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: errLifecycleNoRule,
		},
		{ // Valid lifecycle config
			inputConfig: `<LifecycleConfiguration>
					  <Rule>
					  <Filter>
					  <And><Tag><Key>key1</Key><Value>val1</Value><Key>key2</Key><Value>val2</Value></Tag></And>
		                          </Filter>
		                          <Expiration><Days>3</Days></Expiration>
		                          </Rule>
		                          </LifecycleConfiguration>`,
			expectedParsingErr:    errDuplicatedXMLTag,
			expectedValidationErr: nil,
		},
		{ // lifecycle config without prefixes
			inputConfig: `<LifecycleConfiguration>
					  <Rule>
		                          <Expiration><Days>3</Days></Expiration>
                                          <Status>Enabled</Status>
		                          </Rule>
		                          </LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: nil,
		},
		{ // lifecycle config with rules having overlapping prefix
			inputConfig:           `<LifecycleConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><Filter><Prefix>/a/b</Prefix></Filter><Expiration><Days>3</Days></Expiration></Rule><Rule><ID>rule2</ID><Status>Enabled</Status><Filter><And><Prefix>/a/b/c</Prefix><Tag><Key>key1</Key><Value>val1</Value></Tag></And></Filter><Expiration><Days>3</Days></Expiration></Rule></LifecycleConfiguration> `,
			expectedParsingErr:    nil,
			expectedValidationErr: nil,
		},
		{ // lifecycle config with rules having duplicate ID
			inputConfig:           `<LifecycleConfiguration><Rule><ID>duplicateID</ID><Status>Enabled</Status><Filter><Prefix>/a/b</Prefix></Filter><Expiration><Days>3</Days></Expiration></Rule><Rule><ID>duplicateID</ID><Status>Enabled</Status><Filter><And><Prefix>/x/z</Prefix><Tag><Key>key1</Key><Value>val1</Value></Tag></And></Filter><Expiration><Days>4</Days></Expiration></Rule></LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: errLifecycleDuplicateID,
		},
		// Missing <Tag> in <And>
		{
			inputConfig:           `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>sample-rule-2</ID><Filter><And><Prefix>/a/b/c</Prefix></And></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: errXMLNotWellFormed,
		},
		// Lifecycle with the deprecated Prefix tag
		{
			inputConfig:           `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Prefix /><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: nil,
		},
		// Lifecycle with empty Filter tag
		{
			inputConfig:           `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Filter></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: nil,
		},
		// Lifecycle with zero Transition Days
		{
			inputConfig:           `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Filter></Filter><Status>Enabled</Status><Transition><Days>0</Days><StorageClass>S3TIER-1</StorageClass></Transition></Rule></LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: nil,
		},
		// Lifecycle with max noncurrent versions
		{
			inputConfig:           `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Status>Enabled</Status><Filter></Filter><NoncurrentVersionExpiration><NewerNoncurrentVersions>5</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: nil,
		},
		// Lifecycle with delmarker expiration
		{
			inputConfig:           `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Status>Enabled</Status><Filter></Filter><DelMarkerExpiration><Days>5</Days></DelMarkerExpiration></Rule></LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: nil,
		},
		// Lifecycle with empty delmarker expiration
		{
			inputConfig:           `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Status>Enabled</Status><Filter></Filter><DelMarkerExpiration><Days></Days></DelMarkerExpiration></Rule></LifecycleConfiguration>`,
			expectedParsingErr:    errInvalidDaysDelMarkerExpiration,
			expectedValidationErr: nil,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			lc, err := ParseLifecycleConfig(bytes.NewReader([]byte(tc.inputConfig)))
			if err != tc.expectedParsingErr {
				t.Fatalf("%d: Expected %v during parsing but got %v", i+1, tc.expectedParsingErr, err)
			}
			if tc.expectedParsingErr != nil {
				// We already expect a parsing error,
				// no need to continue this test.
				return
			}
			err = lc.Validate(tc.lr)
			if err != tc.expectedValidationErr {
				t.Fatalf("%d: Expected %v during validation but got %v", i+1, tc.expectedValidationErr, err)
			}
		})
	}
}

// TestMarshalLifecycleConfig checks if lifecycleconfig xml
// marshaling/unmarshaling can handle output from each other
func TestMarshalLifecycleConfig(t *testing.T) {
	// Time at midnight UTC
	midnightTS := ExpirationDate{time.Date(2019, time.April, 20, 0, 0, 0, 0, time.UTC)}
	lc := Lifecycle{
		Rules: []Rule{
			{
				Status:     "Enabled",
				Filter:     Filter{Prefix: Prefix{string: "prefix-1", set: true}},
				Expiration: Expiration{Days: ExpirationDays(3)},
			},
			{
				Status:     "Enabled",
				Filter:     Filter{Prefix: Prefix{string: "prefix-1", set: true}},
				Expiration: Expiration{Date: midnightTS},
			},
			{
				Status:                      "Enabled",
				Filter:                      Filter{Prefix: Prefix{string: "prefix-1", set: true}},
				Expiration:                  Expiration{Date: midnightTS},
				NoncurrentVersionTransition: NoncurrentVersionTransition{NoncurrentDays: TransitionDays(2), StorageClass: "TEST"},
			},
		},
	}
	b, err := xml.MarshalIndent(&lc, "", "\t")
	if err != nil {
		t.Fatal(err)
	}
	var lc1 Lifecycle
	err = xml.Unmarshal(b, &lc1)
	if err != nil {
		t.Fatal(err)
	}

	ruleSet := make(map[string]struct{})
	for _, rule := range lc.Rules {
		ruleBytes, err := xml.Marshal(rule)
		if err != nil {
			t.Fatal(err)
		}
		ruleSet[string(ruleBytes)] = struct{}{}
	}
	for _, rule := range lc1.Rules {
		ruleBytes, err := xml.Marshal(rule)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := ruleSet[string(ruleBytes)]; !ok {
			t.Fatalf("Expected %v to be equal to %v, %v missing", lc, lc1, rule)
		}
	}
}

func TestExpectedExpiryTime(t *testing.T) {
	testCases := []struct {
		modTime  time.Time
		days     ExpirationDays
		expected time.Time
	}{
		{
			time.Date(2020, time.March, 15, 10, 10, 10, 0, time.UTC),
			4,
			time.Date(2020, time.March, 20, 0, 0, 0, 0, time.UTC),
		},
		{
			time.Date(2020, time.March, 15, 0, 0, 0, 0, time.UTC),
			1,
			time.Date(2020, time.March, 17, 0, 0, 0, 0, time.UTC),
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			got := ExpectedExpiryTime(tc.modTime, int(tc.days))
			if !got.Equal(tc.expected) {
				t.Fatalf("Expected %v to be equal to %v", got, tc.expected)
			}
		})
	}
}

func TestEval(t *testing.T) {
	testCases := []struct {
		inputConfig            string
		objectName             string
		objectTags             string
		objectModTime          time.Time
		isDelMarker            bool
		hasManyVersions        bool
		expectedAction         Action
		isNoncurrent           bool
		objectSuccessorModTime time.Time
		versionID              string
	}{
		// Empty object name (unexpected case) should always return NoneAction
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>prefix</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			expectedAction: NoneAction,
		},
		// Disabled should always return NoneAction
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Disabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().UTC().Add(-10 * 24 * time.Hour), // Created 10 days ago
			expectedAction: NoneAction,
		},
		// No modTime, should be none-action
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			expectedAction: NoneAction,
		},
		// Prefix not matched
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foxdir/fooobject",
			objectModTime:  time.Now().UTC().Add(-10 * 24 * time.Hour), // Created 10 days ago
			expectedAction: NoneAction,
		},
		// Test rule with empty prefix e.g. for whole bucket
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foxdir/fooobject/foo.txt",
			objectModTime:  time.Now().UTC().Add(-10 * 24 * time.Hour), // Created 10 days ago
			expectedAction: DeleteAction,
		},
		// Too early to remove (test Days)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foxdir/fooobject",
			objectModTime:  time.Now().UTC().Add(-10 * 24 * time.Hour), // Created 10 days ago
			expectedAction: NoneAction,
		},
		// Should remove (test Days)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().UTC().Add(-6 * 24 * time.Hour), // Created 6 days ago
			expectedAction: DeleteAction,
		},
		// Too early to remove (test Date)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().UTC().Truncate(24*time.Hour).Add(24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().UTC().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: NoneAction,
		},
		// Should remove (test Days)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().UTC().Truncate(24*time.Hour).Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().UTC().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// Should remove (Tags match)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix>foodir/</Prefix><Tag><Key>tag1</Key><Value>value1</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().UTC().Truncate(24*time.Hour).Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectTags:     "tag1=value1&tag2=value2",
			objectModTime:  time.Now().UTC().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// Should remove (Multiple Rules, Tags match)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix>foodir/</Prefix><Tag><Key>tag1</Key><Value>value1</Value></Tag><Tag><Key>tag2</Key><Value>value2</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).UTC().Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule><Rule><Filter><And><Prefix>abc/</Prefix><Tag><Key>tag2</Key><Value>value</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).UTC().Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectTags:     "tag1=value1&tag2=value2",
			objectModTime:  time.Now().UTC().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// Should remove (Tags match)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix>foodir/</Prefix><Tag><Key>tag1</Key><Value>value1</Value></Tag><Tag><Key>tag2</Key><Value>value2</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).UTC().Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectTags:     "tag1=value1&tag2=value2",
			objectModTime:  time.Now().UTC().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// Should remove (Tags match with inverted order)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Tag><Key>factory</Key><Value>true</Value></Tag><Tag><Key>storeforever</Key><Value>false</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).UTC().Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "fooobject",
			objectTags:     "storeforever=false&factory=true",
			objectModTime:  time.Now().UTC().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// Should remove (Tags with encoded chars)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Tag><Key>factory</Key><Value>true</Value></Tag><Tag><Key>store forever</Key><Value>false</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).UTC().Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "fooobject",
			objectTags:     "store+forever=false&factory=true",
			objectModTime:  time.Now().UTC().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},

		// Should not remove (Tags don't match)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix>foodir/</Prefix><Tag><Key>tag</Key><Value>value1</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().UTC().Truncate(24*time.Hour).Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectTags:     "tag1=value1",
			objectModTime:  time.Now().UTC().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: NoneAction,
		},
		// Should not remove (Tags match, but prefix doesn't match)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix>foodir/</Prefix><Tag><Key>tag1</Key><Value>value1</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).UTC().Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foxdir/fooobject",
			objectTags:     "tag1=value1",
			objectModTime:  time.Now().UTC().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: NoneAction,
		},
		// Should remove - empty prefix, tags match, date expiration kicked in
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Tag><Key>tag1</Key><Value>value1</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).UTC().Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foxdir/fooobject",
			objectTags:     "tag1=value1",
			objectModTime:  time.Now().UTC().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// Should remove - empty prefix, tags match, object is expired based on specified Days
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix></Prefix><Tag><Key>tag1</Key><Value>value1</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foxdir/fooobject",
			objectTags:     "tag1=value1",
			objectModTime:  time.Now().UTC().Add(-48 * time.Hour), // Created 2 day ago
			expectedAction: DeleteAction,
		},
		// Should remove, the second rule has expiration kicked in
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).UTC().Add(24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule><Rule><Filter><Prefix>foxdir/</Prefix></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).UTC().Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foxdir/fooobject",
			objectModTime:  time.Now().UTC().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// Should accept BucketLifecycleConfiguration root tag
		{
			inputConfig:    `<BucketLifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).UTC().Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></BucketLifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().UTC().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// Should delete expired delete marker right away
		{
			inputConfig:    `<BucketLifecycleConfiguration><Rule><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration><Filter></Filter><Status>Enabled</Status></Rule></BucketLifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().UTC().Add(-1 * time.Hour), // Created one hour ago
			isDelMarker:    true,
			expectedAction: DeleteVersionAction,
		},
		// Should not expire a delete marker; ExpiredObjectDeleteAllVersions applies only when current version is not a DEL marker.
		{
			inputConfig:     `<BucketLifecycleConfiguration><Rule><Expiration><Days>1</Days><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration><Filter></Filter><Status>Enabled</Status></Rule></BucketLifecycleConfiguration>`,
			objectName:      "foodir/fooobject",
			objectModTime:   time.Now().UTC().Add(-10 * 24 * time.Hour), // Created 10 days ago
			isDelMarker:     true,
			hasManyVersions: true,
			expectedAction:  NoneAction,
		},
		// Should delete all versions of this object since the latest version has past the expiry days criteria
		{
			inputConfig:     `<BucketLifecycleConfiguration><Rule><Expiration><Days>1</Days><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration><Filter></Filter><Status>Enabled</Status></Rule></BucketLifecycleConfiguration>`,
			objectName:      "foodir/fooobject",
			objectModTime:   time.Now().UTC().Add(-10 * 24 * time.Hour), // Created 10 days ago
			hasManyVersions: true,
			expectedAction:  DeleteAllVersionsAction,
		},
		// TransitionAction applies since object doesn't meet the age criteria for DeleteAllVersions
		{
			inputConfig:     `<BucketLifecycleConfiguration><Rule><Expiration><Days>30</Days><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration><Transition><Days>10</Days><StorageClass>WARM-1</StorageClass></Transition><Filter></Filter><Status>Enabled</Status></Rule></BucketLifecycleConfiguration>`,
			objectName:      "foodir/fooobject",
			objectModTime:   time.Now().UTC().Add(-11 * 24 * time.Hour), // Created 11 days ago
			hasManyVersions: true,
			expectedAction:  TransitionAction,
		},
		// Should not delete expired marker if its time has not come yet
		{
			inputConfig:    `<BucketLifecycleConfiguration><Rule><Filter></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></BucketLifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().UTC().Add(-12 * time.Hour), // Created 12 hours ago
			isDelMarker:    true,
			expectedAction: NoneAction,
		},
		// Should delete expired marker since its time has come
		{
			inputConfig:    `<BucketLifecycleConfiguration><Rule><Filter></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></BucketLifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().UTC().Add(-10 * 24 * time.Hour), // Created 10 days ago
			isDelMarker:    true,
			expectedAction: DeleteVersionAction,
		},
		// Should transition immediately when Transition days is zero
		{
			inputConfig:    `<BucketLifecycleConfiguration><Rule><Filter></Filter><Status>Enabled</Status><Transition><Days>0</Days><StorageClass>S3TIER-1</StorageClass></Transition></Rule></BucketLifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().Add(-1 * time.Nanosecond).UTC(), // Created now
			expectedAction: TransitionAction,
		},
		// Should transition immediately when NoncurrentVersion Transition days is zero
		{
			inputConfig:            `<BucketLifecycleConfiguration><Rule><Filter></Filter><Status>Enabled</Status><NoncurrentVersionTransition><NoncurrentDays>0</NoncurrentDays><StorageClass>S3TIER-1</StorageClass></NoncurrentVersionTransition></Rule></BucketLifecycleConfiguration>`,
			objectName:             "foodir/fooobject",
			objectModTime:          time.Now().Add(-1 * time.Nanosecond).UTC(), // Created now
			expectedAction:         TransitionVersionAction,
			isNoncurrent:           true,
			objectSuccessorModTime: time.Now().Add(-1 * time.Nanosecond).UTC(),
			versionID:              uuid.New().String(),
		},
		// Lifecycle rules with NewerNoncurrentVersions specified must return NoneAction.
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><NoncurrentVersionExpiration><NewerNoncurrentVersions>5</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			versionID:      uuid.NewString(),
			objectModTime:  time.Now().UTC().Add(-10 * 24 * time.Hour), // Created 10 days ago
			expectedAction: NoneAction,
		},
		// Disabled rules with NewerNoncurrentVersions shouldn't affect outcome.
		{
			inputConfig:            `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>5</NoncurrentDays></NoncurrentVersionExpiration></Rule><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Disabled</Status><NoncurrentVersionExpiration><NewerNoncurrentVersions>5</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>`,
			objectName:             "foodir/fooobject",
			versionID:              uuid.NewString(),
			objectModTime:          time.Now().UTC().Add(-10 * 24 * time.Hour), // Created 10 days ago
			objectSuccessorModTime: time.Now().UTC().Add(-10 * 24 * time.Hour), // Created 10 days ago
			isNoncurrent:           true,
			expectedAction:         DeleteVersionAction,
		},
		{
			inputConfig: `<LifecycleConfiguration>
                             <Rule>
                               <ID>Rule 1</ID>
                               <Filter>
                               </Filter>
                               <Status>Enabled</Status>
                               <Expiration>
                                 <Days>365</Days>
                               </Expiration>
                             </Rule>
                             <Rule>
                               <ID>Rule 2</ID>
                               <Filter>
                                 <Prefix>logs/</Prefix>
                               </Filter>
                               <Status>Enabled</Status>
                               <Transition>
                                 <StorageClass>STANDARD_IA</StorageClass>
                                 <Days>30</Days>
                               </Transition>
                              </Rule>
                          </LifecycleConfiguration>`,
			objectName:     "logs/obj-1",
			objectModTime:  time.Now().UTC().Add(-31 * 24 * time.Hour),
			expectedAction: TransitionAction,
		},
		{
			inputConfig: `<LifecycleConfiguration>
                             <Rule>
                               <ID>Rule 1</ID>
                               <Filter>
                                 <Prefix>logs/</Prefix>
                               </Filter>
                               <Status>Enabled</Status>
                               <Expiration>
                                 <Days>365</Days>
                               </Expiration>
                             </Rule>
                             <Rule>
                               <ID>Rule 2</ID>
                               <Filter>
                                 <Prefix>logs/</Prefix>
                               </Filter>
                               <Status>Enabled</Status>
                               <Transition>
                                 <StorageClass>STANDARD_IA</StorageClass>
                                 <Days>365</Days>
                               </Transition>
                             </Rule>
                          </LifecycleConfiguration>`,
			objectName:     "logs/obj-1",
			objectModTime:  time.Now().UTC().Add(-366 * 24 * time.Hour),
			expectedAction: DeleteAction,
		},
		{
			inputConfig: `<LifecycleConfiguration>
                            <Rule>
                              <ID>Rule 1</ID>
                              <Filter>
                                <Tag>
                                   <Key>tag1</Key>
                                   <Value>value1</Value>
                                </Tag>
                              </Filter>
                              <Status>Enabled</Status>
                              <Transition>
                                <StorageClass>GLACIER</StorageClass>
                                <Days>365</Days>
                              </Transition>
                            </Rule>
                            <Rule>
                              <ID>Rule 2</ID>
                              <Filter>
                                <Tag>
                                   <Key>tag2</Key>
                                   <Value>value2</Value>
                                </Tag>
                              </Filter>
                              <Status>Enabled</Status>
                              <Expiration>
                                <Days>14</Days>
                              </Expiration>
                             </Rule>
                         </LifecycleConfiguration>`,
			objectName:     "obj-1",
			objectTags:     "tag1=value1&tag2=value2",
			objectModTime:  time.Now().UTC().Add(-15 * 24 * time.Hour),
			expectedAction: DeleteAction,
		},
		{
			inputConfig: `<LifecycleConfiguration>
                            <Rule>
                              <ID>Rule 1</ID>
                              <Status>Enabled</Status>
                              <Filter></Filter>
                              <Transition>
                                <StorageClass>WARM-1</StorageClass>
                                <Days>30</Days>
                              </Transition>
                              <Expiration>
                                <Days>60</Days>
                              </Expiration>
                            </Rule>
                       </LifecycleConfiguration>`,
			objectName:     "obj-1",
			objectModTime:  time.Now().UTC().Add(-90 * 24 * time.Hour),
			expectedAction: DeleteAction,
		},
		{
			inputConfig: `<LifecycleConfiguration>
                            <Rule>
                              <ID>Rule 2</ID>
                              <Filter></Filter>
                              <Status>Enabled</Status>
                              <NoncurrentVersionExpiration>
                                <NoncurrentDays>60</NoncurrentDays>
                              </NoncurrentVersionExpiration>
	                      <NoncurrentVersionTransition>
                                <StorageClass>WARM-1</StorageClass>
                                <NoncurrentDays>30</NoncurrentDays>
                              </NoncurrentVersionTransition>
                             </Rule>
                       </LifecycleConfiguration>`,
			objectName:             "obj-1",
			isNoncurrent:           true,
			objectModTime:          time.Now().UTC().Add(-90 * 24 * time.Hour),
			objectSuccessorModTime: time.Now().UTC().Add(-90 * 24 * time.Hour),
			expectedAction:         DeleteVersionAction,
		},
		{
			// DelMarkerExpiration is preferred since object age is past both transition and expiration days.
			inputConfig: `<LifecycleConfiguration>
                            <Rule>
                              <ID>DelMarkerExpiration with Transition</ID>
                              <Filter></Filter>
                              <Status>Enabled</Status>
                              <DelMarkerExpiration>
                                <Days>60</Days>
                              </DelMarkerExpiration>
	                      <Transition>
                                <StorageClass>WARM-1</StorageClass>
                                <Days>30</Days>
                              </Transition>
                             </Rule>
                       </LifecycleConfiguration>`,
			objectName:     "obj-1",
			objectModTime:  time.Now().UTC().Add(-90 * 24 * time.Hour),
			isDelMarker:    true,
			expectedAction: DelMarkerDeleteAllVersionsAction,
		},
		{
			// NoneAction since object doesn't qualify for DelMarkerExpiration yet.
			// Note: TransitionAction doesn't apply to DEL marker
			inputConfig: `<LifecycleConfiguration>
                            <Rule>
                              <ID>DelMarkerExpiration with Transition</ID>
                              <Filter></Filter>
                              <Status>Enabled</Status>
                              <DelMarkerExpiration>
                                <Days>60</Days>
                              </DelMarkerExpiration>
	                      <Transition>
                                <StorageClass>WARM-1</StorageClass>
                                <Days>30</Days>
                              </Transition>
                             </Rule>
                       </LifecycleConfiguration>`,
			objectName:     "obj-1",
			objectModTime:  time.Now().UTC().Add(-50 * 24 * time.Hour),
			isDelMarker:    true,
			expectedAction: NoneAction,
		},
		{
			inputConfig: `<LifecycleConfiguration>
                            <Rule>
                              <ID>DelMarkerExpiration with non DEL-marker object</ID>
                              <Filter></Filter>
                              <Status>Enabled</Status>
                              <DelMarkerExpiration>
                                <Days>60</Days>
                              </DelMarkerExpiration>
                             </Rule>
                       </LifecycleConfiguration>`,
			objectName:     "obj-1",
			objectModTime:  time.Now().UTC().Add(-90 * 24 * time.Hour),
			expectedAction: NoneAction,
		},
		{
			inputConfig: `<LifecycleConfiguration>
                            <Rule>
                              <ID>DelMarkerExpiration with noncurrent DEL-marker</ID>
                              <Filter></Filter>
                              <Status>Enabled</Status>
                              <DelMarkerExpiration>
                                <Days>60</Days>
                              </DelMarkerExpiration>
                             </Rule>
                       </LifecycleConfiguration>`,
			objectName:             "obj-1",
			objectModTime:          time.Now().UTC().Add(-90 * 24 * time.Hour),
			objectSuccessorModTime: time.Now().UTC().Add(-60 * 24 * time.Hour),
			isDelMarker:            true,
			isNoncurrent:           true,
			expectedAction:         NoneAction,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			lc, err := ParseLifecycleConfig(bytes.NewReader([]byte(tc.inputConfig)))
			if err != nil {
				t.Fatalf("Got unexpected error: %v", err)
			}
			opts := ObjectOpts{
				Name:             tc.objectName,
				UserTags:         tc.objectTags,
				ModTime:          tc.objectModTime,
				DeleteMarker:     tc.isDelMarker,
				IsLatest:         !tc.isNoncurrent,
				SuccessorModTime: tc.objectSuccessorModTime,
				VersionID:        tc.versionID,
			}
			opts.NumVersions = 1
			if tc.hasManyVersions {
				opts.NumVersions = 2 // at least one noncurrent version
			}
			if res := lc.Eval(opts); res.Action != tc.expectedAction {
				t.Fatalf("Expected action: `%v`, got: `%v`", tc.expectedAction, res.Action)
			}
		})
	}
}

func TestHasActiveRules(t *testing.T) {
	testCases := []struct {
		inputConfig string
		prefix      string
		want        bool
	}{
		{
			inputConfig: `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			prefix:      "foodir/foobject",
			want:        true,
		},
		{ // empty prefix
			inputConfig: `<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			prefix:      "foodir/foobject/foo.txt",
			want:        true,
		},
		{
			inputConfig: `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			prefix:      "zdir/foobject",
			want:        false,
		},
		{
			inputConfig: `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/zdir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			prefix:      "foodir/",
			want:        true,
		},
		{
			inputConfig: `<LifecycleConfiguration><Rule><Filter><Prefix></Prefix></Filter><Status>Disabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			prefix:      "foodir/",
			want:        false,
		},
		{
			inputConfig: `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Date>2999-01-01T00:00:00.000Z</Date></Expiration></Rule></LifecycleConfiguration>`,
			prefix:      "foodir/foobject",
			want:        false,
		},
		{
			inputConfig: `<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><Transition><StorageClass>S3TIER-1</StorageClass></Transition></Rule></LifecycleConfiguration>`,
			prefix:      "foodir/foobject/foo.txt",
			want:        true,
		},
		{
			inputConfig: `<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><NoncurrentVersionTransition><StorageClass>S3TIER-1</StorageClass></NoncurrentVersionTransition></Rule></LifecycleConfiguration>`,
			prefix:      "foodir/foobject/foo.txt",
			want:        true,
		},
		{
			inputConfig: `<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule></LifecycleConfiguration>`,
			prefix:      "",
			want:        true,
		},
		{
			inputConfig: `<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><Expiration><Days>42</Days><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration></Rule></LifecycleConfiguration>`,
			prefix:      "",
			want:        true,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test_%d", i+1), func(t *testing.T) {
			lc, err := ParseLifecycleConfig(bytes.NewReader([]byte(tc.inputConfig)))
			if err != nil {
				t.Fatalf("Got unexpected error: %v", err)
			}
			// To ensure input lifecycle configurations are valid
			if err := lc.Validate(lock.Retention{}); err != nil {
				t.Fatalf("Invalid test case: %d %v", i+1, err)
			}
			if got := lc.HasActiveRules(tc.prefix); got != tc.want {
				t.Fatalf("Expected result: `%v`, got: `%v`", tc.want, got)
			}
		})
	}
}

func TestSetPredictionHeaders(t *testing.T) {
	lc := Lifecycle{
		Rules: []Rule{
			{
				ID:     "rule-1",
				Status: "Enabled",
				Expiration: Expiration{
					Days: ExpirationDays(3),
					set:  true,
				},
			},
			{
				ID:     "rule-2",
				Status: "Enabled",
				Transition: Transition{
					Days:         TransitionDays(3),
					StorageClass: "TIER-1",
					set:          true,
				},
			},
			{
				ID:     "rule-3",
				Status: "Enabled",
				NoncurrentVersionTransition: NoncurrentVersionTransition{
					NoncurrentDays: TransitionDays(5),
					StorageClass:   "TIER-2",
					set:            true,
				},
			},
		},
	}

	// current version
	obj1 := ObjectOpts{
		Name:     "obj1",
		IsLatest: true,
	}
	// non-current version
	obj2 := ObjectOpts{
		Name: "obj2",
	}

	tests := []struct {
		obj         ObjectOpts
		expRuleID   int
		transRuleID int
	}{
		{
			obj:         obj1,
			expRuleID:   0,
			transRuleID: 1,
		},
		{
			obj:         obj2,
			expRuleID:   0,
			transRuleID: 2,
		},
	}
	for i, tc := range tests {
		w := httptest.NewRecorder()
		lc.SetPredictionHeaders(w, tc.obj)
		if expHdrs, ok := w.Header()[xhttp.AmzExpiration]; ok && !strings.Contains(expHdrs[0], lc.Rules[tc.expRuleID].ID) {
			t.Fatalf("Test %d: Expected %s header", i+1, xhttp.AmzExpiration)
		}
		if transHdrs, ok := w.Header()[xhttp.MinIOTransition]; ok {
			if !strings.Contains(transHdrs[0], lc.Rules[tc.transRuleID].ID) {
				t.Fatalf("Test %d: Expected %s header", i+1, xhttp.MinIOTransition)
			}

			if tc.obj.IsLatest {
				if expectedDue, _ := lc.Rules[tc.transRuleID].Transition.NextDue(tc.obj); !strings.Contains(transHdrs[0], expectedDue.Format(http.TimeFormat)) {
					t.Fatalf("Test %d: Expected transition time %s", i+1, expectedDue)
				}
			} else {
				if expectedDue, _ := lc.Rules[tc.transRuleID].NoncurrentVersionTransition.NextDue(tc.obj); !strings.Contains(transHdrs[0], expectedDue.Format(http.TimeFormat)) {
					t.Fatalf("Test %d: Expected transition time %s", i+1, expectedDue)
				}
			}
		}
	}
}

func TestTransitionTier(t *testing.T) {
	lc := Lifecycle{
		Rules: []Rule{
			{
				ID:     "rule-1",
				Status: "Enabled",
				Transition: Transition{
					Days:         TransitionDays(3),
					StorageClass: "TIER-1",
				},
			},
			{
				ID:     "rule-2",
				Status: "Enabled",
				NoncurrentVersionTransition: NoncurrentVersionTransition{
					NoncurrentDays: TransitionDays(3),
					StorageClass:   "TIER-2",
				},
			},
		},
	}

	now := time.Now().UTC()

	obj1 := ObjectOpts{
		Name:     "obj1",
		IsLatest: true,
		ModTime:  now,
	}

	obj2 := ObjectOpts{
		Name:    "obj2",
		ModTime: now,
	}

	// Go back seven days in the past
	now = now.Add(7 * 24 * time.Hour)

	evaluator := NewEvaluator(lc)
	evts := evaluator.eval([]ObjectOpts{obj1, obj2}, now)
	evt := evts[0]
	if evt.Action != TransitionAction {
		t.Fatalf("Expected action: %s but got %s", TransitionAction, evt.Action)
	}
	if evt.StorageClass != "TIER-1" {
		t.Fatalf("Expected TIER-1 but got %s", evt.StorageClass)
	}

	evt = evts[1]
	if evt.Action != TransitionVersionAction {
		t.Fatalf("Expected action: %s but got %s", TransitionVersionAction, evt.Action)
	}
	if evt.StorageClass != "TIER-2" {
		t.Fatalf("Expected TIER-2 but got %s", evt.StorageClass)
	}
}

func TestTransitionTierWithPrefixAndTags(t *testing.T) {
	lc := Lifecycle{
		Rules: []Rule{
			{
				ID:     "rule-1",
				Status: "Enabled",
				Filter: Filter{
					Prefix: Prefix{
						set:    true,
						string: "abcd/",
					},
				},
				Transition: Transition{
					Days:         TransitionDays(3),
					StorageClass: "TIER-1",
				},
			},
			{
				ID:     "rule-2",
				Status: "Enabled",
				Filter: Filter{
					tagSet: true,
					Tag: Tag{
						Key:   "priority",
						Value: "low",
					},
				},
				Transition: Transition{
					Days:         TransitionDays(3),
					StorageClass: "TIER-2",
				},
			},
		},
	}

	now := time.Now().UTC()

	obj1 := ObjectOpts{
		Name:     "obj1",
		IsLatest: true,
		ModTime:  now,
	}

	obj2 := ObjectOpts{
		Name:     "abcd/obj2",
		IsLatest: true,
		ModTime:  now,
	}

	obj3 := ObjectOpts{
		Name:     "obj3",
		IsLatest: true,
		ModTime:  now,
		UserTags: "priority=low",
	}

	// Go back seven days in the past
	now = now.Add(7 * 24 * time.Hour)

	evaluator := NewEvaluator(lc)
	evts := evaluator.eval([]ObjectOpts{obj1, obj2, obj3}, now)
	// Eval object 1
	evt := evts[0]
	if evt.Action != NoneAction {
		t.Fatalf("Expected action: %s but got %s", NoneAction, evt.Action)
	}

	// Eval object 2
	evt = evts[1]
	if evt.Action != TransitionAction {
		t.Fatalf("Expected action: %s but got %s", TransitionAction, evt.Action)
	}
	if evt.StorageClass != "TIER-1" {
		t.Fatalf("Expected TIER-1 but got %s", evt.StorageClass)
	}

	// Eval object 3
	evt = evts[2]
	if evt.Action != TransitionAction {
		t.Fatalf("Expected action: %s but got %s", TransitionAction, evt.Action)
	}
	if evt.StorageClass != "TIER-2" {
		t.Fatalf("Expected TIER-2 but got %s", evt.StorageClass)
	}
}

func TestNoncurrentVersionsLimit(t *testing.T) {
	// test that the lowest max noncurrent versions limit is returned among
	// matching rules
	var rules []Rule
	for i := 1; i <= 10; i++ {
		rules = append(rules, Rule{
			ID:     strconv.Itoa(i),
			Status: "Enabled",
			NoncurrentVersionExpiration: NoncurrentVersionExpiration{
				NewerNoncurrentVersions: i,
				NoncurrentDays:          ExpirationDays(i),
			},
		})
	}
	lc := Lifecycle{
		Rules: rules,
	}
	if event := lc.NoncurrentVersionsExpirationLimit(ObjectOpts{Name: "obj"}); event.RuleID != "1" || event.NoncurrentDays != 1 || event.NewerNoncurrentVersions != 1 {
		t.Fatalf("Expected (ruleID, days, lim) to be (\"1\", 1, 1) but got (%s, %d, %d)", event.RuleID, event.NoncurrentDays, event.NewerNoncurrentVersions)
	}
}

func TestMaxNoncurrentBackwardCompat(t *testing.T) {
	testCases := []struct {
		xml      string
		expected NoncurrentVersionExpiration
	}{
		{
			xml: `<NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays><NewerNoncurrentVersions>3</NewerNoncurrentVersions></NoncurrentVersionExpiration>`,
			expected: NoncurrentVersionExpiration{
				XMLName: xml.Name{
					Local: "NoncurrentVersionExpiration",
				},
				NoncurrentDays:          1,
				NewerNoncurrentVersions: 3,
				set:                     true,
			},
		},
		{
			xml: `<NoncurrentVersionExpiration><NoncurrentDays>2</NoncurrentDays><MaxNoncurrentVersions>4</MaxNoncurrentVersions></NoncurrentVersionExpiration>`,
			expected: NoncurrentVersionExpiration{
				XMLName: xml.Name{
					Local: "NoncurrentVersionExpiration",
				},
				NoncurrentDays:          2,
				NewerNoncurrentVersions: 4,
				set:                     true,
			},
		},
	}
	for i, tc := range testCases {
		var got NoncurrentVersionExpiration
		dec := xml.NewDecoder(strings.NewReader(tc.xml))
		if err := dec.Decode(&got); err != nil || got != tc.expected {
			if err != nil {
				t.Fatalf("%d: Failed to unmarshal xml %v", i+1, err)
			}
			t.Fatalf("%d: Expected %v but got %v", i+1, tc.expected, got)
		}
	}
}

func TestParseLifecycleConfigWithID(t *testing.T) {
	r := bytes.NewReader([]byte(`<LifecycleConfiguration>
								  <Rule>
	                              <ID>rule-1</ID>
		                          <Filter>
		                             <Prefix>prefix</Prefix>
		                          </Filter>
		                          <Status>Enabled</Status>
		                          <Expiration><Days>3</Days></Expiration>
		                          </Rule>
		                          <Rule>
		                          <Filter>
		                             <Prefix>another-prefix</Prefix>
		                          </Filter>
		                          <Status>Enabled</Status>
		                          <Expiration><Days>3</Days></Expiration>
		                          </Rule>
		                          </LifecycleConfiguration>`))
	lc, err := ParseLifecycleConfigWithID(r)
	if err != nil {
		t.Fatalf("Expected parsing to succeed but failed with %v", err)
	}
	for _, rule := range lc.Rules {
		if rule.ID == "" {
			t.Fatalf("Expected all rules to have a unique id assigned %#v", rule)
		}
	}
}

func TestFilterAndSetPredictionHeaders(t *testing.T) {
	lc := Lifecycle{
		Rules: []Rule{
			{
				ID:     "rule-1",
				Status: "Enabled",
				Filter: Filter{
					set: true,
					Prefix: Prefix{
						string: "folder1/folder1/exp_dt=2022-",
						set:    true,
					},
				},
				Expiration: Expiration{
					Days: 1,
					set:  true,
				},
			},
		},
	}
	tests := []struct {
		opts ObjectOpts
		lc   Lifecycle
		want int
	}{
		{
			opts: ObjectOpts{
				Name:        "folder1/folder1/exp_dt=2022-08-01/obj-1",
				ModTime:     time.Now().UTC().Add(-10 * 24 * time.Hour),
				VersionID:   "",
				IsLatest:    true,
				NumVersions: 1,
			},
			want: 1,
			lc:   lc,
		},
		{
			opts: ObjectOpts{
				Name:        "folder1/folder1/exp_dt=9999-01-01/obj-1",
				ModTime:     time.Now().UTC().Add(-10 * 24 * time.Hour),
				VersionID:   "",
				IsLatest:    true,
				NumVersions: 1,
			},
			want: 0,
			lc:   lc,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("test-%d", i+1), func(t *testing.T) {
			if got := tc.lc.FilterRules(tc.opts); len(got) != tc.want {
				t.Fatalf("Expected %d rules to match but got %d", tc.want, len(got))
			}
			w := httptest.NewRecorder()
			tc.lc.SetPredictionHeaders(w, tc.opts)
			expHdr, ok := w.Header()[xhttp.AmzExpiration]
			switch {
			case ok && tc.want == 0:
				t.Fatalf("Expected no rule to match but found x-amz-expiration header set: %v", expHdr)
			case !ok && tc.want > 0:
				t.Fatal("Expected x-amz-expiration header to be set but not found")
			}
		})
	}
}

func TestFilterRules(t *testing.T) {
	rules := []Rule{
		{
			ID:     "rule-1",
			Status: "Enabled",
			Filter: Filter{
				set: true,
				Tag: Tag{
					Key:   "key1",
					Value: "val1",
				},
			},
			Expiration: Expiration{
				set:  true,
				Days: 1,
			},
		},
		{
			ID:     "rule-with-sz-lt",
			Status: "Enabled",
			Filter: Filter{
				set:                true,
				ObjectSizeLessThan: 100 * humanize.MiByte,
			},
			Expiration: Expiration{
				set:  true,
				Days: 1,
			},
		},
		{
			ID:     "rule-with-sz-gt",
			Status: "Enabled",
			Filter: Filter{
				set:                   true,
				ObjectSizeGreaterThan: 1 * humanize.MiByte,
			},
			Expiration: Expiration{
				set:  true,
				Days: 1,
			},
		},
		{
			ID:     "rule-with-sz-lt-and-tag",
			Status: "Enabled",
			Filter: Filter{
				set: true,
				And: And{
					ObjectSizeLessThan: 100 * humanize.MiByte,
					Tags: []Tag{
						{
							Key:   "key1",
							Value: "val1",
						},
					},
				},
			},
			Expiration: Expiration{
				set:  true,
				Days: 1,
			},
		},
		{
			ID:     "rule-with-sz-gt-and-tag",
			Status: "Enabled",
			Filter: Filter{
				set: true,
				And: And{
					ObjectSizeGreaterThan: 1 * humanize.MiByte,
					Tags: []Tag{
						{
							Key:   "key1",
							Value: "val1",
						},
					},
				},
			},
			Expiration: Expiration{
				set:  true,
				Days: 1,
			},
		},
		{
			ID:     "rule-with-sz-lt-and-gt",
			Status: "Enabled",
			Filter: Filter{
				set: true,
				And: And{
					ObjectSizeGreaterThan: 101 * humanize.MiByte,
					ObjectSizeLessThan:    200 * humanize.MiByte,
				},
			},
			Expiration: Expiration{
				set:  true,
				Days: 1,
			},
		},
	}
	tests := []struct {
		lc       Lifecycle
		opts     ObjectOpts
		hasRules bool
	}{
		{ // Delete marker shouldn't match filter without tags
			lc: Lifecycle{
				Rules: []Rule{
					rules[0],
				},
			},
			opts: ObjectOpts{
				DeleteMarker: true,
				IsLatest:     true,
				Name:         "obj-1",
			},
			hasRules: false,
		},
		{ // PUT version with no matching tags
			lc: Lifecycle{
				Rules: []Rule{
					rules[0],
				},
			},
			opts: ObjectOpts{
				IsLatest: true,
				Name:     "obj-1",
				Size:     1 * humanize.MiByte,
			},
			hasRules: false,
		},
		{ // PUT version with matching tags
			lc: Lifecycle{
				Rules: []Rule{
					rules[0],
				},
			},
			opts: ObjectOpts{
				IsLatest: true,
				UserTags: "key1=val1",
				Name:     "obj-1",
				Size:     2 * humanize.MiByte,
			},
			hasRules: true,
		},
		{ // PUT version with size based filters
			lc: Lifecycle{
				Rules: []Rule{
					rules[1],
					rules[2],
					rules[3],
					rules[4],
					rules[5],
				},
			},
			opts: ObjectOpts{
				IsLatest: true,
				UserTags: "key1=val1",
				Name:     "obj-1",
				Size:     1*humanize.MiByte - 1,
			},
			hasRules: true,
		},
		{ // PUT version with size based filters
			lc: Lifecycle{
				Rules: []Rule{
					rules[1],
					rules[2],
					rules[3],
					rules[4],
					rules[5],
				},
			},
			opts: ObjectOpts{
				IsLatest: true,
				Name:     "obj-1",
				Size:     1*humanize.MiByte + 1,
			},
			hasRules: true,
		},
		{ // DEL version with size based filters
			lc: Lifecycle{
				Rules: []Rule{
					rules[1],
					rules[2],
					rules[3],
					rules[4],
					rules[5],
				},
			},
			opts: ObjectOpts{
				DeleteMarker: true,
				IsLatest:     true,
				Name:         "obj-1",
			},
			hasRules: true,
		},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("test-%d", i+1), func(t *testing.T) {
			if err := tc.lc.Validate(lock.Retention{}); err != nil {
				t.Fatalf("Lifecycle validation failed - %v", err)
			}
			rules := tc.lc.FilterRules(tc.opts)
			if tc.hasRules && len(rules) == 0 {
				t.Fatalf("%d: Expected at least one rule to match but none matched", i+1)
			}
			if !tc.hasRules && len(rules) > 0 {
				t.Fatalf("%d: Expected no rules to match but got matches %v", i+1, rules)
			}
		})
	}
}

// TestDeleteAllVersions tests ordering among events, especially ones which
// expire all versions like ExpiredObjectDeleteAllVersions and
// DelMarkerExpiration
func TestDeleteAllVersions(t *testing.T) {
	// ExpiredObjectDeleteAllVersions
	lc := Lifecycle{
		Rules: []Rule{
			{
				ID:     "ExpiredObjectDeleteAllVersions-20",
				Status: "Enabled",
				Expiration: Expiration{
					set:       true,
					DeleteAll: Boolean{val: true, set: true},
					Days:      20,
				},
			},
			{
				ID:     "Transition-10",
				Status: "Enabled",
				Transition: Transition{
					set:          true,
					StorageClass: "WARM-1",
					Days:         10,
				},
			},
		},
	}
	opts := ObjectOpts{
		Name:        "foo.txt",
		ModTime:     time.Now().UTC().Add(-10 * 24 * time.Hour), // created 10 days ago
		Size:        0,
		VersionID:   uuid.New().String(),
		IsLatest:    true,
		NumVersions: 4,
	}

	evaluator := NewEvaluator(lc)
	events := evaluator.eval([]ObjectOpts{opts}, time.Time{})
	event := events[0]
	if event.Action != TransitionAction {
		t.Fatalf("Expected %v action but got %v", TransitionAction, event.Action)
	}
	// The earlier upcoming lifecycle event must be picked, i.e rule with id "Transition-10"
	if exp := ExpectedExpiryTime(opts.ModTime, 10); exp != event.Due {
		t.Fatalf("Expected due %v but got %v, ruleID=%v", exp, event.Due, event.RuleID)
	}

	// DelMarkerExpiration
	lc = Lifecycle{
		Rules: []Rule{
			{
				ID:     "delmarker-exp-20",
				Status: "Enabled",
				DelMarkerExpiration: DelMarkerExpiration{
					Days: 20,
				},
			},
			{
				ID:     "delmarker-exp-10",
				Status: "Enabled",
				DelMarkerExpiration: DelMarkerExpiration{
					Days: 10,
				},
			},
		},
	}
	opts = ObjectOpts{
		Name:         "foo.txt",
		ModTime:      time.Now().UTC().Add(-10 * 24 * time.Hour), // created 10 days ago
		Size:         0,
		VersionID:    uuid.New().String(),
		IsLatest:     true,
		DeleteMarker: true,
		NumVersions:  4,
	}
	evaluator = NewEvaluator(lc)
	events = evaluator.eval([]ObjectOpts{opts}, time.Time{})
	event = events[0]
	if event.Action != DelMarkerDeleteAllVersionsAction {
		t.Fatalf("Expected %v action but got %v", DelMarkerDeleteAllVersionsAction, event.Action)
	}
	// The earlier upcoming lifecycle event must be picked, i.e rule with id "delmarker-exp-10"
	if exp := ExpectedExpiryTime(opts.ModTime, 10); exp != event.Due {
		t.Fatalf("Expected due %v but got %v, ruleID=%v", exp, event.Due, event.RuleID)
	}
}
