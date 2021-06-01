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
	"testing"
	"time"
)

func TestParseAndValidateLifecycleConfig(t *testing.T) {
	testCases := []struct {
		inputConfig           string
		expectedParsingErr    error
		expectedValidationErr error
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
		{ // lifecycle config with no rules
			inputConfig: `<LifecycleConfiguration>
		                          </LifecycleConfiguration>`,
			expectedParsingErr:    nil,
			expectedValidationErr: errLifecycleNoRule,
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
			err = lc.Validate()
			if err != tc.expectedValidationErr {
				t.Fatalf("%d: Expected %v during parsing but got %v", i+1, tc.expectedValidationErr, err)
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
				NoncurrentVersionTransition: NoncurrentVersionTransition{NoncurrentDays: 2, StorageClass: "TEST"},
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

func TestComputeActions(t *testing.T) {
	testCases := []struct {
		inputConfig    string
		objectName     string
		objectTags     string
		objectModTime  time.Time
		expectedAction Action
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
	}

	for _, tc := range testCases {
		tc := tc
		t.Run("", func(t *testing.T) {
			lc, err := ParseLifecycleConfig(bytes.NewReader([]byte(tc.inputConfig)))
			if err != nil {
				t.Fatalf("Got unexpected error: %v", err)
			}
			if resultAction := lc.ComputeAction(ObjectOpts{
				Name:     tc.objectName,
				UserTags: tc.objectTags,
				ModTime:  tc.objectModTime,
				IsLatest: true,
			}); resultAction != tc.expectedAction {
				t.Fatalf("Expected action: `%v`, got: `%v`", tc.expectedAction, resultAction)
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
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			prefix:         "foodir/foobject",
			expectedNonRec: true, expectedRec: true,
		},
		{ // empty prefix
			inputConfig:    `<LifecycleConfiguration><Rule><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			prefix:         "foodir/foobject/foo.txt",
			expectedNonRec: true, expectedRec: true,
		},
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			prefix:         "zdir/foobject",
			expectedNonRec: false, expectedRec: false,
		},
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/zdir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			prefix:         "foodir/",
			expectedNonRec: false, expectedRec: true,
		},
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix></Prefix></Filter><Status>Disabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			prefix:         "foodir/",
			expectedNonRec: false, expectedRec: false,
		},
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Date>2999-01-01T00:00:00.000Z</Date></Expiration></Rule></LifecycleConfiguration>`,
			prefix:         "foodir/foobject",
			expectedNonRec: false, expectedRec: false,
		},
	}

	for i, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("Test_%d", i+1), func(t *testing.T) {
			lc, err := ParseLifecycleConfig(bytes.NewReader([]byte(tc.inputConfig)))
			if err != nil {
				t.Fatalf("Got unexpected error: %v", err)
			}
			if got := lc.HasActiveRules(tc.prefix, false); got != tc.expectedNonRec {
				t.Fatalf("Expected result with recursive set to false: `%v`, got: `%v`", tc.expectedNonRec, got)
			}
			if got := lc.HasActiveRules(tc.prefix, true); got != tc.expectedRec {
				t.Fatalf("Expected result with recursive set to true: `%v`, got: `%v`", tc.expectedRec, got)
			}

		})

	}
}
