/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package lifecycle

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"testing"
	"time"
)

func TestParseAndValidateLifecycleConfig(t *testing.T) {
	// Test for  lifecycle config with more than 1000 rules
	var manyRules []Rule
	rule := Rule{
		Status:     "Enabled",
		Expiration: Expiration{Days: ExpirationDays(3)},
	}
	for i := 0; i < 1001; i++ {
		manyRules = append(manyRules, rule)
	}

	manyRuleLcConfig, err := xml.Marshal(Lifecycle{Rules: manyRules})
	if err != nil {
		t.Fatal("Failed to marshal lifecycle config with more than 1000 rules")
	}

	// Test for lifecycle config with rules containing overlapping prefixes
	rule1 := Rule{
		Status:     "Enabled",
		Expiration: Expiration{Days: ExpirationDays(3)},
		Filter: Filter{
			Prefix: "/a/b",
		},
	}
	rule2 := Rule{
		Status:     "Enabled",
		Expiration: Expiration{Days: ExpirationDays(3)},
		Filter: Filter{
			And: And{
				Prefix: "/a/b/c",
			},
		},
	}
	overlappingRules := []Rule{rule1, rule2}
	overlappingLcConfig, err := xml.Marshal(Lifecycle{Rules: overlappingRules})
	if err != nil {
		t.Fatal("Failed to marshal lifecycle config with rules having overlapping prefix")
	}

	testCases := []struct {
		inputConfig           string
		expectedParsingErr    error
		expectedValidationErr error
	}{
		{ // Valid lifecycle config
			inputConfig: `<LifecycleConfiguration>
		                          <Rule>
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
		{ // lifecycle config with more than 1000 rules
			inputConfig:           string(manyRuleLcConfig),
			expectedParsingErr:    nil,
			expectedValidationErr: errLifecycleTooManyRules,
		},
		{ // lifecycle config with rules having overlapping prefix
			inputConfig:           string(overlappingLcConfig),
			expectedParsingErr:    nil,
			expectedValidationErr: errLifecycleOverlappingPrefix,
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
				Filter:     Filter{Prefix: "prefix-1"},
				Expiration: Expiration{Days: ExpirationDays(3)},
			},
			{
				Status:     "Enabled",
				Filter:     Filter{Prefix: "prefix-1"},
				Expiration: Expiration{Date: ExpirationDate(midnightTS)},
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
			got := expectedExpiryTime(tc.modTime, tc.days)
			if got != tc.expected {
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
		// Should remove, the second rule has expiration kicked in
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).UTC().Add(24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule><Rule><Filter><Prefix>foxdir/</Prefix></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).UTC().Add(-24*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foxdir/fooobject",
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
