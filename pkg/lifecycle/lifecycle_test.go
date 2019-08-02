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

func TestParseLifecycleConfig(t *testing.T) {
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
			Prefix: "/a/b/c",
		},
	}
	overlappingRules := []Rule{rule1, rule2}
	overlappingLcConfig, err := xml.Marshal(Lifecycle{Rules: overlappingRules})
	if err != nil {
		t.Fatal("Failed to marshal lifecycle config with rules having overlapping prefix")
	}

	testCases := []struct {
		inputConfig string
		expectedErr error
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
			expectedErr: nil,
		},
		{ // lifecycle config with no rules
			inputConfig: `<LifecycleConfiguration>
	                              </LifecycleConfiguration>`,
			expectedErr: errLifecycleNoRule,
		},
		{ // lifecycle config with more than 1000 rules
			inputConfig: string(manyRuleLcConfig),
			expectedErr: errLifecycleTooManyRules,
		},
		{ // lifecycle config with rules having overlapping prefix
			inputConfig: string(overlappingLcConfig),
			expectedErr: errLifecycleOverlappingPrefix,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var err error
			if _, err = ParseLifecycleConfig(bytes.NewReader([]byte(tc.inputConfig))); err != tc.expectedErr {
				t.Fatalf("%d: Expected %v but got %v", i+1, tc.expectedErr, err)
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
