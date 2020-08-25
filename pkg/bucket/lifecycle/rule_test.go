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
	"encoding/xml"
	"fmt"
	"testing"
)

// TestUnsupportedRules checks if Rule xml with unsuported tags return
// appropriate errors on parsing
func TestUnsupportedRules(t *testing.T) {
	// NoncurrentVersionTransition, and Transition tags aren't supported
	unsupportedTestCases := []struct {
		inputXML    string
		expectedErr error
	}{
		{ // Rule with unsupported NoncurrentVersionTransition
			inputXML: ` <Rule>
	                     <NoncurrentVersionTransition></NoncurrentVersionTransition>
	                    </Rule>`,
			expectedErr: errNoncurrentVersionTransitionUnsupported,
		},
		{ // Rule with unsupported Transition action
			inputXML: ` <Rule>
	                     <Transition></Transition>
	                    </Rule>`,
			expectedErr: errTransitionUnsupported,
		},
	}

	for i, tc := range unsupportedTestCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var rule Rule
			err := xml.Unmarshal([]byte(tc.inputXML), &rule)
			if err != tc.expectedErr {
				t.Fatalf("%d: Expected %v but got %v", i+1, tc.expectedErr, err)
			}
		})
	}
}

// TestInvalidRules checks if Rule xml with invalid elements returns
// appropriate errors on validation
func TestInvalidRules(t *testing.T) {
	invalidTestCases := []struct {
		inputXML    string
		expectedErr error
	}{
		{ // Rule with ID longer than 255 characters
			inputXML: ` <Rule>
	                    <ID> babababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab </ID>
	                    </Rule>`,
			expectedErr: errInvalidRuleID,
		},
		{ // Rule with empty ID
			inputXML: `<Rule>
							<ID></ID>
							<Expiration>
								<Days>365</Days>
							</Expiration>
                            <Status>Enabled</Status>
	                    </Rule>`,
			expectedErr: nil,
		},
		{ // Rule with empty status
			inputXML: ` <Rule>
			                  <ID>rule with empty status</ID>
                              <Status></Status>
	                    </Rule>`,
			expectedErr: errEmptyRuleStatus,
		},
		{ // Rule with invalid status
			inputXML: ` <Rule>
			                  <ID>rule with invalid status</ID>
                              <Status>OK</Status>
	                    </Rule>`,
			expectedErr: errInvalidRuleStatus,
		},
	}

	for i, tc := range invalidTestCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var rule Rule
			err := xml.Unmarshal([]byte(tc.inputXML), &rule)
			if err != nil {
				t.Fatal(err)
			}

			if err := rule.Validate(); err != tc.expectedErr {
				t.Fatalf("%d: Expected %v but got %v", i+1, tc.expectedErr, err)
			}
		})
	}
}
