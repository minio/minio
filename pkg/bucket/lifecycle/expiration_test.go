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

// appropriate errors on validation
func TestInvalidExpiration(t *testing.T) {
	testCases := []struct {
		inputXML    string
		expectedErr error
	}{
		{ // Expiration with zero days
			inputXML: ` <Expiration>
                                    <Days>0</Days>
                                    </Expiration>`,
			expectedErr: errLifecycleInvalidDays,
		},
		{ // Expiration with invalid date
			inputXML: ` <Expiration>
                                    <Date>invalid date</Date>
                                    </Expiration>`,
			expectedErr: errLifecycleInvalidDate,
		},
		{ // Expiration with both number of days nor a date
			inputXML: `<Expiration>
		                    <Date>2019-04-20T00:01:00Z</Date>
		                    </Expiration>`,
			expectedErr: errLifecycleDateNotMidnight,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var expiration Expiration
			err := xml.Unmarshal([]byte(tc.inputXML), &expiration)
			if err != tc.expectedErr {
				t.Fatalf("%d: Expected %v but got %v", i+1, tc.expectedErr, err)
			}
		})

	}

	validationTestCases := []struct {
		inputXML    string
		expectedErr error
	}{
		{ // Expiration with a valid ISO 8601 date
			inputXML: `<Expiration>
                                    <Date>2019-04-20T00:00:00Z</Date>
                                    </Expiration>`,
			expectedErr: nil,
		},
		{ // Expiration with a valid number of days
			inputXML: `<Expiration>
                                    <Days>3</Days>
                                    </Expiration>`,
			expectedErr: nil,
		},
		{ // Expiration with neither number of days nor a date
			inputXML: `<Expiration>
                                    </Expiration>`,
			expectedErr: errXMLNotWellFormed,
		},
		{ // Expiration with both number of days and a date
			inputXML: `<Expiration>
                                    <Days>3</Days>
                                    <Date>2019-04-20T00:00:00Z</Date>
                                    </Expiration>`,
			expectedErr: errLifecycleInvalidExpiration,
		},
		{ // Expiration with both ExpiredObjectDeleteMarker and days
			inputXML: `<Expiration>
                                    <Days>3</Days>
			            <ExpiredObjectDeleteMarker>false</ExpiredObjectDeleteMarker>
                                    </Expiration>`,
			expectedErr: errLifecycleInvalidDeleteMarker,
		},
	}
	for i, tc := range validationTestCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var expiration Expiration
			err := xml.Unmarshal([]byte(tc.inputXML), &expiration)
			if err != nil {
				t.Fatalf("%d: %v", i+1, err)
			}

			err = expiration.Validate()
			if err != tc.expectedErr {
				t.Fatalf("%d: got: %v, expected: %v", i+1, err, tc.expectedErr)
			}
		})
	}
}
