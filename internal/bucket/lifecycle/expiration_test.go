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
		{ // Expiration with a valid number of days and ExpiredObjectAllVersions
			inputXML: `<Expiration>
									<Days>3</Days>
			            <ExpiredObjectAllVersions>true</ExpiredObjectAllVersions>
                                    </Expiration>`,
			expectedErr: nil,
		},
		{ // Expiration with a valid ISO 8601 date and ExpiredObjectAllVersions
			inputXML: `<Expiration>
									<Date>2019-04-20T00:00:00Z</Date>
			            <ExpiredObjectAllVersions>true</ExpiredObjectAllVersions>
                                    </Expiration>`,
			expectedErr: errLifecycleInvalidDeleteAll,
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
