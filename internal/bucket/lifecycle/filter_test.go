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

// TestUnsupportedFilters checks if parsing Filter xml with
// unsupported elements returns appropriate errors
func TestUnsupportedFilters(t *testing.T) {
	testCases := []struct {
		inputXML    string
		expectedErr error
	}{
		{ // Filter with And tags
			inputXML: ` <Filter>
						<And>
							<Prefix>key-prefix</Prefix>
						</And>
						</Filter>`,
			expectedErr: errXMLNotWellFormed,
		},
		{ // Filter with Tag tags
			inputXML: ` <Filter>
						<Tag>
							<Key>key1</Key>
							<Value>value1</Value>
						</Tag>
						</Filter>`,
			expectedErr: nil,
		},
		{ // Filter with Prefix tag
			inputXML: ` <Filter>
							<Prefix>key-prefix</Prefix>
						</Filter>`,
			expectedErr: nil,
		},
		{ // Filter without And and multiple Tag tags
			inputXML: ` <Filter>
							<Prefix>key-prefix</Prefix>
							<Tag>
								<Key>key1</Key>
								<Value>value1</Value>
							</Tag>
							<Tag>
								<Key>key2</Key>
								<Value>value2</Value>
							</Tag>
						</Filter>`,
			expectedErr: errInvalidFilter,
		},
		{ // Filter with And, Prefix & multiple Tag tags
			inputXML: ` <Filter>
							<And>
							<Prefix>key-prefix</Prefix>
							<Tag>
								<Key>key1</Key>
								<Value>value1</Value>
							</Tag>
							<Tag>
								<Key>key2</Key>
								<Value>value2</Value>
							</Tag>
							</And>
						</Filter>`,
			expectedErr: nil,
		},
		{ // Filter with And and multiple Tag tags
			inputXML: ` <Filter>
							<And>
							<Prefix></Prefix>
							<Tag>
								<Key>key1</Key>
								<Value>value1</Value>
							</Tag>
							<Tag>
								<Key>key2</Key>
								<Value>value2</Value>
							</Tag>
							</And>
						</Filter>`,
			expectedErr: nil,
		},
		{ // Filter without And and single Tag tag
			inputXML: ` <Filter>
							<Prefix>key-prefix</Prefix>
							<Tag>
								<Key>key1</Key>
								<Value>value1</Value>
							</Tag>
						</Filter>`,
			expectedErr: errInvalidFilter,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var filter Filter
			err := xml.Unmarshal([]byte(tc.inputXML), &filter)
			if err != nil {
				t.Fatalf("%d: Expected no error but got %v", i+1, err)
			}
			err = filter.Validate()
			if err != tc.expectedErr {
				t.Fatalf("%d: Expected %v but got %v", i+1, tc.expectedErr, err)
			}
		})
	}
}
