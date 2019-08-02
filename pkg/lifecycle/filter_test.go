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
	                     <Prefix></Prefix>
	                     </And>
	                    </Filter>`,
			expectedErr: errAndUnsupported,
		},
		{ // Filter with Tag tags
			inputXML: ` <Filter>
	                     <Tag></Tag>
	                    </Filter>`,
			expectedErr: errTagUnsupported,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var filter Filter
			err := xml.Unmarshal([]byte(tc.inputXML), &filter)
			if err != tc.expectedErr {
				t.Fatalf("%d: Expected %v but got %v", i+1, tc.expectedErr, err)
			}
		})
	}
}
