// Copyright (c) 2024 MinIO, Inc.
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

func TestDelMarkerExpParseAndValidate(t *testing.T) {
	tests := []struct {
		xml string
		err error
	}{
		{
			xml: `<DelMarkerExpiration> <Days> 1 </Days> </DelMarkerExpiration>`,
			err: nil,
		},
		{
			xml: `<DelMarkerExpiration> <Days> -1 </Days> </DelMarkerExpiration>`,
			err: errInvalidDaysDelMarkerExpiration,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("TestDelMarker-%d", i), func(t *testing.T) {
			var dexp DelMarkerExpiration
			var fail bool
			err := xml.Unmarshal([]byte(test.xml), &dexp)
			if test.err == nil {
				if err != nil {
					fail = true
				}
			} else {
				if err == nil {
					fail = true
				}
				if test.err.Error() != err.Error() {
					fail = true
				}
			}
			if fail {
				t.Fatalf("Expected %v but got %v", test.err, err)
			}
		})
	}
}
