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

package cmd

import (
	"testing"
)

func TestNewRequestID(t *testing.T) {
	// Ensure that it returns an alphanumeric result of length 16.
	id := mustGetRequestID(UTCNow())

	if len(id) != 16 {
		t.Fail()
	}

	var e rune
	for _, char := range id {
		e = char

		// Ensure that it is alphanumeric, in this case, between 0-9 and A-Z.
		isAlnum := ('0' <= e && e <= '9') || ('A' <= e && e <= 'Z')
		if !isAlnum {
			t.Fail()
		}
	}
}
