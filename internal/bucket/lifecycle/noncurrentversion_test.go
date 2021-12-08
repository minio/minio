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

import "testing"

func Test_NoncurrentVersionsExpiration_Validation(t *testing.T) {
	testcases := []struct {
		n   NoncurrentVersionExpiration
		err error
	}{
		{
			n: NoncurrentVersionExpiration{
				NoncurrentDays:          0,
				NewerNoncurrentVersions: 0,
				set:                     true,
			},
			err: errXMLNotWellFormed,
		},
		{
			n: NoncurrentVersionExpiration{
				NoncurrentDays:          90,
				NewerNoncurrentVersions: 0,
				set:                     true,
			},
			err: nil,
		},
		{
			n: NoncurrentVersionExpiration{
				NoncurrentDays:          90,
				NewerNoncurrentVersions: 2,
				set:                     true,
			},
			err: nil,
		},
		{
			n: NoncurrentVersionExpiration{
				NoncurrentDays: -1,
				set:            true,
			},
			err: errXMLNotWellFormed,
		},
		{
			n: NoncurrentVersionExpiration{
				NoncurrentDays:          90,
				NewerNoncurrentVersions: -2,
				set:                     true,
			},
			err: errXMLNotWellFormed,
		},
		// MinIO extension: supports zero NoncurrentDays when NewerNoncurrentVersions > 0
		{
			n: NoncurrentVersionExpiration{
				NoncurrentDays:          0,
				NewerNoncurrentVersions: 5,
				set:                     true,
			},
			err: nil,
		},
	}

	for i, tc := range testcases {
		if got := tc.n.Validate(); got != tc.err {
			t.Fatalf("%d: expected %v but got %v", i+1, tc.err, got)
		}
	}
}
