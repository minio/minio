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
	"testing"
)

func TestTransitionUnmarshalXML(t *testing.T) {
	trTests := []struct {
		input string
		err   error
	}{
		{
			input: `<Transition>
			<Days>0</Days>
			<StorageClass>S3TIER-1</StorageClass>
		  </Transition>`,
			err: nil,
		},
		{
			input: `<Transition>
			<Days>1</Days>
			<Date>2021-01-01T00:00:00Z</Date>
			<StorageClass>S3TIER-1</StorageClass>
		  </Transition>`,
			err: errTransitionInvalid,
		},
		{
			input: `<Transition>
			<Days>1</Days>
		  </Transition>`,
			err: errXMLNotWellFormed,
		},
	}

	for i, tc := range trTests {
		var tr Transition
		err := xml.Unmarshal([]byte(tc.input), &tr)
		if err != nil {
			t.Fatalf("%d: xml unmarshal failed with %v", i+1, err)
		}
		if err = tr.Validate(); err != tc.err {
			t.Fatalf("%d: Invalid transition %v: err %v", i+1, tr, err)
		}
	}

	ntrTests := []struct {
		input string
		err   error
	}{
		{
			input: `<NoncurrentVersionTransition>
			<NoncurrentDays>0</NoncurrentDays>
			<StorageClass>S3TIER-1</StorageClass>
		  </NoncurrentVersionTransition>`,
			err: nil,
		},
		{
			input: `<NoncurrentVersionTransition>
			<Days>1</Days>
		  </NoncurrentVersionTransition>`,
			err: errXMLNotWellFormed,
		},
	}

	for i, tc := range ntrTests {
		var ntr NoncurrentVersionTransition
		err := xml.Unmarshal([]byte(tc.input), &ntr)
		if err != nil {
			t.Fatalf("%d: xml unmarshal failed with %v", i+1, err)
		}
		if err = ntr.Validate(); err != tc.err {
			t.Fatalf("%d: Invalid noncurrent version transition %v: err %v", i+1, ntr, err)
		}
	}
}
