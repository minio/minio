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
	"bytes"
	"testing"

	"github.com/tinylib/msgp/msgp"
)

// TestJEntryReadOldToNew1 - tests that adding the RemoteVersionID parameter to the
// jentry struct does not cause unexpected errors when reading the serialized
// old version into new version.
func TestJEntryReadOldToNew1(t *testing.T) {
	readOldToNewCases := []struct {
		je  jentryV1
		exp jentry
	}{
		{jentryV1{"obj1", "tier1"}, jentry{"obj1", "", "tier1"}},
		{jentryV1{"obj1", ""}, jentry{"obj1", "", ""}},
		{jentryV1{"", "tier1"}, jentry{"", "", "tier1"}},
		{jentryV1{"", ""}, jentry{"", "", ""}},
	}

	var b bytes.Buffer
	for _, item := range readOldToNewCases {
		bs, err := item.je.MarshalMsg(nil)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(bs)
	}

	mr := msgp.NewReader(&b)
	for i, item := range readOldToNewCases {
		var je jentry
		err := je.DecodeMsg(mr)
		if err != nil {
			t.Fatal(err)
		}
		if je != item.exp {
			t.Errorf("Case %d: Expected: %v Got: %v", i, item.exp, je)
		}
	}
}

// TestJEntryWriteNewToOldMix1 - tests that adding the RemoteVersionID parameter
// to the jentry struct does not cause unexpected errors when writing. This
// simulates the case when the active journal has entries in the older version
// struct and due to errors new entries are added in the new version of the
// struct.
func TestJEntryWriteNewToOldMix1(t *testing.T) {
	oldStructVals := []jentryV1{
		{"obj1", "tier1"},
		{"obj2", "tier2"},
		{"obj3", "tier3"},
	}
	newStructVals := []jentry{
		{"obj4", "", "tier1"},
		{"obj5", "ver2", "tier2"},
		{"obj6", "", "tier3"},
	}

	// Write old struct version values followed by new version values.
	var b bytes.Buffer
	for _, item := range oldStructVals {
		bs, err := item.MarshalMsg(nil)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(bs)
	}
	for _, item := range newStructVals {
		bs, err := item.MarshalMsg(nil)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(bs)
	}

	// Read into new struct version and check.
	mr := msgp.NewReader(&b)
	for i := 0; i < len(oldStructVals)+len(newStructVals); i++ {
		var je jentry
		err := je.DecodeMsg(mr)
		if err != nil {
			t.Fatal(err)
		}
		var expectedJe jentry
		if i < len(oldStructVals) {
			// For old struct values, the RemoteVersionID will be
			// empty
			expectedJe = jentry{
				ObjName:   oldStructVals[i].ObjName,
				VersionID: "",
				TierName:  oldStructVals[i].TierName,
			}
		} else {
			expectedJe = newStructVals[i-len(oldStructVals)]
		}
		if expectedJe != je {
			t.Errorf("Case %d: Expected: %v, Got: %v", i, expectedJe, je)
		}
	}
}
