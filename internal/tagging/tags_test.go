// Copyright (c) 2015-2023 MinIO, Inc.
//
// # This file is part of MinIO Object Storage stack
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
package tagging

import (
	"encoding/json"
	"testing"

	xxml "github.com/minio/xxml"
)

func TestTags_MarshalXML(t *testing.T) {
	type Sample struct {
		Tagging *Tagging
	}
	tagString := Tagging("key=value&key2=&key3=else")
	sample := Sample{Tagging: &tagString}
	got, err := xxml.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	want := "<Sample><Tagging><TagSet><Tag><Key>key</Key><Value>value</Value></Tag><Tag><Key>key2</Key><Value></Value></Tag><Tag><Key>key3</Key><Value>else</Value></Tag></TagSet></Tagging></Sample>"
	if want != string(got) {
		t.Errorf("want %s\ngot %s", want, got)
	}
}

func TestTagging_MarshalJSON(t *testing.T) {
	type Sample struct {
		Tagging *Tagging
	}
	tagString := Tagging("key=value&key2=&key3=else")
	sample := Sample{Tagging: &tagString}
	got, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"Tagging":{"key":"value","key2":"","key3":"else"}}`
	if want != string(got) {
		t.Errorf("want %s\ngot %s", want, got)
	}
}
