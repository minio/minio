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

package replication

import (
	"encoding/xml"
)

var (
	errInvalidFilter = Errorf("Filter must have exactly one of Prefix, Tag, or And specified")
)

// Filter - a filter for a replication configuration Rule.
type Filter struct {
	XMLName xml.Name `xml:"Filter" json:"Filter"`
	Prefix  string
	And     And
	Tag     Tag
	// Caching tags, only once
	cachedTags map[string]struct{}
}

// IsEmpty returns true if filter is not set
func (f Filter) IsEmpty() bool {
	return f.And.isEmpty() && f.Tag.IsEmpty() && f.Prefix == ""
}

// MarshalXML - produces the xml representation of the Filter struct
// only one of Prefix, And and Tag should be present in the output.
func (f Filter) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeToken(start); err != nil {
		return err
	}

	switch {
	case !f.And.isEmpty():
		if err := e.EncodeElement(f.And, xml.StartElement{Name: xml.Name{Local: "And"}}); err != nil {
			return err
		}
	case !f.Tag.IsEmpty():
		if err := e.EncodeElement(f.Tag, xml.StartElement{Name: xml.Name{Local: "Tag"}}); err != nil {
			return err
		}
	default:
		// Always print Prefix field when both And & Tag are empty
		if err := e.EncodeElement(f.Prefix, xml.StartElement{Name: xml.Name{Local: "Prefix"}}); err != nil {
			return err
		}
	}

	return e.EncodeToken(xml.EndElement{Name: start.Name})
}

// Validate - validates the filter element
func (f Filter) Validate() error {
	// A Filter must have exactly one of Prefix, Tag, or And specified.
	if !f.And.isEmpty() {
		if f.Prefix != "" {
			return errInvalidFilter
		}
		if !f.Tag.IsEmpty() {
			return errInvalidFilter
		}
		if err := f.And.Validate(); err != nil {
			return err
		}
	}
	if f.Prefix != "" {
		if !f.Tag.IsEmpty() {
			return errInvalidFilter
		}
	}
	if !f.Tag.IsEmpty() {
		if err := f.Tag.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// TestTags tests if the object tags satisfy the Filter tags requirement,
// it returns true if there is no tags in the underlying Filter.
func (f *Filter) TestTags(ttags []string) bool {
	if f.cachedTags == nil {
		tags := make(map[string]struct{})
		for _, t := range append(f.And.Tags, f.Tag) {
			if !t.IsEmpty() {
				tags[t.String()] = struct{}{}
			}
		}
		f.cachedTags = tags
	}
	for ct := range f.cachedTags {
		foundTag := false
		for _, t := range ttags {
			if ct == t {
				foundTag = true
				break
			}
		}
		if !foundTag {
			return false
		}
	}
	return true
}
