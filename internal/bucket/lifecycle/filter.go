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
	"io"
)

var (
	errInvalidFilter = Errorf("Filter must have exactly one of Prefix, Tag, or And specified")
)

// Filter - a filter for a lifecycle configuration Rule.
type Filter struct {
	XMLName xml.Name `xml:"Filter"`
	set     bool

	Prefix Prefix

	And    And
	andSet bool

	Tag    Tag
	tagSet bool
	// Caching tags, only once
	cachedTags []string
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

// UnmarshalXML - decodes XML data.
func (f *Filter) UnmarshalXML(d *xml.Decoder, start xml.StartElement) (err error) {
	f.set = true
	for {
		// Read tokens from the XML document in a stream.
		t, err := d.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch se := t.(type) {
		case xml.StartElement:
			switch se.Name.Local {
			case "Prefix":
				var p Prefix
				if err = d.DecodeElement(&p, &se); err != nil {
					return err
				}
				f.Prefix = p
			case "And":
				var and And
				if err = d.DecodeElement(&and, &se); err != nil {
					return err
				}
				f.And = and
				f.andSet = true
			case "Tag":
				var tag Tag
				if err = d.DecodeElement(&tag, &se); err != nil {
					return err
				}
				f.Tag = tag
				f.tagSet = true
			default:
				return errUnknownXMLTag
			}
		}
	}
	return nil
}

// IsEmpty returns true if Filter is not specified in the XML
func (f Filter) IsEmpty() bool {
	return !f.set
}

// Validate - validates the filter element
func (f Filter) Validate() error {
	if f.IsEmpty() {
		return errXMLNotWellFormed
	}
	// A Filter must have exactly one of Prefix, Tag, or And specified.
	if !f.And.isEmpty() {
		if f.Prefix.set {
			return errInvalidFilter
		}
		if !f.Tag.IsEmpty() {
			return errInvalidFilter
		}
		if err := f.And.Validate(); err != nil {
			return err
		}
	}
	if f.Prefix.set {
		if !f.Tag.IsEmpty() {
			return errInvalidFilter
		}
	}
	if !f.Tag.IsEmpty() {
		if f.Prefix.set {
			return errInvalidFilter
		}
		if err := f.Tag.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// TestTags tests if the object tags satisfy the Filter tags requirement,
// it returns true if there is no tags in the underlying Filter.
func (f Filter) TestTags(tags []string) bool {
	if f.cachedTags == nil {
		tags := make([]string, 0)
		for _, t := range append(f.And.Tags, f.Tag) {
			if !t.IsEmpty() {
				tags = append(tags, t.String())
			}
		}
		f.cachedTags = tags
	}
	for _, ct := range f.cachedTags {
		foundTag := false
		for _, t := range tags {
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
