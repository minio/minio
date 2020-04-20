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
	"strings"

	"github.com/minio/minio-go/v6/pkg/tags"
)

var errInvalidFilter = Errorf("Filter must have exactly one of Prefix, Tag, or And specified")

func isAllowed(prefix string, and *And, tag *tags.Tag) bool {
	return (prefix != "") != (and != nil) != (tag != nil) || // prefix XOR and XOR tag.
		(prefix == "" && and == nil && tag == nil) // empty prefix allowed.
}

// Filter - a filter for a lifecycle configuration Rule.
type Filter struct {
	XMLName xml.Name  `xml:"Filter"`
	Prefix  string    `xml:"Prefix"`
	And     *And      `xml:"And,omitempty"`
	Tag     *tags.Tag `xml:"Tag,omitempty"`
}

func (f Filter) getPrefix() string {
	if f.Prefix != "" {
		return f.Prefix
	}

	if f.And != nil {
		return f.And.Prefix
	}

	return ""
}

func (f Filter) getTags() string {
	s := []string{}
	if f.And != nil {
		for key, value := range f.And.Tags {
			s = append(s, key+"="+value)
		}
	} else if f.Tag != nil {
		s = append(s, f.Tag.String())
	}

	return strings.Join(s, "&")
}

// MarshalXML encodes to XML data.
func (f Filter) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if isAllowed(f.Prefix, f.And, f.Tag) {
		type subFilter Filter // sub-type to avoid recursively called MarshalXML()
		return e.EncodeElement(subFilter(f), start)
	}

	return errInvalidFilter
}

// UnmarshalXML decodes XML data.
func (f *Filter) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type subFilter Filter // sub-type to avoid recursively called UnmarshalXML()
	sf := subFilter{}
	if err := d.DecodeElement(&sf, &start); err != nil {
		return err
	}

	if isAllowed(sf.Prefix, sf.And, sf.Tag) {
		*f = Filter(sf)
		return nil
	}

	return errInvalidFilter
}
