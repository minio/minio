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

	"github.com/minio/minio-go/v6/pkg/tags"
)

var errDuplicateTagKey = Errorf("Duplicate Tag Keys are not allowed")

type andRule struct {
	Prefix string     `xml:"Prefix,omitempty"`
	Tags   []tags.Tag `xml:"Tag"`
}

// And - a tag to combine a prefix and multiple tags for lifecycle configuration rule.
type And struct {
	Prefix string
	Tags   map[string]string
}

// MarshalXML encodes to XML data.
func (a And) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	sa := andRule{Prefix: a.Prefix}
	for key, value := range a.Tags {
		sa.Tags = append(sa.Tags, tags.Tag{Key: key, Value: value})
	}

	return e.EncodeElement(sa, start)
}

// UnmarshalXML decodes XML data.
func (a *And) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	sa := andRule{}
	if err := d.DecodeElement(&sa, &start); err != nil {
		return err
	}

	m := make(map[string]string)
	for _, tag := range sa.Tags {
		if _, found := m[tag.Key]; found {
			return errDuplicateTagKey
		}

		m[tag.Key] = tag.Value
	}

	a.Prefix = sa.Prefix
	a.Tags = m
	return nil
}
