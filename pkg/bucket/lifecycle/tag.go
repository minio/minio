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
)

// Tag - a tag for a lifecycle configuration Rule filter.
type Tag struct {
	XMLName xml.Name `xml:"Tag"`
	Key     string   `xml:"Key,omitempty"`
	Value   string   `xml:"Value,omitempty"`
}

var errTagUnsupported = Errorf("Specifying <Tag></Tag> is not supported")

// UnmarshalXML is extended to indicate lack of support for Tag
// xml tag in object lifecycle configuration
func (t Tag) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	return errTagUnsupported
}

// MarshalXML is extended to leave out <Tag></Tag> tags
func (t Tag) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return nil
}
