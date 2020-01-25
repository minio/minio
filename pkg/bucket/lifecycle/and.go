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
	"errors"
)

// And - a tag to combine a prefix and multiple tags for lifecycle configuration rule.
type And struct {
	XMLName xml.Name `xml:"And"`
	Prefix  string   `xml:"Prefix,omitempty"`
	Tags    []Tag    `xml:"Tag,omitempty"`
}

var errAndUnsupported = errors.New("Specifying <And></And> tag is not supported")

// UnmarshalXML is extended to indicate lack of support for And xml
// tag in object lifecycle configuration
func (a And) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	return errAndUnsupported
}

// MarshalXML is extended to leave out <And></And> tags
func (a And) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return nil
}
