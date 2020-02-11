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

// Transition - transition actions for a rule in lifecycle configuration.
type Transition struct {
	XMLName      xml.Name `xml:"Transition"`
	Days         int      `xml:"Days,omitempty"`
	Date         string   `xml:"Date,omitempty"`
	StorageClass string   `xml:"StorageClass"`
}

var errTransitionUnsupported = Errorf("Specifying <Transition></Transition> tag is not supported")

// UnmarshalXML is extended to indicate lack of support for Transition
// xml tag in object lifecycle configuration
func (t Transition) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	return errTransitionUnsupported
}

// MarshalXML is extended to leave out <Transition></Transition> tags
func (t Transition) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return nil
}
