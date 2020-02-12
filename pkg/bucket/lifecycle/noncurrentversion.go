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

// NoncurrentVersionExpiration - an action for lifecycle configuration rule.
type NoncurrentVersionExpiration struct {
	XMLName        xml.Name `xml:"NoncurrentVersionExpiration"`
	NoncurrentDays int      `xml:"NoncurrentDays,omitempty"`
}

// NoncurrentVersionTransition - an action for lifecycle configuration rule.
type NoncurrentVersionTransition struct {
	NoncurrentDays int    `xml:"NoncurrentDays"`
	StorageClass   string `xml:"StorageClass"`
}

var (
	errNoncurrentVersionExpirationUnsupported = Errorf("Specifying <NoncurrentVersionExpiration></NoncurrentVersionExpiration> is not supported")
	errNoncurrentVersionTransitionUnsupported = Errorf("Specifying <NoncurrentVersionTransition></NoncurrentVersionTransition> is not supported")
)

// UnmarshalXML is extended to indicate lack of support for
// NoncurrentVersionExpiration xml tag in object lifecycle
// configuration
func (n NoncurrentVersionExpiration) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	return errNoncurrentVersionExpirationUnsupported
}

// UnmarshalXML is extended to indicate lack of support for
// NoncurrentVersionTransition xml tag in object lifecycle
// configuration
func (n NoncurrentVersionTransition) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	return errNoncurrentVersionTransitionUnsupported
}

// MarshalXML is extended to leave out
// <NoncurrentVersionTransition></NoncurrentVersionTransition> tags
func (n NoncurrentVersionTransition) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return nil
}

// MarshalXML is extended to leave out
// <NoncurrentVersionExpiration></NoncurrentVersionExpiration> tags
func (n NoncurrentVersionExpiration) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return nil
}
