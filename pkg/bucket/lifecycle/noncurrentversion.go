/*
 * MinIO Cloud Storage, (C) 2019-2020 MinIO, Inc.
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
	XMLName        xml.Name       `xml:"NoncurrentVersionExpiration"`
	NoncurrentDays ExpirationDays `xml:"NoncurrentDays,omitempty"`
	set            bool
}

// MarshalXML if non-current days not set to non zero value
func (n NoncurrentVersionExpiration) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if n.IsDaysNull() {
		return nil
	}
	type noncurrentVersionExpirationWrapper NoncurrentVersionExpiration
	return e.EncodeElement(noncurrentVersionExpirationWrapper(n), start)
}

// UnmarshalXML decodes NoncurrentVersionExpiration
func (n *NoncurrentVersionExpiration) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	type noncurrentVersionExpirationWrapper NoncurrentVersionExpiration
	var val noncurrentVersionExpirationWrapper
	err := d.DecodeElement(&val, &startElement)
	if err != nil {
		return err
	}
	*n = NoncurrentVersionExpiration(val)
	n.set = true
	return nil
}

// IsDaysNull returns true if days field is null
func (n NoncurrentVersionExpiration) IsDaysNull() bool {
	return n.NoncurrentDays == ExpirationDays(0)
}

// Validate returns an error with wrong value
func (n NoncurrentVersionExpiration) Validate() error {
	if !n.set {
		return nil
	}
	val := int(n.NoncurrentDays)
	if val <= 0 {
		return errXMLNotWellFormed
	}
	return nil
}

// NoncurrentVersionTransition - an action for lifecycle configuration rule.
type NoncurrentVersionTransition struct {
	NoncurrentDays ExpirationDays `xml:"NoncurrentDays"`
	StorageClass   string         `xml:"StorageClass"`
	set            bool
}

// MarshalXML is extended to leave out
// <NoncurrentVersionTransition></NoncurrentVersionTransition> tags
func (n NoncurrentVersionTransition) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if n.NoncurrentDays == ExpirationDays(0) {
		return nil
	}
	type noncurrentVersionTransitionWrapper NoncurrentVersionTransition
	return e.EncodeElement(noncurrentVersionTransitionWrapper(n), start)
}

// IsDaysNull returns true if days field is null
func (n NoncurrentVersionTransition) IsDaysNull() bool {
	return n.NoncurrentDays == ExpirationDays(0)
}

// UnmarshalXML decodes NoncurrentVersionExpiration
func (n *NoncurrentVersionTransition) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	type noncurrentVersionTransitionWrapper NoncurrentVersionTransition
	var val noncurrentVersionTransitionWrapper
	err := d.DecodeElement(&val, &startElement)
	if err != nil {
		return err
	}
	*n = NoncurrentVersionTransition(val)
	n.set = true
	return nil
}

// Validate returns an error with wrong value
func (n NoncurrentVersionTransition) Validate() error {
	if !n.set {
		return nil
	}
	if int(n.NoncurrentDays) <= 0 || n.StorageClass == "" {
		return errXMLNotWellFormed
	}
	return nil
}
