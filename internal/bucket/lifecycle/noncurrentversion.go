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
