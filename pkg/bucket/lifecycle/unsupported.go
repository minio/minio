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

var (
	errTransitionUnsupported                  = Errorf("Specifying <Transition></Transition> tag is not supported")
	errNoncurrentVersionExpirationUnsupported = Errorf(
		"Specifying <NoncurrentVersionExpiration></NoncurrentVersionExpiration> is not supported")
	errNoncurrentVersionTransitionUnsupported = Errorf(
		"Specifying <NoncurrentVersionTransition></NoncurrentVersionTransition> is not supported")
	errAbortIncompleteMultipartUpload = Errorf(
		"Specifying <AbortIncompleteMultipartUpload></AbortIncompleteMultipartUpload> tag is not supported")
)

// Transition - transition actions for a rule in lifecycle configuration.
type Transition struct {
	XMLName      xml.Name `xml:"Transition"`
	Days         int      `xml:"Days,omitempty"`
	Date         string   `xml:"Date,omitempty"`
	StorageClass string   `xml:"StorageClass"`
}

// MarshalXML is extended to leave out <Transition></Transition> tags
func (t Transition) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return errTransitionUnsupported
}

// UnmarshalXML is extended to indicate lack of support for Transition
// xml tag in object lifecycle configuration
func (t *Transition) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	return errTransitionUnsupported
}

// NoncurrentVersionExpiration - an action for lifecycle configuration rule.
type NoncurrentVersionExpiration struct {
	XMLName        xml.Name `xml:"NoncurrentVersionExpiration"`
	NoncurrentDays int      `xml:"NoncurrentDays,omitempty"`
}

// MarshalXML is extended to leave out
// <NoncurrentVersionExpiration></NoncurrentVersionExpiration> tags
func (n NoncurrentVersionExpiration) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return errNoncurrentVersionExpirationUnsupported
}

// UnmarshalXML is extended to indicate lack of support for
// NoncurrentVersionExpiration xml tag in object lifecycle
// configuration
func (n *NoncurrentVersionExpiration) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	return errNoncurrentVersionExpirationUnsupported
}

// NoncurrentVersionTransition - an action for lifecycle configuration rule.
type NoncurrentVersionTransition struct {
	NoncurrentDays int    `xml:"NoncurrentDays"`
	StorageClass   string `xml:"StorageClass"`
}

// MarshalXML is extended to leave out
// <NoncurrentVersionTransition></NoncurrentVersionTransition> tags
func (n NoncurrentVersionTransition) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return errNoncurrentVersionTransitionUnsupported
}

// UnmarshalXML is extended to indicate lack of support for
// NoncurrentVersionTransition xml tag in object lifecycle
// configuration
func (n *NoncurrentVersionTransition) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	return errNoncurrentVersionTransitionUnsupported
}

// AbortIncompleteMultipartUpload - an action for lifecycle configuration rule.
type AbortIncompleteMultipartUpload struct {
	Days uint `xml:"DaysAfterInitiation,omitempty"`
}

// MarshalXML is extended to leave out <Transition></Transition> tags
func (a AbortIncompleteMultipartUpload) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return errTransitionUnsupported
}

// UnmarshalXML is extended to indicate lack of support for Transition
// xml tag in object lifecycle configuration
func (a *AbortIncompleteMultipartUpload) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	return errTransitionUnsupported
}
