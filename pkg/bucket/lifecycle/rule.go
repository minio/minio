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

import "encoding/xml"

var (
	errInvalidRuleID           = Errorf("ID must be less than 255 characters")
	errEmptyRuleStatus         = Errorf("Status should not be empty")
	errInvalidRuleStatus       = Errorf("Status must be set to either Enabled or Disabled")
	errMissingExpirationAction = Errorf("No expiration action found")
)

// Status represents lifecycle configuration status
type Status string

// Supported status types
const (
	Enabled  Status = "Enabled"
	Disabled Status = "Disabled"
)

// MarshalXML encodes to XML data.
func (status Status) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	switch status {
	case Enabled, Disabled:
		return e.EncodeElement(string(status), start)
	}

	if status == "" {
		return errEmptyRuleStatus
	}

	return errInvalidRuleStatus
}

// UnmarshalXML decodes XML data.
func (status *Status) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string
	if err := d.DecodeElement(&s, &start); err != nil {
		return err
	}

	switch Status(s) {
	case Enabled, Disabled:
		*status = Status(s)
		return nil
	}

	if s == "" {
		return errEmptyRuleStatus
	}

	return errInvalidRuleStatus
}

// Rule - a rule for lifecycle configuration.
type Rule struct {
	XMLName                        xml.Name                        `xml:"Rule"`
	ID                             string                          `xml:"ID,omitempty"`
	Status                         Status                          `xml:"Status"`
	Filter                         *Filter                         `xml:"Filter,omitempty"`
	Expiration                     *Expiration                     `xml:"Expiration,omitempty"`
	Transition                     *Transition                     `xml:"Transition,omitempty"`                     // unsupported
	AbortIncompleteMultipartUpload *AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"` // unsupported
	NoncurrentVersionExpiration    *NoncurrentVersionExpiration    `xml:"NoncurrentVersionExpiration,omitempty"`    // unsupported
	NoncurrentVersionTransition    *NoncurrentVersionTransition    `xml:"NoncurrentVersionTransition,omitempty"`    // unsupported
}

// Prefix - a rule can either have prefix under <filter></filter> or under
// <filter><and></and></filter>. This method returns the prefix from the
// location where it is available
func (r Rule) Prefix() string {
	return r.Filter.getPrefix()
}

// Tags - a rule can either have tag under <filter></filter> or under
// <filter><and></and></filter>. This method returns all the tags from the
// rule in the format tag1=value1&tag2=value2
func (r Rule) Tags() string {
	return r.Filter.getTags()
}

// MarshalXML encodes to XML data.
func (r Rule) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if len(r.ID) > 255 {
		return errInvalidRuleID
	}

	if r.Expiration == nil {
		return errMissingExpirationAction
	}

	type subRule Rule
	return e.EncodeElement(subRule(r), start)
}

// UnmarshalXML decodes XML data.
func (r *Rule) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type subRule Rule
	var sr subRule
	if err := d.DecodeElement(&sr, &start); err != nil {
		return err
	}

	// cannot be longer than 255 characters
	if len(sr.ID) > 255 {
		return errInvalidRuleID
	}

	if sr.Expiration == nil {
		return errMissingExpirationAction
	}

	*r = Rule(sr)
	return nil
}
