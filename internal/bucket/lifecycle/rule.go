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
	"bytes"
	"encoding/xml"
)

// Status represents lifecycle configuration status
type Status string

// Supported status types
const (
	Enabled  Status = "Enabled"
	Disabled Status = "Disabled"
)

// Rule - a rule for lifecycle configuration.
type Rule struct {
	XMLName             xml.Name            `xml:"Rule"`
	ID                  string              `xml:"ID,omitempty"`
	Status              Status              `xml:"Status"`
	Filter              Filter              `xml:"Filter,omitempty"`
	Prefix              Prefix              `xml:"Prefix,omitempty"`
	Expiration          Expiration          `xml:"Expiration,omitempty"`
	Transition          Transition          `xml:"Transition,omitempty"`
	DelMarkerExpiration DelMarkerExpiration `xml:"DelMarkerExpiration,omitempty"`
	// FIXME: add a type to catch unsupported AbortIncompleteMultipartUpload AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
	NoncurrentVersionExpiration NoncurrentVersionExpiration `xml:"NoncurrentVersionExpiration,omitempty"`
	NoncurrentVersionTransition NoncurrentVersionTransition `xml:"NoncurrentVersionTransition,omitempty"`
}

var (
	errInvalidRuleID                  = Errorf("ID length is limited to 255 characters")
	errEmptyRuleStatus                = Errorf("Status should not be empty")
	errInvalidRuleStatus              = Errorf("Status must be set to either Enabled or Disabled")
	errInvalidRuleDelMarkerExpiration = Errorf("Rule with DelMarkerExpiration cannot have tags based filtering")
)

// validateID - checks if ID is valid or not.
func (r Rule) validateID() error {
	if len(r.ID) > 255 {
		return errInvalidRuleID
	}
	return nil
}

// validateStatus - checks if status is valid or not.
func (r Rule) validateStatus() error {
	// Status can't be empty
	if len(r.Status) == 0 {
		return errEmptyRuleStatus
	}

	// Status must be one of Enabled or Disabled
	if r.Status != Enabled && r.Status != Disabled {
		return errInvalidRuleStatus
	}
	return nil
}

func (r Rule) validateExpiration() error {
	return r.Expiration.Validate()
}

func (r Rule) validateNoncurrentExpiration() error {
	return r.NoncurrentVersionExpiration.Validate()
}

func (r Rule) validatePrefixAndFilter() error {
	// In the now deprecated PutBucketLifecycle API, Rule had a mandatory Prefix element and there existed no Filter field.
	// See https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycle.html
	// In the newer PutBucketLifecycleConfiguration API, Rule has a prefix field that is deprecated, and there exists an optional
	// Filter field, and within it, an optional Prefix field.
	// See https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html
	// A valid rule could be a pre-existing one created using the now deprecated PutBucketLifecycle.
	// Or, a valid rule could also be either a pre-existing or a new rule that is created using PutBucketLifecycleConfiguration.
	// Prefix validation below may check that either Rule.Prefix or Rule.Filter.Prefix exist but not both.
	// Here, we assume the pre-existing rule created using PutBucketLifecycle API is already valid and won't fail the validation if Rule.Prefix is empty.

	if r.Prefix.set && !r.Filter.IsEmpty() && r.Filter.Prefix.set {
		return errXMLNotWellFormed
	}

	if r.Filter.set {
		return r.Filter.Validate()
	}
	return nil
}

func (r Rule) validateTransition() error {
	return r.Transition.Validate()
}

func (r Rule) validateNoncurrentTransition() error {
	return r.NoncurrentVersionTransition.Validate()
}

// GetPrefix - a rule can either have prefix under <rule></rule>, <filter></filter>
// or under <filter><and></and></filter>. This method returns the prefix from the
// location where it is available.
func (r Rule) GetPrefix() string {
	if p := r.Prefix.String(); p != "" {
		return p
	}
	if p := r.Filter.Prefix.String(); p != "" {
		return p
	}
	if p := r.Filter.And.Prefix.String(); p != "" {
		return p
	}
	return ""
}

// Tags - a rule can either have tag under <filter></filter> or under
// <filter><and></and></filter>. This method returns all the tags from the
// rule in the format tag1=value1&tag2=value2
func (r Rule) Tags() string {
	if !r.Filter.Tag.IsEmpty() {
		return r.Filter.Tag.String()
	}
	if len(r.Filter.And.Tags) != 0 {
		var buf bytes.Buffer
		for _, t := range r.Filter.And.Tags {
			if buf.Len() > 0 {
				buf.WriteString("&")
			}
			buf.WriteString(t.String())
		}
		return buf.String()
	}
	return ""
}

// Validate - validates the rule element
func (r Rule) Validate() error {
	if err := r.validateID(); err != nil {
		return err
	}
	if err := r.validateStatus(); err != nil {
		return err
	}
	if err := r.validateExpiration(); err != nil {
		return err
	}
	if err := r.validateNoncurrentExpiration(); err != nil {
		return err
	}
	if err := r.validatePrefixAndFilter(); err != nil {
		return err
	}
	if err := r.validateTransition(); err != nil {
		return err
	}
	if err := r.validateNoncurrentTransition(); err != nil {
		return err
	}
	if (!r.Filter.Tag.IsEmpty() || len(r.Filter.And.Tags) != 0) && !r.DelMarkerExpiration.Empty() {
		return errInvalidRuleDelMarkerExpiration
	}
	if !r.Expiration.set && !r.Transition.set && !r.NoncurrentVersionExpiration.set && !r.NoncurrentVersionTransition.set && r.DelMarkerExpiration.Empty() {
		return errXMLNotWellFormed
	}
	return nil
}

// CloneNonTransition - returns a clone of the object containing non transition rules
func (r Rule) CloneNonTransition() Rule {
	return Rule{
		XMLName:                     r.XMLName,
		ID:                          r.ID,
		Status:                      r.Status,
		Filter:                      r.Filter,
		Prefix:                      r.Prefix,
		Expiration:                  r.Expiration,
		NoncurrentVersionExpiration: r.NoncurrentVersionExpiration,
	}
}
