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

// Rule - a rule for lifecycle configuration.
type Rule struct {
	XMLName    xml.Name   `xml:"Rule"`
	ID         string     `xml:"ID,omitempty"`
	Status     string     `xml:"Status"`
	Filter     Filter     `xml:"Filter"`
	Expiration Expiration `xml:"Expiration,omitempty"`
	Transition Transition `xml:"Transition,omitempty"`
	// FIXME: add a type to catch unsupported AbortIncompleteMultipartUpload AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
	NoncurrentVersionExpiration NoncurrentVersionExpiration `xml:"NoncurrentVersionExpiration,omitempty"`
	NoncurrentVersionTransition NoncurrentVersionTransition `xml:"NoncurrentVersionTransition,omitempty"`
}

var (
	errInvalidRuleID           = errors.New("ID must be less than 255 characters")
	errEmptyRuleStatus         = errors.New("Status should not be empty")
	errInvalidRuleStatus       = errors.New("Status must be set to either Enabled or Disabled")
	errMissingExpirationAction = errors.New("No expiration action found")
)

// isIDValid - checks if ID is valid or not.
func (r Rule) validateID() error {
	// cannot be longer than 255 characters
	if len(string(r.ID)) > 255 {
		return errInvalidRuleID
	}
	return nil
}

// isStatusValid - checks if status is valid or not.
func (r Rule) validateStatus() error {
	// Status can't be empty
	if len(r.Status) == 0 {
		return errEmptyRuleStatus
	}

	// Status must be one of Enabled or Disabled
	if r.Status != "Enabled" && r.Status != "Disabled" {
		return errInvalidRuleStatus
	}
	return nil
}

func (r Rule) validateAction() error {
	if r.Expiration == (Expiration{}) {
		return errMissingExpirationAction
	}
	return nil
}

// Validate - validates the rule element
func (r Rule) Validate() error {
	if err := r.validateID(); err != nil {
		return err
	}
	if err := r.validateStatus(); err != nil {
		return err
	}
	if err := r.validateAction(); err != nil {
		return err
	}
	return nil
}
