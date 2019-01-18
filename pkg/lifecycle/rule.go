/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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
	"fmt"
)

// Rule - a rule for lifecycle configuration.
type Rule struct {
	XMLName    xml.Name   `xml:"Rule"`
	ID         string     `xml:"ID,omitempty"`
	Status     string     `xml:"Status"`
	Filter     Filter     `xml:"Filter"`
	Expiration Expiration `xml:"Expiration,omitempty"`
	Transition Transition `xml:"Transition,omitempty"`
	//AbortIncompleteMultipartUpload AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
	NoncurrentVersionExpiration NoncurrentVersionExpiration `xml:"NoncurrentVersionExpiration,omitempty"`
	NoncurrentVersionTransition NoncurrentVersionTransition `xml:"NoncurrentVersionTransition,omitempty"`
}

// isIDValid - checks if ID is valid or not.
func (r Rule) isIDValid() error {
	// cannot be longer than 255 characters
	if len(string(r.ID)) > 255 {
		return fmt.Errorf("ID must be less than 255 characters")
	}
	return nil
}

// isStatusValid - checks if status is valid or not.
func (r Rule) isStatusValid() error {
	// can be only Enabled or Disabled
	if r.Status != "Enabled" && r.Status != "Disabled" {
		return fmt.Errorf("Status must be set to either %s or %s", "Enabled", "Disabled")
	}
	if len(r.Status) == 0 {
		return fmt.Errorf("Status should not be empty")
	}
	return nil
}

func (r Rule) isAtleastOneActionPresent() error {
	if r.NoncurrentVersionTransition == (NoncurrentVersionTransition{}) && r.NoncurrentVersionExpiration == (NoncurrentVersionExpiration{}) &&
		r.Transition == (Transition{}) && r.Expiration == (Expiration{}) /*&& r.AbortIncompleteMultipartUpload == (AbortIncompleteMultipartUpload{}*/ {
		return fmt.Errorf("At least one action should be present inside Rule tag")
	}
	return nil
}

func (r Rule) isTransitionActionPresent() bool {
	if r.Transition == (Transition{}) {
		return false
	}
	return true
}

func (r Rule) isExpirationActionPresent() bool {
	if r.Expiration == (Expiration{}) {
		return false
	}
	return true
}

// Validate - validates the rule element
func (r Rule) Validate() error {
	if err := r.isIDValid(); err != nil {
		return err
	}
	if err := r.isStatusValid(); err != nil {
		return err
	}
	if err := r.isAtleastOneActionPresent(); err != nil {
		return err
	}
	if err := r.Filter.Validate(); err != nil {
		return err
	}
	if err := r.Expiration.Validate(); err != nil {
		return err
	}
	if err := r.Transition.Validate(); err != nil {
		return err
	}
	// if err := r.AbortIncompleteMultipartUpload.Validate(); err != nil {
	// 	return err
	// }
	if err := r.NoncurrentVersionExpiration.Validate(); err != nil {
		return err
	}
	if err := r.NoncurrentVersionTransition.Validate(); err != nil {
		return err
	}
	return nil
}
