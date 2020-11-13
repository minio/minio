/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package replication

import (
	"bytes"
	"encoding/xml"
)

// Status represents Enabled/Disabled status
type Status string

// Supported status types
const (
	Enabled  Status = "Enabled"
	Disabled Status = "Disabled"
)

// DeleteMarkerReplication - whether delete markers are replicated - https://docs.aws.amazon.com/AmazonS3/latest/dev/replication-add-config.html
type DeleteMarkerReplication struct {
	Status Status `xml:"Status"` // should be set to "Disabled" by default
}

// IsEmpty returns true if DeleteMarkerReplication is not set
func (d DeleteMarkerReplication) IsEmpty() bool {
	return len(d.Status) == 0
}

// Validate validates whether the status is disabled.
func (d DeleteMarkerReplication) Validate() error {
	if d.IsEmpty() {
		return errDeleteMarkerReplicationMissing
	}
	if d.Status != Disabled {
		return errInvalidDeleteMarkerReplicationStatus
	}
	return nil
}

// Rule - a rule for replication configuration.
type Rule struct {
	XMLName                 xml.Name                `xml:"Rule" json:"Rule"`
	ID                      string                  `xml:"ID,omitempty" json:"ID,omitempty"`
	Status                  Status                  `xml:"Status" json:"Status"`
	Priority                int                     `xml:"Priority" json:"Priority"`
	DeleteMarkerReplication DeleteMarkerReplication `xml:"DeleteMarkerReplication" json:"DeleteMarkerReplication"`
	Destination             Destination             `xml:"Destination" json:"Destination"`
	Filter                  Filter                  `xml:"Filter" json:"Filter"`
}

var (
	errInvalidRuleID                        = Errorf("ID must be less than 255 characters")
	errEmptyRuleStatus                      = Errorf("Status should not be empty")
	errInvalidRuleStatus                    = Errorf("Status must be set to either Enabled or Disabled")
	errDeleteMarkerReplicationMissing       = Errorf("DeleteMarkerReplication must be specified")
	errPriorityMissing                      = Errorf("Priority must be specified")
	errInvalidDeleteMarkerReplicationStatus = Errorf("Delete marker replication is currently not supported")
	errDestinationSourceIdentical           = Errorf("Destination bucket cannot be the same as the source bucket.")
)

// validateID - checks if ID is valid or not.
func (r Rule) validateID() error {
	// cannot be longer than 255 characters
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

func (r Rule) validateFilter() error {
	if err := r.Filter.Validate(); err != nil {
		return err
	}
	return nil
}

// Prefix - a rule can either have prefix under <filter></filter> or under
// <filter><and></and></filter>. This method returns the prefix from the
// location where it is available
func (r Rule) Prefix() string {
	if r.Filter.Prefix != "" {
		return r.Filter.Prefix
	}
	return r.Filter.And.Prefix
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
func (r Rule) Validate(bucket string, sameTarget bool) error {
	if err := r.validateID(); err != nil {
		return err
	}
	if err := r.validateStatus(); err != nil {
		return err
	}
	if err := r.validateFilter(); err != nil {
		return err
	}
	if err := r.DeleteMarkerReplication.Validate(); err != nil {
		return err
	}
	if r.Priority < 0 {
		return errPriorityMissing
	}
	if r.Destination.Bucket == bucket && sameTarget {
		return errDestinationSourceIdentical
	}
	return nil
}
