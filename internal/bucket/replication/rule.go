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
	if d.Status != Disabled && d.Status != Enabled {
		return errInvalidDeleteMarkerReplicationStatus
	}
	return nil
}

// DeleteReplication - whether versioned deletes are replicated - this is a MinIO only
// extension.
type DeleteReplication struct {
	Status Status `xml:"Status"` // should be set to "Disabled" by default
}

// IsEmpty returns true if DeleteReplication is not set
func (d DeleteReplication) IsEmpty() bool {
	return len(d.Status) == 0
}

// Validate validates whether the status is disabled.
func (d DeleteReplication) Validate() error {
	if d.IsEmpty() {
		return errDeleteReplicationMissing
	}
	if d.Status != Disabled && d.Status != Enabled {
		return errInvalidDeleteReplicationStatus
	}
	return nil
}

// UnmarshalXML - decodes XML data.
func (d *DeleteReplication) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) (err error) {
	// Make subtype to avoid recursive UnmarshalXML().
	type deleteReplication DeleteReplication
	drep := deleteReplication{}

	if err := dec.DecodeElement(&drep, &start); err != nil {
		return err
	}
	if len(drep.Status) == 0 {
		drep.Status = Disabled
	}
	d.Status = drep.Status
	return nil
}

// ExistingObjectReplication - whether existing object replication is enabled
type ExistingObjectReplication struct {
	Status Status `xml:"Status"` // should be set to "Disabled" by default
}

// IsEmpty returns true if ExistingObjectReplication is not set
func (e ExistingObjectReplication) IsEmpty() bool {
	return len(e.Status) == 0
}

// Validate validates whether the status is disabled.
func (e ExistingObjectReplication) Validate() error {
	if e.IsEmpty() {
		return nil
	}
	if e.Status != Disabled && e.Status != Enabled {
		return errInvalidExistingObjectReplicationStatus
	}
	return nil
}

// UnmarshalXML - decodes XML data. Default to Disabled unless specified
func (e *ExistingObjectReplication) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) (err error) {
	// Make subtype to avoid recursive UnmarshalXML().
	type existingObjectReplication ExistingObjectReplication
	erep := existingObjectReplication{}

	if err := dec.DecodeElement(&erep, &start); err != nil {
		return err
	}
	if len(erep.Status) == 0 {
		erep.Status = Disabled
	}
	e.Status = erep.Status
	return nil
}

// Rule - a rule for replication configuration.
type Rule struct {
	XMLName                 xml.Name                `xml:"Rule" json:"Rule"`
	ID                      string                  `xml:"ID,omitempty" json:"ID,omitempty"`
	Status                  Status                  `xml:"Status" json:"Status"`
	Priority                int                     `xml:"Priority" json:"Priority"`
	DeleteMarkerReplication DeleteMarkerReplication `xml:"DeleteMarkerReplication" json:"DeleteMarkerReplication"`
	// MinIO extension to replicate versioned deletes
	DeleteReplication         DeleteReplication         `xml:"DeleteReplication" json:"DeleteReplication"`
	Destination               Destination               `xml:"Destination" json:"Destination"`
	SourceSelectionCriteria   SourceSelectionCriteria   `xml:"SourceSelectionCriteria" json:"SourceSelectionCriteria"`
	Filter                    Filter                    `xml:"Filter" json:"Filter"`
	ExistingObjectReplication ExistingObjectReplication `xml:"ExistingObjectReplication,omitempty" json:"ExistingObjectReplication"`
}

var (
	errInvalidRuleID                          = Errorf("ID must be less than 255 characters")
	errEmptyRuleStatus                        = Errorf("Status should not be empty")
	errInvalidRuleStatus                      = Errorf("Status must be set to either Enabled or Disabled")
	errDeleteMarkerReplicationMissing         = Errorf("DeleteMarkerReplication must be specified")
	errPriorityMissing                        = Errorf("Priority must be specified")
	errInvalidDeleteMarkerReplicationStatus   = Errorf("Delete marker replication status is invalid")
	errDestinationSourceIdentical             = Errorf("Destination bucket cannot be the same as the source bucket.")
	errDeleteReplicationMissing               = Errorf("Delete replication must be specified")
	errInvalidDeleteReplicationStatus         = Errorf("Delete replication is either enable|disable")
	errInvalidExistingObjectReplicationStatus = Errorf("Existing object replication status is invalid")
	errTagsDeleteMarkerReplicationDisallowed  = Errorf("Delete marker replication is not supported if any Tag filter is specified")
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
	return r.Filter.Validate()
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
	if err := r.DeleteReplication.Validate(); err != nil {
		return err
	}
	if err := r.SourceSelectionCriteria.Validate(); err != nil {
		return err
	}

	if r.Priority < 0 {
		return errPriorityMissing
	}
	if r.Destination.Bucket == bucket && sameTarget {
		return errDestinationSourceIdentical
	}
	if !r.Filter.Tag.IsEmpty() && (r.DeleteMarkerReplication.Status == Enabled) {
		return errTagsDeleteMarkerReplicationDisallowed
	}
	return r.ExistingObjectReplication.Validate()
}

// MetadataReplicate  returns true if object is not a replica or in the case of replicas,
// replica modification sync is enabled.
func (r Rule) MetadataReplicate(obj ObjectOpts) bool {
	if !obj.Replica {
		return true
	}
	return obj.Replica && r.SourceSelectionCriteria.ReplicaModifications.Status == Enabled
}
