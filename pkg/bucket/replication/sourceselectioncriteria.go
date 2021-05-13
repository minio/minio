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
	"encoding/xml"
)

// ReplicaModifications specifies if replica modification sync is enabled
type ReplicaModifications struct {
	Status Status `xml:"Status" json:"Status"`
}

// SourceSelectionCriteria - specifies additional source selection criteria in ReplicationConfiguration.
type SourceSelectionCriteria struct {
	ReplicaModifications ReplicaModifications `xml:"ReplicaModifications" json:"ReplicaModifications"`
}

// IsValid - checks whether SourceSelectionCriteria is valid or not.
func (s SourceSelectionCriteria) IsValid() bool {
	return s.ReplicaModifications.Status == Enabled || s.ReplicaModifications.Status == Disabled
}

// Validate source selection criteria
func (s SourceSelectionCriteria) Validate() error {
	if (s == SourceSelectionCriteria{}) {
		return nil
	}
	if !s.IsValid() {
		return errInvalidSourceSelectionCriteria
	}
	return nil
}

// UnmarshalXML - decodes XML data.
func (s *SourceSelectionCriteria) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) (err error) {
	// Make subtype to avoid recursive UnmarshalXML().
	type sourceSelectionCriteria SourceSelectionCriteria
	ssc := sourceSelectionCriteria{}
	if err := dec.DecodeElement(&ssc, &start); err != nil {
		return err
	}
	if len(ssc.ReplicaModifications.Status) == 0 {
		ssc.ReplicaModifications.Status = Enabled
	}
	*s = SourceSelectionCriteria(ssc)
	return nil
}

// MarshalXML - encodes to XML data.
func (s SourceSelectionCriteria) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	if s.IsValid() {
		if err := e.EncodeElement(s.ReplicaModifications, xml.StartElement{Name: xml.Name{Local: "ReplicaModifications"}}); err != nil {
			return err
		}
	}
	return e.EncodeToken(xml.EndElement{Name: start.Name})
}
