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
	*s = SourceSelectionCriteria{
		ReplicaModifications: ReplicaModifications{
			Status: ssc.ReplicaModifications.Status,
		}}
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
