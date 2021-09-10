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

package cmd

import (
	"encoding/xml"
	"time"
)

// DeletedObject objects deleted
type DeletedObject struct {
	DeleteMarker          bool   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionID string `xml:"DeleteMarkerVersionId,omitempty"`
	ObjectName            string `xml:"Key,omitempty"`
	VersionID             string `xml:"VersionId,omitempty"`

	// MinIO extensions to support delete marker replication
	// Replication status of DeleteMarker
	DeleteMarkerReplicationStatus string `xml:"DeleteMarkerReplicationStatus,omitempty"`
	// MTime of DeleteMarker on source that needs to be propagated to replica
	DeleteMarkerMTime DeleteMarkerMTime `xml:"DeleteMarkerMTime,omitempty"`
	// Status of versioned delete (of object or DeleteMarker)
	VersionPurgeStatus VersionPurgeStatusType `xml:"VersionPurgeStatus,omitempty"`
}

// DeleteMarkerMTime is an embedded type containing time.Time for XML marshal
type DeleteMarkerMTime struct {
	time.Time
}

// MarshalXML encodes expiration date if it is non-zero and encodes
// empty string otherwise
func (t DeleteMarkerMTime) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if t.Time.IsZero() {
		return nil
	}
	return e.EncodeElement(t.Time.Format(time.RFC3339), startElement)
}

// ObjectToDelete carries key name for the object to delete.
type ObjectToDelete struct {
	ObjectName string `xml:"Key"`
	VersionID  string `xml:"VersionId"`
	// Replication status of DeleteMarker
	DeleteMarkerReplicationStatus string `xml:"DeleteMarkerReplicationStatus"`
	// Status of versioned delete (of object or DeleteMarker)
	VersionPurgeStatus VersionPurgeStatusType `xml:"VersionPurgeStatus"`
	// Version ID of delete marker
	DeleteMarkerVersionID string `xml:"DeleteMarkerVersionId"`
}

// createBucketConfiguration container for bucket configuration request from client.
// Used for parsing the location from the request body for Makebucket.
type createBucketLocationConfiguration struct {
	XMLName  xml.Name `xml:"CreateBucketConfiguration" json:"-"`
	Location string   `xml:"LocationConstraint"`
}

// DeleteObjectsRequest - xml carrying the object key names which needs to be deleted.
type DeleteObjectsRequest struct {
	// Element to enable quiet mode for the request
	Quiet bool
	// List of objects to be deleted
	Objects []ObjectToDelete `xml:"Object"`
}
