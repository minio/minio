/*
 * MinIO Cloud Storage, (C) 2015, 2016 MinIO, Inc.
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

package cmd

import (
	"encoding/xml"
	"time"
)

// DeletedObject objects deleted
type DeletedObject struct {
	DeleteMarkerMTime             DeleteMarkerMTime      `xml:"DeleteMarkerMTime,omitempty"`
	VersionPurgeStatus            VersionPurgeStatusType `xml:"VersionPurgeStatus,omitempty"`
	DeleteMarkerVersionID         string                 `xml:"DeleteMarkerVersionId,omitempty"`
	ObjectName                    string                 `xml:"Key,omitempty"`
	VersionID                     string                 `xml:"VersionId,omitempty"`
	DeleteMarkerReplicationStatus string                 `xml:"DeleteMarkerReplicationStatus,omitempty"`
	PurgeTransitioned             string                 `xml:"PurgeTransitioned,omitempty"`
	DeleteMarker                  bool                   `xml:"DeleteMarker,omitempty"` // MTime of DeleteMarker on source that needs to be propagated to replica
	// PurgeTransitioned is nonempty if object is in transition tier
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
	// PurgeTransitioned is nonempty if object is in transition tier
	PurgeTransitioned string `xml:"PurgeTransitioned"`
}

// createBucketConfiguration container for bucket configuration request from client.
// Used for parsing the location from the request body for Makebucket.
type createBucketLocationConfiguration struct {
	XMLName  xml.Name `xml:"CreateBucketConfiguration" json:"-"`
	Location string   `xml:"LocationConstraint"`
}

// DeleteObjectsRequest - xml carrying the object key names which needs to be deleted.
type DeleteObjectsRequest struct {
	Objects []ObjectToDelete `xml:"Object"` // List of objects to be deleted

	Quiet bool // Element to enable quiet mode for the request
}
