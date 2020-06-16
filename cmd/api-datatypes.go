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
)

// DeletedObject objects deleted
type DeletedObject struct {
	DeleteMarker          bool   `xml:"DeleteMarker"`
	DeleteMarkerVersionID string `xml:"DeleteMarkerVersionId,omitempty"`
	ObjectName            string `xml:"Key,omitempty"`
	VersionID             string `xml:"VersionId,omitempty"`
}

// ObjectToDelete carries key name for the object to delete.
type ObjectToDelete struct {
	ObjectName string `xml:"Key"`
	VersionID  string `xml:"VersionId"`
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
