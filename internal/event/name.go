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

package event

import (
	"encoding/json"
	"encoding/xml"
)

// Name - event type enum.
// Refer http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html#notification-how-to-event-types-and-destinations
// for most basic values we have since extend this and its not really much applicable other than a reference point.
// "s3:Replication:OperationCompletedReplication" is a MinIO extension.
type Name int

// Values of event Name
const (
	// Single event types (does not require expansion)

	ObjectAccessedGet Name = 1 + iota
	ObjectAccessedGetRetention
	ObjectAccessedGetLegalHold
	ObjectAccessedHead
	ObjectAccessedAttributes
	ObjectCreatedCompleteMultipartUpload
	ObjectCreatedCopy
	ObjectCreatedPost
	ObjectCreatedPut
	ObjectCreatedPutRetention
	ObjectCreatedPutLegalHold
	ObjectCreatedPutTagging
	ObjectCreatedDeleteTagging
	ObjectRemovedDelete
	ObjectRemovedDeleteMarkerCreated
	ObjectRemovedDeleteAllVersions
	ObjectRemovedNoOP
	BucketCreated
	BucketRemoved
	ObjectReplicationFailed
	ObjectReplicationComplete
	ObjectReplicationMissedThreshold
	ObjectReplicationReplicatedAfterThreshold
	ObjectReplicationNotTracked
	ObjectRestorePost
	ObjectRestoreCompleted
	ObjectTransitionFailed
	ObjectTransitionComplete
	ObjectManyVersions
	ObjectLargeVersions
	PrefixManyFolders
	ILMDelMarkerExpirationDelete

	objectSingleTypesEnd
	// Start Compound types that require expansion:

	ObjectAccessedAll
	ObjectCreatedAll
	ObjectRemovedAll
	ObjectReplicationAll
	ObjectRestoreAll
	ObjectTransitionAll
	ObjectScannerAll
	Everything
)

// The number of single names should not exceed 64.
// This will break masking. Use bit 63 as extension.
var _ = uint64(1 << objectSingleTypesEnd)

// Expand - returns expanded values of abbreviated event type.
func (name Name) Expand() []Name {
	switch name {
	case ObjectAccessedAll:
		return []Name{
			ObjectAccessedGet, ObjectAccessedHead,
			ObjectAccessedGetRetention, ObjectAccessedGetLegalHold, ObjectAccessedAttributes,
		}
	case ObjectCreatedAll:
		return []Name{
			ObjectCreatedCompleteMultipartUpload, ObjectCreatedCopy,
			ObjectCreatedPost, ObjectCreatedPut,
			ObjectCreatedPutRetention, ObjectCreatedPutLegalHold,
			ObjectCreatedPutTagging, ObjectCreatedDeleteTagging,
		}
	case ObjectRemovedAll:
		return []Name{
			ObjectRemovedDelete,
			ObjectRemovedDeleteMarkerCreated,
			ObjectRemovedNoOP,
			ObjectRemovedDeleteAllVersions,
		}
	case ObjectReplicationAll:
		return []Name{
			ObjectReplicationFailed,
			ObjectReplicationComplete,
			ObjectReplicationNotTracked,
			ObjectReplicationMissedThreshold,
			ObjectReplicationReplicatedAfterThreshold,
		}
	case ObjectRestoreAll:
		return []Name{
			ObjectRestorePost,
			ObjectRestoreCompleted,
		}
	case ObjectTransitionAll:
		return []Name{
			ObjectTransitionFailed,
			ObjectTransitionComplete,
		}
	case ObjectScannerAll:
		return []Name{
			ObjectManyVersions,
			ObjectLargeVersions,
			PrefixManyFolders,
		}
	case Everything:
		res := make([]Name, objectSingleTypesEnd-1)
		for i := range res {
			res[i] = Name(i + 1)
		}
		return res
	default:
		return []Name{name}
	}
}

// Mask returns the type as mask.
// Compound "All" types are expanded.
func (name Name) Mask() uint64 {
	if name < objectSingleTypesEnd {
		return 1 << (name - 1)
	}
	var mask uint64
	for _, n := range name.Expand() {
		mask |= 1 << (n - 1)
	}
	return mask
}

// String - returns string representation of event type.
func (name Name) String() string {
	switch name {
	case BucketCreated:
		return "s3:BucketCreated:*"
	case BucketRemoved:
		return "s3:BucketRemoved:*"
	case ObjectAccessedAll:
		return "s3:ObjectAccessed:*"
	case ObjectAccessedGet:
		return "s3:ObjectAccessed:Get"
	case ObjectAccessedGetRetention:
		return "s3:ObjectAccessed:GetRetention"
	case ObjectAccessedGetLegalHold:
		return "s3:ObjectAccessed:GetLegalHold"
	case ObjectAccessedHead:
		return "s3:ObjectAccessed:Head"
	case ObjectAccessedAttributes:
		return "s3:ObjectAccessed:Attributes"
	case ObjectCreatedAll:
		return "s3:ObjectCreated:*"
	case ObjectCreatedCompleteMultipartUpload:
		return "s3:ObjectCreated:CompleteMultipartUpload"
	case ObjectCreatedCopy:
		return "s3:ObjectCreated:Copy"
	case ObjectCreatedPost:
		return "s3:ObjectCreated:Post"
	case ObjectCreatedPut:
		return "s3:ObjectCreated:Put"
	case ObjectCreatedPutTagging:
		return "s3:ObjectCreated:PutTagging"
	case ObjectCreatedDeleteTagging:
		return "s3:ObjectCreated:DeleteTagging"
	case ObjectCreatedPutRetention:
		return "s3:ObjectCreated:PutRetention"
	case ObjectCreatedPutLegalHold:
		return "s3:ObjectCreated:PutLegalHold"
	case ObjectRemovedAll:
		return "s3:ObjectRemoved:*"
	case ObjectRemovedDelete:
		return "s3:ObjectRemoved:Delete"
	case ObjectRemovedDeleteMarkerCreated:
		return "s3:ObjectRemoved:DeleteMarkerCreated"
	case ObjectRemovedNoOP:
		return "s3:ObjectRemoved:NoOP"
	case ObjectRemovedDeleteAllVersions:
		return "s3:ObjectRemoved:DeleteAllVersions"
	case ILMDelMarkerExpirationDelete:
		return "s3:LifecycleDelMarkerExpiration:Delete"
	case ObjectReplicationAll:
		return "s3:Replication:*"
	case ObjectReplicationFailed:
		return "s3:Replication:OperationFailedReplication"
	case ObjectReplicationComplete:
		return "s3:Replication:OperationCompletedReplication"
	case ObjectReplicationNotTracked:
		return "s3:Replication:OperationNotTracked"
	case ObjectReplicationMissedThreshold:
		return "s3:Replication:OperationMissedThreshold"
	case ObjectReplicationReplicatedAfterThreshold:
		return "s3:Replication:OperationReplicatedAfterThreshold"
	case ObjectRestoreAll:
		return "s3:ObjectRestore:*"
	case ObjectRestorePost:
		return "s3:ObjectRestore:Post"
	case ObjectRestoreCompleted:
		return "s3:ObjectRestore:Completed"
	case ObjectTransitionAll:
		return "s3:ObjectTransition:*"
	case ObjectTransitionFailed:
		return "s3:ObjectTransition:Failed"
	case ObjectTransitionComplete:
		return "s3:ObjectTransition:Complete"
	case ObjectManyVersions:
		return "s3:Scanner:ManyVersions"
	case ObjectLargeVersions:
		return "s3:Scanner:LargeVersions"

	case PrefixManyFolders:
		return "s3:Scanner:BigPrefix"
	}

	return ""
}

// MarshalXML - encodes to XML data.
func (name Name) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return e.EncodeElement(name.String(), start)
}

// UnmarshalXML - decodes XML data.
func (name *Name) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string
	if err := d.DecodeElement(&s, &start); err != nil {
		return err
	}

	eventName, err := ParseName(s)
	if err != nil {
		return err
	}

	*name = eventName
	return nil
}

// MarshalJSON - encodes to JSON data.
func (name Name) MarshalJSON() ([]byte, error) {
	return json.Marshal(name.String())
}

// UnmarshalJSON - decodes JSON data.
func (name *Name) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	eventName, err := ParseName(s)
	if err != nil {
		return err
	}

	*name = eventName
	return nil
}

// ParseName - parses string to Name.
func ParseName(s string) (Name, error) {
	switch s {
	case "s3:BucketCreated:*":
		return BucketCreated, nil
	case "s3:BucketRemoved:*":
		return BucketRemoved, nil
	case "s3:ObjectAccessed:*":
		return ObjectAccessedAll, nil
	case "s3:ObjectAccessed:Get":
		return ObjectAccessedGet, nil
	case "s3:ObjectAccessed:GetRetention":
		return ObjectAccessedGetRetention, nil
	case "s3:ObjectAccessed:GetLegalHold":
		return ObjectAccessedGetLegalHold, nil
	case "s3:ObjectAccessed:Head":
		return ObjectAccessedHead, nil
	case "s3:ObjectAccessed:Attributes":
		return ObjectAccessedAttributes, nil
	case "s3:ObjectCreated:*":
		return ObjectCreatedAll, nil
	case "s3:ObjectCreated:CompleteMultipartUpload":
		return ObjectCreatedCompleteMultipartUpload, nil
	case "s3:ObjectCreated:Copy":
		return ObjectCreatedCopy, nil
	case "s3:ObjectCreated:Post":
		return ObjectCreatedPost, nil
	case "s3:ObjectCreated:Put":
		return ObjectCreatedPut, nil
	case "s3:ObjectCreated:PutRetention":
		return ObjectCreatedPutRetention, nil
	case "s3:ObjectCreated:PutLegalHold":
		return ObjectCreatedPutLegalHold, nil
	case "s3:ObjectCreated:PutTagging":
		return ObjectCreatedPutTagging, nil
	case "s3:ObjectCreated:DeleteTagging":
		return ObjectCreatedDeleteTagging, nil
	case "s3:ObjectRemoved:*":
		return ObjectRemovedAll, nil
	case "s3:ObjectRemoved:Delete":
		return ObjectRemovedDelete, nil
	case "s3:ObjectRemoved:DeleteMarkerCreated":
		return ObjectRemovedDeleteMarkerCreated, nil
	case "s3:ObjectRemoved:NoOP":
		return ObjectRemovedNoOP, nil
	case "s3:ObjectRemoved:DeleteAllVersions":
		return ObjectRemovedDeleteAllVersions, nil
	case "s3:LifecycleDelMarkerExpiration:Delete":
		return ILMDelMarkerExpirationDelete, nil
	case "s3:Replication:*":
		return ObjectReplicationAll, nil
	case "s3:Replication:OperationFailedReplication":
		return ObjectReplicationFailed, nil
	case "s3:Replication:OperationCompletedReplication":
		return ObjectReplicationComplete, nil
	case "s3:Replication:OperationMissedThreshold":
		return ObjectReplicationMissedThreshold, nil
	case "s3:Replication:OperationReplicatedAfterThreshold":
		return ObjectReplicationReplicatedAfterThreshold, nil
	case "s3:Replication:OperationNotTracked":
		return ObjectReplicationNotTracked, nil
	case "s3:ObjectRestore:*":
		return ObjectRestoreAll, nil
	case "s3:ObjectRestore:Post":
		return ObjectRestorePost, nil
	case "s3:ObjectRestore:Completed":
		return ObjectRestoreCompleted, nil
	case "s3:ObjectTransition:Failed":
		return ObjectTransitionFailed, nil
	case "s3:ObjectTransition:Complete":
		return ObjectTransitionComplete, nil
	case "s3:ObjectTransition:*":
		return ObjectTransitionAll, nil
	case "s3:Scanner:ManyVersions":
		return ObjectManyVersions, nil
	case "s3:Scanner:LargeVersions":
		return ObjectLargeVersions, nil
	case "s3:Scanner:BigPrefix":
		return PrefixManyFolders, nil
	default:
		return 0, &ErrInvalidEventName{s}
	}
}
