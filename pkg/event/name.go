/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package event

import (
	"encoding/json"
	"encoding/xml"
)

// Name - event type enum.
// Refer http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html#notification-how-to-event-types-and-destinations
type Name int

// Values of Name
const (
	ObjectAccessedAll Name = 1 + iota
	ObjectAccessedGet
	ObjectAccessedGetRetention
	ObjectAccessedGetLegalHold
	ObjectAccessedHead
	ObjectCreatedAll
	ObjectCreatedCompleteMultipartUpload
	ObjectCreatedCopy
	ObjectCreatedPost
	ObjectCreatedPut
	ObjectCreatedPutRetention
	ObjectCreatedPutLegalHold
	ObjectRemovedAll
	ObjectRemovedDelete
	ObjectRemovedDeleteMarkerCreated
)

// Expand - returns expanded values of abbreviated event type.
func (name Name) Expand() []Name {
	switch name {
	case ObjectAccessedAll:
		return []Name{ObjectAccessedGet, ObjectAccessedHead, ObjectAccessedGetRetention, ObjectAccessedGetLegalHold}
	case ObjectCreatedAll:
		return []Name{ObjectCreatedCompleteMultipartUpload, ObjectCreatedCopy, ObjectCreatedPost, ObjectCreatedPut, ObjectCreatedPutRetention, ObjectCreatedPutLegalHold}
	case ObjectRemovedAll:
		return []Name{ObjectRemovedDelete}
	default:
		return []Name{name}
	}
}

// String - returns string representation of event type.
func (name Name) String() string {
	switch name {
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
	case "s3:ObjectRemoved:*":
		return ObjectRemovedAll, nil
	case "s3:ObjectRemoved:Delete":
		return ObjectRemovedDelete, nil
	case "s3:ObjectRemoved:DeleteMarkerCreated":
		return ObjectRemovedDeleteMarkerCreated, nil
	default:
		return 0, &ErrInvalidEventName{s}
	}
}
