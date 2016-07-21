/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

//go:generate jsonenums -type=EventName -prefix=notifications-

import (
	"fmt"
	"net/url"
	"time"
)

// EventName is AWS S3 event type:
// http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
type EventName int

const (
	// ObjectCreatedPut is s3:ObjectCreated:Put
	ObjectCreatedPut EventName = iota
	// ObjectCreatedPost is s3:ObjectCreated:POst
	ObjectCreatedPost
	// ObjectCreatedCopy is s3:ObjectCreated:Post
	ObjectCreatedCopy
	// ObjectCreatedCompleteMultipartUpload is s3:ObjectCreated:CompleteMultipartUpload
	ObjectCreatedCompleteMultipartUpload
	// ObjectRemovedDelete is s3:ObjectRemoved:Delete
	ObjectRemovedDelete
	// ObjectRemovedDeleteMarkerCreated is s3:ObjectRemoved:DeleteMarkerCreated
	ObjectRemovedDeleteMarkerCreated
	// ReducedRedundancyLostObject is s3:ReducedRedundancyLostObject
	ReducedRedundancyLostObject
)

func (eventName EventName) String() string {
	switch eventName {
	case ObjectCreatedPut:
		return "s3:ObjectCreated:Put"
	case ObjectCreatedPost:
		return "s3:ObjectCreated:Post"
	case ObjectCreatedCopy:
		return "s3:ObjectCreated:Copy"
	case ObjectCreatedCompleteMultipartUpload:
		return "s3:ObjectCreated:CompleteMultipartUpload"
	case ObjectRemovedDelete:
		return "s3:ObjectRemoved:Delete"
	case ObjectRemovedDeleteMarkerCreated:
		return "s3:ObjectRemoved:DeleteMarkerCreated"
	case ReducedRedundancyLostObject:
		return "s3:ReducedRedundancyLostObject"
	default:
		return "s3:Unknown"
	}
}

type identity struct {
	PrincipalID string `json:"principalId"`
}

func defaultIdentity() identity {
	return identity{"minio"}
}

type s3bucketReference struct {
	Name          string   `json:"name"`
	OwnerIdentity identity `json:"ownerIdentity"`
	ARN           string   `json:"arn"`
}

type s3objectReference struct {
	Key       string  `json:"key"`
	Size      int64   `json:"size"`
	ETag      string  `json:"eTag"`
	VersionID *string `json:"versionId"`
	Sequencer *string `json:"sequencer"`
}

type s3reference struct {
	SchemaVersion   string            `json:"s3SchemaVersion"`
	ConfigurationID string            `json:"configurationId"`
	Bucket          s3bucketReference `json:"bucket"`
	Object          s3objectReference `json:"object"`
}

// NotificationEvent represents an Amazon an S3 event
type NotificationEvent struct {
	EventVersion      string            `json:"eventVersion"`
	EventSource       string            `json:"eventSource"`
	AwsRegion         string            `json:"awsRegion"`
	EventTime         time.Time         `json:"eventTime"`
	EventName         EventName         `json:"eventName"`
	UserIdentity      identity          `json:"userIdentity"`
	RequestParameters map[string]string `json:"requestParameters"`
	ResponseElements  map[string]string `json:"responseElements"`
	S3                s3reference       `json:"s3"`
}

// NotificationRecords encapsulates multiple events
type NotificationRecords struct {
	Records []*NotificationEvent `json:"Records"`
}

// NewNotificationEvent creates a new notification event
func NewNotificationEvent(fs ObjectLayer, eventName EventName, bucket, object string, md5 string) (event *NotificationEvent, err error) {
	objectInfo, err := fs.GetObjectInfo(bucket, object)
	if err != nil {
		return
	}
	region := "us-east-1"
	if serverConfig != nil {
		region = serverConfig.GetRegion()
	}
	sequencer := fmt.Sprintf("%X", time.Now().UnixNano())
	event = &NotificationEvent{
		EventVersion:      "2.0",
		EventSource:       "aws:s3",
		AwsRegion:         region,
		EventTime:         time.Now(),
		EventName:         eventName,
		UserIdentity:      defaultIdentity(),
		RequestParameters: make(map[string]string),
		ResponseElements:  make(map[string]string),
		S3: s3reference{
			SchemaVersion:   "1.0",
			ConfigurationID: "Config",
			Bucket:          s3bucketReference{Name: bucket, OwnerIdentity: defaultIdentity(), ARN: "arn:aws:s3:::" + bucket},
			Object:          s3objectReference{Key: url.QueryEscape(objectInfo.Name), Size: objectInfo.Size, ETag: md5, VersionID: nil, Sequencer: &sequencer},
		},
	}
	return event, nil
}
