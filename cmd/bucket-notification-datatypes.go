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

package cmd

import (
	"encoding/xml"
	"errors"
)

// Represents the criteria for the filter rule.
type filterRule struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

// Collection of filter rules per service config.
type keyFilter struct {
	FilterRules []filterRule `xml:"FilterRule,omitempty"`
}

type filterStruct struct {
	Key keyFilter `xml:"S3Key,omitempty" json:"S3Key,omitempty"`
}

// ServiceConfig - Common elements of service notification.
type ServiceConfig struct {
	Events []string     `xml:"Event" json:"Event"`
	Filter filterStruct `xml:"Filter" json:"Filter"`
	ID     string       `xml:"Id" json:"Id"`
}

// Queue SQS configuration.
type queueConfig struct {
	ServiceConfig
	QueueARN string `xml:"Queue"`
}

// Topic SNS configuration, this is a compliance field not used by minio yet.
type topicConfig struct {
	ServiceConfig
	TopicARN string `xml:"Topic" json:"Topic"`
}

// Lambda function configuration, this is a compliance field not used by minio yet.
type lambdaConfig struct {
	ServiceConfig
	LambdaARN string `xml:"CloudFunction"`
}

// Notification configuration structure represents the XML format of
// notification configuration of buckets.
type notificationConfig struct {
	XMLName       xml.Name       `xml:"NotificationConfiguration"`
	QueueConfigs  []queueConfig  `xml:"QueueConfiguration"`
	LambdaConfigs []lambdaConfig `xml:"CloudFunctionConfiguration"`
	TopicConfigs  []topicConfig  `xml:"TopicConfiguration"`
}

// listenerConfig structure represents run-time notification
// configuration for live listeners
type listenerConfig struct {
	TopicConfig  topicConfig `json:"TopicConfiguration"`
	TargetServer string      `json:"TargetServer"`
}

// Internal error used to signal notifications not set.
var errNoSuchNotifications = errors.New("The specified bucket does not have bucket notifications")

// EventName is AWS S3 event type:
// http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
type EventName int

const (
	// ObjectCreatedPut is s3:ObjectCreated:Put
	ObjectCreatedPut EventName = iota
	// ObjectCreatedPost is s3:ObjectCreated:Post
	ObjectCreatedPost
	// ObjectCreatedCopy is s3:ObjectCreated:Copy
	ObjectCreatedCopy
	// ObjectCreatedCompleteMultipartUpload is s3:ObjectCreated:CompleteMultipartUpload
	ObjectCreatedCompleteMultipartUpload
	// ObjectRemovedDelete is s3:ObjectRemoved:Delete
	ObjectRemovedDelete
	// ObjectAccessedGet is s3:ObjectAccessed:Get
	ObjectAccessedGet
	// ObjectAccessedHead is s3:ObjectAccessed:Head
	ObjectAccessedHead
)

// Stringer interface for event name.
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
	case ObjectAccessedGet:
		return "s3:ObjectAccessed:Get"
	case ObjectAccessedHead:
		return "s3:ObjectAccessed:Head"
	default:
		return "s3:Unknown"
	}
}

// Indentity represents the accessKey who caused the event.
type identity struct {
	PrincipalID string `json:"principalId"`
}

// Notification event bucket metadata.
type bucketMeta struct {
	Name          string   `json:"name"`
	OwnerIdentity identity `json:"ownerIdentity"`
	ARN           string   `json:"arn"`
}

// Notification event object metadata.
type objectMeta struct {
	Key          string            `json:"key"`
	Size         int64             `json:"size,omitempty"`
	ETag         string            `json:"eTag,omitempty"`
	ContentType  string            `json:"contentType,omitempty"`
	UserMetadata map[string]string `json:"userMetadata,omitempty"`
	VersionID    string            `json:"versionId,omitempty"`
	Sequencer    string            `json:"sequencer"`
}

const (
	// Event schema version number defaulting to the value in S3 spec.
	// ref: http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
	eventSchemaVersion = "1.0"

	// Default ID found in bucket notification configuration.
	// ref: http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
	eventConfigID = "Config"
)

const (
	// Response element origin endpoint key.
	responseOriginEndpointKey = "x-minio-origin-endpoint"
)

// Notification event server specific metadata.
type eventMeta struct {
	SchemaVersion   string     `json:"s3SchemaVersion"`
	ConfigurationID string     `json:"configurationId"`
	Bucket          bucketMeta `json:"bucket"`
	Object          objectMeta `json:"object"`
}

const (
	// Event source static value defaulting to the value in S3 spec.
	// ref: http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
	eventSource = "aws:s3"

	// Event version number defaulting to the value in S3 spec.
	// ref: http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
	eventVersion = "2.0"
)

// sourceInfo represents information on the client that triggered the
// event notification.
type sourceInfo struct {
	Host      string `json:"host"`
	Port      string `json:"port"`
	UserAgent string `json:"userAgent"`
}

// NotificationEvent represents an Amazon an S3 bucket notification event.
type NotificationEvent struct {
	EventVersion      string            `json:"eventVersion"`
	EventSource       string            `json:"eventSource"`
	AwsRegion         string            `json:"awsRegion"`
	EventTime         string            `json:"eventTime"`
	EventName         string            `json:"eventName"`
	UserIdentity      identity          `json:"userIdentity"`
	RequestParameters map[string]string `json:"requestParameters"`
	ResponseElements  map[string]string `json:"responseElements"`
	S3                eventMeta         `json:"s3"`
	Source            sourceInfo        `json:"source"`
}

// Represents the minio sqs type and account id's.
type arnSQS struct {
	Type      string
	AccountID string
}

// Stringer for constructing AWS ARN compatible string.
func (m arnSQS) String() string {
	return minioSqs + globalServerConfig.GetRegion() + ":" + m.AccountID + ":" + m.Type
}
