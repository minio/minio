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

import (
	"encoding/xml"
	"strings"
	"time"
)

type filterRule struct {
	Name  string `xml:"FilterRuleName"`
	Value string
}

type keyFilter struct {
	FilterRules []filterRule `xml:"FilterRule"`
}

type notificationConfigFilter struct {
	Key keyFilter `xml:"S3Key"`
}

// Queue SQS configuration.
type queueConfig struct {
	Events   []string `xml:"Event"`
	Filter   notificationConfigFilter
	ID       string `xml:"Id"`
	QueueArn string `xml:"Queue"`
}

// Check - validates queue configuration and returns error if any.
func checkQueueConfig(qConfig queueConfig) APIErrorCode {
	// Check queue arn is valid.
	if s3Error := checkQueueArn(qConfig.QueueArn); s3Error != ErrNone {
		return s3Error
	}

	// Unmarshals QueueArn into structured object.
	sqsArn := unmarshalSqsArn(qConfig.QueueArn)
	if isAMQPQueue(sqsArn) { // Validate AMQP inputs.
		conn, dialURL, err := connectAMQPQueue(sqsArn.inputs)
		if err != nil {
			errorIf(err, "Unable to dial to AMQP server %s", dialURL)
			return ErrARNNotification
		}
		defer conn.Close()
	} else if isElasticQueue(sqsArn) { // Validate elasticsearch inputs.
		client, _, err := connectElasticQueue(sqsArn.inputs)
		if err != nil {
			errorIf(err, "Unable to dial to Elastic server %s", sqsArn.inputs)
			return ErrARNNotification
		}
		defer client.Stop()
	} // Support new queues here.

	// Check valid events.
	if s3Error := checkEvents(qConfig.Events); s3Error != ErrNone {
		return s3Error
	}

	// Success.
	return ErrNone
}

// Topic SNS configuration.
type topicConfig struct {
	Events   []string `xml:"Event"`
	Filter   notificationConfigFilter
	ID       string `xml:"Id"`
	TopicArn string `xml:"Topic"`
}

// Lambda function configuration.
type lambdaFuncConfig struct {
	Events            []string `xml:"Event"`
	Filter            notificationConfigFilter
	ID                string `xml:"Id"`
	LambdaFunctionArn string `xml:"CloudFunction"`
}

// Notification configuration.
type notificationConfig struct {
	XMLName              xml.Name           `xml:"NotificationConfiguration"`
	QueueConfigurations  []queueConfig      `xml:"QueueConfiguration"`
	TopicConfigurations  []topicConfig      `xml:"TopicConfiguration"`
	LambdaConfigurations []lambdaFuncConfig `xml:"CloudFunctionConfiguration"`
}

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

type s3BucketReference struct {
	Name          string   `json:"name"`
	OwnerIdentity identity `json:"ownerIdentity"`
	ARN           string   `json:"arn"`
}

type s3ObjectReference struct {
	Key       string `json:"key"`
	Size      int64  `json:"size,omitempty"`
	ETag      string `json:"eTag,omitempty"`
	VersionID string `json:"versionId,omitempty"`
	Sequencer string `json:"sequencer"`
}

type s3Reference struct {
	SchemaVersion   string            `json:"s3SchemaVersion"`
	ConfigurationID string            `json:"configurationId"`
	Bucket          s3BucketReference `json:"bucket"`
	Object          s3ObjectReference `json:"object"`
}

// NotificationEvent represents an Amazon an S3 event.
type NotificationEvent struct {
	EventVersion      string            `json:"eventVersion"`
	EventSource       string            `json:"eventSource"`
	AwsRegion         string            `json:"awsRegion"`
	EventTime         time.Time         `json:"eventTime"`
	EventName         string            `json:"eventName"`
	UserIdentity      identity          `json:"userIdentity"`
	RequestParameters map[string]string `json:"requestParameters"`
	ResponseElements  map[string]string `json:"responseElements"`
	S3                s3Reference       `json:"s3"`
}

func validateQueueConfigs(queueConfigs []queueConfig) APIErrorCode {
	for _, qConfig := range queueConfigs {
		if s3Error := checkQueueConfig(qConfig); s3Error != ErrNone {
			return s3Error
		}
	}
	return ErrNone
}

func validateNotificationConfig(nConfig notificationConfig) APIErrorCode {
	if s3Error := validateQueueConfigs(nConfig.QueueConfigurations); s3Error != ErrNone {
		return s3Error
	}
	// Add validation for other configurations.
	return ErrNone
}

type minioSqsArn struct {
	sqsType string
	inputs  []string
}

func (m minioSqsArn) String() string {
	return arnMinioSqs + serverConfig.GetRegion() + ":" + m.sqsType + strings.Join(m.inputs, ":")
}

func unmarshalSqsArn(queueArn string) (mSqs minioSqsArn) {
	sqsArnPrefix := arnMinioSqs + serverConfig.GetRegion() + ":"
	queueArn = strings.TrimPrefix(queueArn, sqsArnPrefix)
	mSqs = minioSqsArn{}
	switch {
	case strings.HasPrefix(queueArn, arnAmqpQueue):
		queueArn = strings.TrimPrefix(queueArn, arnAmqpQueue)
		mSqs.sqsType = arnAmqpQueue
		mSqs.inputs = strings.Split(queueArn, ":")
	case strings.HasPrefix(queueArn, arnElasticQueue):
		queueArn = strings.TrimPrefix(queueArn, arnElasticQueue)
		mSqs.sqsType = arnElasticQueue
		mSqs.inputs = strings.Split(queueArn, ":")
	}
	return mSqs
}

func enableQueues(queueConfigs []queueConfig) error {
	for _, qConfig := range queueConfigs {
		sqsArn := unmarshalSqsArn(qConfig.QueueArn)
		if isAMQPQueue(sqsArn) {
			if err := enableAMQPQueue(sqsArn.inputs); err != nil {
				return err
			}
		} else if isElasticQueue(sqsArn) {
			if err := enableElasticQueue(sqsArn.inputs); err != nil {
				return err
			}
		}
	}
	return nil
}

func enableNotification(nConfig notificationConfig) error {
	if err := enableQueues(nConfig.QueueConfigurations); err != nil {
		return err
	}
	// Enable for other configurations.
	return nil
}
