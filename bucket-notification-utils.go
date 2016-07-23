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

import "strings"

// List of valid event types.
var suppportedEventTypes = map[string]struct{}{
	// Object created event types.
	"s3:ObjectCreated:*":                       {},
	"s3:ObjectCreated:Put":                     {},
	"s3:ObjectCreated:Post":                    {},
	"s3:ObjectCreated:Copy":                    {},
	"s3:ObjectCreated:CompleteMultipartUpload": {},
	// Object removed event types.
	"s3:ObjectRemoved:*":      {},
	"s3:ObjectRemoved:Delete": {},
}

// checkEvent - checks if an event is supported.
func checkEvent(event string) APIErrorCode {
	_, ok := suppportedEventTypes[event]
	if !ok {
		return ErrEventNotification
	}
	return ErrNone
}

// checkEvents - checks given list of events if all of them are valid.
// given if one of them is invalid, this function returns an error.
func checkEvents(events []string) APIErrorCode {
	for _, event := range events {
		if s3Error := checkEvent(event); s3Error != ErrNone {
			return s3Error
		}
	}
	return ErrNone
}

// checkQueueArn - check if the queue arn is valid.
func checkQueueArn(queueArn string) APIErrorCode {
	if !strings.HasPrefix(queueArn, minioSqs) {
		return ErrARNNotification
	}
	if !strings.HasPrefix(queueArn, minioSqs+serverConfig.GetRegion()+":") {
		return ErrRegionNotification
	}
	return ErrNone
}

// Check - validates queue configuration and returns error if any.
func checkQueueConfig(qConfig queueConfig) APIErrorCode {
	// Check queue arn is valid.
	if s3Error := checkQueueArn(qConfig.QueueArn); s3Error != ErrNone {
		return s3Error
	}

	// Unmarshals QueueArn into structured object.
	sqsArn := unmarshalSqsArn(qConfig.QueueArn)
	// Validate if sqsArn requested any of the known supported queues.
	if !isAMQPQueue(sqsArn) || !isElasticQueue(sqsArn) || !isRedisQueue(sqsArn) {
		return ErrARNNotification
	}

	// Check if valid events are set in queue config.
	if s3Error := checkEvents(qConfig.Events); s3Error != ErrNone {
		return s3Error
	}

	// Success.
	return ErrNone
}

// Validates all incoming queue configs, checkQueueConfig validates if the
// input fields for each queues is not malformed and has valid configuration
// information.  If validation fails bucket notifications are not enabled.
func validateQueueConfigs(queueConfigs []queueConfig) APIErrorCode {
	for _, qConfig := range queueConfigs {
		if s3Error := checkQueueConfig(qConfig); s3Error != ErrNone {
			return s3Error
		}
	}
	// Success.
	return ErrNone
}

// Validates all the bucket notification configuration for their validity,
// if one of the config is malformed or has invalid data it is rejected.
// Configuration is never applied partially.
func validateNotificationConfig(nConfig notificationConfig) APIErrorCode {
	if s3Error := validateQueueConfigs(nConfig.QueueConfigurations); s3Error != ErrNone {
		return s3Error
	}
	// Add validation for other configurations.
	return ErrNone
}

// Unmarshals input value of AWS ARN format into minioSqs object.
// Returned value represents minio sqs types, currently supported are
// - amqp
// - elasticsearch
func unmarshalSqsArn(queueArn string) (mSqs arnMinioSqs) {
	sqsType := strings.TrimPrefix(queueArn, minioSqs+serverConfig.GetRegion()+":")
	mSqs = arnMinioSqs{}
	switch sqsType {
	case queueTypeAMQP:
		mSqs.sqsType = queueTypeAMQP
	case queueTypeElastic:
		mSqs.sqsType = queueTypeElastic
	case queueTypeRedis:
		mSqs.sqsType = queueTypeRedis
	} // Add more cases here.
	return mSqs
}
