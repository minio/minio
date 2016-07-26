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
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
)

const (
	minioSqs = "arn:minio:sqs:"
	// Static string indicating queue type 'amqp'.
	queueTypeAMQP = "1:amqp"
	// Static string indicating queue type 'elasticsearch'.
	queueTypeElastic = "1:elasticsearch"
	// Static string indicating queue type 'redis'.
	queueTypeRedis = "1:redis"
)

// Returns true if queueArn is for an AMQP queue.
func isAMQPQueue(sqsArn arnMinioSqs) bool {
	if sqsArn.sqsType != queueTypeAMQP {
		return false
	}
	amqpL := serverConfig.GetAMQPLogger()
	// Connect to amqp server to validate.
	amqpC, err := dialAMQP(amqpL)
	if err != nil {
		errorIf(err, "Unable to connect to amqp service. %#v", amqpL)
		return false
	}
	defer amqpC.Close()
	return true
}

// Returns true if queueArn is for an Redis queue.
func isRedisQueue(sqsArn arnMinioSqs) bool {
	if sqsArn.sqsType != queueTypeRedis {
		return false
	}
	rLogger := serverConfig.GetRedisLogger()
	// Connect to redis server to validate.
	rPool, err := dialRedis(rLogger)
	if err != nil {
		errorIf(err, "Unable to connect to redis service. %#v", rLogger)
		return false
	}
	defer rPool.Close()
	return true
}

// Returns true if queueArn is for an ElasticSearch queue.
func isElasticQueue(sqsArn arnMinioSqs) bool {
	if sqsArn.sqsType != queueTypeElastic {
		return false
	}
	esLogger := serverConfig.GetElasticSearchLogger()
	elasticC, err := dialElastic(esLogger)
	if err != nil {
		errorIf(err, "Unable to connect to elasticsearch service %#v", esLogger)
		return false
	}
	defer elasticC.Stop()
	return true
}

// Match function matches wild cards in 'pattern' for events.
func eventMatch(eventType EventName, events []string) (ok bool) {
	for _, event := range events {
		ok = wildCardMatch(event, eventType.String())
		if ok {
			break
		}
	}
	return ok
}

// Filter rule match, matches an object against the filter rules.
func filterRuleMatch(object string, frs []filterRule) bool {
	var prefixMatch, suffixMatch = true, true
	for _, fr := range frs {
		if isValidFilterNamePrefix(fr.Name) {
			prefixMatch = strings.HasPrefix(object, fr.Value)
		} else if isValidFilterNameSuffix(fr.Name) {
			suffixMatch = strings.HasSuffix(object, fr.Value)
		}
	}
	return prefixMatch && suffixMatch
}

// NotifyObjectCreatedEvent - notifies a new 's3:ObjectCreated' event.
// List of events reported through this function are
//  - s3:ObjectCreated:Put
//  - s3:ObjectCreated:Post
//  - s3:ObjectCreated:Copy
//  - s3:ObjectCreated:CompleteMultipartUpload
func notifyObjectCreatedEvent(nConfig notificationConfig, eventType EventName, bucket string, object string, etag string, size int64) {
	/// Construct a new object created event.
	region := serverConfig.GetRegion()
	tnow := time.Now().UTC()
	sequencer := fmt.Sprintf("%X", tnow.UnixNano())
	// Following blocks fills in all the necessary details of s3 event message structure.
	// http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
	events := []*NotificationEvent{
		{
			EventVersion:      "2.0",
			EventSource:       "aws:s3",
			AwsRegion:         region,
			EventTime:         tnow.Format(iso8601Format),
			EventName:         eventType.String(),
			UserIdentity:      defaultIdentity(),
			RequestParameters: map[string]string{},
			ResponseElements:  map[string]string{},
			S3: eventMeta{
				SchemaVersion:   "1.0",
				ConfigurationID: "Config",
				Bucket: bucketMeta{
					Name:          bucket,
					OwnerIdentity: defaultIdentity(),
					ARN:           "arn:aws:s3:::" + bucket,
				},
				Object: objectMeta{
					Key:       url.QueryEscape(object),
					ETag:      etag,
					Size:      size,
					Sequencer: sequencer,
				},
			},
		},
	}
	// Notify to all the configured queues.
	for _, qConfig := range nConfig.QueueConfigurations {
		ruleMatch := filterRuleMatch(object, qConfig.Filter.Key.FilterRules)
		if eventMatch(eventType, qConfig.Events) && ruleMatch {
			log.WithFields(logrus.Fields{
				"Records": events,
			}).Info()
		}
	}
}

// NotifyObjectRemovedEvent - notifies a new 's3:ObjectRemoved' event.
// List of events reported through this function are
//  - s3:ObjectRemoved:Delete
func notifyObjectDeletedEvent(nConfig notificationConfig, bucket string, object string) {
	region := serverConfig.GetRegion()
	tnow := time.Now().UTC()
	sequencer := fmt.Sprintf("%X", tnow.UnixNano())
	// Following blocks fills in all the necessary details of s3 event message structure.
	// http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
	events := []*NotificationEvent{
		{
			EventVersion:      "2.0",
			EventSource:       "aws:s3",
			AwsRegion:         region,
			EventTime:         tnow.Format(iso8601Format),
			EventName:         ObjectRemovedDelete.String(),
			UserIdentity:      defaultIdentity(),
			RequestParameters: map[string]string{},
			ResponseElements:  map[string]string{},
			S3: eventMeta{
				SchemaVersion:   "1.0",
				ConfigurationID: "Config",
				Bucket: bucketMeta{
					Name:          bucket,
					OwnerIdentity: defaultIdentity(),
					ARN:           "arn:aws:s3:::" + bucket,
				},
				Object: objectMeta{
					Key:       url.QueryEscape(object),
					Sequencer: sequencer,
				},
			},
		},
	}
	// Notify to all the configured queues.
	for _, qConfig := range nConfig.QueueConfigurations {
		ruleMatch := filterRuleMatch(object, qConfig.Filter.Key.FilterRules)
		if eventMatch(ObjectRemovedDelete, qConfig.Events) && ruleMatch {
			log.WithFields(logrus.Fields{
				"Records": events,
			}).Info()
		}
	}
}
