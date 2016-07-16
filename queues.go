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
	"time"

	"github.com/Sirupsen/logrus"
)

const (
	arnMinioSqs = "arn:minio:sqs:"
)

// Maintains queue map for validating input fields.
var queueInputFields = map[string]int{
	arnAmqpQueue:    6,
	arnElasticQueue: 4,
	// Add new queues here.
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

func notifyObjectCreatedEvent(nConfig notificationConfig, eventType EventName, bucket string, object string, etag string, size int64) {
	region := serverConfig.GetRegion()
	sequencer := fmt.Sprintf("%X", time.Now().UTC().UnixNano())
	events := []*NotificationEvent{
		&NotificationEvent{
			EventVersion:      "2.0",
			EventSource:       "aws:s3",
			AwsRegion:         region,
			EventTime:         time.Now().UTC(),
			EventName:         eventType.String(),
			UserIdentity:      defaultIdentity(),
			RequestParameters: make(map[string]string), // TODO - not supported yet.
			ResponseElements:  make(map[string]string), // TODO - not supported yet.
			S3: s3Reference{
				SchemaVersion:   "1.0",
				ConfigurationID: "Config",
				Bucket: s3BucketReference{
					Name:          bucket,
					OwnerIdentity: defaultIdentity(),
					ARN:           "arn:aws:s3:::" + bucket,
				},
				Object: s3ObjectReference{
					Key:       url.QueryEscape(object),
					ETag:      etag,
					Size:      size,
					Sequencer: sequencer,
				},
			},
		},
	}
	for _, qConfig := range nConfig.QueueConfigurations {
		if eventMatch(eventType, qConfig.Events) {
			log.WithFields(logrus.Fields{
				"Records": events,
			}).Info()
		}
	}
}

func notifyObjectDeletedEvent(nConfig notificationConfig, bucket string, object string) {
	region := serverConfig.GetRegion()
	sequencer := fmt.Sprintf("%X", time.Now().UTC().UnixNano())
	events := []*NotificationEvent{
		&NotificationEvent{
			EventVersion:      "2.0",
			EventSource:       "aws:s3",
			AwsRegion:         region,
			EventTime:         time.Now().UTC(),
			EventName:         ObjectRemovedDelete.String(),
			UserIdentity:      defaultIdentity(),
			RequestParameters: make(map[string]string), // TODO - not supported yet.
			ResponseElements:  make(map[string]string), // TODO - not supported yet.
			S3: s3Reference{
				SchemaVersion:   "1.0",
				ConfigurationID: "Config",
				Bucket: s3BucketReference{
					Name:          bucket,
					OwnerIdentity: defaultIdentity(),
					ARN:           "arn:aws:s3:::" + bucket,
				},
				Object: s3ObjectReference{
					Key:       url.QueryEscape(object),
					Sequencer: sequencer,
				},
			},
		},
	}
	for _, qConfig := range nConfig.QueueConfigurations {
		if eventMatch(ObjectRemovedDelete, qConfig.Events) {
			log.WithFields(logrus.Fields{
				"Records": events,
			}).Info()
		}
	}
}
