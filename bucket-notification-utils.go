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
	if !strings.HasPrefix(queueArn, arnMinioSqs) {
		return ErrARNNotification
	}
	if !strings.HasPrefix(queueArn, arnMinioSqs+serverConfig.GetRegion()+":") {
		return ErrRegionNotification
	}
	return ErrNone
}
