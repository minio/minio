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

import "testing"

// Tests filter name validation.
func TestIsValidFilterName(t *testing.T) {
	testCases := []struct {
		filterName string
		status     bool
	}{
		// Validate if 'prefix' is correct.
		{
			filterName: "prefix",
			status:     true,
		},
		// Validate if 'suffix' is correct.
		{
			filterName: "suffix",
			status:     true,
		},
		// Invalid filter name empty string should return false.
		{
			filterName: "",
			status:     false,
		},
		// Invalid filter name random character should return false.
		{
			filterName: "unknown",
			status:     false,
		},
	}

	for i, testCase := range testCases {
		status := isValidFilterName(testCase.filterName)
		if testCase.status != status {
			t.Errorf("Test %d: Expected \"%t\", got \"%t\"", i+1, testCase.status, status)
		}
	}
}

// Tests list of valid and invalid events.
func TestValidEvents(t *testing.T) {
	testCases := []struct {
		events  []string
		errCode APIErrorCode
	}{
		// Return error for unknown event element.
		{
			events: []string{
				"s3:UnknownAPI",
			},
			errCode: ErrEventNotification,
		},
		// Return success for supported event.
		{
			events: []string{
				"s3:ObjectCreated:Put",
			},
			errCode: ErrNone,
		},
		// Return success for supported events.
		{
			events: []string{
				"s3:ObjectCreated:*",
				"s3:ObjectRemoved:*",
			},
			errCode: ErrNone,
		},
		// Return error for empty event list.
		{
			events:  []string{""},
			errCode: ErrEventNotification,
		},
	}

	for i, testCase := range testCases {
		errCode := checkEvents(testCase.events)
		if testCase.errCode != errCode {
			t.Errorf("Test %d: Expected \"%d\", got \"%d\"", i+1, testCase.errCode, errCode)
		}
	}
}

// Tests queue arn validation.
func TestQueueArn(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer removeAll(rootPath)

	testCases := []struct {
		queueArn string
		errCode  APIErrorCode
	}{
		// Valid redis queue arn.
		{
			queueArn: "arn:minio:sqs:us-east-1:1:redis",
			errCode:  ErrNone,
		},
		// Valid elasticsearch queue arn.
		{
			queueArn: "arn:minio:sqs:us-east-1:1:elasticsearch",
			errCode:  ErrNone,
		},
		// Valid amqp queue arn.
		{
			queueArn: "arn:minio:sqs:us-east-1:1:amqp",
			errCode:  ErrNone,
		},
		// Invalid empty queue arn.
		{
			queueArn: "",
			errCode:  ErrARNNotification,
		},
		// Invalid region 'us-west-1' in queue arn.
		{
			queueArn: "arn:minio:sqs:us-west-1:1:redis",
			errCode:  ErrRegionNotification,
		},
	}

	for i, testCase := range testCases {
		errCode := checkQueueArn(testCase.queueArn)
		if testCase.errCode != errCode {
			t.Errorf("Test %d: Expected \"%d\", got \"%d\"", i+1, testCase.errCode, errCode)
		}
	}
}

// Test unmarshal queue arn.
func TestUnmarshalSqsArn(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer removeAll(rootPath)

	testCases := []struct {
		queueArn string
		sqsType  string
	}{
		// Valid redis queue arn.
		{
			queueArn: "arn:minio:sqs:us-east-1:1:redis",
			sqsType:  "1:redis",
		},
		// Valid elasticsearch queue arn.
		{
			queueArn: "arn:minio:sqs:us-east-1:1:elasticsearch",
			sqsType:  "1:elasticsearch",
		},
		// Valid amqp queue arn.
		{
			queueArn: "arn:minio:sqs:us-east-1:1:amqp",
			sqsType:  "1:amqp",
		},
		// Invalid empty queue arn.
		{
			queueArn: "",
			sqsType:  "",
		},
		// Invalid region 'us-west-1' in queue arn.
		{
			queueArn: "arn:minio:sqs:us-west-1:1:redis",
			sqsType:  "",
		},
		// Partial queue arn.
		{
			queueArn: "arn:minio:sqs:",
			sqsType:  "",
		},
		// Invalid queue service value.
		{
			queueArn: "arn:minio:sqs:us-east-1:1:*",
			sqsType:  "",
		},
	}

	for i, testCase := range testCases {
		mSqs := unmarshalSqsArn(testCase.queueArn)
		if testCase.sqsType != mSqs.sqsType {
			t.Errorf("Test %d: Expected \"%s\", got \"%s\"", i+1, testCase.sqsType, mSqs.sqsType)
		}
	}

}
