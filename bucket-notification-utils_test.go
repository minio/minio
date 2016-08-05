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

// Tests lambda arn validation.
func TestLambdaARN(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer removeAll(rootPath)

	testCases := []struct {
		lambdaARN string
		errCode   APIErrorCode
	}{
		// Valid minio lambda with '1' account id.
		{
			lambdaARN: "arn:minio:lambda:us-east-1:1:minio",
			errCode:   ErrNone,
		},
		// Valid minio lambda with '10' account id.
		{
			lambdaARN: "arn:minio:lambda:us-east-1:10:minio",
			errCode:   ErrNone,
		},
		// Invalid empty queue arn.
		{
			lambdaARN: "",
			errCode:   ErrARNNotification,
		},
		// Invalid region 'us-west-1' in queue arn.
		{
			lambdaARN: "arn:minio:lambda:us-west-1:1:redis",
			errCode:   ErrRegionNotification,
		},
	}

	for i, testCase := range testCases {
		errCode := checkLambdaARN(testCase.lambdaARN)
		if testCase.errCode != errCode {
			t.Errorf("Test %d: Expected \"%d\", got \"%d\"", i+1, testCase.errCode, errCode)
		}
	}
}

// Tests queue arn validation.
func TestQueueARN(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer removeAll(rootPath)

	testCases := []struct {
		queueARN string
		errCode  APIErrorCode
	}{
		// Valid redis queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1:1:redis",
			errCode:  ErrNone,
		},
		// Valid elasticsearch queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1:1:elasticsearch",
			errCode:  ErrNone,
		},
		// Valid amqp queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1:1:amqp",
			errCode:  ErrNone,
		},
		// Invalid empty queue arn.
		{
			queueARN: "",
			errCode:  ErrARNNotification,
		},
		// Invalid region 'us-west-1' in queue arn.
		{
			queueARN: "arn:minio:sqs:us-west-1:1:redis",
			errCode:  ErrRegionNotification,
		},
	}

	for i, testCase := range testCases {
		errCode := checkQueueARN(testCase.queueARN)
		if testCase.errCode != errCode {
			t.Errorf("Test %d: Expected \"%d\", got \"%d\"", i+1, testCase.errCode, errCode)
		}
	}
}

// Test unmarshal queue arn.
func TestUnmarshalLambdaARN(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer removeAll(rootPath)

	testCases := []struct {
		lambdaARN string
		Type      string
	}{
		// Valid minio lambda arn.
		{
			lambdaARN: "arn:minio:lambda:us-east-1:1:lambda",
			Type:      "lambda",
		},
		// Invalid empty queue arn.
		{
			lambdaARN: "",
			Type:      "",
		},
		// Invalid region 'us-west-1' in queue arn.
		{
			lambdaARN: "arn:minio:lambda:us-west-1:1:lambda",
			Type:      "",
		},
		// Partial queue arn.
		{
			lambdaARN: "arn:minio:lambda:",
			Type:      "",
		},
		// Invalid queue service value.
		{
			lambdaARN: "arn:minio:lambda:us-east-1:1:*",
			Type:      "",
		},
	}

	for i, testCase := range testCases {
		lambda := unmarshalLambdaARN(testCase.lambdaARN)
		if testCase.Type != lambda.Type {
			t.Errorf("Test %d: Expected \"%s\", got \"%s\"", i+1, testCase.Type, lambda.Type)
		}
	}
}

// Test unmarshal queue arn.
func TestUnmarshalSqsARN(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer removeAll(rootPath)

	testCases := []struct {
		queueARN string
		Type     string
	}{
		// Valid redis queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1:1:redis",
			Type:     "redis",
		},
		// Valid elasticsearch queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1:1:elasticsearch",
			Type:     "elasticsearch",
		},
		// Valid amqp queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1:1:amqp",
			Type:     "amqp",
		},
		// Invalid empty queue arn.
		{
			queueARN: "",
			Type:     "",
		},
		// Invalid region 'us-west-1' in queue arn.
		{
			queueARN: "arn:minio:sqs:us-west-1:1:redis",
			Type:     "",
		},
		// Partial queue arn.
		{
			queueARN: "arn:minio:sqs:",
			Type:     "",
		},
		// Invalid queue service value.
		{
			queueARN: "arn:minio:sqs:us-east-1:1:*",
			Type:     "",
		},
	}

	for i, testCase := range testCases {
		mSqs := unmarshalSqsARN(testCase.queueARN)
		if testCase.Type != mSqs.Type {
			t.Errorf("Test %d: Expected \"%s\", got \"%s\"", i+1, testCase.Type, mSqs.Type)
		}
	}

}
