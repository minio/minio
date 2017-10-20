/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"os"
	"strings"
	"testing"
)

// Test validates for duplicate configs.
func TestCheckDuplicateConfigs(t *testing.T) {
	testCases := []struct {
		qConfigs        []queueConfig
		expectedErrCode APIErrorCode
	}{
		// Error for duplicate queue configs.
		{
			qConfigs: []queueConfig{
				{
					QueueARN: "arn:minio:sqs:us-east-1:1:redis",
				},
				{
					QueueARN: "arn:minio:sqs:us-east-1:1:redis",
				},
			},
			expectedErrCode: ErrOverlappingConfigs,
		},
		// Valid queue configs.
		{
			qConfigs: []queueConfig{
				{
					QueueARN: "arn:minio:sqs:us-east-1:1:redis",
				},
			},
			expectedErrCode: ErrNone,
		},
	}

	// ... validate for duplicate queue configs.
	for i, testCase := range testCases {
		errCode := checkDuplicateQueueConfigs(testCase.qConfigs)
		if errCode != testCase.expectedErrCode {
			t.Errorf("Test %d: Expected %d, got %d", i+1, testCase.expectedErrCode, errCode)
		}
	}
}

// Tests for validating filter rules.
func TestCheckFilterRules(t *testing.T) {
	testCases := []struct {
		rules           []filterRule
		expectedErrCode APIErrorCode
	}{
		// Valid prefix and suffix values.
		{
			rules: []filterRule{
				{
					Name:  "prefix",
					Value: "test/test1",
				},
				{
					Name:  "suffix",
					Value: ".jpg",
				},
			},
			expectedErrCode: ErrNone,
		},
		// Invalid filter name.
		{
			rules: []filterRule{
				{
					Name:  "unknown",
					Value: "test/test1",
				},
			},
			expectedErrCode: ErrFilterNameInvalid,
		},
		// Cannot have duplicate prefixes.
		{
			rules: []filterRule{
				{
					Name:  "prefix",
					Value: "test/test1",
				},
				{
					Name:  "prefix",
					Value: "test/test1",
				},
			},
			expectedErrCode: ErrFilterNamePrefix,
		},
		// Cannot have duplicate suffixes.
		{
			rules: []filterRule{
				{
					Name:  "suffix",
					Value: ".jpg",
				},
				{
					Name:  "suffix",
					Value: ".txt",
				},
			},
			expectedErrCode: ErrFilterNameSuffix,
		},
		// Filter value cannot be bigger than > 1024.
		{
			rules: []filterRule{
				{
					Name:  "prefix",
					Value: strings.Repeat("a", 1025),
				},
			},
			expectedErrCode: ErrFilterValueInvalid,
		},
	}

	for i, testCase := range testCases {
		errCode := checkFilterRules(testCase.rules)
		if errCode != testCase.expectedErrCode {
			t.Errorf("Test %d: Expected %d, got %d", i+1, testCase.expectedErrCode, errCode)
		}
	}
}

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
func TestQueueARN(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer os.RemoveAll(rootPath)

	testCases := []struct {
		queueARN string
		errCode  APIErrorCode
	}{

		// Valid webhook queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1:1:webhook",
			errCode:  ErrNone,
		},
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
		// Invalid notification service type.
		{
			queueARN: "arn:minio:sns:us-east-1:1:listen",
			errCode:  ErrARNNotification,
		},
		// Invalid queue name empty in queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1:1:",
			errCode:  ErrARNNotification,
		},
		// Invalid queue id empty in queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1::redis",
			errCode:  ErrARNNotification,
		},
		// Invalid queue id and queue name empty in queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1::",
			errCode:  ErrARNNotification,
		},
		// Missing queue id and separator missing at the end in queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1:amqp",
			errCode:  ErrARNNotification,
		},
		// Missing queue id and empty string at the end in queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1:",
			errCode:  ErrARNNotification,
		},
	}

	// Validate all tests for queue arn.
	for i, testCase := range testCases {
		errCode := checkQueueARN(testCase.queueARN)
		if testCase.errCode != errCode {
			t.Errorf("Test %d: Expected \"%d\", got \"%d\"", i+1, testCase.errCode, errCode)
		}
	}

	// Test when server region is set.
	rootPath, err = newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer os.RemoveAll(rootPath)

	testCases = []struct {
		queueARN string
		errCode  APIErrorCode
	}{
		// Incorrect region should produce error.
		{
			queueARN: "arn:minio:sqs:us-west-1:1:webhook",
			errCode:  ErrRegionNotification,
		},
		// Correct region should not produce error.
		{
			queueARN: "arn:minio:sqs:us-east-1:1:webhook",
			errCode:  ErrNone,
		},
	}

	// Validate all tests for queue arn.
	for i, testCase := range testCases {
		errCode := checkQueueARN(testCase.queueARN)
		if testCase.errCode != errCode {
			t.Errorf("Test %d: Expected \"%d\", got \"%d\"", i+1, testCase.errCode, errCode)
		}
	}
}

// Test unmarshal queue arn.
func TestUnmarshalSQSARN(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer os.RemoveAll(rootPath)

	testCases := []struct {
		queueARN string
		Type     string
	}{
		// Valid webhook queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1:1:webhook",
			Type:     "webhook",
		},
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
		// Valid mqtt queue arn.
		{
			queueARN: "arn:minio:sqs:us-east-1:1:mqtt",
			Type:     "mqtt",
		},
		// Invalid empty queue arn.
		{
			queueARN: "",
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

	// Test when the server region is set.
	rootPath, err = newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}
	defer os.RemoveAll(rootPath)

	testCases = []struct {
		queueARN string
		Type     string
	}{
		// Incorrect region in ARN returns empty mSqs.Type
		{
			queueARN: "arn:minio:sqs:us-west-1:1:webhook",
			Type:     "",
		},
		// Correct regionin ARN returns valid mSqs.Type
		{
			queueARN: "arn:minio:sqs:us-east-1:1:webhook",
			Type:     "webhook",
		},
	}

	for i, testCase := range testCases {
		mSqs := unmarshalSqsARN(testCase.queueARN)
		if testCase.Type != mSqs.Type {
			t.Errorf("Test %d: Expected \"%s\", got \"%s\"", i+1, testCase.Type, mSqs.Type)
		}
	}
}
