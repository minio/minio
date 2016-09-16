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

import "testing"

// Tests for event filter rules.
func TestFilterMatch(t *testing.T) {
	testCases := []struct {
		objectName        string
		rules             []filterRule
		expectedRuleMatch bool
	}{
		// Prefix matches for a parent.
		{
			objectName: "test/test1/object.txt",
			rules: []filterRule{
				{
					Name:  "prefix",
					Value: "test",
				},
			},
			expectedRuleMatch: true,
		},
		// Prefix matches for the object.
		{
			objectName: "test/test1/object.txt",
			rules: []filterRule{
				{
					Name:  "prefix",
					Value: "test/test1/object",
				},
			},
			expectedRuleMatch: true,
		},
		// Prefix doesn't match.
		{
			objectName: "test/test1/object.txt",
			rules: []filterRule{
				{
					Name:  "prefix",
					Value: "test/test1/object/",
				},
			},
			expectedRuleMatch: false,
		},
		// Suffix matches.
		{
			objectName: "test/test1/object.txt",
			rules: []filterRule{
				{
					Name:  "suffix",
					Value: ".txt",
				},
			},
			expectedRuleMatch: true,
		},
		// Suffix doesn't match but prefix matches.
		{
			objectName: "test/test1/object.txt",
			rules: []filterRule{
				{
					Name:  "suffix",
					Value: ".jpg",
				},
				{
					Name:  "prefix",
					Value: "test/test1",
				},
			},
			expectedRuleMatch: false,
		},
		// Prefix doesn't match but suffix matches.
		{
			objectName: "test/test2/object.jpg",
			rules: []filterRule{
				{
					Name:  "suffix",
					Value: ".jpg",
				},
				{
					Name:  "prefix",
					Value: "test/test1",
				},
			},
			expectedRuleMatch: false,
		},
		// Suffix and prefix doesn't match.
		{
			objectName: "test/test2/object.jpg",
			rules: []filterRule{
				{
					Name:  "suffix",
					Value: ".txt",
				},
				{
					Name:  "prefix",
					Value: "test/test1",
				},
			},
			expectedRuleMatch: false,
		},
	}

	// .. Validate all cases.
	for i, testCase := range testCases {
		ruleMatch := filterRuleMatch(testCase.objectName, testCase.rules)
		if ruleMatch != testCase.expectedRuleMatch {
			t.Errorf("Test %d: Expected %t, got %t", i+1, testCase.expectedRuleMatch, ruleMatch)
		}
	}
}

// Tests all event match.
func TestEventMatch(t *testing.T) {
	testCases := []struct {
		eventName EventName
		events    []string
		match     bool
	}{
		// Valid object created PUT event.
		{
			eventName: ObjectCreatedPut,
			events: []string{
				"s3:ObjectCreated:Put",
			},
			match: true,
		},
		// Valid object removed DELETE event.
		{
			eventName: ObjectRemovedDelete,
			events: []string{
				"s3:ObjectRemoved:Delete",
			},
			match: true,
		},
		// Invalid events fails to match with empty events.
		{
			eventName: ObjectRemovedDelete,
			events:    []string{""},
			match:     false,
		},
		// Invalid events fails to match with valid events.
		{
			eventName: ObjectCreatedCompleteMultipartUpload,
			events: []string{
				"s3:ObjectRemoved:*",
			},
			match: false,
		},
		// Valid events wild card match.
		{
			eventName: ObjectCreatedPut,
			events: []string{
				"s3:ObjectCreated:*",
			},
			match: true,
		},
		// Valid events wild card match.
		{
			eventName: ObjectCreatedPost,
			events: []string{
				"s3:ObjectCreated:*",
			},
			match: true,
		},
		// Valid events wild card match.
		{
			eventName: ObjectCreatedCopy,
			events: []string{
				"s3:ObjectCreated:*",
			},
			match: true,
		},
		// Valid events wild card match.
		{
			eventName: ObjectCreatedCompleteMultipartUpload,
			events: []string{
				"s3:ObjectCreated:*",
			},
			match: true,
		},
		// Valid events wild card match.
		{
			eventName: ObjectCreatedPut,
			events: []string{
				"s3:ObjectCreated:*",
				"s3:ObjectRemoved:*",
			},
			match: true,
		},
	}

	for i, testCase := range testCases {
		ok := eventMatch(testCase.eventName.String(), testCase.events)
		if testCase.match != ok {
			t.Errorf("Test %d: Expected \"%t\", got \"%t\"", i+1, testCase.match, ok)
		}
	}
}
