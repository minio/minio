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
