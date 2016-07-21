/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"encoding/json"
	"strings"
	"testing"
)

// TestEventNameJson tests if EventName JSON serialization is performed correctly
func TestEventNameJson(t *testing.T) {
	testEventName(t, ObjectCreatedPut, "s3:ObjectCreated:Put")
	testEventName(t, ObjectCreatedPost, "s3:ObjectCreated:Post")
	testEventName(t, ObjectCreatedCopy, "s3:ObjectCreated:Copy")
	testEventName(t, ObjectCreatedCompleteMultipartUpload, "s3:ObjectCreated:CompleteMultipartUpload")
	testEventName(t, ObjectRemovedDelete, "s3:ObjectRemoved:Delete")
	testEventName(t, ObjectRemovedDeleteMarkerCreated, "s3:ObjectRemoved:DeleteMarkerCreated")
	testEventName(t, ReducedRedundancyLostObject, "s3:ReducedRedundancyLostObject")
}

func testEventName(t *testing.T, eventName EventName, expected string) {
	if j, _ := json.Marshal(eventName); strings.Compare(string(j), "\""+expected+"\"") != 0 {
		t.Errorf("%s expected, got %s", "\""+expected+"\"", string(j))
	}
}
