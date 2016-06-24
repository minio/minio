/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
 * * Licensed under the Apache License, Version 2.0 (the "License"); * you may not use this file except in compliance with the License.  * You may obtain a copy of the License at
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
	"bytes"
	"strings"
	"testing"
)

// Test if tree walker returns entries matching prefix alone are received
// when a non empty prefix is supplied
func TestTreeWalkPrefix(t *testing.T) {
	// Create a FS-backend
	objLayer, disks, err := getSingleNodeObjectLayer()
	if err != nil {
		t.Fatal(err)
	}
	fs := objLayer.(fsObjects)
	// Cleanup temporary backend directory
	defer removeRoots([]string{disks})

	// Make a bucket
	err = fs.MakeBucket("abc")
	if err != nil {
		t.Fatal(err)
	}

	// Create objects
	for _, objName := range []string{"d/e", "d/f", "d/g/h", "i/j/k", "lmn"} {
		_, err = fs.PutObject("abc", objName, int64(len("hello")),
			bytes.NewReader([]byte("hello")), nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Start the tree walk go-routine
	twResults := fs.startTreeWalk("abc", "d/", "", true, func(bucket, object string) bool {
		return !strings.HasSuffix(object, slashSeparator)

	})

	// Check if all entries received on the channel match the prefix
	for res := range twResults.ch {
		if !strings.HasPrefix(res.entry, "d/") {
			t.Errorf("Entry %s doesn't match prefix d/", res.entry)
		}
	}
}

// Test if entries received on tree walk's channel appear after the supplied marker
func TestTreeWalkMarker(t *testing.T) {
	// Create a FS-backend
	objLayer, disks, err := getSingleNodeObjectLayer()
	if err != nil {
		t.Fatal(err)
	}
	fs := objLayer.(fsObjects)
	// Cleanup temporary backend directory
	defer removeRoots([]string{disks})

	// Make a bucket
	err = fs.MakeBucket("abc")
	if err != nil {
		t.Fatal(err)
	}

	// Create objects
	for _, objName := range []string{"d/e", "d/f", "d/g/h", "i/j/k", "lmn"} {
		_, err = fs.PutObject("abc", objName, int64(len("hello")),
			bytes.NewReader([]byte("hello")), nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Start the tree walk go-routine with marker at d/g.
	twResults := fs.startTreeWalk("abc", "", "d/g", true, func(bucket, object string) bool {
		return !strings.HasSuffix(object, slashSeparator)

	})

	// Check if only 3 entries, namely d/g/h, i/j/k, lmn are received on the channel
	expectedCount := 3
	actualCount := 0
	for range twResults.ch {
		actualCount++
	}
	if expectedCount != actualCount {
		t.Errorf("Expected %d entries, actual no. of entries were %d", expectedCount, actualCount)
	}

}
