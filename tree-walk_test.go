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
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Helper function that invokes startTreeWalk depending on the type implementing objectLayer.
func startTreeWalk(obj ObjectLayer, bucket, prefix, marker string,
	recursive bool, endWalkCh chan struct{}) chan treeWalkResult {
	var twResultCh chan treeWalkResult
	switch typ := obj.(type) {
	case fsObjects:
		twResultCh = typ.startTreeWalk(bucket, prefix, marker, true,
			func(bucket, object string) bool {
				return !strings.HasSuffix(object, slashSeparator)
			}, endWalkCh)
	case xlObjects:
		twResultCh = typ.startTreeWalk(bucket, prefix, marker, true,
			typ.isObject, endWalkCh)
	}
	return twResultCh
}

// Helper function that creates a bucket, bucket and objects from objects []string.
func createObjNamespace(obj ObjectLayer, bucket string, objects []string) error {
	// Make a bucket.
	var err error
	err = obj.MakeBucket(bucket)
	if err != nil {
		return err
	}

	// Create objects.
	for _, object := range objects {
		_, err = obj.PutObject(bucket, object, int64(len("hello")),
			bytes.NewReader([]byte("hello")), nil)
		if err != nil {
			return err
		}
	}
	return err
}

// Wrapper for testTreeWalkPrefix to run the unit test for both FS and XL backend.
func TestTreeWalkPrefix(t *testing.T) {
	ExecObjectLayerTest(t, testTreeWalkPrefix)
}

// Test if tree walker returns entries matching prefix alone are received
// when a non empty prefix is supplied.
func testTreeWalkPrefix(obj ObjectLayer, instanceType string, t *testing.T) {
	bucket := "abc"
	objects := []string{
		"d/e",
		"d/f",
		"d/g/h",
		"i/j/k",
		"lmn",
	}

	err := createObjNamespace(obj, bucket, objects)
	if err != nil {
		t.Fatal(err)
	}

	// Start the tree walk go-routine.
	prefix := "d/"
	endWalkCh := make(chan struct{})
	twResultCh := startTreeWalk(obj, bucket, prefix, "", true, endWalkCh)

	// Check if all entries received on the channel match the prefix.
	for res := range twResultCh {
		if !strings.HasPrefix(res.entry, prefix) {
			t.Errorf("Entry %s doesn't match prefix %s", res.entry, prefix)
		}
	}
}

// Wrapper for testTreeWalkMarker to run the unit test for both FS and XL backend.
func TestTreeWalkMarker(t *testing.T) {
	ExecObjectLayerTest(t, testTreeWalkMarker)
}

// Test if entries received on tree walk's channel appear after the supplied marker.
func testTreeWalkMarker(obj ObjectLayer, instanceType string, t *testing.T) {
	bucket := "abc"
	objects := []string{
		"d/e",
		"d/f",
		"d/g/h",
		"i/j/k",
		"lmn",
	}

	err := createObjNamespace(obj, bucket, objects)
	if err != nil {
		t.Fatal(err)
	}

	// Start the tree walk go-routine.
	prefix := ""
	endWalkCh := make(chan struct{})
	twResultCh := startTreeWalk(obj, bucket, prefix, "d/g", true, endWalkCh)

	// Check if only 3 entries, namely d/g/h, i/j/k, lmn are received on the channel.
	expectedCount := 3
	actualCount := 0
	for range twResultCh {
		actualCount++
	}
	if expectedCount != actualCount {
		t.Errorf("Expected %d entries, actual no. of entries were %d", expectedCount, actualCount)
	}

}

// Wrapper for testTreeWalkAbort to run the unit test for both FS and XL backend.
func TestTreeWalkAbort(t *testing.T) {
	ExecObjectLayerTest(t, testTreeWalkAbort)
}

// Extend treeWalk type to provide a method to reset timeout
func (t *treeWalkPool) setTimeout(newTimeout time.Duration) {
	t.timeOut = newTimeout
}

// Helper function to set treewalk (idle) timeout
func setTimeout(obj ObjectLayer, newTimeout time.Duration) {
	switch typ := obj.(type) {
	case fsObjects:
		typ.listPool.setTimeout(newTimeout)
	case xlObjects:
		typ.listPool.setTimeout(newTimeout)

	}
}

// Helper function to put the tree walk go-routine into the pool
func putbackTreeWalk(obj ObjectLayer, params listParams, resultCh chan treeWalkResult, endWalkCh chan struct{}) {
	switch typ := obj.(type) {
	case fsObjects:
		typ.listPool.Set(params, resultCh, endWalkCh)
	case xlObjects:
		typ.listPool.Set(params, resultCh, endWalkCh)

	}
}

// Test if tree walk go-routine exits cleanly if tree walk is aborted before compeletion.
func testTreeWalkAbort(obj ObjectLayer, instanceType string, t *testing.T) {
	bucket := "abc"

	var objects []string
	for i := 0; i < 1001; i++ {
		objects = append(objects, "obj"+strconv.Itoa(i))
	}

	err := createObjNamespace(obj, bucket, objects)
	if err != nil {
		t.Fatal(err)
	}

	// Set treewalk pool timeout to be test friendly
	setTimeout(obj, 2*time.Second)

	// Start the tree walk go-routine.
	prefix := ""
	marker := ""
	recursive := true
	endWalkCh := make(chan struct{})
	twResultCh := startTreeWalk(obj, bucket, prefix, marker, recursive, endWalkCh)

	// Pull one result entry from the tree walk result channel.
	<-twResultCh

	// Put the treewalk go-routine into tree walk pool
	putbackTreeWalk(obj, listParams{bucket, recursive, marker, prefix}, twResultCh, endWalkCh)

	// Confirm that endWalkCh is closed on tree walk pool timer expiry
	if _, open := <-endWalkCh; open {
		t.Error("Expected tree walk endWalk channel to be closed, found to be open")
	}

	// Drain the buffered channel result channel of entries that were pushed before
	// it was signalled to abort.
	for range twResultCh {
	}
	if _, open := <-twResultCh; open {
		t.Error("Expected tree walk result channel to be closed, found to be open")
	}
}

// Helper function to get a slice of disks depending on the backend
func getPhysicalDisks(obj ObjectLayer) []string {
	switch typ := obj.(type) {
	case fsObjects:
		return []string{typ.physicalDisk}
	case xlObjects:
		return typ.physicalDisks
	}
	return []string{}
}

// Wrapper for testTreeWalkFailedDisks to run the unit test for both FS and XL backend.
func TestTreeWalkFailedDisks(t *testing.T) {
	ExecObjectLayerTest(t, testTreeWalkFailedDisks)
}

// Test if tree walk go routine exits cleanly when more than quorum number of disks fail
// in XL and the single disk in FS.
func testTreeWalkFailedDisks(obj ObjectLayer, instanceType string, t *testing.T) {
	bucket := "abc"
	objects := []string{
		"d/e",
		"d/f",
		"d/g/h",
		"i/j/k",
		"lmn",
	}

	err := createObjNamespace(obj, bucket, objects)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate disk failures by removing the directories backing them
	disks := getPhysicalDisks(obj)
	switch obj.(type) {
	case fsObjects:
		removeDiskN(disks, 1)
	case xlObjects:
		removeDiskN(disks, len(disks))
	}

	// Start the tree walk go-routine.
	prefix := ""
	marker := ""
	recursive := true
	endWalkCh := make(chan struct{})
	twResultCh := startTreeWalk(obj, bucket, prefix, marker, recursive, endWalkCh)

	if res := <-twResultCh; res.err.Error() != "disk not found" {
		t.Error("Expected disk not found error")
	}

}

// FIXME: Test the abort timeout when the tree-walk go routine is 'parked' in
// the pool.  Currently, we need to create objects greater than maxObjectList
// (== 1000) which would increase time to run the test. If (and when) we decide
// to make maxObjectList configurable we can re-evaluate adding a unit test for
// this.
