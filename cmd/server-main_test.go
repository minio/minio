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
	"reflect"
	"testing"
)

// Tests initializing new object layer.
func TestNewObjectLayer(t *testing.T) {
	// Tests for FS object layer.
	nDisks := 1
	disks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal("Failed to create disks for the backend")
	}
	defer removeRoots(disks)

	endpoints := mustGetNewEndpointList(disks...)
	obj, err := newObjectLayer(endpoints)
	if err != nil {
		t.Fatal("Unexpected object layer initialization error", err)
	}
	_, ok := obj.(*FSObjects)
	if !ok {
		t.Fatal("Unexpected object layer detected", reflect.TypeOf(obj))
	}

	// Tests for XL object layer initialization.

	// Create temporary backend for the test server.
	nDisks = 16
	disks, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal("Failed to create disks for the backend")
	}
	defer removeRoots(disks)

	globalXLSetCount = 1
	globalXLSetDriveCount = 16

	endpoints = mustGetNewEndpointList(disks...)
	obj, err = newObjectLayer(endpoints)
	if err != nil {
		t.Fatal("Unexpected object layer initialization error", err)
	}

	_, ok = obj.(*xlSets)
	if !ok {
		t.Fatal("Unexpected object layer detected", reflect.TypeOf(obj))
	}
}
