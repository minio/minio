// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package target

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/minio/minio/internal/event"
)

// TestDir
var queueDir = filepath.Join(os.TempDir(), "minio_test")

// Sample test event.
var testEvent = event.Event{EventVersion: "1.0", EventSource: "test_source", AwsRegion: "test_region", EventTime: "test_time", EventName: event.ObjectAccessedGet}

// Initialize the store.
func setUpStore(directory string, limit uint64) (Store, error) {
	store := NewQueueStore(queueDir, limit)
	if oErr := store.Open(); oErr != nil {
		return nil, oErr
	}
	return store, nil
}

// Tear down store
func tearDownStore() error {
	return os.RemoveAll(queueDir)
}

// TestQueueStorePut - tests for store.Put
func TestQueueStorePut(t *testing.T) {
	defer func() {
		if err := tearDownStore(); err != nil {
			t.Fatal("Failed to tear down store ", err)
		}
	}()
	store, err := setUpStore(queueDir, 100)
	if err != nil {
		t.Fatal("Failed to create a queue store ", err)

	}
	// Put 100 events.
	for i := 0; i < 100; i++ {
		if err := store.Put(testEvent); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	// Count the events.
	names, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 100 {
		t.Fatalf("List() Expected: 100, got %d", len(names))
	}
}

// TestQueueStoreGet - tests for store.Get
func TestQueueStoreGet(t *testing.T) {
	defer func() {
		if err := tearDownStore(); err != nil {
			t.Fatal("Failed to tear down store ", err)
		}
	}()
	store, err := setUpStore(queueDir, 10)
	if err != nil {
		t.Fatal("Failed to create a queue store ", err)
	}
	// Put 10 events
	for i := 0; i < 10; i++ {
		if err := store.Put(testEvent); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	eventKeys, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	// Get 10 events.
	if len(eventKeys) == 10 {
		for _, key := range eventKeys {
			event, eErr := store.Get(strings.TrimSuffix(key, eventExt))
			if eErr != nil {
				t.Fatal("Failed to Get the event from the queue store ", eErr)
			}
			if !reflect.DeepEqual(testEvent, event) {
				t.Fatalf("Failed to read the event: error: expected = %v, got = %v", testEvent, event)
			}
		}
	} else {
		t.Fatalf("List() Expected: 10, got %d", len(eventKeys))
	}
}

// TestQueueStoreDel - tests for store.Del
func TestQueueStoreDel(t *testing.T) {
	defer func() {
		if err := tearDownStore(); err != nil {
			t.Fatal("Failed to tear down store ", err)
		}
	}()
	store, err := setUpStore(queueDir, 20)
	if err != nil {
		t.Fatal("Failed to create a queue store ", err)
	}
	// Put 20 events.
	for i := 0; i < 20; i++ {
		if err := store.Put(testEvent); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	eventKeys, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	// Remove all the events.
	if len(eventKeys) == 20 {
		for _, key := range eventKeys {
			err := store.Del(strings.TrimSuffix(key, eventExt))
			if err != nil {
				t.Fatal("queue store Del failed with ", err)
			}
		}
	} else {
		t.Fatalf("List() Expected: 20, got %d", len(eventKeys))
	}

	names, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		t.Fatalf("List() Expected: 0, got %d", len(names))
	}
}

// TestQueueStoreLimit - tests the event limit for the store.
func TestQueueStoreLimit(t *testing.T) {
	defer func() {
		if err := tearDownStore(); err != nil {
			t.Fatal("Failed to tear down store ", err)
		}
	}()
	// The max limit is set to 5.
	store, err := setUpStore(queueDir, 5)
	if err != nil {
		t.Fatal("Failed to create a queue store ", err)
	}
	for i := 0; i < 5; i++ {
		if err := store.Put(testEvent); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	// Should not allow 6th Put.
	if err := store.Put(testEvent); err == nil {
		t.Fatalf("Expected to fail with %s, but passes", errLimitExceeded)
	}
}

// TestQueueStoreLimit - tests for store.LimitN.
func TestQueueStoreListN(t *testing.T) {
	defer func() {
		if err := tearDownStore(); err != nil {
			t.Fatal("Failed to tear down store ", err)
		}
	}()
	store, err := setUpStore(queueDir, 10)
	if err != nil {
		t.Fatal("Failed to create a queue store ", err)
	}
	for i := 0; i < 10; i++ {
		if err := store.Put(testEvent); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	// Should return all the event keys in the store.
	names, err := store.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(names) != 10 {
		t.Fatalf("List() Expected: 10, got %d", len(names))
	}

	if err = os.RemoveAll(queueDir); err != nil {
		t.Fatal(err)
	}

	_, err = store.List()
	if !os.IsNotExist(err) {
		t.Fatalf("Expected List() to fail with os.ErrNotExist, %s", err)
	}
}
