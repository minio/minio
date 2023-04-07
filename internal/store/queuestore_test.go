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

package store

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type TestItem struct {
	Name     string `json:"Name"`
	Property string `json:"property"`
}

var (
	// TestDir
	queueDir = filepath.Join(os.TempDir(), "minio_test")
	// Sample test item.
	testItem = TestItem{Name: "test-item", Property: "property"}
	// Ext for test item
	testItemExt = ".test"
)

// Initialize the queue store.
func setUpQueueStore(directory string, limit uint64) (Store[TestItem], error) {
	queueStore := NewQueueStore[TestItem](queueDir, limit, testItemExt)
	if oErr := queueStore.Open(); oErr != nil {
		return nil, oErr
	}
	return queueStore, nil
}

// Tear down queue store.
func tearDownQueueStore() error {
	return os.RemoveAll(queueDir)
}

// TestQueueStorePut - tests for store.Put
func TestQueueStorePut(t *testing.T) {
	defer func() {
		if err := tearDownQueueStore(); err != nil {
			t.Fatal("Failed to tear down store ", err)
		}
	}()
	store, err := setUpQueueStore(queueDir, 100)
	if err != nil {
		t.Fatal("Failed to create a queue store ", err)
	}
	// Put 100 items.
	for i := 0; i < 100; i++ {
		if err := store.Put(testItem); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	// Count the items.
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
		if err := tearDownQueueStore(); err != nil {
			t.Fatal("Failed to tear down store ", err)
		}
	}()
	store, err := setUpQueueStore(queueDir, 10)
	if err != nil {
		t.Fatal("Failed to create a queue store ", err)
	}
	// Put 10 items
	for i := 0; i < 10; i++ {
		if err := store.Put(testItem); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	itemKeys, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	// Get 10 items.
	if len(itemKeys) == 10 {
		for _, key := range itemKeys {
			item, eErr := store.Get(strings.TrimSuffix(key, testItemExt))
			if eErr != nil {
				t.Fatal("Failed to Get the item from the queue store ", eErr)
			}
			if !reflect.DeepEqual(testItem, item) {
				t.Fatalf("Failed to read the item: error: expected = %v, got = %v", testItem, item)
			}
		}
	} else {
		t.Fatalf("List() Expected: 10, got %d", len(itemKeys))
	}
}

// TestQueueStoreDel - tests for store.Del
func TestQueueStoreDel(t *testing.T) {
	defer func() {
		if err := tearDownQueueStore(); err != nil {
			t.Fatal("Failed to tear down store ", err)
		}
	}()
	store, err := setUpQueueStore(queueDir, 20)
	if err != nil {
		t.Fatal("Failed to create a queue store ", err)
	}
	// Put 20 items.
	for i := 0; i < 20; i++ {
		if err := store.Put(testItem); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	itemKeys, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	// Remove all the items.
	if len(itemKeys) == 20 {
		for _, key := range itemKeys {
			err := store.Del(strings.TrimSuffix(key, testItemExt))
			if err != nil {
				t.Fatal("queue store Del failed with ", err)
			}
		}
	} else {
		t.Fatalf("List() Expected: 20, got %d", len(itemKeys))
	}

	names, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		t.Fatalf("List() Expected: 0, got %d", len(names))
	}
}

// TestQueueStoreLimit - tests the item limit for the store.
func TestQueueStoreLimit(t *testing.T) {
	defer func() {
		if err := tearDownQueueStore(); err != nil {
			t.Fatal("Failed to tear down store ", err)
		}
	}()
	// The max limit is set to 5.
	store, err := setUpQueueStore(queueDir, 5)
	if err != nil {
		t.Fatal("Failed to create a queue store ", err)
	}
	for i := 0; i < 5; i++ {
		if err := store.Put(testItem); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	// Should not allow 6th Put.
	if err := store.Put(testItem); err == nil {
		t.Fatalf("Expected to fail with %s, but passes", errLimitExceeded)
	}
}

// TestQueueStoreLimit - tests for store.LimitN.
func TestQueueStoreListN(t *testing.T) {
	defer func() {
		if err := tearDownQueueStore(); err != nil {
			t.Fatal("Failed to tear down store ", err)
		}
	}()
	store, err := setUpQueueStore(queueDir, 10)
	if err != nil {
		t.Fatal("Failed to create a queue store ", err)
	}
	for i := 0; i < 10; i++ {
		if err := store.Put(testItem); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	// Should return all the item keys in the store.
	names, err := store.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(names) != 10 {
		t.Fatalf("List() Expected: 10, got %d", len(names))
	}

	// re-open
	store, err = setUpQueueStore(queueDir, 10)
	if err != nil {
		t.Fatal("Failed to create a queue store ", err)
	}
	names, err = store.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(names) != 10 {
		t.Fatalf("List() Expected: 10, got %d", len(names))
	}
	if len(names) != store.Len() {
		t.Fatalf("List() Expected: 10, got %d", len(names))
	}

	// Delete all
	for _, key := range names {
		err := store.Del(key)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Re-list
	lst, err := store.List()
	if len(lst) > 0 || err != nil {
		t.Fatalf("Expected List() to return empty list and no error, got %v err: %v", lst, err)
	}
}
