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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/bytebufferpool"
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
	for range 100 {
		if _, err := store.Put(testItem); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	// Count the items.
	keys := store.List()
	if len(keys) != 100 {
		t.Fatalf("List() Expected: 100, got %d", len(keys))
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
	for range 10 {
		if _, err := store.Put(testItem); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	itemKeys := store.List()
	// Get 10 items.
	if len(itemKeys) == 10 {
		for _, key := range itemKeys {
			item, eErr := store.Get(key)
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
	for range 20 {
		if _, err := store.Put(testItem); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	itemKeys := store.List()
	// Remove all the items.
	if len(itemKeys) == 20 {
		for _, key := range itemKeys {
			err := store.Del(key)
			if err != nil {
				t.Fatal("queue store Del failed with ", err)
			}
		}
	} else {
		t.Fatalf("List() Expected: 20, got %d", len(itemKeys))
	}

	keys := store.List()
	if len(keys) != 0 {
		t.Fatalf("List() Expected: 0, got %d", len(keys))
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
	for range 5 {
		if _, err := store.Put(testItem); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	// Should not allow 6th Put.
	if _, err := store.Put(testItem); err == nil {
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
	for range 10 {
		if _, err := store.Put(testItem); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	// Should return all the item keys in the store.
	keys := store.List()

	if len(keys) != 10 {
		t.Fatalf("List() Expected: 10, got %d", len(keys))
	}

	// re-open
	store, err = setUpQueueStore(queueDir, 10)
	if err != nil {
		t.Fatal("Failed to create a queue store ", err)
	}
	keys = store.List()

	if len(keys) != 10 {
		t.Fatalf("List() Expected: 10, got %d", len(keys))
	}
	if len(keys) != store.Len() {
		t.Fatalf("List() Expected: 10, got %d", len(keys))
	}

	// Delete all
	for _, key := range keys {
		err := store.Del(key)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Re-list
	keys = store.List()
	if len(keys) > 0 || err != nil {
		t.Fatalf("Expected List() to return empty list and no error, got %v err: %v", keys, err)
	}
}

func TestMultiplePutGetRaw(t *testing.T) {
	defer func() {
		if err := tearDownQueueStore(); err != nil {
			t.Fatalf("Failed to tear down store; %v", err)
		}
	}()
	store, err := setUpQueueStore(queueDir, 10)
	if err != nil {
		t.Fatalf("Failed to create a queue store; %v", err)
	}
	// TestItem{Name: "test-item", Property: "property"}
	var items []TestItem
	for i := range 10 {
		items = append(items, TestItem{
			Name:     fmt.Sprintf("test-item-%d", i),
			Property: "property",
		})
	}

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	enc := jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(buf)
	for i := range items {
		if err = enc.Encode(items[i]); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := store.PutMultiple(items); err != nil {
		t.Fatalf("failed to put multiple; %v", err)
	}

	keys := store.List()
	if len(keys) != 1 {
		t.Fatalf("expected len(keys)=1, but found %d", len(keys))
	}

	key := keys[0]
	if !key.Compress {
		t.Fatal("expected the item to be compressed")
	}
	if key.ItemCount != 10 {
		t.Fatalf("expected itemcount=10 but found %v", key.ItemCount)
	}

	raw, err := store.GetRaw(key)
	if err != nil {
		t.Fatalf("unable to get multiple items; %v", err)
	}

	if !bytes.Equal(buf.Bytes(), raw) {
		t.Fatalf("expected bytes: %d vs read bytes is wrong %d", len(buf.Bytes()), len(raw))
	}

	if err := store.Del(key); err != nil {
		t.Fatalf("unable to Del; %v", err)
	}

	// Re-list
	keys = store.List()
	if len(keys) > 0 || err != nil {
		t.Fatalf("Expected List() to return empty list and no error, got %v err: %v", keys, err)
	}
}

func TestMultiplePutGets(t *testing.T) {
	defer func() {
		if err := tearDownQueueStore(); err != nil {
			t.Fatalf("Failed to tear down store; %v", err)
		}
	}()
	store, err := setUpQueueStore(queueDir, 10)
	if err != nil {
		t.Fatalf("Failed to create a queue store; %v", err)
	}
	// TestItem{Name: "test-item", Property: "property"}
	var items []TestItem
	for i := range 10 {
		items = append(items, TestItem{
			Name:     fmt.Sprintf("test-item-%d", i),
			Property: "property",
		})
	}

	if _, err := store.PutMultiple(items); err != nil {
		t.Fatalf("failed to put multiple; %v", err)
	}

	keys := store.List()
	if len(keys) != 1 {
		t.Fatalf("expected len(keys)=1, but found %d", len(keys))
	}

	key := keys[0]
	if !key.Compress {
		t.Fatal("expected the item to be compressed")
	}
	if key.ItemCount != 10 {
		t.Fatalf("expected itemcount=10 but found %v", key.ItemCount)
	}

	resultItems, err := store.GetMultiple(key)
	if err != nil {
		t.Fatalf("unable to get multiple items; %v", err)
	}

	if !reflect.DeepEqual(resultItems, items) {
		t.Fatalf("expected item list: %v; but got %v", items, resultItems)
	}

	if err := store.Del(key); err != nil {
		t.Fatalf("unable to Del; %v", err)
	}

	// Re-list
	keys = store.List()
	if len(keys) > 0 || err != nil {
		t.Fatalf("Expected List() to return empty list and no error, got %v err: %v", keys, err)
	}
}

func TestMixedPutGets(t *testing.T) {
	defer func() {
		if err := tearDownQueueStore(); err != nil {
			t.Fatalf("Failed to tear down store; %v", err)
		}
	}()
	store, err := setUpQueueStore(queueDir, 10)
	if err != nil {
		t.Fatalf("Failed to create a queue store; %v", err)
	}
	// TestItem{Name: "test-item", Property: "property"}
	var items []TestItem
	for i := range 5 {
		items = append(items, TestItem{
			Name:     fmt.Sprintf("test-item-%d", i),
			Property: "property",
		})
	}
	if _, err := store.PutMultiple(items); err != nil {
		t.Fatalf("failed to put multiple; %v", err)
	}

	for i := 5; i < 10; i++ {
		item := TestItem{
			Name:     fmt.Sprintf("test-item-%d", i),
			Property: "property",
		}
		if _, err := store.Put(item); err != nil {
			t.Fatalf("unable to store.Put(); %v", err)
		}
		items = append(items, item)
	}

	keys := store.List()
	if len(keys) != 6 {
		// 1 multiple + 5 single PUTs
		t.Fatalf("expected len(keys)=6, but found %d", len(keys))
	}

	var resultItems []TestItem
	for _, key := range keys {
		if key.ItemCount > 1 {
			items, err := store.GetMultiple(key)
			if err != nil {
				t.Fatalf("unable to get multiple items; %v", err)
			}
			resultItems = append(resultItems, items...)
			continue
		}
		item, err := store.Get(key)
		if err != nil {
			t.Fatalf("unable to get item; %v", err)
		}
		resultItems = append(resultItems, item)
	}

	if !reflect.DeepEqual(resultItems, items) {
		t.Fatalf("expected item list: %v; but got %v", items, resultItems)
	}

	// Delete all
	for _, key := range keys {
		if err := store.Del(key); err != nil {
			t.Fatalf("unable to Del; %v", err)
		}
	}
	// Re-list
	keys = store.List()
	if len(keys) > 0 || err != nil {
		t.Fatalf("Expected List() to return empty list and no error, got %v err: %v", keys, err)
	}
}
