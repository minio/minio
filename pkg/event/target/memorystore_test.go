/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package target

import (
	"reflect"
	"testing"
)

// TestMemoryStorePut - Tests for store.Put
func TestMemoryStorePut(t *testing.T) {
	store := NewMemoryStore(100)
	defer func() {
		store = nil
	}()
	for i := 0; i < 100; i++ {
		if err := store.Put(testEvent); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	if len(store.ListN(-1)) != 100 {
		t.Fatalf("ListN() Expected: 100, got %d", len(store.ListN(-1)))
	}
}

// TestMemoryStoreGet - Tests for store.Get.
func TestMemoryStoreGet(t *testing.T) {
	store := NewMemoryStore(10)
	defer func() {
		store = nil
	}()
	for i := 0; i < 10; i++ {
		if err := store.Put(testEvent); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	eventKeys := store.ListN(-1)
	if len(eventKeys) == 10 {
		for _, key := range eventKeys {
			event, eErr := store.Get(key)
			if eErr != nil {
				t.Fatal("Failed to Get the event from the queue store ", eErr)
			}
			if !reflect.DeepEqual(testEvent, event) {
				t.Fatalf("Failed to read the event: error: expected = %v, got = %v", testEvent, event)
			}
		}
	} else {
		t.Fatalf("ListN() Expected: 10, got %d", len(eventKeys))
	}
}

// TestMemoryStoreDel - Tests for store.Del.
func TestMemoryStoreDel(t *testing.T) {
	store := NewMemoryStore(20)
	defer func() {
		store = nil
	}()
	for i := 0; i < 20; i++ {
		if err := store.Put(testEvent); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	eventKeys := store.ListN(-1)
	if len(eventKeys) == 20 {
		for _, key := range eventKeys {
			_ = store.Del(key)
		}
	} else {
		t.Fatalf("ListN() Expected: 20, got %d", len(eventKeys))
	}

	if len(store.ListN(-1)) != 0 {
		t.Fatalf("ListN() Expected: 0, got %d", len(store.ListN(-1)))
	}
}

// TestMemoryStoreLimit - tests for store limit.
func TestMemoryStoreLimit(t *testing.T) {
	store := NewMemoryStore(5)
	defer func() {
		store = nil
	}()
	for i := 0; i < 5; i++ {
		if err := store.Put(testEvent); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	if err := store.Put(testEvent); err == nil {
		t.Fatalf("Expected to fail with %s, but passes", errLimitExceeded)
	}
}

// TestMemoryStoreListN - tests for store.ListN.
func TestMemoryStoreListN(t *testing.T) {
	store := NewMemoryStore(10)
	defer func() {
		store = nil
	}()
	for i := 0; i < 10; i++ {
		if err := store.Put(testEvent); err != nil {
			t.Fatal("Failed to put to queue store ", err)
		}
	}
	if len(store.ListN(5)) != 5 {
		t.Fatalf("ListN(5) Expected: 5, got %d", len(store.ListN(5)))
	}
	if len(store.ListN(-1)) != 10 {
		t.Fatalf("ListN(-1) Expected: 10, got %d", len(store.ListN(-1)))
	}
}
