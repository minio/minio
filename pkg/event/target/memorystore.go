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
	"sync"

	"github.com/minio/minio/pkg/event"
)

const (
	maxStoreLimit = 10000
)

// MemoryStore persists events in memory.
type MemoryStore struct {
	sync.RWMutex
	events map[string]event.Event
	eC     uint16
	limit  uint16
}

// NewMemoryStore creates a memory store instance.
func NewMemoryStore(limit uint16) *MemoryStore {
	if limit == 0 || limit > maxStoreLimit {
		limit = maxStoreLimit
	}
	memoryStore := &MemoryStore{
		events: make(map[string]event.Event),
		limit:  limit,
	}
	return memoryStore
}

// Open is in-effective here.
// Implemented for interface compatibility.
func (store *MemoryStore) Open() error {
	return nil
}

// Put - puts the event in store.
func (store *MemoryStore) Put(e event.Event) error {
	store.Lock()
	defer store.Unlock()
	if store.eC == store.limit {
		return errLimitExceeded
	}
	key, kErr := getNewUUID()
	if kErr != nil {
		return kErr
	}
	store.events[key] = e
	store.eC++
	return nil
}

// Get - retrieves the event from store.
func (store *MemoryStore) Get(key string) (event.Event, error) {
	store.RLock()
	defer store.RUnlock()

	if event, exist := store.events[key]; exist {
		return event, nil
	}

	return event.Event{}, errNoSuchKey
}

// Del - deletes the event from store.
func (store *MemoryStore) Del(key string) error {
	store.Lock()
	defer store.Unlock()

	delete(store.events, key)

	store.eC--

	return nil
}

// ListN - lists atmost N keys in the store.
func (store *MemoryStore) ListN(n int) []string {
	store.RLock()
	defer store.RUnlock()

	var i int

	if n == -1 {
		n = len(store.events)
	}

	keys := []string{}
	for k := range store.events {
		if i < n {
			keys = append(keys, k)
			i++
			continue
		} else {
			break
		}
	}

	return keys
}
