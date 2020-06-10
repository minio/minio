/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
 *
 */

package kv

import (
	"context"
	"strings"
	"sync"

	"github.com/minio/minio-go/v6/pkg/set"
)

// NewInmemoryBackend creates a new in memory key store.
// The inmemory backend is a example and test implementation of the Backend API.
// It is not meant for production use.
func NewInmemoryBackend() *InmemoryBackend {
	events := make(chan Event, 10)
	return &InmemoryBackend{
		values: map[string]value{},
		events: events,
		lock:   sync.RWMutex{},
	}
}

type value []byte

// InmemoryBackend simple inmmory backend.
type InmemoryBackend struct {
	values map[string]value
	events chan Event
	lock   sync.RWMutex
}

// Save saves key/value pair
func (b *InmemoryBackend) Save(ctx context.Context, key string, data []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.values[key] = data
	e := Event{
		Key:  key,
		Type: EventTypeCreated,
	}
	b.events <- e
	return nil
}

// Delete deletes key
func (b *InmemoryBackend) Delete(ctx context.Context, key string) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	delete(b.values, key)
	e := Event{
		Key:  key,
		Type: EventTypeDeleted,
	}
	b.events <- e

	return nil
}

// Keys returns child keys for given key
func (b *InmemoryBackend) Keys(ctx context.Context, key string) (set.StringSet, error) {
	return b.KeysFilter(ctx, key, KeyTransformTrimBase)
}

// KeysFilter returns child keys for given key filtered by given KeyTransform
func (b *InmemoryBackend) KeysFilter(ctx context.Context, key string, transform KeyTransform) (set.StringSet, error) {
	result := set.NewStringSet()
	keys := b.getKeys(key)
	for _, k := range keys {
		newkey := transform(key, k)
		// skip key if transform returns a empty key
		if newkey != "" {
			result.Add(newkey)
		}
	}
	return result, nil
}

// Get get value for given key
func (b *InmemoryBackend) Get(ctx context.Context, key string) ([]byte, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.values[key], nil
}

// Watch registers a event handler to key value store
func (b *InmemoryBackend) Watch(ctx context.Context, key string, handler EventHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-b.events:
			handler(&resp)
		}
	}
}

// internal functon to iterate keys.
func (b *InmemoryBackend) getKeys(key string) []string {
	b.lock.RLock()
	defer b.lock.RUnlock()
	keys := make([]string, 0, len(b.values))
	for k := range b.values {
		if strings.HasPrefix(k, key) {
			keys = append(keys, k)
		}
	}
	return keys
}

// Info returns backend information
func (b *InmemoryBackend) Info() string {
	return "inmemory"
}
