// Copyright (c) 2015-2023 MinIO, Inc.
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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/bytebufferpool"
)

const (
	defaultLimit = 100000 // Default store limit.
	defaultExt   = ".unknown"
)

// errLimitExceeded error is sent when the maximum limit is reached.
var errLimitExceeded = errors.New("the maximum store limit reached")

// QueueStore - Filestore for persisting items.
type QueueStore[_ any] struct {
	sync.RWMutex
	entryLimit uint64
	directory  string
	fileExt    string

	entries map[string]int64 // key -> modtime as unix nano
}

// NewQueueStore - Creates an instance for QueueStore.
func NewQueueStore[I any](directory string, limit uint64, ext string) *QueueStore[I] {
	if limit == 0 {
		limit = defaultLimit
	}

	if ext == "" {
		ext = defaultExt
	}

	return &QueueStore[I]{
		directory:  directory,
		entryLimit: limit,
		fileExt:    ext,
		entries:    make(map[string]int64, limit),
	}
}

// Open - Creates the directory if not present.
func (store *QueueStore[_]) Open() error {
	store.Lock()
	defer store.Unlock()

	if err := os.MkdirAll(store.directory, os.FileMode(0o770)); err != nil {
		return err
	}

	files, err := store.list()
	if err != nil {
		return err
	}

	// Truncate entries.
	if uint64(len(files)) > store.entryLimit {
		files = files[:store.entryLimit]
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		key := strings.TrimSuffix(file.Name(), store.fileExt)
		if fi, err := file.Info(); err == nil {
			store.entries[key] = fi.ModTime().UnixNano()
		}
	}

	return nil
}

// Delete - Remove the store directory from disk
func (store *QueueStore[_]) Delete() error {
	return os.Remove(store.directory)
}

// PutMultiple - puts an item to the store.
func (store *QueueStore[I]) PutMultiple(item []I) error {
	store.Lock()
	defer store.Unlock()
	if uint64(len(store.entries)) >= store.entryLimit {
		return errLimitExceeded
	}
	// Generate a new UUID for the key.
	key, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	return store.multiWrite(key.String(), item)
}

// write - writes an item to the directory.
func (store *QueueStore[I]) multiWrite(key string, item []I) error {

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	enc := jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(buf)

	for i := range item {
		err := enc.Encode(item[i])
		if err != nil {
			return err
		}
	}
	b := buf.Bytes()

	path := filepath.Join(store.directory, key+store.fileExt)
	err := os.WriteFile(path, b, os.FileMode(0o770))
	buf.Reset()
	if err != nil {
		return err
	}

	// Increment the item count.
	store.entries[key] = time.Now().UnixNano()

	return nil
}

// write - writes an item to the directory.
func (store *QueueStore[I]) write(key string, item I) error {
	// Marshalls the item.
	eventData, err := json.Marshal(item)
	if err != nil {
		return err
	}

	path := filepath.Join(store.directory, key+store.fileExt)
	if err := os.WriteFile(path, eventData, os.FileMode(0o770)); err != nil {
		return err
	}

	// Increment the item count.
	store.entries[key] = time.Now().UnixNano()

	return nil
}

// Put - puts an item to the store.
func (store *QueueStore[I]) Put(item I) error {
	store.Lock()
	defer store.Unlock()
	if uint64(len(store.entries)) >= store.entryLimit {
		return errLimitExceeded
	}
	// Generate a new UUID for the key.
	key, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	return store.write(key.String(), item)
}

// GetRaw - gets an item from the store.
func (store *QueueStore[I]) GetRaw(key string) (raw []byte, err error) {
	store.RLock()

	defer func(store *QueueStore[I]) {
		store.RUnlock()
		if err != nil {
			// Upon error we remove the entry.
			store.Del(key)
		}
	}(store)

	raw, err = os.ReadFile(filepath.Join(store.directory, key+store.fileExt))
	if err != nil {
		return
	}

	if len(raw) == 0 {
		return raw, os.ErrNotExist
	}

	return
}

// Get - gets an item from the store.
func (store *QueueStore[I]) Get(key string) (item I, err error) {
	store.RLock()

	defer func(store *QueueStore[I]) {
		store.RUnlock()
		if err != nil {
			// Upon error we remove the entry.
			store.Del(key)
		}
	}(store)

	var eventData []byte
	eventData, err = os.ReadFile(filepath.Join(store.directory, key+store.fileExt))
	if err != nil {
		return item, err
	}

	if len(eventData) == 0 {
		return item, os.ErrNotExist
	}

	if err = json.Unmarshal(eventData, &item); err != nil {
		return item, err
	}

	return item, nil
}

// Del - Deletes an entry from the store.
func (store *QueueStore[_]) Del(key string) error {
	store.Lock()
	defer store.Unlock()
	return store.del(key)
}

// DelList - Deletes a list of entries from the store.
// Returns an error even if one key fails to be deleted.
func (store *QueueStore[_]) DelList(keys []string) error {
	store.Lock()
	defer store.Unlock()

	for _, key := range keys {
		if err := store.del(key); err != nil {
			return err
		}
	}

	return nil
}

// Len returns the entry count.
func (store *QueueStore[_]) Len() int {
	store.RLock()
	l := len(store.entries)
	defer store.RUnlock()
	return l
}

// lockless call
func (store *QueueStore[_]) del(key string) error {
	err := os.Remove(filepath.Join(store.directory, key+store.fileExt))

	// Delete as entry no matter the result
	delete(store.entries, key)

	return err
}

// List - lists all files registered in the store.
func (store *QueueStore[_]) List() ([]string, error) {
	store.RLock()
	l := make([]string, 0, len(store.entries))
	for k := range store.entries {
		l = append(l, k)
	}

	// Sort entries...
	sort.Slice(l, func(i, j int) bool {
		return store.entries[l[i]] < store.entries[l[j]]
	})
	store.RUnlock()

	return l, nil
}

// list will read all entries from disk.
// Entries are returned sorted by modtime, oldest first.
// Underlying entry list in store is *not* updated.
func (store *QueueStore[_]) list() ([]os.DirEntry, error) {
	files, err := os.ReadDir(store.directory)
	if err != nil {
		return nil, err
	}

	// Sort the entries.
	sort.Slice(files, func(i, j int) bool {
		ii, err := files[i].Info()
		if err != nil {
			return false
		}
		ji, err := files[j].Info()
		if err != nil {
			return true
		}
		return ii.ModTime().Before(ji.ModTime())
	})

	return files, nil
}

// Extension will return the file extension used
// for the items stored in the queue.
func (store *QueueStore[_]) Extension() string {
	return store.fileExt
}
