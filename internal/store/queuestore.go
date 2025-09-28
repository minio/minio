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
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/s2"
	"github.com/valyala/bytebufferpool"
)

const (
	defaultLimit = 100000 // Default store limit.
	defaultExt   = ".unknown"
	compressExt  = ".snappy"
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

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if fi, err := file.Info(); err == nil {
			store.entries[file.Name()] = fi.ModTime().UnixNano()
		}
	}

	return nil
}

// Delete - Remove the store directory from disk
func (store *QueueStore[_]) Delete() error {
	return os.Remove(store.directory)
}

// PutMultiple - puts an item to the store.
func (store *QueueStore[I]) PutMultiple(items []I) (Key, error) {
	// Generate a new UUID for the key.
	uid, err := uuid.NewRandom()
	if err != nil {
		return Key{}, err
	}

	store.Lock()
	defer store.Unlock()
	if uint64(len(store.entries)) >= store.entryLimit {
		return Key{}, errLimitExceeded
	}
	key := Key{
		Name:      uid.String(),
		ItemCount: len(items),
		Compress:  true,
		Extension: store.fileExt,
	}
	return key, store.multiWrite(key, items)
}

// multiWrite - writes an item to the directory.
func (store *QueueStore[I]) multiWrite(key Key, items []I) (err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	enc := jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(buf)

	for i := range items {
		if err = enc.Encode(items[i]); err != nil {
			return err
		}
	}

	path := filepath.Join(store.directory, key.String())
	if key.Compress {
		err = os.WriteFile(path, s2.Encode(nil, buf.Bytes()), os.FileMode(0o770))
	} else {
		err = os.WriteFile(path, buf.Bytes(), os.FileMode(0o770))
	}

	buf.Reset()
	if err != nil {
		return err
	}

	// Increment the item count.
	store.entries[key.String()] = time.Now().UnixNano()

	return err
}

// write - writes an item to the directory.
func (store *QueueStore[I]) write(key Key, item I) error {
	// Marshals the item.
	eventData, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return store.writeBytes(key, eventData)
}

// writeBytes - writes bytes to the directory.
func (store *QueueStore[I]) writeBytes(key Key, b []byte) (err error) {
	path := filepath.Join(store.directory, key.String())

	if key.Compress {
		err = os.WriteFile(path, s2.Encode(nil, b), os.FileMode(0o770))
	} else {
		err = os.WriteFile(path, b, os.FileMode(0o770))
	}

	if err != nil {
		return err
	}
	// Increment the item count.
	store.entries[key.String()] = time.Now().UnixNano()
	return nil
}

// Put - puts an item to the store.
func (store *QueueStore[I]) Put(item I) (Key, error) {
	store.Lock()
	defer store.Unlock()
	if uint64(len(store.entries)) >= store.entryLimit {
		return Key{}, errLimitExceeded
	}
	// Generate a new UUID for the key.
	uid, err := uuid.NewRandom()
	if err != nil {
		return Key{}, err
	}
	key := Key{
		Name:      uid.String(),
		Extension: store.fileExt,
		ItemCount: 1,
	}
	return key, store.write(key, item)
}

// PutRaw - puts the raw bytes to the store
func (store *QueueStore[I]) PutRaw(b []byte) (Key, error) {
	store.Lock()
	defer store.Unlock()
	if uint64(len(store.entries)) >= store.entryLimit {
		return Key{}, errLimitExceeded
	}
	// Generate a new UUID for the key.
	uid, err := uuid.NewRandom()
	if err != nil {
		return Key{}, err
	}
	key := Key{
		Name:      uid.String(),
		Extension: store.fileExt,
	}
	return key, store.writeBytes(key, b)
}

// GetRaw - gets an item from the store.
func (store *QueueStore[I]) GetRaw(key Key) (raw []byte, err error) {
	store.RLock()

	defer func(store *QueueStore[I]) {
		store.RUnlock()
		if err != nil && !os.IsNotExist(err) {
			// Upon error we remove the entry.
			store.Del(key)
		}
	}(store)

	raw, err = os.ReadFile(filepath.Join(store.directory, key.String()))
	if err != nil {
		return raw, err
	}

	if len(raw) == 0 {
		return raw, os.ErrNotExist
	}

	if key.Compress {
		raw, err = s2.Decode(nil, raw)
	}

	return raw, err
}

// Get - gets an item from the store.
func (store *QueueStore[I]) Get(key Key) (item I, err error) {
	items, err := store.GetMultiple(key)
	if err != nil {
		return item, err
	}
	return items[0], nil
}

// GetMultiple will read the multi payload file and fetch the items
func (store *QueueStore[I]) GetMultiple(key Key) (items []I, err error) {
	raw, err := store.GetRaw(key)
	if err != nil {
		return nil, err
	}

	decoder := jsoniter.ConfigCompatibleWithStandardLibrary.NewDecoder(bytes.NewReader(raw))
	for decoder.More() {
		var item I
		if err := decoder.Decode(&item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}

	return items, err
}

// Del - Deletes an entry from the store.
func (store *QueueStore[_]) Del(key Key) error {
	store.Lock()
	defer store.Unlock()
	return store.del(key)
}

// Len returns the entry count.
func (store *QueueStore[_]) Len() int {
	store.RLock()
	l := len(store.entries)
	defer store.RUnlock()
	return l
}

// lockless call
func (store *QueueStore[_]) del(key Key) error {
	err := os.Remove(filepath.Join(store.directory, key.String()))

	// Delete as entry no matter the result
	delete(store.entries, key.String())

	return err
}

// List - lists all files registered in the store.
func (store *QueueStore[_]) List() (keys []Key) {
	store.RLock()
	defer store.RUnlock()

	entries := make([]string, 0, len(store.entries))
	for entry := range store.entries {
		entries = append(entries, entry)
	}

	// Sort entries...
	sort.Slice(entries, func(i, j int) bool {
		return store.entries[entries[i]] < store.entries[entries[j]]
	})

	for i := range entries {
		keys = append(keys, parseKey(entries[i]))
	}

	return keys
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
