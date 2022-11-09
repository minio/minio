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
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/internal/event"
)

const (
	defaultLimit = 100000 // Default store limit.
	eventExt     = ".event"
)

// QueueStore - Filestore for persisting events.
type QueueStore struct {
	sync.RWMutex
	entryLimit uint64
	directory  string

	entries map[string]int64 // key -> modtime as unix nano
}

// NewQueueStore - Creates an instance for QueueStore.
func NewQueueStore(directory string, limit uint64) Store {
	if limit == 0 {
		limit = defaultLimit
	}

	return &QueueStore{
		directory:  directory,
		entryLimit: limit,
		entries:    make(map[string]int64, limit),
	}
}

// Open - Creates the directory if not present.
func (store *QueueStore) Open() error {
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
		key := strings.TrimSuffix(file.Name(), eventExt)
		if fi, err := file.Info(); err == nil {
			store.entries[key] = fi.ModTime().UnixNano()
		}
	}

	return nil
}

// write - writes event to the directory.
func (store *QueueStore) write(key string, e event.Event) error {
	// Marshalls the event.
	eventData, err := json.Marshal(e)
	if err != nil {
		return err
	}

	path := filepath.Join(store.directory, key+eventExt)
	if err := os.WriteFile(path, eventData, os.FileMode(0o770)); err != nil {
		return err
	}

	// Increment the event count.
	store.entries[key] = time.Now().UnixNano()

	return nil
}

// Put - puts a event to the store.
func (store *QueueStore) Put(e event.Event) error {
	store.Lock()
	defer store.Unlock()
	if uint64(len(store.entries)) >= store.entryLimit {
		return errLimitExceeded
	}
	key, err := getNewUUID()
	if err != nil {
		return err
	}
	return store.write(key, e)
}

// Get - gets a event from the store.
func (store *QueueStore) Get(key string) (event event.Event, err error) {
	store.RLock()

	defer func(store *QueueStore) {
		store.RUnlock()
		if err != nil {
			// Upon error we remove the entry.
			store.Del(key)
		}
	}(store)

	var eventData []byte
	eventData, err = os.ReadFile(filepath.Join(store.directory, key+eventExt))
	if err != nil {
		return event, err
	}

	if len(eventData) == 0 {
		return event, os.ErrNotExist
	}

	if err = json.Unmarshal(eventData, &event); err != nil {
		return event, err
	}

	return event, nil
}

// Del - Deletes an entry from the store.
func (store *QueueStore) Del(key string) error {
	store.Lock()
	defer store.Unlock()
	return store.del(key)
}

// Len returns the entry count.
func (store *QueueStore) Len() int {
	store.RLock()
	l := len(store.entries)
	defer store.RUnlock()
	return l
}

// lockless call
func (store *QueueStore) del(key string) error {
	err := os.Remove(filepath.Join(store.directory, key+eventExt))

	// Delete as entry no matter the result
	delete(store.entries, key)

	return err
}

// List - lists all files registered in the store.
func (store *QueueStore) List() ([]string, error) {
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
func (store *QueueStore) list() ([]os.DirEntry, error) {
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
