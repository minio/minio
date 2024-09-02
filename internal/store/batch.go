// Copyright (c) 2023 MinIO, Inc.
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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrBatchFull indicates that the batch is full
var ErrBatchFull = errors.New("batch is full")

const defaultCommitTimeout = 30 * time.Second

type key interface {
	string | int | int64
}

// Batch represents an ordered batch
type Batch[K key, T any] struct {
	keys  []K
	items map[K]T
	limit uint32
	store Store[T]

	sync.Mutex
}

// BatchConfig represents the batch config
type BatchConfig[I any] struct {
	Limit         uint32
	Store         Store[I]
	CommitTimeout time.Duration
	Log           logger
	DoneCh        <-chan struct{}
}

// Add adds the item to the batch
func (b *Batch[K, T]) Add(key K, item T) error {
	b.Lock()
	defer b.Unlock()

	if b.isFull() {
		if b.store == nil {
			return ErrBatchFull
		}
		// commit batch to store
		if err := b.commit(); err != nil {
			return err
		}
	}

	if _, ok := b.items[key]; !ok {
		b.keys = append(b.keys, key)
	}
	b.items[key] = item

	return nil
}

// Len returns the no of items in the batch
func (b *Batch[K, T]) Len() int {
	b.Lock()
	defer b.Unlock()

	return len(b.keys)
}

func (b *Batch[K, T]) isFull() bool {
	return len(b.items) >= int(b.limit)
}

func (b *Batch[K, T]) commit() error {
	switch len(b.keys) {
	case 0:
		return nil
	case 1:
		item, ok := b.items[b.keys[0]]
		if !ok {
			return fmt.Errorf("item not found for the key: %v; should not happen;", b.keys[0])
		}
		return b.store.Put(item)
	default:
	}

	orderedKeys := append([]K(nil), b.keys...)
	var orderedItems []T
	for _, key := range orderedKeys {
		item, ok := b.items[key]
		if !ok {
			return fmt.Errorf("item not found for the key: %v; should not happen;", key)
		}
		orderedItems = append(orderedItems, item)
	}
	if err := b.store.PutMultiple(orderedItems); err != nil {
		return err
	}

	b.keys = make([]K, 0, b.limit)
	b.items = make(map[K]T, b.limit)

	return nil
}

// NewBatch creates a new batch
func NewBatch[K key, T any](config BatchConfig[T]) *Batch[K, T] {
	if config.CommitTimeout == 0 {
		config.CommitTimeout = defaultCommitTimeout
	}
	batch := &Batch[K, T]{
		keys:  make([]K, 0, config.Limit),
		items: make(map[K]T, config.Limit),
		limit: config.Limit,
		store: config.Store,
	}
	if batch.store != nil {
		go func() {
			var done bool
			commitTicker := time.NewTicker(config.CommitTimeout)
			defer commitTicker.Stop()
			for {
				select {
				case <-commitTicker.C:
				case <-config.DoneCh:
					done = true
				}
				batch.Lock()
				err := batch.commit()
				batch.Unlock()
				if err != nil {
					config.Log(context.Background(), err, "")
				}
				if done {
					return
				}
			}
		}()
	}
	return batch
}
