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
	"errors"
	"sync"
)

// ErrBatchFull indicates that the batch is full
var ErrBatchFull = errors.New("batch is full")

// Batch can be used to replay the events in batches
type Batch[T any] struct {
	items []T
	limit uint32

	sync.Mutex
}

// Add adds the item to the batch
func (b *Batch[T]) Add(item T) error {
	b.Lock()
	defer b.Unlock()

	if b.isFull() {
		return ErrBatchFull
	}

	b.items = append(b.items, item)
	return nil
}

// Get fetches the items and resets the batch
func (b *Batch[T]) Get() (items []T) {
	b.Lock()
	defer b.Unlock()

	items = append([]T(nil), b.items...)
	b.items = make([]T, 0, b.limit)

	return
}

// Len returns the no of items in the batch
func (b *Batch[T]) Len() int {
	b.Lock()
	defer b.Unlock()

	return len(b.items)
}

// IsFull checks if the batch is full or not
func (b *Batch[T]) IsFull() bool {
	b.Lock()
	defer b.Unlock()

	return b.isFull()
}

func (b *Batch[T]) isFull() bool {
	return len(b.items) >= int(b.limit)
}

// NewBatch creates a new batch
func NewBatch[T any](limit uint32) *Batch[T] {
	return &Batch[T]{
		items: make([]T, 0, limit),
		limit: limit,
	}
}
