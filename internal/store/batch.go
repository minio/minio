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
	"sync"
	"time"
)

// ErrBatchFull indicates that the batch is full
var ErrBatchFull = errors.New("batch is full")

const defaultCommitTimeout = 30 * time.Second

// Batch represents an ordered batch
type Batch[I any] struct {
	items  []I
	limit  uint32
	store  Store[I]
	quitCh chan struct{}

	sync.Mutex
}

// BatchConfig represents the batch config
type BatchConfig[I any] struct {
	Limit         uint32
	Store         Store[I]
	CommitTimeout time.Duration
	Log           logger
}

// Add adds the item to the batch
func (b *Batch[I]) Add(item I) error {
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

	b.items = append(b.items, item)
	return nil
}

// Len returns the no of items in the batch
func (b *Batch[_]) Len() int {
	b.Lock()
	defer b.Unlock()

	return len(b.items)
}

func (b *Batch[_]) isFull() bool {
	return len(b.items) >= int(b.limit)
}

func (b *Batch[I]) commit() error {
	switch len(b.items) {
	case 0:
		return nil
	case 1:
		_, err := b.store.Put(b.items[0])
		return err
	default:
	}
	if _, err := b.store.PutMultiple(b.items); err != nil {
		return err
	}
	b.items = make([]I, 0, b.limit)
	return nil
}

// Close commits the pending items and quits the goroutines
func (b *Batch[I]) Close() error {
	defer func() {
		close(b.quitCh)
	}()

	b.Lock()
	defer b.Unlock()
	return b.commit()
}

// NewBatch creates a new batch
func NewBatch[I any](config BatchConfig[I]) *Batch[I] {
	if config.CommitTimeout == 0 {
		config.CommitTimeout = defaultCommitTimeout
	}
	quitCh := make(chan struct{})
	batch := &Batch[I]{
		items:  make([]I, 0, config.Limit),
		limit:  config.Limit,
		store:  config.Store,
		quitCh: quitCh,
	}
	if batch.store != nil {
		go func() {
			commitTicker := time.NewTicker(config.CommitTimeout)
			defer commitTicker.Stop()
			for {
				select {
				case <-commitTicker.C:
				case <-batch.quitCh:
					return
				}
				batch.Lock()
				err := batch.commit()
				batch.Unlock()
				if err != nil {
					config.Log(context.Background(), err, "")
				}
			}
		}()
	}
	return batch
}
