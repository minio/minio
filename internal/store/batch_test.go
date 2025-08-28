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
	"sync"
	"testing"
	"time"
)

func TestBatchCommit(t *testing.T) {
	defer func() {
		if err := tearDownQueueStore(); err != nil {
			t.Fatalf("Failed to tear down store; %v", err)
		}
	}()
	store, err := setUpQueueStore(queueDir, 100)
	if err != nil {
		t.Fatalf("Failed to create a queue store; %v", err)
	}

	var limit uint32 = 100

	batch := NewBatch[TestItem](BatchConfig[TestItem]{
		Limit:         limit,
		Store:         store,
		CommitTimeout: 5 * time.Minute,
		Log: func(ctx context.Context, err error, id string, errKind ...any) {
			t.Log(err)
		},
	})
	defer batch.Close()

	for i := 0; i < int(limit); i++ {
		if err := batch.Add(testItem); err != nil {
			t.Fatalf("failed to add %v; %v", i, err)
		}
	}

	batchLen := batch.Len()
	if batchLen != int(limit) {
		t.Fatalf("Expected batch.Len() %v; but got %v", limit, batchLen)
	}

	keys := store.List()
	if len(keys) > 0 {
		t.Fatalf("Expected empty store list but got len(list) %v", len(keys))
	}
	if err := batch.Add(testItem); err != nil {
		t.Fatalf("unable to add to the batch; %v", err)
	}
	batchLen = batch.Len()
	if batchLen != 1 {
		t.Fatalf("expected batch length to be 1 but got %v", batchLen)
	}
	keys = store.List()
	if len(keys) != 1 {
		t.Fatalf("expected len(store.List())=1; but got %v", len(keys))
	}
	key := keys[0]
	if !key.Compress {
		t.Fatal("expected key.Compress=true; but got false")
	}
	if key.ItemCount != int(limit) {
		t.Fatalf("expected key.ItemCount=%d; but got %v", limit, key.ItemCount)
	}
	items, err := store.GetMultiple(key)
	if err != nil {
		t.Fatalf("unable to read key %v; %v", key.String(), err)
	}
	if len(items) != int(limit) {
		t.Fatalf("expected len(items)=%d; but got %v", limit, len(items))
	}
}

func TestBatchCommitOnExit(t *testing.T) {
	defer func() {
		if err := tearDownQueueStore(); err != nil {
			t.Fatalf("Failed to tear down store; %v", err)
		}
	}()
	store, err := setUpQueueStore(queueDir, 100)
	if err != nil {
		t.Fatalf("Failed to create a queue store; %v", err)
	}

	var limit uint32 = 100

	batch := NewBatch[TestItem](BatchConfig[TestItem]{
		Limit:         limit,
		Store:         store,
		CommitTimeout: 5 * time.Minute,
		Log: func(ctx context.Context, err error, id string, errKind ...any) {
			t.Log([]any{err, id, errKind}...)
		},
	})

	for i := 0; i < int(limit); i++ {
		if err := batch.Add(testItem); err != nil {
			t.Fatalf("failed to add %v; %v", i, err)
		}
	}

	batch.Close()
	time.Sleep(1 * time.Second)

	batchLen := batch.Len()
	if batchLen != 0 {
		t.Fatalf("Expected batch.Len()=0; but got %v", batchLen)
	}

	keys := store.List()
	if len(keys) != 1 {
		t.Fatalf("expected len(store.List())=1; but got %v", len(keys))
	}

	key := keys[0]
	if !key.Compress {
		t.Fatal("expected key.Compress=true; but got false")
	}
	if key.ItemCount != int(limit) {
		t.Fatalf("expected key.ItemCount=%d; but got %v", limit, key.ItemCount)
	}
	items, err := store.GetMultiple(key)
	if err != nil {
		t.Fatalf("unable to read key %v; %v", key.String(), err)
	}
	if len(items) != int(limit) {
		t.Fatalf("expected len(items)=%d; but got %v", limit, len(items))
	}
}

func TestBatchWithConcurrency(t *testing.T) {
	defer func() {
		if err := tearDownQueueStore(); err != nil {
			t.Fatalf("Failed to tear down store; %v", err)
		}
	}()
	store, err := setUpQueueStore(queueDir, 100)
	if err != nil {
		t.Fatalf("Failed to create a queue store; %v", err)
	}

	var limit uint32 = 100

	batch := NewBatch[TestItem](BatchConfig[TestItem]{
		Limit:         limit,
		Store:         store,
		CommitTimeout: 5 * time.Minute,
		Log: func(ctx context.Context, err error, id string, errKind ...any) {
			t.Log(err)
		},
	})
	defer batch.Close()

	var wg sync.WaitGroup
	for i := 0; i < int(limit); i++ {
		wg.Add(1)
		go func(key int) {
			defer wg.Done()
			if err := batch.Add(testItem); err != nil {
				t.Errorf("failed to add item %v; %v", key, err)
				return
			}
		}(i)
	}
	wg.Wait()

	batchLen := batch.Len()
	if batchLen != int(limit) {
		t.Fatalf("Expected batch.Len() %v; but got %v", limit, batchLen)
	}

	keys := store.List()
	if len(keys) > 0 {
		t.Fatalf("Expected empty store list but got len(list) %v", len(keys))
	}
	if err := batch.Add(testItem); err != nil {
		t.Fatalf("unable to add to the batch; %v", err)
	}
	batchLen = batch.Len()
	if batchLen != 1 {
		t.Fatalf("expected batch length to be 1 but got %v", batchLen)
	}
	keys = store.List()
	if len(keys) != 1 {
		t.Fatalf("expected len(store.List())=1; but got %v", len(keys))
	}
	key := keys[0]
	if !key.Compress {
		t.Fatal("expected key.Compress=true; but got false")
	}
	if key.ItemCount != int(limit) {
		t.Fatalf("expected key.ItemCount=%d; but got %v", limit, key.ItemCount)
	}
	items, err := store.GetMultiple(key)
	if err != nil {
		t.Fatalf("unable to read key %v; %v", key.String(), err)
	}
	if len(items) != int(limit) {
		t.Fatalf("expected len(items)=%d; but got %v", limit, len(items))
	}
}
