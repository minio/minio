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
	"testing"
)

func TestBatch(t *testing.T) {
	var limit uint32 = 100
	batch := NewBatch[int, int](limit)
	for i := 0; i < int(limit); i++ {
		if err := batch.Add(i, i); err != nil {
			t.Fatalf("failed to add %v; %v", i, err)
		}
		if _, ok := batch.GetByKey(i); !ok {
			t.Fatalf("failed to get the item by key %v after adding", i)
		}
	}
	err := batch.Add(101, 101)
	if err == nil || !errors.Is(err, ErrBatchFull) {
		t.Fatalf("Expected err %v but got %v", ErrBatchFull, err)
	}
	if !batch.IsFull() {
		t.Fatal("Expected batch.IsFull to be true but got false")
	}
	batchLen := batch.Len()
	if batchLen != int(limit) {
		t.Fatalf("expected batch length to be %v but got %v", limit, batchLen)
	}
	keys, items, err := batch.GetAll()
	if err != nil {
		t.Fatalf("unable to get the items from the batch; %v", err)
	}
	if len(items) != int(limit) {
		t.Fatalf("Expected length of the batch items to be %v but got %v", limit, len(items))
	}
	if len(keys) != int(limit) {
		t.Fatalf("Expected length of the batch keys to be %v but got %v", limit, len(items))
	}
	batchLen = batch.Len()
	if batchLen != 0 {
		t.Fatalf("expected batch to be empty but still left with %d items", batchLen)
	}
	// Add duplicate entries
	for i := 0; i < 10; i++ {
		if err := batch.Add(99, 99); err != nil {
			t.Fatalf("failed to add duplicate item %v to batch after Get; %v", i, err)
		}
	}
	if _, ok := batch.GetByKey(99); !ok {
		t.Fatal("failed to get the duplicxate item by key '99' after adding")
	}
	keys, items, err = batch.GetAll()
	if err != nil {
		t.Fatalf("unable to get the items from the batch; %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("Expected length of the batch items to be 1 but got %v", len(items))
	}
	if len(keys) != 1 {
		t.Fatalf("Expected length of the batch keys to be 1 but got %v", len(items))
	}
	// try adding again after Get.
	for i := 0; i < int(limit); i++ {
		if err := batch.Add(i, i); err != nil {
			t.Fatalf("failed to add item %v to batch after Get; %v", i, err)
		}
		if _, ok := batch.GetByKey(i); !ok {
			t.Fatalf("failed to get the item by key %v after adding", i)
		}
	}
}

func TestBatchWithConcurrency(t *testing.T) {
	var limit uint32 = 100
	batch := NewBatch[int, int](limit)

	var wg sync.WaitGroup
	for i := 0; i < int(limit); i++ {
		wg.Add(1)
		go func(item int) {
			defer wg.Done()
			if err := batch.Add(item, item); err != nil {
				t.Errorf("failed to add item %v; %v", item, err)
				return
			}
			if _, ok := batch.GetByKey(item); !ok {
				t.Errorf("failed to get the item by key %v after adding", item)
			}
		}(i)
	}
	wg.Wait()

	keys, items, err := batch.GetAll()
	if err != nil {
		t.Fatalf("unable to get the items from the batch; %v", err)
	}
	if len(items) != int(limit) {
		t.Fatalf("expected batch length %v but got %v", limit, len(items))
	}
	if len(keys) != int(limit) {
		t.Fatalf("Expected length of the batch keys to be %v but got %v", limit, len(items))
	}
	batchLen := batch.Len()
	if batchLen != 0 {
		t.Fatalf("expected batch to be empty but still left with %d items", batchLen)
	}
}
