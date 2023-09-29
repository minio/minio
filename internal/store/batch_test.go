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
	batch := NewBatch[int](limit)
	for i := 0; i < int(limit); i++ {
		if err := batch.Add(i); err != nil {
			t.Fatalf("failed to add %v; %v", i, err)
		}
	}
	err := batch.Add(101)
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
	items := batch.Get()
	if len(items) != int(limit) {
		t.Fatalf("Expected length of the batch to be %v but got %v", limit, len(items))
	}
	batchLen = batch.Len()
	if batchLen != 0 {
		t.Fatalf("expected batch to be empty but still left with %d items", batchLen)
	}
	// try adding again after Get.
	for i := 0; i < int(limit); i++ {
		if err := batch.Add(i); err != nil {
			t.Fatalf("failed to add %v; %v", i, err)
		}
	}
}

func TestBatchWithConcurrency(t *testing.T) {
	var limit uint32 = 100
	batch := NewBatch[int](limit)

	var wg sync.WaitGroup
	for i := 0; i < int(limit); i++ {
		wg.Add(1)
		go func(item int) {
			defer wg.Done()
			if err := batch.Add(item); err != nil {
				t.Errorf("failed to add item %v; %v", item, err)
				return
			}
		}(i)
	}
	wg.Wait()

	items := batch.Get()
	if len(items) != int(limit) {
		t.Fatalf("expected batch length %v but got %v", limit, len(items))
	}
	batchLen := batch.Len()
	if batchLen != 0 {
		t.Fatalf("expected batch to be empty but still left with %d items", batchLen)
	}
}
