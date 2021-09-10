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

package cmd

import (
	"testing"
	"time"
)

// Test if tree walker go-routine is removed from the pool after timeout
// and that is available in the pool before the timeout.
func TestTreeWalkPoolBasic(t *testing.T) {
	// Create a treeWalkPool
	tw := NewTreeWalkPool(1 * time.Second)

	// Create sample params
	params := listParams{
		bucket: "test-bucket",
	}

	// Add a treeWalk to the pool
	resultCh := make(chan TreeWalkResult)
	endWalkCh := make(chan struct{})
	tw.Set(params, resultCh, endWalkCh)

	// Wait for treeWalkPool timeout to happen
	<-time.After(2 * time.Second)
	if c1, _ := tw.Release(params); c1 != nil {
		t.Error("treeWalk go-routine must have been freed")
	}

	// Add the treeWalk back to the pool
	tw.Set(params, resultCh, endWalkCh)

	// Release the treeWalk before timeout
	select {
	case <-time.After(1 * time.Second):
		break
	default:
		if c1, _ := tw.Release(params); c1 == nil {
			t.Error("treeWalk go-routine got freed before timeout")
		}
	}
}

// Test if multiple tree walkers for the same listParams are managed as expected by the pool.
func TestManyWalksSameParam(t *testing.T) {
	// Create a treeWalkPool.
	tw := NewTreeWalkPool(5 * time.Second)

	// Create sample params.
	params := listParams{
		bucket: "test-bucket",
	}

	select {
	// This timeout is an upper-bound. This is started
	// before the first treeWalk go-routine's timeout period starts.
	case <-time.After(5 * time.Second):
		break
	default:
		// Create many treeWalk go-routines for the same params.
		for i := 0; i < treeWalkSameEntryLimit; i++ {
			resultCh := make(chan TreeWalkResult)
			endWalkCh := make(chan struct{})
			tw.Set(params, resultCh, endWalkCh)
		}

		tw.mu.Lock()
		if walks, ok := tw.pool[params]; ok {
			if len(walks) != treeWalkSameEntryLimit {
				t.Error("There aren't as many walks as were Set")
			}
		}
		tw.mu.Unlock()
		for i := 0; i < treeWalkSameEntryLimit; i++ {
			tw.mu.Lock()
			if walks, ok := tw.pool[params]; ok {
				// Before ith Release we should have n-i treeWalk go-routines.
				if treeWalkSameEntryLimit-i != len(walks) {
					t.Error("There aren't as many walks as were Set")
				}
			}
			tw.mu.Unlock()
			tw.Release(params)
		}
	}
}

// Test if multiple tree walkers for the same listParams are managed as expected by the pool
// but that treeWalkSameEntryLimit is respected.
func TestManyWalksSameParamPrune(t *testing.T) {
	// Create a treeWalkPool.
	tw := NewTreeWalkPool(5 * time.Second)

	// Create sample params.
	params := listParams{
		bucket: "test-bucket",
	}

	select {
	// This timeout is an upper-bound. This is started
	// before the first treeWalk go-routine's timeout period starts.
	case <-time.After(5 * time.Second):
		break
	default:
		// Create many treeWalk go-routines for the same params.
		for i := 0; i < treeWalkSameEntryLimit*4; i++ {
			resultCh := make(chan TreeWalkResult)
			endWalkCh := make(chan struct{})
			tw.Set(params, resultCh, endWalkCh)
		}

		tw.mu.Lock()
		if walks, ok := tw.pool[params]; ok {
			if len(walks) != treeWalkSameEntryLimit {
				t.Error("There aren't as many walks as were Set")
			}
		}
		tw.mu.Unlock()
		for i := 0; i < treeWalkSameEntryLimit; i++ {
			tw.mu.Lock()
			if walks, ok := tw.pool[params]; ok {
				// Before ith Release we should have n-i treeWalk go-routines.
				if treeWalkSameEntryLimit-i != len(walks) {
					t.Error("There aren't as many walks as were Set")
				}
			}
			tw.mu.Unlock()
			tw.Release(params)
		}
	}
}
