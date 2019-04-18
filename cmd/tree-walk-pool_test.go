/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
		for i := 0; i < 10; i++ {
			resultCh := make(chan TreeWalkResult)
			endWalkCh := make(chan struct{})
			tw.Set(params, resultCh, endWalkCh)
		}

		tw.lock.Lock()
		if walks, ok := tw.pool[params]; ok {
			if len(walks) != 10 {
				t.Error("There aren't as many walks as were Set")
			}
		}
		tw.lock.Unlock()
		for i := 0; i < 10; i++ {
			tw.lock.Lock()
			if walks, ok := tw.pool[params]; ok {
				// Before ith Release we should have 10-i treeWalk go-routines.
				if 10-i != len(walks) {
					t.Error("There aren't as many walks as were Set")
				}
			}
			tw.lock.Unlock()
			tw.Release(params)
		}
	}

}
