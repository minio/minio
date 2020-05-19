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
	"context"
	"runtime"
	"testing"
	"time"
)

// WARNING:
//
// Expected source line number is hard coded, 31, in the
// following test. Adding new code before this test or changing its
// position will cause the line number to change and the test to FAIL
// Tests getSource().
func TestGetSource(t *testing.T) {
	currentSource := func() string { return getSource(2) }
	gotSource := currentSource()
	// Hard coded line number, 34, in the "expectedSource" value
	expectedSource := "[namespace-lock_test.go:34:TestGetSource()]"
	if gotSource != expectedSource {
		t.Errorf("expected : %s, got : %s", expectedSource, gotSource)
	}
}

// Test lock race
func TestNSLockRace(t *testing.T) {
	ctx := context.Background()

	for i := 0; i < 10000; i++ {
		nsLk := newNSLock(false)

		// lk1; ref=1
		if !nsLk.lock(ctx, "volume", "path", "source", "opsID", false, time.Second) {
			t.Fatal("failed to acquire lock")
		}

		// lk2
		lk2ch := make(chan struct{})
		go func() {
			defer close(lk2ch)
			nsLk.lock(ctx, "volume", "path", "source", "opsID", false, 1*time.Millisecond)
		}()
		time.Sleep(1 * time.Millisecond) // wait for goroutine to advance; ref=2

		// Unlock the 1st lock; ref=1 after this line
		nsLk.unlock("volume", "path", false)

		// Taking another lockMapMutex here allows queuing up additional lockers. This should
		// not be required but makes reproduction much easier.
		nsLk.lockMapMutex.Lock()

		// lk3 blocks.
		lk3ch := make(chan bool)
		go func() {
			lk3ch <- nsLk.lock(ctx, "volume", "path", "source", "opsID", false, 0)
		}()

		// lk4, blocks.
		lk4ch := make(chan bool)
		go func() {
			lk4ch <- nsLk.lock(ctx, "volume", "path", "source", "opsID", false, 0)
		}()
		runtime.Gosched()

		// unlock the manual lock
		nsLk.lockMapMutex.Unlock()

		// To trigger the race:
		// 1) lk3 or lk4 need to advance and increment the ref on the existing resource,
		//    successfully acquiring the lock.
		// 2) lk2 then needs to advance and remove the resource from lockMap.
		// 3) lk3 or lk4 (whichever didn't execute in step 1) then executes and creates
		//    a new entry in lockMap and acquires a lock for the same resource.

		<-lk2ch
		lk3ok := <-lk3ch
		lk4ok := <-lk4ch

		if lk3ok && lk4ok {
			t.Fatalf("multiple locks acquired; iteration=%d, lk3=%t, lk4=%t", i, lk3ok, lk4ok)
		}
	}
}
