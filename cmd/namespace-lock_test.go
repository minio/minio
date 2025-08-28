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
	"runtime"
	"testing"
	"time"
)

// WARNING:
//
// Expected source line number is hard coded, 35, in the
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
	t.Skip("long test skip it")

	ctx := t.Context()

	for i := range 10000 {
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
