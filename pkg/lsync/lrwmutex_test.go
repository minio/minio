/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

// GOMAXPROCS=10 go test

package lsync_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"runtime"

	. "github.com/minio/minio/pkg/lsync"
)

func testSimpleWriteLock(t *testing.T, duration time.Duration) (locked bool) {

	ctx := context.Background()
	lrwm := NewLRWMutex()

	if !lrwm.GetRLock(ctx, "", "object1", time.Second) {
		panic("Failed to acquire read lock")
	}
	// fmt.Println("1st read lock acquired, waiting...")

	if !lrwm.GetRLock(ctx, "", "object1", time.Second) {
		panic("Failed to acquire read lock")
	}
	// fmt.Println("2nd read lock acquired, waiting...")

	go func() {
		time.Sleep(2 * time.Second)
		lrwm.RUnlock()
		// fmt.Println("1st read lock released, waiting...")
	}()

	go func() {
		time.Sleep(3 * time.Second)
		lrwm.RUnlock()
		// fmt.Println("2nd read lock released, waiting...")
	}()

	// fmt.Println("Trying to acquire write lock, waiting...")
	locked = lrwm.GetLock(ctx, "", "", duration)
	if locked {
		// fmt.Println("Write lock acquired, waiting...")
		time.Sleep(1 * time.Second)

		lrwm.Unlock()
	} else {
		// fmt.Println("Write lock failed due to timeout")
	}
	return
}

func TestSimpleWriteLockAcquired(t *testing.T) {
	locked := testSimpleWriteLock(t, 5*time.Second)

	expected := true
	if locked != expected {
		t.Errorf("TestSimpleWriteLockAcquired(): \nexpected %#v\ngot      %#v", expected, locked)
	}
}

func TestSimpleWriteLockTimedOut(t *testing.T) {
	locked := testSimpleWriteLock(t, time.Second)

	expected := false
	if locked != expected {
		t.Errorf("TestSimpleWriteLockTimedOut(): \nexpected %#v\ngot      %#v", expected, locked)
	}
}

func testDualWriteLock(t *testing.T, duration time.Duration) (locked bool) {

	ctx := context.Background()
	lrwm := NewLRWMutex()

	// fmt.Println("Getting initial write lock")
	if !lrwm.GetLock(ctx, "", "", time.Second) {
		panic("Failed to acquire initial write lock")
	}

	go func() {
		time.Sleep(2 * time.Second)
		lrwm.Unlock()
		// fmt.Println("Initial write lock released, waiting...")
	}()

	// fmt.Println("Trying to acquire 2nd write lock, waiting...")
	locked = lrwm.GetLock(ctx, "", "", duration)
	if locked {
		// fmt.Println("2nd write lock acquired, waiting...")
		time.Sleep(time.Second)

		lrwm.Unlock()
	} else {
		// fmt.Println("2nd write lock failed due to timeout")
	}
	return
}

func TestDualWriteLockAcquired(t *testing.T) {
	locked := testDualWriteLock(t, 3*time.Second)

	expected := true
	if locked != expected {
		t.Errorf("TestDualWriteLockAcquired(): \nexpected %#v\ngot      %#v", expected, locked)
	}

}

func TestDualWriteLockTimedOut(t *testing.T) {
	locked := testDualWriteLock(t, time.Second)

	expected := false
	if locked != expected {
		t.Errorf("TestDualWriteLockTimedOut(): \nexpected %#v\ngot      %#v", expected, locked)
	}

}

// Test cases below are copied 1 to 1 from sync/rwmutex_test.go (adapted to use LRWMutex)

// Borrowed from rwmutex_test.go
func parallelReader(ctx context.Context, m *LRWMutex, clocked, cunlock, cdone chan bool) {
	if m.GetRLock(ctx, "", "", time.Second) {
		clocked <- true
		<-cunlock
		m.RUnlock()
		cdone <- true
	}
}

// Borrowed from rwmutex_test.go
func doTestParallelReaders(numReaders, gomaxprocs int) {
	runtime.GOMAXPROCS(gomaxprocs)
	m := NewLRWMutex()

	clocked := make(chan bool)
	cunlock := make(chan bool)
	cdone := make(chan bool)
	for i := 0; i < numReaders; i++ {
		go parallelReader(context.Background(), m, clocked, cunlock, cdone)
	}
	// Wait for all parallel RLock()s to succeed.
	for i := 0; i < numReaders; i++ {
		<-clocked
	}
	for i := 0; i < numReaders; i++ {
		cunlock <- true
	}
	// Wait for the goroutines to finish.
	for i := 0; i < numReaders; i++ {
		<-cdone
	}
}

// Borrowed from rwmutex_test.go
func TestParallelReaders(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	doTestParallelReaders(1, 4)
	doTestParallelReaders(3, 4)
	doTestParallelReaders(4, 2)
}

// Borrowed from rwmutex_test.go
func reader(rwm *LRWMutex, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		if rwm.GetRLock(context.Background(), "", "", time.Second) {
			n := atomic.AddInt32(activity, 1)
			if n < 1 || n >= 10000 {
				panic(fmt.Sprintf("wlock(%d)\n", n))
			}
			for i := 0; i < 100; i++ {
			}
			atomic.AddInt32(activity, -1)
			rwm.RUnlock()
		}
	}
	cdone <- true
}

// Borrowed from rwmutex_test.go
func writer(rwm *LRWMutex, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		if rwm.GetLock(context.Background(), "", "", time.Second) {
			n := atomic.AddInt32(activity, 10000)
			if n != 10000 {
				panic(fmt.Sprintf("wlock(%d)\n", n))
			}
			for i := 0; i < 100; i++ {
			}
			atomic.AddInt32(activity, -10000)
			rwm.Unlock()
		}
	}
	cdone <- true
}

// Borrowed from rwmutex_test.go
func HammerRWMutex(gomaxprocs, numReaders, numIterations int) {
	runtime.GOMAXPROCS(gomaxprocs)
	// Number of active readers + 10000 * number of active writers.
	var activity int32
	rwm := NewLRWMutex()
	cdone := make(chan bool)
	go writer(rwm, numIterations, &activity, cdone)
	var i int
	for i = 0; i < numReaders/2; i++ {
		go reader(rwm, numIterations, &activity, cdone)
	}
	go writer(rwm, numIterations, &activity, cdone)
	for ; i < numReaders; i++ {
		go reader(rwm, numIterations, &activity, cdone)
	}
	// Wait for the 2 writers and all readers to finish.
	for i := 0; i < 2+numReaders; i++ {
		<-cdone
	}
}

// Borrowed from rwmutex_test.go
func TestRWMutex(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 1000
	if testing.Short() {
		n = 5
	}
	HammerRWMutex(1, 1, n)
	HammerRWMutex(1, 3, n)
	HammerRWMutex(1, 10, n)
	HammerRWMutex(4, 1, n)
	HammerRWMutex(4, 3, n)
	HammerRWMutex(4, 10, n)
	HammerRWMutex(10, 1, n)
	HammerRWMutex(10, 3, n)
	HammerRWMutex(10, 10, n)
	HammerRWMutex(10, 5, n)
}

// Borrowed from rwmutex_test.go
func TestDRLocker(t *testing.T) {
	wl := NewLRWMutex()
	var rl sync.Locker
	wlocked := make(chan bool, 1)
	rlocked := make(chan bool, 1)
	rl = wl.DRLocker()
	n := 10
	go func() {
		for i := 0; i < n; i++ {
			rl.Lock()
			rl.Lock()
			rlocked <- true
			wl.Lock()
			wlocked <- true
		}
	}()
	for i := 0; i < n; i++ {
		<-rlocked
		rl.Unlock()
		select {
		case <-wlocked:
			t.Fatal("RLocker() didn't read-lock it")
		default:
		}
		rl.Unlock()
		<-wlocked
		select {
		case <-rlocked:
			t.Fatal("RLocker() didn't respect the write lock")
		default:
		}
		wl.Unlock()
	}
}

// Borrowed from rwmutex_test.go
func TestUnlockPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("unlock of unlocked RWMutex did not panic")
		}
	}()
	mu := NewLRWMutex()
	mu.Unlock()
}

// Borrowed from rwmutex_test.go
func TestUnlockPanic2(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("unlock of unlocked RWMutex did not panic")
		}
	}()
	mu := NewLRWMutex()
	mu.RLock()
	mu.Unlock()
}

// Borrowed from rwmutex_test.go
func TestRUnlockPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("read unlock of unlocked RWMutex did not panic")
		}
	}()
	mu := NewLRWMutex()
	mu.RUnlock()
}

// Borrowed from rwmutex_test.go
func TestRUnlockPanic2(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("read unlock of unlocked RWMutex did not panic")
		}
	}()
	mu := NewLRWMutex()
	mu.Lock()
	mu.RUnlock()
}
