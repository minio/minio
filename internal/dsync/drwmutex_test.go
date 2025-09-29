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

package dsync

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

const (
	id     = "1234-5678"
	source = "main.go"
)

func testSimpleWriteLock(t *testing.T, duration time.Duration) (locked bool) {
	drwm1 := NewDRWMutex(ds, "simplelock")
	ctx1, cancel1 := context.WithCancel(t.Context())
	if !drwm1.GetRLock(ctx1, cancel1, id, source, Options{Timeout: time.Second}) {
		panic("Failed to acquire read lock")
	}
	// fmt.Println("1st read lock acquired, waiting...")

	drwm2 := NewDRWMutex(ds, "simplelock")
	ctx2, cancel2 := context.WithCancel(t.Context())
	if !drwm2.GetRLock(ctx2, cancel2, id, source, Options{Timeout: time.Second}) {
		panic("Failed to acquire read lock")
	}
	// fmt.Println("2nd read lock acquired, waiting...")

	go func() {
		time.Sleep(2 * testDrwMutexAcquireTimeout)
		drwm1.RUnlock(t.Context())
		// fmt.Println("1st read lock released, waiting...")
	}()

	go func() {
		time.Sleep(3 * testDrwMutexAcquireTimeout)
		drwm2.RUnlock(t.Context())
		// fmt.Println("2nd read lock released, waiting...")
	}()

	drwm3 := NewDRWMutex(ds, "simplelock")
	// fmt.Println("Trying to acquire write lock, waiting...")
	ctx3, cancel3 := context.WithCancel(t.Context())
	locked = drwm3.GetLock(ctx3, cancel3, id, source, Options{Timeout: duration})
	if locked {
		// fmt.Println("Write lock acquired, waiting...")
		time.Sleep(testDrwMutexAcquireTimeout)

		drwm3.Unlock(t.Context())
	}
	// fmt.Println("Write lock failed due to timeout")
	return locked
}

func TestSimpleWriteLockAcquired(t *testing.T) {
	locked := testSimpleWriteLock(t, 10*testDrwMutexAcquireTimeout)

	expected := true
	if locked != expected {
		t.Errorf("TestSimpleWriteLockAcquired(): \nexpected %#v\ngot      %#v", expected, locked)
	}
}

func TestSimpleWriteLockTimedOut(t *testing.T) {
	locked := testSimpleWriteLock(t, testDrwMutexAcquireTimeout)

	expected := false
	if locked != expected {
		t.Errorf("TestSimpleWriteLockTimedOut(): \nexpected %#v\ngot      %#v", expected, locked)
	}
}

func testDualWriteLock(t *testing.T, duration time.Duration) (locked bool) {
	drwm1 := NewDRWMutex(ds, "duallock")

	// fmt.Println("Getting initial write lock")
	ctx1, cancel1 := context.WithCancel(t.Context())
	if !drwm1.GetLock(ctx1, cancel1, id, source, Options{Timeout: time.Second}) {
		panic("Failed to acquire initial write lock")
	}

	go func() {
		time.Sleep(3 * testDrwMutexAcquireTimeout)
		drwm1.Unlock(t.Context())
		// fmt.Println("Initial write lock released, waiting...")
	}()

	// fmt.Println("Trying to acquire 2nd write lock, waiting...")
	drwm2 := NewDRWMutex(ds, "duallock")
	ctx2, cancel2 := context.WithCancel(t.Context())
	locked = drwm2.GetLock(ctx2, cancel2, id, source, Options{Timeout: duration})
	if locked {
		// fmt.Println("2nd write lock acquired, waiting...")
		time.Sleep(testDrwMutexAcquireTimeout)

		drwm2.Unlock(t.Context())
	}
	// fmt.Println("2nd write lock failed due to timeout")
	return locked
}

func TestDualWriteLockAcquired(t *testing.T) {
	locked := testDualWriteLock(t, 10*testDrwMutexAcquireTimeout)

	expected := true
	if locked != expected {
		t.Errorf("TestDualWriteLockAcquired(): \nexpected %#v\ngot      %#v", expected, locked)
	}
}

func TestDualWriteLockTimedOut(t *testing.T) {
	locked := testDualWriteLock(t, testDrwMutexAcquireTimeout)

	expected := false
	if locked != expected {
		t.Errorf("TestDualWriteLockTimedOut(): \nexpected %#v\ngot      %#v", expected, locked)
	}
}

// Test cases below are copied 1 to 1 from sync/rwmutex_test.go (adapted to use DRWMutex)

// Borrowed from rwmutex_test.go
func parallelReader(ctx context.Context, m *DRWMutex, clocked, cunlock, cdone chan bool) {
	if m.GetRLock(ctx, nil, id, source, Options{Timeout: time.Second}) {
		clocked <- true
		<-cunlock
		m.RUnlock(context.Background())
		cdone <- true
	}
}

// Borrowed from rwmutex_test.go
func doTestParallelReaders(numReaders, gomaxprocs int) {
	runtime.GOMAXPROCS(gomaxprocs)
	m := NewDRWMutex(ds, "test-parallel")

	clocked := make(chan bool)
	cunlock := make(chan bool)
	cdone := make(chan bool)
	for range numReaders {
		go parallelReader(context.Background(), m, clocked, cunlock, cdone)
	}
	// Wait for all parallel RLock()s to succeed.
	for range numReaders {
		<-clocked
	}
	for range numReaders {
		cunlock <- true
	}
	// Wait for the goroutines to finish.
	for range numReaders {
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
func reader(resource string, numIterations int, activity *int32, cdone chan bool) {
	rwm := NewDRWMutex(ds, resource)
	for range numIterations {
		if rwm.GetRLock(context.Background(), nil, id, source, Options{Timeout: time.Second}) {
			n := atomic.AddInt32(activity, 1)
			if n < 1 || n >= 10000 {
				panic(fmt.Sprintf("wlock(%d)\n", n))
			}
			for range 100 {
			}
			atomic.AddInt32(activity, -1)
			rwm.RUnlock(context.Background())
		}
	}
	cdone <- true
}

// Borrowed from rwmutex_test.go
func writer(resource string, numIterations int, activity *int32, cdone chan bool) {
	rwm := NewDRWMutex(ds, resource)
	for range numIterations {
		if rwm.GetLock(context.Background(), nil, id, source, Options{Timeout: time.Second}) {
			n := atomic.AddInt32(activity, 10000)
			if n != 10000 {
				panic(fmt.Sprintf("wlock(%d)\n", n))
			}
			for range 100 {
			}
			atomic.AddInt32(activity, -10000)
			rwm.Unlock(context.Background())
		}
	}
	cdone <- true
}

// Borrowed from rwmutex_test.go
func hammerRWMutex(t *testing.T, gomaxprocs, numReaders, numIterations int) {
	t.Run(fmt.Sprintf("%d-%d-%d", gomaxprocs, numReaders, numIterations), func(t *testing.T) {
		resource := "test"
		runtime.GOMAXPROCS(gomaxprocs)
		// Number of active readers + 10000 * number of active writers.
		var activity int32
		cdone := make(chan bool)
		go writer(resource, numIterations, &activity, cdone)
		var i int
		for i = 0; i < numReaders/2; i++ {
			go reader(resource, numIterations, &activity, cdone)
		}
		go writer(resource, numIterations, &activity, cdone)
		for ; i < numReaders; i++ {
			go reader(resource, numIterations, &activity, cdone)
		}
		// Wait for the 2 writers and all readers to finish.
		for i := 0; i < 2+numReaders; i++ {
			<-cdone
		}
	})
}

// Borrowed from rwmutex_test.go
func TestRWMutex(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 100
	if testing.Short() {
		n = 5
	}
	hammerRWMutex(t, 1, 1, n)
	hammerRWMutex(t, 1, 3, n)
	hammerRWMutex(t, 1, 10, n)
	hammerRWMutex(t, 4, 1, n)
	hammerRWMutex(t, 4, 3, n)
	hammerRWMutex(t, 4, 10, n)
	hammerRWMutex(t, 10, 1, n)
	hammerRWMutex(t, 10, 3, n)
	hammerRWMutex(t, 10, 10, n)
	hammerRWMutex(t, 10, 5, n)
}

// Borrowed from rwmutex_test.go
func TestUnlockPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("unlock of unlocked RWMutex did not panic")
		}
	}()
	mu := NewDRWMutex(ds, "test")
	mu.Unlock(t.Context())
}

// Borrowed from rwmutex_test.go
func TestUnlockPanic2(t *testing.T) {
	mu := NewDRWMutex(ds, "test-unlock-panic-2")
	defer func() {
		if recover() == nil {
			t.Fatalf("unlock of unlocked RWMutex did not panic")
		}
		mu.RUnlock(t.Context()) // Unlock, so -test.count > 1 works
	}()
	mu.RLock(id, source)
	mu.Unlock(t.Context())
}

// Borrowed from rwmutex_test.go
func TestRUnlockPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("read unlock of unlocked RWMutex did not panic")
		}
	}()
	mu := NewDRWMutex(ds, "test")
	mu.RUnlock(t.Context())
}

// Borrowed from rwmutex_test.go
func TestRUnlockPanic2(t *testing.T) {
	mu := NewDRWMutex(ds, "test-runlock-panic-2")
	defer func() {
		if recover() == nil {
			t.Fatalf("read unlock of unlocked RWMutex did not panic")
		}
		mu.Unlock(t.Context()) // Unlock, so -test.count > 1 works
	}()
	mu.Lock(id, source)
	mu.RUnlock(t.Context())
}

// Borrowed from rwmutex_test.go
func benchmarkRWMutex(b *testing.B, localWork, writeRatio int) {
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			rwm := NewDRWMutex(ds, "test")
			foo++
			if foo%writeRatio == 0 {
				rwm.Lock(id, source)
				rwm.Unlock(b.Context())
			} else {
				rwm.RLock(id, source)
				for i := 0; i != localWork; i++ {
					foo *= 2
					foo /= 2
				}
				rwm.RUnlock(b.Context())
			}
		}
		_ = foo
	})
}

// Borrowed from rwmutex_test.go
func BenchmarkRWMutexWrite100(b *testing.B) {
	benchmarkRWMutex(b, 0, 100)
}

// Borrowed from rwmutex_test.go
func BenchmarkRWMutexWrite10(b *testing.B) {
	benchmarkRWMutex(b, 0, 10)
}

// Borrowed from rwmutex_test.go
func BenchmarkRWMutexWorkWrite100(b *testing.B) {
	benchmarkRWMutex(b, 100, 100)
}

// Borrowed from rwmutex_test.go
func BenchmarkRWMutexWorkWrite10(b *testing.B) {
	benchmarkRWMutex(b, 100, 10)
}
