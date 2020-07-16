/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package dsync_test

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/minio/minio/pkg/dsync"
)

const (
	id     = "1234-5678"
	source = "main.go"
)

func testSimpleWriteLock(t *testing.T, duration time.Duration) (locked bool) {

	drwm := NewDRWMutex(ds, "simplelock")

	if !drwm.GetRLock(context.Background(), id, source, time.Second) {
		panic("Failed to acquire read lock")
	}
	// fmt.Println("1st read lock acquired, waiting...")

	if !drwm.GetRLock(context.Background(), id, source, time.Second) {
		panic("Failed to acquire read lock")
	}
	// fmt.Println("2nd read lock acquired, waiting...")

	go func() {
		time.Sleep(2 * time.Second)
		drwm.RUnlock()
		// fmt.Println("1st read lock released, waiting...")
	}()

	go func() {
		time.Sleep(3 * time.Second)
		drwm.RUnlock()
		// fmt.Println("2nd read lock released, waiting...")
	}()

	// fmt.Println("Trying to acquire write lock, waiting...")
	locked = drwm.GetLock(context.Background(), id, source, duration)
	if locked {
		// fmt.Println("Write lock acquired, waiting...")
		time.Sleep(time.Second)

		drwm.Unlock()
	}
	// fmt.Println("Write lock failed due to timeout")
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

	drwm := NewDRWMutex(ds, "duallock")

	// fmt.Println("Getting initial write lock")
	if !drwm.GetLock(context.Background(), id, source, time.Second) {
		panic("Failed to acquire initial write lock")
	}

	go func() {
		time.Sleep(2 * time.Second)
		drwm.Unlock()
		// fmt.Println("Initial write lock released, waiting...")
	}()

	// fmt.Println("Trying to acquire 2nd write lock, waiting...")
	locked = drwm.GetLock(context.Background(), id, source, duration)
	if locked {
		// fmt.Println("2nd write lock acquired, waiting...")
		time.Sleep(time.Second)

		drwm.Unlock()
	}
	// fmt.Println("2nd write lock failed due to timeout")
	return
}

func TestDualWriteLockAcquired(t *testing.T) {
	locked := testDualWriteLock(t, 5*time.Second)

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

// Test cases below are copied 1 to 1 from sync/rwmutex_test.go (adapted to use DRWMutex)

// Borrowed from rwmutex_test.go
func parallelReader(ctx context.Context, m *DRWMutex, clocked, cunlock, cdone chan bool) {
	if m.GetRLock(ctx, id, source, time.Second) {
		clocked <- true
		<-cunlock
		m.RUnlock()
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
func reader(rwm *DRWMutex, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		if rwm.GetRLock(context.Background(), id, source, time.Second) {
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
func writer(rwm *DRWMutex, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		if rwm.GetLock(context.Background(), id, source, time.Second) {
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
	rwm := NewDRWMutex(ds, "test")
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
	n := 100
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
func TestUnlockPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("unlock of unlocked RWMutex did not panic")
		}
	}()
	mu := NewDRWMutex(ds, "test")
	mu.Unlock()
}

// Borrowed from rwmutex_test.go
func TestUnlockPanic2(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("unlock of unlocked RWMutex did not panic")
		}
	}()
	mu := NewDRWMutex(ds, "test-unlock-panic-2")
	mu.RLock(id, source)
	mu.Unlock()
}

// Borrowed from rwmutex_test.go
func TestRUnlockPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("read unlock of unlocked RWMutex did not panic")
		}
	}()
	mu := NewDRWMutex(ds, "test")
	mu.RUnlock()
}

// Borrowed from rwmutex_test.go
func TestRUnlockPanic2(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("read unlock of unlocked RWMutex did not panic")
		}
	}()
	mu := NewDRWMutex(ds, "test-runlock-panic-2")
	mu.Lock(id, source)
	mu.RUnlock()
}

// Borrowed from rwmutex_test.go
func benchmarkRWMutex(b *testing.B, localWork, writeRatio int) {
	rwm := NewDRWMutex(ds, "test")
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			foo++
			if foo%writeRatio == 0 {
				rwm.Lock(id, source)
				rwm.Unlock()
			} else {
				rwm.RLock(id, source)
				for i := 0; i != localWork; i++ {
					foo *= 2
					foo /= 2
				}
				rwm.RUnlock()
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
