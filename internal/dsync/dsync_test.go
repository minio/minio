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
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

const (
	testDrwMutexAcquireTimeout         = 250 * time.Millisecond
	testDrwMutexRefreshCallTimeout     = 250 * time.Millisecond
	testDrwMutexUnlockCallTimeout      = 250 * time.Millisecond
	testDrwMutexForceUnlockCallTimeout = 250 * time.Millisecond
	testDrwMutexRefreshInterval        = 100 * time.Millisecond
)

// TestMain initializes the testing framework
func TestMain(m *testing.M) {
	startLockServers()

	// Initialize locker clients for dsync.
	var clnts []NetLocker
	for i := range nodes {
		clnts = append(clnts, newClient(nodes[i].URL))
	}

	ds = &Dsync{
		GetLockers: func() ([]NetLocker, string) { return clnts, uuid.New().String() },
		Timeouts: Timeouts{
			Acquire:         testDrwMutexAcquireTimeout,
			RefreshCall:     testDrwMutexRefreshCallTimeout,
			UnlockCall:      testDrwMutexUnlockCallTimeout,
			ForceUnlockCall: testDrwMutexForceUnlockCallTimeout,
		},
	}

	code := m.Run()
	stopLockServers()
	os.Exit(code)
}

func TestSimpleLock(t *testing.T) {
	dm := NewDRWMutex(ds, "test")

	dm.Lock(id, source)

	// fmt.Println("Lock acquired, waiting...")
	time.Sleep(testDrwMutexRefreshCallTimeout)

	dm.Unlock(t.Context())
}

func TestSimpleLockUnlockMultipleTimes(t *testing.T) {
	dm := NewDRWMutex(ds, "test")

	dm.Lock(id, source)
	time.Sleep(time.Duration(10+(rand.Float32()*50)) * time.Millisecond)
	dm.Unlock(t.Context())

	dm.Lock(id, source)
	time.Sleep(time.Duration(10+(rand.Float32()*50)) * time.Millisecond)
	dm.Unlock(t.Context())

	dm.Lock(id, source)
	time.Sleep(time.Duration(10+(rand.Float32()*50)) * time.Millisecond)
	dm.Unlock(t.Context())

	dm.Lock(id, source)
	time.Sleep(time.Duration(10+(rand.Float32()*50)) * time.Millisecond)
	dm.Unlock(t.Context())

	dm.Lock(id, source)
	time.Sleep(time.Duration(10+(rand.Float32()*50)) * time.Millisecond)
	dm.Unlock(t.Context())
}

// Test two locks for same resource, one succeeds, one fails (after timeout)
func TestTwoSimultaneousLocksForSameResource(t *testing.T) {
	dm1st := NewDRWMutex(ds, "aap")
	dm2nd := NewDRWMutex(ds, "aap")

	dm1st.Lock(id, source)

	// Release lock after 10 seconds
	go func() {
		time.Sleep(5 * testDrwMutexAcquireTimeout)
		// fmt.Println("Unlocking dm1")

		dm1st.Unlock(t.Context())
	}()

	dm2nd.Lock(id, source)

	// fmt.Printf("2nd lock obtained after 1st lock is released\n")
	time.Sleep(testDrwMutexRefreshCallTimeout * 2)

	dm2nd.Unlock(t.Context())
}

// Test three locks for same resource, one succeeds, one fails (after timeout)
func TestThreeSimultaneousLocksForSameResource(t *testing.T) {
	dm1st := NewDRWMutex(ds, "aap")
	dm2nd := NewDRWMutex(ds, "aap")
	dm3rd := NewDRWMutex(ds, "aap")

	dm1st.Lock(id, source)
	started := time.Now()
	var expect time.Duration
	// Release lock after 10 seconds
	go func() {
		// TOTAL
		time.Sleep(2 * testDrwMutexAcquireTimeout)
		// fmt.Println("Unlocking dm1")

		dm1st.Unlock(t.Context())
	}()
	expect += 2 * testDrwMutexAcquireTimeout

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		dm2nd.Lock(id, source)

		// Release lock after 10 seconds
		go func() {
			time.Sleep(2 * testDrwMutexAcquireTimeout)
			// fmt.Println("Unlocking dm2")

			dm2nd.Unlock(t.Context())
		}()

		dm3rd.Lock(id, source)

		// fmt.Printf("3rd lock obtained after 1st & 2nd locks are released\n")
		time.Sleep(testDrwMutexRefreshCallTimeout)

		dm3rd.Unlock(t.Context())
	}()
	expect += 2*testDrwMutexAcquireTimeout + testDrwMutexRefreshCallTimeout

	go func() {
		defer wg.Done()

		dm3rd.Lock(id, source)

		// Release lock after 10 seconds
		go func() {
			time.Sleep(2 * testDrwMutexAcquireTimeout)
			// fmt.Println("Unlocking dm3")

			dm3rd.Unlock(t.Context())
		}()

		dm2nd.Lock(id, source)

		// fmt.Printf("2nd lock obtained after 1st & 3rd locks are released\n")
		time.Sleep(testDrwMutexRefreshCallTimeout)

		dm2nd.Unlock(t.Context())
	}()
	expect += 2*testDrwMutexAcquireTimeout + testDrwMutexRefreshCallTimeout

	wg.Wait()
	// We expect at least 3 x 2 x testDrwMutexAcquireTimeout to have passed
	elapsed := time.Since(started)
	if elapsed < expect {
		t.Errorf("expected at least %v time have passed, however %v passed", expect, elapsed)
	}
	t.Logf("expected at least %v time have passed, %v passed", expect, elapsed)
}

// Test two locks for different resources, both succeed
func TestTwoSimultaneousLocksForDifferentResources(t *testing.T) {
	dm1 := NewDRWMutex(ds, "aap")
	dm2 := NewDRWMutex(ds, "noot")

	dm1.Lock(id, source)
	dm2.Lock(id, source)
	dm1.Unlock(t.Context())
	dm2.Unlock(t.Context())
}

// Test refreshing lock - refresh should always return true
func TestSuccessfulLockRefresh(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	dm := NewDRWMutex(ds, "aap")
	dm.refreshInterval = testDrwMutexRefreshInterval

	ctx, cancel := context.WithCancel(t.Context())

	if !dm.GetLock(ctx, cancel, id, source, Options{Timeout: 5 * time.Minute}) {
		t.Fatal("GetLock() should be successful")
	}

	// Make it run twice.
	timer := time.NewTimer(testDrwMutexRefreshInterval * 2)

	select {
	case <-ctx.Done():
		t.Fatal("Lock context canceled which is not expected")
	case <-timer.C:
	}

	// Should be safe operation in all cases
	dm.Unlock(t.Context())
}

// Test canceling context while quorum servers report lock not found
func TestFailedRefreshLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	// Simulate Refresh response to return no locking found
	for i := range lockServers[:3] {
		lockServers[i].setRefreshReply(false)
		defer lockServers[i].setRefreshReply(true)
	}

	dm := NewDRWMutex(ds, "aap")
	dm.refreshInterval = 500 * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cl := context.WithCancel(t.Context())
	cancel := func() {
		cl()
		wg.Done()
	}

	if !dm.GetLock(ctx, cancel, id, source, Options{Timeout: 5 * time.Minute}) {
		t.Fatal("GetLock() should be successful")
	}

	// Wait until context is canceled
	wg.Wait()
	if ctx.Err() == nil {
		t.Fatal("Unexpected error", ctx.Err())
	}

	// Should be safe operation in all cases
	dm.Unlock(t.Context())
}

// Test Unlock should not timeout
func TestUnlockShouldNotTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	dm := NewDRWMutex(ds, "aap")
	dm.refreshInterval = testDrwMutexUnlockCallTimeout
	if !dm.GetLock(t.Context(), nil, id, source, Options{Timeout: 5 * time.Minute}) {
		t.Fatal("GetLock() should be successful")
	}

	// Add delay to lock server responses to ensure that lock does not timeout
	for i := range lockServers {
		lockServers[i].setResponseDelay(5 * testDrwMutexUnlockCallTimeout)
		defer lockServers[i].setResponseDelay(0)
	}

	unlockReturned := make(chan struct{}, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		dm.Unlock(ctx)
		// Unlock is not blocking. Try to get a new lock.
		dm.GetLock(ctx, nil, id, source, Options{Timeout: 5 * time.Minute})
		unlockReturned <- struct{}{}
	}()

	timer := time.NewTimer(2 * testDrwMutexUnlockCallTimeout)
	defer timer.Stop()

	select {
	case <-unlockReturned:
		t.Fatal("Unlock timed out, which should not happen")
	case <-timer.C:
	}
}

// Borrowed from mutex_test.go
func HammerMutex(m *DRWMutex, loops int, cdone chan bool) {
	for range loops {
		m.Lock(id, source)
		m.Unlock(context.Background())
	}
	cdone <- true
}

// Borrowed from mutex_test.go
func TestMutex(t *testing.T) {
	loops := 200
	if testing.Short() {
		loops = 5
	}
	c := make(chan bool)
	m := NewDRWMutex(ds, "test")
	for range 10 {
		go HammerMutex(m, loops, c)
	}
	for range 10 {
		<-c
	}
}

func BenchmarkMutexUncontended(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	type PaddedMutex struct {
		*DRWMutex
	}
	b.RunParallel(func(pb *testing.PB) {
		mu := PaddedMutex{NewDRWMutex(ds, "")}
		for pb.Next() {
			mu.Lock(id, source)
			mu.Unlock(b.Context())
		}
	})
}

func benchmarkMutex(b *testing.B, slack, work bool) {
	b.ResetTimer()
	b.ReportAllocs()

	mu := NewDRWMutex(ds, "")
	if slack {
		b.SetParallelism(10)
	}
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			mu.Lock(id, source)
			mu.Unlock(b.Context())
			if work {
				for range 100 {
					foo *= 2
					foo /= 2
				}
			}
		}
		_ = foo
	})
}

func BenchmarkMutex(b *testing.B) {
	benchmarkMutex(b, false, false)
}

func BenchmarkMutexSlack(b *testing.B) {
	benchmarkMutex(b, true, false)
}

func BenchmarkMutexWork(b *testing.B) {
	benchmarkMutex(b, false, true)
}

func BenchmarkMutexWorkSlack(b *testing.B) {
	benchmarkMutex(b, true, true)
}

func BenchmarkMutexNoSpin(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	// This benchmark models a situation where spinning in the mutex should be
	// non-profitable and allows to confirm that spinning does not do harm.
	// To achieve this we create excess of goroutines most of which do local work.
	// These goroutines yield during local work, so that switching from
	// a blocked goroutine to other goroutines is profitable.
	// As a matter of fact, this benchmark still triggers some spinning in the mutex.
	m := NewDRWMutex(ds, "")
	var acc0, acc1 uint64
	b.SetParallelism(4)
	b.RunParallel(func(pb *testing.PB) {
		c := make(chan bool)
		var data [4 << 10]uint64
		for i := 0; pb.Next(); i++ {
			if i%4 == 0 {
				m.Lock(id, source)
				acc0 -= 100
				acc1 += 100
				m.Unlock(b.Context())
			} else {
				for i := 0; i < len(data); i += 4 {
					data[i]++
				}
				// Elaborate way to say runtime.Gosched
				// that does not put the goroutine onto global runq.
				go func() {
					c <- true
				}()
				<-c
			}
		}
	})
}

func BenchmarkMutexSpin(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	// This benchmark models a situation where spinning in the mutex should be
	// profitable. To achieve this we create a goroutine per-proc.
	// These goroutines access considerable amount of local data so that
	// unnecessary rescheduling is penalized by cache misses.
	m := NewDRWMutex(ds, "")
	var acc0, acc1 uint64
	b.RunParallel(func(pb *testing.PB) {
		var data [16 << 10]uint64
		for i := 0; pb.Next(); i++ {
			m.Lock(id, source)
			acc0 -= 100
			acc1 += 100
			m.Unlock(b.Context())
			for i := 0; i < len(data); i += 4 {
				data[i]++
			}
		}
	})
}
