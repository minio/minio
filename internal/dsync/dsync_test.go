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
	golog "log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

const numberOfNodes = 5

var ds *Dsync
var rpcPaths []string // list of rpc paths where lock server is serving.

var nodes = make([]string, numberOfNodes) // list of node IP addrs or hostname with ports.
var lockServers []*lockServer

func startRPCServers() {
	for i := range nodes {
		server := rpc.NewServer()
		ls := &lockServer{
			mutex:   sync.Mutex{},
			lockMap: make(map[string]int64),
		}
		server.RegisterName("Dsync", ls)
		// For some reason the registration paths need to be different (even for different server objs)
		server.HandleHTTP(rpcPaths[i], fmt.Sprintf("%s-debug", rpcPaths[i]))
		l, e := net.Listen("tcp", ":"+strconv.Itoa(i+12345))
		if e != nil {
			golog.Fatal("listen error:", e)
		}
		go http.Serve(l, nil)

		lockServers = append(lockServers, ls)
	}

	// Let servers start
	time.Sleep(10 * time.Millisecond)
}

// TestMain initializes the testing framework
func TestMain(m *testing.M) {
	const rpcPath = "/dsync"

	rand.Seed(time.Now().UTC().UnixNano())

	for i := range nodes {
		nodes[i] = fmt.Sprintf("127.0.0.1:%d", i+12345)
	}
	for i := range nodes {
		rpcPaths = append(rpcPaths, rpcPath+"-"+strconv.Itoa(i))
	}

	// Initialize net/rpc clients for dsync.
	var clnts []NetLocker
	for i := 0; i < len(nodes); i++ {
		clnts = append(clnts, newClient(nodes[i], rpcPaths[i]))
	}

	ds = &Dsync{
		GetLockers: func() ([]NetLocker, string) { return clnts, uuid.New().String() },
	}

	startRPCServers()

	os.Exit(m.Run())
}

func TestSimpleLock(t *testing.T) {

	dm := NewDRWMutex(ds, "test")

	dm.Lock(id, source)

	// fmt.Println("Lock acquired, waiting...")
	time.Sleep(2500 * time.Millisecond)

	dm.Unlock()
}

func TestSimpleLockUnlockMultipleTimes(t *testing.T) {

	dm := NewDRWMutex(ds, "test")

	dm.Lock(id, source)
	time.Sleep(time.Duration(10+(rand.Float32()*50)) * time.Millisecond)
	dm.Unlock()

	dm.Lock(id, source)
	time.Sleep(time.Duration(10+(rand.Float32()*50)) * time.Millisecond)
	dm.Unlock()

	dm.Lock(id, source)
	time.Sleep(time.Duration(10+(rand.Float32()*50)) * time.Millisecond)
	dm.Unlock()

	dm.Lock(id, source)
	time.Sleep(time.Duration(10+(rand.Float32()*50)) * time.Millisecond)
	dm.Unlock()

	dm.Lock(id, source)
	time.Sleep(time.Duration(10+(rand.Float32()*50)) * time.Millisecond)
	dm.Unlock()
}

// Test two locks for same resource, one succeeds, one fails (after timeout)
func TestTwoSimultaneousLocksForSameResource(t *testing.T) {

	dm1st := NewDRWMutex(ds, "aap")
	dm2nd := NewDRWMutex(ds, "aap")

	dm1st.Lock(id, source)

	// Release lock after 10 seconds
	go func() {
		time.Sleep(10 * time.Second)
		// fmt.Println("Unlocking dm1")

		dm1st.Unlock()
	}()

	dm2nd.Lock(id, source)

	// fmt.Printf("2nd lock obtained after 1st lock is released\n")
	time.Sleep(2500 * time.Millisecond)

	dm2nd.Unlock()
}

// Test three locks for same resource, one succeeds, one fails (after timeout)
func TestThreeSimultaneousLocksForSameResource(t *testing.T) {

	dm1st := NewDRWMutex(ds, "aap")
	dm2nd := NewDRWMutex(ds, "aap")
	dm3rd := NewDRWMutex(ds, "aap")

	dm1st.Lock(id, source)

	// Release lock after 10 seconds
	go func() {
		time.Sleep(10 * time.Second)
		// fmt.Println("Unlocking dm1")

		dm1st.Unlock()
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		dm2nd.Lock(id, source)

		// Release lock after 10 seconds
		go func() {
			time.Sleep(2500 * time.Millisecond)
			// fmt.Println("Unlocking dm2")

			dm2nd.Unlock()
		}()

		dm3rd.Lock(id, source)

		// fmt.Printf("3rd lock obtained after 1st & 2nd locks are released\n")
		time.Sleep(2500 * time.Millisecond)

		dm3rd.Unlock()
	}()

	go func() {
		defer wg.Done()

		dm3rd.Lock(id, source)

		// Release lock after 10 seconds
		go func() {
			time.Sleep(2500 * time.Millisecond)
			// fmt.Println("Unlocking dm3")

			dm3rd.Unlock()
		}()

		dm2nd.Lock(id, source)

		// fmt.Printf("2nd lock obtained after 1st & 3rd locks are released\n")
		time.Sleep(2500 * time.Millisecond)

		dm2nd.Unlock()
	}()

	wg.Wait()
}

// Test two locks for different resources, both succeed
func TestTwoSimultaneousLocksForDifferentResources(t *testing.T) {

	dm1 := NewDRWMutex(ds, "aap")
	dm2 := NewDRWMutex(ds, "noot")

	dm1.Lock(id, source)
	dm2.Lock(id, source)

	// fmt.Println("Both locks acquired, waiting...")
	time.Sleep(2500 * time.Millisecond)

	dm1.Unlock()
	dm2.Unlock()

	time.Sleep(10 * time.Millisecond)
}

// Test refreshing lock
func TestFailedRefreshLock(t *testing.T) {
	// Simulate Refresh RPC response to return no locking found
	for i := range lockServers {
		lockServers[i].setRefreshReply(false)
		defer lockServers[i].setRefreshReply(true)
	}

	dm := NewDRWMutex(ds, "aap")
	wg := sync.WaitGroup{}
	wg.Add(1)

	ctx, cl := context.WithCancel(context.Background())
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
	dm.Unlock()
}

// Test Unlock should not timeout
func TestUnlockShouldNotTimeout(t *testing.T) {
	dm := NewDRWMutex(ds, "aap")

	if !dm.GetLock(context.Background(), nil, id, source, Options{Timeout: 5 * time.Minute}) {
		t.Fatal("GetLock() should be successful")
	}

	// Add delay to lock server responses to ensure that lock does not timeout
	for i := range lockServers {
		lockServers[i].setResponseDelay(2 * drwMutexUnlockCallTimeout)
		defer lockServers[i].setResponseDelay(0)
	}

	unlockReturned := make(chan struct{}, 1)
	go func() {
		dm.Unlock()
		unlockReturned <- struct{}{}
	}()

	timer := time.NewTimer(2 * drwMutexUnlockCallTimeout)
	defer timer.Stop()

	select {
	case <-unlockReturned:
		t.Fatal("Unlock timed out, which should not happen")
	case <-timer.C:
	}
}

// Borrowed from mutex_test.go
func HammerMutex(m *DRWMutex, loops int, cdone chan bool) {
	for i := 0; i < loops; i++ {
		m.Lock(id, source)
		m.Unlock()
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
	for i := 0; i < 10; i++ {
		go HammerMutex(m, loops, c)
	}
	for i := 0; i < 10; i++ {
		<-c
	}
}

func BenchmarkMutexUncontended(b *testing.B) {
	type PaddedMutex struct {
		*DRWMutex
	}
	b.RunParallel(func(pb *testing.PB) {
		var mu = PaddedMutex{NewDRWMutex(ds, "")}
		for pb.Next() {
			mu.Lock(id, source)
			mu.Unlock()
		}
	})
}

func benchmarkMutex(b *testing.B, slack, work bool) {
	mu := NewDRWMutex(ds, "")
	if slack {
		b.SetParallelism(10)
	}
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			mu.Lock(id, source)
			mu.Unlock()
			if work {
				for i := 0; i < 100; i++ {
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
				m.Unlock()
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
			m.Unlock()
			for i := 0; i < len(data); i += 4 {
				data[i]++
			}
		}
	})
}
