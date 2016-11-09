package lock_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/minio/minio/pkg/lock"
)

func TestLockAndUnlock(t *testing.T) {
	f, err := ioutil.TempFile("", "lock")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		if err != nil {
			t.Fatal(err)
		}
	}()

	frw := NewFlock(f.Name())

	frw.RLock()
	// fmt.Println("1st read lock acquired, waiting...")

	frw.RLock()
	// fmt.Println("2nd read lock acquired, waiting...")

	go func() {
		time.Sleep(1000 * time.Millisecond)
		frw.RUnlock()
		// fmt.Println("1st read lock released, waiting...")
	}()

	go func() {
		time.Sleep(2000 * time.Millisecond)
		frw.RUnlock()
		// fmt.Println("2nd read lock released, waiting...")
	}()

	// fmt.Println("Trying to acquire write lock, waiting...")
	frw.Lock()

	// fmt.Println("Write lock acquired, waiting...")
	time.Sleep(2500 * time.Millisecond)

	frw.Unlock()
}

// Test cases below are copied 1 to 1 from sync/rwmutex_test.go (adapted to use DRWMutex)

// Borrowed from rwmutex_test.go
func parallelReader(m RWLocker, clocked, cunlock, cdone chan bool) {
	m.RLock()
	clocked <- true
	<-cunlock
	m.RUnlock()
	cdone <- true
}

// Borrowed from rwmutex_test.go
func doTestParallelReaders(numReaders, gomaxprocs int, name string) {
	runtime.GOMAXPROCS(gomaxprocs)
	m := NewFlock(name)

	clocked := make(chan bool)
	cunlock := make(chan bool)
	cdone := make(chan bool)
	for i := 0; i < numReaders; i++ {
		go parallelReader(m, clocked, cunlock, cdone)
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
	f, err := ioutil.TempFile("", "test-parallel")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		if err != nil {
			t.Fatal(err)
		}
	}()
	doTestParallelReaders(1, 4, f.Name())
	doTestParallelReaders(3, 4, f.Name())
	doTestParallelReaders(4, 2, f.Name())
}

// Borrowed from rwmutex_test.go
func reader(rwm RWLocker, num_iterations int, activity *int32, cdone chan bool) {
	for i := 0; i < num_iterations; i++ {
		rwm.RLock()
		n := atomic.AddInt32(activity, 1)
		if n < 1 || n >= 10000 {
			panic(fmt.Sprintf("rlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -1)
		rwm.RUnlock()
	}
	cdone <- true
}

// Borrowed from rwmutex_test.go
func writer(rwm RWLocker, num_iterations int, activity *int32, cdone chan bool) {
	for i := 0; i < num_iterations; i++ {
		rwm.Lock()
		n := atomic.AddInt32(activity, 10000)
		if n != 10000 {
			panic(fmt.Sprintf("wlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -10000)
		rwm.Unlock()
	}
	cdone <- true
}

// Borrowed from rwmutex_test.go
func HammerRWMutex(gomaxprocs, numReaders, num_iterations int, t *testing.T) {
	runtime.GOMAXPROCS(gomaxprocs)
	// Number of active readers + 10000 * number of active writers.
	var activity int32
	cdone := make(chan bool)
	f, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		if err != nil {
			t.Fatal(err)
		}
	}()
	rwm := NewFlock(f.Name())
	go writer(rwm, num_iterations, &activity, cdone)
	var i int
	for i = 0; i < numReaders/2; i++ {
		go reader(rwm, num_iterations, &activity, cdone)
	}
	go writer(rwm, num_iterations, &activity, cdone)
	for ; i < numReaders; i++ {
		go reader(rwm, num_iterations, &activity, cdone)
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
	HammerRWMutex(1, 1, n, t)
	HammerRWMutex(1, 3, n, t)
	HammerRWMutex(1, 10, n, t)
	/* FIXME.
	HammerRWMutex(4, 3, n, t)
	HammerRWMutex(4, 10, n, t)
	HammerRWMutex(10, 1, n, t)
	HammerRWMutex(10, 3, n, t)
	HammerRWMutex(10, 10, n, t)
	HammerRWMutex(10, 5, n, t)
	*/
}

// Borrowed from rwmutex_test.go
func TestUnlockPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("unlock of unlocked RWMutex did not panic")
		}
	}()
	f, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		if err != nil {
			t.Fatal(err)
		}
	}()
	mu := NewFlock(f.Name())
	mu.Unlock()
}

// Borrowed from rwmutex_test.go
func TestUnlockPanic2(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("unlock of unlocked RWMutex did not panic")
		}
	}()
	f, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		if err != nil {
			t.Fatal(err)
		}
	}()
	mu := NewFlock(f.Name())
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
	f, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		if err != nil {
			t.Fatal(err)
		}
	}()
	mu := NewFlock(f.Name())
	mu.RUnlock()
}

// Borrowed from rwmutex_test.go
func TestRUnlockPanic2(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("read unlock of unlocked RWMutex did not panic")
		}
	}()
	f, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		if err != nil {
			t.Fatal(err)
		}
	}()
	mu := NewFlock(f.Name())
	mu.Lock()
	mu.RUnlock()
}

// Borrowed from rwmutex_test.go
func benchmarkRWMutex(b *testing.B, localWork, writeRatio int) {
	f, err := ioutil.TempFile("", "test")
	if err != nil {
		b.Fatal(err)
	}
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		if err != nil {
			b.Fatal(err)
		}
	}()
	rwm := NewFlock(f.Name())
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			foo++
			if foo%writeRatio == 0 {
				rwm.Lock()
				rwm.Unlock()
			} else {
				rwm.RLock()
				for i := 0; i != localWork; i += 1 {
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
