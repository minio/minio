package lock

import (
	"os"
	"sync"
)

// RWLocker - interface that any read-write locking library should implement.
type RWLocker interface {
	sync.Locker
	RLock()
	RUnlock()
}

// Locker - file based lock/unlock implementation.
type Locker struct {
	name   string
	wfiles []*os.File
	rfiles []*os.File
	wMutex sync.Mutex
	rMutex sync.Mutex
}

// Private file mode.
var privateFileMode = os.FileMode(0600)

// Lock locks f. If the lock is already in use, the calling
// goroutine blocks until the mutex is available.
func (f *Locker) Lock() {
	f.wMutex.Lock()
	defer f.wMutex.Unlock()
	file, err := open(f.name, os.O_WRONLY, os.ModeExclusive)
	if err != nil {
		panic(err)
	}
	if err := lock(file); err != nil {
		panic(err)
	}
	f.wfiles = append(f.wfiles, file)
}

// Unlock unlocks f. A locked Mutex is not associated with a
// particular goroutine. It is allowed for one goroutine to
// lock a Mutex and then arrange for another goroutine to unlock it.
func (f *Locker) Unlock() {
	f.wMutex.Lock()
	defer f.wMutex.Unlock()
	if len(f.wfiles) == 0 {
		panic("Trying to Unlock() while no Lock() is active")
	}
	file := f.wfiles[0]
	if file == nil {
		panic("Trying to Unlock() while no Lock() is active")
	}
	if err := unlock(file); err != nil {
		panic(err)
	}
	if err := file.Close(); err != nil {
		panic(err)
	}
	f.wfiles = f.wfiles[1:]
}

// RLock locks f for reading.
func (f *Locker) RLock() {
	f.rMutex.Lock()
	defer f.rMutex.Unlock()
	file, err := open(f.name, os.O_RDONLY, os.ModeExclusive)
	if err != nil {
		panic(err)
	}
	if err := rlock(file); err != nil {
		panic(err)
	}
	f.rfiles = append(f.rfiles, file)
}

// RUnlock undoes a single RLock call; it does not affect other
// simultaneous readers.
func (f *Locker) RUnlock() {
	f.rMutex.Lock()
	defer f.rMutex.Unlock()
	if len(f.rfiles) == 0 {
		panic("Trying to RUnlock() while no RLock() is active")
	}
	file := f.rfiles[0]
	if file == nil {
		panic("Trying to RUnlock() while no RLock() is active")
	}
	if err := runlock(file); err != nil {
		panic(err)
	}
	if err := file.Close(); err != nil {
		panic(err)
	}
	f.rfiles = f.rfiles[1:]
}

// NewFlock - initializes a new lock, if the path doesn't
// exist NewFlock will panic.
func NewFlock(name string) RWLocker {
	return &Locker{name: name}
}
