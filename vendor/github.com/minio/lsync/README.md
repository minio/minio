# lsync

Local syncing package with support for timeouts. This package offers both a `sync.Mutex` and `sync.RWMutex` compatible interface.

Additionally it provides `lsync.LFrequentAccess` which uses an atomic load and store of a consistently typed value. This can be usefull for shared data structures that are frequently read but infrequently updated (using an copy-on-write mechanism) without the need for protection with a regular mutex.

### Example of LRWMutex

```go
	// Create RWMutex compatible mutex 
	lrwm := NewLRWMutex()

	// Try to get lock within timeout 
	if !lrwm.GetLock(1000 * time.Millisecond) {
		fmt.Println("Timeout occured")
		return
	}
	
	// Acquired lock, do your stuff ...

	lrwm.Unlock() // Release lock
```

### Example of LFrequentAccess
````go
	type Map map[string]string

	// Create new LFrequentAccess for type Map
	freqaccess := NewLFrequentAccess(make(Map))

	cur := freqaccess.LockBeforeSet().(Map) // Lock in order to update
	mp := make(Map)                         // Create new Map
	for k, v := range cur {                 // Copy over old contents
		mp[k] = v
	}
	mp[key] = val                      // Add new value
	freqaccess.SetNewCopyAndUnlock(mp) // Exchange old version of map with new version

	mpReadOnly := freqaccess.ReadOnlyAccess().(Map) // Get read only access to Map
	fmt.Println(mpReadOnly[key])                    // Safe access with no further synchronization
````

## Design

The design is pretty straightforward in the sense that `lsync` tries to get a lock in a loop with an exponential [backoff](https://www.awsarchitectureblog.com/2015/03/backoff.html) algorithm. The algorithm is configurable in terms of initial delay and jitter.

If the lock is acquired before the timeout has occured, it will return success to the caller and the caller can proceed as intended. The caller must call `unlock` after the operation that is to be protected has completed in order to release the lock.

When more time has elapsed than the timeout value the lock loop will cancel out and signal back to the caller that the lock has not been acquired. In this case the caller must _not_ call `unlock` since no lock was obtained. Typically it should signal an error back up the call stack so that errors can be dealt with appropriately at the correct level.

Note that this algorithm is not 'real-time' in the sense that it will time out exactly at the timeout value, but instead a (short) while after the timeout has lapsed. It is even possible that (in edge cases) a succesful lock can be returned a very short time after the timeout has lapsed.

## API

#### LMutex

```go
func (lm *LMutex) Lock()
func (lm *LMutex) GetLock(timeout time.Duration) (locked bool)
func (lm *LMutex) Unlock()
```

#### LRWMutex

```go
func (lm *LRWMutex) Lock()
func (lm *LRWMutex) GetLock(timeout time.Duration) (locked bool)
func (lm *LRWMutex) RLock()
func (lm *LRWMutex) GetRLock(timeout time.Duration) (locked bool)
func (lm *LRWMutex) Unlock()
func (lm *LRWMutex) RUnlock()
```

#### LFrequentAccess 
```go
func (lm *LFrequentAccess) ReadOnlyAccess() (constReadOnly interface{})
func (lm *LFrequentAccess) LockBeforeSet() (constCurVersion interface{})
func (lm *LFrequentAccess) SetNewCopyAndUnlock(newCopy interface{})
```

## Benchmarks

### sync.Mutex vs lsync.LMutex 

(with `defaultRetryUnit` and `defaultRetryCap` at 10 microsec)

```
BenchmarkMutex-8                   111           1579          +1322.52%
BenchmarkMutexSlack-8              120           1033          +760.83%
BenchmarkMutexWork-8               133           1604          +1106.02%
BenchmarkMutexWorkSlack-8          137           1038          +657.66%
```

(with `defaultRetryUnit` and `defaultRetryCap` at 1 millisec)
```
benchmark                          old ns/op     new ns/op     delta
BenchmarkMutex-8                   111           2649          +2286.49%
BenchmarkMutexSlack-8              120           1719          +1332.50%
BenchmarkMutexWork-8               133           2637          +1882.71%
BenchmarkMutexWorkSlack-8          137           1729          +1162.04%
```

(with `defaultRetryUnit` and `defaultRetryCap` at 100 millisec)

```
benchmark                          old ns/op     new ns/op     delta
BenchmarkMutex-8                   111           2649          +2286.49%
BenchmarkMutexSlack-8              120           2478          +1965.00%
BenchmarkMutexWork-8               133           2547          +1815.04%
BenchmarkMutexWorkSlack-8          137           2683          +1858.39%
```

### LFrequentAccess

An `lsync.LFrequentAccess` provides an atomic load and store of a consistently typed value.

```
benchmark                           old ns/op     new ns/op     delta
BenchmarkLFrequentAccessMap-8       114           4.67          -95.90%
BenchmarkLFrequentAccessSlice-8     109           5.95          -94.54%
```
