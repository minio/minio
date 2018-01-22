dsync [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io) [![Go Report Card](https://goreportcard.com/badge/minio/minio)](https://goreportcard.com/report/minio/minio) [![codecov](https://codecov.io/gh/minio/dsync/branch/master/graph/badge.svg)](https://codecov.io/gh/minio/dsync)
=====

A distributed locking and syncing package for Go.

Introduction
------------
 
`dsync` is a package for doing distributed locks over a network of `n` nodes. It is designed with simplicity in mind and hence offers limited scalability (`n <= 16`). Each node will be connected to all other nodes and lock requests from any node will be broadcast to all connected nodes. A node will succeed in getting the lock if `n/2 + 1` nodes (whether or not including itself) respond positively. If the lock is acquired it can be held for as long as the client desires and needs to be released afterwards. This will cause the release to be broadcast to all nodes after which the lock becomes available again.

Motivation
----------

This package was developed for the distributed server version of [Minio Object Storage](https://minio.io/). For this we needed a distributed locking mechanism for up to 16 servers that each would be running `minio server`. The locking mechanism itself should be a reader/writer mutual exclusion lock meaning that it can be held by a single writer or an arbitrary number of readers.

For [minio](https://minio.io/) the distributed version is started as follows (for a 6-server system):

```
$ minio server http://server1/disk http://server2/disk http://server3/disk http://server4/disk http://server5/disk http://server6/disk 
```
 
_(note that the same identical command should be run on servers `server1` through to `server6`)_

Design goals
------------

* **Simple design**: by keeping the design simple, many tricky edge cases can be avoided.
* **No master node**: there is no concept of a master node which, if this would be used and the master would be down, causes locking to come to a complete stop. (Unless you have a design with a slave node but this adds yet more complexity.)
* **Resilient**: if one or more nodes go down, the other nodes should not be affected and can continue to acquire locks (provided not more than `n/2 - 1` nodes are down).
* Drop-in replacement for `sync.RWMutex` and supports [`sync.Locker`](https://github.com/golang/go/blob/master/src/sync/mutex.go#L30) interface.
* Automatically reconnect to (restarted) nodes.

Restrictions
------------

* Limited scalability: up to 16 nodes.
* Fixed configuration: changes in the number and/or network names/IP addresses need a restart of all nodes in order to take effect.
* If a down node comes up, it will not try to (re)acquire any locks that it may have held.
* Not designed for high performance applications such as key/value stores.

Performance
-----------

* Support up to a total of 7500 locks/second for maximum size of 16 nodes (consuming 10% CPU usage per server) on moderately powerful server hardware.
* Lock requests (successful) should not take longer than 1ms (provided decent network connection of 1 Gbit or more between the nodes).

The tables below show detailed performance numbers. 

### Performance with varying number of nodes

This table shows test performance on the same (EC2) instance type but with a varying number of nodes:

| EC2 Instance Type    | Nodes |     Locks/server/sec | Total Locks/sec | CPU Usage |
| -------------------- | -----:| --------------------:| ---------------:| ---------:|
| c3.2xlarge           |     4 | (min=3110, max=3376) |           12972 |       25% |
| c3.2xlarge           |     8 | (min=1884, max=2096) |           15920 |       25% |
| c3.2xlarge           |    12 | (min=1239, max=1558) |           16782 |       25% |
| c3.2xlarge           |    16 |  (min=996, max=1391) |           19096 |       25% |

The min and max locks/server/sec gradually declines but due to the larger number of nodes the overall total number of locks rises steadily (at the same CPU usage level).

### Performance with difference instance types

This table shows test performance for a fixed number of 8 nodes on different EC2 instance types:

| EC2 Instance Type    | Nodes |     Locks/server/sec | Total Locks/sec | CPU Usage |
| -------------------- | -----:| --------------------:| ---------------:| ---------:|
| c3.large (2 vCPU)    |     8 |  (min=823,  max=896) |            6876 |       75% |
| c3.2xlarge (8 vCPU)  |     8 | (min=1884, max=2096) |           15920 |       25% |
| c3.8xlarge (32 vCPU) |     8 | (min=2601, max=2898) |           21996 |       10% |

With the rise in the number of cores the CPU load decreases and overall performance increases.

### Stress test

Stress test on a c3.8xlarge (32 vCPU) instance type:

| EC2 Instance Type    | Nodes |     Locks/server/sec | Total Locks/sec | CPU Usage |
| -------------------- | -----:| --------------------:| ---------------:| ---------:|
| c3.8xlarge           |     8 | (min=2601, max=2898) |           21996 |       10% |
| c3.8xlarge           |     8 | (min=4756, max=5227) |           39932 |       20% |
| c3.8xlarge           |     8 | (min=7979, max=8517) |           65984 |       40% |
| c3.8xlarge           |     8 | (min=9267, max=9469) |           74944 |       50% |

The system can be pushed to 75K locks/sec at 50% CPU load.

Usage
-----

> NOTE: Previously if you were using `dsync.Init([]NetLocker, nodeIndex)` to initialize dsync has
been changed to `dsync.New([]NetLocker, nodeIndex)` which returns a `*Dsync` object to be used in
every instance of `NewDRWMutex("test", *Dsync)`

### Exclusive lock 

Here is a simple example showing how to protect a single resource (drop-in replacement for `sync.Mutex`):

```go
import (
	"github.com/minio/dsync"
)

func lockSameResource() {

	// Create distributed mutex to protect resource 'test'
	dm := dsync.NewDRWMutex("test", ds)

	dm.Lock()
	log.Println("first lock granted")

	// Release 1st lock after 5 seconds
	go func() {
		time.Sleep(5 * time.Second)
		log.Println("first lock unlocked")
		dm.Unlock()
	}()

	// Try to acquire lock again, will block until initial lock is released
	log.Println("about to lock same resource again...")
	dm.Lock()
	log.Println("second lock granted")

	time.Sleep(2 * time.Second)
	dm.Unlock()
}
```

which gives the following output:

```
2016/09/02 14:50:00 first lock granted
2016/09/02 14:50:00 about to lock same resource again...
2016/09/02 14:50:05 first lock unlocked
2016/09/02 14:50:05 second lock granted
```

### Read locks

DRWMutex also supports multiple simultaneous read locks as shown below (analogous to `sync.RWMutex`)

```
func twoReadLocksAndSingleWriteLock() {

	drwm := dsync.NewDRWMutex("resource", ds)

	drwm.RLock()
	log.Println("1st read lock acquired, waiting...")

	drwm.RLock()
	log.Println("2nd read lock acquired, waiting...")

	go func() {
		time.Sleep(1 * time.Second)
		drwm.RUnlock()
		log.Println("1st read lock released, waiting...")
	}()

	go func() {
		time.Sleep(2 * time.Second)
		drwm.RUnlock()
		log.Println("2nd read lock released, waiting...")
	}()

	log.Println("Trying to acquire write lock, waiting...")
	drwm.Lock()
	log.Println("Write lock acquired, waiting...")

	time.Sleep(3 * time.Second)

	drwm.Unlock()
}
```

which gives the following output:

```
2016/09/02 15:05:20 1st read lock acquired, waiting...
2016/09/02 15:05:20 2nd read lock acquired, waiting...
2016/09/02 15:05:20 Trying to acquire write lock, waiting...
2016/09/02 15:05:22 1st read lock released, waiting...
2016/09/02 15:05:24 2nd read lock released, waiting...
2016/09/02 15:05:24 Write lock acquired, waiting...
```

Basic architecture
------------------

### Lock process

The basic steps in the lock process are as follows:
- broadcast lock message to all `n` nodes
- collect all responses within certain time-out window
  - if quorum met (minimally `n/2 + 1` responded positively) then grant lock 
  - otherwise release all underlying locks and try again after a (semi-)random delay
- release any locks that (still) came in after time time-out window

### Unlock process

The unlock process is really simple:
- broadcast unlock message to all nodes that granted lock
- if a destination is not available, retry with gradually longer back-off window to still deliver
- ignore the 'result' (cover for cases where destination node has gone down and came back up)

Dealing with Stale Locks
------------------------

A 'stale' lock is a lock that is left at a node while the client that originally acquired the client either:
- never released the lock (due to eg a crash) or
- is disconnected from the network and henceforth not able to deliver the unlock message.

Too many stale locks can prevent a new lock on a resource from being acquired, that is, if the sum of the stale locks and the number of down nodes is greater than `n/2 - 1`. In `dsync` a recovery mechanism is implemented to remove stale locks (see [here](https://github.com/minio/dsync/pull/22#issue-176751755) for the details).

Known deficiencies
------------------

Known deficiencies can be divided into two categories, namely a) more than one write lock granted and b) lock not becoming available anymore.

### More than one write lock

So far we have identified one case during which this can happen (example for 8 node system):
- 3 nodes are down (say 6, 7, and 8)
- node 1 acquires a lock on "test" (nodes 1 through to 5 giving quorum)
- node 4 and 5 crash (dropping the lock)
- nodes 4 through to 8 restart
- node 4 acquires a lock on "test" (nodes 4 through to 8 giving quorum)

Now we have two concurrent locks on the same resource name which violates the core requirement. Note that if just a single server out of 4 or 5 crashes that we are still fine because the second lock cannot acquire quorum.

This table summarizes the conditions for different configurations during which this can happen:

| Nodes | Down nodes | Crashed nodes | Total nodes |
| -----:| ----------:| -------------:| -----------:|
|     4 |          1 |             2 |           3 |
|     8 |          3 |             2 |           5 |
|    12 |          5 |             2 |           7 |
|    16 |          7 |             2 |           9 |

(for more info see `testMultipleServersOverQuorumDownDuringLockKnownError` in [chaos.go](https://github.com/minio/dsync/blob/master/chaos/chaos.go))
 
### Lock not available anymore

This would be due to too many stale locks and/or too many servers down (total over `n/2 - 1`). The following table shows the maximum toterable number for different node sizes:

| Nodes |  Max tolerable |
| -----:|  -------------:|
|     4 |              1 |
|     8 |              3 |
|    12 |              5 |
|    16 |              7 |

If you see any other short comings, we would be interested in hearing about them.

Tackled issues
--------------

* When two nodes want to acquire the same lock at precisely the same time, it is possible for both to just acquire `n/2` locks and there is no majority winner. Both will fail back to their clients and will retry later after a semi-randomized delay.

Server side logic
-----------------

On the server side just the following logic needs to be added (barring some extra error checking):

```
const WriteLock = -1

type lockServer struct {
	mutex   sync.Mutex
	lockMap map[string]int64 // Map of locks, with negative value indicating (exclusive) write lock
	                         // and positive values indicating number of read locks
}

func (l *lockServer) Lock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if _, *reply = l.lockMap[args.Name]; !*reply {
		l.lockMap[args.Name] = WriteLock // No locks held on the given name, so claim write lock
	}
	*reply = !*reply // Negate *reply to return true when lock is granted or false otherwise
	return nil
}

func (l *lockServer) Unlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var locksHeld int64
	if locksHeld, *reply = l.lockMap[args.Name]; !*reply { // No lock is held on the given name
		return fmt.Errorf("Unlock attempted on an unlocked entity: %s", args.Name) 
	}
	if *reply = locksHeld == WriteLock; !*reply { // Unless it is a write lock
		return fmt.Errorf("Unlock attempted on a read locked entity: %s (%d read locks active)", args.Name, locksHeld)
	}
	delete(l.lockMap, args.Name) // Remove the write lock
	return nil
}
```

If you also want RLock()/RUnlock() functionality, then add this as well:

```
const ReadLock = 1

func (l *lockServer) RLock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var locksHeld int64
	if locksHeld, *reply = l.lockMap[args.Name]; !*reply {
		l.lockMap[args.Name] = ReadLock // No locks held on the given name, so claim (first) read lock
		*reply = true
	} else {
		if *reply = locksHeld != WriteLock; *reply { // Unless there is a write lock
			l.lockMap[args.Name] = locksHeld + ReadLock // Grant another read lock
		}
	}
	return nil
}

func (l *lockServer) RUnlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var locksHeld int64
	if locksHeld, *reply = l.lockMap[args.Name]; !*reply { // No lock is held on the given name
		return fmt.Errorf("RUnlock attempted on an unlocked entity: %s", args.Name)
	}
	if *reply = locksHeld != WriteLock; !*reply { // A write-lock is held, cannot release a read lock
		return fmt.Errorf("RUnlock attempted on a write locked entity: %s", args.Name)
	}
	if locksHeld > ReadLock {
		l.lockMap[args.Name] = locksHeld - ReadLock // Remove one of the read locks held
	} else {
		delete(l.lockMap, args.Name) // Remove the (last) read lock
	}
	return nil
}
```

See [dsync-server_test.go](https://github.com/fwessels/dsync/blob/master/dsync-server_test.go) for a full implementation.

Sub projects
------------

* See [performance](https://github.com/minio/dsync/tree/master/performance) directory for performance measurements
* See [chaos](https://github.com/minio/dsync/tree/master/chaos) directory for some edge cases

Testing
-------

The full test code (including benchmarks) from `sync/rwmutex_test.go` is used for testing purposes.

Extensions / Other use cases
----------------------------

### Robustness vs Performance

It is possible to trade some level of robustness with overall performance by not contacting each node for every Lock()/Unlock() cycle. In the normal case (example for `n = 16` nodes) a total of 32 RPC messages is sent and the lock is granted if at least a quorum of `n/2 + 1` nodes respond positively. When all nodes are functioning normally this would mean `n = 16` positive responses and, in fact, `n/2 - 1 = 7` responses over the (minimum) quorum of `n/2 + 1 = 9`. So you could say that this is some overkill, meaning that even if 6 nodes are down you still have an extra node over the quorum.

For this case it is possible to reduce the number of nodes to be contacted to for example `12`. Instead of 32 RPC messages now 24 message will be sent which is 25% less. As the performance is mostly depending on the number of RPC messages sent, the total locks/second handled by all nodes would increase by 33% (given the same CPU load).

You do however want to make sure that you have some sort of 'random' selection of which 12 out of the 16 nodes will participate in every lock. See [here](https://gist.github.com/fwessels/dbbafd537c13ec8f88b360b3a0091ac0) for some sample code that could help with this.

### Scale beyond 16 nodes?

Building on the previous example and depending on how resilient you want to be for outages of nodes, you can also go the other way, namely to increase the total number of nodes while keeping the number of nodes contacted per lock the same.

For instance you could imagine a system of 32 nodes where only a quorom majority of `9` would be needed out of `12` nodes. Again this requires some sort of pseudo-random 'deterministic' selection of 12 nodes out of the total of 32 servers (same [example](https://gist.github.com/fwessels/dbbafd537c13ec8f88b360b3a0091ac0) as above). 

Other techniques
----------------

We are well aware that there are more sophisticated systems such as zookeeper, raft, etc. However we found that for our limited use case this was adding too much complexity. So if `dsync` does not meet your requirements than you are probably better off using one of those systems.

Other links that you may find interesting:
- [Distributed locks with Redis](http://redis.io/topics/distlock)
- Based on the above: [Redis-based distributed mutual exclusion lock implementation for Go](https://github.com/hjr265/redsync.go)

Performance of `net/rpc` vs `grpc`
----------------------------------

We did an analysis of the performance of `net/rpc` vs `grpc`, see [here](https://github.com/golang/go/issues/16844#issuecomment-245261755), so we'll stick with `net/rpc` for now.

License
-------

Released under the Apache License v2.0. You can find the complete text in the file LICENSE.

Contributing
------------

Contributions are welcome, please send PRs for any enhancements.
