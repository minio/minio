// Copyright (c) 2015-2024 MinIO, Inc.
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
	"context"
	"sort"
	"sync"
	"time"
)

/*

bucketsScanMgr decides what is the next bucket to scan for a given pool/set. It
holds internal information about when the last time a bucket was finished scanned
to select buckets with the oldest last scan for the next scan operation.

A minimal use of the bucketsScanMgr is by doing this:
```
  mgr := newBucketsScanMgr(ObjectLayer)
  for bucket := range mgr.getBucketCh() {
      ...
  }
```

*/

const (
	// the interval to discover if there are new buckets created in the cluster
	bucketsListInterval = time.Minute
)

type setID struct {
	pool, set int
}

type bucketScanStat struct {
	ongoing      bool      // this bucket is currently being scanned
	lastFinished time.Time // the last cycle of this scan
	lastUpdate   time.Time
	cycle        uint32 // the last cycle of this scan
	lifecycle    bool   // This bucket has lifecycle set
}

type bucketsScanMgr struct {
	ctx context.Context

	// A registered function which knows how to list buckets
	bucketsLister func(context.Context, BucketOptions) ([]BucketInfo, error)

	mu           sync.RWMutex
	knownBuckets map[string]bool                     // Current buckets in the S3 namespace
	bucketsCh    map[setID]chan string               // A map of an erasure set identifier and a channel of buckets to scan
	bucketStats  map[setID]map[string]bucketScanStat // A map of an erasure set identifier and bucket scan stats
}

func newBucketsScanMgr(s3 ObjectLayer) *bucketsScanMgr {
	mgr := &bucketsScanMgr{
		ctx:           GlobalContext,
		bucketsLister: s3.ListBuckets,
		bucketStats:   make(map[setID]map[string]bucketScanStat),
		bucketsCh:     make(map[setID]chan string),
	}
	return mgr
}

func (mgr *bucketsScanMgr) getKnownBuckets() []string {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	ret := make([]string, 0, len(mgr.knownBuckets))
	for k := range mgr.knownBuckets {
		ret = append(ret, k)
	}
	sort.Strings(ret)
	return ret
}

func (mgr *bucketsScanMgr) isKnownBucket(bucket string) bool {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	_, ok := mgr.knownBuckets[bucket]
	return ok
}

func (mgr *bucketsScanMgr) start() {
	firstListBucketsDone := make(chan interface{})

	// A routine that discovers new buckets,  initialize scan stats for each new bucket
	// and update if each bucket has a lifecycle document
	go func() {
		t := time.NewTimer(bucketsListInterval)
		defer t.Stop()

		firstTime := true

		for {
			select {
			case <-t.C:
				buckets, err := mgr.bucketsLister(mgr.ctx, BucketOptions{})
				if err == nil {
					knownBuckets := make(map[string]bool, len(buckets)) // bucket => has-lifecycle
					for _, b := range buckets {
						hasLifecycle := false
						if l, err := globalLifecycleSys.Get(b.Name); err == nil && l.HasActiveRules("") {
							hasLifecycle = true
						}
						knownBuckets[b.Name] = hasLifecycle
					}

					mgr.mu.Lock()
					for bucket, hasLifecycle := range knownBuckets {
						for _, set := range mgr.bucketStats {
							st := set[bucket]
							st.lifecycle = hasLifecycle
							set[bucket] = st
						}
					}
					mgr.knownBuckets = knownBuckets
					mgr.mu.Unlock()

					if firstTime {
						close(firstListBucketsDone)
						firstTime = false
					}
				}
				t.Reset(bucketsListInterval)
			case <-mgr.ctx.Done():
				return
			}
		}
	}()

	<-firstListBucketsDone

	// Clean up internal data when a deleted bucket is found
	go func() {
		const cleanInterval = 30 * time.Second

		t := time.NewTimer(cleanInterval)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				mgr.mu.Lock()
				for _, set := range mgr.bucketStats {
					for bkt := range set {
						if _, ok := mgr.knownBuckets[bkt]; !ok {
							delete(set, bkt)
						}
					}
				}
				mgr.mu.Unlock()

				t.Reset(cleanInterval)
			case <-mgr.ctx.Done():
				return
			}
		}
	}()

	// A routine that sends the next bucket to scan for each erasure set listener
	go func() {
		tick := 10 * time.Second

		t := time.NewTimer(tick)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				mgr.mu.RLock()
				for id, ch := range mgr.bucketsCh {
					if len(ch) == 0 {
						b := mgr.getNextBucketLocked(id)
						if b != "" {
							select {
							case ch <- b:
							default:
							}
						}
					}
				}
				mgr.mu.RUnlock()

				t.Reset(tick)
			case <-mgr.ctx.Done():
				return
			}
		}
	}()
}

// Return a channel of buckets names to scan a given erasure set identifier
func (mgr *bucketsScanMgr) getBucketCh(id setID) chan string {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	mgr.bucketStats[id] = make(map[string]bucketScanStat)
	mgr.bucketsCh[id] = make(chan string, 1)

	return mgr.bucketsCh[id]
}

func scanBefore(st1, st2 bucketScanStat) bool {
	if st1.ongoing != st2.ongoing {
		return st1.ongoing == false
	}
	if st1.lastFinished.Before(st2.lastFinished) {
		return true
	}
	if st1.lifecycle != st2.lifecycle {
		return st1.lifecycle == true
	}
	return false
}

// Return the next bucket name to scan of a given erasure set identifier
// If all buckets are in a scanning state, return empty result
func (mgr *bucketsScanMgr) getNextBucketLocked(id setID) string {
	var (
		nextBucketStat = bucketScanStat{}
		nextBucketName = ""
	)

	for bucket, stat := range mgr.bucketStats[id] {
		if stat.ongoing {
			continue
		}
		if nextBucketName == "" {
			nextBucketName = bucket
			nextBucketStat = stat
			continue
		}
		if nextBucketName == "" || scanBefore(stat, nextBucketStat) {
			nextBucketStat = stat
			nextBucketName = bucket
		}
	}

	return nextBucketName
}

// Mark a bucket as started in a specific erasure set
func (mgr *bucketsScanMgr) markBucketScanStarted(id setID, bucket string, cycle uint32, lastKnownUpdate time.Time) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	m, _ := mgr.bucketStats[id][bucket]
	m.ongoing = true
	m.cycle = cycle
	m.lastUpdate = lastKnownUpdate
	mgr.bucketStats[id][bucket] = m
	return
}

// Mark a bucket as done in a specific erasure set
func (mgr *bucketsScanMgr) markBucketScanDone(id setID, bucket string) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	m, _ := mgr.bucketStats[id][bucket]
	m.ongoing = false
	m.lastFinished = time.Now()
	mgr.bucketStats[id][bucket] = m
}
