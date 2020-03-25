/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/color"

	"github.com/willf/bloom"
)

const (
	// Estimate bloom filter size. With this many items
	dataUsageTrackerEstItems = 250000
	// ... we want this false positive rate:
	dataUsageTrackerFP        = 0.99
	dataUsageTrackerQueueSize = 1000

	dataUsageTrackerVersion      = 1
	dataUsageTrackerFilename     = "tracker.bin"
	dataUsageTrackerSaveInterval = 5 * time.Minute
)

var (
	ObjUpdatedCh             chan<- string
	internalDataUsageTracker *dataUsageTracker
)

type dataUsageTracker struct {
	mu    sync.Mutex
	input chan string

	Current dataUsageFilter
	History dataUsageTrackerHistory
	Saved   time.Time
}

type dataUsageTrackerHistory []dataUsageFilter

type dataUsageFilter struct {
	idx uint64
	bf  bloomFilter
}

type bloomFilter struct {
	*bloom.BloomFilter
}

func (b bloomFilter) containsDir(in string) bool {
	split := strings.Split(in, SlashSeparator)

	// Trim empty start/end
	for len(split) > 0 {
		if len(split[0]) > 0 {
			break
		}
		split = split[1:]
	}
	for len(split) > 0 {
		if len(split[len(split)-1]) > 0 {
			break
		}
		split = split[:len(split)-1]
	}
	if len(split) == 0 {
		return true
	}
	// No deeper than level 3
	if len(split) > 3 {
		split = split[:3]
	}
	var tmp [dataUsageHashLen]byte
	hashPath(path.Join(split...)).bytes(tmp[:])
	return b.Test(tmp[:])
}

// sort the dataUsageTrackerHistory, newest first.
// Returns whether the history is complete.
func (d dataUsageTrackerHistory) sort() bool {
	if len(d) == 0 {
		return true
	}
	sort.Slice(d, func(i, j int) bool {
		return d[i].idx > d[j].idx
	})
	return d[0].idx-d[len(d)-1].idx == uint64(len(d))
}

func (d dataUsageTrackerHistory) all() bool {
	if len(d) == 0 {
		return true
	}
	sort.Slice(d, func(i, j int) bool {
		return d[i].idx > d[j].idx
	})
	return d[0].idx-d[len(d)-1].idx == uint64(len(d))
}

// removeOlderThan will remove entries older than index 'n'.
func (d *dataUsageTrackerHistory) removeOlderThan(n uint64) {
	d.sort()
	dd := *d
	end := len(dd)
	for i := end - 1; i >= 0; i-- {
		if dd[i].idx < n {
			end = i
		}
	}
	dd = dd[:end]
	*d = dd
}

func initDataUsageTracker() {
	internalDataUsageTracker = &dataUsageTracker{
		Current: dataUsageFilter{
			idx: 1,
		},

		input: make(chan string, dataUsageTrackerQueueSize),
	}
	internalDataUsageTracker.Current.bf = internalDataUsageTracker.newBloomFilter()
	ObjUpdatedCh = internalDataUsageTracker.input
}

func (d *dataUsageTracker) newBloomFilter() bloomFilter {
	return bloomFilter{bloom.NewWithEstimates(dataUsageTrackerEstItems, dataUsageTrackerFP)}
}

// start will load the current data from the drives start collecting information and
// start a saver goroutine.
// All of these will exit when the context is cancelled.
func (d *dataUsageTracker) start(ctx context.Context, drives []string) {
	d.load(ctx, drives)
	go d.startCollector(ctx)
	go d.startSaver(ctx, dataUsageTrackerSaveInterval, drives)
}

// load will attempt to load data tracking information from the supplied drives.
// The data will only be loaded if d.Saved is older than the found.
// The newest working cache will be kept in d.
// If no valid data usage tracker can be found d will remain unchanged.
// If object is shared the caller should lock it.
func (d *dataUsageTracker) load(ctx context.Context, drives []string) {
	for _, drive := range drives {
		func(drive string) {
			cacheFormatPath := pathJoin(drive, minioMetaBucket, dataUsageTrackerFilename)
			f, err := os.OpenFile(cacheFormatPath, os.O_RDWR, 0)

			if err != nil {
				if os.IsNotExist(err) {
					return
				}
				logger.LogIf(ctx, err)
				return
			}
			defer f.Close()
			err = d.deserialize(f, d.Saved)
			if err != nil {
				logger.LogIf(ctx, err)
				return
			}
		}(drive)
	}
}

// startSaver will start a saver that will write d to all supplied drives at specific intervals.
// The saver will save and exit when supplied context is closed.
func (d *dataUsageTracker) startSaver(ctx context.Context, interval time.Duration, drives []string) {
	t := time.NewTicker(interval)
	var buf bytes.Buffer
	for {
		var exit bool
		select {
		case <-ctx.Done():
			exit = true
		case <-t.C:
		}
		buf.Reset()
		d.mu.Lock()
		d.Saved = UTCNow()
		err := d.serialize(&buf)
		d.mu.Unlock()
		if err != nil {
			logger.LogIf(ctx, err, "Error serializing usage tracker data")
			if exit {
				return
			}
			continue
		}
		for _, drive := range drives {
			func(drive string) {
				cacheFormatPath := pathJoin(drive, minioMetaBucket, dataUsageTrackerFilename)
				err := ioutil.WriteFile(cacheFormatPath, buf.Bytes(), os.ModePerm)
				if err != nil {
					logger.LogIf(ctx, err)
					return
				}
			}(drive)
		}
		if exit {
			return
		}
	}
}

// serialize all data in d to dst.
// Caller should hold lock if d is expected to be shared.
// If an error is returned, there will likely be partial data written to dst.
func (d *dataUsageTracker) serialize(dst io.Writer) (err error) {
	var tmp [8]byte
	o := bufio.NewWriter(dst)
	defer func() {
		if err == nil {
			err = o.Flush()
		}
	}()

	// Version
	if err := o.WriteByte(dataUsageTrackerVersion); err != nil {
		return err
	}
	// Timestamp.
	binary.LittleEndian.PutUint64(tmp[:], uint64(d.Saved.Unix()))
	if _, err := o.Write(tmp[:]); err != nil {
		return err
	}

	// Current
	binary.LittleEndian.PutUint64(tmp[:], d.Current.idx)
	if _, err := o.Write(tmp[:]); err != nil {
		return err
	}

	if _, err := d.Current.bf.WriteTo(o); err != nil {
		return err
	}

	// History
	binary.LittleEndian.PutUint64(tmp[:], uint64(len(d.History)))
	for _, bf := range d.History {
		// Current
		binary.LittleEndian.PutUint64(tmp[:], bf.idx)
		if _, err := o.Write(tmp[:]); err != nil {
			return err
		}

		if _, err := bf.bf.WriteTo(o); err != nil {
			return err
		}
	}
	return nil
}

func (d *dataUsageTracker) deserialize(src io.Reader, newerThan time.Time) error {
	var dst dataUsageTracker
	var tmp [8]byte

	// Version
	if _, err := io.ReadFull(src, tmp[:1]); err != nil {
		return err
	}
	switch tmp[0] {
	case dataUsageTrackerVersion:
	default:
		return errors.New("dataUsageTracker: Unknown data version")
	}
	// Timestamp.
	if _, err := io.ReadFull(src, tmp[:8]); err != nil {
		return err
	}
	t := time.Unix(int64(binary.LittleEndian.Uint64(tmp[:])), 0)
	if !t.After(newerThan) {
		return nil
	}

	// Current
	if _, err := io.ReadFull(src, tmp[:8]); err != nil {
		return err
	}
	dst.Current.idx = binary.LittleEndian.Uint64(tmp[:])
	dst.Current.bf = d.newBloomFilter()
	if _, err := d.Current.bf.ReadFrom(src); err != nil {
		return err
	}

	// History
	if _, err := io.ReadFull(src, tmp[:8]); err != nil {
		return err
	}
	n := binary.LittleEndian.Uint64(tmp[:])
	dst.History = make(dataUsageTrackerHistory, int(n))
	for i, e := range dst.History {
		if _, err := io.ReadFull(src, tmp[:8]); err != nil {
			return err
		}
		e.idx = binary.LittleEndian.Uint64(tmp[:])
		e.bf = bloomFilter{}
		if _, err := e.bf.ReadFrom(src); err != nil {
			return err
		}
		dst.History[i] = e
	}
	// Ignore what remains on the stream.
	*d = dst
	return nil
}

// start a collector that picks up entries from ObjUpdatedCh
// and adds them  to the current bloom filter.
func (d *dataUsageTracker) startCollector(ctx context.Context) {
	var tmp [dataUsageHashLen]byte
	for {
		select {
		case <-ctx.Done():
			return
		case in := <-d.input:
			bucket, _ := path2BucketObjectWithBasePath("", in)
			if bucket == "" {
				if false {
					logger.Info(color.Green("data-usage:")+" no bucket (%s)", in)
				}
				continue
			}

			if isReservedOrInvalidBucket(bucket, false) {
				if false {
					logger.Info(color.Green("data-usage:")+" isReservedOrInvalidBucket: %v, entry: %v", bucket, in)
				}
				continue
			}
			in = path.Dir(in)
			split := strings.Split(in, SlashSeparator)

			// Trim empty start/end
			for len(split) > 0 {
				if len(split[0]) > 0 {
					break
				}
				split = split[1:]
			}
			for len(split) > 0 {
				if len(split[len(split)-1]) > 0 {
					break
				}
				split = split[:len(split)-1]
			}
			if len(split) == 0 {
				return
			}
			if len(split) > 3 {
				split = split[:3]
			}
			// Add all paths until level 3.
			d.mu.Lock()
			for i := range split {
				hashPath(path.Join(split[:i+1]...)).bytes(tmp[:])
				d.Current.bf.Add(tmp[:])
			}
			d.mu.Unlock()
		}
	}
}

func (d dataUsageTrackerHistory) find(idx uint64) *dataUsageFilter {
	for _, f := range d {
		if f.idx == idx {
			return &f
		}
	}
	return nil
}

// filterFrom will return a combined bloom filter.
func (d *dataUsageTracker) filterFrom(ctx context.Context, oldest, newest uint64) *bloomFilterResponse {
	bf := d.newBloomFilter()
	bfr := bloomFilterResponse{
		OldestIdx:  oldest,
		CurrentIdx: d.Current.idx,
		Complete:   true,
	}
	// Loop through each index requested.
	for idx := oldest; idx <= newest; idx++ {
		v := d.History.find(idx)
		if v == nil {
			if d.Current.idx == idx {
				// Merge current.
				err := bf.Merge(d.Current.bf.BloomFilter)
				logger.LogIf(ctx, err)
				if err != nil {
					bfr.Complete = false
				}
				continue
			}
			bfr.Complete = false
			bfr.OldestIdx = idx + 1
			continue
		}

		err := bf.Merge(v.bf.BloomFilter)
		if err != nil {
			bfr.Complete = false
			logger.LogIf(ctx, err)
			continue
		}
		bfr.NewestIdx = idx
	}
	var dst bytes.Buffer
	_, err := bf.WriteTo(&dst)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil
	}

	return &bfr
}

// cycleFilter will cycle the bloom filter to start recording to index y if not already.
// The response will contain a bloom filter starting at index x up to, but not including index y.
// If y is 0, the response will not update y, but return the currently recorded information
// from the up until and including current y.
func (d *dataUsageTracker) cycleFilter(ctx context.Context, oldest, current uint64) (*bloomFilterResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if current == 0 {
		if len(d.History) == 0 {
			return d.filterFrom(ctx, d.Current.idx, d.Current.idx), nil
		}
		d.History.sort()
		return d.filterFrom(ctx, d.History[len(d.History)-1].idx, d.Current.idx), nil
	}

	// Move current to history if new one requested
	if d.Current.idx != current {
		d.History = append(d.History, d.Current)
		d.Current.idx = current
		d.Current.bf = d.newBloomFilter()
	}
	d.History.removeOlderThan(oldest)
	return d.filterFrom(ctx, oldest, current), nil
}

type bloomFilterRequest struct {
	Oldest  uint64
	Current uint64
}

type bloomFilterResponse struct {
	// Current index being written to.
	CurrentIdx uint64
	// Oldest index in the returned bloom filter.
	OldestIdx uint64
	// Newest Index in the returned bloom filter.
	NewestIdx uint64
	// Are all indexes between oldest and newest filled?
	Complete bool
	// Binary data of the bloom filter.
	Filter []byte
}

// CycleBloomFilter will cycle the bloom filter to start recording to index y if not already.
// The response will contain a bloom filter starting at index x up to, but not including index y.
// If y is 0, the response will not update y, but return the currently recorded information
// from the current x to y-1.
func CycleBloomFilter(ctx context.Context, oldest, current uint64) (*bloomFilterResponse, error) {
	return internalDataUsageTracker.cycleFilter(ctx, oldest, current)
}
