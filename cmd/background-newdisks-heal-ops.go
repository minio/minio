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

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/env"
)

const (
	defaultMonitorNewDiskInterval = time.Second * 10
	healingTrackerFilename        = ".healing.bin"
)

//go:generate msgp -file $GOFILE -unexported

// healingTracker is used to persist healing information during a heal.
type healingTracker struct {
	disk StorageAPI    `msg:"-"`
	mu   *sync.RWMutex `msg:"-"`

	ID         string
	PoolIndex  int
	SetIndex   int
	DiskIndex  int
	Path       string
	Endpoint   string
	Started    time.Time
	LastUpdate time.Time

	ObjectsTotalCount uint64
	ObjectsTotalSize  uint64

	ItemsHealed uint64
	ItemsFailed uint64

	BytesDone   uint64
	BytesFailed uint64

	// Last object scanned.
	Bucket string `json:"-"`
	Object string `json:"-"`

	// Numbers when current bucket started healing,
	// for resuming with correct numbers.
	ResumeItemsHealed  uint64 `json:"-"`
	ResumeItemsFailed  uint64 `json:"-"`
	ResumeItemsSkipped uint64 `json:"-"`
	ResumeBytesDone    uint64 `json:"-"`
	ResumeBytesFailed  uint64 `json:"-"`
	ResumeBytesSkipped uint64 `json:"-"`

	// Filled on startup/restarts.
	QueuedBuckets []string

	// Filled during heal.
	HealedBuckets []string

	// ID of the current healing operation
	HealID string

	ItemsSkipped uint64
	BytesSkipped uint64

	RetryAttempts uint64

	Finished bool // finished healing, whether with errors or not

	// Add future tracking capabilities
	// Be sure that they are included in toHealingDisk
}

// loadHealingTracker will load the healing tracker from the supplied disk.
// The disk ID will be validated against the loaded one.
func loadHealingTracker(ctx context.Context, disk StorageAPI) (*healingTracker, error) {
	if disk == nil {
		return nil, errors.New("loadHealingTracker: nil drive given")
	}
	diskID, err := disk.GetDiskID()
	if err != nil {
		return nil, err
	}
	b, err := disk.ReadAll(ctx, minioMetaBucket,
		pathJoin(bucketMetaPrefix, healingTrackerFilename))
	if err != nil {
		return nil, err
	}
	var h healingTracker
	_, err = h.UnmarshalMsg(b)
	if err != nil {
		return nil, err
	}
	if h.ID != diskID && h.ID != "" {
		return nil, fmt.Errorf("loadHealingTracker: drive id mismatch expected %s, got %s", h.ID, diskID)
	}
	h.disk = disk
	h.ID = diskID
	h.mu = &sync.RWMutex{}
	return &h, nil
}

// newHealingTracker will create a new healing tracker for the disk.
func newHealingTracker() *healingTracker {
	return &healingTracker{
		mu: &sync.RWMutex{},
	}
}

func initHealingTracker(disk StorageAPI, healID string) *healingTracker {
	h := newHealingTracker()
	diskID, _ := disk.GetDiskID()
	h.disk = disk
	h.ID = diskID
	h.HealID = healID
	h.Path = disk.String()
	h.Endpoint = disk.Endpoint().String()
	h.Started = time.Now().UTC()
	h.PoolIndex, h.SetIndex, h.DiskIndex = disk.GetDiskLoc()
	return h
}

func (h *healingTracker) resetHealing() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.ItemsHealed = 0
	h.ItemsFailed = 0
	h.BytesDone = 0
	h.BytesFailed = 0
	h.ResumeItemsHealed = 0
	h.ResumeItemsFailed = 0
	h.ResumeBytesDone = 0
	h.ResumeBytesFailed = 0
	h.ItemsSkipped = 0
	h.BytesSkipped = 0

	h.HealedBuckets = nil
	h.Object = ""
	h.Bucket = ""
}

func (h *healingTracker) getLastUpdate() time.Time {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.LastUpdate
}

func (h *healingTracker) getBucket() string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.Bucket
}

func (h *healingTracker) setBucket(bucket string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.Bucket = bucket
}

func (h *healingTracker) getObject() string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.Object
}

func (h *healingTracker) setObject(object string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.Object = object
}

func (h *healingTracker) updateProgress(success, skipped bool, bytes uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch {
	case success:
		h.ItemsHealed++
		h.BytesDone += bytes
	case skipped:
		h.ItemsSkipped++
		h.BytesSkipped += bytes
	default:
		h.ItemsFailed++
		h.BytesFailed += bytes
	}
}

// update will update the tracker on the disk.
// If the tracker has been deleted an error is returned.
func (h *healingTracker) update(ctx context.Context) error {
	h.mu.Lock()
	if h.ID == "" || h.PoolIndex < 0 || h.SetIndex < 0 || h.DiskIndex < 0 {
		h.ID, _ = h.disk.GetDiskID()
		h.PoolIndex, h.SetIndex, h.DiskIndex = h.disk.GetDiskLoc()
	}
	h.mu.Unlock()
	return h.save(ctx)
}

// save will unconditionally save the tracker and will be created if not existing.
func (h *healingTracker) save(ctx context.Context) error {
	h.mu.Lock()
	if h.PoolIndex < 0 || h.SetIndex < 0 || h.DiskIndex < 0 {
		// Attempt to get location.
		if api := newObjectLayerFn(); api != nil {
			if ep, ok := api.(*erasureServerPools); ok {
				h.PoolIndex, h.SetIndex, h.DiskIndex, _ = ep.getPoolAndSet(h.ID)
			}
		}
	}
	h.LastUpdate = time.Now().UTC()
	htrackerBytes, err := h.MarshalMsg(nil)
	h.mu.Unlock()
	if err != nil {
		return err
	}
	globalBackgroundHealState.updateHealStatus(h)
	return h.disk.WriteAll(ctx, minioMetaBucket,
		pathJoin(bucketMetaPrefix, healingTrackerFilename),
		htrackerBytes)
}

// delete the tracker on disk.
func (h *healingTracker) delete(ctx context.Context) error {
	return h.disk.Delete(ctx, minioMetaBucket,
		pathJoin(bucketMetaPrefix, healingTrackerFilename),
		DeleteOptions{
			Recursive: false,
			Immediate: false,
		},
	)
}

func (h *healingTracker) isHealed(bucket string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return slices.Contains(h.HealedBuckets, bucket)
}

// resume will reset progress to the numbers at the start of the bucket.
func (h *healingTracker) resume() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.ItemsHealed = h.ResumeItemsHealed
	h.ItemsFailed = h.ResumeItemsFailed
	h.ItemsSkipped = h.ResumeItemsSkipped
	h.BytesDone = h.ResumeBytesDone
	h.BytesFailed = h.ResumeBytesFailed
	h.BytesSkipped = h.ResumeBytesSkipped
}

// bucketDone should be called when a bucket is done healing.
// Adds the bucket to the list of healed buckets and updates resume numbers.
func (h *healingTracker) bucketDone(bucket string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.ResumeItemsHealed = h.ItemsHealed
	h.ResumeItemsFailed = h.ItemsFailed
	h.ResumeItemsSkipped = h.ItemsSkipped
	h.ResumeBytesDone = h.BytesDone
	h.ResumeBytesFailed = h.BytesFailed
	h.ResumeBytesSkipped = h.BytesSkipped
	h.HealedBuckets = append(h.HealedBuckets, bucket)
	for i, b := range h.QueuedBuckets {
		if b == bucket {
			// Delete...
			h.QueuedBuckets = append(h.QueuedBuckets[:i], h.QueuedBuckets[i+1:]...)
		}
	}
}

// setQueuedBuckets will add buckets, but exclude any that is already in h.HealedBuckets.
// Order is preserved.
func (h *healingTracker) setQueuedBuckets(buckets []BucketInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()

	s := set.CreateStringSet(h.HealedBuckets...)
	h.QueuedBuckets = make([]string, 0, len(buckets))
	for _, b := range buckets {
		if !s.Contains(b.Name) {
			h.QueuedBuckets = append(h.QueuedBuckets, b.Name)
		}
	}
}

func (h *healingTracker) printTo(writer io.Writer) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	b, err := json.MarshalIndent(h, "", "  ")
	if err != nil {
		writer.Write([]byte(err.Error()))
		return
	}
	writer.Write(b)
}

// toHealingDisk converts the information to madmin.HealingDisk
func (h *healingTracker) toHealingDisk() madmin.HealingDisk {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return madmin.HealingDisk{
		ID:                h.ID,
		HealID:            h.HealID,
		Endpoint:          h.Endpoint,
		PoolIndex:         h.PoolIndex,
		SetIndex:          h.SetIndex,
		DiskIndex:         h.DiskIndex,
		Finished:          h.Finished,
		Path:              h.Path,
		Started:           h.Started.UTC(),
		LastUpdate:        h.LastUpdate.UTC(),
		ObjectsTotalCount: h.ObjectsTotalCount,
		ObjectsTotalSize:  h.ObjectsTotalSize,
		ItemsHealed:       h.ItemsHealed,
		ItemsSkipped:      h.ItemsSkipped,
		ItemsFailed:       h.ItemsFailed,
		BytesDone:         h.BytesDone,
		BytesSkipped:      h.BytesSkipped,
		BytesFailed:       h.BytesFailed,
		Bucket:            h.Bucket,
		Object:            h.Object,
		QueuedBuckets:     h.QueuedBuckets,
		HealedBuckets:     h.HealedBuckets,
		RetryAttempts:     h.RetryAttempts,

		ObjectsHealed: h.ItemsHealed, // Deprecated July 2021
		ObjectsFailed: h.ItemsFailed, // Deprecated July 2021

	}
}

func initAutoHeal(ctx context.Context, objAPI ObjectLayer) {
	z, ok := objAPI.(*erasureServerPools)
	if !ok {
		return
	}

	initBackgroundHealing(ctx, objAPI) // start quick background healing
	if env.Get("_MINIO_AUTO_DRIVE_HEALING", config.EnableOn) == config.EnableOn {
		globalBackgroundHealState.pushHealLocalDisks(getLocalDisksToHeal()...)
		go monitorLocalDisksAndHeal(ctx, z)
	}

	go globalMRFState.startMRFPersistence()
	go globalMRFState.healRoutine(z)
}

func getLocalDisksToHeal() (disksToHeal Endpoints) {
	globalLocalDrivesMu.RLock()
	localDrives := cloneDrives(globalLocalDrivesMap)
	globalLocalDrivesMu.RUnlock()
	for _, disk := range localDrives {
		_, err := disk.DiskInfo(context.Background(), DiskInfoOptions{})
		if errors.Is(err, errUnformattedDisk) {
			disksToHeal = append(disksToHeal, disk.Endpoint())
			continue
		}
		if h := disk.Healing(); h != nil && !h.Finished {
			disksToHeal = append(disksToHeal, disk.Endpoint())
		}
	}
	if len(disksToHeal) == globalEndpoints.NEndpoints() {
		// When all disks == all command line endpoints
		// this is a fresh setup, no need to trigger healing.
		return Endpoints{}
	}
	return disksToHeal
}

var newDiskHealingTimeout = newDynamicTimeout(30*time.Second, 10*time.Second)

var errRetryHealing = errors.New("some items failed to heal, we will retry healing this drive again")

func healFreshDisk(ctx context.Context, z *erasureServerPools, endpoint Endpoint) error {
	poolIdx, setIdx := endpoint.PoolIdx, endpoint.SetIdx
	disk := getStorageViaEndpoint(endpoint)
	if disk == nil {
		return fmt.Errorf("Unexpected error disk must be initialized by now after formatting: %s", endpoint)
	}

	_, err := disk.DiskInfo(ctx, DiskInfoOptions{})
	if err != nil {
		if errors.Is(err, errDriveIsRoot) {
			// This is a root drive, ignore and move on
			return nil
		}
		if !errors.Is(err, errUnformattedDisk) {
			return err
		}
	}

	// Prevent parallel erasure set healing
	locker := z.NewNSLock(minioMetaBucket, fmt.Sprintf("new-drive-healing/%d/%d", poolIdx, setIdx))
	lkctx, err := locker.GetLock(ctx, newDiskHealingTimeout)
	if err != nil {
		return fmt.Errorf("Healing of drive '%v' on %s pool, belonging to %s erasure set already in progress: %w",
			disk, humanize.Ordinal(poolIdx+1), humanize.Ordinal(setIdx+1), err)
	}
	ctx = lkctx.Context()
	defer locker.Unlock(lkctx)

	// Load healing tracker in this disk
	tracker, err := loadHealingTracker(ctx, disk)
	if err != nil {
		// A healing tracker may be deleted if another disk in the
		// same erasure set with same healing-id successfully finished
		// healing.
		if errors.Is(err, errFileNotFound) {
			return nil
		}
		healingLogIf(ctx, fmt.Errorf("Unable to load healing tracker on '%s': %w, re-initializing..", disk, err))
		tracker = initHealingTracker(disk, mustGetUUID())
	}

	healingLogEvent(ctx, "Healing drive '%s' - 'mc admin heal alias/ --verbose' to check the current status.", endpoint)

	buckets, _ := z.ListBuckets(ctx, BucketOptions{})
	// Buckets data are dispersed in multiple pools/sets, make
	// sure to heal all bucket metadata configuration.
	buckets = append(buckets, BucketInfo{
		Name: pathJoin(minioMetaBucket, minioConfigPrefix),
	}, BucketInfo{
		Name: pathJoin(minioMetaBucket, bucketMetaPrefix),
	})

	// Heal latest buckets first.
	sort.Slice(buckets, func(i, j int) bool {
		a, b := strings.HasPrefix(buckets[i].Name, minioMetaBucket), strings.HasPrefix(buckets[j].Name, minioMetaBucket)
		if a != b {
			return a
		}
		return buckets[i].Created.After(buckets[j].Created)
	})

	// Load bucket totals
	cache := dataUsageCache{}
	if err := cache.load(ctx, z.serverPools[poolIdx].sets[setIdx], dataUsageCacheName); err == nil {
		dataUsageInfo := cache.dui(dataUsageRoot, nil)
		tracker.ObjectsTotalCount = dataUsageInfo.ObjectsTotalCount
		tracker.ObjectsTotalSize = dataUsageInfo.ObjectsTotalSize
	}

	tracker.PoolIndex, tracker.SetIndex, tracker.DiskIndex = disk.GetDiskLoc()
	tracker.setQueuedBuckets(buckets)
	if err := tracker.save(ctx); err != nil {
		return err
	}

	// Start or resume healing of this erasure set
	if err = z.serverPools[poolIdx].sets[setIdx].healErasureSet(ctx, tracker.QueuedBuckets, tracker); err != nil {
		return err
	}

	// if objects have failed healing, we attempt a retry to heal the drive upto 3 times before giving up.
	if tracker.ItemsFailed > 0 && tracker.RetryAttempts < 4 {
		tracker.RetryAttempts++

		healingLogEvent(ctx, "Healing of drive '%s' is incomplete, retrying %s time (healed: %d, skipped: %d, failed: %d).", disk,
			humanize.Ordinal(int(tracker.RetryAttempts)), tracker.ItemsHealed, tracker.ItemsSkipped, tracker.ItemsFailed)

		tracker.resetHealing()
		bugLogIf(ctx, tracker.update(ctx))

		return errRetryHealing
	}

	if tracker.ItemsFailed > 0 {
		healingLogEvent(ctx, "Healing of drive '%s' is incomplete, retried %d times (healed: %d, skipped: %d, failed: %d).", disk,
			tracker.RetryAttempts, tracker.ItemsHealed, tracker.ItemsSkipped, tracker.ItemsFailed)
	} else {
		if tracker.RetryAttempts > 0 {
			healingLogEvent(ctx, "Healing of drive '%s' is complete, retried %d times (healed: %d, skipped: %d).", disk,
				tracker.RetryAttempts-1, tracker.ItemsHealed, tracker.ItemsSkipped)
		} else {
			healingLogEvent(ctx, "Healing of drive '%s' is finished (healed: %d, skipped: %d).", disk, tracker.ItemsHealed, tracker.ItemsSkipped)
		}
	}
	if serverDebugLog {
		tracker.printTo(os.Stdout)
		fmt.Printf("\n")
	}

	if tracker.HealID == "" { // HealID was empty only before Feb 2023
		bugLogIf(ctx, tracker.delete(ctx))
		return nil
	}

	// Remove .healing.bin from all disks with similar heal-id
	disks, err := z.GetDisks(poolIdx, setIdx)
	if err != nil {
		return err
	}

	for _, disk := range disks {
		if disk == nil {
			continue
		}

		t, err := loadHealingTracker(ctx, disk)
		if err != nil {
			if !errors.Is(err, errFileNotFound) {
				healingLogIf(ctx, err)
			}
			continue
		}
		if t.HealID == tracker.HealID {
			t.Finished = true
			t.update(ctx)
		}
	}

	return nil
}

// monitorLocalDisksAndHeal - ensures that detected new disks are healed
//  1. Only the concerned erasure set will be listed and healed
//  2. Only the node hosting the disk is responsible to perform the heal
func monitorLocalDisksAndHeal(ctx context.Context, z *erasureServerPools) {
	// Perform automatic disk healing when a disk is replaced locally.
	diskCheckTimer := time.NewTimer(defaultMonitorNewDiskInterval)
	defer diskCheckTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-diskCheckTimer.C:
			healDisks := globalBackgroundHealState.getHealLocalDiskEndpoints()
			if len(healDisks) == 0 {
				// Reset for next interval.
				diskCheckTimer.Reset(defaultMonitorNewDiskInterval)
				continue
			}

			// Reformat disks immediately
			_, err := z.HealFormat(context.Background(), false)
			if err != nil && !errors.Is(err, errNoHealRequired) {
				healingLogIf(ctx, err)
				// Reset for next interval.
				diskCheckTimer.Reset(defaultMonitorNewDiskInterval)
				continue
			}

			for _, disk := range healDisks {
				go func(disk Endpoint) {
					globalBackgroundHealState.setDiskHealingStatus(disk, true)
					if err := healFreshDisk(ctx, z, disk); err != nil {
						globalBackgroundHealState.setDiskHealingStatus(disk, false)
						timedout := OperationTimedOut{}
						if !errors.Is(err, context.Canceled) && !errors.As(err, &timedout) && !errors.Is(err, errRetryHealing) {
							printEndpointError(disk, err, false)
						}
						return
					}
					// Only upon success pop the healed disk.
					globalBackgroundHealState.popHealLocalDisks(disk)
				}(disk)
			}

			// Reset for next interval.
			diskCheckTimer.Reset(defaultMonitorNewDiskInterval)
		}
	}
}
