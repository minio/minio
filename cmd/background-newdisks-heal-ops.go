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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/console"
)

const (
	defaultMonitorNewDiskInterval = time.Second * 10
	healingTrackerFilename        = ".healing.bin"
)

//go:generate msgp -file $GOFILE -unexported

// healingTracker is used to persist healing information during a heal.
type healingTracker struct {
	disk StorageAPI `msg:"-"`

	ID            string
	PoolIndex     int
	SetIndex      int
	DiskIndex     int
	Path          string
	Endpoint      string
	Started       time.Time
	LastUpdate    time.Time
	ObjectsHealed uint64
	ObjectsFailed uint64
	BytesDone     uint64
	BytesFailed   uint64

	// Last object scanned.
	Bucket string `json:"-"`
	Object string `json:"-"`

	// Numbers when current bucket started healing,
	// for resuming with correct numbers.
	ResumeObjectsHealed uint64 `json:"-"`
	ResumeObjectsFailed uint64 `json:"-"`
	ResumeBytesDone     uint64 `json:"-"`
	ResumeBytesFailed   uint64 `json:"-"`

	// Filled on startup/restarts.
	QueuedBuckets []string

	// Filled during heal.
	HealedBuckets []string
	// Add future tracking capabilities
	// Be sure that they are included in toHealingDisk
}

// loadHealingTracker will load the healing tracker from the supplied disk.
// The disk ID will be validated against the loaded one.
func loadHealingTracker(ctx context.Context, disk StorageAPI) (*healingTracker, error) {
	if disk == nil {
		return nil, errors.New("loadHealingTracker: nil disk given")
	}
	diskID, err := disk.GetDiskID()
	if err != nil {
		return nil, err
	}
	b, err := disk.ReadAll(ctx, minioMetaBucket,
		pathJoin(bucketMetaPrefix, slashSeparator, healingTrackerFilename))
	if err != nil {
		return nil, err
	}
	var h healingTracker
	_, err = h.UnmarshalMsg(b)
	if err != nil {
		return nil, err
	}
	if h.ID != diskID && h.ID != "" {
		return nil, fmt.Errorf("loadHealingTracker: disk id mismatch expected %s, got %s", h.ID, diskID)
	}
	h.disk = disk
	h.ID = diskID
	return &h, nil
}

// newHealingTracker will create a new healing tracker for the disk.
func newHealingTracker(disk StorageAPI) *healingTracker {
	diskID, _ := disk.GetDiskID()
	h := healingTracker{
		disk:     disk,
		ID:       diskID,
		Path:     disk.String(),
		Endpoint: disk.Endpoint().String(),
		Started:  time.Now().UTC(),
	}
	h.PoolIndex, h.SetIndex, h.DiskIndex = disk.GetDiskLoc()
	return &h
}

// update will update the tracker on the disk.
// If the tracker has been deleted an error is returned.
func (h *healingTracker) update(ctx context.Context) error {
	if h.disk.Healing() == nil {
		return fmt.Errorf("healingTracker: disk %q is not marked as healing", h.ID)
	}
	if h.ID == "" || h.PoolIndex < 0 || h.SetIndex < 0 || h.DiskIndex < 0 {
		h.ID, _ = h.disk.GetDiskID()
		h.PoolIndex, h.SetIndex, h.DiskIndex = h.disk.GetDiskLoc()
	}
	return h.save(ctx)
}

// save will unconditionally save the tracker and will be created if not existing.
func (h *healingTracker) save(ctx context.Context) error {
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
	if err != nil {
		return err
	}
	globalBackgroundHealState.updateHealStatus(h)
	return h.disk.WriteAll(ctx, minioMetaBucket,
		pathJoin(bucketMetaPrefix, slashSeparator, healingTrackerFilename),
		htrackerBytes)
}

// delete the tracker on disk.
func (h *healingTracker) delete(ctx context.Context) error {
	return h.disk.Delete(ctx, minioMetaBucket,
		pathJoin(bucketMetaPrefix, slashSeparator, healingTrackerFilename),
		false)
}

func (h *healingTracker) isHealed(bucket string) bool {
	for _, v := range h.HealedBuckets {
		if v == bucket {
			return true
		}
	}
	return false
}

// resume will reset progress to the numbers at the start of the bucket.
func (h *healingTracker) resume() {
	h.ObjectsHealed = h.ResumeObjectsHealed
	h.ObjectsFailed = h.ResumeObjectsFailed
	h.BytesDone = h.ResumeBytesDone
	h.BytesFailed = h.ResumeBytesFailed
}

// bucketDone should be called when a bucket is done healing.
// Adds the bucket to the list of healed buckets and updates resume numbers.
func (h *healingTracker) bucketDone(bucket string) {
	h.ResumeObjectsHealed = h.ObjectsHealed
	h.ResumeObjectsFailed = h.ObjectsFailed
	h.ResumeBytesDone = h.BytesDone
	h.ResumeBytesFailed = h.BytesFailed
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
	s := set.CreateStringSet(h.HealedBuckets...)
	h.QueuedBuckets = make([]string, 0, len(buckets))
	for _, b := range buckets {
		if !s.Contains(b.Name) {
			h.QueuedBuckets = append(h.QueuedBuckets, b.Name)
		}
	}
}

func (h *healingTracker) printTo(writer io.Writer) {
	b, err := json.MarshalIndent(h, "", "  ")
	if err != nil {
		writer.Write([]byte(err.Error()))
	}
	writer.Write(b)
}

// toHealingDisk converts the information to madmin.HealingDisk
func (h *healingTracker) toHealingDisk() madmin.HealingDisk {
	return madmin.HealingDisk{
		ID:            h.ID,
		Endpoint:      h.Endpoint,
		PoolIndex:     h.PoolIndex,
		SetIndex:      h.SetIndex,
		DiskIndex:     h.DiskIndex,
		Path:          h.Path,
		Started:       h.Started.UTC(),
		LastUpdate:    h.LastUpdate.UTC(),
		ObjectsHealed: h.ObjectsHealed,
		ObjectsFailed: h.ObjectsFailed,
		BytesDone:     h.BytesDone,
		BytesFailed:   h.BytesFailed,
		Bucket:        h.Bucket,
		Object:        h.Object,
		QueuedBuckets: h.QueuedBuckets,
		HealedBuckets: h.HealedBuckets,
	}
}

func initAutoHeal(ctx context.Context, objAPI ObjectLayer) {
	z, ok := objAPI.(*erasureServerPools)
	if !ok {
		return
	}

	initBackgroundHealing(ctx, objAPI) // start quick background healing

	bgSeq := mustGetHealSequence(ctx)

	globalBackgroundHealState.pushHealLocalDisks(getLocalDisksToHeal()...)

	if drivesToHeal := globalBackgroundHealState.healDriveCount(); drivesToHeal > 0 {
		logger.Info(fmt.Sprintf("Found drives to heal %d, waiting until %s to heal the content...",
			drivesToHeal, defaultMonitorNewDiskInterval))

		// Heal any disk format and metadata early, if possible.
		// Start with format healing
		if err := bgSeq.healDiskFormat(); err != nil {
			if newObjectLayerFn() != nil {
				// log only in situations, when object layer
				// has fully initialized.
				logger.LogIf(bgSeq.ctx, err)
			}
		}
	}

	if err := bgSeq.healDiskMeta(objAPI); err != nil {
		if newObjectLayerFn() != nil {
			// log only in situations, when object layer
			// has fully initialized.
			logger.LogIf(bgSeq.ctx, err)
		}
	}

	go monitorLocalDisksAndHeal(ctx, z, bgSeq)
}

func getLocalDisksToHeal() (disksToHeal Endpoints) {
	for _, ep := range globalEndpoints {
		for _, endpoint := range ep.Endpoints {
			if !endpoint.IsLocal {
				continue
			}
			// Try to connect to the current endpoint
			// and reformat if the current disk is not formatted
			disk, _, err := connectEndpoint(endpoint)
			if errors.Is(err, errUnformattedDisk) {
				disksToHeal = append(disksToHeal, endpoint)
			} else if err == nil && disk != nil && disk.Healing() != nil {
				disksToHeal = append(disksToHeal, disk.Endpoint())
			}
		}
	}
	return disksToHeal

}

func initBackgroundHealing(ctx context.Context, objAPI ObjectLayer) {
	// Run the background healer
	globalBackgroundHealRoutine = newHealRoutine()
	go globalBackgroundHealRoutine.run(ctx, objAPI)

	globalBackgroundHealState.LaunchNewHealSequence(newBgHealSequence(), objAPI)
}

// monitorLocalDisksAndHeal - ensures that detected new disks are healed
//  1. Only the concerned erasure set will be listed and healed
//  2. Only the node hosting the disk is responsible to perform the heal
func monitorLocalDisksAndHeal(ctx context.Context, z *erasureServerPools, bgSeq *healSequence) {
	// Perform automatic disk healing when a disk is replaced locally.
	diskCheckTimer := time.NewTimer(defaultMonitorNewDiskInterval)
	defer diskCheckTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-diskCheckTimer.C:
			// Reset to next interval.
			diskCheckTimer.Reset(defaultMonitorNewDiskInterval)

			var erasureSetInPoolDisksToHeal []map[int][]StorageAPI

			healDisks := globalBackgroundHealState.getHealLocalDiskEndpoints()
			if len(healDisks) > 0 {
				// Reformat disks
				bgSeq.sourceCh <- healSource{bucket: SlashSeparator}

				// Ensure that reformatting disks is finished
				bgSeq.sourceCh <- healSource{bucket: nopHeal}

				logger.Info(fmt.Sprintf("Found drives to heal %d, proceeding to heal content...",
					len(healDisks)))

				erasureSetInPoolDisksToHeal = make([]map[int][]StorageAPI, len(z.serverPools))
				for i := range z.serverPools {
					erasureSetInPoolDisksToHeal[i] = map[int][]StorageAPI{}
				}
			}

			if serverDebugLog {
				console.Debugf(color.Green("healDisk:")+" disk check timer fired, attempting to heal %d drives\n", len(healDisks))
			}

			// heal only if new disks found.
			for _, endpoint := range healDisks {
				disk, format, err := connectEndpoint(endpoint)
				if err != nil {
					printEndpointError(endpoint, err, true)
					continue
				}

				poolIdx := globalEndpoints.GetLocalPoolIdx(disk.Endpoint())
				if poolIdx < 0 {
					continue
				}

				// Calculate the set index where the current endpoint belongs
				z.serverPools[poolIdx].erasureDisksMu.RLock()
				// Protect reading reference format.
				setIndex, _, err := findDiskIndex(z.serverPools[poolIdx].format, format)
				z.serverPools[poolIdx].erasureDisksMu.RUnlock()
				if err != nil {
					printEndpointError(endpoint, err, false)
					continue
				}

				erasureSetInPoolDisksToHeal[poolIdx][setIndex] = append(erasureSetInPoolDisksToHeal[poolIdx][setIndex], disk)
			}

			buckets, _ := z.ListBuckets(ctx)

			buckets = append(buckets, BucketInfo{
				Name: pathJoin(minioMetaBucket, minioConfigPrefix),
			})

			// Buckets data are dispersed in multiple zones/sets, make
			// sure to heal all bucket metadata configuration.
			buckets = append(buckets, []BucketInfo{
				{Name: pathJoin(minioMetaBucket, bucketMetaPrefix)},
			}...)

			// Heal latest buckets first.
			sort.Slice(buckets, func(i, j int) bool {
				a, b := strings.HasPrefix(buckets[i].Name, minioMetaBucket), strings.HasPrefix(buckets[j].Name, minioMetaBucket)
				if a != b {
					return a
				}
				return buckets[i].Created.After(buckets[j].Created)
			})

			// TODO(klauspost): This will block until all heals are done,
			// in the future this should be able to start healing other sets at once.
			var wg sync.WaitGroup
			for i, setMap := range erasureSetInPoolDisksToHeal {
				i := i
				for setIndex, disks := range setMap {
					if len(disks) == 0 {
						continue
					}
					wg.Add(1)
					go func(setIndex int, disks []StorageAPI) {
						defer wg.Done()
						for _, disk := range disks {
							logger.Info("Healing disk '%v' on %s pool", disk, humanize.Ordinal(i+1))

							// So someone changed the drives underneath, healing tracker missing.
							tracker, err := loadHealingTracker(ctx, disk)
							if err != nil {
								logger.Info("Healing tracker missing on '%s', disk was swapped again on %s pool", disk, humanize.Ordinal(i+1))
								tracker = newHealingTracker(disk)
							}

							tracker.PoolIndex, tracker.SetIndex, tracker.DiskIndex = disk.GetDiskLoc()
							tracker.setQueuedBuckets(buckets)
							if err := tracker.save(ctx); err != nil {
								logger.LogIf(ctx, err)
								// Unable to write healing tracker, permission denied or some
								// other unexpected error occurred. Proceed to look for new
								// disks to be healed again, we cannot proceed further.
								return
							}

							err = z.serverPools[i].sets[setIndex].healErasureSet(ctx, buckets, tracker)
							if err != nil {
								logger.LogIf(ctx, err)
								continue
							}

							logger.Info("Healing disk '%s' on %s pool complete", disk, humanize.Ordinal(i+1))
							var buf bytes.Buffer
							tracker.printTo(&buf)
							logger.Info("Summary:\n%s", buf.String())
							logger.LogIf(ctx, tracker.delete(ctx))

							// Only upon success pop the healed disk.
							globalBackgroundHealState.popHealLocalDisks(disk.Endpoint())
						}
					}(setIndex, disks)
				}
			}
			wg.Wait()
		}
	}
}
