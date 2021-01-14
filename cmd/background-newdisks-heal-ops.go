/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/console"
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
	Path          string
	Started       time.Time
	LastUpdate    time.Time
	ObjectsHealed uint64
	ObjectsFailed uint64
	BytesDone     uint64
	BytesFailed   uint64

	// Last object scanned.
	Bucket string
	Object string

	HealedBuckets []string
	// future add more tracking capabilities
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
	if h.ID != diskID {
		return nil, errors.New("loadHealingTracker: disk id mismatch")
	}
	h.disk = disk
	return &h, nil
}

// newHealingTracker will create a new healing tracker for the disk.
func newHealingTracker(disk StorageAPI) (*healingTracker, error) {
	if disk == nil {
		return nil, errors.New("newHealingTracker: nil disk given")
	}

	diskID, err := disk.GetDiskID()
	if err != nil {
		return nil, err
	}

	return &healingTracker{
		disk:    disk,
		ID:      diskID,
		Path:    disk.String(),
		Started: time.Now().UTC(),
	}, err
}

// update will update the tracker on the disk.
// If the tracker has been deleted an error is returned.
func (h *healingTracker) update(ctx context.Context) error {
	if !h.disk.Healing() {
		return fmt.Errorf("healingTracker: disk %q is not marked as healing", h.ID)
	}
	return h.save(ctx)
}

// save will unconditionally save the tracker and will be created if not existing.
func (h *healingTracker) save(ctx context.Context) error {
	h.LastUpdate = time.Now().UTC()
	htrackerBytes, err := h.MarshalMsg(nil)
	if err != nil {
		return err
	}
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

func (h *healingTracker) printTo(writer io.Writer) {
	b, err := json.MarshalIndent(h, "", "  ")
	if err != nil {
		writer.Write([]byte(err.Error()))
	}
	writer.Write(b)
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
			} else if err == nil && disk != nil && disk.Healing() {
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
wait:
	for {
		select {
		case <-ctx.Done():
			return
		case <-diskCheckTimer.C:
			// Reset to next interval.
			diskCheckTimer.Reset(defaultMonitorNewDiskInterval)

			var erasureSetInZoneDisksToHeal []map[int][]StorageAPI

			healDisks := globalBackgroundHealState.getHealLocalDisks()
			if len(healDisks) > 0 {
				// Reformat disks
				bgSeq.sourceCh <- healSource{bucket: SlashSeparator}

				// Ensure that reformatting disks is finished
				bgSeq.sourceCh <- healSource{bucket: nopHeal}

				logger.Info(fmt.Sprintf("Found drives to heal %d, proceeding to heal content...",
					len(healDisks)))

				erasureSetInZoneDisksToHeal = make([]map[int][]StorageAPI, len(z.serverPools))
				for i := range z.serverPools {
					erasureSetInZoneDisksToHeal[i] = map[int][]StorageAPI{}
				}
			}

			if serverDebugLog {
				console.Debugf("disk check timer fired, attempting to heal %d drives\n", len(healDisks))
			}

			// heal only if new disks found.
			for _, endpoint := range healDisks {
				disk, format, err := connectEndpoint(endpoint)
				if err != nil {
					printEndpointError(endpoint, err, true)
					continue
				}

				poolIdx := globalEndpoints.GetLocalZoneIdx(disk.Endpoint())
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

				erasureSetInZoneDisksToHeal[poolIdx][setIndex] = append(erasureSetInZoneDisksToHeal[poolIdx][setIndex], disk)
			}

			buckets, _ := z.ListBuckets(ctx)

			// Heal latest buckets first.
			sort.Slice(buckets, func(i, j int) bool {
				return buckets[i].Created.After(buckets[j].Created)
			})

			for i, setMap := range erasureSetInZoneDisksToHeal {
				for setIndex, disks := range setMap {
					for _, disk := range disks {
						logger.Info("Healing disk '%v' on %s pool", disk, humanize.Ordinal(i+1))

						// So someone changed the drives underneath, healing tracker missing.
						tracker, err := loadHealingTracker(ctx, disk)
						if err != nil {
							logger.Info("Healing tracker missing on '%s', disk was swapped again on %s pool", disk, humanize.Ordinal(i+1))
							tracker, err = newHealingTracker(disk)
							if err != nil {
								logger.LogIf(ctx, err)
								// reading format.json failed or not found, proceed to look
								// for new disks to be healed again, we cannot proceed further.
								goto wait
							}

							if err := tracker.save(ctx); err != nil {
								logger.LogIf(ctx, err)
								// Unable to write healing tracker, permission denied or some
								// other unexpected error occurred. Proceed to look for new
								// disks to be healed again, we cannot proceed further.
								goto wait
							}
						}

						lbDisks := z.serverPools[i].sets[setIndex].getOnlineDisks()
						if err := healErasureSet(ctx, buckets, lbDisks, tracker); err != nil {
							logger.LogIf(ctx, err)
							continue
						}

						logger.Info("Healing disk '%s' on %s pool complete", disk, humanize.Ordinal(i+1))

						logger.LogIf(ctx, tracker.delete(ctx))

						// Only upon success pop the healed disk.
						globalBackgroundHealState.popHealLocalDisks(disk.Endpoint())
					}
				}
			}
		}
	}
}
