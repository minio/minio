// Copyright (c) 2015-2022 MinIO, Inc.
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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/lithammer/shortuuid/v4"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/bucket/lifecycle"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/minio/internal/hash"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/env"
	"github.com/minio/pkg/v3/workers"
)

//go:generate msgp -file $GOFILE -unexported

// rebalanceStats contains per-pool rebalance statistics like number of objects,
// versions and bytes rebalanced out of a pool
type rebalanceStats struct {
	InitFreeSpace uint64 `json:"initFreeSpace" msg:"ifs"` // Pool free space at the start of rebalance
	InitCapacity  uint64 `json:"initCapacity" msg:"ic"`   // Pool capacity at the start of rebalance

	Buckets           []string      `json:"buckets" msg:"bus"`           // buckets being rebalanced or to be rebalanced
	RebalancedBuckets []string      `json:"rebalancedBuckets" msg:"rbs"` // buckets rebalanced
	Bucket            string        `json:"bucket" msg:"bu"`             // Last rebalanced bucket
	Object            string        `json:"object" msg:"ob"`             // Last rebalanced object
	NumObjects        uint64        `json:"numObjects" msg:"no"`         // Number of objects rebalanced
	NumVersions       uint64        `json:"numVersions" msg:"nv"`        // Number of versions rebalanced
	Bytes             uint64        `json:"bytes" msg:"bs"`              // Number of bytes rebalanced
	Participating     bool          `json:"participating" msg:"par"`
	Info              rebalanceInfo `json:"info" msg:"inf"`
}

func (rs *rebalanceStats) update(bucket string, fi FileInfo) {
	if fi.IsLatest {
		rs.NumObjects++
	}

	rs.NumVersions++
	onDiskSz := int64(0)
	if !fi.Deleted {
		onDiskSz = fi.Size * int64(fi.Erasure.DataBlocks+fi.Erasure.ParityBlocks) / int64(fi.Erasure.DataBlocks)
	}
	rs.Bytes += uint64(onDiskSz)
	rs.Bucket = bucket
	rs.Object = fi.Name
}

type rstats []*rebalanceStats

//go:generate stringer -type=rebalStatus -trimprefix=rebal $GOFILE
type rebalStatus uint8

const (
	rebalNone rebalStatus = iota
	rebalStarted
	rebalCompleted
	rebalStopped
	rebalFailed
)

type rebalanceInfo struct {
	StartTime time.Time   `msg:"startTs"` // Time at which rebalance-start was issued
	EndTime   time.Time   `msg:"stopTs"`  // Time at which rebalance operation completed or rebalance-stop was called
	Status    rebalStatus `msg:"status"`  // Current state of rebalance operation. One of Started|Stopped|Completed|Failed.
}

// rebalanceMeta contains information pertaining to an ongoing rebalance operation.
type rebalanceMeta struct {
	StoppedAt       time.Time         `msg:"stopTs"` // Time when rebalance-stop was issued.
	ID              string            `msg:"id"`     // ID of the ongoing rebalance operation
	PercentFreeGoal float64           `msg:"pf"`     // Computed from total free space and capacity at the start of rebalance
	PoolStats       []*rebalanceStats `msg:"rss"`    // Per-pool rebalance stats keyed by pool index
}

var errRebalanceNotStarted = errors.New("rebalance not started")

func (z *erasureServerPools) loadRebalanceMeta(ctx context.Context) error {
	r := &rebalanceMeta{}
	if err := r.load(ctx, z.serverPools[0]); err == nil {
		z.rebalMu.Lock()
		z.rebalMeta = r
		z.updateRebalanceStats(ctx)
		z.rebalMu.Unlock()
	} else if !errors.Is(err, errConfigNotFound) {
		rebalanceLogIf(ctx, fmt.Errorf("failed to load rebalance metadata, continue to restart rebalance as needed: %w", err))
	}
	return nil
}

// updates rebalance.bin from let's say 2 pool setup in the middle
// of a rebalance, was expanded can cause z.rebalMeta to be outdated
// due to a missing new pool. This function tries to handle this
// scenario, albeit rare it seems to have occurred in the wild.
//
// since we do not explicitly disallow it, but it is okay for them
// expand and then we continue to rebalance.
func (z *erasureServerPools) updateRebalanceStats(ctx context.Context) error {
	var ok bool
	for i := range z.serverPools {
		if z.findIndex(i) == -1 {
			// Also ensure to initialize rebalanceStats to indicate
			// its a new pool that can receive rebalanced data.
			z.rebalMeta.PoolStats = append(z.rebalMeta.PoolStats, &rebalanceStats{})
			ok = true
		}
	}
	if ok {
		return z.rebalMeta.save(ctx, z.serverPools[0])
	}

	return nil
}

func (z *erasureServerPools) findIndex(index int) int {
	if z.rebalMeta == nil {
		return 0
	}
	for i := range len(z.rebalMeta.PoolStats) {
		if i == index {
			return index
		}
	}
	return -1
}

// initRebalanceMeta initializes rebalance metadata for a new rebalance
// operation and saves it in the object store.
func (z *erasureServerPools) initRebalanceMeta(ctx context.Context, buckets []string) (arn string, err error) {
	r := &rebalanceMeta{
		ID:        shortuuid.New(),
		PoolStats: make([]*rebalanceStats, len(z.serverPools)),
	}

	// Fetch disk capacity and available space.
	si := z.StorageInfo(ctx, true)
	diskStats := make([]struct {
		AvailableSpace uint64
		TotalSpace     uint64
	}, len(z.serverPools))
	var totalCap, totalFree uint64
	for _, disk := range si.Disks {
		// Ignore invalid.
		if disk.PoolIndex < 0 || len(diskStats) <= disk.PoolIndex {
			// https://github.com/minio/minio/issues/16500
			continue
		}
		totalCap += disk.TotalSpace
		totalFree += disk.AvailableSpace

		diskStats[disk.PoolIndex].AvailableSpace += disk.AvailableSpace
		diskStats[disk.PoolIndex].TotalSpace += disk.TotalSpace
	}
	r.PercentFreeGoal = float64(totalFree) / float64(totalCap)

	now := time.Now()
	for idx := range z.serverPools {
		r.PoolStats[idx] = &rebalanceStats{
			Buckets:           make([]string, len(buckets)),
			RebalancedBuckets: make([]string, 0, len(buckets)),
			InitFreeSpace:     diskStats[idx].AvailableSpace,
			InitCapacity:      diskStats[idx].TotalSpace,
		}
		copy(r.PoolStats[idx].Buckets, buckets)

		if pfi := float64(diskStats[idx].AvailableSpace) / float64(diskStats[idx].TotalSpace); pfi < r.PercentFreeGoal {
			r.PoolStats[idx].Participating = true
			r.PoolStats[idx].Info = rebalanceInfo{
				StartTime: now,
				Status:    rebalStarted,
			}
		}
	}

	err = r.save(ctx, z.serverPools[0])
	if err != nil {
		return arn, err
	}

	z.rebalMeta = r
	return r.ID, nil
}

func (z *erasureServerPools) updatePoolStats(poolIdx int, bucket string, fi FileInfo) {
	z.rebalMu.Lock()
	defer z.rebalMu.Unlock()

	r := z.rebalMeta
	if r == nil {
		return
	}

	r.PoolStats[poolIdx].update(bucket, fi)
}

const (
	rebalMetaName = "rebalance.bin"
	rebalMetaFmt  = 1
	rebalMetaVer  = 1
)

func (z *erasureServerPools) nextRebalBucket(poolIdx int) (string, bool) {
	z.rebalMu.RLock()
	defer z.rebalMu.RUnlock()

	r := z.rebalMeta
	if r == nil {
		return "", false
	}

	ps := r.PoolStats[poolIdx]
	if ps == nil {
		return "", false
	}

	if ps.Info.Status == rebalCompleted || !ps.Participating {
		return "", false
	}

	if len(ps.Buckets) == 0 {
		return "", false
	}

	return ps.Buckets[0], true
}

func (z *erasureServerPools) bucketRebalanceDone(bucket string, poolIdx int) {
	z.rebalMu.Lock()
	defer z.rebalMu.Unlock()

	if z.rebalMeta == nil {
		return
	}

	ps := z.rebalMeta.PoolStats[poolIdx]
	if ps == nil {
		return
	}

	for i, b := range ps.Buckets {
		if b == bucket {
			ps.Buckets = append(ps.Buckets[:i], ps.Buckets[i+1:]...)
			ps.RebalancedBuckets = append(ps.RebalancedBuckets, bucket)
			break
		}
	}
}

func (r *rebalanceMeta) load(ctx context.Context, store objectIO) error {
	return r.loadWithOpts(ctx, store, ObjectOptions{})
}

func (r *rebalanceMeta) loadWithOpts(ctx context.Context, store objectIO, opts ObjectOptions) error {
	data, _, err := readConfigWithMetadata(ctx, store, rebalMetaName, opts)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}
	if len(data) <= 4 {
		return fmt.Errorf("rebalanceMeta: no data")
	}

	// Read header
	switch binary.LittleEndian.Uint16(data[0:2]) {
	case rebalMetaFmt:
	default:
		return fmt.Errorf("rebalanceMeta: unknown format: %d", binary.LittleEndian.Uint16(data[0:2]))
	}
	switch binary.LittleEndian.Uint16(data[2:4]) {
	case rebalMetaVer:
	default:
		return fmt.Errorf("rebalanceMeta: unknown version: %d", binary.LittleEndian.Uint16(data[2:4]))
	}

	// OK, parse data.
	if _, err = r.UnmarshalMsg(data[4:]); err != nil {
		return err
	}

	return nil
}

func (r *rebalanceMeta) saveWithOpts(ctx context.Context, store objectIO, opts ObjectOptions) error {
	if r == nil {
		return nil
	}

	data := make([]byte, 4, r.Msgsize()+4)

	// Initialize the header.
	binary.LittleEndian.PutUint16(data[0:2], rebalMetaFmt)
	binary.LittleEndian.PutUint16(data[2:4], rebalMetaVer)

	buf, err := r.MarshalMsg(data)
	if err != nil {
		return err
	}

	return saveConfigWithOpts(ctx, store, rebalMetaName, buf, opts)
}

func (r *rebalanceMeta) save(ctx context.Context, store objectIO) error {
	return r.saveWithOpts(ctx, store, ObjectOptions{})
}

func (z *erasureServerPools) IsRebalanceStarted(ctx context.Context) bool {
	_ = z.loadRebalanceMeta(ctx)
	z.rebalMu.RLock()
	defer z.rebalMu.RUnlock()

	r := z.rebalMeta
	if r == nil {
		return false
	}
	if !r.StoppedAt.IsZero() {
		return false
	}
	for _, ps := range r.PoolStats {
		if ps.Participating && ps.Info.Status != rebalCompleted {
			return true
		}
	}
	return false
}

func (z *erasureServerPools) IsPoolRebalancing(poolIndex int) bool {
	z.rebalMu.RLock()
	defer z.rebalMu.RUnlock()

	if r := z.rebalMeta; r != nil {
		if !r.StoppedAt.IsZero() {
			return false
		}
		ps := r.PoolStats[poolIndex]
		return ps.Participating && ps.Info.Status == rebalStarted
	}
	return false
}

func (z *erasureServerPools) rebalanceBuckets(ctx context.Context, poolIdx int) (err error) {
	doneCh := make(chan error, 1)
	defer xioutil.SafeClose(doneCh)

	// Save rebalance.bin periodically.
	go func() {
		// Update rebalance.bin periodically once every 5-10s, chosen randomly
		// to avoid multiple pool leaders herding to update around the same
		// time.
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		randSleepFor := func() time.Duration {
			return 5*time.Second + time.Duration(float64(5*time.Second)*r.Float64())
		}

		timer := time.NewTimer(randSleepFor())
		defer timer.Stop()

		var (
			quit     bool
			traceMsg string
			notify   bool // if status changed, notify nodes to reload rebalance metadata
		)

		for {
			select {
			case rebalErr := <-doneCh:
				quit = true
				notify = true
				now := time.Now()
				var status rebalStatus

				switch {
				case errors.Is(rebalErr, context.Canceled):
					status = rebalStopped
					traceMsg = fmt.Sprintf("stopped at %s", now)
				case rebalErr == nil:
					status = rebalCompleted
					traceMsg = fmt.Sprintf("completed at %s", now)
				default:
					status = rebalFailed
					traceMsg = fmt.Sprintf("stopped at %s with err: %v", now, rebalErr)
				}

				z.rebalMu.Lock()
				z.rebalMeta.PoolStats[poolIdx].Info.Status = status
				z.rebalMeta.PoolStats[poolIdx].Info.EndTime = now
				z.rebalMu.Unlock()

			case <-timer.C:
				notify = false
				traceMsg = fmt.Sprintf("saved at %s", time.Now())
			}

			stopFn := globalRebalanceMetrics.log(rebalanceMetricSaveMetadata, poolIdx, traceMsg)
			err := z.saveRebalanceStats(GlobalContext, poolIdx, rebalSaveStats)
			stopFn(0, err)
			if err == nil && notify {
				globalNotificationSys.LoadRebalanceMeta(GlobalContext, false)
			}
			rebalanceLogIf(GlobalContext, err)

			if quit {
				return
			}

			timer.Reset(randSleepFor())
		}
	}()

	rebalanceLogEvent(ctx, "Pool %d rebalancing is started", poolIdx+1)

	for {
		select {
		case <-ctx.Done():
			doneCh <- ctx.Err()
			return err
		default:
		}

		bucket, ok := z.nextRebalBucket(poolIdx)
		if !ok {
			// no more buckets to rebalance or target free_space/capacity reached
			break
		}

		stopFn := globalRebalanceMetrics.log(rebalanceMetricRebalanceBucket, poolIdx, bucket)
		if err = z.rebalanceBucket(ctx, bucket, poolIdx); err != nil {
			stopFn(0, err)
			if errors.Is(err, errServerNotInitialized) || errors.Is(err, errBucketMetadataNotInitialized) {
				continue
			}
			rebalanceLogIf(GlobalContext, err)
			doneCh <- err
			return err
		}
		stopFn(0, nil)
		z.bucketRebalanceDone(bucket, poolIdx)
	}

	rebalanceLogEvent(GlobalContext, "Pool %d rebalancing is done", poolIdx+1)

	return err
}

func (z *erasureServerPools) checkIfRebalanceDone(poolIdx int) bool {
	z.rebalMu.Lock()
	defer z.rebalMu.Unlock()

	// check if enough objects have been rebalanced
	r := z.rebalMeta
	poolStats := r.PoolStats[poolIdx]
	if poolStats.Info.Status == rebalCompleted {
		return true
	}

	pfi := float64(poolStats.InitFreeSpace+poolStats.Bytes) / float64(poolStats.InitCapacity)
	// Mark pool rebalance as done if within 5% from PercentFreeGoal.
	if diff := math.Abs(pfi - r.PercentFreeGoal); diff <= 0.05 {
		r.PoolStats[poolIdx].Info.Status = rebalCompleted
		r.PoolStats[poolIdx].Info.EndTime = time.Now()
		return true
	}

	return false
}

func (set *erasureObjects) listObjectsToRebalance(ctx context.Context, bucketName string, fn func(entry metaCacheEntry)) error {
	disks, _ := set.getOnlineDisksWithHealing(false)
	if len(disks) == 0 {
		return fmt.Errorf("no online drives found for set with endpoints %s", set.getEndpoints())
	}

	// However many we ask, versions must exist on ~50%
	listingQuorum := (set.setDriveCount + 1) / 2

	// How to resolve partial results.
	resolver := metadataResolutionParams{
		dirQuorum: listingQuorum, // make sure to capture all quorum ratios
		objQuorum: listingQuorum, // make sure to capture all quorum ratios
		bucket:    bucketName,
	}

	err := listPathRaw(ctx, listPathRawOptions{
		disks:          disks,
		bucket:         bucketName,
		recursive:      true,
		forwardTo:      "",
		minDisks:       listingQuorum,
		reportNotFound: false,
		agreed:         fn,
		partial: func(entries metaCacheEntries, _ []error) {
			entry, ok := entries.resolve(&resolver)
			if ok {
				fn(*entry)
			}
		},
		finished: nil,
	})
	return err
}

// rebalanceBucket rebalances objects under bucket in poolIdx pool
func (z *erasureServerPools) rebalanceBucket(ctx context.Context, bucket string, poolIdx int) (err error) {
	ctx = logger.SetReqInfo(ctx, &logger.ReqInfo{})

	var vc *versioning.Versioning
	var lc *lifecycle.Lifecycle
	var lr objectlock.Retention
	var rcfg *replication.Config
	if bucket != minioMetaBucket {
		vc, err = globalBucketVersioningSys.Get(bucket)
		if err != nil {
			return err
		}

		// Check if the current bucket has a configured lifecycle policy
		lc, err = globalLifecycleSys.Get(bucket)
		if err != nil && !errors.Is(err, BucketLifecycleNotFound{Bucket: bucket}) {
			return err
		}

		// Check if bucket is object locked.
		lr, err = globalBucketObjectLockSys.Get(bucket)
		if err != nil {
			return err
		}

		rcfg, err = getReplicationConfig(ctx, bucket)
		if err != nil {
			return err
		}
	}

	pool := z.serverPools[poolIdx]

	const envRebalanceWorkers = "_MINIO_REBALANCE_WORKERS"
	workerSize, err := env.GetInt(envRebalanceWorkers, len(pool.sets))
	if err != nil {
		rebalanceLogIf(ctx, fmt.Errorf("invalid workers value err: %v, defaulting to %d", err, len(pool.sets)))
		workerSize = len(pool.sets)
	}

	// Each decom worker needs one List() goroutine/worker
	// add that many extra workers.
	workerSize += len(pool.sets)

	wk, err := workers.New(workerSize)
	if err != nil {
		return err
	}

	for setIdx, set := range pool.sets {
		filterLifecycle := func(bucket, object string, fi FileInfo) bool {
			if lc == nil {
				return false
			}
			versioned := vc != nil && vc.Versioned(object)
			objInfo := fi.ToObjectInfo(bucket, object, versioned)

			evt := evalActionFromLifecycle(ctx, *lc, lr, rcfg, objInfo)
			if evt.Action.Delete() {
				globalExpiryState.enqueueByDays(objInfo, evt, lcEventSrc_Rebal)
				return true
			}
			return false
		}

		rebalanceEntry := func(entry metaCacheEntry) {
			defer wk.Give()

			if entry.isDir() {
				return
			}

			// rebalance on poolIdx has reached its goal
			if z.checkIfRebalanceDone(poolIdx) {
				return
			}

			fivs, err := entry.fileInfoVersions(bucket)
			if err != nil {
				return
			}

			// We need a reversed order for rebalance,
			// to create the appropriate stack.
			versionsSorter(fivs.Versions).reverse()

			var rebalanced, expired int
			for _, version := range fivs.Versions {
				stopFn := globalRebalanceMetrics.log(rebalanceMetricRebalanceObject, poolIdx, bucket, version.Name, version.VersionID)

				// Skip transitioned objects for now. TBD
				if version.IsRemote() {
					stopFn(version.Size, errors.New("ILM Tiered version will be skipped for now"))
					continue
				}

				// Apply lifecycle rules on the objects that are expired.
				if filterLifecycle(bucket, version.Name, version) {
					expired++
					stopFn(version.Size, errors.New("ILM expired object/version will be skipped"))
					continue
				}

				// any object with only single DEL marker we don't need
				// to rebalance, just skip it, this also includes
				// any other versions that have already expired.
				remainingVersions := len(fivs.Versions) - expired
				if version.Deleted && remainingVersions == 1 {
					rebalanced++
					stopFn(version.Size, errors.New("DELETE marked object with no other non-current versions will be skipped"))
					continue
				}

				versionID := version.VersionID
				if versionID == "" {
					versionID = nullVersionID
				}

				var failure, ignore bool
				if version.Deleted {
					_, err := z.DeleteObject(ctx,
						bucket,
						version.Name,
						ObjectOptions{
							Versioned:         true,
							VersionID:         versionID,
							MTime:             version.ModTime,
							DeleteReplication: version.ReplicationState,
							SrcPoolIdx:        poolIdx,
							DataMovement:      true,
							DeleteMarker:      true, // make sure we create a delete marker
							SkipRebalancing:   true, // make sure we skip the decommissioned pool
							NoAuditLog:        true,
						})
					// This can happen when rebalance stop races with ongoing rebalance workers.
					// These rebalance failures can be ignored.
					if err != nil {
						// This can happen when rebalance stop races with ongoing rebalance workers.
						// These rebalance failures can be ignored.
						if isErrObjectNotFound(err) || isErrVersionNotFound(err) || isDataMovementOverWriteErr(err) {
							ignore = true
							stopFn(0, nil)
							continue
						}
					}
					stopFn(version.Size, err)
					rebalanceLogIf(ctx, err)
					failure = err != nil
					if !failure {
						z.updatePoolStats(poolIdx, bucket, version)
						rebalanced++
					}
					auditLogRebalance(ctx, "Rebalance:DeleteMarker", bucket, version.Name, versionID, err)
					continue
				}

				for range 3 {
					// GetObjectReader.Close is called by rebalanceObject
					gr, err := set.GetObjectNInfo(ctx,
						bucket,
						encodeDirObject(version.Name),
						nil,
						http.Header{},
						ObjectOptions{
							VersionID:    versionID,
							NoDecryption: true,
							NoLock:       true,
							NoAuditLog:   true,
						})
					if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
						// object deleted by the application, nothing to do here we move on.
						ignore = true
						stopFn(0, nil)
						break
					}
					if err != nil {
						failure = true
						rebalanceLogIf(ctx, err)
						stopFn(0, err)
						continue
					}

					if err = z.rebalanceObject(ctx, poolIdx, bucket, gr); err != nil {
						// This can happen when rebalance stop races with ongoing rebalance workers.
						// These rebalance failures can be ignored.
						if isErrObjectNotFound(err) || isErrVersionNotFound(err) || isDataMovementOverWriteErr(err) {
							ignore = true
							stopFn(0, nil)
							break
						}
						failure = true
						rebalanceLogIf(ctx, err)
						stopFn(version.Size, err)
						continue
					}

					stopFn(version.Size, nil)
					failure = false
					break
				}
				if ignore {
					continue
				}
				if failure {
					break // break out on first error
				}
				z.updatePoolStats(poolIdx, bucket, version)
				rebalanced++
			}

			// if all versions were rebalanced, we can delete the object versions.
			if rebalanced == len(fivs.Versions) {
				stopFn := globalRebalanceMetrics.log(rebalanceMetricRebalanceRemoveObject, poolIdx, bucket, entry.name)
				_, err := set.DeleteObject(ctx,
					bucket,
					encodeDirObject(entry.name),
					ObjectOptions{
						DeletePrefix:       true, // use prefix delete to delete all versions at once.
						DeletePrefixObject: true, // use prefix delete on exact object (this is an optimization to avoid fan-out calls)
						NoAuditLog:         true,
					},
				)
				stopFn(0, err)
				auditLogRebalance(ctx, "Rebalance:DeleteObject", bucket, entry.name, "", err)
				if err != nil {
					rebalanceLogIf(ctx, err)
				}
			}
		}

		wk.Take()
		go func(setIdx int) {
			defer wk.Give()
			err := set.listObjectsToRebalance(ctx, bucket,
				func(entry metaCacheEntry) {
					wk.Take()
					go rebalanceEntry(entry)
				},
			)
			if err == nil || errors.Is(err, context.Canceled) {
				return
			}
			setN := humanize.Ordinal(setIdx + 1)
			rebalanceLogIf(ctx, fmt.Errorf("listing objects from %s set failed with %v", setN, err), "rebalance-listing-failed"+setN)
		}(setIdx)
	}

	wk.Wait()
	return nil
}

type rebalSaveOpts uint8

const (
	rebalSaveStats rebalSaveOpts = iota
	rebalSaveStoppedAt
)

func (z *erasureServerPools) saveRebalanceStats(ctx context.Context, poolIdx int, opts rebalSaveOpts) error {
	lock := z.serverPools[0].NewNSLock(minioMetaBucket, rebalMetaName)
	lkCtx, err := lock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		rebalanceLogIf(ctx, fmt.Errorf("failed to acquire write lock on %s/%s: %w", minioMetaBucket, rebalMetaName, err))
		return err
	}
	defer lock.Unlock(lkCtx)

	ctx = lkCtx.Context()
	noLockOpts := ObjectOptions{NoLock: true}
	r := &rebalanceMeta{}
	err = r.loadWithOpts(ctx, z.serverPools[0], noLockOpts)
	if err != nil && !errors.Is(err, errConfigNotFound) {
		return err
	}

	z.rebalMu.Lock()
	defer z.rebalMu.Unlock()

	// if not found, we store the memory metadata back
	// when rebalance status changed, will notify all nodes update status to memory, we can treat the memory metadata is the latest status
	if errors.Is(err, errConfigNotFound) {
		r = z.rebalMeta
	}

	switch opts {
	case rebalSaveStoppedAt:
		r.StoppedAt = time.Now()
	case rebalSaveStats:
		if z.rebalMeta != nil {
			r.PoolStats[poolIdx] = z.rebalMeta.PoolStats[poolIdx]
		}
	}
	z.rebalMeta = r

	return z.rebalMeta.saveWithOpts(ctx, z.serverPools[0], noLockOpts)
}

func auditLogRebalance(ctx context.Context, apiName, bucket, object, versionID string, err error) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	auditLogInternal(ctx, AuditLogOptions{
		Event:     "rebalance",
		APIName:   apiName,
		Bucket:    bucket,
		Object:    object,
		VersionID: versionID,
		Error:     errStr,
	})
}

func (z *erasureServerPools) rebalanceObject(ctx context.Context, poolIdx int, bucket string, gr *GetObjectReader) (err error) {
	oi := gr.ObjInfo

	defer func() {
		gr.Close()
		auditLogRebalance(ctx, "RebalanceCopyData", oi.Bucket, oi.Name, oi.VersionID, err)
	}()

	actualSize, err := oi.GetActualSize()
	if err != nil {
		return err
	}

	if oi.isMultipart() {
		res, err := z.NewMultipartUpload(ctx, bucket, oi.Name, ObjectOptions{
			VersionID:    oi.VersionID,
			UserDefined:  oi.UserDefined,
			NoAuditLog:   true,
			DataMovement: true,
			SrcPoolIdx:   poolIdx,
		})
		if err != nil {
			return fmt.Errorf("rebalanceObject: NewMultipartUpload() %w", err)
		}
		defer z.AbortMultipartUpload(ctx, bucket, oi.Name, res.UploadID, ObjectOptions{NoAuditLog: true})

		parts := make([]CompletePart, len(oi.Parts))
		for i, part := range oi.Parts {
			hr, err := hash.NewReader(ctx, io.LimitReader(gr, part.Size), part.Size, "", "", part.ActualSize)
			if err != nil {
				return fmt.Errorf("rebalanceObject: hash.NewReader() %w", err)
			}
			pi, err := z.PutObjectPart(ctx, bucket, oi.Name, res.UploadID,
				part.Number,
				NewPutObjReader(hr),
				ObjectOptions{
					PreserveETag: part.ETag, // Preserve original ETag to ensure same metadata.
					IndexCB: func() []byte {
						return part.Index // Preserve part Index to ensure decompression works.
					},
					NoAuditLog: true,
				})
			if err != nil {
				return fmt.Errorf("rebalanceObject: PutObjectPart() %w", err)
			}
			parts[i] = CompletePart{
				ETag:       pi.ETag,
				PartNumber: pi.PartNumber,
			}
		}
		_, err = z.CompleteMultipartUpload(ctx, bucket, oi.Name, res.UploadID, parts, ObjectOptions{
			DataMovement: true,
			MTime:        oi.ModTime,
			NoAuditLog:   true,
		})
		if err != nil {
			err = fmt.Errorf("rebalanceObject: CompleteMultipartUpload() %w", err)
		}
		return err
	}

	hr, err := hash.NewReader(ctx, gr, oi.Size, "", "", actualSize)
	if err != nil {
		return fmt.Errorf("rebalanceObject: hash.NewReader() %w", err)
	}

	_, err = z.PutObject(ctx,
		bucket,
		oi.Name,
		NewPutObjReader(hr),
		ObjectOptions{
			SrcPoolIdx:   poolIdx,
			DataMovement: true,
			VersionID:    oi.VersionID,
			MTime:        oi.ModTime,
			UserDefined:  oi.UserDefined,
			PreserveETag: oi.ETag, // Preserve original ETag to ensure same metadata.
			IndexCB: func() []byte {
				return oi.Parts[0].Index // Preserve part Index to ensure decompression works.
			},
			NoAuditLog: true,
		})
	if err != nil {
		err = fmt.Errorf("rebalanceObject: PutObject() %w", err)
	}
	return err
}

func (z *erasureServerPools) StartRebalance() {
	z.rebalMu.Lock()
	if z.rebalMeta == nil || !z.rebalMeta.StoppedAt.IsZero() { // rebalance not running, nothing to do
		z.rebalMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(GlobalContext)
	z.rebalCancel = cancel // to be used when rebalance-stop is called
	z.rebalMu.Unlock()

	z.rebalMu.RLock()
	participants := make([]bool, len(z.rebalMeta.PoolStats))
	for i, ps := range z.rebalMeta.PoolStats {
		// skip pools which have completed rebalancing
		if ps.Info.Status != rebalStarted {
			continue
		}

		participants[i] = ps.Participating
	}
	z.rebalMu.RUnlock()

	for poolIdx, doRebalance := range participants {
		if !doRebalance {
			continue
		}
		// nothing to do if this node is not pool's first node (i.e pool's rebalance 'leader').
		if !globalEndpoints[poolIdx].Endpoints[0].IsLocal {
			continue
		}

		go func(idx int) {
			stopfn := globalRebalanceMetrics.log(rebalanceMetricRebalanceBuckets, idx)
			err := z.rebalanceBuckets(ctx, idx)
			stopfn(0, err)
		}(poolIdx)
	}
}

// StopRebalance signals the rebalance goroutine running on this node (if any)
// to stop, using the context.CancelFunc(s) saved at the time ofStartRebalance.
func (z *erasureServerPools) StopRebalance() error {
	z.rebalMu.Lock()
	defer z.rebalMu.Unlock()

	r := z.rebalMeta
	if r == nil { // rebalance not running in this node, nothing to do
		return nil
	}

	if cancel := z.rebalCancel; cancel != nil {
		cancel()
		z.rebalCancel = nil
	}
	return nil
}

// for rebalance trace support
type rebalanceMetrics struct{}

var globalRebalanceMetrics rebalanceMetrics

//go:generate stringer -type=rebalanceMetric -trimprefix=rebalanceMetric $GOFILE
type rebalanceMetric uint8

const (
	rebalanceMetricRebalanceBuckets rebalanceMetric = iota
	rebalanceMetricRebalanceBucket
	rebalanceMetricRebalanceObject
	rebalanceMetricRebalanceRemoveObject
	rebalanceMetricSaveMetadata
)

var errDataMovementSrcDstPoolSame = errors.New("source and destination pool are the same")

func rebalanceTrace(r rebalanceMetric, poolIdx int, startTime time.Time, duration time.Duration, err error, path string, sz int64) madmin.TraceInfo {
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	return madmin.TraceInfo{
		TraceType: madmin.TraceRebalance,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  fmt.Sprintf("rebalance.%s (pool-id=%d)", r.String(), poolIdx),
		Duration:  duration,
		Path:      path,
		Error:     errStr,
		Bytes:     sz,
	}
}

func (p *rebalanceMetrics) log(r rebalanceMetric, poolIdx int, paths ...string) func(sz int64, err error) {
	startTime := time.Now()
	return func(sz int64, err error) {
		duration := time.Since(startTime)
		if globalTrace.NumSubscribers(madmin.TraceRebalance) > 0 {
			globalTrace.Publish(rebalanceTrace(r, poolIdx, startTime, duration, err, strings.Join(paths, " "), sz))
		}
	}
}
