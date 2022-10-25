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
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lithammer/shortuuid/v4"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/env"
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

func (rs *rebalanceStats) update(bucket string, oi ObjectInfo) {
	if oi.IsLatest {
		rs.NumObjects++
	}

	rs.NumVersions++
	rs.Bytes += uint64(oi.Size)
	rs.Bucket = bucket
	rs.Object = oi.Name
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
	cancel          context.CancelFunc `msg:"-"` // to be invoked on rebalance-stop
	lastRefreshedAt time.Time          `msg:"-"`
	StoppedAt       time.Time          `msg:"stopTs"` // Time when rebalance-stop was issued.
	ID              string             `msg:"id"`     // ID of the ongoing rebalance operation
	PercentFreeGoal float64            `msg:"pf"`     // Computed from total free space and capacity at the start of rebalance
	PoolStats       []*rebalanceStats  `msg:"rss"`    // Per-pool rebalance stats keyed by pool index
}

var errRebalanceNotStarted = errors.New("rebalance not started")

func (z *erasureServerPools) loadRebalanceMeta(ctx context.Context) error {
	r := &rebalanceMeta{}
	err := r.load(ctx, z.serverPools[0])
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil
		}
		return err
	}

	z.rebalMu.Lock()
	z.rebalMeta = r
	z.rebalMu.Unlock()

	return nil
}

// initRebalanceMeta initializes rebalance metadata for a new rebalance
// operation and saves it in the object store.
func (z *erasureServerPools) initRebalanceMeta(ctx context.Context, buckets []string) (arn string, err error) {
	r := &rebalanceMeta{
		ID:        shortuuid.New(),
		PoolStats: make([]*rebalanceStats, len(z.serverPools)),
	}

	// Fetch disk capacity and available space.
	si, _ := z.StorageInfo(ctx)
	diskStats := make([]struct {
		AvailableSpace uint64
		TotalSpace     uint64
	}, len(z.serverPools))
	var totalCap, totalFree uint64
	for _, disk := range si.Disks {
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

func (z *erasureServerPools) updatePoolStats(poolIdx int, bucket string, oi ObjectInfo) {
	z.rebalMu.Lock()
	defer z.rebalMu.Unlock()

	r := z.rebalMeta
	if r == nil {
		return
	}

	r.PoolStats[poolIdx].update(bucket, oi)
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

	r.lastRefreshedAt = time.Now()

	return nil
}

func (r *rebalanceMeta) saveWithOpts(ctx context.Context, store objectIO, opts ObjectOptions) error {
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

func (z *erasureServerPools) IsRebalanceStarted() bool {
	z.rebalMu.RLock()
	defer z.rebalMu.RUnlock()

	if r := z.rebalMeta; r != nil {
		if r.StoppedAt.IsZero() {
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
		ps := z.rebalMeta.PoolStats[poolIndex]
		return ps.Participating && ps.Info.Status == rebalStarted
	}
	return false
}

func (z *erasureServerPools) rebalanceBuckets(ctx context.Context, poolIdx int) (err error) {
	doneCh := make(chan struct{})
	defer close(doneCh)

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
		var rebalDone bool
		var traceMsg string

		for {
			select {
			case <-doneCh:
				// rebalance completed for poolIdx
				now := time.Now()
				z.rebalMu.Lock()
				z.rebalMeta.PoolStats[poolIdx].Info.Status = rebalCompleted
				z.rebalMeta.PoolStats[poolIdx].Info.EndTime = now
				z.rebalMu.Unlock()

				rebalDone = true
				traceMsg = fmt.Sprintf("completed at %s", now)

			case <-ctx.Done():

				// rebalance stopped for poolIdx
				now := time.Now()
				z.rebalMu.Lock()
				z.rebalMeta.PoolStats[poolIdx].Info.Status = rebalStopped
				z.rebalMeta.PoolStats[poolIdx].Info.EndTime = now
				z.rebalMu.Unlock()

				rebalDone = true
				traceMsg = fmt.Sprintf("stopped at %s", now)

			case <-timer.C:
				traceMsg = fmt.Sprintf("saved at %s", time.Now())
			}

			stopFn := globalRebalanceMetrics.log(rebalanceMetricSaveMetadata, poolIdx, traceMsg)
			err := z.saveRebalanceStats(ctx, poolIdx, rebalSaveStats)
			stopFn(err)
			logger.LogIf(ctx, err)
			timer.Reset(randSleepFor())

			if rebalDone {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		bucket, ok := z.nextRebalBucket(poolIdx)
		if !ok {
			// no more buckets to rebalance or target free_space/capacity reached
			break
		}

		stopFn := globalRebalanceMetrics.log(rebalanceMetricRebalanceBucket, poolIdx, bucket)
		err = z.rebalanceBucket(ctx, bucket, poolIdx)
		if err != nil {
			stopFn(err)
			logger.LogIf(ctx, err)
			return
		}
		stopFn(nil)
		z.bucketRebalanceDone(bucket, poolIdx)
	}

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

// rebalanceBucket rebalances objects under bucket in poolIdx pool
func (z *erasureServerPools) rebalanceBucket(ctx context.Context, bucket string, poolIdx int) error {
	ctx = logger.SetReqInfo(ctx, &logger.ReqInfo{})
	vc, _ := globalBucketVersioningSys.Get(bucket)
	// Check if the current bucket has a configured lifecycle policy
	lc, _ := globalLifecycleSys.Get(bucket)
	// Check if bucket is object locked.
	lr, _ := globalBucketObjectLockSys.Get(bucket)

	pool := z.serverPools[poolIdx]
	const envRebalanceWorkers = "_MINIO_REBALANCE_WORKERS"
	wStr := env.Get(envRebalanceWorkers, strconv.Itoa(len(pool.sets)))
	workerSize, err := strconv.Atoi(wStr)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("invalid %s value: %s err: %v, defaulting to %d", envRebalanceWorkers, wStr, err, len(pool.sets)))
		workerSize = len(pool.sets)
	}
	workers := make(chan struct{}, workerSize)
	var wg sync.WaitGroup
	for _, set := range pool.sets {
		set := set
		disks := set.getOnlineDisks()
		if len(disks) == 0 {
			logger.LogIf(ctx, fmt.Errorf("no online disks found for set with endpoints %s",
				set.getEndpoints()))
			continue
		}

		filterLifecycle := func(bucket, object string, fi FileInfo) bool {
			if lc == nil {
				return false
			}
			versioned := vc != nil && vc.Versioned(object)
			objInfo := fi.ToObjectInfo(bucket, object, versioned)
			event := evalActionFromLifecycle(ctx, *lc, lr, objInfo)
			switch action := event.Action; action {
			case lifecycle.DeleteVersionAction, lifecycle.DeleteAction:
				globalExpiryState.enqueueByDays(objInfo, false, action == lifecycle.DeleteVersionAction)
				// Skip this entry.
				return true
			case lifecycle.DeleteRestoredAction, lifecycle.DeleteRestoredVersionAction:
				globalExpiryState.enqueueByDays(objInfo, true, action == lifecycle.DeleteRestoredVersionAction)
				// Skip this entry.
				return true
			}
			return false
		}

		rebalanceEntry := func(entry metaCacheEntry) {
			defer func() {
				<-workers
				wg.Done()
			}()

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

			var rebalanced int
			for _, version := range fivs.Versions {
				// Skip transitioned objects for now. TBD
				if version.IsRemote() {
					continue
				}

				// Apply lifecycle rules on the objects that are expired.
				if filterLifecycle(bucket, version.Name, version) {
					logger.LogIf(ctx, fmt.Errorf("found %s/%s (%s) expired object based on ILM rules, skipping and scheduled for deletion", bucket, version.Name, version.VersionID))
					continue
				}

				// We will skip rebalancing delete markers
				// with single version, its as good as there
				// is no data associated with the object.
				if version.Deleted && len(fivs.Versions) == 1 {
					logger.LogIf(ctx, fmt.Errorf("found %s/%s delete marked object with no other versions, skipping since there is no data to be rebalanced", bucket, version.Name))
					continue
				}

				if version.Deleted {
					_, err := z.DeleteObject(ctx,
						bucket,
						version.Name,
						ObjectOptions{
							Versioned:         vc.PrefixEnabled(version.Name),
							VersionID:         version.VersionID,
							MTime:             version.ModTime,
							DeleteReplication: version.ReplicationState,
							DeleteMarker:      true, // make sure we create a delete marker
							SkipRebalancing:   true, // make sure we skip the decommissioned pool
						})
					var failure bool
					if err != nil && !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
						logger.LogIf(ctx, err)
						failure = true
					}

					if !failure {
						z.updatePoolStats(poolIdx, bucket, version.ToObjectInfo(bucket, version.Name, vc.PrefixEnabled(version.Name)))
						rebalanced++
					}
					continue
				}

				var failure bool
				var oi ObjectInfo
				for try := 0; try < 3; try++ {
					// GetObjectReader.Close is called by rebalanceObject
					gr, err := set.GetObjectNInfo(ctx,
						bucket,
						encodeDirObject(version.Name),
						nil,
						http.Header{},
						noLock, // all mutations are blocked reads are safe without locks.
						ObjectOptions{
							VersionID:    version.VersionID,
							NoDecryption: true,
						})
					if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
						// object deleted by the application, nothing to do here we move on.
						return
					}
					if err != nil {
						failure = true
						logger.LogIf(ctx, err)
						continue
					}

					stopFn := globalRebalanceMetrics.log(rebalanceMetricRebalanceObject, poolIdx, bucket, version.Name)
					if err = z.rebalanceObject(ctx, bucket, gr); err != nil {
						stopFn(err)
						failure = true
						logger.LogIf(ctx, err)
						continue
					}

					stopFn(nil)
					failure = false
					oi = gr.ObjInfo
					break
				}

				if failure {
					break // break out on first error
				}
				z.updatePoolStats(poolIdx, bucket, oi)
				rebalanced++
			}

			// if all versions were rebalanced, we can delete the object versions.
			if rebalanced == len(fivs.Versions) {
				stopFn := globalRebalanceMetrics.log(rebalanceMetricRebalanceRemoveObject, poolIdx, bucket, entry.name)
				_, err := set.DeleteObject(ctx,
					bucket,
					encodeDirObject(entry.name),
					ObjectOptions{
						DeletePrefix: true, // use prefix delete to delete all versions at once.
					},
				)
				stopFn(err)
				auditLogRebalance(ctx, "Rebalance:DeleteObject", bucket, entry.name, "", err)
				if err != nil {
					logger.LogIf(ctx, err)
				}
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			// How to resolve partial results.
			resolver := metadataResolutionParams{
				dirQuorum: len(disks) / 2, // make sure to capture all quorum ratios
				objQuorum: len(disks) / 2, // make sure to capture all quorum ratios
				bucket:    bucket,
			}
			err := listPathRaw(ctx, listPathRawOptions{
				disks:          disks,
				bucket:         bucket,
				recursive:      true,
				forwardTo:      "",
				minDisks:       len(disks) / 2, // to capture all quorum ratios
				reportNotFound: false,
				agreed: func(entry metaCacheEntry) {
					workers <- struct{}{}
					wg.Add(1)
					go rebalanceEntry(entry)
				},
				partial: func(entries metaCacheEntries, _ []error) {
					entry, ok := entries.resolve(&resolver)
					if ok {
						workers <- struct{}{}
						wg.Add(1)
						go rebalanceEntry(*entry)
					}
				},
				finished: nil,
			})
			logger.LogIf(ctx, err)
		}()
	}
	wg.Wait()
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
		logger.LogIf(ctx, fmt.Errorf("failed to acquire write lock on %s/%s: %w", minioMetaBucket, rebalMetaName, err))
		return err
	}
	defer lock.Unlock(lkCtx.Cancel)

	ctx = lkCtx.Context()
	noLockOpts := ObjectOptions{NoLock: true}
	r := &rebalanceMeta{}
	if err := r.loadWithOpts(ctx, z.serverPools[0], noLockOpts); err != nil {
		return err
	}

	z.rebalMu.Lock()
	defer z.rebalMu.Unlock()

	switch opts {
	case rebalSaveStoppedAt:
		r.StoppedAt = time.Now()
	case rebalSaveStats:
		r.PoolStats[poolIdx] = z.rebalMeta.PoolStats[poolIdx]
	}
	z.rebalMeta = r

	err = z.rebalMeta.saveWithOpts(ctx, z.serverPools[0], noLockOpts)
	return err
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

func (z *erasureServerPools) rebalanceObject(ctx context.Context, bucket string, gr *GetObjectReader) (err error) {
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
			VersionID:   oi.VersionID,
			MTime:       oi.ModTime,
			UserDefined: oi.UserDefined,
		})
		if err != nil {
			return fmt.Errorf("rebalanceObject: NewMultipartUpload() %w", err)
		}
		defer z.AbortMultipartUpload(ctx, bucket, oi.Name, res.UploadID, ObjectOptions{})

		parts := make([]CompletePart, len(oi.Parts))
		for i, part := range oi.Parts {
			hr, err := hash.NewReader(gr, part.Size, "", "", part.ActualSize)
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
			MTime: oi.ModTime,
		})
		if err != nil {
			err = fmt.Errorf("rebalanceObject: CompleteMultipartUpload() %w", err)
		}
		return err
	}

	hr, err := hash.NewReader(gr, oi.Size, "", "", actualSize)
	if err != nil {
		return fmt.Errorf("rebalanceObject: hash.NewReader() %w", err)
	}
	_, err = z.PutObject(ctx,
		bucket,
		oi.Name,
		NewPutObjReader(hr),
		ObjectOptions{
			VersionID:    oi.VersionID,
			MTime:        oi.ModTime,
			UserDefined:  oi.UserDefined,
			PreserveETag: oi.ETag, // Preserve original ETag to ensure same metadata.
			IndexCB: func() []byte {
				return oi.Parts[0].Index // Preserve part Index to ensure decompression works.
			},
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
	z.rebalMeta.cancel = cancel // to be used when rebalance-stop is called
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
			stopfn(err)
		}(poolIdx)
	}
	return
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

	if cancel := r.cancel; cancel != nil {
		// cancel != nil only on pool leaders
		r.cancel = nil
		cancel()
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

func rebalanceTrace(r rebalanceMetric, poolIdx int, startTime time.Time, duration time.Duration, err error, path string) madmin.TraceInfo {
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
	}
}

func (p *rebalanceMetrics) log(r rebalanceMetric, poolIdx int, paths ...string) func(err error) {
	startTime := time.Now()
	return func(err error) {
		duration := time.Since(startTime)
		if globalTrace.NumSubscribers(madmin.TraceRebalance) > 0 {
			globalTrace.Publish(rebalanceTrace(r, poolIdx, startTime, duration, err, strings.Join(paths, " ")))
		}
	}
}
