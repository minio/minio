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
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/env"
)

// PoolDecommissionInfo currently decommissioning information
type PoolDecommissionInfo struct {
	StartTime   time.Time `json:"startTime" msg:"st"`
	StartSize   int64     `json:"startSize" msg:"ss"`
	TotalSize   int64     `json:"totalSize" msg:"ts"`
	CurrentSize int64     `json:"currentSize" msg:"cs"`

	Complete bool `json:"complete" msg:"cmp"`
	Failed   bool `json:"failed" msg:"fl"`
	Canceled bool `json:"canceled" msg:"cnl"`

	// Internal information.
	QueuedBuckets         []string `json:"-" msg:"bkts"`
	DecommissionedBuckets []string `json:"-" msg:"dbkts"`

	// Last bucket/object decommissioned.
	Bucket string `json:"-" msg:"bkt"`
	// Captures prefix that is currently being
	// decommissioned inside the 'Bucket'
	Prefix string `json:"-" msg:"pfx"`
	Object string `json:"-" msg:"obj"`

	// Verbose information
	ItemsDecommissioned     int64 `json:"-" msg:"id"`
	ItemsDecommissionFailed int64 `json:"-" msg:"idf"`
	BytesDone               int64 `json:"-" msg:"bd"`
	BytesFailed             int64 `json:"-" msg:"bf"`
}

// bucketPop should be called when a bucket is done decommissioning.
// Adds the bucket to the list of decommissioned buckets and updates resume numbers.
func (pd *PoolDecommissionInfo) bucketPop(bucket string) {
	pd.DecommissionedBuckets = append(pd.DecommissionedBuckets, bucket)
	for i, b := range pd.QueuedBuckets {
		if b == bucket {
			// Bucket is done.
			pd.QueuedBuckets = append(pd.QueuedBuckets[:i], pd.QueuedBuckets[i+1:]...)
			// Clear tracker info.
			if pd.Bucket == bucket {
				pd.Bucket = "" // empty this out for next bucket
				pd.Prefix = "" // empty this out for the next bucket
				pd.Object = "" // empty this out for next object
			}
			return
		}
	}
}

func (pd *PoolDecommissionInfo) isBucketDecommissioned(bucket string) bool {
	for _, b := range pd.DecommissionedBuckets {
		if b == bucket {
			return true
		}
	}
	return false
}

func (pd *PoolDecommissionInfo) bucketPush(bucket decomBucketInfo) {
	for _, b := range pd.QueuedBuckets {
		if pd.isBucketDecommissioned(b) {
			return
		}
		if b == bucket.String() {
			return
		}
	}
	pd.QueuedBuckets = append(pd.QueuedBuckets, bucket.String())
	pd.Bucket = bucket.Name
	pd.Prefix = bucket.Prefix
}

// PoolStatus captures current pool status
type PoolStatus struct {
	ID           int                   `json:"id" msg:"id"`
	CmdLine      string                `json:"cmdline" msg:"cl"`
	LastUpdate   time.Time             `json:"lastUpdate" msg:"lu"`
	Decommission *PoolDecommissionInfo `json:"decommissionInfo,omitempty" msg:"dec"`
}

//go:generate msgp -file $GOFILE -unexported
type poolMeta struct {
	Version int          `msg:"v"`
	Pools   []PoolStatus `msg:"pls"`
}

// A decommission resumable tells us if decommission is worth
// resuming upon restart of a cluster.
func (p *poolMeta) returnResumablePools(n int) []PoolStatus {
	var newPools []PoolStatus
	for _, pool := range p.Pools {
		if pool.Decommission == nil {
			continue
		}
		if pool.Decommission.Complete || pool.Decommission.Canceled {
			// Do not resume decommission upon startup for
			// - decommission complete
			// - decommission canceled
			continue
		} // In all other situations we need to resume
		newPools = append(newPools, pool)
		if n > 0 && len(newPools) == n {
			return newPools
		}
	}
	return nil
}

func (p *poolMeta) DecommissionComplete(idx int) bool {
	if p.Pools[idx].Decommission != nil && !p.Pools[idx].Decommission.Complete {
		p.Pools[idx].LastUpdate = UTCNow()
		p.Pools[idx].Decommission.Complete = true
		p.Pools[idx].Decommission.Failed = false
		p.Pools[idx].Decommission.Canceled = false
		return true
	}
	return false
}

func (p *poolMeta) DecommissionFailed(idx int) bool {
	if p.Pools[idx].Decommission != nil && !p.Pools[idx].Decommission.Failed {
		p.Pools[idx].LastUpdate = UTCNow()
		p.Pools[idx].Decommission.StartTime = time.Time{}
		p.Pools[idx].Decommission.Complete = false
		p.Pools[idx].Decommission.Failed = true
		p.Pools[idx].Decommission.Canceled = false
		return true
	}
	return false
}

func (p *poolMeta) DecommissionCancel(idx int) bool {
	if p.Pools[idx].Decommission != nil && !p.Pools[idx].Decommission.Canceled {
		p.Pools[idx].LastUpdate = UTCNow()
		p.Pools[idx].Decommission.StartTime = time.Time{}
		p.Pools[idx].Decommission.Complete = false
		p.Pools[idx].Decommission.Failed = false
		p.Pools[idx].Decommission.Canceled = true
		return true
	}
	return false
}

func (p poolMeta) isBucketDecommissioned(idx int, bucket string) bool {
	return p.Pools[idx].Decommission.isBucketDecommissioned(bucket)
}

func (p *poolMeta) BucketDone(idx int, bucket decomBucketInfo) {
	if p.Pools[idx].Decommission == nil {
		// Decommission not in progress.
		return
	}
	p.Pools[idx].Decommission.bucketPop(bucket.String())
}

func (p poolMeta) ResumeBucketObject(idx int) (bucket, object string) {
	if p.Pools[idx].Decommission != nil {
		bucket = p.Pools[idx].Decommission.Bucket
		object = p.Pools[idx].Decommission.Object
	}
	return
}

func (p *poolMeta) TrackCurrentBucketObject(idx int, bucket string, object string) {
	if p.Pools[idx].Decommission == nil {
		// Decommission not in progress.
		return
	}
	p.Pools[idx].Decommission.Bucket = bucket
	p.Pools[idx].Decommission.Object = object
}

func (p *poolMeta) PendingBuckets(idx int) []decomBucketInfo {
	if p.Pools[idx].Decommission == nil {
		// Decommission not in progress.
		return nil
	}

	decomBuckets := make([]decomBucketInfo, len(p.Pools[idx].Decommission.QueuedBuckets))
	for i := range decomBuckets {
		bucket, prefix := path2BucketObject(p.Pools[idx].Decommission.QueuedBuckets[i])
		decomBuckets[i] = decomBucketInfo{
			Name:   bucket,
			Prefix: prefix,
		}
	}

	return decomBuckets
}

//msgp:ignore decomBucketInfo
type decomBucketInfo struct {
	Name   string
	Prefix string
}

func (db decomBucketInfo) String() string {
	return pathJoin(db.Name, db.Prefix)
}

func (p *poolMeta) QueueBuckets(idx int, buckets []decomBucketInfo) {
	// add new queued buckets
	for _, bucket := range buckets {
		p.Pools[idx].Decommission.bucketPush(bucket)
	}
}

var (
	errDecommissionAlreadyRunning = errors.New("decommission is already in progress")
	errDecommissionComplete       = errors.New("decommission is complete, please remove the servers from command-line")
)

func (p *poolMeta) Decommission(idx int, pi poolSpaceInfo) error {
	for i, pool := range p.Pools {
		if idx == i {
			continue
		}
		if pool.Decommission != nil {
			// Do not allow multiple decommissions at the same time.
			// We shall for now only allow one pool decommission at
			// a time.
			return fmt.Errorf("%w at index: %d", errDecommissionAlreadyRunning, i)
		}
	}

	// Return an error when there is decommission on going - the user needs
	// to explicitly cancel it first in order to restart decommissioning again.
	if p.Pools[idx].Decommission != nil &&
		!p.Pools[idx].Decommission.Complete &&
		!p.Pools[idx].Decommission.Failed &&
		!p.Pools[idx].Decommission.Canceled {
		return errDecommissionAlreadyRunning
	}

	now := UTCNow()
	p.Pools[idx].LastUpdate = now
	p.Pools[idx].Decommission = &PoolDecommissionInfo{
		StartTime:   now,
		StartSize:   pi.Free,
		CurrentSize: pi.Free,
		TotalSize:   pi.Total,
	}
	return nil
}

func (p poolMeta) IsSuspended(idx int) bool {
	return p.Pools[idx].Decommission != nil
}

func (p *poolMeta) validate(pools []*erasureSets) (bool, error) {
	type poolInfo struct {
		position     int
		completed    bool
		decomStarted bool // started but not finished yet
	}

	rememberedPools := make(map[string]poolInfo)
	for idx, pool := range p.Pools {
		complete := false
		decomStarted := false
		if pool.Decommission != nil {
			if pool.Decommission.Complete {
				complete = true
			}
			decomStarted = true
		}
		rememberedPools[pool.CmdLine] = poolInfo{
			position:     idx,
			completed:    complete,
			decomStarted: decomStarted,
		}
	}

	specifiedPools := make(map[string]int)
	for idx, pool := range pools {
		specifiedPools[pool.endpoints.CmdLine] = idx
	}

	replaceScheme := func(k string) string {
		// This is needed as fallback when users are changeing
		// from http->https or https->http, we need to verify
		// both because MinIO remembers the command-line in
		// "exact" order - as long as this order is not disturbed
		// we allow changing the "scheme" i.e internode communication
		// from plain-text to TLS or from TLS to plain-text.
		if strings.HasPrefix(k, "http://") {
			k = strings.ReplaceAll(k, "http://", "https://")
		} else if strings.HasPrefix(k, "https://") {
			k = strings.ReplaceAll(k, "https://", "http://")
		}
		return k
	}

	var update bool
	// Check if specified pools need to remove decommissioned pool.
	for k := range specifiedPools {
		pi, ok := rememberedPools[k]
		if !ok {
			pi, ok = rememberedPools[replaceScheme(k)]
			if ok {
				update = true // Looks like user is changing from http->https or https->http
			}
		}
		if ok && pi.completed {
			return false, fmt.Errorf("pool(%s) = %s is decommissioned, please remove from server command line", humanize.Ordinal(pi.position+1), k)
		}
	}

	// check if remembered pools are in right position or missing from command line.
	for k, pi := range rememberedPools {
		if pi.completed {
			continue
		}
		_, ok := specifiedPools[k]
		if !ok {
			_, ok = specifiedPools[replaceScheme(k)]
			if ok {
				update = true // Looks like user is changing from http->https or https->http
			}
		}
		if !ok {
			if globalIsErasureSD {
				update = true
			} else {
				return false, fmt.Errorf("pool(%s) = %s is not specified, please specify on server command line", humanize.Ordinal(pi.position+1), k)
			}
		}
	}

	// check when remembered pools and specified pools are same they are at the expected position
	if len(rememberedPools) == len(specifiedPools) {
		for k, pi := range rememberedPools {
			pos, ok := specifiedPools[k]
			if !ok {
				pos, ok = specifiedPools[replaceScheme(k)]
				if ok {
					update = true // Looks like user is changing from http->https or https->http
				}
			}
			if !ok {
				if globalIsErasureSD {
					update = true
				} else {
					return false, fmt.Errorf("pool(%s) = %s is not specified, please specify on server command line", humanize.Ordinal(pi.position+1), k)
				}
			}
			if ok && pos != pi.position {
				return false, fmt.Errorf("pool order change detected for %s, expected position is (%s) but found (%s)", k, humanize.Ordinal(pi.position+1), humanize.Ordinal(pos+1))
			}
		}
	}

	if !update {
		update = len(rememberedPools) != len(specifiedPools)
	}
	if update {
		for k, pi := range rememberedPools {
			if pi.decomStarted && !pi.completed {
				return false, fmt.Errorf("pool(%s) = %s is being decommissioned, No changes should be made to the command line arguments. Please complete the decommission in progress", humanize.Ordinal(pi.position+1), k)
			}
		}
	}
	return update, nil
}

func (p *poolMeta) load(ctx context.Context, pool *erasureSets, pools []*erasureSets) error {
	data, err := readConfig(ctx, pool, poolMetaName)
	if err != nil {
		if errors.Is(err, errConfigNotFound) || isErrObjectNotFound(err) {
			return nil
		}
		return err
	}
	if len(data) == 0 {
		// Seems to be empty create a new poolMeta object.
		return nil
	}
	if len(data) <= 4 {
		return fmt.Errorf("poolMeta: no data")
	}
	// Read header
	switch binary.LittleEndian.Uint16(data[0:2]) {
	case poolMetaFormat:
	default:
		return fmt.Errorf("poolMeta: unknown format: %d", binary.LittleEndian.Uint16(data[0:2]))
	}
	switch binary.LittleEndian.Uint16(data[2:4]) {
	case poolMetaVersion:
	default:
		return fmt.Errorf("poolMeta: unknown version: %d", binary.LittleEndian.Uint16(data[2:4]))
	}

	// OK, parse data.
	if _, err = p.UnmarshalMsg(data[4:]); err != nil {
		return err
	}

	switch p.Version {
	case poolMetaVersionV1:
	default:
		return fmt.Errorf("unexpected pool meta version: %d", p.Version)
	}

	return nil
}

func (p *poolMeta) CountItem(idx int, size int64, failed bool) {
	pd := p.Pools[idx].Decommission
	if pd != nil {
		if failed {
			pd.ItemsDecommissionFailed++
			pd.BytesFailed += size
		} else {
			pd.ItemsDecommissioned++
			pd.BytesDone += size
		}
		p.Pools[idx].Decommission = pd
	}
}

func (p *poolMeta) updateAfter(ctx context.Context, idx int, pools []*erasureSets, duration time.Duration) (bool, error) {
	if p.Pools[idx].Decommission == nil {
		return false, errInvalidArgument
	}
	now := UTCNow()
	if now.Sub(p.Pools[idx].LastUpdate) >= duration {
		if serverDebugLog {
			console.Debugf("decommission: persisting poolMeta on drive: threshold:%s, poolMeta:%#v\n", now.Sub(p.Pools[idx].LastUpdate), p.Pools[idx])
		}
		p.Pools[idx].LastUpdate = now
		if err := p.save(ctx, pools); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (p poolMeta) save(ctx context.Context, pools []*erasureSets) error {
	data := make([]byte, 4, p.Msgsize()+4)

	// Initialize the header.
	binary.LittleEndian.PutUint16(data[0:2], poolMetaFormat)
	binary.LittleEndian.PutUint16(data[2:4], poolMetaVersion)

	buf, err := p.MarshalMsg(data)
	if err != nil {
		return err
	}

	// Saves on all pools to make sure decommissioning of first pool is allowed.
	for _, eset := range pools {
		if err = saveConfig(ctx, eset, poolMetaName, buf); err != nil {
			return err
		}
	}
	return nil
}

const (
	poolMetaName      = "pool.bin"
	poolMetaFormat    = 1
	poolMetaVersionV1 = 1
	poolMetaVersion   = poolMetaVersionV1
)

// Init() initializes pools and saves additional information about them
// in 'pool.bin', this is eventually used for decommissioning the pool.
func (z *erasureServerPools) Init(ctx context.Context) error {
	// Load rebalance metadata if present
	err := z.loadRebalanceMeta(ctx)
	if err != nil {
		return fmt.Errorf("failed to load rebalance data: %w", err)
	}

	// Start rebalance routine
	z.StartRebalance()

	meta := poolMeta{}

	if err := meta.load(ctx, z.serverPools[0], z.serverPools); err != nil {
		return err
	}

	update, err := meta.validate(z.serverPools)
	if err != nil {
		return err
	}

	// if no update is needed return right away.
	if !update {
		z.poolMeta = meta

		// We are only supporting single pool decommission at this time
		// so it makes sense to only resume single pools at any given
		// time, in future meta.returnResumablePools() might take
		// '-1' as argument to decommission multiple pools at a time
		// but this is not a priority at the moment.
		for _, pool := range meta.returnResumablePools(1) {
			idx := globalEndpoints.GetPoolIdx(pool.CmdLine)
			if idx == -1 {
				return fmt.Errorf("unexpected state present for decommission status pool(%s) not found", pool.CmdLine)
			}
			if globalEndpoints[idx].Endpoints[0].IsLocal {
				go func(pool PoolStatus) {
					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					for {
						if err := z.Decommission(ctx, pool.ID); err != nil {
							switch err {
							// we already started decommission
							case errDecommissionAlreadyRunning:
								// A previous decommission running found restart it.
								z.doDecommissionInRoutine(ctx, idx)
								return
							default:
								if configRetriableErrors(err) {
									logger.LogIf(ctx, fmt.Errorf("Unable to resume decommission of pool %v: %w: retrying..", pool, err))
									time.Sleep(time.Second + time.Duration(r.Float64()*float64(5*time.Second)))
									continue
								}
								logger.LogIf(ctx, fmt.Errorf("Unable to resume decommission of pool %v: %w", pool, err))
								return
							}
						}
						break
					}
				}(pool)
			}
		}

		return nil
	}

	meta = poolMeta{} // to update write poolMeta fresh.
	// looks like new pool was added we need to update,
	// or this is a fresh installation (or an existing
	// installation with pool removed)
	meta.Version = poolMetaVersion
	for idx, pool := range z.serverPools {
		meta.Pools = append(meta.Pools, PoolStatus{
			CmdLine:    pool.endpoints.CmdLine,
			ID:         idx,
			LastUpdate: UTCNow(),
		})
	}
	if err = meta.save(ctx, z.serverPools); err != nil {
		return err
	}
	z.poolMeta = meta
	return nil
}

func (z *erasureServerPools) IsDecommissionRunning() bool {
	z.poolMetaMutex.RLock()
	defer z.poolMetaMutex.RUnlock()
	meta := z.poolMeta
	for _, pool := range meta.Pools {
		if pool.Decommission != nil {
			return true
		}
	}

	return false
}

func (z *erasureServerPools) decommissionObject(ctx context.Context, bucket string, gr *GetObjectReader) (err error) {
	objInfo := gr.ObjInfo

	defer func() {
		gr.Close()
		auditLogDecom(ctx, "DecomCopyData", objInfo.Bucket, objInfo.Name, objInfo.VersionID, err)
	}()

	actualSize, err := objInfo.GetActualSize()
	if err != nil {
		return err
	}

	if objInfo.isMultipart() {
		res, err := z.NewMultipartUpload(ctx, bucket, objInfo.Name, ObjectOptions{
			VersionID:   objInfo.VersionID,
			MTime:       objInfo.ModTime,
			UserDefined: objInfo.UserDefined,
		})
		if err != nil {
			return fmt.Errorf("decommissionObject: NewMultipartUpload() %w", err)
		}
		defer z.AbortMultipartUpload(ctx, bucket, objInfo.Name, res.UploadID, ObjectOptions{})
		parts := make([]CompletePart, len(objInfo.Parts))
		for i, part := range objInfo.Parts {
			hr, err := hash.NewReader(gr, part.Size, "", "", part.ActualSize)
			if err != nil {
				return fmt.Errorf("decommissionObject: hash.NewReader() %w", err)
			}
			pi, err := z.PutObjectPart(ctx, bucket, objInfo.Name, res.UploadID,
				part.Number,
				NewPutObjReader(hr),
				ObjectOptions{
					PreserveETag: part.ETag, // Preserve original ETag to ensure same metadata.
					IndexCB: func() []byte {
						return part.Index // Preserve part Index to ensure decompression works.
					},
				})
			if err != nil {
				return fmt.Errorf("decommissionObject: PutObjectPart() %w", err)
			}
			parts[i] = CompletePart{
				ETag:           pi.ETag,
				PartNumber:     pi.PartNumber,
				ChecksumCRC32:  pi.ChecksumCRC32,
				ChecksumCRC32C: pi.ChecksumCRC32C,
				ChecksumSHA256: pi.ChecksumSHA256,
				ChecksumSHA1:   pi.ChecksumSHA1,
			}
		}
		_, err = z.CompleteMultipartUpload(ctx, bucket, objInfo.Name, res.UploadID, parts, ObjectOptions{
			MTime: objInfo.ModTime,
		})
		if err != nil {
			err = fmt.Errorf("decommissionObject: CompleteMultipartUpload() %w", err)
		}
		return err
	}

	hr, err := hash.NewReader(gr, objInfo.Size, "", "", actualSize)
	if err != nil {
		return fmt.Errorf("decommissionObject: hash.NewReader() %w", err)
	}
	_, err = z.PutObject(ctx,
		bucket,
		objInfo.Name,
		NewPutObjReader(hr),
		ObjectOptions{
			VersionID:    objInfo.VersionID,
			MTime:        objInfo.ModTime,
			UserDefined:  objInfo.UserDefined,
			PreserveETag: objInfo.ETag, // Preserve original ETag to ensure same metadata.
			IndexCB: func() []byte {
				return objInfo.Parts[0].Index // Preserve part Index to ensure decompression works.
			},
		})
	if err != nil {
		err = fmt.Errorf("decommissionObject: PutObject() %w", err)
	}
	return err
}

// versionsSorter sorts FileInfo slices by version.
//
//msgp:ignore versionsSorter
type versionsSorter []FileInfo

func (v versionsSorter) reverse() {
	sort.Slice(v, func(i, j int) bool {
		return v[i].ModTime.Before(v[j].ModTime)
	})
}

func (z *erasureServerPools) decommissionPool(ctx context.Context, idx int, pool *erasureSets, bi decomBucketInfo) error {
	ctx = logger.SetReqInfo(ctx, &logger.ReqInfo{})

	var wg sync.WaitGroup
	wStr := env.Get("_MINIO_DECOMMISSION_WORKERS", strconv.Itoa(len(pool.sets)))
	workerSize, err := strconv.Atoi(wStr)
	if err != nil {
		return err
	}

	parallelWorkers := make(chan struct{}, workerSize)

	for _, set := range pool.sets {
		set := set
		disks := set.getOnlineDisks()
		if len(disks) == 0 {
			logger.LogIf(GlobalContext, fmt.Errorf("no online drives found for set with endpoints %s",
				set.getEndpoints()))
			continue
		}

		vc, _ := globalBucketVersioningSys.Get(bi.Name)

		// Check if the current bucket has a configured lifecycle policy
		lc, _ := globalLifecycleSys.Get(bi.Name)

		// Check if bucket is object locked.
		lr, _ := globalBucketObjectLockSys.Get(bi.Name)

		filterLifecycle := func(bucket, object string, fi FileInfo) bool {
			if lc == nil {
				return false
			}
			versioned := vc != nil && vc.Versioned(object)
			objInfo := fi.ToObjectInfo(bucket, object, versioned)
			evt := evalActionFromLifecycle(ctx, *lc, lr, objInfo)
			switch evt.Action {
			case lifecycle.DeleteVersionAction, lifecycle.DeleteAction:
				globalExpiryState.enqueueByDays(objInfo, false, evt.Action == lifecycle.DeleteVersionAction)
				// Skip this entry.
				return true
			case lifecycle.DeleteRestoredAction, lifecycle.DeleteRestoredVersionAction:
				globalExpiryState.enqueueByDays(objInfo, true, evt.Action == lifecycle.DeleteRestoredVersionAction)
				// Skip this entry.
				return true
			}
			return false
		}

		decommissionEntry := func(entry metaCacheEntry) {
			defer func() {
				<-parallelWorkers
				wg.Done()
			}()

			if entry.isDir() {
				return
			}

			fivs, err := entry.fileInfoVersions(bi.Name)
			if err != nil {
				return
			}

			// We need a reversed order for decommissioning,
			// to create the appropriate stack.
			versionsSorter(fivs.Versions).reverse()

			var decommissionedCount int
			for _, version := range fivs.Versions {
				// TODO: Skip transitioned objects for now.
				if version.IsRemote() {
					logger.LogIf(ctx, fmt.Errorf("found %s/%s transitioned object, transitioned object won't be decommissioned", bi.Name, version.Name))
					continue
				}

				// Apply lifecycle rules on the objects that are expired.
				if filterLifecycle(bi.Name, version.Name, version) {
					logger.LogIf(ctx, fmt.Errorf("found %s/%s (%s) expired object based on ILM rules, skipping and scheduled for deletion", bi.Name, version.Name, version.VersionID))
					continue
				}

				// We will skip decommissioning delete markers
				// with single version, its as good as there
				// is no data associated with the object.
				if version.Deleted && len(fivs.Versions) == 1 {
					logger.LogIf(ctx, fmt.Errorf("found %s/%s delete marked object with no other versions, skipping since there is no content left", bi.Name, version.Name))
					continue
				}

				if version.Deleted {
					_, err := z.DeleteObject(ctx,
						bi.Name,
						version.Name,
						ObjectOptions{
							Versioned:          vc.PrefixEnabled(version.Name),
							VersionID:          version.VersionID,
							MTime:              version.ModTime,
							DeleteReplication:  version.ReplicationState,
							DeleteMarker:       true, // make sure we create a delete marker
							SkipDecommissioned: true, // make sure we skip the decommissioned pool
						})
					if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
						// object/version already deleted by the application, nothing to do here we move on.
						continue
					}
					var failure bool
					if err != nil {
						logger.LogIf(ctx, err)
						failure = true
					}
					z.poolMetaMutex.Lock()
					z.poolMeta.CountItem(idx, 0, failure)
					z.poolMetaMutex.Unlock()
					if !failure {
						// Success keep a count.
						decommissionedCount++
					}
					continue
				}

				var failure, ignore bool
				// gr.Close() is ensured by decommissionObject().
				for try := 0; try < 3; try++ {
					gr, err := set.GetObjectNInfo(ctx,
						bi.Name,
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
						ignore = true
						break
					}
					if err != nil {
						failure = true
						logger.LogIf(ctx, err)
						continue
					}
					stopFn := globalDecommissionMetrics.log(decomMetricDecommissionObject, idx, bi.Name, version.Name, version.VersionID)
					if err = z.decommissionObject(ctx, bi.Name, gr); err != nil {
						stopFn(err)
						failure = true
						logger.LogIf(ctx, err)
						continue
					}
					stopFn(nil)
					failure = false
					break
				}
				if ignore {
					continue
				}
				z.poolMetaMutex.Lock()
				z.poolMeta.CountItem(idx, version.Size, failure)
				z.poolMetaMutex.Unlock()
				if failure {
					break // break out on first error
				}
				decommissionedCount++
			}

			// if all versions were decommissioned, then we can delete the object versions.
			if decommissionedCount == len(fivs.Versions) {
				stopFn := globalDecommissionMetrics.log(decomMetricDecommissionRemoveObject, idx, bi.Name, entry.name)
				_, err := set.DeleteObject(ctx,
					bi.Name,
					encodeDirObject(entry.name),
					ObjectOptions{
						DeletePrefix: true, // use prefix delete to delete all versions at once.
					},
				)
				stopFn(err)
				auditLogDecom(ctx, "DecomDeleteObject", bi.Name, entry.name, "", err)
				if err != nil {
					logger.LogIf(ctx, err)
				}
			}
			z.poolMetaMutex.Lock()
			z.poolMeta.TrackCurrentBucketObject(idx, bi.Name, entry.name)
			ok, err := z.poolMeta.updateAfter(ctx, idx, z.serverPools, 30*time.Second)
			logger.LogIf(ctx, err)
			if ok {
				globalNotificationSys.ReloadPoolMeta(ctx)
			}
			z.poolMetaMutex.Unlock()
		}

		// How to resolve partial results.
		resolver := metadataResolutionParams{
			dirQuorum: len(disks) / 2, // make sure to capture all quorum ratios
			objQuorum: len(disks) / 2, // make sure to capture all quorum ratios
			bucket:    bi.Name,
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := listPathRaw(ctx, listPathRawOptions{
				disks:          disks,
				bucket:         bi.Name,
				path:           bi.Prefix,
				recursive:      true,
				forwardTo:      "",
				minDisks:       len(disks) / 2, // to capture all quorum ratios
				reportNotFound: false,
				agreed: func(entry metaCacheEntry) {
					parallelWorkers <- struct{}{}
					wg.Add(1)
					go decommissionEntry(entry)
				},
				partial: func(entries metaCacheEntries, _ []error) {
					entry, ok := entries.resolve(&resolver)
					if ok {
						parallelWorkers <- struct{}{}
						wg.Add(1)
						go decommissionEntry(*entry)
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

//msgp:ignore decomMetrics
type decomMetrics struct{}

var globalDecommissionMetrics decomMetrics

//msgp:ignore decomMetric
//go:generate stringer -type=decomMetric -trimprefix=decomMetric $GOFILE
type decomMetric uint8

const (
	decomMetricDecommissionBucket decomMetric = iota
	decomMetricDecommissionObject
	decomMetricDecommissionRemoveObject
)

func decomTrace(d decomMetric, poolIdx int, startTime time.Time, duration time.Duration, path string, err error) madmin.TraceInfo {
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	return madmin.TraceInfo{
		TraceType: madmin.TraceDecommission,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  fmt.Sprintf("decommission.%s (pool-id=%d)", d.String(), poolIdx),
		Duration:  duration,
		Path:      path,
		Error:     errStr,
	}
}

func (m *decomMetrics) log(d decomMetric, poolIdx int, paths ...string) func(err error) {
	startTime := time.Now()
	return func(err error) {
		duration := time.Since(startTime)
		if globalTrace.NumSubscribers(madmin.TraceDecommission) > 0 {
			globalTrace.Publish(decomTrace(d, poolIdx, startTime, duration, strings.Join(paths, " "), err))
		}
	}
}

func (z *erasureServerPools) decommissionInBackground(ctx context.Context, idx int) error {
	pool := z.serverPools[idx]
	for _, bucket := range z.poolMeta.PendingBuckets(idx) {
		if z.poolMeta.isBucketDecommissioned(idx, bucket.String()) {
			if serverDebugLog {
				console.Debugln("decommission: already done, moving on", bucket)
			}

			z.poolMetaMutex.Lock()
			z.poolMeta.BucketDone(idx, bucket) // remove from pendingBuckets and persist.
			z.poolMeta.save(ctx, z.serverPools)
			z.poolMetaMutex.Unlock()
			continue
		}
		if serverDebugLog {
			console.Debugln("decommission: currently on bucket", bucket.Name)
		}
		stopFn := globalDecommissionMetrics.log(decomMetricDecommissionBucket, idx, bucket.Name)
		if err := z.decommissionPool(ctx, idx, pool, bucket); err != nil {
			stopFn(err)
			return err
		}
		stopFn(nil)

		z.poolMetaMutex.Lock()
		z.poolMeta.BucketDone(idx, bucket)
		z.poolMeta.save(ctx, z.serverPools)
		z.poolMetaMutex.Unlock()
	}
	return nil
}

func (z *erasureServerPools) doDecommissionInRoutine(ctx context.Context, idx int) {
	z.poolMetaMutex.Lock()
	var dctx context.Context
	dctx, z.decommissionCancelers[idx] = context.WithCancel(GlobalContext)
	z.poolMetaMutex.Unlock()

	// Generate an empty request info so it can be directly modified later by audit
	dctx = logger.SetReqInfo(dctx, &logger.ReqInfo{})

	if err := z.decommissionInBackground(dctx, idx); err != nil {
		logger.LogIf(GlobalContext, err)
		logger.LogIf(GlobalContext, z.DecommissionFailed(dctx, idx))
		return
	}

	z.poolMetaMutex.Lock()
	failed := z.poolMeta.Pools[idx].Decommission.ItemsDecommissionFailed > 0
	z.poolMetaMutex.Unlock()

	if failed {
		// Decommission failed indicate as such.
		logger.LogIf(GlobalContext, z.DecommissionFailed(dctx, idx))
	} else {
		// Complete the decommission..
		logger.LogIf(GlobalContext, z.CompleteDecommission(dctx, idx))
	}
}

func (z *erasureServerPools) IsSuspended(idx int) bool {
	z.poolMetaMutex.RLock()
	defer z.poolMetaMutex.RUnlock()
	return z.poolMeta.IsSuspended(idx)
}

// Decommission - start decommission session.
func (z *erasureServerPools) Decommission(ctx context.Context, idx int) error {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	// Make pool unwritable before decommissioning.
	if err := z.StartDecommission(ctx, idx); err != nil {
		return err
	}

	go z.doDecommissionInRoutine(ctx, idx)

	// Successfully started decommissioning.
	return nil
}

type decomError struct {
	Err string
}

func (d decomError) Error() string {
	return d.Err
}

type poolSpaceInfo struct {
	Free  int64
	Total int64
	Used  int64
}

func (z *erasureServerPools) getDecommissionPoolSpaceInfo(idx int) (pi poolSpaceInfo, err error) {
	if idx < 0 {
		return pi, errInvalidArgument
	}
	if idx+1 > len(z.serverPools) {
		return pi, errInvalidArgument
	}

	info, _ := z.serverPools[idx].StorageInfo(context.Background())
	info.Backend = z.BackendInfo()

	usableTotal := int64(GetTotalUsableCapacity(info.Disks, info))
	usableFree := int64(GetTotalUsableCapacityFree(info.Disks, info))
	return poolSpaceInfo{
		Total: usableTotal,
		Free:  usableFree,
		Used:  usableTotal - usableFree,
	}, nil
}

func (z *erasureServerPools) Status(ctx context.Context, idx int) (PoolStatus, error) {
	if idx < 0 {
		return PoolStatus{}, errInvalidArgument
	}

	z.poolMetaMutex.RLock()
	defer z.poolMetaMutex.RUnlock()

	pi, err := z.getDecommissionPoolSpaceInfo(idx)
	if err != nil {
		return PoolStatus{}, err
	}

	poolInfo := z.poolMeta.Pools[idx]
	if poolInfo.Decommission != nil {
		poolInfo.Decommission.TotalSize = pi.Total
		poolInfo.Decommission.CurrentSize = poolInfo.Decommission.StartSize + poolInfo.Decommission.BytesDone
	} else {
		poolInfo.Decommission = &PoolDecommissionInfo{
			TotalSize:   pi.Total,
			CurrentSize: pi.Free,
		}
	}
	return poolInfo, nil
}

func (z *erasureServerPools) ReloadPoolMeta(ctx context.Context) (err error) {
	meta := poolMeta{}

	if err = meta.load(ctx, z.serverPools[0], z.serverPools); err != nil {
		return err
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	z.poolMeta = meta
	return nil
}

func (z *erasureServerPools) DecommissionCancel(ctx context.Context, idx int) (err error) {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	if z.poolMeta.DecommissionCancel(idx) {
		if fn := z.decommissionCancelers[idx]; fn != nil {
			defer fn() // cancel any active thread.
		}
		if err = z.poolMeta.save(ctx, z.serverPools); err != nil {
			return err
		}
		globalNotificationSys.ReloadPoolMeta(ctx)
	}
	return nil
}

func (z *erasureServerPools) DecommissionFailed(ctx context.Context, idx int) (err error) {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	if z.poolMeta.DecommissionFailed(idx) {
		if fn := z.decommissionCancelers[idx]; fn != nil {
			defer fn() // cancel any active thread.
		}
		if err = z.poolMeta.save(ctx, z.serverPools); err != nil {
			return err
		}
		globalNotificationSys.ReloadPoolMeta(ctx)
	}
	return nil
}

func (z *erasureServerPools) CompleteDecommission(ctx context.Context, idx int) (err error) {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	if z.poolMeta.DecommissionComplete(idx) {
		if fn := z.decommissionCancelers[idx]; fn != nil {
			defer fn() // cancel any active thread.
		}
		if err = z.poolMeta.save(ctx, z.serverPools); err != nil {
			return err
		}
		globalNotificationSys.ReloadPoolMeta(ctx)
	}
	return nil
}

func (z *erasureServerPools) StartDecommission(ctx context.Context, idx int) (err error) {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	buckets, err := z.ListBuckets(ctx, BucketOptions{})
	if err != nil {
		return err
	}

	// Make sure to heal the buckets to ensure the new
	// pool has the new buckets, this is to avoid
	// failures later.
	for _, bucket := range buckets {
		z.HealBucket(ctx, bucket.Name, madmin.HealOpts{})
	}

	decomBuckets := make([]decomBucketInfo, len(buckets))
	for i := range buckets {
		decomBuckets[i] = decomBucketInfo{
			Name: buckets[i].Name,
		}
	}

	// TODO: Support decommissioning transition tiers.
	for _, bucket := range decomBuckets {
		if lc, err := globalLifecycleSys.Get(bucket.Name); err == nil {
			if lc.HasTransition() {
				return decomError{
					Err: fmt.Sprintf("Bucket is part of transitioned tier %s: decommission is not allowed in Tier'd setups", bucket.Name),
				}
			}
		}
	}

	// Create .minio.sys/conifg, .minio.sys/buckets paths if missing,
	// this code is present to avoid any missing meta buckets on other
	// pools.
	for _, metaBucket := range []string{
		pathJoin(minioMetaBucket, minioConfigPrefix),
		pathJoin(minioMetaBucket, bucketMetaPrefix),
	} {
		var bucketExists BucketExists
		if err = z.MakeBucketWithLocation(ctx, metaBucket, MakeBucketOptions{}); err != nil {
			if !errors.As(err, &bucketExists) {
				return err
			}
		}
	}

	// Buckets data are dispersed in multiple zones/sets, make
	// sure to decommission the necessary metadata.
	decomBuckets = append(decomBuckets, decomBucketInfo{
		Name:   minioMetaBucket,
		Prefix: minioConfigPrefix,
	})
	decomBuckets = append(decomBuckets, decomBucketInfo{
		Name:   minioMetaBucket,
		Prefix: bucketMetaPrefix,
	})

	var pool *erasureSets
	for pidx := range z.serverPools {
		if pidx == idx {
			pool = z.serverPools[idx]
			break
		}
	}

	if pool == nil {
		return errInvalidArgument
	}

	pi, err := z.getDecommissionPoolSpaceInfo(idx)
	if err != nil {
		return err
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	if err = z.poolMeta.Decommission(idx, pi); err != nil {
		return err
	}
	z.poolMeta.QueueBuckets(idx, decomBuckets)
	if err = z.poolMeta.save(ctx, z.serverPools); err != nil {
		return err
	}
	globalNotificationSys.ReloadPoolMeta(ctx)
	return nil
}

func auditLogDecom(ctx context.Context, apiName, bucket, object, versionID string, err error) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	auditLogInternal(ctx, AuditLogOptions{
		Event:     "decommission",
		APIName:   apiName,
		Bucket:    bucket,
		Object:    object,
		VersionID: versionID,
		Error:     errStr,
	})
}
