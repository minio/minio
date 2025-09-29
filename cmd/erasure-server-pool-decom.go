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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/bucket/lifecycle"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/bucket/versioning"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/pkg/v3/env"
	"github.com/minio/pkg/v3/workers"
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
	ItemsDecommissioned     int64 `json:"objectsDecommissioned" msg:"id"`
	ItemsDecommissionFailed int64 `json:"objectsDecommissionedFailed" msg:"idf"`
	BytesDone               int64 `json:"bytesDecommissioned" msg:"bd"`
	BytesFailed             int64 `json:"bytesDecommissionedFailed" msg:"bf"`
}

// Clone make a copy of PoolDecommissionInfo
func (pd *PoolDecommissionInfo) Clone() *PoolDecommissionInfo {
	if pd == nil {
		return nil
	}
	return &PoolDecommissionInfo{
		StartTime:               pd.StartTime,
		StartSize:               pd.StartSize,
		TotalSize:               pd.TotalSize,
		CurrentSize:             pd.CurrentSize,
		Complete:                pd.Complete,
		Failed:                  pd.Failed,
		Canceled:                pd.Canceled,
		QueuedBuckets:           pd.QueuedBuckets,
		DecommissionedBuckets:   pd.DecommissionedBuckets,
		Bucket:                  pd.Bucket,
		Prefix:                  pd.Prefix,
		Object:                  pd.Object,
		ItemsDecommissioned:     pd.ItemsDecommissioned,
		ItemsDecommissionFailed: pd.ItemsDecommissionFailed,
		BytesDone:               pd.BytesDone,
		BytesFailed:             pd.BytesFailed,
	}
}

// bucketPop should be called when a bucket is done decommissioning.
// Adds the bucket to the list of decommissioned buckets and updates resume numbers.
func (pd *PoolDecommissionInfo) bucketPop(bucket string) bool {
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
			return true
		}
	}
	return false
}

func (pd *PoolDecommissionInfo) isBucketDecommissioned(bucket string) bool {
	return slices.Contains(pd.DecommissionedBuckets, bucket)
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

// Clone returns a copy of PoolStatus
func (ps PoolStatus) Clone() PoolStatus {
	return PoolStatus{
		ID:           ps.ID,
		CmdLine:      ps.CmdLine,
		LastUpdate:   ps.LastUpdate,
		Decommission: ps.Decommission.Clone(),
	}
}

//go:generate msgp -file $GOFILE -unexported
type poolMeta struct {
	Version int          `msg:"v"`
	Pools   []PoolStatus `msg:"pls"`

	// Value should not be saved when we have not loaded anything yet.
	dontSave bool `msg:"-"`
}

// A decommission resumable tells us if decommission is worth
// resuming upon restart of a cluster.
func (p *poolMeta) returnResumablePools() []PoolStatus {
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
	}
	return newPools
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

func (p *poolMeta) BucketDone(idx int, bucket decomBucketInfo) bool {
	if p.Pools[idx].Decommission == nil {
		// Decommission not in progress.
		return false
	}
	return p.Pools[idx].Decommission.bucketPop(bucket.String())
}

func (p poolMeta) ResumeBucketObject(idx int) (bucket, object string) {
	if p.Pools[idx].Decommission != nil {
		bucket = p.Pools[idx].Decommission.Bucket
		object = p.Pools[idx].Decommission.Object
	}
	return bucket, object
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
	errDecommissionNotStarted     = errors.New("decommission is not in progress")
)

func (p *poolMeta) Decommission(idx int, pi poolSpaceInfo) error {
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
	if idx >= len(p.Pools) {
		// We don't really know if the pool is suspended or not, since it doesn't exist.
		return false
	}
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

	var update bool
	// Check if specified pools need to be removed from decommissioned pool.
	for k := range specifiedPools {
		pi, ok := rememberedPools[k]
		if !ok {
			// we do not have the pool anymore that we previously remembered, since all
			// the CLI checks out we can allow updates since we are mostly adding a pool here.
			update = true
		}
		if ok && pi.completed {
			logger.LogIf(GlobalContext, "decommission", fmt.Errorf("pool(%s) = %s is decommissioned, please remove from server command line", humanize.Ordinal(pi.position+1), k))
		}
	}

	if len(specifiedPools) == len(rememberedPools) {
		for k, pi := range rememberedPools {
			pos, ok := specifiedPools[k]
			if ok && pos != pi.position {
				update = true // pool order is changing, its okay to allow it.
			}
		}
	}

	if !update {
		update = len(specifiedPools) != len(rememberedPools)
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
	if pd == nil {
		return
	}
	if failed {
		pd.ItemsDecommissionFailed++
		pd.BytesFailed += size
	} else {
		pd.ItemsDecommissioned++
		pd.BytesDone += size
	}
	p.Pools[idx].Decommission = pd
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
	if p.dontSave {
		return nil
	}
	data := make([]byte, 4, p.Msgsize()+4)

	// Initialize the header.
	binary.LittleEndian.PutUint16(data[0:2], poolMetaFormat)
	binary.LittleEndian.PutUint16(data[2:4], poolMetaVersion)

	buf, err := p.MarshalMsg(data)
	if err != nil {
		return err
	}

	// Saves on all pools to make sure decommissioning of first pool is allowed.
	for i, eset := range pools {
		if err = saveConfig(ctx, eset, poolMetaName, buf); err != nil {
			if !errors.Is(err, context.Canceled) {
				storageLogIf(ctx, fmt.Errorf("saving pool.bin for pool index %d failed with: %v", i, err))
			}
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
	if err := z.loadRebalanceMeta(ctx); err == nil {
		// Start rebalance routine if we can reload rebalance metadata.
		z.StartRebalance()
	}

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
		z.poolMetaMutex.Lock()
		z.poolMeta = meta
		z.poolMetaMutex.Unlock()
	} else {
		newMeta := newPoolMeta(z, meta)
		if err = newMeta.save(ctx, z.serverPools); err != nil {
			return err
		}
		z.poolMetaMutex.Lock()
		z.poolMeta = newMeta
		z.poolMetaMutex.Unlock()
	}

	pools := meta.returnResumablePools()
	poolIndices := make([]int, 0, len(pools))
	for _, pool := range pools {
		idx := globalEndpoints.GetPoolIdx(pool.CmdLine)
		if idx == -1 {
			return fmt.Errorf("unexpected state present for decommission status pool(%s) not found", pool.CmdLine)
		}
		poolIndices = append(poolIndices, idx)
	}

	if len(poolIndices) > 0 && globalEndpoints[poolIndices[0]].Endpoints[0].IsLocal {
		go func() {
			// Resume decommissioning of pools, but wait 3 minutes for cluster to stabilize.
			if err := sleepContext(ctx, 3*time.Minute); err != nil {
				return
			}
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				if err := z.Decommission(ctx, poolIndices...); err != nil {
					if errors.Is(err, errDecommissionAlreadyRunning) {
						// A previous decommission running found restart it.
						for _, idx := range poolIndices {
							z.doDecommissionInRoutine(ctx, idx)
						}
						return
					}
					if configRetriableErrors(err) {
						decomLogIf(ctx, fmt.Errorf("Unable to resume decommission of pools %v: %w: retrying..", pools, err))
						time.Sleep(time.Second + time.Duration(r.Float64()*float64(5*time.Second)))
						continue
					}
					decomLogIf(ctx, fmt.Errorf("Unable to resume decommission of pool %v: %w", pools, err))
					return
				}
			}
		}()
	}

	return nil
}

func newPoolMeta(z *erasureServerPools, prevMeta poolMeta) poolMeta {
	newMeta := poolMeta{} // to update write poolMeta fresh.
	// looks like new pool was added we need to update,
	// or this is a fresh installation (or an existing
	// installation with pool removed)
	newMeta.Version = poolMetaVersion
	for idx, pool := range z.serverPools {
		var skip bool
		for _, currentPool := range prevMeta.Pools {
			// Preserve any current pool status.
			if currentPool.CmdLine == pool.endpoints.CmdLine {
				currentPool.ID = idx
				newMeta.Pools = append(newMeta.Pools, currentPool)
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		newMeta.Pools = append(newMeta.Pools, PoolStatus{
			CmdLine:    pool.endpoints.CmdLine,
			ID:         idx,
			LastUpdate: UTCNow(),
		})
	}
	return newMeta
}

func (z *erasureServerPools) IsDecommissionRunning() bool {
	z.poolMetaMutex.RLock()
	defer z.poolMetaMutex.RUnlock()
	meta := z.poolMeta
	for _, pool := range meta.Pools {
		if pool.Decommission != nil &&
			!pool.Decommission.Complete &&
			!pool.Decommission.Failed &&
			!pool.Decommission.Canceled {
			return true
		}
	}

	return false
}

func (z *erasureServerPools) decommissionObject(ctx context.Context, idx int, bucket string, gr *GetObjectReader) (err error) {
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
			VersionID:    objInfo.VersionID,
			UserDefined:  objInfo.UserDefined,
			NoAuditLog:   true,
			SrcPoolIdx:   idx,
			DataMovement: true,
		})
		if err != nil {
			return fmt.Errorf("decommissionObject: NewMultipartUpload() %w", err)
		}
		defer z.AbortMultipartUpload(ctx, bucket, objInfo.Name, res.UploadID, ObjectOptions{NoAuditLog: true})
		parts := make([]CompletePart, len(objInfo.Parts))
		for i, part := range objInfo.Parts {
			hr, err := hash.NewReader(ctx, io.LimitReader(gr, part.Size), part.Size, "", "", part.ActualSize)
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
					NoAuditLog: true,
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
			SrcPoolIdx:   idx,
			DataMovement: true,
			MTime:        objInfo.ModTime,
			NoAuditLog:   true,
		})
		if err != nil {
			err = fmt.Errorf("decommissionObject: CompleteMultipartUpload() %w", err)
		}
		return err
	}

	hr, err := hash.NewReader(ctx, io.LimitReader(gr, objInfo.Size), objInfo.Size, "", "", actualSize)
	if err != nil {
		return fmt.Errorf("decommissionObject: hash.NewReader() %w", err)
	}

	_, err = z.PutObject(ctx,
		bucket,
		objInfo.Name,
		NewPutObjReader(hr),
		ObjectOptions{
			DataMovement: true,
			SrcPoolIdx:   idx,
			VersionID:    objInfo.VersionID,
			MTime:        objInfo.ModTime,
			UserDefined:  objInfo.UserDefined,
			PreserveETag: objInfo.ETag, // Preserve original ETag to ensure same metadata.
			IndexCB: func() []byte {
				return objInfo.Parts[0].Index // Preserve part Index to ensure decompression works.
			},
			NoAuditLog: true,
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

func (set *erasureObjects) listObjectsToDecommission(ctx context.Context, bi decomBucketInfo, fn func(entry metaCacheEntry)) error {
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
		bucket:    bi.Name,
	}

	err := listPathRaw(ctx, listPathRawOptions{
		disks:          disks,
		bucket:         bi.Name,
		path:           bi.Prefix,
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

func (z *erasureServerPools) decommissionPool(ctx context.Context, idx int, pool *erasureSets, bi decomBucketInfo) error {
	ctx = logger.SetReqInfo(ctx, &logger.ReqInfo{})

	const envDecomWorkers = "_MINIO_DECOMMISSION_WORKERS"
	workerSize, err := env.GetInt(envDecomWorkers, len(pool.sets))
	if err != nil {
		decomLogIf(ctx, fmt.Errorf("invalid workers value err: %v, defaulting to %d", err, len(pool.sets)))
		workerSize = len(pool.sets)
	}

	// Each decom worker needs one List() goroutine/worker
	// add that many extra workers.
	workerSize += len(pool.sets)

	wk, err := workers.New(workerSize)
	if err != nil {
		return err
	}

	var vc *versioning.Versioning
	var lc *lifecycle.Lifecycle
	var lr objectlock.Retention
	var rcfg *replication.Config
	if bi.Name != minioMetaBucket {
		vc, err = globalBucketVersioningSys.Get(bi.Name)
		if err != nil {
			return err
		}

		// Check if the current bucket has a configured lifecycle policy
		lc, err = globalLifecycleSys.Get(bi.Name)
		if err != nil && !errors.Is(err, BucketLifecycleNotFound{Bucket: bi.Name}) {
			return err
		}

		// Check if bucket is object locked.
		lr, err = globalBucketObjectLockSys.Get(bi.Name)
		if err != nil {
			return err
		}

		rcfg, err = getReplicationConfig(ctx, bi.Name)
		if err != nil {
			return err
		}
	}

	for setIdx, set := range pool.sets {
		filterLifecycle := func(bucket, object string, fi FileInfo) bool {
			if lc == nil {
				return false
			}
			versioned := vc != nil && vc.Versioned(object)
			objInfo := fi.ToObjectInfo(bucket, object, versioned)

			evt := evalActionFromLifecycle(ctx, *lc, lr, rcfg, objInfo)
			switch {
			case evt.Action.DeleteRestored(): // if restored copy has expired, delete it synchronously
				applyExpiryOnTransitionedObject(ctx, z, objInfo, evt, lcEventSrc_Decom)
				return false
			case evt.Action.Delete():
				globalExpiryState.enqueueByDays(objInfo, evt, lcEventSrc_Decom)
				return true
			default:
				return false
			}
		}

		decommissionEntry := func(entry metaCacheEntry) {
			defer wk.Give()

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

			var decommissioned, expired int
			for _, version := range fivs.Versions {
				stopFn := globalDecommissionMetrics.log(decomMetricDecommissionObject, idx, bi.Name, version.Name, version.VersionID)
				// Apply lifecycle rules on the objects that are expired.
				if filterLifecycle(bi.Name, version.Name, version) {
					expired++
					decommissioned++
					stopFn(version.Size, errors.New("ILM expired object/version will be skipped"))
					continue
				}

				// any object with only single DEL marker we don't need
				// to decommission, just skip it, this also includes
				// any other versions that have already expired.
				remainingVersions := len(fivs.Versions) - expired
				if version.Deleted && remainingVersions == 1 && rcfg == nil {
					decommissioned++
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
						bi.Name,
						version.Name,
						ObjectOptions{
							// Since we are preserving a delete marker, we have to make sure this is always true.
							// regardless of the current configuration of the bucket we must preserve all versions
							// on the pool being decommissioned.
							Versioned:          true,
							VersionID:          versionID,
							MTime:              version.ModTime,
							DeleteReplication:  version.ReplicationState,
							SrcPoolIdx:         idx,
							DataMovement:       true,
							DeleteMarker:       true, // make sure we create a delete marker
							SkipDecommissioned: true, // make sure we skip the decommissioned pool
							NoAuditLog:         true,
						})
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
					if err != nil {
						decomLogIf(ctx, err)
						failure = true
					}
					z.poolMetaMutex.Lock()
					z.poolMeta.CountItem(idx, 0, failure)
					z.poolMetaMutex.Unlock()
					if !failure {
						// Success keep a count.
						decommissioned++
					}
					auditLogDecom(ctx, "DecomCopyDeleteMarker", bi.Name, version.Name, versionID, err)
					continue
				}

				// gr.Close() is ensured by decommissionObject().
				for range 3 {
					if version.IsRemote() {
						if err := z.DecomTieredObject(ctx, bi.Name, version.Name, version, ObjectOptions{
							VersionID:    versionID,
							MTime:        version.ModTime,
							UserDefined:  version.Metadata,
							SrcPoolIdx:   idx,
							DataMovement: true,
						}); err != nil {
							if isErrObjectNotFound(err) || isErrVersionNotFound(err) || isDataMovementOverWriteErr(err) {
								ignore = true
								stopFn(0, nil)
							}
						}
						if !ignore {
							stopFn(version.Size, err)
							failure = err != nil
							decomLogIf(ctx, err)
						}
						break
					}
					gr, err := set.GetObjectNInfo(ctx,
						bi.Name,
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
					if err != nil && !ignore {
						// if usage-cache.bin is not readable log and ignore it.
						if bi.Name == minioMetaBucket && strings.Contains(version.Name, dataUsageCacheName) {
							ignore = true
							stopFn(version.Size, err)
							decomLogIf(ctx, err)
							break
						}
					}
					if err != nil {
						failure = true
						decomLogIf(ctx, err)
						stopFn(version.Size, err)
						continue
					}
					if err = z.decommissionObject(ctx, idx, bi.Name, gr); err != nil {
						if isErrObjectNotFound(err) || isErrVersionNotFound(err) || isDataMovementOverWriteErr(err) {
							ignore = true
							stopFn(0, nil)
							break
						}
						stopFn(version.Size, err)
						failure = true
						decomLogIf(ctx, err)
						continue
					}
					stopFn(version.Size, nil)
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
				decommissioned++
			}

			// if all versions were decommissioned, then we can delete the object versions.
			if decommissioned == len(fivs.Versions) {
				stopFn := globalDecommissionMetrics.log(decomMetricDecommissionRemoveObject, idx, bi.Name, entry.name)
				_, err := set.DeleteObject(ctx,
					bi.Name,
					encodeDirObject(entry.name),
					ObjectOptions{
						DeletePrefix:       true, // use prefix delete to delete all versions at once.
						DeletePrefixObject: true, // use prefix delete on exact object (this is an optimization to avoid fan-out calls)
						NoAuditLog:         true,
					},
				)
				stopFn(0, err)
				auditLogDecom(ctx, "DecomDeleteObject", bi.Name, entry.name, "", err)
				if err != nil {
					decomLogIf(ctx, err)
				}
			}
			z.poolMetaMutex.Lock()
			z.poolMeta.TrackCurrentBucketObject(idx, bi.Name, entry.name)
			ok, err := z.poolMeta.updateAfter(ctx, idx, z.serverPools, 30*time.Second)
			decomLogIf(ctx, err)
			if ok {
				globalNotificationSys.ReloadPoolMeta(ctx)
			}
			z.poolMetaMutex.Unlock()
		}

		wk.Take()
		go func(setIdx int) {
			defer wk.Give()
			// We will perpetually retry listing if it fails, since we cannot
			// possibly give up in this matter
			for !contextCanceled(ctx) {
				err := set.listObjectsToDecommission(ctx, bi,
					func(entry metaCacheEntry) {
						wk.Take()
						go decommissionEntry(entry)
					},
				)
				if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, errVolumeNotFound) {
					break
				}
				setN := humanize.Ordinal(setIdx + 1)
				retryDur := time.Duration(rand.Float64() * float64(5*time.Second))
				decomLogOnceIf(ctx, fmt.Errorf("listing objects from %s set failed with %v, retrying in %v", setN, err, retryDur), "decom-listing-failed"+setN)
				time.Sleep(retryDur)
			}
		}(setIdx)
	}
	wk.Wait()
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

func decomTrace(d decomMetric, poolIdx int, startTime time.Time, duration time.Duration, path string, err error, sz int64) madmin.TraceInfo {
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
		Bytes:     sz,
	}
}

func (m *decomMetrics) log(d decomMetric, poolIdx int, paths ...string) func(z int64, err error) {
	startTime := time.Now()
	return func(sz int64, err error) {
		duration := time.Since(startTime)
		if globalTrace.NumSubscribers(madmin.TraceDecommission) > 0 {
			globalTrace.Publish(decomTrace(d, poolIdx, startTime, duration, strings.Join(paths, " "), err, sz))
		}
	}
}

func (z *erasureServerPools) decommissionInBackground(ctx context.Context, idx int) error {
	pool := z.serverPools[idx]
	z.poolMetaMutex.RLock()
	pending := z.poolMeta.PendingBuckets(idx)
	z.poolMetaMutex.RUnlock()

	for _, bucket := range pending {
		z.poolMetaMutex.RLock()
		isDecommissioned := z.poolMeta.isBucketDecommissioned(idx, bucket.String())
		z.poolMetaMutex.RUnlock()
		if isDecommissioned {
			if serverDebugLog {
				console.Debugln("decommission: already done, moving on", bucket)
			}

			z.poolMetaMutex.Lock()
			if z.poolMeta.BucketDone(idx, bucket) {
				// remove from pendingBuckets and persist.
				decomLogIf(ctx, z.poolMeta.save(ctx, z.serverPools))
			}
			z.poolMetaMutex.Unlock()
			continue
		}
		if serverDebugLog {
			console.Debugln("decommission: currently on bucket", bucket.Name)
		}
		stopFn := globalDecommissionMetrics.log(decomMetricDecommissionBucket, idx, bucket.Name)
		if err := z.decommissionPool(ctx, idx, pool, bucket); err != nil {
			stopFn(0, err)
			return err
		}
		stopFn(0, nil)

		z.poolMetaMutex.Lock()
		if z.poolMeta.BucketDone(idx, bucket) {
			decomLogIf(ctx, z.poolMeta.save(ctx, z.serverPools))
		}
		z.poolMetaMutex.Unlock()
	}
	return nil
}

func (z *erasureServerPools) checkAfterDecom(ctx context.Context, idx int) error {
	buckets, err := z.getBucketsToDecommission(ctx)
	if err != nil {
		return err
	}

	pool := z.serverPools[idx]
	for _, set := range pool.sets {
		for _, bi := range buckets {
			var vc *versioning.Versioning
			var lc *lifecycle.Lifecycle
			var lr objectlock.Retention
			var rcfg *replication.Config
			if bi.Name != minioMetaBucket {
				vc, err = globalBucketVersioningSys.Get(bi.Name)
				if err != nil {
					return err
				}

				// Check if the current bucket has a configured lifecycle policy
				lc, err = globalLifecycleSys.Get(bi.Name)
				if err != nil && !errors.Is(err, BucketLifecycleNotFound{Bucket: bi.Name}) {
					return err
				}

				// Check if bucket is object locked.
				lr, err = globalBucketObjectLockSys.Get(bi.Name)
				if err != nil {
					return err
				}

				rcfg, err = getReplicationConfig(ctx, bi.Name)
				if err != nil {
					return err
				}
			}

			filterLifecycle := func(bucket, object string, fi FileInfo) bool {
				if lc == nil {
					return false
				}
				versioned := vc != nil && vc.Versioned(object)
				objInfo := fi.ToObjectInfo(bucket, object, versioned)

				evt := evalActionFromLifecycle(ctx, *lc, lr, rcfg, objInfo)
				switch {
				case evt.Action.DeleteRestored(): // if restored copy has expired,delete it synchronously
					applyExpiryOnTransitionedObject(ctx, z, objInfo, evt, lcEventSrc_Decom)
					return false
				case evt.Action.Delete():
					globalExpiryState.enqueueByDays(objInfo, evt, lcEventSrc_Decom)
					return true
				default:
					return false
				}
			}

			var versionsFound int
			if err = set.listObjectsToDecommission(ctx, bi, func(entry metaCacheEntry) {
				if !entry.isObject() {
					return
				}

				// `.usage-cache.bin` still exists, must be not readable ignore it.
				if bi.Name == minioMetaBucket && strings.Contains(entry.name, dataUsageCacheName) {
					// skipping bucket usage cache name, as its autogenerated.
					return
				}

				fivs, err := entry.fileInfoVersions(bi.Name)
				if err != nil {
					return
				}

				var ignored int

				for _, version := range fivs.Versions {
					// Apply lifecycle rules on the objects that are expired.
					if filterLifecycle(bi.Name, version.Name, version) {
						ignored++
						continue
					}
					if version.Deleted {
						ignored++
						continue
					}
				}

				versionsFound += len(fivs.Versions) - ignored
			}); err != nil {
				return err
			}

			if versionsFound > 0 {
				return fmt.Errorf("at least %d object(s)/version(s) were found in bucket `%s` after decommissioning", versionsFound, bi.Name)
			}
		}
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
		decomLogIf(GlobalContext, err)
		decomLogIf(GlobalContext, z.DecommissionFailed(dctx, idx))
		return
	}

	z.poolMetaMutex.Lock()
	failed := z.poolMeta.Pools[idx].Decommission.ItemsDecommissionFailed > 0 || contextCanceled(dctx)
	poolCmdLine := z.poolMeta.Pools[idx].CmdLine
	z.poolMetaMutex.Unlock()

	if !failed {
		decomLogEvent(dctx, "Decommissioning complete for pool '%s', verifying for any pending objects", poolCmdLine)
		err := z.checkAfterDecom(dctx, idx)
		if err != nil {
			decomLogIf(ctx, err)
			failed = true
		}
	}

	if failed {
		// Decommission failed indicate as such.
		decomLogIf(GlobalContext, z.DecommissionFailed(dctx, idx))
	} else {
		// Complete the decommission..
		decomLogIf(GlobalContext, z.CompleteDecommission(dctx, idx))
	}
}

func (z *erasureServerPools) IsSuspended(idx int) bool {
	z.poolMetaMutex.RLock()
	defer z.poolMetaMutex.RUnlock()
	return z.poolMeta.IsSuspended(idx)
}

// Decommission - start decommission session.
func (z *erasureServerPools) Decommission(ctx context.Context, indices ...int) error {
	if len(indices) == 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	// Make pool unwritable before decommissioning.
	if err := z.StartDecommission(ctx, indices...); err != nil {
		return err
	}

	go func() {
		for _, idx := range indices {
			// decommission all pools serially one after
			// the other.
			z.doDecommissionInRoutine(ctx, idx)
		}
	}()

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

	info := z.serverPools[idx].StorageInfo(context.Background())
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

	pi, err := z.getDecommissionPoolSpaceInfo(idx)
	if err != nil {
		return PoolStatus{}, err
	}

	z.poolMetaMutex.RLock()
	defer z.poolMetaMutex.RUnlock()

	poolInfo := z.poolMeta.Pools[idx].Clone()
	if poolInfo.Decommission != nil {
		poolInfo.Decommission.TotalSize = pi.Total
		poolInfo.Decommission.CurrentSize = pi.Free
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

	fn := z.decommissionCancelers[idx]
	if fn == nil {
		// canceling a decommission before it started return an error.
		return errDecommissionNotStarted
	}

	defer fn() // cancel any active thread.

	if z.poolMeta.DecommissionCancel(idx) {
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
			defer fn()
		} // cancel any active thread.

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
			defer fn()
		} // cancel any active thread.

		if err = z.poolMeta.save(ctx, z.serverPools); err != nil {
			return err
		}
		globalNotificationSys.ReloadPoolMeta(ctx)
	}
	return nil
}

func (z *erasureServerPools) getBucketsToDecommission(ctx context.Context) ([]decomBucketInfo, error) {
	buckets, err := z.ListBuckets(ctx, BucketOptions{})
	if err != nil {
		return nil, err
	}

	decomBuckets := make([]decomBucketInfo, len(buckets))
	for i := range buckets {
		decomBuckets[i] = decomBucketInfo{
			Name: buckets[i].Name,
		}
	}

	// Buckets data are dispersed in multiple zones/sets, make
	// sure to decommission the necessary metadata.
	decomMetaBuckets := []decomBucketInfo{
		{
			Name:   minioMetaBucket,
			Prefix: minioConfigPrefix,
		},
		{
			Name:   minioMetaBucket,
			Prefix: bucketMetaPrefix,
		},
	}

	return append(decomMetaBuckets, decomBuckets...), nil
}

func (z *erasureServerPools) StartDecommission(ctx context.Context, indices ...int) (err error) {
	if len(indices) == 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	decomBuckets, err := z.getBucketsToDecommission(ctx)
	if err != nil {
		return err
	}

	for _, bucket := range decomBuckets {
		z.HealBucket(ctx, bucket.Name, madmin.HealOpts{})
	}

	// Create .minio.sys/config, .minio.sys/buckets paths if missing,
	// this code is present to avoid any missing meta buckets on other
	// pools.
	for _, metaBucket := range []string{
		pathJoin(minioMetaBucket, minioConfigPrefix),
		pathJoin(minioMetaBucket, bucketMetaPrefix),
	} {
		var bucketExists BucketExists
		if err = z.MakeBucket(ctx, metaBucket, MakeBucketOptions{}); err != nil {
			if !errors.As(err, &bucketExists) {
				return err
			}
		}
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	for _, idx := range indices {
		pi, err := z.getDecommissionPoolSpaceInfo(idx)
		if err != nil {
			return err
		}

		if err = z.poolMeta.Decommission(idx, pi); err != nil {
			return err
		}

		z.poolMeta.QueueBuckets(idx, decomBuckets)
	}

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
