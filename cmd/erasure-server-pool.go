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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/bpool"
	"github.com/minio/minio/internal/cachevalue"
	"github.com/minio/minio/internal/config/storageclass"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/sync/errgroup"
	"github.com/minio/pkg/v3/wildcard"
	"github.com/minio/pkg/v3/workers"
	"github.com/puzpuzpuz/xsync/v3"
)

type erasureServerPools struct {
	poolMetaMutex sync.RWMutex
	poolMeta      poolMeta

	rebalMu   sync.RWMutex
	rebalMeta *rebalanceMeta

	deploymentID     [16]byte
	distributionAlgo string

	serverPools []*erasureSets

	// Active decommission canceler
	decommissionCancelers []context.CancelFunc

	s3Peer *S3PeerSys

	mpCache *xsync.MapOf[string, MultipartInfo]
}

func (z *erasureServerPools) SinglePool() bool {
	return len(z.serverPools) == 1
}

// Initialize new pool of erasure sets.
func newErasureServerPools(ctx context.Context, endpointServerPools EndpointServerPools) (ObjectLayer, error) {
	var (
		deploymentID       string
		commonParityDrives int
		err                error

		formats      = make([]*formatErasureV3, len(endpointServerPools))
		storageDisks = make([][]StorageAPI, len(endpointServerPools))
		z            = &erasureServerPools{
			serverPools:      make([]*erasureSets, len(endpointServerPools)),
			s3Peer:           NewS3PeerSys(endpointServerPools),
			distributionAlgo: formatErasureVersionV3DistributionAlgoV3,
		}
	)

	// Maximum number of reusable buffers per node at any given point in time.
	n := uint64(1024) // single node single/multiple drives set this to 1024 entries

	if globalIsDistErasure {
		n = 2048
	}

	// Avoid allocating more than half of the available memory
	if maxN := availableMemory() / (blockSizeV2 * 2); n > maxN {
		n = maxN
	}

	if globalIsCICD || strconv.IntSize == 32 {
		n = 256 // 256MiB for CI/CD environments is sufficient or on 32bit platforms.
	}

	// Initialize byte pool once for all sets, bpool size is set to
	// setCount * setDriveCount with each memory upto blockSizeV2.
	buffers := bpool.NewBytePoolCap(n, blockSizeV2, blockSizeV2*2)
	if n >= 16384 {
		// pre-populate buffers only n >= 16384 which is (32Gi/2Mi)
		// for all setups smaller than this avoid pre-alloc.
		buffers.Populate()
	}
	globalBytePoolCap.Store(buffers)

	var localDrives []StorageAPI
	local := endpointServerPools.FirstLocal()
	for i, ep := range endpointServerPools {
		// If storage class is not set during startup, default values are used
		// -- Default for Reduced Redundancy Storage class is, parity = 2
		// -- Default for Standard Storage class is, parity = 2 - disks 4, 5
		// -- Default for Standard Storage class is, parity = 3 - disks 6, 7
		// -- Default for Standard Storage class is, parity = 4 - disks 8 to 16
		if commonParityDrives == 0 {
			commonParityDrives, err = ecDrivesNoConfig(ep.DrivesPerSet)
			if err != nil {
				return nil, err
			}
		}

		if err = storageclass.ValidateParity(commonParityDrives, ep.DrivesPerSet); err != nil {
			return nil, fmt.Errorf("parity validation returned an error: %w <- (%d, %d), for pool(%s)", err, commonParityDrives, ep.DrivesPerSet, humanize.Ordinal(i+1))
		}

		bootstrapTrace("waitForFormatErasure: loading disks", func() {
			storageDisks[i], formats[i], err = waitForFormatErasure(local, ep.Endpoints, i+1,
				ep.SetCount, ep.DrivesPerSet, deploymentID)
		})
		if err != nil {
			return nil, err
		}

		if deploymentID == "" {
			// all pools should have same deployment ID
			deploymentID = formats[i].ID
		}

		// Validate if users brought different DeploymentID pools.
		if deploymentID != formats[i].ID {
			return nil, fmt.Errorf("all pools must have same deployment ID - expected %s, got %s for pool(%s)", deploymentID, formats[i].ID, humanize.Ordinal(i+1))
		}

		bootstrapTrace(fmt.Sprintf("newErasureSets: initializing %s pool", humanize.Ordinal(i+1)), func() {
			z.serverPools[i], err = newErasureSets(ctx, ep, storageDisks[i], formats[i], commonParityDrives, i)
		})
		if err != nil {
			return nil, err
		}

		if deploymentID != "" && bytes.Equal(z.deploymentID[:], []byte{}) {
			z.deploymentID = uuid.MustParse(deploymentID)
		}

		for _, storageDisk := range storageDisks[i] {
			if storageDisk != nil && storageDisk.IsLocal() {
				localDrives = append(localDrives, storageDisk)
			}
		}
	}

	if !globalIsDistErasure {
		globalLocalDrivesMu.Lock()
		globalLocalDrivesMap = make(map[string]StorageAPI, len(localDrives))
		for _, drive := range localDrives {
			globalLocalDrivesMap[drive.Endpoint().String()] = drive
		}
		globalLocalDrivesMu.Unlock()
	}

	z.decommissionCancelers = make([]context.CancelFunc, len(z.serverPools))

	// Initialize the pool meta, but set it to not save.
	// When z.Init below has loaded the poolmeta will be initialized,
	// and allowed to save.
	z.poolMeta = newPoolMeta(z, poolMeta{})
	z.poolMeta.dontSave = true

	bootstrapTrace("newSharedLock", func() {
		globalLeaderLock = newSharedLock(GlobalContext, z, "leader.lock")
	})

	// Start self healing after the object initialization
	// so various tasks will be useful
	bootstrapTrace("initAutoHeal", func() {
		initAutoHeal(GlobalContext, z)
	})

	// initialize the object layer.
	defer setObjectLayer(z)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	attempt := 1
	for {
		var err error
		bootstrapTrace(fmt.Sprintf("poolMeta.Init: loading pool metadata, attempt: %d", attempt), func() {
			err = z.Init(ctx) // Initializes all pools.
		})
		if err != nil {
			if !configRetriableErrors(err) {
				logger.Fatal(err, "Unable to initialize backend")
			}
			retry := time.Duration(r.Float64() * float64(5*time.Second))
			storageLogIf(ctx, fmt.Errorf("Unable to initialize backend: %w, retrying in %s", err, retry))
			time.Sleep(retry)
			attempt++
			continue
		}
		break
	}

	// initialize the incomplete uploads cache
	z.mpCache = xsync.NewMapOf[string, MultipartInfo]()

	go z.cleanupStaleMPCache(ctx)

	return z, nil
}

func (z *erasureServerPools) cleanupStaleMPCache(ctx context.Context) {
	timer := time.NewTimer(globalAPIConfig.getStaleUploadsCleanupInterval())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			z.mpCache.Range(func(id string, info MultipartInfo) bool {
				if time.Since(info.Initiated) >= globalAPIConfig.getStaleUploadsExpiry() {
					z.mpCache.Delete(id)
					// No need to notify to peers, each node will delete its own cache.
				}
				return true
			})

			// Reset for the next interval
			timer.Reset(globalAPIConfig.getStaleUploadsCleanupInterval())
		}
	}
}

func (z *erasureServerPools) NewNSLock(bucket string, objects ...string) RWLocker {
	return z.serverPools[0].NewNSLock(bucket, objects...)
}

// GetDisksID will return disks by their ID.
func (z *erasureServerPools) GetDisksID(ids ...string) []StorageAPI {
	idMap := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		idMap[id] = struct{}{}
	}
	res := make([]StorageAPI, 0, len(idMap))
	for _, s := range z.serverPools {
		for _, set := range s.sets {
			for _, disk := range set.getDisks() {
				if disk == OfflineDisk {
					continue
				}
				if id, _ := disk.GetDiskID(); id != "" {
					if _, ok := idMap[id]; ok {
						res = append(res, disk)
					}
				}
			}
		}
	}
	return res
}

// GetRawData will return all files with a given raw path to the callback.
// Errors are ignored, only errors from the callback are returned.
// For now only direct file paths are supported.
func (z *erasureServerPools) GetRawData(ctx context.Context, volume, file string, fn func(r io.Reader, host string, disk string, filename string, info StatInfo) error) error {
	found := 0
	for _, s := range z.serverPools {
		for _, set := range s.sets {
			for _, disk := range set.getDisks() {
				if disk == OfflineDisk {
					continue
				}
				stats, err := disk.StatInfoFile(ctx, volume, file, true)
				if err != nil {
					continue
				}
				for _, si := range stats {
					found++
					var r io.ReadCloser
					if !si.Dir {
						r, err = disk.ReadFileStream(ctx, volume, si.Name, 0, si.Size)
						if err != nil {
							continue
						}
					} else {
						r = io.NopCloser(bytes.NewBuffer([]byte{}))
					}
					// Keep disk path instead of ID, to ensure that the downloaded zip file can be
					// easily automated with `minio server hostname{1...n}/disk{1...m}`.
					err = fn(r, disk.Hostname(), disk.Endpoint().Path, pathJoin(volume, si.Name), si)
					r.Close()
					if err != nil {
						return err
					}
				}
			}
		}
	}
	if found == 0 {
		return errFileNotFound
	}
	return nil
}

// Return the disks belonging to the poolIdx, and setIdx.
func (z *erasureServerPools) GetDisks(poolIdx, setIdx int) ([]StorageAPI, error) {
	if poolIdx < len(z.serverPools) && setIdx < len(z.serverPools[poolIdx].sets) {
		return z.serverPools[poolIdx].sets[setIdx].getDisks(), nil
	}
	return nil, fmt.Errorf("Matching pool %s, set %s not found", humanize.Ordinal(poolIdx+1), humanize.Ordinal(setIdx+1))
}

// Return the count of disks in each pool
func (z *erasureServerPools) SetDriveCounts() []int {
	setDriveCounts := make([]int, len(z.serverPools))
	for i := range z.serverPools {
		setDriveCounts[i] = z.serverPools[i].SetDriveCount()
	}
	return setDriveCounts
}

type serverPoolsAvailableSpace []poolAvailableSpace

type poolAvailableSpace struct {
	Index      int
	Available  uint64 // in bytes
	MaxUsedPct int    // Used disk percentage of most filled disk, rounded down.
}

// TotalAvailable - total available space
func (p serverPoolsAvailableSpace) TotalAvailable() uint64 {
	total := uint64(0)
	for _, z := range p {
		total += z.Available
	}
	return total
}

// FilterMaxUsed will filter out any pools that has used percent bigger than max,
// unless all have that, in which case all are preserved.
func (p serverPoolsAvailableSpace) FilterMaxUsed(maxUsed int) {
	// We aren't modifying p, only entries in it, so we don't need to receive a pointer.
	if len(p) <= 1 {
		// Nothing to do.
		return
	}
	var ok bool
	for _, z := range p {
		if z.Available > 0 && z.MaxUsedPct < maxUsed {
			ok = true
			break
		}
	}
	if !ok {
		// All above limit.
		// Do not modify
		return
	}

	// Remove entries that are above.
	for i, z := range p {
		if z.Available > 0 && z.MaxUsedPct < maxUsed {
			continue
		}
		p[i].Available = 0
	}
}

// getAvailablePoolIdx will return an index that can hold size bytes.
// -1 is returned if no serverPools have available space for the size given.
func (z *erasureServerPools) getAvailablePoolIdx(ctx context.Context, bucket, object string, size int64) int {
	serverPools := z.getServerPoolsAvailableSpace(ctx, bucket, object, size)
	serverPools.FilterMaxUsed(100 - (100 * diskReserveFraction))
	total := serverPools.TotalAvailable()
	if total == 0 {
		return -1
	}
	// choose when we reach this many
	choose := rand.Uint64() % total
	atTotal := uint64(0)
	for _, pool := range serverPools {
		atTotal += pool.Available
		if atTotal > choose && pool.Available > 0 {
			return pool.Index
		}
	}
	// Should not happen, but print values just in case.
	storageLogIf(ctx, fmt.Errorf("reached end of serverPools (total: %v, atTotal: %v, choose: %v)", total, atTotal, choose))
	return -1
}

// getServerPoolsAvailableSpace will return the available space of each pool after storing the content.
// If there is not enough space the pool will return 0 bytes available.
// The size of each will be multiplied by the number of sets.
// Negative sizes are seen as 0 bytes.
func (z *erasureServerPools) getServerPoolsAvailableSpace(ctx context.Context, bucket, object string, size int64) serverPoolsAvailableSpace {
	serverPools := make(serverPoolsAvailableSpace, len(z.serverPools))

	storageInfos := make([][]*DiskInfo, len(z.serverPools))
	nSets := make([]int, len(z.serverPools))
	g := errgroup.WithNErrs(len(z.serverPools))
	for index := range z.serverPools {
		index := index
		// Skip suspended pools or pools participating in rebalance for any new
		// I/O.
		if z.IsSuspended(index) || z.IsPoolRebalancing(index) {
			continue
		}
		pool := z.serverPools[index]
		nSets[index] = pool.setCount
		g.Go(func() error {
			// Get the set where it would be placed.
			storageInfos[index] = getDiskInfos(ctx, pool.getHashedSet(object).getDisks()...)
			return nil
		}, index)
	}

	// Wait for the go routines.
	g.Wait()

	for i, zinfo := range storageInfos {
		if zinfo == nil {
			serverPools[i] = poolAvailableSpace{Index: i}
			continue
		}
		var available uint64
		if !isMinioMetaBucketName(bucket) {
			if avail, err := hasSpaceFor(zinfo, size); err != nil || !avail {
				serverPools[i] = poolAvailableSpace{Index: i}
				continue
			}
		}
		var maxUsedPct int
		for _, disk := range zinfo {
			if disk == nil || disk.Total == 0 {
				continue
			}
			available += disk.Total - disk.Used

			// set maxUsedPct to the value from the disk with the least space percentage.
			if pctUsed := int(disk.Used * 100 / disk.Total); pctUsed > maxUsedPct {
				maxUsedPct = pctUsed
			}
		}

		// Since we are comparing pools that may have a different number of sets
		// we multiply by the number of sets in the pool.
		// This will compensate for differences in set sizes
		// when choosing destination pool.
		// Different set sizes are already compensated by less disks.
		available *= uint64(nSets[i])

		serverPools[i] = poolAvailableSpace{
			Index:      i,
			Available:  available,
			MaxUsedPct: maxUsedPct,
		}
	}
	return serverPools
}

// PoolObjInfo represents the state of current object version per pool
type PoolObjInfo struct {
	Index   int
	ObjInfo ObjectInfo
	Err     error
}

type poolErrs struct {
	Index int
	Err   error
}

func (z *erasureServerPools) getPoolInfoExistingWithOpts(ctx context.Context, bucket, object string, opts ObjectOptions) (PoolObjInfo, []poolErrs, error) {
	var noReadQuorumPools []poolErrs
	poolObjInfos := make([]PoolObjInfo, len(z.serverPools))
	poolOpts := make([]ObjectOptions, len(z.serverPools))
	for i := range z.serverPools {
		poolOpts[i] = opts
	}

	var wg sync.WaitGroup
	for i, pool := range z.serverPools {
		wg.Add(1)
		go func(i int, pool *erasureSets, opts ObjectOptions) {
			defer wg.Done()
			// remember the pool index, we may sort the slice original index might be lost.
			pinfo := PoolObjInfo{
				Index: i,
			}
			// do not remove this check as it can lead to inconsistencies
			// for all callers of bucket replication.
			if !opts.MetadataChg {
				opts.VersionID = ""
			}
			pinfo.ObjInfo, pinfo.Err = pool.GetObjectInfo(ctx, bucket, object, opts)
			poolObjInfos[i] = pinfo
		}(i, pool, poolOpts[i])
	}
	wg.Wait()

	// Sort the objInfos such that we always serve latest
	// this is a defensive change to handle any duplicate
	// content that may have been created, we always serve
	// the latest object.
	sort.Slice(poolObjInfos, func(i, j int) bool {
		mtime1 := poolObjInfos[i].ObjInfo.ModTime
		mtime2 := poolObjInfos[j].ObjInfo.ModTime
		return mtime1.After(mtime2)
	})

	defPool := PoolObjInfo{Index: -1}
	for _, pinfo := range poolObjInfos {
		// skip all objects from suspended pools if asked by the
		// caller.
		if opts.SkipDecommissioned && z.IsSuspended(pinfo.Index) {
			continue
		}
		// Skip object if it's from pools participating in a rebalance operation.
		if opts.SkipRebalancing && z.IsPoolRebalancing(pinfo.Index) {
			continue
		}
		if pinfo.Err == nil {
			// found a pool
			return pinfo, z.poolsWithObject(poolObjInfos, opts), nil
		}

		if isErrReadQuorum(pinfo.Err) && !opts.MetadataChg {
			// read quorum is returned when the object is visibly
			// present but its unreadable, we simply ask the writes to
			// schedule to this pool instead. If there is no quorum
			// it will fail anyways, however if there is quorum available
			// with enough disks online but sufficiently inconsistent to
			// break parity threshold, allow them to be overwritten
			// or allow new versions to be added.

			return pinfo, z.poolsWithObject(poolObjInfos, opts), nil
		}
		defPool = pinfo
		if !isErrObjectNotFound(pinfo.Err) && !isErrVersionNotFound(pinfo.Err) {
			return pinfo, noReadQuorumPools, pinfo.Err
		}

		// No object exists or its a delete marker,
		// check objInfo to confirm.
		if pinfo.ObjInfo.DeleteMarker && pinfo.ObjInfo.Name != "" {
			return pinfo, noReadQuorumPools, nil
		}
	}
	if opts.ReplicationRequest && opts.DeleteMarker && defPool.Index >= 0 {
		// If the request is a delete marker replication request, return a default pool
		// in cases where the object does not exist.
		// This is to ensure that the delete marker is replicated to the destination.
		return defPool, noReadQuorumPools, nil
	}
	return PoolObjInfo{}, noReadQuorumPools, toObjectErr(errFileNotFound, bucket, object)
}

// return all pools with read quorum error or no error for an object with given opts.Note that this array is
// returned in the order of recency of object ModTime.
func (z *erasureServerPools) poolsWithObject(pools []PoolObjInfo, opts ObjectOptions) (errs []poolErrs) {
	for _, pool := range pools {
		if opts.SkipDecommissioned && z.IsSuspended(pool.Index) {
			continue
		}
		// Skip object if it's from pools participating in a rebalance operation.
		if opts.SkipRebalancing && z.IsPoolRebalancing(pool.Index) {
			continue
		}
		if isErrReadQuorum(pool.Err) || pool.Err == nil {
			errs = append(errs, poolErrs{Err: pool.Err, Index: pool.Index})
		}
	}
	return errs
}

func (z *erasureServerPools) getPoolIdxExistingWithOpts(ctx context.Context, bucket, object string, opts ObjectOptions) (idx int, err error) {
	pinfo, _, err := z.getPoolInfoExistingWithOpts(ctx, bucket, object, opts)
	if err != nil {
		return -1, err
	}
	return pinfo.Index, nil
}

// getPoolIdxExistingNoLock returns the (first) found object pool index containing an object.
// If the object exists, but the latest version is a delete marker, the index with it is still returned.
// If the object does not exist ObjectNotFound error is returned.
// If any other error is found, it is returned.
// The check is skipped if there is only one pool, and 0, nil is always returned in that case.
func (z *erasureServerPools) getPoolIdxExistingNoLock(ctx context.Context, bucket, object string) (idx int, err error) {
	return z.getPoolIdxExistingWithOpts(ctx, bucket, object, ObjectOptions{
		NoLock:             true,
		SkipDecommissioned: true,
		SkipRebalancing:    true,
	})
}

func (z *erasureServerPools) getPoolIdxNoLock(ctx context.Context, bucket, object string, size int64) (idx int, err error) {
	idx, err = z.getPoolIdxExistingNoLock(ctx, bucket, object)
	if err != nil && !isErrObjectNotFound(err) {
		return idx, err
	}

	if isErrObjectNotFound(err) {
		idx = z.getAvailablePoolIdx(ctx, bucket, object, size)
		if idx < 0 {
			return -1, toObjectErr(errDiskFull)
		}
	}

	return idx, nil
}

// getPoolIdx returns the found previous object and its corresponding pool idx,
// if none are found falls back to most available space pool, this function is
// designed to be only used by PutObject, CopyObject (newObject creation) and NewMultipartUpload.
func (z *erasureServerPools) getPoolIdx(ctx context.Context, bucket, object string, size int64) (idx int, err error) {
	idx, err = z.getPoolIdxExistingWithOpts(ctx, bucket, object, ObjectOptions{
		SkipDecommissioned: true,
		SkipRebalancing:    true,
	})
	if err != nil && !isErrObjectNotFound(err) {
		return idx, err
	}

	if isErrObjectNotFound(err) {
		idx = z.getAvailablePoolIdx(ctx, bucket, object, size)
		if idx < 0 {
			return -1, toObjectErr(errDiskFull)
		}
	}

	return idx, nil
}

func (z *erasureServerPools) Shutdown(ctx context.Context) error {
	g := errgroup.WithNErrs(len(z.serverPools))

	for index := range z.serverPools {
		index := index
		g.Go(func() error {
			return z.serverPools[index].Shutdown(ctx)
		}, index)
	}

	for _, err := range g.Wait() {
		if err != nil {
			storageLogIf(ctx, err)
		}
		// let's the rest shutdown
	}
	return nil
}

// Legacy returns 'true' if distribution algo is CRCMOD
func (z *erasureServerPools) Legacy() (ok bool) {
	ok = true
	for _, set := range z.serverPools {
		ok = ok && set.Legacy()
	}
	return ok
}

func (z *erasureServerPools) BackendInfo() (b madmin.BackendInfo) {
	b.Type = madmin.Erasure

	scParity := globalStorageClass.GetParityForSC(storageclass.STANDARD)
	if scParity < 0 {
		scParity = z.serverPools[0].defaultParityCount
	}
	rrSCParity := globalStorageClass.GetParityForSC(storageclass.RRS)

	// Data blocks can vary per pool, but parity is same.
	for i, setDriveCount := range z.SetDriveCounts() {
		b.StandardSCData = append(b.StandardSCData, setDriveCount-scParity)
		b.RRSCData = append(b.RRSCData, setDriveCount-rrSCParity)
		b.DrivesPerSet = append(b.DrivesPerSet, setDriveCount)
		b.TotalSets = append(b.TotalSets, z.serverPools[i].setCount)
	}

	b.StandardSCParity = scParity
	b.RRSCParity = rrSCParity
	return
}

func (z *erasureServerPools) LocalStorageInfo(ctx context.Context, metrics bool) StorageInfo {
	var storageInfo StorageInfo

	storageInfos := make([]StorageInfo, len(z.serverPools))
	g := errgroup.WithNErrs(len(z.serverPools))
	for index := range z.serverPools {
		index := index
		g.Go(func() error {
			storageInfos[index] = z.serverPools[index].LocalStorageInfo(ctx, metrics)
			return nil
		}, index)
	}

	// Wait for the go routines.
	g.Wait()

	storageInfo.Backend = z.BackendInfo()
	for _, lstorageInfo := range storageInfos {
		storageInfo.Disks = append(storageInfo.Disks, lstorageInfo.Disks...)
	}

	return storageInfo
}

func (z *erasureServerPools) StorageInfo(ctx context.Context, metrics bool) StorageInfo {
	return globalNotificationSys.StorageInfo(ctx, z, metrics)
}

func (z *erasureServerPools) NSScanner(ctx context.Context, updates chan<- DataUsageInfo, wantCycle uint32, healScanMode madmin.HealScanMode) error {
	// Updates must be closed before we return.
	defer xioutil.SafeClose(updates)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []dataUsageCache
	var firstErr error

	allBuckets, err := z.ListBuckets(ctx, BucketOptions{})
	if err != nil {
		return err
	}

	if len(allBuckets) == 0 {
		updates <- DataUsageInfo{} // no buckets found update data usage to reflect latest state
		return nil
	}
	totalResults := 0
	resultIndex := -1
	for _, z := range z.serverPools {
		totalResults += len(z.sets)
	}
	results = make([]dataUsageCache, totalResults)
	// Collect for each set in serverPools.
	for _, z := range z.serverPools {
		for _, erObj := range z.sets {
			resultIndex++
			wg.Add(1)
			go func(i int, erObj *erasureObjects) {
				updates := make(chan dataUsageCache, 1)
				defer xioutil.SafeClose(updates)
				// Start update collector.
				go func() {
					defer wg.Done()
					for info := range updates {
						mu.Lock()
						results[i] = info
						mu.Unlock()
					}
				}()
				// Start scanner. Blocks until done.
				err := erObj.nsScanner(ctx, allBuckets, wantCycle, updates, healScanMode)
				if err != nil {
					scannerLogIf(ctx, err)
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					// Cancel remaining...
					cancel()
					mu.Unlock()
					return
				}
			}(resultIndex, erObj)
		}
	}
	updateCloser := make(chan chan struct{})
	go func() {
		updateTicker := time.NewTicker(30 * time.Second)
		defer updateTicker.Stop()
		var lastUpdate time.Time

		// We need to merge since we will get the same buckets from each pool.
		// Therefore to get the exact bucket sizes we must merge before we can convert.
		var allMerged dataUsageCache

		update := func() {
			mu.Lock()
			defer mu.Unlock()

			allMerged = dataUsageCache{Info: dataUsageCacheInfo{Name: dataUsageRoot}}
			for _, info := range results {
				if info.Info.LastUpdate.IsZero() {
					// Not filled yet.
					return
				}
				allMerged.merge(info)
			}
			if allMerged.root() != nil && allMerged.Info.LastUpdate.After(lastUpdate) {
				updates <- allMerged.dui(allMerged.Info.Name, allBuckets)
				lastUpdate = allMerged.Info.LastUpdate
			}
		}
		for {
			select {
			case <-ctx.Done():
				return
			case v := <-updateCloser:
				update()
				xioutil.SafeClose(v)
				return
			case <-updateTicker.C:
				update()
			}
		}
	}()

	wg.Wait()
	ch := make(chan struct{})
	select {
	case updateCloser <- ch:
		<-ch
	case <-ctx.Done():
		mu.Lock()
		if firstErr == nil {
			firstErr = ctx.Err()
		}
		defer mu.Unlock()
	}
	return firstErr
}

// MakeBucket - creates a new bucket across all serverPools simultaneously
// even if one of the sets fail to create buckets, we proceed all the successful
// operations.
func (z *erasureServerPools) MakeBucket(ctx context.Context, bucket string, opts MakeBucketOptions) error {
	// Verify if bucket is valid.
	if !isMinioMetaBucketName(bucket) {
		if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
			return BucketNameInvalid{Bucket: bucket}
		}

		if !opts.NoLock {
			// Lock the bucket name before creating.
			lk := z.NewNSLock(minioMetaTmpBucket, bucket+".lck")
			lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return err
			}

			ctx = lkctx.Context()
			defer lk.Unlock(lkctx)
		}
	}

	if err := z.s3Peer.MakeBucket(ctx, bucket, opts); err != nil {
		if _, ok := err.(BucketExists); !ok {
			// Delete created buckets, ignoring errors.
			z.DeleteBucket(context.Background(), bucket, DeleteBucketOptions{
				NoLock:     true,
				NoRecreate: true,
			})
		}
		return err
	}

	// If it doesn't exist we get a new, so ignore errors
	meta := newBucketMetadata(bucket)
	meta.SetCreatedAt(opts.CreatedAt)
	if opts.LockEnabled {
		meta.VersioningConfigXML = enabledBucketVersioningConfig
		meta.ObjectLockConfigXML = enabledBucketObjectLockConfig
	}

	if opts.VersioningEnabled {
		meta.VersioningConfigXML = enabledBucketVersioningConfig
	}

	if err := meta.Save(context.Background(), z); err != nil {
		return toObjectErr(err, bucket)
	}

	globalBucketMetadataSys.Set(bucket, meta)

	// Success.
	return nil
}

func (z *erasureServerPools) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}

	// This is a special call attempted first to check for SOS-API calls.
	gr, err = veeamSOSAPIGetObject(ctx, bucket, object, rs, opts)
	if err == nil {
		return gr, nil
	}

	// reset any error to 'nil' and any reader to be 'nil'
	gr = nil
	err = nil

	object = encodeDirObject(object)

	if z.SinglePool() {
		return z.serverPools[0].GetObjectNInfo(ctx, bucket, object, rs, h, opts)
	}

	var unlockOnDefer bool
	nsUnlocker := func() {}
	defer func() {
		if unlockOnDefer {
			nsUnlocker()
		}
	}()

	// Acquire lock
	if !opts.NoLock {
		lock := z.NewNSLock(bucket, object)
		lkctx, err := lock.GetRLock(ctx, globalOperationTimeout)
		if err != nil {
			return nil, err
		}
		ctx = lkctx.Context()
		nsUnlocker = func() { lock.RUnlock(lkctx) }
		unlockOnDefer = true
	}

	checkPrecondFn := opts.CheckPrecondFn
	opts.CheckPrecondFn = nil // do not need to apply pre-conditions at lower layer.
	opts.NoLock = true        // no locks needed at lower levels for getObjectInfo()
	objInfo, zIdx, err := z.getLatestObjectInfoWithIdx(ctx, bucket, object, opts)
	if err != nil {
		if objInfo.DeleteMarker {
			if opts.VersionID == "" {
				return &GetObjectReader{
					ObjInfo: objInfo,
				}, toObjectErr(errFileNotFound, bucket, object)
			}
			// Make sure to return object info to provide extra information.
			return &GetObjectReader{
				ObjInfo: objInfo,
			}, toObjectErr(errMethodNotAllowed, bucket, object)
		}
		return nil, err
	}

	// check preconditions before reading the stream.
	if checkPrecondFn != nil && checkPrecondFn(objInfo) {
		return nil, PreConditionFailed{}
	}

	opts.NoLock = true
	gr, err = z.serverPools[zIdx].GetObjectNInfo(ctx, bucket, object, rs, h, opts)
	if err != nil {
		return nil, err
	}

	if unlockOnDefer {
		unlockOnDefer = gr.ObjInfo.Inlined
	}

	if !unlockOnDefer {
		return gr.WithCleanupFuncs(nsUnlocker), nil
	}
	return gr, nil
}

// getLatestObjectInfoWithIdx returns the objectInfo of the latest object from multiple pools (this function
// is present in-case there were duplicate writes to both pools, this function also returns the
// additional index where the latest object exists, that is used to start the GetObject stream.
func (z *erasureServerPools) getLatestObjectInfoWithIdx(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, int, error) {
	object = encodeDirObject(object)
	results := make([]struct {
		zIdx int
		oi   ObjectInfo
		err  error
	}, len(z.serverPools))
	var wg sync.WaitGroup
	for i, pool := range z.serverPools {
		wg.Add(1)
		go func(i int, pool *erasureSets) {
			defer wg.Done()
			results[i].zIdx = i
			results[i].oi, results[i].err = pool.GetObjectInfo(ctx, bucket, object, opts)
		}(i, pool)
	}
	wg.Wait()

	// Sort the objInfos such that we always serve latest
	// this is a defensive change to handle any duplicate
	// content that may have been created, we always serve
	// the latest object.
	sort.Slice(results, func(i, j int) bool {
		a, b := results[i], results[j]
		if a.oi.ModTime.Equal(b.oi.ModTime) {
			// On tiebreak, select the lowest pool index.
			return a.zIdx < b.zIdx
		}
		return a.oi.ModTime.After(b.oi.ModTime)
	})

	for _, res := range results {
		err := res.err
		if err == nil {
			return res.oi, res.zIdx, nil
		}
		if !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
			// some errors such as MethodNotAllowed for delete marker
			// should be returned upwards.
			return res.oi, res.zIdx, err
		}
		// When its a delete marker and versionID is empty
		// we should simply return the error right away.
		if res.oi.DeleteMarker && opts.VersionID == "" {
			return res.oi, res.zIdx, err
		}
	}

	object = decodeDirObject(object)
	if opts.VersionID != "" {
		return ObjectInfo{}, -1, VersionNotFound{Bucket: bucket, Object: object, VersionID: opts.VersionID}
	}
	return ObjectInfo{}, -1, ObjectNotFound{Bucket: bucket, Object: object}
}

func (z *erasureServerPools) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return objInfo, err
	}

	// This is a special call attempted first to check for SOS-API calls.
	objInfo, err = veeamSOSAPIHeadObject(ctx, bucket, object, opts)
	if err == nil {
		return objInfo, nil
	}

	// reset any error to 'nil', and object info to be empty.
	err = nil
	objInfo = ObjectInfo{}

	object = encodeDirObject(object)

	if z.SinglePool() {
		return z.serverPools[0].GetObjectInfo(ctx, bucket, object, opts)
	}

	if !opts.NoLock {
		opts.NoLock = true // avoid taking locks at lower levels for multi-pool setups.

		// Lock the object before reading.
		lk := z.NewNSLock(bucket, object)
		lkctx, err := lk.GetRLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.RUnlock(lkctx)
	}

	objInfo, _, err = z.getLatestObjectInfoWithIdx(ctx, bucket, object, opts)
	return objInfo, err
}

// PutObject - writes an object to least used erasure pool.
func (z *erasureServerPools) PutObject(ctx context.Context, bucket string, object string, data *PutObjReader, opts ObjectOptions) (ObjectInfo, error) {
	// Validate put object input args.
	if err := checkPutObjectArgs(ctx, bucket, object); err != nil {
		return ObjectInfo{}, err
	}

	object = encodeDirObject(object)
	if z.SinglePool() {
		return z.serverPools[0].PutObject(ctx, bucket, object, data, opts)
	}

	idx, err := z.getPoolIdx(ctx, bucket, object, data.Size())
	if err != nil {
		return ObjectInfo{}, err
	}

	if opts.DataMovement && idx == opts.SrcPoolIdx {
		return ObjectInfo{}, DataMovementOverwriteErr{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
			Err:       errDataMovementSrcDstPoolSame,
		}
	}

	return z.serverPools[idx].PutObject(ctx, bucket, object, data, opts)
}

func (z *erasureServerPools) deletePrefix(ctx context.Context, bucket string, prefix string) error {
	for _, pool := range z.serverPools {
		if _, err := pool.DeleteObject(ctx, bucket, prefix, ObjectOptions{DeletePrefix: true}); err != nil {
			return err
		}
	}
	return nil
}

func (z *erasureServerPools) DeleteObject(ctx context.Context, bucket string, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if err = checkDelObjArgs(ctx, bucket, object); err != nil {
		return objInfo, err
	}

	if !opts.DeletePrefix { // DeletePrefix handles dir object encoding differently.
		object = encodeDirObject(object)
	}

	// Acquire a write lock before deleting the object.
	lk := z.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalDeleteOperationTimeout)
	if err != nil {
		return ObjectInfo{}, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx)

	if opts.DeletePrefix {
		return ObjectInfo{}, z.deletePrefix(ctx, bucket, object)
	}

	gopts := opts
	gopts.NoLock = true

	pinfo, noReadQuorumPools, err := z.getPoolInfoExistingWithOpts(ctx, bucket, object, gopts)
	if err != nil {
		if _, ok := err.(InsufficientReadQuorum); ok {
			return objInfo, InsufficientWriteQuorum{}
		}
		return objInfo, err
	}

	// Delete marker already present we are not going to create new delete markers.
	if pinfo.ObjInfo.DeleteMarker && opts.VersionID == "" {
		pinfo.ObjInfo.Name = decodeDirObject(object)
		return pinfo.ObjInfo, nil
	}

	// Datamovement must never be allowed on the same pool.
	if opts.DataMovement && opts.SrcPoolIdx == pinfo.Index {
		return pinfo.ObjInfo, DataMovementOverwriteErr{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
			Err:       errDataMovementSrcDstPoolSame,
		}
	}

	if opts.DataMovement {
		objInfo, err = z.serverPools[pinfo.Index].DeleteObject(ctx, bucket, object, opts)
		objInfo.Name = decodeDirObject(object)
		return objInfo, err
	}

	// Delete concurrently in all server pools with read quorum error for unversioned objects.
	if len(noReadQuorumPools) > 0 && !opts.Versioned && !opts.VersionSuspended {
		return z.deleteObjectFromAllPools(ctx, bucket, object, opts, noReadQuorumPools)
	}

	for _, pool := range z.serverPools {
		objInfo, err := pool.DeleteObject(ctx, bucket, object, opts)
		if err != nil && !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
			objInfo.Name = decodeDirObject(object)
			return objInfo, err
		}
		if err == nil {
			objInfo.Name = decodeDirObject(object)
			return objInfo, nil
		}
	}

	objInfo.Name = decodeDirObject(object)
	if opts.VersionID != "" {
		return objInfo, VersionNotFound{Bucket: bucket, Object: object, VersionID: opts.VersionID}
	}
	return objInfo, ObjectNotFound{Bucket: bucket, Object: object}
}

func (z *erasureServerPools) deleteObjectFromAllPools(ctx context.Context, bucket string, object string, opts ObjectOptions, poolIndices []poolErrs) (objInfo ObjectInfo, err error) {
	derrs := make([]error, len(poolIndices))
	dobjects := make([]ObjectInfo, len(poolIndices))

	// Delete concurrently in all server pools that reported no error or read quorum error
	// where the read quorum issue is from metadata inconsistency.
	var wg sync.WaitGroup
	for idx, pe := range poolIndices {
		if v, ok := pe.Err.(InsufficientReadQuorum); ok && v.Type != RQInconsistentMeta {
			derrs[idx] = InsufficientWriteQuorum{}
			continue
		}
		wg.Add(1)
		pool := z.serverPools[pe.Index]
		go func(idx int, pool *erasureSets) {
			defer wg.Done()
			dobjects[idx], derrs[idx] = pool.DeleteObject(ctx, bucket, object, opts)
		}(idx, pool)
	}
	wg.Wait()

	// the poolIndices array is pre-sorted in order of latest ModTime, we care only about pool with latest object though
	// the delete call tries to clean up other pools during DeleteObject call.
	objInfo = dobjects[0]
	objInfo.Name = decodeDirObject(object)
	err = derrs[0]
	return objInfo, err
}

func (z *erasureServerPools) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	derrs := make([]error, len(objects))
	dobjects := make([]DeletedObject, len(objects))
	objSets := set.NewStringSet()
	for i := range derrs {
		objects[i].ObjectName = encodeDirObject(objects[i].ObjectName)

		derrs[i] = checkDelObjArgs(ctx, bucket, objects[i].ObjectName)
		objSets.Add(objects[i].ObjectName)
	}

	// Acquire a bulk write lock across 'objects'
	multiDeleteLock := z.NewNSLock(bucket, objSets.ToSlice()...)
	lkctx, err := multiDeleteLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		for i := range derrs {
			derrs[i] = err
		}
		return dobjects, derrs
	}
	ctx = lkctx.Context()
	defer multiDeleteLock.Unlock(lkctx)

	dObjectsByPool := make([][]DeletedObject, len(z.serverPools))
	dErrsByPool := make([][]error, len(z.serverPools))

	eg := errgroup.WithNErrs(len(z.serverPools)).WithConcurrency(len(z.serverPools))
	for i, pool := range z.serverPools {
		i := i
		pool := pool
		eg.Go(func() error {
			dObjectsByPool[i], dErrsByPool[i] = pool.DeleteObjects(ctx, bucket, objects, opts)
			return nil
		}, i)
	}
	eg.Wait() // wait to check all the pools.

	for i := range dobjects {
		// Iterate over pools
		for pool := range z.serverPools {
			if dErrsByPool[pool][i] == nil && dObjectsByPool[pool][i].found {
				// A fast exit when the object is found and removed
				dobjects[i] = dObjectsByPool[pool][i]
				derrs[i] = nil
				break
			}
			if derrs[i] == nil {
				// No error related to this object is found, assign this pool result
				// whether it is nil because there is no object found or because of
				// some other errors such erasure quorum errors.
				dobjects[i] = dObjectsByPool[pool][i]
				derrs[i] = dErrsByPool[pool][i]
			}
		}
	}

	for i := range dobjects {
		dobjects[i].ObjectName = decodeDirObject(dobjects[i].ObjectName)
	}
	return dobjects, derrs
}

func (z *erasureServerPools) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	if err := checkCopyObjArgs(ctx, srcBucket, srcObject); err != nil {
		return ObjectInfo{}, err
	}
	if err := checkCopyObjArgs(ctx, dstBucket, dstObject); err != nil {
		return ObjectInfo{}, err
	}

	srcObject = encodeDirObject(srcObject)
	dstObject = encodeDirObject(dstObject)

	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))

	if !dstOpts.NoLock {
		ns := z.NewNSLock(dstBucket, dstObject)
		lkctx, err := ns.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer ns.Unlock(lkctx)
		dstOpts.NoLock = true
	}

	poolIdx, err := z.getPoolIdxNoLock(ctx, dstBucket, dstObject, srcInfo.Size)
	if err != nil {
		return objInfo, err
	}

	if cpSrcDstSame && srcInfo.metadataOnly {
		// Version ID is set for the destination and source == destination version ID.
		if dstOpts.VersionID != "" && srcOpts.VersionID == dstOpts.VersionID {
			return z.serverPools[poolIdx].CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		}
		// Destination is not versioned and source version ID is empty
		// perform an in-place update.
		if !dstOpts.Versioned && srcOpts.VersionID == "" {
			return z.serverPools[poolIdx].CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		}
		// Destination is versioned, source is not destination version,
		// as a special case look for if the source object is not legacy
		// from older format, for older format we will rewrite them as
		// newer using PutObject() - this is an optimization to save space
		if dstOpts.Versioned && srcOpts.VersionID != dstOpts.VersionID && !srcInfo.Legacy {
			// CopyObject optimization where we don't create an entire copy
			// of the content, instead we add a reference.
			srcInfo.versionOnly = true
			return z.serverPools[poolIdx].CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		}
	}

	putOpts := ObjectOptions{
		ServerSideEncryption: dstOpts.ServerSideEncryption,
		UserDefined:          srcInfo.UserDefined,
		Versioned:            dstOpts.Versioned,
		VersionID:            dstOpts.VersionID,
		MTime:                dstOpts.MTime,
		NoLock:               true,
	}

	return z.serverPools[poolIdx].PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, putOpts)
}

func (z *erasureServerPools) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return z.listObjectsGeneric(ctx, bucket, prefix, marker, delimiter, maxKeys, true)
}

func (z *erasureServerPools) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error) {
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	loi, err := z.listObjectsGeneric(ctx, bucket, prefix, marker, delimiter, maxKeys, false)
	if err != nil {
		return ListObjectsV2Info{}, err
	}

	listObjectsV2Info := ListObjectsV2Info{
		IsTruncated:           loi.IsTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: loi.NextMarker,
		Objects:               loi.Objects,
		Prefixes:              loi.Prefixes,
	}
	return listObjectsV2Info, err
}

func (z *erasureServerPools) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (ListObjectVersionsInfo, error) {
	loi := ListObjectVersionsInfo{}
	if marker == "" && versionMarker != "" {
		return loi, NotImplemented{}
	}

	opts := listPathOptions{
		Bucket:      bucket,
		Prefix:      prefix,
		Separator:   delimiter,
		Limit:       maxKeysPlusOne(maxKeys, marker != ""),
		Marker:      marker,
		InclDeleted: true,
		AskDisks:    globalAPIConfig.getListQuorum(),
		Versioned:   true,
	}

	// Shortcut for APN/1.0 Veeam/1.0 Backup/10.0
	// It requests unique blocks with a specific prefix.
	// We skip scanning the parent directory for
	// more objects matching the prefix.
	if isVeeamClient(ctx) && strings.HasSuffix(prefix, ".blk") {
		opts.BaseDir = prefix
		opts.Transient = true
	}

	// set bucket metadata in opts
	opts.setBucketMeta(ctx)

	merged, err := z.listPath(ctx, &opts)
	if err != nil && err != io.EOF {
		if !isErrBucketNotFound(err) {
			storageLogOnceIf(ctx, err, "erasure-list-objects-path-"+bucket)
		}
		return loi, toObjectErr(err, bucket)
	}
	defer merged.truncate(0) // Release when returning

	if contextCanceled(ctx) {
		return ListObjectVersionsInfo{}, ctx.Err()
	}

	if versionMarker == "" {
		o := listPathOptions{Marker: marker}
		// If we are not looking for a specific version skip it.

		o.parseMarker()
		merged.forwardPast(o.Marker)
	}
	objects := merged.fileInfoVersions(bucket, prefix, delimiter, versionMarker)
	loi.IsTruncated = err == nil && len(objects) > 0
	if maxKeys > 0 && len(objects) > maxKeys {
		objects = objects[:maxKeys]
		loi.IsTruncated = true
	}
	for _, obj := range objects {
		if obj.IsDir && obj.ModTime.IsZero() && delimiter != "" {
			// Only add each once.
			// With slash delimiter we only get the directory once.
			found := false
			if delimiter != slashSeparator {
				for _, p := range loi.Prefixes {
					if found {
						break
					}
					found = p == obj.Name
				}
			}
			if !found {
				loi.Prefixes = append(loi.Prefixes, obj.Name)
			}
		} else {
			loi.Objects = append(loi.Objects, obj)
		}
	}
	if loi.IsTruncated {
		last := objects[len(objects)-1]
		loi.NextMarker = opts.encodeMarker(last.Name)
		loi.NextVersionIDMarker = last.VersionID
	}
	return loi, nil
}

func maxKeysPlusOne(maxKeys int, addOne bool) int {
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}
	if addOne {
		maxKeys++
	}
	return maxKeys
}

func (z *erasureServerPools) listObjectsGeneric(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int, v1 bool) (loi ListObjectsInfo, err error) {
	opts := listPathOptions{
		V1:          v1,
		Bucket:      bucket,
		Prefix:      prefix,
		Separator:   delimiter,
		Limit:       maxKeysPlusOne(maxKeys, marker != ""),
		Marker:      marker,
		InclDeleted: false,
		AskDisks:    globalAPIConfig.getListQuorum(),
	}
	opts.setBucketMeta(ctx)
	listFn := func(ctx context.Context, opts listPathOptions, limitTo int) (ListObjectsInfo, error) {
		var loi ListObjectsInfo
		merged, err := z.listPath(ctx, &opts)
		if err != nil && err != io.EOF {
			if !isErrBucketNotFound(err) {
				storageLogOnceIf(ctx, err, "erasure-list-objects-path-"+bucket)
			}
			return loi, toObjectErr(err, bucket)
		}
		merged.forwardPast(opts.Marker)
		defer merged.truncate(0) // Release when returning

		if contextCanceled(ctx) {
			return ListObjectsInfo{}, ctx.Err()
		}

		// Default is recursive, if delimiter is set then list non recursive.
		objects := merged.fileInfos(bucket, prefix, delimiter)
		loi.IsTruncated = err == nil && len(objects) > 0
		if limitTo > 0 && len(objects) > limitTo {
			objects = objects[:limitTo]
			loi.IsTruncated = true
		}
		for _, obj := range objects {
			if obj.IsDir && obj.ModTime.IsZero() && delimiter != "" {
				// Only add each once.
				// With slash delimiter we only get the directory once.
				found := false
				if delimiter != slashSeparator {
					for _, p := range loi.Prefixes {
						if found {
							break
						}
						found = p == obj.Name
					}
				}
				if !found {
					loi.Prefixes = append(loi.Prefixes, obj.Name)
				}
			} else {
				loi.Objects = append(loi.Objects, obj)
			}
		}
		if loi.IsTruncated {
			last := objects[len(objects)-1]
			loi.NextMarker = last.Name
		}

		if loi.IsTruncated && merged.lastSkippedEntry > loi.NextMarker {
			// An object hidden by ILM was found during a truncated listing. Set the next marker
			// as the last skipped entry if it is lexically higher loi.NextMarker as an optimization
			loi.NextMarker = merged.lastSkippedEntry
		}

		if loi.NextMarker != "" {
			loi.NextMarker = opts.encodeMarker(loi.NextMarker)
		}
		return loi, nil
	}
	ri := logger.GetReqInfo(ctx)
	hadoop := ri != nil && strings.Contains(ri.UserAgent, "Hadoop ") && strings.Contains(ri.UserAgent, "scala/")
	matches := func() bool {
		if prefix == "" {
			return false
		}
		// List of standard files supported by s3a
		// that involves a List() on a directory
		// where directory is actually an object on
		// namespace.
		for _, k := range []string{
			"_SUCCESS/",
			".parquet/",
			".csv/",
			".json/",
			".avro/",
			".orc/",
			".txt/",
			// Add any other files in future
		} {
			if strings.HasSuffix(prefix, k) {
				return true
			}
		}
		return false
	}

	if hadoop && delimiter == SlashSeparator && maxKeys == 2 && marker == "" {
		// Optimization for Spark/Hadoop workload where spark sends a garbage
		// request of this kind
		//
		// GET /testbucket/?list-type=2&delimiter=%2F&max-keys=2&prefix=parquet%2F_SUCCESS%2F&fetch-owner=false
		//
		// Here spark is expecting that the List() return empty instead, so from MinIO's point
		// of view if we simply do a GetObjectInfo() on this prefix by treating it as an object
		// We save a lot of calls over the network.
		//
		// This happens repeatedly for all objects that are created concurrently() avoiding this
		// as a List() call is an important performance improvement.
		//
		// Spark based s3a committers are a  big enough use-case to have this optimization.
		//
		// A sample code to see the improvements is as follows, this sample code is
		// simply a read on JSON from MinIO and write it back as "parquet".
		//
		// import org.apache.spark.sql.SparkSession
		// import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
		// object SparkJSONRead {
		//   def main(args: Array[String]): Unit = {
		//     val spark:SparkSession = SparkSession.builder()
		//       .appName("SparkByExample")
		//       .master("local[1]").getOrCreate()
		//
		//     spark.sparkContext.setLogLevel("ERROR")
		//     spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://minio-lb:9000")
		//     spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
		//     spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minioadmin")
		//     spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minioadmin")
		//
		//     val df = spark.read.json("s3a://testbucket/s3.json")
		//
		//     df.write.parquet("s3a://testbucket/parquet/")
		//   }
		// }
		if matches() {
			objInfo, err := z.GetObjectInfo(ctx, bucket, path.Dir(prefix), ObjectOptions{NoLock: true})
			if err == nil || objInfo.IsLatest && objInfo.DeleteMarker {
				if opts.Lifecycle != nil {
					evt := evalActionFromLifecycle(ctx, *opts.Lifecycle, opts.Retention, opts.Replication.Config, objInfo)
					if evt.Action.Delete() {
						globalExpiryState.enqueueByDays(objInfo, evt, lcEventSrc_s3ListObjects)
						if !evt.Action.DeleteRestored() {
							// Skip entry if ILM action was DeleteVersionAction or DeleteAction
							return loi, nil
						}
					}
				}
				return loi, nil
			}
			if isErrBucketNotFound(err) {
				return loi, err
			}
			if contextCanceled(ctx) {
				return ListObjectsInfo{}, ctx.Err()
			}
		}
		// Hadoop makes the max-keys=2 listing call just to find if the directory is empty or not, or in the case
		// of an object to check for object existence. For versioned buckets, MinIO's non-recursive
		// call will report top level prefixes in deleted state, whereas spark/hadoop interpret this as non-empty
		// and throw a 404 exception. This is especially a problem for spark jobs overwriting the same partition
		// repeatedly. This workaround recursively lists the top 3 entries including delete markers to reflect the
		// correct state of the directory in the list results.
		if strings.HasSuffix(opts.Prefix, SlashSeparator) {
			li, err := listFn(ctx, opts, maxKeys)
			if err != nil {
				return loi, err
			}
			if len(li.Objects) == 0 {
				prefixes := li.Prefixes[:0]
				for _, prefix := range li.Prefixes {
					objInfo, _ := z.GetObjectInfo(ctx, bucket, pathJoin(prefix, "_SUCCESS"), ObjectOptions{NoLock: true})
					if objInfo.IsLatest && objInfo.DeleteMarker {
						continue
					}
					prefixes = append(prefixes, prefix)
				}
				if len(prefixes) > 0 {
					objInfo, _ := z.GetObjectInfo(ctx, bucket, pathJoin(opts.Prefix, "_SUCCESS"), ObjectOptions{NoLock: true})
					if objInfo.IsLatest && objInfo.DeleteMarker {
						return loi, nil
					}
				}
				li.Prefixes = prefixes
			}
			return li, nil
		}
	}

	if len(prefix) > 0 && maxKeys == 1 && marker == "" {
		// Optimization for certain applications like
		// - Cohesity
		// - Actifio, Splunk etc.
		// which send ListObjects requests where the actual object
		// itself is the prefix and max-keys=1 in such scenarios
		// we can simply verify locally if such an object exists
		// to avoid the need for ListObjects().
		objInfo, err := z.GetObjectInfo(ctx, bucket, prefix, ObjectOptions{NoLock: true})
		if err == nil {
			if opts.Lifecycle != nil {
				evt := evalActionFromLifecycle(ctx, *opts.Lifecycle, opts.Retention, opts.Replication.Config, objInfo)
				if evt.Action.Delete() {
					globalExpiryState.enqueueByDays(objInfo, evt, lcEventSrc_s3ListObjects)
					if !evt.Action.DeleteRestored() {
						// Skip entry if ILM action was DeleteVersionAction or DeleteAction
						return loi, nil
					}
				}
			}
			loi.Objects = append(loi.Objects, objInfo)
			return loi, nil
		}
		if isErrBucketNotFound(err) {
			return ListObjectsInfo{}, err
		}
		if contextCanceled(ctx) {
			return ListObjectsInfo{}, ctx.Err()
		}
	}
	return listFn(ctx, opts, maxKeys)
}

func (z *erasureServerPools) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	if err := checkListMultipartArgs(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter); err != nil {
		return ListMultipartsInfo{}, err
	}

	poolResult := ListMultipartsInfo{}
	poolResult.MaxUploads = maxUploads
	poolResult.KeyMarker = keyMarker
	poolResult.Prefix = prefix
	poolResult.Delimiter = delimiter

	// if no prefix provided, return the list from cache
	if prefix == "" {
		if _, err := z.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
			return ListMultipartsInfo{}, toObjectErr(err, bucket)
		}

		z.mpCache.Range(func(_ string, mp MultipartInfo) bool {
			if mp.Bucket == bucket {
				poolResult.Uploads = append(poolResult.Uploads, mp)
			}
			return true
		})
		sort.Slice(poolResult.Uploads, func(i int, j int) bool {
			return poolResult.Uploads[i].Initiated.Before(poolResult.Uploads[j].Initiated)
		})
		return poolResult, nil
	}

	if z.SinglePool() {
		return z.serverPools[0].ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	}

	for idx, pool := range z.serverPools {
		if z.IsSuspended(idx) {
			continue
		}
		result, err := pool.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker,
			delimiter, maxUploads)
		if err != nil {
			return result, err
		}
		poolResult.Uploads = append(poolResult.Uploads, result.Uploads...)
	}

	return poolResult, nil
}

// Initiate a new multipart upload on a hashedSet based on object name.
func (z *erasureServerPools) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (mp *NewMultipartUploadResult, err error) {
	if err := checkNewMultipartArgs(ctx, bucket, object); err != nil {
		return nil, err
	}

	defer func() {
		if err == nil && mp != nil {
			z.mpCache.Store(mp.UploadID, MultipartInfo{
				Bucket:    bucket,
				Object:    object,
				UploadID:  mp.UploadID,
				Initiated: time.Now(),
			})
		}
	}()

	if z.SinglePool() {
		return z.serverPools[0].NewMultipartUpload(ctx, bucket, object, opts)
	}

	for idx, pool := range z.serverPools {
		if z.IsSuspended(idx) || z.IsPoolRebalancing(idx) {
			continue
		}

		result, err := pool.ListMultipartUploads(ctx, bucket, object, "", "", "", maxUploadsList)
		if err != nil {
			return nil, err
		}
		// If there is a multipart upload with the same bucket/object name,
		// create the new multipart in the same pool, this will avoid
		// creating two multiparts uploads in two different pools
		if len(result.Uploads) != 0 {
			return z.serverPools[idx].NewMultipartUpload(ctx, bucket, object, opts)
		}
	}

	// any parallel writes on the object will block for this poolIdx
	// to return since this holds a read lock on the namespace.
	idx, err := z.getPoolIdx(ctx, bucket, object, -1)
	if err != nil {
		return nil, err
	}

	if opts.DataMovement && idx == opts.SrcPoolIdx {
		return nil, DataMovementOverwriteErr{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
			Err:       errDataMovementSrcDstPoolSame,
		}
	}

	return z.serverPools[idx].NewMultipartUpload(ctx, bucket, object, opts)
}

// Copies a part of an object from source hashedSet to destination hashedSet.
func (z *erasureServerPools) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (PartInfo, error) {
	if err := checkNewMultipartArgs(ctx, srcBucket, srcObject); err != nil {
		return PartInfo{}, err
	}

	return z.PutObjectPart(ctx, destBucket, destObject, uploadID, partID,
		srcInfo.PutObjReader, dstOpts)
}

// PutObjectPart - writes part of an object to hashedSet based on the object name.
func (z *erasureServerPools) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (PartInfo, error) {
	if err := checkPutObjectPartArgs(ctx, bucket, object, uploadID); err != nil {
		return PartInfo{}, err
	}

	if z.SinglePool() {
		return z.serverPools[0].PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
	}

	for idx, pool := range z.serverPools {
		if z.IsSuspended(idx) {
			continue
		}
		pi, err := pool.PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
		if err == nil {
			return pi, nil
		}
		if _, ok := err.(InvalidUploadID); ok {
			// Look for information on the next pool
			continue
		}
		// Any other unhandled errors such as quorum return.
		return PartInfo{}, err
	}

	return PartInfo{}, InvalidUploadID{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}
}

func (z *erasureServerPools) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	if err := checkListPartsArgs(ctx, bucket, object, uploadID); err != nil {
		return MultipartInfo{}, err
	}

	uploadIDLock := z.NewNSLock(bucket, pathJoin(object, uploadID))
	lkctx, err := uploadIDLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return MultipartInfo{}, err
	}
	ctx = lkctx.Context()
	defer uploadIDLock.RUnlock(lkctx)

	if z.SinglePool() {
		return z.serverPools[0].GetMultipartInfo(ctx, bucket, object, uploadID, opts)
	}

	for idx, pool := range z.serverPools {
		if z.IsSuspended(idx) {
			continue
		}
		mi, err := pool.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
		if err == nil {
			return mi, nil
		}
		if _, ok := err.(InvalidUploadID); ok {
			// upload id not found, continue to the next pool.
			continue
		}
		// any other unhandled error return right here.
		return MultipartInfo{}, err
	}

	return MultipartInfo{}, InvalidUploadID{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}
}

// ListObjectParts - lists all uploaded parts to an object in hashedSet.
func (z *erasureServerPools) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (ListPartsInfo, error) {
	if err := checkListPartsArgs(ctx, bucket, object, uploadID); err != nil {
		return ListPartsInfo{}, err
	}

	uploadIDLock := z.NewNSLock(bucket, pathJoin(object, uploadID))
	lkctx, err := uploadIDLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return ListPartsInfo{}, err
	}
	ctx = lkctx.Context()
	defer uploadIDLock.RUnlock(lkctx)

	if z.SinglePool() {
		return z.serverPools[0].ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
	}

	for idx, pool := range z.serverPools {
		if z.IsSuspended(idx) {
			continue
		}
		result, err := pool.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
		if err == nil {
			return result, nil
		}
		if _, ok := err.(InvalidUploadID); ok {
			continue
		}
		return ListPartsInfo{}, err
	}

	return ListPartsInfo{}, InvalidUploadID{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}
}

// Aborts an in-progress multipart operation on hashedSet based on the object name.
func (z *erasureServerPools) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (err error) {
	if err := checkAbortMultipartArgs(ctx, bucket, object, uploadID); err != nil {
		return err
	}

	defer func() {
		if err == nil {
			z.mpCache.Delete(uploadID)
			globalNotificationSys.DeleteUploadID(ctx, uploadID)
		}
	}()

	lk := z.NewNSLock(bucket, pathJoin(object, uploadID))
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx)

	if z.SinglePool() {
		return z.serverPools[0].AbortMultipartUpload(ctx, bucket, object, uploadID, opts)
	}

	for idx, pool := range z.serverPools {
		if z.IsSuspended(idx) {
			continue
		}
		err := pool.AbortMultipartUpload(ctx, bucket, object, uploadID, opts)
		if err == nil {
			return nil
		}
		if _, ok := err.(InvalidUploadID); ok {
			// upload id not found move to next pool
			continue
		}
		return err
	}
	return InvalidUploadID{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}
}

// CompleteMultipartUpload - completes a pending multipart transaction, on hashedSet based on object name.
func (z *erasureServerPools) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if err = checkCompleteMultipartArgs(ctx, bucket, object, uploadID); err != nil {
		return objInfo, err
	}

	defer func() {
		if err == nil {
			z.mpCache.Delete(uploadID)
			globalNotificationSys.DeleteUploadID(ctx, uploadID)
		}
	}()

	// Hold write locks to verify uploaded parts, also disallows any
	// parallel PutObjectPart() requests.
	uploadIDLock := z.NewNSLock(bucket, pathJoin(object, uploadID))
	wlkctx, err := uploadIDLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return objInfo, err
	}
	ctx = wlkctx.Context()
	defer uploadIDLock.Unlock(wlkctx)

	if z.SinglePool() {
		return z.serverPools[0].CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
	}

	for idx, pool := range z.serverPools {
		if z.IsSuspended(idx) {
			continue
		}
		objInfo, err = pool.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
		if err == nil {
			return objInfo, nil
		}
		if _, ok := err.(InvalidUploadID); ok {
			// upload id not found move to next pool
			continue
		}
		return objInfo, err
	}

	return objInfo, InvalidUploadID{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}
}

// GetBucketInfo - returns bucket info from one of the erasure coded serverPools.
func (z *erasureServerPools) GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (bucketInfo BucketInfo, err error) {
	bucketInfo, err = z.s3Peer.GetBucketInfo(ctx, bucket, opts)
	if err != nil {
		return bucketInfo, toObjectErr(err, bucket)
	}

	meta, err := globalBucketMetadataSys.Get(bucket)
	if err == nil {
		bucketInfo.Created = meta.Created
		bucketInfo.Versioning = meta.Versioning()
		bucketInfo.ObjectLocking = meta.ObjectLocking()
	}
	return bucketInfo, nil
}

// ClearUploadID deletes given uploadID from cache
func (z *erasureServerPools) ClearUploadID(uploadID string) error {
	z.mpCache.Delete(uploadID)
	return nil
}

// DeleteBucket - deletes a bucket on all serverPools simultaneously,
// even if one of the serverPools fail to delete buckets, we proceed to
// undo a successful operation.
func (z *erasureServerPools) DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	if isMinioMetaBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	// Verify if bucket is valid.
	if err := s3utils.CheckValidBucketName(bucket); err != nil {
		return BucketNameInvalid{Bucket: bucket}
	}

	if !opts.NoLock {
		// Lock the bucket name before creating.
		lk := z.NewNSLock(minioMetaTmpBucket, bucket+".lck")
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

	if !opts.Force {
		results := make(chan itemOrErr[ObjectInfo])

		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		err := z.Walk(ctx, bucket, "", results, WalkOptions{Limit: 1})
		if err != nil {
			s3LogIf(ctx, fmt.Errorf("unable to verify if the bucket %s is empty: %w", bucket, err))
			return toObjectErr(err, bucket)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, found := <-results:
			if found {
				if r.Err != nil {
					s3LogIf(ctx, fmt.Errorf("unable to verify if the bucket %s is empty: %w", bucket, r.Err))
					return toObjectErr(r.Err, bucket)
				}
				return toObjectErr(errVolumeNotEmpty, bucket)
			}
		}

		// Always pass force to the lower level
		opts.Force = true
	}

	err := z.s3Peer.DeleteBucket(ctx, bucket, opts)
	if err == nil || isErrBucketNotFound(err) {
		// If site replication is configured, hold on to deleted bucket state until sites sync
		if opts.SRDeleteOp == MarkDelete {
			z.s3Peer.MakeBucket(context.Background(), pathJoin(minioMetaBucket, bucketMetaPrefix, deletedBucketsPrefix, bucket), MakeBucketOptions{})
		}
	}

	if err == nil {
		// Purge the entire bucket metadata entirely.
		z.deleteAll(context.Background(), minioMetaBucket, pathJoin(bucketMetaPrefix, bucket))
	}

	return toObjectErr(err, bucket)
}

// deleteAll will rename bucket+prefix unconditionally across all disks to
// minioMetaTmpDeletedBucket + unique uuid,
// Note that set distribution is ignored so it should only be used in cases where
// data is not distributed across sets. Errors are logged but individual
// disk failures are not returned.
func (z *erasureServerPools) deleteAll(ctx context.Context, bucket, prefix string) {
	for _, servers := range z.serverPools {
		for _, set := range servers.sets {
			set.deleteAll(ctx, bucket, prefix)
		}
	}
}

var listBucketsCache = cachevalue.New[[]BucketInfo]()

// List all buckets from one of the serverPools, we are not doing merge
// sort here just for simplification. As per design it is assumed
// that all buckets are present on all serverPools.
func (z *erasureServerPools) ListBuckets(ctx context.Context, opts BucketOptions) (buckets []BucketInfo, err error) {
	if opts.Cached {
		listBucketsCache.InitOnce(time.Second,
			cachevalue.Opts{ReturnLastGood: true, NoWait: true},
			func(ctx context.Context) ([]BucketInfo, error) {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				buckets, err = z.s3Peer.ListBuckets(ctx, opts)
				if err != nil {
					return nil, err
				}
				if !opts.NoMetadata {
					for i := range buckets {
						createdAt, err := globalBucketMetadataSys.CreatedAt(buckets[i].Name)
						if err == nil {
							buckets[i].Created = createdAt
						}
					}
				}
				return buckets, nil
			},
		)

		return listBucketsCache.GetWithCtx(ctx)
	}

	buckets, err = z.s3Peer.ListBuckets(ctx, opts)
	if err != nil {
		return nil, err
	}

	if !opts.NoMetadata {
		for i := range buckets {
			createdAt, err := globalBucketMetadataSys.CreatedAt(buckets[i].Name)
			if err == nil {
				buckets[i].Created = createdAt
			}
		}
	}
	return buckets, nil
}

func (z *erasureServerPools) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	// Acquire lock on format.json
	formatLock := z.NewNSLock(minioMetaBucket, formatConfigFile)
	lkctx, err := formatLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return madmin.HealResultItem{}, err
	}
	ctx = lkctx.Context()
	defer formatLock.Unlock(lkctx)

	r := madmin.HealResultItem{
		Type:   madmin.HealItemMetadata,
		Detail: "disk-format",
	}

	var countNoHeal int
	for _, pool := range z.serverPools {
		result, err := pool.HealFormat(ctx, dryRun)
		if err != nil && !errors.Is(err, errNoHealRequired) {
			healingLogOnceIf(ctx, err, "erasure-heal-format")
			continue
		}
		// Count errNoHealRequired across all serverPools,
		// to return appropriate error to the caller
		if errors.Is(err, errNoHealRequired) {
			countNoHeal++
		}
		r.DiskCount += result.DiskCount
		r.SetCount += result.SetCount
		r.Before.Drives = append(r.Before.Drives, result.Before.Drives...)
		r.After.Drives = append(r.After.Drives, result.After.Drives...)
	}

	// No heal returned by all serverPools, return errNoHealRequired
	if countNoHeal == len(z.serverPools) {
		return r, errNoHealRequired
	}

	return r, nil
}

func (z *erasureServerPools) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	// .metadata.bin healing is not needed here, it is automatically healed via read() call.
	return z.s3Peer.HealBucket(ctx, bucket, opts)
}

// Walk a bucket, optionally prefix recursively, until we have returned
// all the contents of the provided bucket+prefix.
func (z *erasureServerPools) Walk(ctx context.Context, bucket, prefix string, results chan<- itemOrErr[ObjectInfo], opts WalkOptions) error {
	if err := checkListObjsArgs(ctx, bucket, prefix, ""); err != nil {
		xioutil.SafeClose(results)
		return err
	}
	parentCtx := ctx
	ctx, cancelCause := context.WithCancelCause(ctx)
	var entries []chan metaCacheEntry

	for poolIdx, erasureSet := range z.serverPools {
		for setIdx, set := range erasureSet.sets {
			set := set
			listOut := make(chan metaCacheEntry, 1)
			entries = append(entries, listOut)
			disks, infos, _ := set.getOnlineDisksWithHealingAndInfo(true)
			if len(disks) == 0 {
				xioutil.SafeClose(results)
				err := fmt.Errorf("Walk: no online disks found in (set:%d pool:%d) %w", setIdx, poolIdx, errErasureReadQuorum)
				cancelCause(err)
				return err
			}
			go func() {
				defer xioutil.SafeClose(listOut)
				send := func(e metaCacheEntry) {
					if e.isDir() {
						// Ignore directories.
						return
					}
					select {
					case listOut <- e:
					case <-ctx.Done():
					}
				}

				askDisks := getListQuorum(opts.AskDisks, set.setDriveCount)
				if askDisks == -1 {
					newDisks := getQuorumDisks(disks, infos, (len(disks)+1)/2)
					if newDisks != nil {
						// If we found disks signature in quorum, we proceed to list
						// from a single drive, shuffling of the drives is subsequently.
						disks = newDisks
						askDisks = 1
					} else {
						// If we did not find suitable disks, perform strict quorum listing
						// as no disk agrees on quorum anymore.
						askDisks = getListQuorum("strict", set.setDriveCount)
					}
				}

				// Special case: ask all disks if the drive count is 4
				if set.setDriveCount == 4 || askDisks > len(disks) {
					askDisks = len(disks) // use all available drives
				}

				var fallbackDisks []StorageAPI
				if askDisks > 0 && len(disks) > askDisks {
					rand.Shuffle(len(disks), func(i, j int) {
						disks[i], disks[j] = disks[j], disks[i]
					})
					fallbackDisks = disks[askDisks:]
					disks = disks[:askDisks]
				}

				requestedVersions := 0
				if opts.LatestOnly {
					requestedVersions = 1
				}

				// However many we ask, versions must exist on ~50%
				listingQuorum := (askDisks + 1) / 2

				// How to resolve partial results.
				resolver := metadataResolutionParams{
					dirQuorum:         listingQuorum,
					objQuorum:         listingQuorum,
					bucket:            bucket,
					requestedVersions: requestedVersions,
				}

				path := baseDirFromPrefix(prefix)
				filterPrefix := strings.Trim(strings.TrimPrefix(prefix, path), slashSeparator)
				if path == prefix {
					filterPrefix = ""
				}

				lopts := listPathRawOptions{
					disks:          disks,
					fallbackDisks:  fallbackDisks,
					bucket:         bucket,
					path:           path,
					filterPrefix:   filterPrefix,
					recursive:      true,
					forwardTo:      opts.Marker,
					perDiskLimit:   opts.Limit,
					minDisks:       listingQuorum,
					reportNotFound: false,
					agreed:         send,
					partial: func(entries metaCacheEntries, _ []error) {
						entry, ok := entries.resolve(&resolver)
						if ok {
							send(*entry)
						}
					},
					finished: nil,
				}

				if err := listPathRaw(ctx, lopts); err != nil {
					cancelCause(fmt.Errorf("listPathRaw returned %w: opts(%#v)", err, lopts))
					return
				}
			}()
		}
	}

	// Convert and filter merged entries.
	merged := make(chan metaCacheEntry, 100)
	vcfg, _ := globalBucketVersioningSys.Get(bucket)
	errCh := make(chan error, 1)
	go func() {
		sentErr := false
		sendErr := func(err error) {
			if !sentErr {
				select {
				case results <- itemOrErr[ObjectInfo]{Err: err}:
					sentErr = true
				case <-parentCtx.Done():
				}
			}
		}
		defer func() {
			select {
			case <-ctx.Done():
				sendErr(ctx.Err())
			default:
			}
			xioutil.SafeClose(results)
			cancelCause(nil)
		}()
		send := func(oi ObjectInfo) bool {
			select {
			case results <- itemOrErr[ObjectInfo]{Item: oi}:
				return true
			case <-ctx.Done():
				sendErr(context.Cause(ctx))
				return false
			}
		}
		for entry := range merged {
			if opts.LatestOnly {
				fi, err := entry.fileInfo(bucket)
				if err != nil {
					sendErr(err)
					return
				}
				if opts.Filter != nil {
					if opts.Filter(fi) {
						if !send(fi.ToObjectInfo(bucket, fi.Name, vcfg != nil && vcfg.Versioned(fi.Name))) {
							return
						}
					}
				} else {
					if !send(fi.ToObjectInfo(bucket, fi.Name, vcfg != nil && vcfg.Versioned(fi.Name))) {
						return
					}
				}
				continue
			}
			fivs, err := entry.fileInfoVersions(bucket)
			if err != nil {
				sendErr(err)
				return
			}

			// Note: entry.fileInfoVersions returns versions sorted in reverse chronological order based on ModTime
			if opts.VersionsSort == WalkVersionsSortAsc {
				versionsSorter(fivs.Versions).reverse()
			}

			for _, version := range fivs.Versions {
				if opts.Filter != nil {
					if opts.Filter(version) {
						if !send(version.ToObjectInfo(bucket, version.Name, vcfg != nil && vcfg.Versioned(version.Name))) {
							return
						}
					}
				} else {
					if !send(version.ToObjectInfo(bucket, version.Name, vcfg != nil && vcfg.Versioned(version.Name))) {
						return
					}
				}
			}
		}
		if err := <-errCh; err != nil {
			sendErr(err)
		}
	}()
	go func() {
		defer close(errCh)
		// Merge all entries from all disks.
		// We leave quorum at 1, since entries are already resolved to have the desired quorum.
		// mergeEntryChannels will close 'merged' channel upon completion or cancellation.
		errCh <- mergeEntryChannels(ctx, entries, merged, 1)
	}()

	return nil
}

// HealObjectFn closure function heals the object.
type HealObjectFn func(bucket, object, versionID string, scanMode madmin.HealScanMode) error

// List a prefix or a single object versions and heal
func (z *erasureServerPools) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, healObjectFn HealObjectFn) error {
	healEntry := func(bucket string, entry metaCacheEntry, scanMode madmin.HealScanMode) error {
		if entry.isDir() {
			return nil
		}
		// We might land at .metacache, .trash, .multipart
		// no need to heal them skip, only when bucket
		// is '.minio.sys'
		if bucket == minioMetaBucket {
			if wildcard.Match("buckets/*/.metacache/*", entry.name) {
				return nil
			}
			if wildcard.Match("tmp/*", entry.name) {
				return nil
			}
			if wildcard.Match("multipart/*", entry.name) {
				return nil
			}
			if wildcard.Match("tmp-old/*", entry.name) {
				return nil
			}
		}
		fivs, err := entry.fileInfoVersions(bucket)
		if err != nil {
			return healObjectFn(bucket, entry.name, "", scanMode)
		}
		if opts.Remove && !opts.DryRun {
			err := z.CheckAbandonedParts(ctx, bucket, entry.name, opts)
			if err != nil {
				healingLogIf(ctx, fmt.Errorf("unable to check object %s/%s for abandoned data: %w", bucket, entry.name, err))
			}
		}
		for _, version := range fivs.Versions {
			err := healObjectFn(bucket, version.Name, version.VersionID, scanMode)
			if err != nil && !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
				return err
			}
		}

		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	poolErrs := make([][]error, len(z.serverPools))
	for idx, erasureSet := range z.serverPools {
		if opts.Pool != nil && *opts.Pool != idx {
			continue
		}
		if z.IsSuspended(idx) {
			continue
		}
		errs := make([]error, len(erasureSet.sets))
		wk, _ := workers.New(3)
		for idx, set := range erasureSet.sets {
			if opts.Set != nil && *opts.Set != idx {
				continue
			}
			wk.Take()
			go func(idx int, set *erasureObjects) {
				defer wk.Give()

				errs[idx] = set.listAndHeal(ctx, bucket, prefix, opts.Recursive, opts.ScanMode, healEntry)
			}(idx, set)
		}
		wk.Wait()
		poolErrs[idx] = errs
	}
	for _, errs := range poolErrs {
		for _, err := range errs {
			if err == nil {
				continue
			}
			return err
		}
	}
	return nil
}

func (z *erasureServerPools) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	object = encodeDirObject(object)

	errs := make([]error, len(z.serverPools))
	results := make([]madmin.HealResultItem, len(z.serverPools))
	var wg sync.WaitGroup
	for idx, pool := range z.serverPools {
		if z.IsSuspended(idx) {
			continue
		}
		wg.Add(1)
		go func(idx int, pool *erasureSets) {
			defer wg.Done()
			result, err := pool.HealObject(ctx, bucket, object, versionID, opts)
			result.Object = decodeDirObject(result.Object)
			errs[idx] = err
			results[idx] = result
		}(idx, pool)
	}
	wg.Wait()

	// Return the first nil error
	for idx, err := range errs {
		if err == nil {
			return results[idx], nil
		}
	}

	// No pool returned a nil error, return the first non 'not found' error
	for idx, err := range errs {
		if !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
			return results[idx], err
		}
	}

	hr := madmin.HealResultItem{
		Type:      madmin.HealItemObject,
		Bucket:    bucket,
		Object:    object,
		VersionID: versionID,
	}

	// At this stage, all errors are 'not found'
	if versionID != "" {
		return hr, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: versionID,
		}
	}
	return hr, ObjectNotFound{
		Bucket: bucket,
		Object: object,
	}
}

func (z *erasureServerPools) getPoolAndSet(id string) (poolIdx, setIdx, diskIdx int, err error) {
	for poolIdx := range z.serverPools {
		format := z.serverPools[poolIdx].format
		for setIdx, set := range format.Erasure.Sets {
			for i, diskID := range set {
				if diskID == id {
					return poolIdx, setIdx, i, nil
				}
			}
		}
	}
	return -1, -1, -1, fmt.Errorf("DriveID(%s) %w", id, errDiskNotFound)
}

const (
	vmware = "VMWare"
)

// HealthOptions takes input options to return specific information
type HealthOptions struct {
	Maintenance    bool
	DeploymentType string
	NoLogging      bool
}

// HealthResult returns the current state of the system, also
// additionally with any specific heuristic information which
// was queried
type HealthResult struct {
	Healthy       bool
	HealthyRead   bool
	HealingDrives int
	ESHealth      []struct {
		Maintenance   bool
		PoolID, SetID int
		Healthy       bool
		HealthyRead   bool
		HealthyDrives int
		HealingDrives int
		ReadQuorum    int
		WriteQuorum   int
	}
	WriteQuorum   int
	ReadQuorum    int
	UsingDefaults bool
}

func (hr HealthResult) String() string {
	var str strings.Builder
	for i, es := range hr.ESHealth {
		str.WriteString("(Pool: ")
		str.WriteString(strconv.Itoa(es.PoolID))
		str.WriteString(" Set: ")
		str.WriteString(strconv.Itoa(es.SetID))
		str.WriteString(" Healthy: ")
		str.WriteString(strconv.FormatBool(es.Healthy))
		if i == 0 {
			str.WriteString(")")
		} else {
			str.WriteString(") | ")
		}
	}
	return str.String()
}

// Health - returns current status of the object layer health,
// provides if write access exists across sets, additionally
// can be used to query scenarios if health may be lost
// if this node is taken down by an external orchestrator.
func (z *erasureServerPools) Health(ctx context.Context, opts HealthOptions) HealthResult {
	reqInfo := (&logger.ReqInfo{}).AppendTags("maintenance", strconv.FormatBool(opts.Maintenance))

	type setInfo struct {
		online  int
		healing int
	}

	var drivesHealing int

	erasureSetUpCount := make([][]setInfo, len(z.serverPools))
	for i := range z.serverPools {
		erasureSetUpCount[i] = make([]setInfo, len(z.serverPools[i].sets))
	}

	storageInfo := z.StorageInfo(ctx, false)

	for _, disk := range storageInfo.Disks {
		if opts.Maintenance {
			globalLocalDrivesMu.RLock()
			_, ok := globalLocalDrivesMap[disk.Endpoint]
			globalLocalDrivesMu.RUnlock()
			if ok {
				continue
			}
		}

		if disk.PoolIndex > -1 && disk.SetIndex > -1 {
			if disk.State == madmin.DriveStateOk {
				si := erasureSetUpCount[disk.PoolIndex][disk.SetIndex]
				si.online++
				if disk.Healing {
					si.healing++
					drivesHealing++
				}
				erasureSetUpCount[disk.PoolIndex][disk.SetIndex] = si
			}
		}
	}

	b := z.BackendInfo()
	poolReadQuorums := make([]int, len(b.StandardSCData))
	poolWriteQuorums := make([]int, len(b.StandardSCData))
	for i, data := range b.StandardSCData {
		poolReadQuorums[i] = data
		poolWriteQuorums[i] = data
		if data == b.StandardSCParity {
			poolWriteQuorums[i] = data + 1
		}
	}

	// Check if disks are healing on in-case of VMware vsphere deployments.
	if opts.Maintenance && opts.DeploymentType == vmware {
		if drivesHealing > 0 {
			healingLogIf(logger.SetReqInfo(ctx, reqInfo), fmt.Errorf("Total drives to be healed %d", drivesHealing))
		}
	}

	var usingDefaults bool
	if globalStorageClass.GetParityForSC(storageclass.STANDARD) < 0 {
		usingDefaults = true
	}

	var maximumWriteQuorum int
	for _, writeQuorum := range poolWriteQuorums {
		if maximumWriteQuorum == 0 {
			maximumWriteQuorum = writeQuorum
		}
		if writeQuorum > maximumWriteQuorum {
			maximumWriteQuorum = writeQuorum
		}
	}

	var maximumReadQuorum int
	for _, readQuorum := range poolReadQuorums {
		if maximumReadQuorum == 0 {
			maximumReadQuorum = readQuorum
		}
		if readQuorum > maximumReadQuorum {
			maximumReadQuorum = readQuorum
		}
	}

	result := HealthResult{
		Healthy:       true,
		HealthyRead:   true,
		WriteQuorum:   maximumWriteQuorum,
		ReadQuorum:    maximumReadQuorum,
		UsingDefaults: usingDefaults, // indicates if config was not initialized and we are using defaults on this node.
	}

	for poolIdx := range erasureSetUpCount {
		for setIdx := range erasureSetUpCount[poolIdx] {
			result.ESHealth = append(result.ESHealth, struct {
				Maintenance                  bool
				PoolID, SetID                int
				Healthy                      bool
				HealthyRead                  bool
				HealthyDrives, HealingDrives int
				ReadQuorum, WriteQuorum      int
			}{
				Maintenance:   opts.Maintenance,
				SetID:         setIdx,
				PoolID:        poolIdx,
				Healthy:       erasureSetUpCount[poolIdx][setIdx].online >= poolWriteQuorums[poolIdx],
				HealthyRead:   erasureSetUpCount[poolIdx][setIdx].online >= poolReadQuorums[poolIdx],
				HealthyDrives: erasureSetUpCount[poolIdx][setIdx].online,
				HealingDrives: erasureSetUpCount[poolIdx][setIdx].healing,
				ReadQuorum:    poolReadQuorums[poolIdx],
				WriteQuorum:   poolWriteQuorums[poolIdx],
			})

			healthy := erasureSetUpCount[poolIdx][setIdx].online >= poolWriteQuorums[poolIdx]
			if !healthy && !opts.NoLogging {
				storageLogIf(logger.SetReqInfo(ctx, reqInfo),
					fmt.Errorf("Write quorum could not be established on pool: %d, set: %d, expected write quorum: %d, drives-online: %d",
						poolIdx, setIdx, poolWriteQuorums[poolIdx], erasureSetUpCount[poolIdx][setIdx].online), logger.FatalKind)
			}
			result.Healthy = result.Healthy && healthy

			healthyRead := erasureSetUpCount[poolIdx][setIdx].online >= poolReadQuorums[poolIdx]
			if !healthyRead && !opts.NoLogging {
				storageLogIf(logger.SetReqInfo(ctx, reqInfo),
					fmt.Errorf("Read quorum could not be established on pool: %d, set: %d, expected read quorum: %d, drives-online: %d",
						poolIdx, setIdx, poolReadQuorums[poolIdx], erasureSetUpCount[poolIdx][setIdx].online))
			}
			result.HealthyRead = result.HealthyRead && healthyRead
		}
	}

	if opts.Maintenance {
		result.Healthy = result.Healthy && drivesHealing == 0
		result.HealthyRead = result.HealthyRead && drivesHealing == 0
		result.HealingDrives = drivesHealing
	}

	return result
}

// PutObjectMetadata - replace or add tags to an existing object
func (z *erasureServerPools) PutObjectMetadata(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	object = encodeDirObject(object)
	if z.SinglePool() {
		return z.serverPools[0].PutObjectMetadata(ctx, bucket, object, opts)
	}

	if !opts.NoLock {
		// Lock the object before updating metadata.
		lk := z.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

	opts.MetadataChg = true
	opts.NoLock = true
	// We don't know the size here set 1GiB at least.
	idx, err := z.getPoolIdxExistingWithOpts(ctx, bucket, object, opts)
	if err != nil {
		return ObjectInfo{}, err
	}

	return z.serverPools[idx].PutObjectMetadata(ctx, bucket, object, opts)
}

// PutObjectTags - replace or add tags to an existing object
func (z *erasureServerPools) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) (ObjectInfo, error) {
	object = encodeDirObject(object)
	if z.SinglePool() {
		return z.serverPools[0].PutObjectTags(ctx, bucket, object, tags, opts)
	}

	if !opts.NoLock {
		// Lock the object before updating tags.
		lk := z.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

	opts.MetadataChg = true
	opts.NoLock = true

	// We don't know the size here set 1GiB at least.
	idx, err := z.getPoolIdxExistingWithOpts(ctx, bucket, object, opts)
	if err != nil {
		return ObjectInfo{}, err
	}

	return z.serverPools[idx].PutObjectTags(ctx, bucket, object, tags, opts)
}

// DeleteObjectTags - delete object tags from an existing object
func (z *erasureServerPools) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	object = encodeDirObject(object)
	if z.SinglePool() {
		return z.serverPools[0].DeleteObjectTags(ctx, bucket, object, opts)
	}

	if !opts.NoLock {
		// Lock the object before deleting tags.
		lk := z.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

	opts.MetadataChg = true
	opts.NoLock = true
	idx, err := z.getPoolIdxExistingWithOpts(ctx, bucket, object, opts)
	if err != nil {
		return ObjectInfo{}, err
	}

	return z.serverPools[idx].DeleteObjectTags(ctx, bucket, object, opts)
}

// GetObjectTags - get object tags from an existing object
func (z *erasureServerPools) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	object = encodeDirObject(object)
	if z.SinglePool() {
		return z.serverPools[0].GetObjectTags(ctx, bucket, object, opts)
	}

	oi, _, err := z.getLatestObjectInfoWithIdx(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	return tags.ParseObjectTags(oi.UserTags)
}

// TransitionObject - transition object content to target tier.
func (z *erasureServerPools) TransitionObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	object = encodeDirObject(object)
	if z.SinglePool() {
		return z.serverPools[0].TransitionObject(ctx, bucket, object, opts)
	}

	if !opts.NoLock {
		// Acquire write lock before starting to transition the object.
		lk := z.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalDeleteOperationTimeout)
		if err != nil {
			return err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

	// Avoid transitioning an object from a pool being decommissioned.
	opts.SkipDecommissioned = true
	opts.NoLock = true
	idx, err := z.getPoolIdxExistingWithOpts(ctx, bucket, object, opts)
	if err != nil {
		return err
	}

	return z.serverPools[idx].TransitionObject(ctx, bucket, object, opts)
}

// RestoreTransitionedObject - restore transitioned object content locally on this cluster.
func (z *erasureServerPools) RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	object = encodeDirObject(object)
	if z.SinglePool() {
		return z.serverPools[0].RestoreTransitionedObject(ctx, bucket, object, opts)
	}

	if !opts.NoLock {
		// Acquire write lock before restoring transitioned object
		lk := z.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalDeleteOperationTimeout)
		if err != nil {
			return err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

	// Avoid restoring object from a pool being decommissioned.
	opts.SkipDecommissioned = true
	opts.NoLock = true
	idx, err := z.getPoolIdxExistingWithOpts(ctx, bucket, object, opts)
	if err != nil {
		return err
	}

	return z.serverPools[idx].RestoreTransitionedObject(ctx, bucket, object, opts)
}

func (z *erasureServerPools) CheckAbandonedParts(ctx context.Context, bucket, object string, opts madmin.HealOpts) error {
	object = encodeDirObject(object)
	if z.SinglePool() {
		return z.serverPools[0].CheckAbandonedParts(ctx, bucket, object, opts)
	}
	errs := make([]error, len(z.serverPools))
	var wg sync.WaitGroup
	for idx, pool := range z.serverPools {
		if z.IsSuspended(idx) {
			continue
		}
		wg.Add(1)
		go func(idx int, pool *erasureSets) {
			defer wg.Done()
			err := pool.CheckAbandonedParts(ctx, bucket, object, opts)
			if err != nil && !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
				errs[idx] = err
			}
		}(idx, pool)
	}
	wg.Wait()
	for _, err := range errs {
		return err
	}
	return nil
}

// DecomTieredObject - moves tiered object to another pool during decommissioning.
func (z *erasureServerPools) DecomTieredObject(ctx context.Context, bucket, object string, fi FileInfo, opts ObjectOptions) error {
	object = encodeDirObject(object)
	if z.SinglePool() {
		return fmt.Errorf("error decommissioning %s/%s", bucket, object)
	}
	if !opts.NoLock {
		ns := z.NewNSLock(bucket, object)
		lkctx, err := ns.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return err
		}
		ctx = lkctx.Context()
		defer ns.Unlock(lkctx)
		opts.NoLock = true
	}
	idx, err := z.getPoolIdxNoLock(ctx, bucket, object, fi.Size)
	if err != nil {
		return err
	}

	if opts.DataMovement && idx == opts.SrcPoolIdx {
		return DataMovementOverwriteErr{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
			Err:       errDataMovementSrcDstPoolSame,
		}
	}

	return z.serverPools[idx].DecomTieredObject(ctx, bucket, object, fi, opts)
}
