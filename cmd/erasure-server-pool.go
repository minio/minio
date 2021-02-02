/*
 * MinIO Cloud Storage, (C) 2019,2020 MinIO, Inc.
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
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/cmd/config/storageclass"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/sync/errgroup"
)

type erasureServerPools struct {
	GatewayUnsupported

	serverPools []*erasureSets

	// Shut down async operations
	shutdown context.CancelFunc
}

func (z *erasureServerPools) SinglePool() bool {
	return len(z.serverPools) == 1
}

// Initialize new pool of erasure sets.
func newErasureServerPools(ctx context.Context, endpointServerPools EndpointServerPools) (ObjectLayer, error) {
	var (
		deploymentID       string
		distributionAlgo   string
		commonParityDrives int
		err                error

		formats      = make([]*formatErasureV3, len(endpointServerPools))
		storageDisks = make([][]StorageAPI, len(endpointServerPools))
		z            = &erasureServerPools{serverPools: make([]*erasureSets, len(endpointServerPools))}
	)

	var localDrives []string

	local := endpointServerPools.FirstLocal()
	for i, ep := range endpointServerPools {
		for _, endpoint := range ep.Endpoints {
			if endpoint.IsLocal {
				localDrives = append(localDrives, endpoint.Path)
			}
		}

		// If storage class is not set during startup, default values are used
		// -- Default for Reduced Redundancy Storage class is, parity = 2
		// -- Default for Standard Storage class is, parity = 2 - disks 4, 5
		// -- Default for Standard Storage class is, parity = 3 - disks 6, 7
		// -- Default for Standard Storage class is, parity = 4 - disks 8 to 16
		if commonParityDrives == 0 {
			commonParityDrives = ecDrivesNoConfig(ep.DrivesPerSet)
		}

		if err = storageclass.ValidateParity(commonParityDrives, ep.DrivesPerSet); err != nil {
			return nil, fmt.Errorf("All current serverPools should have same parity ratio - expected %d, got %d", commonParityDrives, ecDrivesNoConfig(ep.DrivesPerSet))
		}

		storageDisks[i], formats[i], err = waitForFormatErasure(local, ep.Endpoints, i+1,
			ep.SetCount, ep.DrivesPerSet, deploymentID, distributionAlgo)
		if err != nil {
			return nil, err
		}

		if deploymentID == "" {
			// all zones should have same deployment ID
			deploymentID = formats[i].ID
		}

		if distributionAlgo == "" {
			distributionAlgo = formats[i].Erasure.DistributionAlgo
		}

		// Validate if users brought different DeploymentID pools.
		if deploymentID != formats[i].ID {
			return nil, fmt.Errorf("All serverPools should have same deployment ID expected %s, got %s", deploymentID, formats[i].ID)
		}

		// Validate if users brought different different distribution algo pools.
		if distributionAlgo != formats[i].Erasure.DistributionAlgo {
			return nil, fmt.Errorf("All serverPools should have same distributionAlgo expected %s, got %s", distributionAlgo, formats[i].Erasure.DistributionAlgo)
		}

		z.serverPools[i], err = newErasureSets(ctx, ep.Endpoints, storageDisks[i], formats[i], commonParityDrives, i)
		if err != nil {
			return nil, err
		}
	}
	ctx, z.shutdown = context.WithCancel(ctx)
	go intDataUpdateTracker.start(ctx, localDrives...)
	return z, nil
}

func (z *erasureServerPools) NewNSLock(bucket string, objects ...string) RWLocker {
	return z.serverPools[0].NewNSLock(bucket, objects...)
}

// GetDisksID will return disks by their ID.
func (z *erasureServerPools) GetDisksID(ids ...string) []StorageAPI {
	idMap := make(map[string]struct{})
	for _, id := range ids {
		idMap[id] = struct{}{}
	}
	res := make([]StorageAPI, 0, len(idMap))
	for _, s := range z.serverPools {
		s.erasureDisksMu.RLock()
		defer s.erasureDisksMu.RUnlock()
		for _, disks := range s.erasureDisks {
			for _, disk := range disks {
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

func (z *erasureServerPools) SetDriveCounts() []int {
	setDriveCounts := make([]int, len(z.serverPools))
	for i := range z.serverPools {
		setDriveCounts[i] = z.serverPools[i].SetDriveCount()
	}
	return setDriveCounts
}

type serverPoolsAvailableSpace []poolAvailableSpace

type poolAvailableSpace struct {
	Index     int
	Available uint64
}

// TotalAvailable - total available space
func (p serverPoolsAvailableSpace) TotalAvailable() uint64 {
	total := uint64(0)
	for _, z := range p {
		total += z.Available
	}
	return total
}

// getAvailablePoolIdx will return an index that can hold size bytes.
// -1 is returned if no serverPools have available space for the size given.
func (z *erasureServerPools) getAvailablePoolIdx(ctx context.Context, size int64) int {
	serverPools := z.getServerPoolsAvailableSpace(ctx, size)
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
	logger.LogIf(ctx, fmt.Errorf("reached end of serverPools (total: %v, atTotal: %v, choose: %v)", total, atTotal, choose))
	return -1
}

// getServerPoolsAvailableSpace will return the available space of each pool after storing the content.
// If there is not enough space the pool will return 0 bytes available.
// Negative sizes are seen as 0 bytes.
func (z *erasureServerPools) getServerPoolsAvailableSpace(ctx context.Context, size int64) serverPoolsAvailableSpace {
	if size < 0 {
		size = 0
	}
	var serverPools = make(serverPoolsAvailableSpace, len(z.serverPools))

	storageInfos := make([]StorageInfo, len(z.serverPools))
	g := errgroup.WithNErrs(len(z.serverPools))
	for index := range z.serverPools {
		index := index
		g.Go(func() error {
			storageInfos[index] = z.serverPools[index].StorageUsageInfo(ctx)
			return nil
		}, index)
	}

	// Wait for the go routines.
	g.Wait()

	for i, zinfo := range storageInfos {
		var available uint64
		var total uint64
		for _, disk := range zinfo.Disks {
			total += disk.TotalSpace
			available += disk.TotalSpace - disk.UsedSpace
		}
		// Make sure we can fit "size" on to the disk without getting above the diskFillFraction
		if available < uint64(size) {
			available = 0
		}
		if available > 0 {
			// How much will be left after adding the file.
			available -= -uint64(size)

			// wantLeft is how much space there at least must be left.
			wantLeft := uint64(float64(total) * (1.0 - diskFillFraction))
			if available <= wantLeft {
				available = 0
			}
		}
		serverPools[i] = poolAvailableSpace{
			Index:     i,
			Available: available,
		}
	}
	return serverPools
}

// getPoolIdx returns the found previous object and its corresponding pool idx,
// if none are found falls back to most available space pool.
func (z *erasureServerPools) getPoolIdx(ctx context.Context, bucket, object string, opts ObjectOptions, size int64) (idx int, err error) {
	if z.SinglePool() {
		return 0, nil
	}
	for i, pool := range z.serverPools {
		objInfo, err := pool.GetObjectInfo(ctx, bucket, object, opts)
		switch err.(type) {
		case ObjectNotFound:
			// VersionId was not specified but found delete marker or no versions exist.
		case MethodNotAllowed:
			// VersionId was specified but found delete marker
		default:
			if err != nil {
				// any other un-handled errors return right here.
				return -1, err
			}
		}
		// delete marker not specified means no versions
		// exist continue to next pool.
		if !objInfo.DeleteMarker && err != nil {
			continue
		}
		// Success case and when DeleteMarker is true return.
		return i, nil
	}

	// We multiply the size by 2 to account for erasure coding.
	idx = z.getAvailablePoolIdx(ctx, size*2)
	if idx < 0 {
		return -1, toObjectErr(errDiskFull)
	}
	return idx, nil
}

func (z *erasureServerPools) Shutdown(ctx context.Context) error {
	defer z.shutdown()

	g := errgroup.WithNErrs(len(z.serverPools))

	for index := range z.serverPools {
		index := index
		g.Go(func() error {
			return z.serverPools[index].Shutdown(ctx)
		}, index)
	}

	for _, err := range g.Wait() {
		if err != nil {
			logger.LogIf(ctx, err)
		}
		// let's the rest shutdown
	}
	return nil
}

func (z *erasureServerPools) BackendInfo() (b madmin.BackendInfo) {
	b.Type = madmin.Erasure

	scParity := globalStorageClass.GetParityForSC(storageclass.STANDARD)
	if scParity <= 0 {
		scParity = z.serverPools[0].defaultParityCount
	}
	rrSCParity := globalStorageClass.GetParityForSC(storageclass.RRS)

	// Data blocks can vary per pool, but parity is same.
	for _, setDriveCount := range z.SetDriveCounts() {
		b.StandardSCData = append(b.StandardSCData, setDriveCount-scParity)
		b.RRSCData = append(b.RRSCData, setDriveCount-rrSCParity)
	}

	b.StandardSCParity = scParity
	b.RRSCParity = rrSCParity
	return
}

func (z *erasureServerPools) StorageInfo(ctx context.Context) (StorageInfo, []error) {
	var storageInfo StorageInfo

	storageInfos := make([]StorageInfo, len(z.serverPools))
	storageInfosErrs := make([][]error, len(z.serverPools))
	g := errgroup.WithNErrs(len(z.serverPools))
	for index := range z.serverPools {
		index := index
		g.Go(func() error {
			storageInfos[index], storageInfosErrs[index] = z.serverPools[index].StorageInfo(ctx)
			return nil
		}, index)
	}

	// Wait for the go routines.
	g.Wait()

	storageInfo.Backend = z.BackendInfo()
	for _, lstorageInfo := range storageInfos {
		storageInfo.Disks = append(storageInfo.Disks, lstorageInfo.Disks...)
	}

	var errs []error
	for i := range z.serverPools {
		errs = append(errs, storageInfosErrs[i]...)
	}
	return storageInfo, errs
}

func (z *erasureServerPools) CrawlAndGetDataUsage(ctx context.Context, bf *bloomFilter, updates chan<- DataUsageInfo) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []dataUsageCache
	var firstErr error

	allBuckets, err := z.ListBuckets(ctx)
	if err != nil {
		return err
	}

	if len(allBuckets) == 0 {
		logger.Info(color.Green("data-crawl:") + " No buckets found, skipping crawl")
		updates <- DataUsageInfo{} // no buckets found update data usage to reflect latest state
		return nil
	}

	// Crawl latest allBuckets first.
	sort.Slice(allBuckets, func(i, j int) bool {
		return allBuckets[i].Created.After(allBuckets[j].Created)
	})

	// Collect for each set in serverPools.
	for _, z := range z.serverPools {
		for _, erObj := range z.sets {
			wg.Add(1)
			results = append(results, dataUsageCache{})
			go func(i int, erObj *erasureObjects) {
				updates := make(chan dataUsageCache, 1)
				defer close(updates)
				// Start update collector.
				go func() {
					defer wg.Done()
					for info := range updates {
						mu.Lock()
						results[i] = info
						mu.Unlock()
					}
				}()
				// Start crawler. Blocks until done.
				err := erObj.crawlAndGetDataUsage(ctx, allBuckets, bf, updates)
				if err != nil {
					logger.LogIf(ctx, err)
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					// Cancel remaining...
					cancel()
					mu.Unlock()
					return
				}
			}(len(results)-1, erObj)
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
				// Enforce quotas when all is done.
				if firstErr == nil {
					for _, b := range allBuckets {
						enforceFIFOQuotaBucket(ctx, z, b.Name, allMerged.bucketUsageInfo(b.Name))
					}
				}
				close(v)
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
		if firstErr == nil {
			firstErr = ctx.Err()
		}
	}
	return firstErr
}

// MakeBucketWithLocation - creates a new bucket across all serverPools simultaneously
// even if one of the sets fail to create buckets, we proceed all the successful
// operations.
func (z *erasureServerPools) MakeBucketWithLocation(ctx context.Context, bucket string, opts BucketOptions) error {
	g := errgroup.WithNErrs(len(z.serverPools))

	// Create buckets in parallel across all sets.
	for index := range z.serverPools {
		index := index
		g.Go(func() error {
			return z.serverPools[index].MakeBucketWithLocation(ctx, bucket, opts)
		}, index)
	}

	errs := g.Wait()
	// Return the first encountered error
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	// If it doesn't exist we get a new, so ignore errors
	meta := newBucketMetadata(bucket)
	if opts.LockEnabled {
		meta.VersioningConfigXML = enabledBucketVersioningConfig
		meta.ObjectLockConfigXML = enabledBucketObjectLockConfig
	}

	if err := meta.Save(ctx, z); err != nil {
		return toObjectErr(err, bucket)
	}

	globalBucketMetadataSys.Set(bucket, meta)

	// Success.
	return nil

}

func (z *erasureServerPools) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}

	object = encodeDirObject(object)

	for _, pool := range z.serverPools {
		gr, err = pool.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
		if err != nil {
			if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
				continue
			}
			return gr, err
		}
		return gr, nil
	}
	if opts.VersionID != "" {
		return gr, VersionNotFound{Bucket: bucket, Object: object, VersionID: opts.VersionID}
	}
	return gr, ObjectNotFound{Bucket: bucket, Object: object}
}

func (z *erasureServerPools) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) error {
	if err := checkGetObjArgs(ctx, bucket, object); err != nil {
		return err
	}

	object = encodeDirObject(object)

	for _, pool := range z.serverPools {
		if err := pool.GetObject(ctx, bucket, object, startOffset, length, writer, etag, opts); err != nil {
			if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
				continue
			}
			return err
		}
		return nil
	}
	if opts.VersionID != "" {
		return VersionNotFound{Bucket: bucket, Object: object, VersionID: opts.VersionID}
	}
	return ObjectNotFound{Bucket: bucket, Object: object}
}

func (z *erasureServerPools) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return objInfo, err
	}

	object = encodeDirObject(object)
	for _, pool := range z.serverPools {
		objInfo, err = pool.GetObjectInfo(ctx, bucket, object, opts)
		if err != nil {
			if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
				continue
			}
			return objInfo, err
		}
		return objInfo, nil
	}
	object = decodeDirObject(object)
	if opts.VersionID != "" {
		return objInfo, VersionNotFound{Bucket: bucket, Object: object, VersionID: opts.VersionID}
	}
	return objInfo, ObjectNotFound{Bucket: bucket, Object: object}
}

// PutObject - writes an object to least used erasure pool.
func (z *erasureServerPools) PutObject(ctx context.Context, bucket string, object string, data *PutObjReader, opts ObjectOptions) (ObjectInfo, error) {
	// Validate put object input args.
	if err := checkPutObjectArgs(ctx, bucket, object, z); err != nil {
		return ObjectInfo{}, err
	}

	object = encodeDirObject(object)

	if z.SinglePool() {
		return z.serverPools[0].PutObject(ctx, bucket, object, data, opts)
	}

	idx, err := z.getPoolIdx(ctx, bucket, object, opts, data.Size())
	if err != nil {
		return ObjectInfo{}, err
	}

	// Overwrite the object at the right pool
	return z.serverPools[idx].PutObject(ctx, bucket, object, data, opts)
}

func (z *erasureServerPools) DeleteObject(ctx context.Context, bucket string, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if err = checkDelObjArgs(ctx, bucket, object); err != nil {
		return objInfo, err
	}

	object = encodeDirObject(object)

	if z.SinglePool() {
		return z.serverPools[0].DeleteObject(ctx, bucket, object, opts)
	}
	for _, pool := range z.serverPools {
		objInfo, err = pool.DeleteObject(ctx, bucket, object, opts)
		if err == nil {
			return objInfo, nil
		}
		if err != nil && !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
			break
		}
	}

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
	if err := multiDeleteLock.GetLock(ctx, globalOperationTimeout); err != nil {
		for i := range derrs {
			derrs[i] = err
		}
		return dobjects, derrs
	}
	defer multiDeleteLock.Unlock()

	if z.SinglePool() {
		return z.serverPools[0].DeleteObjects(ctx, bucket, objects, opts)
	}

	for _, pool := range z.serverPools {
		deletedObjects, errs := pool.DeleteObjects(ctx, bucket, objects, opts)
		for i, derr := range errs {
			if derr != nil {
				derrs[i] = derr
			}
			dobjects[i] = deletedObjects[i]
		}
	}
	return dobjects, derrs
}

func (z *erasureServerPools) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	srcObject = encodeDirObject(srcObject)
	dstObject = encodeDirObject(dstObject)

	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))

	poolIdx, err := z.getPoolIdx(ctx, dstBucket, dstObject, dstOpts, srcInfo.Size)
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
	}

	return z.serverPools[poolIdx].PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, putOpts)
}

func (z *erasureServerPools) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error) {
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	loi, err := z.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
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
		Limit:       maxKeys,
		Marker:      marker,
		InclDeleted: true,
		AskDisks:    globalAPIConfig.getListQuorum(),
	}

	// Shortcut for APN/1.0 Veeam/1.0 Backup/10.0
	// It requests unique blocks with a specific prefix.
	// We skip scanning the parent directory for
	// more objects matching the prefix.
	ri := logger.GetReqInfo(ctx)
	if ri != nil && strings.Contains(ri.UserAgent, `1.0 Veeam/1.0 Backup`) && strings.HasSuffix(prefix, ".blk") {
		opts.discardResult = true
		opts.Transient = true
	}

	merged, err := z.listPath(ctx, opts)
	if err != nil && err != io.EOF {
		return loi, err
	}
	objects := merged.fileInfoVersions(bucket, prefix, delimiter, versionMarker)
	loi.IsTruncated = err == nil && len(objects) > 0
	if maxKeys > 0 && len(objects) > maxKeys {
		objects = objects[:maxKeys]
		loi.IsTruncated = true
	}
	for _, obj := range objects {
		if obj.IsDir && delimiter != "" {
			loi.Prefixes = append(loi.Prefixes, obj.Name)
		} else {
			loi.Objects = append(loi.Objects, obj)
		}
	}
	if loi.IsTruncated {
		last := objects[len(objects)-1]
		loi.NextMarker = encodeMarker(last.Name, merged.listID)
		loi.NextVersionIDMarker = last.VersionID
	}
	return loi, nil
}

func (z *erasureServerPools) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	var loi ListObjectsInfo

	merged, err := z.listPath(ctx, listPathOptions{
		Bucket:      bucket,
		Prefix:      prefix,
		Separator:   delimiter,
		Limit:       maxKeys,
		Marker:      marker,
		InclDeleted: false,
		AskDisks:    globalAPIConfig.getListQuorum(),
	})
	if err != nil && err != io.EOF {
		logger.LogIf(ctx, err)
		return loi, err
	}

	// Default is recursive, if delimiter is set then list non recursive.
	objects := merged.fileInfos(bucket, prefix, delimiter)
	loi.IsTruncated = err == nil && len(objects) > 0
	if maxKeys > 0 && len(objects) > maxKeys {
		objects = objects[:maxKeys]
		loi.IsTruncated = true
	}
	for _, obj := range objects {
		if obj.IsDir && delimiter != "" {
			loi.Prefixes = append(loi.Prefixes, obj.Name)
		} else {
			loi.Objects = append(loi.Objects, obj)
		}
	}
	if loi.IsTruncated {
		last := objects[len(objects)-1]
		loi.NextMarker = encodeMarker(last.Name, merged.listID)
	}
	return loi, nil
}

func (z *erasureServerPools) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	if err := checkListMultipartArgs(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, z); err != nil {
		return ListMultipartsInfo{}, err
	}

	if z.SinglePool() {
		return z.serverPools[0].ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	}

	var poolResult = ListMultipartsInfo{}
	poolResult.MaxUploads = maxUploads
	poolResult.KeyMarker = keyMarker
	poolResult.Prefix = prefix
	poolResult.Delimiter = delimiter
	for _, pool := range z.serverPools {
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
func (z *erasureServerPools) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (string, error) {
	if err := checkNewMultipartArgs(ctx, bucket, object, z); err != nil {
		return "", err
	}

	if z.SinglePool() {
		return z.serverPools[0].NewMultipartUpload(ctx, bucket, object, opts)
	}

	// We don't know the exact size, so we ask for at least 1GiB file.
	idx, err := z.getPoolIdx(ctx, bucket, object, opts, 1<<30)
	if err != nil {
		return "", err
	}

	return z.serverPools[idx].NewMultipartUpload(ctx, bucket, object, opts)
}

// Copies a part of an object from source hashedSet to destination hashedSet.
func (z *erasureServerPools) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (PartInfo, error) {
	if err := checkNewMultipartArgs(ctx, srcBucket, srcObject, z); err != nil {
		return PartInfo{}, err
	}

	return z.PutObjectPart(ctx, destBucket, destObject, uploadID, partID,
		NewPutObjReader(srcInfo.Reader, nil, nil), dstOpts)
}

// PutObjectPart - writes part of an object to hashedSet based on the object name.
func (z *erasureServerPools) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (PartInfo, error) {
	if err := checkPutObjectPartArgs(ctx, bucket, object, z); err != nil {
		return PartInfo{}, err
	}

	if z.SinglePool() {
		return z.serverPools[0].PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
	}

	for _, pool := range z.serverPools {
		_, err := pool.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
		if err == nil {
			return pool.PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
		}
		switch err.(type) {
		case InvalidUploadID:
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
	if err := checkListPartsArgs(ctx, bucket, object, z); err != nil {
		return MultipartInfo{}, err
	}

	if z.SinglePool() {
		return z.serverPools[0].GetMultipartInfo(ctx, bucket, object, uploadID, opts)
	}
	for _, pool := range z.serverPools {
		mi, err := pool.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
		if err == nil {
			return mi, nil
		}
		switch err.(type) {
		case InvalidUploadID:
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
	if err := checkListPartsArgs(ctx, bucket, object, z); err != nil {
		return ListPartsInfo{}, err
	}

	if z.SinglePool() {
		return z.serverPools[0].ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
	}
	for _, pool := range z.serverPools {
		_, err := pool.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
		if err == nil {
			return pool.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
		}
		switch err.(type) {
		case InvalidUploadID:
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
func (z *erasureServerPools) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) error {
	if err := checkAbortMultipartArgs(ctx, bucket, object, z); err != nil {
		return err
	}

	if z.SinglePool() {
		return z.serverPools[0].AbortMultipartUpload(ctx, bucket, object, uploadID, opts)
	}

	for _, pool := range z.serverPools {
		_, err := pool.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
		if err == nil {
			return pool.AbortMultipartUpload(ctx, bucket, object, uploadID, opts)
		}
		switch err.(type) {
		case InvalidUploadID:
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
	if err = checkCompleteMultipartArgs(ctx, bucket, object, z); err != nil {
		return objInfo, err
	}

	if z.SinglePool() {
		return z.serverPools[0].CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
	}

	// Purge any existing object.
	for _, pool := range z.serverPools {
		pool.DeleteObject(ctx, bucket, object, opts)
	}

	for _, pool := range z.serverPools {
		result, err := pool.ListMultipartUploads(ctx, bucket, object, "", "", "", maxUploadsList)
		if err != nil {
			return objInfo, err
		}
		if result.Lookup(uploadID) {
			return pool.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
		}
	}
	return objInfo, InvalidUploadID{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}
}

// GetBucketInfo - returns bucket info from one of the erasure coded serverPools.
func (z *erasureServerPools) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error) {
	if z.SinglePool() {
		bucketInfo, err = z.serverPools[0].GetBucketInfo(ctx, bucket)
		if err != nil {
			return bucketInfo, err
		}
		meta, err := globalBucketMetadataSys.Get(bucket)
		if err == nil {
			bucketInfo.Created = meta.Created
		}
		return bucketInfo, nil
	}
	for _, pool := range z.serverPools {
		bucketInfo, err = pool.GetBucketInfo(ctx, bucket)
		if err != nil {
			if isErrBucketNotFound(err) {
				continue
			}
			return bucketInfo, err
		}
		meta, err := globalBucketMetadataSys.Get(bucket)
		if err == nil {
			bucketInfo.Created = meta.Created
		}
		return bucketInfo, nil
	}
	return bucketInfo, BucketNotFound{
		Bucket: bucket,
	}
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (z *erasureServerPools) IsNotificationSupported() bool {
	return true
}

// IsListenSupported returns whether listen bucket notification is applicable for this layer.
func (z *erasureServerPools) IsListenSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (z *erasureServerPools) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (z *erasureServerPools) IsCompressionSupported() bool {
	return true
}

func (z *erasureServerPools) IsTaggingSupported() bool {
	return true
}

// DeleteBucket - deletes a bucket on all serverPools simultaneously,
// even if one of the serverPools fail to delete buckets, we proceed to
// undo a successful operation.
func (z *erasureServerPools) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	if z.SinglePool() {
		return z.serverPools[0].DeleteBucket(ctx, bucket, forceDelete)
	}
	g := errgroup.WithNErrs(len(z.serverPools))

	// Delete buckets in parallel across all serverPools.
	for index := range z.serverPools {
		index := index
		g.Go(func() error {
			return z.serverPools[index].DeleteBucket(ctx, bucket, forceDelete)
		}, index)
	}

	errs := g.Wait()

	// For any write quorum failure, we undo all the delete
	// buckets operation by creating all the buckets again.
	for _, err := range errs {
		if err != nil {
			if _, ok := err.(InsufficientWriteQuorum); ok {
				undoDeleteBucketServerPools(ctx, bucket, z.serverPools, errs)
			}

			return err
		}
	}

	// Success.
	return nil
}

// deleteAll will delete a bucket+prefix unconditionally across all disks.
// Note that set distribution is ignored so it should only be used in cases where
// data is not distributed across sets.
// Errors are logged but individual disk failures are not returned.
func (z *erasureServerPools) deleteAll(ctx context.Context, bucket, prefix string) {
	var wg sync.WaitGroup
	for _, servers := range z.serverPools {
		for _, set := range servers.sets {
			for _, disk := range set.getDisks() {
				if disk == nil {
					continue
				}
				wg.Add(1)
				go func(disk StorageAPI) {
					defer wg.Done()
					disk.Delete(ctx, bucket, prefix, true)
				}(disk)
			}
		}
	}
	wg.Wait()
}

// This function is used to undo a successful DeleteBucket operation.
func undoDeleteBucketServerPools(ctx context.Context, bucket string, serverPools []*erasureSets, errs []error) {
	g := errgroup.WithNErrs(len(serverPools))

	// Undo previous delete bucket on all underlying serverPools.
	for index := range serverPools {
		index := index
		g.Go(func() error {
			if errs[index] == nil {
				return serverPools[index].MakeBucketWithLocation(ctx, bucket, BucketOptions{})
			}
			return nil
		}, index)
	}

	g.Wait()
}

// List all buckets from one of the serverPools, we are not doing merge
// sort here just for simplification. As per design it is assumed
// that all buckets are present on all serverPools.
func (z *erasureServerPools) ListBuckets(ctx context.Context) (buckets []BucketInfo, err error) {
	if z.SinglePool() {
		buckets, err = z.serverPools[0].ListBuckets(ctx)
	} else {
		for _, pool := range z.serverPools {
			buckets, err = pool.ListBuckets(ctx)
			if err != nil {
				logger.LogIf(ctx, err)
				continue
			}
			break
		}
	}
	if err != nil {
		return nil, err
	}
	for i := range buckets {
		meta, err := globalBucketMetadataSys.Get(buckets[i].Name)
		if err == nil {
			buckets[i].Created = meta.Created
		}
	}
	return buckets, nil
}

func (z *erasureServerPools) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	// Acquire lock on format.json
	formatLock := z.NewNSLock(minioMetaBucket, formatConfigFile)
	if err := formatLock.GetLock(ctx, globalOperationTimeout); err != nil {
		return madmin.HealResultItem{}, err
	}
	defer formatLock.Unlock()

	var r = madmin.HealResultItem{
		Type:   madmin.HealItemMetadata,
		Detail: "disk-format",
	}

	var countNoHeal int
	for _, pool := range z.serverPools {
		result, err := pool.HealFormat(ctx, dryRun)
		if err != nil && !errors.Is(err, errNoHealRequired) {
			logger.LogIf(ctx, err)
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
	var r = madmin.HealResultItem{
		Type:   madmin.HealItemBucket,
		Bucket: bucket,
	}

	// Attempt heal on the bucket metadata, ignore any failures
	_, _ = z.HealObject(ctx, minioMetaBucket, pathJoin(bucketConfigPrefix, bucket, bucketMetadataFile), "", opts)

	for _, pool := range z.serverPools {
		result, err := pool.HealBucket(ctx, bucket, opts)
		if err != nil {
			switch err.(type) {
			case BucketNotFound:
				continue
			}
			return result, err
		}
		r.DiskCount += result.DiskCount
		r.SetCount += result.SetCount
		r.Before.Drives = append(r.Before.Drives, result.Before.Drives...)
		r.After.Drives = append(r.After.Drives, result.After.Drives...)
	}

	return r, nil
}

// Walk a bucket, optionally prefix recursively, until we have returned
// all the content to objectInfo channel, it is callers responsibility
// to allocate a receive channel for ObjectInfo, upon any unhandled
// error walker returns error. Optionally if context.Done() is received
// then Walk() stops the walker.
func (z *erasureServerPools) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo, opts ObjectOptions) error {
	if err := checkListObjsArgs(ctx, bucket, prefix, "", z); err != nil {
		// Upon error close the channel.
		close(results)
		return err
	}

	if opts.WalkVersions {
		go func() {
			defer close(results)

			var marker, versionIDMarker string
			for {
				loi, err := z.ListObjectVersions(ctx, bucket, prefix, marker, versionIDMarker, "", 1000)
				if err != nil {
					break
				}

				for _, obj := range loi.Objects {
					results <- obj
				}

				if !loi.IsTruncated {
					break
				}

				marker = loi.NextMarker
				versionIDMarker = loi.NextVersionIDMarker
			}
		}()
		return nil
	}

	go func() {
		defer close(results)

		var marker string
		for {
			loi, err := z.ListObjects(ctx, bucket, prefix, marker, "", 1000)
			if err != nil {
				break
			}

			for _, obj := range loi.Objects {
				results <- obj
			}

			if !loi.IsTruncated {
				break
			}

			marker = loi.NextMarker
		}
	}()

	return nil
}

// HealObjectFn closure function heals the object.
type HealObjectFn func(bucket, object, versionID string) error

func (z *erasureServerPools) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, healObject HealObjectFn) error {
	// If listing did not return any entries upon first attempt, we
	// return `ObjectNotFound`, to indicate the caller for any
	// actions they may want to take as if `prefix` is missing.
	err := toObjectErr(errFileNotFound, bucket, prefix)
	for _, erasureSet := range z.serverPools {
		for _, set := range erasureSet.sets {
			var entryChs []FileInfoVersionsCh
			var mu sync.Mutex
			var wg sync.WaitGroup
			for _, disk := range set.getOnlineDisks() {
				disk := disk
				wg.Add(1)
				go func() {
					defer wg.Done()
					entryCh, err := disk.WalkVersions(ctx, bucket, prefix, "", true, ctx.Done())
					if err != nil {
						// Disk walk returned error, ignore it.
						return
					}
					mu.Lock()
					entryChs = append(entryChs, FileInfoVersionsCh{
						Ch: entryCh,
					})
					mu.Unlock()
				}()
			}
			wg.Wait()

			entriesValid := make([]bool, len(entryChs))
			entries := make([]FileInfoVersions, len(entryChs))

			for {
				entry, quorumCount, ok := lexicallySortedEntryVersions(entryChs, entries, entriesValid)
				if !ok {
					break
				}

				// Remove empty directories if found - they have no meaning.
				// Can be left over from highly concurrent put/remove.
				if quorumCount > set.setDriveCount/2 && entry.IsEmptyDir {
					if !opts.DryRun && opts.Remove {
						set.deleteEmptyDir(ctx, bucket, entry.Name)
					}
				}

				// Indicate that first attempt was a success and subsequent loop
				// knows that its not our first attempt at 'prefix'
				err = nil

				if quorumCount == set.setDriveCount && opts.ScanMode == madmin.HealNormalScan {
					continue
				}

				for _, version := range entry.Versions {
					if err := healObject(bucket, version.Name, version.VersionID); err != nil {
						return toObjectErr(err, bucket, version.Name)
					}
				}
			}
		}
	}

	return err
}

func (z *erasureServerPools) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	object = encodeDirObject(object)

	for _, pool := range z.serverPools {
		result, err := pool.HealObject(ctx, bucket, object, versionID, opts)
		result.Object = decodeDirObject(result.Object)
		if err != nil {
			if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
				continue
			}
			return result, err
		}
		return result, nil
	}
	if versionID != "" {
		return madmin.HealResultItem{}, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: versionID,
		}
	}
	return madmin.HealResultItem{}, ObjectNotFound{
		Bucket: bucket,
		Object: object,
	}
}

// GetMetrics - no op
func (z *erasureServerPools) GetMetrics(ctx context.Context) (*BackendMetrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &BackendMetrics{}, NotImplemented{}
}

func (z *erasureServerPools) getPoolAndSet(id string) (int, int, error) {
	for poolIdx := range z.serverPools {
		format := z.serverPools[poolIdx].format
		for setIdx, set := range format.Erasure.Sets {
			for _, diskID := range set {
				if diskID == id {
					return poolIdx, setIdx, nil
				}
			}
		}
	}
	return 0, 0, fmt.Errorf("DiskID(%s) %w", id, errDiskNotFound)
}

// HealthOptions takes input options to return sepcific information
type HealthOptions struct {
	Maintenance bool
}

// HealthResult returns the current state of the system, also
// additionally with any specific heuristic information which
// was queried
type HealthResult struct {
	Healthy       bool
	HealingDrives int
	PoolID, SetID int
	WriteQuorum   int
}

// Health - returns current status of the object layer health,
// provides if write access exists across sets, additionally
// can be used to query scenarios if health may be lost
// if this node is taken down by an external orchestrator.
func (z *erasureServerPools) Health(ctx context.Context, opts HealthOptions) HealthResult {
	erasureSetUpCount := make([][]int, len(z.serverPools))
	for i := range z.serverPools {
		erasureSetUpCount[i] = make([]int, len(z.serverPools[i].sets))
	}

	diskIDs := globalNotificationSys.GetLocalDiskIDs(ctx)
	if !opts.Maintenance {
		diskIDs = append(diskIDs, getLocalDiskIDs(z))
	}

	for _, localDiskIDs := range diskIDs {
		for _, id := range localDiskIDs {
			poolIdx, setIdx, err := z.getPoolAndSet(id)
			if err != nil {
				logger.LogIf(ctx, err)
				continue
			}
			erasureSetUpCount[poolIdx][setIdx]++
		}
	}

	reqInfo := (&logger.ReqInfo{}).AppendTags("maintenance", strconv.FormatBool(opts.Maintenance))

	b := z.BackendInfo()
	writeQuorum := b.StandardSCData[0]
	if writeQuorum == b.StandardSCParity {
		writeQuorum++
	}

	var aggHealStateResult madmin.BgHealState
	if opts.Maintenance {
		// check if local disks are being healed, if they are being healed
		// we need to tell healthy status as 'false' so that this server
		// is not taken down for maintenance
		var err error
		aggHealStateResult, err = getAggregatedBackgroundHealState(ctx, nil)
		if err != nil {
			logger.LogIf(logger.SetReqInfo(ctx, reqInfo), fmt.Errorf("Unable to verify global heal status: %w", err))
			return HealthResult{
				Healthy: false,
			}
		}

		if len(aggHealStateResult.HealDisks) > 0 {
			logger.LogIf(logger.SetReqInfo(ctx, reqInfo), fmt.Errorf("Total drives to be healed %d", len(aggHealStateResult.HealDisks)))
		}
	}

	for poolIdx := range erasureSetUpCount {
		for setIdx := range erasureSetUpCount[poolIdx] {
			if erasureSetUpCount[poolIdx][setIdx] < writeQuorum {
				logger.LogIf(logger.SetReqInfo(ctx, reqInfo),
					fmt.Errorf("Write quorum may be lost on pool: %d, set: %d, expected write quorum: %d",
						poolIdx, setIdx, writeQuorum))
				return HealthResult{
					Healthy:       false,
					HealingDrives: len(aggHealStateResult.HealDisks),
					PoolID:        poolIdx,
					SetID:         setIdx,
					WriteQuorum:   writeQuorum,
				}
			}
		}
	}

	// when maintenance is not specified we don't have
	// to look at the healing side of the code.
	if !opts.Maintenance {
		return HealthResult{
			Healthy:     true,
			WriteQuorum: writeQuorum,
		}
	}

	return HealthResult{
		Healthy:       len(aggHealStateResult.HealDisks) == 0,
		HealingDrives: len(aggHealStateResult.HealDisks),
		WriteQuorum:   writeQuorum,
	}
}

// PutObjectTags - replace or add tags to an existing object
func (z *erasureServerPools) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) error {
	object = encodeDirObject(object)
	if z.SinglePool() {
		return z.serverPools[0].PutObjectTags(ctx, bucket, object, tags, opts)
	}

	for _, pool := range z.serverPools {
		err := pool.PutObjectTags(ctx, bucket, object, tags, opts)
		if err != nil {
			if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
				continue
			}
			return err
		}
		return nil
	}
	if opts.VersionID != "" {
		return VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	return ObjectNotFound{
		Bucket: bucket,
		Object: object,
	}
}

// DeleteObjectTags - delete object tags from an existing object
func (z *erasureServerPools) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	object = encodeDirObject(object)
	if z.SinglePool() {
		return z.serverPools[0].DeleteObjectTags(ctx, bucket, object, opts)
	}
	for _, pool := range z.serverPools {
		err := pool.DeleteObjectTags(ctx, bucket, object, opts)
		if err != nil {
			if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
				continue
			}
			return err
		}
		return nil
	}
	if opts.VersionID != "" {
		return VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	return ObjectNotFound{
		Bucket: bucket,
		Object: object,
	}
}

// GetObjectTags - get object tags from an existing object
func (z *erasureServerPools) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	object = encodeDirObject(object)
	if z.SinglePool() {
		return z.serverPools[0].GetObjectTags(ctx, bucket, object, opts)
	}
	for _, pool := range z.serverPools {
		tags, err := pool.GetObjectTags(ctx, bucket, object, opts)
		if err != nil {
			if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
				continue
			}
			return tags, err
		}
		return tags, nil
	}
	if opts.VersionID != "" {
		return nil, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	return nil, ObjectNotFound{
		Bucket: bucket,
		Object: object,
	}
}
