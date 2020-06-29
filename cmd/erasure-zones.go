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
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio-go/v6/pkg/tags"
	"github.com/minio/minio/cmd/config/storageclass"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/sync/errgroup"
)

type erasureZones struct {
	GatewayUnsupported

	zones []*erasureSets
}

func (z *erasureZones) SingleZone() bool {
	return len(z.zones) == 1
}

// Initialize new zone of erasure sets.
func newErasureZones(ctx context.Context, endpointZones EndpointZones) (ObjectLayer, error) {
	var (
		deploymentID string
		err          error

		formats      = make([]*formatErasureV3, len(endpointZones))
		storageDisks = make([][]StorageAPI, len(endpointZones))
		z            = &erasureZones{zones: make([]*erasureSets, len(endpointZones))}
	)

	var localDrives []string

	local := endpointZones.FirstLocal()
	for i, ep := range endpointZones {
		for _, endpoint := range ep.Endpoints {
			if endpoint.IsLocal {
				localDrives = append(localDrives, endpoint.Path)
			}
		}
		storageDisks[i], formats[i], err = waitForFormatErasure(local, ep.Endpoints, i+1,
			ep.SetCount, ep.DrivesPerSet, deploymentID)
		if err != nil {
			return nil, err
		}
		if deploymentID == "" {
			deploymentID = formats[i].ID
		}
		z.zones[i], err = newErasureSets(ctx, ep.Endpoints, storageDisks[i], formats[i])
		if err != nil {
			return nil, err
		}
	}

	go intDataUpdateTracker.start(GlobalContext, localDrives...)
	return z, nil
}

func (z *erasureZones) NewNSLock(ctx context.Context, bucket string, objects ...string) RWLocker {
	return z.zones[0].NewNSLock(ctx, bucket, objects...)
}

type zonesAvailableSpace []zoneAvailableSpace

type zoneAvailableSpace struct {
	Index     int
	Available uint64
}

// TotalAvailable - total available space
func (p zonesAvailableSpace) TotalAvailable() uint64 {
	total := uint64(0)
	for _, z := range p {
		total += z.Available
	}
	return total
}

// getAvailableZoneIdx will return an index that can hold size bytes.
// -1 is returned if no zones have available space for the size given.
func (z *erasureZones) getAvailableZoneIdx(ctx context.Context, size int64) int {
	zones := z.getZonesAvailableSpace(ctx, size)
	total := zones.TotalAvailable()
	if total == 0 {
		return -1
	}
	// choose when we reach this many
	choose := rand.Uint64() % total
	atTotal := uint64(0)
	for _, zone := range zones {
		atTotal += zone.Available
		if atTotal > choose && zone.Available > 0 {
			return zone.Index
		}
	}
	// Should not happen, but print values just in case.
	logger.LogIf(ctx, fmt.Errorf("reached end of zones (total: %v, atTotal: %v, choose: %v)", total, atTotal, choose))
	return -1
}

// getZonesAvailableSpace will return the available space of each zone after storing the content.
// If there is not enough space the zone will return 0 bytes available.
// Negative sizes are seen as 0 bytes.
func (z *erasureZones) getZonesAvailableSpace(ctx context.Context, size int64) zonesAvailableSpace {
	if size < 0 {
		size = 0
	}
	var zones = make(zonesAvailableSpace, len(z.zones))

	storageInfos := make([]StorageInfo, len(z.zones))
	g := errgroup.WithNErrs(len(z.zones))
	for index := range z.zones {
		index := index
		g.Go(func() error {
			storageInfos[index] = z.zones[index].StorageUsageInfo(ctx)
			return nil
		}, index)
	}

	// Wait for the go routines.
	g.Wait()

	for i, zinfo := range storageInfos {
		var available uint64
		for _, davailable := range zinfo.Available {
			available += davailable
		}
		var total uint64
		for _, dtotal := range zinfo.Total {
			total += dtotal
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
		zones[i] = zoneAvailableSpace{
			Index:     i,
			Available: available,
		}
	}
	return zones
}

// getZoneIdx returns the found previous object and its corresponding zone idx,
// if none are found falls back to most available space zone.
func (z *erasureZones) getZoneIdx(ctx context.Context, bucket, object string, opts ObjectOptions, size int64) (idx int, err error) {
	if z.SingleZone() {
		return 0, nil
	}
	for i, zone := range z.zones {
		objInfo, err := zone.GetObjectInfo(ctx, bucket, object, opts)
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
		// exist continue to next zone.
		if !objInfo.DeleteMarker && err != nil {
			continue
		}
		// Success case and when DeleteMarker is true return.
		return i, nil
	}

	// We multiply the size by 2 to account for erasure coding.
	idx = z.getAvailableZoneIdx(ctx, size*2)
	if idx < 0 {
		return -1, toObjectErr(errDiskFull)
	}
	return idx, nil
}

func (z *erasureZones) Shutdown(ctx context.Context) error {
	if z.SingleZone() {
		return z.zones[0].Shutdown(ctx)
	}

	g := errgroup.WithNErrs(len(z.zones))

	for index := range z.zones {
		index := index
		g.Go(func() error {
			return z.zones[index].Shutdown(ctx)
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

func (z *erasureZones) StorageInfo(ctx context.Context, local bool) (StorageInfo, []error) {
	if z.SingleZone() {
		return z.zones[0].StorageInfo(ctx, local)
	}

	var storageInfo StorageInfo

	storageInfos := make([]StorageInfo, len(z.zones))
	storageInfosErrs := make([][]error, len(z.zones))
	g := errgroup.WithNErrs(len(z.zones))
	for index := range z.zones {
		index := index
		g.Go(func() error {
			storageInfos[index], storageInfosErrs[index] = z.zones[index].StorageInfo(ctx, local)
			return nil
		}, index)
	}

	// Wait for the go routines.
	g.Wait()

	for _, lstorageInfo := range storageInfos {
		storageInfo.Used = append(storageInfo.Used, lstorageInfo.Used...)
		storageInfo.Total = append(storageInfo.Total, lstorageInfo.Total...)
		storageInfo.Available = append(storageInfo.Available, lstorageInfo.Available...)
		storageInfo.MountPaths = append(storageInfo.MountPaths, lstorageInfo.MountPaths...)
		storageInfo.Backend.OnlineDisks = storageInfo.Backend.OnlineDisks.Merge(lstorageInfo.Backend.OnlineDisks)
		storageInfo.Backend.OfflineDisks = storageInfo.Backend.OfflineDisks.Merge(lstorageInfo.Backend.OfflineDisks)
		storageInfo.Backend.Sets = append(storageInfo.Backend.Sets, lstorageInfo.Backend.Sets...)
	}

	storageInfo.Backend.Type = storageInfos[0].Backend.Type
	storageInfo.Backend.StandardSCData = storageInfos[0].Backend.StandardSCData
	storageInfo.Backend.StandardSCParity = storageInfos[0].Backend.StandardSCParity
	storageInfo.Backend.RRSCData = storageInfos[0].Backend.RRSCData
	storageInfo.Backend.RRSCParity = storageInfos[0].Backend.RRSCParity

	var errs []error
	for i := range z.zones {
		errs = append(errs, storageInfosErrs[i]...)
	}
	return storageInfo, errs
}

func (z *erasureZones) CrawlAndGetDataUsage(ctx context.Context, bf *bloomFilter, updates chan<- DataUsageInfo) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []dataUsageCache
	var firstErr error
	var knownBuckets = make(map[string]struct{}) // used to deduplicate buckets.
	var allBuckets []BucketInfo

	// Collect for each set in zones.
	for _, z := range z.zones {
		for _, erObj := range z.sets {
			// Add new buckets.
			buckets, err := erObj.ListBuckets(ctx)
			if err != nil {
				return err
			}
			for _, b := range buckets {
				if _, ok := knownBuckets[b.Name]; ok {
					continue
				}
				allBuckets = append(allBuckets, b)
				knownBuckets[b.Name] = struct{}{}
			}
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
				err := erObj.crawlAndGetDataUsage(ctx, buckets, bf, updates)
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
		update := func() {
			mu.Lock()
			defer mu.Unlock()

			// We need to merge since we will get the same buckets from each zone.
			// Therefore to get the exact bucket sizes we must merge before we can convert.
			allMerged := dataUsageCache{Info: dataUsageCacheInfo{Name: dataUsageRoot}}
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
	}
	return firstErr
}

// MakeBucketWithLocation - creates a new bucket across all zones simultaneously
// even if one of the sets fail to create buckets, we proceed all the successful
// operations.
func (z *erasureZones) MakeBucketWithLocation(ctx context.Context, bucket string, opts BucketOptions) error {
	if z.SingleZone() {
		if err := z.zones[0].MakeBucketWithLocation(ctx, bucket, opts); err != nil {
			return err
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
		return nil
	}

	g := errgroup.WithNErrs(len(z.zones))

	// Create buckets in parallel across all sets.
	for index := range z.zones {
		index := index
		g.Go(func() error {
			return z.zones[index].MakeBucketWithLocation(ctx, bucket, opts)
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

func (z *erasureZones) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	var nsUnlocker = func() {}

	// Acquire lock
	if lockType != noLock {
		lock := z.NewNSLock(ctx, bucket, object)
		switch lockType {
		case writeLock:
			if err = lock.GetLock(globalObjectTimeout); err != nil {
				return nil, err
			}
			nsUnlocker = lock.Unlock
		case readLock:
			if err = lock.GetRLock(globalObjectTimeout); err != nil {
				return nil, err
			}
			nsUnlocker = lock.RUnlock
		}
	}

	for _, zone := range z.zones {
		gr, err := zone.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
		if err != nil {
			if isErrObjectNotFound(err) {
				continue
			}
			nsUnlocker()
			return nil, err
		}
		gr.cleanUpFns = append(gr.cleanUpFns, nsUnlocker)
		return gr, nil
	}
	nsUnlocker()
	return nil, ObjectNotFound{Bucket: bucket, Object: object}
}

func (z *erasureZones) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) error {
	// Lock the object before reading.
	lk := z.NewNSLock(ctx, bucket, object)
	if err := lk.GetRLock(globalObjectTimeout); err != nil {
		return err
	}
	defer lk.RUnlock()

	if z.SingleZone() {
		return z.zones[0].GetObject(ctx, bucket, object, startOffset, length, writer, etag, opts)
	}
	for _, zone := range z.zones {
		if err := zone.GetObject(ctx, bucket, object, startOffset, length, writer, etag, opts); err != nil {
			if isErrObjectNotFound(err) {
				continue
			}
			return err
		}
		return nil
	}
	return ObjectNotFound{Bucket: bucket, Object: object}
}

func (z *erasureZones) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	// Lock the object before reading.
	lk := z.NewNSLock(ctx, bucket, object)
	if err := lk.GetRLock(globalObjectTimeout); err != nil {
		return ObjectInfo{}, err
	}
	defer lk.RUnlock()

	if z.SingleZone() {
		return z.zones[0].GetObjectInfo(ctx, bucket, object, opts)
	}
	for _, zone := range z.zones {
		objInfo, err := zone.GetObjectInfo(ctx, bucket, object, opts)
		if err != nil {
			if isErrObjectNotFound(err) {
				continue
			}
			return objInfo, err
		}
		return objInfo, nil
	}
	return ObjectInfo{}, ObjectNotFound{Bucket: bucket, Object: object}
}

// PutObject - writes an object to least used erasure zone.
func (z *erasureZones) PutObject(ctx context.Context, bucket string, object string, data *PutObjReader, opts ObjectOptions) (ObjectInfo, error) {
	// Lock the object.
	lk := z.NewNSLock(ctx, bucket, object)
	if err := lk.GetLock(globalObjectTimeout); err != nil {
		return ObjectInfo{}, err
	}
	defer lk.Unlock()

	if z.SingleZone() {
		return z.zones[0].PutObject(ctx, bucket, object, data, opts)
	}

	idx, err := z.getZoneIdx(ctx, bucket, object, opts, data.Size())
	if err != nil {
		return ObjectInfo{}, err
	}

	// Overwrite the object at the right zone
	return z.zones[idx].PutObject(ctx, bucket, object, data, opts)
}

func (z *erasureZones) DeleteObject(ctx context.Context, bucket string, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	// Acquire a write lock before deleting the object.
	lk := z.NewNSLock(ctx, bucket, object)
	if err = lk.GetLock(globalOperationTimeout); err != nil {
		return ObjectInfo{}, err
	}
	defer lk.Unlock()

	if z.SingleZone() {
		return z.zones[0].DeleteObject(ctx, bucket, object, opts)
	}
	for _, zone := range z.zones {
		objInfo, err = zone.DeleteObject(ctx, bucket, object, opts)
		if err == nil {
			return objInfo, nil
		}
		if err != nil && !isErrObjectNotFound(err) {
			break
		}
	}
	return objInfo, err
}

func (z *erasureZones) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	derrs := make([]error, len(objects))
	dobjects := make([]DeletedObject, len(objects))
	objSets := set.NewStringSet()
	for i := range derrs {
		derrs[i] = checkDelObjArgs(ctx, bucket, objects[i].ObjectName)
		objSets.Add(objects[i].ObjectName)
	}

	// Acquire a bulk write lock across 'objects'
	multiDeleteLock := z.NewNSLock(ctx, bucket, objSets.ToSlice()...)
	if err := multiDeleteLock.GetLock(globalOperationTimeout); err != nil {
		for i := range derrs {
			derrs[i] = err
		}
		return nil, derrs
	}
	defer multiDeleteLock.Unlock()

	for _, zone := range z.zones {
		deletedObjects, errs := zone.DeleteObjects(ctx, bucket, objects, opts)
		for i, derr := range errs {
			if derrs[i] == nil {
				if derr != nil && !isErrObjectNotFound(derr) {
					derrs[i] = derr
				}
			}
			if derrs[i] == nil {
				dobjects[i] = deletedObjects[i]
			}
		}
	}
	return dobjects, derrs
}

func (z *erasureZones) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	// Check if this request is only metadata update.
	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))
	if !cpSrcDstSame {
		lk := z.NewNSLock(ctx, dstBucket, dstObject)
		if err := lk.GetLock(globalObjectTimeout); err != nil {
			return objInfo, err
		}
		defer lk.Unlock()
	}

	if z.SingleZone() {
		return z.zones[0].CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
	}

	zoneIdx, err := z.getZoneIdx(ctx, dstBucket, dstObject, dstOpts, srcInfo.Size)
	if err != nil {
		return objInfo, err
	}

	if cpSrcDstSame && srcInfo.metadataOnly && srcOpts.VersionID == dstOpts.VersionID {
		if dstOpts.VersionID != "" && srcOpts.VersionID == dstOpts.VersionID {
			return z.zones[zoneIdx].CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		}
		if !dstOpts.Versioned && srcOpts.VersionID == "" {
			return z.zones[zoneIdx].CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		}
	}

	putOpts := ObjectOptions{
		ServerSideEncryption: dstOpts.ServerSideEncryption,
		UserDefined:          srcInfo.UserDefined,
		Versioned:            dstOpts.Versioned,
		VersionID:            dstOpts.VersionID,
	}

	return z.zones[zoneIdx].PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, putOpts)
}

func (z *erasureZones) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error) {
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

func (z *erasureZones) listObjectsNonSlash(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {

	var zonesEntryChs [][]FileInfoCh

	endWalkCh := make(chan struct{})
	defer close(endWalkCh)

	const ndisks = 3
	for _, zone := range z.zones {
		zonesEntryChs = append(zonesEntryChs,
			zone.startMergeWalksN(ctx, bucket, prefix, "", true, endWalkCh, ndisks))
	}

	var objInfos []ObjectInfo
	var eof bool
	var prevPrefix string

	var zonesEntriesInfos [][]FileInfo
	var zonesEntriesValid [][]bool
	for _, entryChs := range zonesEntryChs {
		zonesEntriesInfos = append(zonesEntriesInfos, make([]FileInfo, len(entryChs)))
		zonesEntriesValid = append(zonesEntriesValid, make([]bool, len(entryChs)))
	}

	for {
		if len(objInfos) == maxKeys {
			break
		}

		result, quorumCount, _, ok := lexicallySortedEntryZone(zonesEntryChs, zonesEntriesInfos, zonesEntriesValid)
		if !ok {
			eof = true
			break
		}

		if quorumCount < ndisks-1 {
			// Skip entries which are not found on upto ndisks.
			continue
		}

		var objInfo ObjectInfo

		index := strings.Index(strings.TrimPrefix(result.Name, prefix), delimiter)
		if index == -1 {
			objInfo = ObjectInfo{
				IsDir:           false,
				Bucket:          bucket,
				Name:            result.Name,
				ModTime:         result.ModTime,
				Size:            result.Size,
				ContentType:     result.Metadata["content-type"],
				ContentEncoding: result.Metadata["content-encoding"],
			}

			// Extract etag from metadata.
			objInfo.ETag = extractETag(result.Metadata)

			// All the parts per object.
			objInfo.Parts = result.Parts

			// etag/md5Sum has already been extracted. We need to
			// remove to avoid it from appearing as part of
			// response headers. e.g, X-Minio-* or X-Amz-*.
			objInfo.UserDefined = cleanMetadata(result.Metadata)

			// Update storage class
			if sc, ok := result.Metadata[xhttp.AmzStorageClass]; ok {
				objInfo.StorageClass = sc
			} else {
				objInfo.StorageClass = globalMinioDefaultStorageClass
			}
		} else {
			index = len(prefix) + index + len(delimiter)
			currPrefix := result.Name[:index]
			if currPrefix == prevPrefix {
				continue
			}
			prevPrefix = currPrefix

			objInfo = ObjectInfo{
				Bucket: bucket,
				Name:   currPrefix,
				IsDir:  true,
			}
		}

		if objInfo.Name <= marker {
			continue
		}

		objInfos = append(objInfos, objInfo)
	}

	result := ListObjectsInfo{}
	for _, objInfo := range objInfos {
		if objInfo.IsDir {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}

	if !eof {
		result.IsTruncated = true
		if len(objInfos) > 0 {
			result.NextMarker = objInfos[len(objInfos)-1].Name
		}
	}

	return result, nil
}

func (z *erasureZones) listObjectsSplunk(ctx context.Context, bucket, prefix, marker string, maxKeys int) (loi ListObjectsInfo, err error) {
	if strings.Contains(prefix, guidSplunk) {
		logger.LogIf(ctx, NotImplemented{})
		return loi, NotImplemented{}
	}

	recursive := true

	var zonesEntryChs [][]FileInfoCh
	var zonesEndWalkCh []chan struct{}

	const ndisks = 3
	for _, zone := range z.zones {
		entryChs, endWalkCh := zone.poolSplunk.Release(listParams{bucket, recursive, marker, prefix})
		if entryChs == nil {
			endWalkCh = make(chan struct{})
			entryChs = zone.startSplunkMergeWalksN(ctx, bucket, prefix, marker, endWalkCh, ndisks)
		}
		zonesEntryChs = append(zonesEntryChs, entryChs)
		zonesEndWalkCh = append(zonesEndWalkCh, endWalkCh)
	}

	entries := mergeZonesEntriesCh(zonesEntryChs, maxKeys, ndisks)
	if len(entries.Files) == 0 {
		return loi, nil
	}

	loi.IsTruncated = entries.IsTruncated
	if loi.IsTruncated {
		loi.NextMarker = entries.Files[len(entries.Files)-1].Name
	}

	for _, entry := range entries.Files {
		objInfo := entry.ToObjectInfo(bucket, entry.Name)
		splits := strings.Split(objInfo.Name, guidSplunk)
		if len(splits) == 0 {
			loi.Objects = append(loi.Objects, objInfo)
			continue
		}

		loi.Prefixes = append(loi.Prefixes, splits[0]+guidSplunk)
	}

	if loi.IsTruncated {
		for i, zone := range z.zones {
			zone.poolSplunk.Set(listParams{bucket, recursive, loi.NextMarker, prefix}, zonesEntryChs[i],
				zonesEndWalkCh[i])
		}
	}
	return loi, nil
}

func (z *erasureZones) listObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	loi := ListObjectsInfo{}

	if err := checkListObjsArgs(ctx, bucket, prefix, marker, z); err != nil {
		return loi, err
	}

	// Marker is set validate pre-condition.
	if marker != "" {
		// Marker not common with prefix is not implemented. Send an empty response
		if !HasPrefix(marker, prefix) {
			return loi, nil
		}
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return loi, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if delimiter == SlashSeparator && prefix == SlashSeparator {
		return loi, nil
	}

	// Over flowing count - reset to maxObjectList.
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}

	if delimiter != SlashSeparator && delimiter != "" {
		if delimiter == guidSplunk {
			return z.listObjectsSplunk(ctx, bucket, prefix, marker, maxKeys)
		}
		return z.listObjectsNonSlash(ctx, bucket, prefix, marker, delimiter, maxKeys)
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == SlashSeparator {
		recursive = false
	}

	var zonesEntryChs [][]FileInfoCh
	var zonesEndWalkCh []chan struct{}

	const ndisks = 3
	for _, zone := range z.zones {
		entryChs, endWalkCh := zone.pool.Release(listParams{bucket, recursive, marker, prefix})
		if entryChs == nil {
			endWalkCh = make(chan struct{})
			entryChs = zone.startMergeWalksN(ctx, bucket, prefix, marker, recursive, endWalkCh, ndisks)
		}
		zonesEntryChs = append(zonesEntryChs, entryChs)
		zonesEndWalkCh = append(zonesEndWalkCh, endWalkCh)
	}

	entries := mergeZonesEntriesCh(zonesEntryChs, maxKeys, ndisks)
	if len(entries.Files) == 0 {
		return loi, nil
	}

	loi.IsTruncated = entries.IsTruncated
	if loi.IsTruncated {
		loi.NextMarker = entries.Files[len(entries.Files)-1].Name
	}

	for _, entry := range entries.Files {
		objInfo := entry.ToObjectInfo(entry.Volume, entry.Name)
		if HasSuffix(objInfo.Name, SlashSeparator) && !recursive {
			loi.Prefixes = append(loi.Prefixes, objInfo.Name)
			continue
		}
		loi.Objects = append(loi.Objects, objInfo)
	}
	if loi.IsTruncated {
		for i, zone := range z.zones {
			zone.pool.Set(listParams{bucket, recursive, loi.NextMarker, prefix}, zonesEntryChs[i],
				zonesEndWalkCh[i])
		}
	}
	return loi, nil
}

// Calculate least entry across zones and across multiple FileInfo
// channels, returns the least common entry and the total number of times
// we found this entry. Additionally also returns a boolean
// to indicate if the caller needs to call this function
// again to list the next entry. It is callers responsibility
// if the caller wishes to list N entries to call lexicallySortedEntry
// N times until this boolean is 'false'.
func lexicallySortedEntryZone(zoneEntryChs [][]FileInfoCh, zoneEntries [][]FileInfo, zoneEntriesValid [][]bool) (FileInfo, int, int, bool) {
	for i, entryChs := range zoneEntryChs {
		i := i
		var wg sync.WaitGroup
		for j := range entryChs {
			j := j
			wg.Add(1)
			// Pop() entries in parallel for large drive setups.
			go func() {
				defer wg.Done()
				zoneEntries[i][j], zoneEntriesValid[i][j] = entryChs[j].Pop()
			}()
		}
		wg.Wait()
	}

	var isTruncated = false
	for _, entriesValid := range zoneEntriesValid {
		for _, valid := range entriesValid {
			if !valid {
				continue
			}
			isTruncated = true
			break
		}
		if isTruncated {
			break
		}
	}

	var lentry FileInfo
	var found bool
	var zoneIndex = -1
	// TODO: following loop can be merged with above
	// loop, explore this possibility.
	for i, entriesValid := range zoneEntriesValid {
		for j, valid := range entriesValid {
			if !valid {
				continue
			}
			if !found {
				lentry = zoneEntries[i][j]
				found = true
				zoneIndex = i
				continue
			}
			if zoneEntries[i][j].Name < lentry.Name {
				lentry = zoneEntries[i][j]
				zoneIndex = i
			}
		}
	}

	// We haven't been able to find any least entry,
	// this would mean that we don't have valid entry.
	if !found {
		return lentry, 0, zoneIndex, isTruncated
	}

	lexicallySortedEntryCount := 0
	for i, entriesValid := range zoneEntriesValid {
		for j, valid := range entriesValid {
			if !valid {
				continue
			}

			// Entries are duplicated across disks,
			// we should simply skip such entries.
			if lentry.Name == zoneEntries[i][j].Name && lentry.ModTime.Equal(zoneEntries[i][j].ModTime) {
				lexicallySortedEntryCount++
				continue
			}

			// Push all entries which are lexically higher
			// and will be returned later in Pop()
			zoneEntryChs[i][j].Push(zoneEntries[i][j])
		}
	}

	return lentry, lexicallySortedEntryCount, zoneIndex, isTruncated
}

// Calculate least entry across zones and across multiple FileInfoVersions
// channels, returns the least common entry and the total number of times
// we found this entry. Additionally also returns a boolean
// to indicate if the caller needs to call this function
// again to list the next entry. It is callers responsibility
// if the caller wishes to list N entries to call lexicallySortedEntry
// N times until this boolean is 'false'.
func lexicallySortedEntryZoneVersions(zoneEntryChs [][]FileInfoVersionsCh, zoneEntries [][]FileInfoVersions, zoneEntriesValid [][]bool) (FileInfoVersions, int, int, bool) {
	for i, entryChs := range zoneEntryChs {
		i := i
		var wg sync.WaitGroup
		for j := range entryChs {
			j := j
			wg.Add(1)
			// Pop() entries in parallel for large drive setups.
			go func() {
				defer wg.Done()
				zoneEntries[i][j], zoneEntriesValid[i][j] = entryChs[j].Pop()
			}()
		}
		wg.Wait()
	}

	var isTruncated = false
	for _, entriesValid := range zoneEntriesValid {
		for _, valid := range entriesValid {
			if !valid {
				continue
			}
			isTruncated = true
			break
		}
		if isTruncated {
			break
		}
	}

	var lentry FileInfoVersions
	var found bool
	var zoneIndex = -1
	for i, entriesValid := range zoneEntriesValid {
		for j, valid := range entriesValid {
			if !valid {
				continue
			}
			if !found {
				lentry = zoneEntries[i][j]
				found = true
				zoneIndex = i
				continue
			}
			if zoneEntries[i][j].Name < lentry.Name {
				lentry = zoneEntries[i][j]
				zoneIndex = i
			}
		}
	}

	// We haven't been able to find any least entry,
	// this would mean that we don't have valid entry.
	if !found {
		return lentry, 0, zoneIndex, isTruncated
	}

	lexicallySortedEntryCount := 0
	for i, entriesValid := range zoneEntriesValid {
		for j, valid := range entriesValid {
			if !valid {
				continue
			}

			// Entries are duplicated across disks,
			// we should simply skip such entries.
			if lentry.Name == zoneEntries[i][j].Name && lentry.LatestModTime.Equal(zoneEntries[i][j].LatestModTime) {
				lexicallySortedEntryCount++
				continue
			}

			// Push all entries which are lexically higher
			// and will be returned later in Pop()
			zoneEntryChs[i][j].Push(zoneEntries[i][j])
		}
	}

	return lentry, lexicallySortedEntryCount, zoneIndex, isTruncated
}

// mergeZonesEntriesVersionsCh - merges FileInfoVersions channel to entries upto maxKeys.
func mergeZonesEntriesVersionsCh(zonesEntryChs [][]FileInfoVersionsCh, maxKeys int, ndisks int) (entries FilesInfoVersions) {
	var i = 0
	var zonesEntriesInfos [][]FileInfoVersions
	var zonesEntriesValid [][]bool
	for _, entryChs := range zonesEntryChs {
		zonesEntriesInfos = append(zonesEntriesInfos, make([]FileInfoVersions, len(entryChs)))
		zonesEntriesValid = append(zonesEntriesValid, make([]bool, len(entryChs)))
	}
	for {
		fi, quorumCount, _, ok := lexicallySortedEntryZoneVersions(zonesEntryChs, zonesEntriesInfos, zonesEntriesValid)
		if !ok {
			// We have reached EOF across all entryChs, break the loop.
			break
		}

		if quorumCount < ndisks-1 {
			// Skip entries which are not found on upto ndisks.
			continue
		}

		entries.FilesVersions = append(entries.FilesVersions, fi)
		i++
		if i == maxKeys {
			entries.IsTruncated = isTruncatedZonesVersions(zonesEntryChs, zonesEntriesInfos, zonesEntriesValid)
			break
		}
	}
	return entries
}

// mergeZonesEntriesCh - merges FileInfo channel to entries upto maxKeys.
func mergeZonesEntriesCh(zonesEntryChs [][]FileInfoCh, maxKeys int, ndisks int) (entries FilesInfo) {
	var i = 0
	var zonesEntriesInfos [][]FileInfo
	var zonesEntriesValid [][]bool
	for _, entryChs := range zonesEntryChs {
		zonesEntriesInfos = append(zonesEntriesInfos, make([]FileInfo, len(entryChs)))
		zonesEntriesValid = append(zonesEntriesValid, make([]bool, len(entryChs)))
	}
	for {
		fi, quorumCount, _, ok := lexicallySortedEntryZone(zonesEntryChs, zonesEntriesInfos, zonesEntriesValid)
		if !ok {
			// We have reached EOF across all entryChs, break the loop.
			break
		}

		if quorumCount < ndisks-1 {
			// Skip entries which are not found on upto ndisks.
			continue
		}

		entries.Files = append(entries.Files, fi)
		i++
		if i == maxKeys {
			entries.IsTruncated = isTruncatedZones(zonesEntryChs, zonesEntriesInfos, zonesEntriesValid)
			break
		}
	}
	return entries
}

func isTruncatedZones(zoneEntryChs [][]FileInfoCh, zoneEntries [][]FileInfo, zoneEntriesValid [][]bool) bool {
	for i, entryChs := range zoneEntryChs {
		i := i
		var wg sync.WaitGroup
		for j := range entryChs {
			j := j
			wg.Add(1)
			// Pop() entries in parallel for large drive setups.
			go func() {
				defer wg.Done()
				zoneEntries[i][j], zoneEntriesValid[i][j] = entryChs[j].Pop()
			}()
		}
		wg.Wait()
	}

	var isTruncated = false
	for _, entriesValid := range zoneEntriesValid {
		for _, valid := range entriesValid {
			if valid {
				isTruncated = true
				break
			}
		}
		if isTruncated {
			break
		}
	}
	for i, entryChs := range zoneEntryChs {
		for j := range entryChs {
			if zoneEntriesValid[i][j] {
				zoneEntryChs[i][j].Push(zoneEntries[i][j])
			}
		}
	}
	return isTruncated
}

func isTruncatedZonesVersions(zoneEntryChs [][]FileInfoVersionsCh, zoneEntries [][]FileInfoVersions, zoneEntriesValid [][]bool) bool {
	for i, entryChs := range zoneEntryChs {
		i := i
		var wg sync.WaitGroup
		for j := range entryChs {
			j := j
			wg.Add(1)
			// Pop() entries in parallel for large drive setups.
			go func() {
				defer wg.Done()
				zoneEntries[i][j], zoneEntriesValid[i][j] = entryChs[j].Pop()
			}()
		}
		wg.Wait()
	}

	var isTruncated = false
	for _, entriesValid := range zoneEntriesValid {
		for _, valid := range entriesValid {
			if !valid {
				continue
			}
			isTruncated = true
			break
		}
		if isTruncated {
			break
		}
	}
	for i, entryChs := range zoneEntryChs {
		for j := range entryChs {
			if zoneEntriesValid[i][j] {
				zoneEntryChs[i][j].Push(zoneEntries[i][j])
			}
		}
	}
	return isTruncated
}

func (z *erasureZones) listObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (ListObjectVersionsInfo, error) {
	loi := ListObjectVersionsInfo{}

	if err := checkListObjsArgs(ctx, bucket, prefix, marker, z); err != nil {
		return loi, err
	}

	// Marker is set validate pre-condition.
	if marker != "" {
		// Marker not common with prefix is not implemented. Send an empty response
		if !HasPrefix(marker, prefix) {
			return loi, nil
		}
	}

	if marker == "" && versionMarker != "" {
		return loi, NotImplemented{}
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return loi, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if delimiter == SlashSeparator && prefix == SlashSeparator {
		return loi, nil
	}

	// Over flowing count - reset to maxObjectList.
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}

	if delimiter != SlashSeparator && delimiter != "" {
		return loi, NotImplemented{}
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == SlashSeparator {
		recursive = false
	}

	var zonesEntryChs [][]FileInfoVersionsCh
	var zonesEndWalkCh []chan struct{}

	const ndisks = 3
	for _, zone := range z.zones {
		entryChs, endWalkCh := zone.poolVersions.Release(listParams{bucket, recursive, marker, prefix})
		if entryChs == nil {
			endWalkCh = make(chan struct{})
			entryChs = zone.startMergeWalksVersionsN(ctx, bucket, prefix, marker, recursive, endWalkCh, ndisks)
		}
		zonesEntryChs = append(zonesEntryChs, entryChs)
		zonesEndWalkCh = append(zonesEndWalkCh, endWalkCh)
	}

	entries := mergeZonesEntriesVersionsCh(zonesEntryChs, maxKeys, ndisks)
	if len(entries.FilesVersions) == 0 {
		return loi, nil
	}

	loi.IsTruncated = entries.IsTruncated
	if loi.IsTruncated {
		loi.NextMarker = entries.FilesVersions[len(entries.FilesVersions)-1].Name
	}

	for _, entry := range entries.FilesVersions {
		for _, version := range entry.Versions {
			objInfo := version.ToObjectInfo(bucket, entry.Name)
			if HasSuffix(objInfo.Name, SlashSeparator) && !recursive {
				loi.Prefixes = append(loi.Prefixes, objInfo.Name)
				continue
			}
			loi.Objects = append(loi.Objects, objInfo)
		}
		for _, deleted := range entry.Deleted {
			loi.DeleteObjects = append(loi.DeleteObjects, DeletedObjectInfo{
				Bucket:    bucket,
				Name:      entry.Name,
				VersionID: deleted.VersionID,
				ModTime:   deleted.ModTime,
				IsLatest:  deleted.IsLatest,
			})
		}

	}
	if loi.IsTruncated {
		for i, zone := range z.zones {
			zone.poolVersions.Set(listParams{bucket, recursive, loi.NextMarker, prefix}, zonesEntryChs[i],
				zonesEndWalkCh[i])
		}
	}
	return loi, nil
}

func (z *erasureZones) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (ListObjectVersionsInfo, error) {
	return z.listObjectVersions(ctx, bucket, prefix, marker, versionMarker, delimiter, maxKeys)
}

func (z *erasureZones) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return z.listObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
}

func (z *erasureZones) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	if err := checkListMultipartArgs(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, z); err != nil {
		return ListMultipartsInfo{}, err
	}

	if z.SingleZone() {
		return z.zones[0].ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	}

	var zoneResult = ListMultipartsInfo{}
	zoneResult.MaxUploads = maxUploads
	zoneResult.KeyMarker = keyMarker
	zoneResult.Prefix = prefix
	zoneResult.Delimiter = delimiter
	for _, zone := range z.zones {
		result, err := zone.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker,
			delimiter, maxUploads)
		if err != nil {
			return result, err
		}
		zoneResult.Uploads = append(zoneResult.Uploads, result.Uploads...)
	}
	return zoneResult, nil
}

// Initiate a new multipart upload on a hashedSet based on object name.
func (z *erasureZones) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (string, error) {
	if err := checkNewMultipartArgs(ctx, bucket, object, z); err != nil {
		return "", err
	}

	if z.SingleZone() {
		return z.zones[0].NewMultipartUpload(ctx, bucket, object, opts)
	}

	// We don't know the exact size, so we ask for at least 1GiB file.
	idx, err := z.getZoneIdx(ctx, bucket, object, opts, 1<<30)
	if err != nil {
		return "", err
	}

	return z.zones[idx].NewMultipartUpload(ctx, bucket, object, opts)
}

// Copies a part of an object from source hashedSet to destination hashedSet.
func (z *erasureZones) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (PartInfo, error) {
	if err := checkNewMultipartArgs(ctx, srcBucket, srcObject, z); err != nil {
		return PartInfo{}, err
	}

	return z.PutObjectPart(ctx, destBucket, destObject, uploadID, partID,
		NewPutObjReader(srcInfo.Reader, nil, nil), dstOpts)
}

// PutObjectPart - writes part of an object to hashedSet based on the object name.
func (z *erasureZones) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (PartInfo, error) {
	if err := checkPutObjectPartArgs(ctx, bucket, object, z); err != nil {
		return PartInfo{}, err
	}

	uploadIDLock := z.NewNSLock(ctx, bucket, pathJoin(object, uploadID))
	if err := uploadIDLock.GetLock(globalOperationTimeout); err != nil {
		return PartInfo{}, err
	}
	defer uploadIDLock.Unlock()

	if z.SingleZone() {
		return z.zones[0].PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
	}

	for _, zone := range z.zones {
		_, err := zone.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
		if err == nil {
			return zone.PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
		}
		switch err.(type) {
		case InvalidUploadID:
			// Look for information on the next zone
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

func (z *erasureZones) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	if err := checkListPartsArgs(ctx, bucket, object, z); err != nil {
		return MultipartInfo{}, err
	}

	uploadIDLock := z.NewNSLock(ctx, bucket, pathJoin(object, uploadID))
	if err := uploadIDLock.GetRLock(globalOperationTimeout); err != nil {
		return MultipartInfo{}, err
	}
	defer uploadIDLock.RUnlock()

	if z.SingleZone() {
		return z.zones[0].GetMultipartInfo(ctx, bucket, object, uploadID, opts)
	}
	for _, zone := range z.zones {
		mi, err := zone.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
		if err == nil {
			return mi, nil
		}
		switch err.(type) {
		case InvalidUploadID:
			// upload id not found, continue to the next zone.
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
func (z *erasureZones) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (ListPartsInfo, error) {
	if err := checkListPartsArgs(ctx, bucket, object, z); err != nil {
		return ListPartsInfo{}, err
	}

	uploadIDLock := z.NewNSLock(ctx, bucket, pathJoin(object, uploadID))
	if err := uploadIDLock.GetRLock(globalOperationTimeout); err != nil {
		return ListPartsInfo{}, err
	}
	defer uploadIDLock.RUnlock()

	if z.SingleZone() {
		return z.zones[0].ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
	}
	for _, zone := range z.zones {
		_, err := zone.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
		if err == nil {
			return zone.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
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
func (z *erasureZones) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	if err := checkAbortMultipartArgs(ctx, bucket, object, z); err != nil {
		return err
	}

	uploadIDLock := z.NewNSLock(ctx, bucket, pathJoin(object, uploadID))
	if err := uploadIDLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer uploadIDLock.Unlock()

	if z.SingleZone() {
		return z.zones[0].AbortMultipartUpload(ctx, bucket, object, uploadID)
	}

	for _, zone := range z.zones {
		_, err := zone.GetMultipartInfo(ctx, bucket, object, uploadID, ObjectOptions{})
		if err == nil {
			return zone.AbortMultipartUpload(ctx, bucket, object, uploadID)
		}
		switch err.(type) {
		case InvalidUploadID:
			// upload id not found move to next zone
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
func (z *erasureZones) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if err = checkCompleteMultipartArgs(ctx, bucket, object, z); err != nil {
		return objInfo, err
	}

	// Hold read-locks to verify uploaded parts, also disallows
	// parallel part uploads as well.
	uploadIDLock := z.NewNSLock(ctx, bucket, pathJoin(object, uploadID))
	if err = uploadIDLock.GetRLock(globalOperationTimeout); err != nil {
		return objInfo, err
	}
	defer uploadIDLock.RUnlock()

	// Hold namespace to complete the transaction, only hold
	// if uploadID can be held exclusively.
	lk := z.NewNSLock(ctx, bucket, object)
	if err = lk.GetLock(globalOperationTimeout); err != nil {
		return objInfo, err
	}
	defer lk.Unlock()

	if z.SingleZone() {
		return z.zones[0].CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
	}

	// Purge any existing object.
	for _, zone := range z.zones {
		zone.DeleteObject(ctx, bucket, object, opts)
	}

	for _, zone := range z.zones {
		result, err := zone.ListMultipartUploads(ctx, bucket, object, "", "", "", maxUploadsList)
		if err != nil {
			return objInfo, err
		}
		if result.Lookup(uploadID) {
			return zone.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
		}
	}
	return objInfo, InvalidUploadID{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}
}

// GetBucketInfo - returns bucket info from one of the erasure coded zones.
func (z *erasureZones) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error) {
	if z.SingleZone() {
		bucketInfo, err = z.zones[0].GetBucketInfo(ctx, bucket)
		if err != nil {
			return bucketInfo, err
		}
		meta, err := globalBucketMetadataSys.Get(bucket)
		if err == nil {
			bucketInfo.Created = meta.Created
		}
		return bucketInfo, nil
	}
	for _, zone := range z.zones {
		bucketInfo, err = zone.GetBucketInfo(ctx, bucket)
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
func (z *erasureZones) IsNotificationSupported() bool {
	return true
}

// IsListenBucketSupported returns whether listen bucket notification is applicable for this layer.
func (z *erasureZones) IsListenBucketSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (z *erasureZones) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (z *erasureZones) IsCompressionSupported() bool {
	return true
}

func (z *erasureZones) IsTaggingSupported() bool {
	return true
}

// DeleteBucket - deletes a bucket on all zones simultaneously,
// even if one of the zones fail to delete buckets, we proceed to
// undo a successful operation.
func (z *erasureZones) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	if z.SingleZone() {
		return z.zones[0].DeleteBucket(ctx, bucket, forceDelete)
	}
	g := errgroup.WithNErrs(len(z.zones))

	// Delete buckets in parallel across all zones.
	for index := range z.zones {
		index := index
		g.Go(func() error {
			return z.zones[index].DeleteBucket(ctx, bucket, forceDelete)
		}, index)
	}

	errs := g.Wait()

	// For any write quorum failure, we undo all the delete
	// buckets operation by creating all the buckets again.
	for _, err := range errs {
		if err != nil {
			if _, ok := err.(InsufficientWriteQuorum); ok {
				undoDeleteBucketZones(ctx, bucket, z.zones, errs)
			}

			return err
		}
	}

	// Success.
	return nil
}

// This function is used to undo a successful DeleteBucket operation.
func undoDeleteBucketZones(ctx context.Context, bucket string, zones []*erasureSets, errs []error) {
	g := errgroup.WithNErrs(len(zones))

	// Undo previous delete bucket on all underlying zones.
	for index := range zones {
		index := index
		g.Go(func() error {
			if errs[index] == nil {
				return zones[index].MakeBucketWithLocation(ctx, bucket, BucketOptions{})
			}
			return nil
		}, index)
	}

	g.Wait()
}

// List all buckets from one of the zones, we are not doing merge
// sort here just for simplification. As per design it is assumed
// that all buckets are present on all zones.
func (z *erasureZones) ListBuckets(ctx context.Context) (buckets []BucketInfo, err error) {
	if z.SingleZone() {
		buckets, err = z.zones[0].ListBuckets(ctx)
	} else {
		for _, zone := range z.zones {
			buckets, err = zone.ListBuckets(ctx)
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

func (z *erasureZones) ReloadFormat(ctx context.Context, dryRun bool) error {
	// Acquire lock on format.json
	formatLock := z.NewNSLock(ctx, minioMetaBucket, formatConfigFile)
	if err := formatLock.GetRLock(globalHealingTimeout); err != nil {
		return err
	}
	defer formatLock.RUnlock()

	for _, zone := range z.zones {
		if err := zone.ReloadFormat(ctx, dryRun); err != nil {
			return err
		}
	}
	return nil
}

func (z *erasureZones) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	// Acquire lock on format.json
	formatLock := z.NewNSLock(ctx, minioMetaBucket, formatConfigFile)
	if err := formatLock.GetLock(globalHealingTimeout); err != nil {
		return madmin.HealResultItem{}, err
	}
	defer formatLock.Unlock()

	var r = madmin.HealResultItem{
		Type:   madmin.HealItemMetadata,
		Detail: "disk-format",
	}

	var countNoHeal int
	for _, zone := range z.zones {
		result, err := zone.HealFormat(ctx, dryRun)
		if err != nil && err != errNoHealRequired {
			logger.LogIf(ctx, err)
			continue
		}
		// Count errNoHealRequired across all zones,
		// to return appropriate error to the caller
		if err == errNoHealRequired {
			countNoHeal++
		}
		r.DiskCount += result.DiskCount
		r.SetCount += result.SetCount
		r.Before.Drives = append(r.Before.Drives, result.Before.Drives...)
		r.After.Drives = append(r.After.Drives, result.After.Drives...)
	}
	// No heal returned by all zones, return errNoHealRequired
	if countNoHeal == len(z.zones) {
		return r, errNoHealRequired
	}
	return r, nil
}

func (z *erasureZones) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (madmin.HealResultItem, error) {
	var r = madmin.HealResultItem{
		Type:   madmin.HealItemBucket,
		Bucket: bucket,
	}

	for _, zone := range z.zones {
		result, err := zone.HealBucket(ctx, bucket, dryRun, remove)
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
func (z *erasureZones) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo) error {
	if err := checkListObjsArgs(ctx, bucket, prefix, "", z); err != nil {
		// Upon error close the channel.
		close(results)
		return err
	}

	var zonesEntryChs [][]FileInfoVersionsCh
	for _, zone := range z.zones {
		zonesEntryChs = append(zonesEntryChs, zone.startMergeWalksVersions(ctx, bucket, prefix, "", true, ctx.Done()))
	}

	var zoneDrivesPerSet []int
	for _, zone := range z.zones {
		zoneDrivesPerSet = append(zoneDrivesPerSet, zone.drivesPerSet)
	}

	var zonesEntriesInfos [][]FileInfoVersions
	var zonesEntriesValid [][]bool
	for _, entryChs := range zonesEntryChs {
		zonesEntriesInfos = append(zonesEntriesInfos, make([]FileInfoVersions, len(entryChs)))
		zonesEntriesValid = append(zonesEntriesValid, make([]bool, len(entryChs)))
	}

	go func() {
		defer close(results)

		for {
			entry, quorumCount, zoneIndex, ok := lexicallySortedEntryZoneVersions(zonesEntryChs, zonesEntriesInfos, zonesEntriesValid)
			if !ok {
				// We have reached EOF across all entryChs, break the loop.
				return
			}

			if quorumCount >= zoneDrivesPerSet[zoneIndex]/2 {
				// Read quorum exists proceed
				for _, version := range entry.Versions {
					results <- version.ToObjectInfo(bucket, version.Name)
				}
				for _, deleted := range entry.Deleted {
					results <- deleted.ToObjectInfo(bucket, deleted.Name)
				}
			}

			// skip entries which do not have quorum
		}
	}()

	return nil
}

// HealObjectFn closure function heals the object.
type HealObjectFn func(string, string, string) error

func (z *erasureZones) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, healObject HealObjectFn) error {
	var zonesEntryChs [][]FileInfoVersionsCh

	endWalkCh := make(chan struct{})
	defer close(endWalkCh)

	for _, zone := range z.zones {
		zonesEntryChs = append(zonesEntryChs,
			zone.startMergeWalksVersions(ctx, bucket, prefix, "", true, endWalkCh))
	}

	var zoneDrivesPerSet []int
	for _, zone := range z.zones {
		zoneDrivesPerSet = append(zoneDrivesPerSet, zone.drivesPerSet)
	}

	var zonesEntriesInfos [][]FileInfoVersions
	var zonesEntriesValid [][]bool
	for _, entryChs := range zonesEntryChs {
		zonesEntriesInfos = append(zonesEntriesInfos, make([]FileInfoVersions, len(entryChs)))
		zonesEntriesValid = append(zonesEntriesValid, make([]bool, len(entryChs)))
	}

	for {
		entry, quorumCount, zoneIndex, ok := lexicallySortedEntryZoneVersions(zonesEntryChs, zonesEntriesInfos, zonesEntriesValid)
		if !ok {
			break
		}

		if quorumCount == zoneDrivesPerSet[zoneIndex] && opts.ScanMode == madmin.HealNormalScan {
			// Skip good entries.
			continue
		}

		// Wait and proceed if there are active requests
		waitForLowHTTPReq(int32(zoneDrivesPerSet[zoneIndex]))

		for _, version := range entry.Versions {
			if err := healObject(bucket, version.Name, version.VersionID); err != nil {
				return toObjectErr(err, bucket, version.Name)
			}
		}
	}

	return nil
}

func (z *erasureZones) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	// Lock the object before healing. Use read lock since healing
	// will only regenerate parts & xl.meta of outdated disks.
	lk := z.NewNSLock(ctx, bucket, object)
	if err := lk.GetRLock(globalHealingTimeout); err != nil {
		return madmin.HealResultItem{}, err
	}
	defer lk.RUnlock()

	if z.SingleZone() {
		return z.zones[0].HealObject(ctx, bucket, object, versionID, opts)
	}
	for _, zone := range z.zones {
		result, err := zone.HealObject(ctx, bucket, object, versionID, opts)
		if err != nil {
			if isErrObjectNotFound(err) {
				continue
			}
			return result, err
		}
		return result, nil
	}
	return madmin.HealResultItem{}, ObjectNotFound{
		Bucket: bucket,
		Object: object,
	}
}

func (z *erasureZones) ListBucketsHeal(ctx context.Context) ([]BucketInfo, error) {
	var healBuckets []BucketInfo
	for _, zone := range z.zones {
		bucketsInfo, err := zone.ListBucketsHeal(ctx)
		if err != nil {
			continue
		}
		healBuckets = append(healBuckets, bucketsInfo...)
	}

	for i := range healBuckets {
		meta, err := globalBucketMetadataSys.Get(healBuckets[i].Name)
		if err == nil {
			healBuckets[i].Created = meta.Created
		}
	}

	return healBuckets, nil
}

// GetMetrics - no op
func (z *erasureZones) GetMetrics(ctx context.Context) (*Metrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &Metrics{}, NotImplemented{}
}

func (z *erasureZones) getZoneAndSet(id string) (int, int, error) {
	for zoneIdx := range z.zones {
		format := z.zones[zoneIdx].format
		for setIdx, set := range format.Erasure.Sets {
			for _, diskID := range set {
				if diskID == id {
					return zoneIdx, setIdx, nil
				}
			}
		}
	}
	return 0, 0, fmt.Errorf("DiskID(%s) %w", id, errDiskNotFound)
}

// IsReady - Returns true, when all the erasure sets are writable.
func (z *erasureZones) IsReady(ctx context.Context) bool {
	erasureSetUpCount := make([][]int, len(z.zones))
	for i := range z.zones {
		erasureSetUpCount[i] = make([]int, len(z.zones[i].sets))
	}

	diskIDs := globalNotificationSys.GetLocalDiskIDs(ctx)

	diskIDs = append(diskIDs, getLocalDiskIDs(z)...)

	for _, id := range diskIDs {
		zoneIdx, setIdx, err := z.getZoneAndSet(id)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		erasureSetUpCount[zoneIdx][setIdx]++
	}

	for zoneIdx := range erasureSetUpCount {
		parityDrives := globalStorageClass.GetParityForSC(storageclass.STANDARD)
		diskCount := len(z.zones[zoneIdx].format.Erasure.Sets[0])
		if parityDrives == 0 {
			parityDrives = getDefaultParityBlocks(diskCount)
		}
		dataDrives := diskCount - parityDrives
		writeQuorum := dataDrives
		if dataDrives == parityDrives {
			writeQuorum++
		}
		for setIdx := range erasureSetUpCount[zoneIdx] {
			if erasureSetUpCount[zoneIdx][setIdx] < writeQuorum {
				logger.LogIf(ctx, fmt.Errorf("Write quorum lost on zone: %d, set: %d, expected write quorum: %d",
					zoneIdx, setIdx, writeQuorum))
				return false
			}
		}
	}
	return true
}

// PutObjectTags - replace or add tags to an existing object
func (z *erasureZones) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) error {
	if z.SingleZone() {
		return z.zones[0].PutObjectTags(ctx, bucket, object, tags, opts)
	}
	for _, zone := range z.zones {
		err := zone.PutObjectTags(ctx, bucket, object, tags, opts)
		if err != nil {
			if isErrBucketNotFound(err) {
				continue
			}
			return err
		}
		return nil
	}
	return BucketNotFound{
		Bucket: bucket,
	}
}

// DeleteObjectTags - delete object tags from an existing object
func (z *erasureZones) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	if z.SingleZone() {
		return z.zones[0].DeleteObjectTags(ctx, bucket, object, opts)
	}
	for _, zone := range z.zones {
		err := zone.DeleteObjectTags(ctx, bucket, object, opts)
		if err != nil {
			if isErrBucketNotFound(err) {
				continue
			}
			return err
		}
		return nil
	}
	return BucketNotFound{
		Bucket: bucket,
	}
}

// GetObjectTags - get object tags from an existing object
func (z *erasureZones) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	if z.SingleZone() {
		return z.zones[0].GetObjectTags(ctx, bucket, object, opts)
	}
	for _, zone := range z.zones {
		tags, err := zone.GetObjectTags(ctx, bucket, object, opts)
		if err != nil {
			if isErrBucketNotFound(err) {
				continue
			}
			return tags, err
		}
		return tags, nil
	}
	return nil, BucketNotFound{
		Bucket: bucket,
	}
}
