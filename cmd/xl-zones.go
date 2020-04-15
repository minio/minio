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

	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	bucketsse "github.com/minio/minio/pkg/bucket/encryption"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/bucket/object/tagging"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/sync/errgroup"
)

type xlZones struct {
	zones []*xlSets
}

func (z *xlZones) SingleZone() bool {
	return len(z.zones) == 1
}

func (z *xlZones) quickHealBuckets(ctx context.Context) {
	bucketsInfo, err := z.ListBucketsHeal(ctx)
	if err != nil {
		return
	}
	for _, bucket := range bucketsInfo {
		z.MakeBucketWithLocation(ctx, bucket.Name, "")
	}
}

// Initialize new zone of erasure sets.
func newXLZones(ctx context.Context, endpointZones EndpointZones) (ObjectLayer, error) {
	var (
		deploymentID string
		err          error

		formats      = make([]*formatXLV3, len(endpointZones))
		storageDisks = make([][]StorageAPI, len(endpointZones))
		z            = &xlZones{zones: make([]*xlSets, len(endpointZones))}
	)
	local := endpointZones.FirstLocal()
	for i, ep := range endpointZones {
		storageDisks[i], formats[i], err = waitForFormatXL(local, ep.Endpoints, i+1,
			ep.SetCount, ep.DrivesPerSet, deploymentID)
		if err != nil {
			return nil, err
		}
		if deploymentID == "" {
			deploymentID = formats[i].ID
		}
		z.zones[i], err = newXLSets(ctx, ep.Endpoints, storageDisks[i], formats[i], ep.SetCount, ep.DrivesPerSet)
		if err != nil {
			return nil, err
		}
	}
	if !z.SingleZone() {
		z.quickHealBuckets(ctx)
	}
	return z, nil
}

func (z *xlZones) NewNSLock(ctx context.Context, bucket string, objects ...string) RWLocker {
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

func (z *xlZones) getAvailableZoneIdx(ctx context.Context) int {
	zones := z.getZonesAvailableSpace(ctx)
	total := zones.TotalAvailable()
	if total == 0 {
		// Houston, we have a problem, maybe panic??
		return zones[0].Index
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
	panic(fmt.Errorf("reached end of zones (total: %v, atTotal: %v, choose: %v)", total, atTotal, choose))
}

func (z *xlZones) getZonesAvailableSpace(ctx context.Context) zonesAvailableSpace {
	var zones = make(zonesAvailableSpace, len(z.zones))

	storageInfos := make([]StorageInfo, len(z.zones))
	g := errgroup.WithNErrs(len(z.zones))
	for index := range z.zones {
		index := index
		g.Go(func() error {
			storageInfos[index] = z.zones[index].StorageInfo(ctx, false)
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
		zones[i] = zoneAvailableSpace{
			Index:     i,
			Available: available,
		}
	}
	return zones
}

func (z *xlZones) Shutdown(ctx context.Context) error {
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

func (z *xlZones) StorageInfo(ctx context.Context, local bool) StorageInfo {
	if z.SingleZone() {
		return z.zones[0].StorageInfo(ctx, local)
	}

	var storageInfo StorageInfo

	storageInfos := make([]StorageInfo, len(z.zones))
	g := errgroup.WithNErrs(len(z.zones))
	for index := range z.zones {
		index := index
		g.Go(func() error {
			storageInfos[index] = z.zones[index].StorageInfo(ctx, local)
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

	return storageInfo
}

func (z *xlZones) CrawlAndGetDataUsage(ctx context.Context, updates chan<- DataUsageInfo) error {
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
		for _, xlObj := range z.sets {
			// Add new buckets.
			buckets, err := xlObj.ListBuckets(ctx)
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
			go func(i int, xl *xlObjects) {
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
				err := xl.crawlAndGetDataUsage(ctx, buckets, updates)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					// Cancel remaining...
					cancel()
					mu.Unlock()
					return
				}
			}(len(results)-1, xlObj)
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
	updateCloser <- ch
	<-ch

	return firstErr
}

// This function is used to undo a successful MakeBucket operation.
func undoMakeBucketZones(bucket string, zones []*xlSets, errs []error) {
	g := errgroup.WithNErrs(len(zones))

	// Undo previous make bucket entry on all underlying zones.
	for index := range zones {
		index := index
		g.Go(func() error {
			if errs[index] == nil {
				return zones[index].DeleteBucket(GlobalContext, bucket, false)
			}
			return nil
		}, index)
	}

	// Wait for all delete bucket to finish.
	g.Wait()
}

// MakeBucketWithLocation - creates a new bucket across all zones simultaneously
// even if one of the sets fail to create buckets, we proceed all the successful
// operations.
func (z *xlZones) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	if z.SingleZone() {
		return z.zones[0].MakeBucketWithLocation(ctx, bucket, location)
	}

	g := errgroup.WithNErrs(len(z.zones))

	// Create buckets in parallel across all sets.
	for index := range z.zones {
		index := index
		g.Go(func() error {
			return z.zones[index].MakeBucketWithLocation(ctx, bucket, location)
		}, index)
	}

	errs := g.Wait()
	// Upon even a single write quorum error we undo all previously created buckets.
	for _, err := range errs {
		if err != nil {
			if _, ok := err.(InsufficientWriteQuorum); ok {
				undoMakeBucketZones(bucket, z.zones, errs)
			}
			return err
		}
	}

	// Success.
	return nil

}

func (z *xlZones) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
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

func (z *xlZones) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) error {
	// Lock the object before reading.
	objectLock := z.NewNSLock(ctx, bucket, object)
	if err := objectLock.GetRLock(globalObjectTimeout); err != nil {
		return err
	}
	defer objectLock.RUnlock()

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

func (z *xlZones) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	// Lock the object before reading.
	objectLock := z.NewNSLock(ctx, bucket, object)
	if err := objectLock.GetRLock(globalObjectTimeout); err != nil {
		return ObjectInfo{}, err
	}
	defer objectLock.RUnlock()

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
func (z *xlZones) PutObject(ctx context.Context, bucket string, object string, data *PutObjReader, opts ObjectOptions) (ObjectInfo, error) {
	// Lock the object.
	objectLock := z.NewNSLock(ctx, bucket, object)
	if err := objectLock.GetLock(globalObjectTimeout); err != nil {
		return ObjectInfo{}, err
	}
	defer objectLock.Unlock()

	if z.SingleZone() {
		return z.zones[0].PutObject(ctx, bucket, object, data, opts)
	}

	for _, zone := range z.zones {
		objInfo, err := zone.GetObjectInfo(ctx, bucket, object, opts)
		if err != nil {
			if isErrObjectNotFound(err) {
				continue
			}
			return objInfo, err
		}
		// Overwrite request upload to right zone.
		return zone.PutObject(ctx, bucket, object, data, opts)
	}
	// Object not found pick the least used and upload to this zone.
	return z.zones[z.getAvailableZoneIdx(ctx)].PutObject(ctx, bucket, object, data, opts)
}

func (z *xlZones) DeleteObject(ctx context.Context, bucket string, object string) error {
	// Acquire a write lock before deleting the object.
	objectLock := z.NewNSLock(ctx, bucket, object)
	if err := objectLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objectLock.Unlock()

	if z.SingleZone() {
		return z.zones[0].DeleteObject(ctx, bucket, object)
	}
	for _, zone := range z.zones {
		err := zone.DeleteObject(ctx, bucket, object)
		if err != nil && !isErrObjectNotFound(err) {
			return err
		}
	}
	return nil
}

func (z *xlZones) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	derrs := make([]error, len(objects))
	for i := range derrs {
		derrs[i] = checkDelObjArgs(ctx, bucket, objects[i])
	}

	// Acquire a bulk write lock across 'objects'
	multiDeleteLock := z.NewNSLock(ctx, bucket, objects...)
	if err := multiDeleteLock.GetLock(globalOperationTimeout); err != nil {
		return nil, err
	}
	defer multiDeleteLock.Unlock()

	for _, zone := range z.zones {
		errs, err := zone.DeleteObjects(ctx, bucket, objects)
		if err != nil {
			return nil, err
		}
		for i, derr := range errs {
			if derrs[i] == nil {
				if derr != nil && !isErrObjectNotFound(derr) {
					derrs[i] = derr
				}
			}
		}
	}
	return derrs, nil
}

func (z *xlZones) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	// Check if this request is only metadata update.
	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(destBucket, destObject))
	if !cpSrcDstSame {
		objectLock := z.NewNSLock(ctx, destBucket, destObject)
		if err := objectLock.GetLock(globalObjectTimeout); err != nil {
			return objInfo, err
		}
		defer objectLock.Unlock()
	}

	if z.SingleZone() {
		return z.zones[0].CopyObject(ctx, srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, dstOpts)
	}
	if cpSrcDstSame && srcInfo.metadataOnly {
		for _, zone := range z.zones {
			objInfo, err = zone.CopyObject(ctx, srcBucket, srcObject, destBucket,
				destObject, srcInfo, srcOpts, dstOpts)
			if err != nil {
				if isErrObjectNotFound(err) {
					continue
				}
				return objInfo, err
			}
			return objInfo, nil
		}
		return objInfo, ObjectNotFound{Bucket: srcBucket, Object: srcObject}
	}
	return z.zones[z.getAvailableZoneIdx(ctx)].CopyObject(ctx, srcBucket, srcObject,
		destBucket, destObject, srcInfo, srcOpts, dstOpts)
}

func (z *xlZones) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error) {
	if z.SingleZone() {
		return z.zones[0].ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	}
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

func (z *xlZones) listObjectsNonSlash(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {

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

		result, quorumCount, _, ok := leastEntryZone(zonesEntryChs, zonesEntriesInfos, zonesEntriesValid)
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

func (z *xlZones) listObjectsSplunk(ctx context.Context, bucket, prefix, marker string, maxKeys int) (loi ListObjectsInfo, err error) {
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
		objInfo := entry.ToObjectInfo()
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

func (z *xlZones) listObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
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
		objInfo := entry.ToObjectInfo()
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
// if the caller wishes to list N entries to call leastEntry
// N times until this boolean is 'false'.
func leastEntryZone(zoneEntryChs [][]FileInfoCh, zoneEntries [][]FileInfo, zoneEntriesValid [][]bool) (FileInfo, int, int, bool) {
	for i, entryChs := range zoneEntryChs {
		for j := range entryChs {
			zoneEntries[i][j], zoneEntriesValid[i][j] = entryChs[j].Pop()
		}
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

	leastEntryCount := 0
	for i, entriesValid := range zoneEntriesValid {
		for j, valid := range entriesValid {
			if !valid {
				continue
			}

			// Entries are duplicated across disks,
			// we should simply skip such entries.
			if lentry.Name == zoneEntries[i][j].Name && lentry.ModTime.Equal(zoneEntries[i][j].ModTime) {
				leastEntryCount++
				continue
			}

			// Push all entries which are lexically higher
			// and will be returned later in Pop()
			zoneEntryChs[i][j].Push(zoneEntries[i][j])
		}
	}

	return lentry, leastEntryCount, zoneIndex, isTruncated
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
		fi, quorumCount, _, ok := leastEntryZone(zonesEntryChs, zonesEntriesInfos, zonesEntriesValid)
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
		for j := range entryChs {
			zoneEntries[i][j], zoneEntriesValid[i][j] = entryChs[j].Pop()
		}
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

func (z *xlZones) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	if z.SingleZone() {
		return z.zones[0].ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	}

	return z.listObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
}

func (z *xlZones) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
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
func (z *xlZones) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (string, error) {
	if z.SingleZone() {
		return z.zones[0].NewMultipartUpload(ctx, bucket, object, opts)
	}
	return z.zones[z.getAvailableZoneIdx(ctx)].NewMultipartUpload(ctx, bucket, object, opts)
}

// Copies a part of an object from source hashedSet to destination hashedSet.
func (z *xlZones) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (PartInfo, error) {
	return z.PutObjectPart(ctx, destBucket, destObject, uploadID, partID,
		NewPutObjReader(srcInfo.Reader, nil, nil), dstOpts)
}

// PutObjectPart - writes part of an object to hashedSet based on the object name.
func (z *xlZones) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (PartInfo, error) {
	uploadIDLock := z.NewNSLock(ctx, bucket, pathJoin(object, uploadID))
	if err := uploadIDLock.GetLock(globalOperationTimeout); err != nil {
		return PartInfo{}, err
	}
	defer uploadIDLock.Unlock()

	if z.SingleZone() {
		return z.zones[0].PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
	}
	for _, zone := range z.zones {
		result, err := zone.ListMultipartUploads(ctx, bucket, object, "", "", "", maxObjectList)
		if err != nil {
			return PartInfo{}, err
		}
		if result.Lookup(uploadID) {
			return zone.PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
		}
	}

	return PartInfo{}, InvalidUploadID{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}
}

// ListObjectParts - lists all uploaded parts to an object in hashedSet.
func (z *xlZones) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (ListPartsInfo, error) {
	uploadIDLock := z.NewNSLock(ctx, bucket, pathJoin(object, uploadID))
	if err := uploadIDLock.GetRLock(globalOperationTimeout); err != nil {
		return ListPartsInfo{}, err
	}
	defer uploadIDLock.RUnlock()

	if z.SingleZone() {
		return z.zones[0].ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
	}
	for _, zone := range z.zones {
		result, err := zone.ListMultipartUploads(ctx, bucket, object, "", "", "", maxObjectList)
		if err != nil {
			return ListPartsInfo{}, err
		}
		if result.Lookup(uploadID) {
			return zone.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
		}
	}
	return ListPartsInfo{}, InvalidUploadID{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}
}

// Aborts an in-progress multipart operation on hashedSet based on the object name.
func (z *xlZones) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	uploadIDLock := z.NewNSLock(ctx, bucket, pathJoin(object, uploadID))
	if err := uploadIDLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer uploadIDLock.Unlock()

	if z.SingleZone() {
		return z.zones[0].AbortMultipartUpload(ctx, bucket, object, uploadID)
	}
	for _, zone := range z.zones {
		result, err := zone.ListMultipartUploads(ctx, bucket, object, "", "", "", maxObjectList)
		if err != nil {
			return err
		}
		if result.Lookup(uploadID) {
			return zone.AbortMultipartUpload(ctx, bucket, object, uploadID)
		}
	}
	return InvalidUploadID{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}
}

// CompleteMultipartUpload - completes a pending multipart transaction, on hashedSet based on object name.
func (z *xlZones) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	// Hold read-locks to verify uploaded parts, also disallows
	// parallel part uploads as well.
	uploadIDLock := z.NewNSLock(ctx, bucket, pathJoin(object, uploadID))
	if err = uploadIDLock.GetRLock(globalOperationTimeout); err != nil {
		return objInfo, err
	}
	defer uploadIDLock.RUnlock()

	// Hold namespace to complete the transaction, only hold
	// if uploadID can be held exclusively.
	objectLock := z.NewNSLock(ctx, bucket, object)
	if err = objectLock.GetLock(globalOperationTimeout); err != nil {
		return objInfo, err
	}
	defer objectLock.Unlock()

	if z.SingleZone() {
		return z.zones[0].CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
	}

	// Purge any existing object.
	for _, zone := range z.zones {
		zone.DeleteObject(ctx, bucket, object)
	}

	for _, zone := range z.zones {
		result, err := zone.ListMultipartUploads(ctx, bucket, object, "", "", "", maxObjectList)
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
func (z *xlZones) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error) {
	if z.SingleZone() {
		return z.zones[0].GetBucketInfo(ctx, bucket)
	}
	for _, zone := range z.zones {
		bucketInfo, err = zone.GetBucketInfo(ctx, bucket)
		if err != nil {
			if isErrBucketNotFound(err) {
				continue
			}
			return bucketInfo, err
		}
		return bucketInfo, nil
	}
	return bucketInfo, BucketNotFound{
		Bucket: bucket,
	}
}

// SetBucketPolicy persist the new policy on the bucket.
func (z *xlZones) SetBucketPolicy(ctx context.Context, bucket string, policy *policy.Policy) error {
	return savePolicyConfig(ctx, z, bucket, policy)
}

// GetBucketPolicy will return a policy on a bucket
func (z *xlZones) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	return getPolicyConfig(z, bucket)
}

// DeleteBucketPolicy deletes all policies on bucket
func (z *xlZones) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return removePolicyConfig(ctx, z, bucket)
}

// SetBucketLifecycle zones lifecycle on bucket
func (z *xlZones) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *lifecycle.Lifecycle) error {
	return saveLifecycleConfig(ctx, z, bucket, lifecycle)
}

// GetBucketLifecycle will get lifecycle on bucket
func (z *xlZones) GetBucketLifecycle(ctx context.Context, bucket string) (*lifecycle.Lifecycle, error) {
	return getLifecycleConfig(z, bucket)
}

// DeleteBucketLifecycle deletes all lifecycle on bucket
func (z *xlZones) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	return removeLifecycleConfig(ctx, z, bucket)
}

// GetBucketSSEConfig returns bucket encryption config on given bucket
func (z *xlZones) GetBucketSSEConfig(ctx context.Context, bucket string) (*bucketsse.BucketSSEConfig, error) {
	return getBucketSSEConfig(z, bucket)
}

// SetBucketSSEConfig sets bucket encryption config on given bucket
func (z *xlZones) SetBucketSSEConfig(ctx context.Context, bucket string, config *bucketsse.BucketSSEConfig) error {
	return saveBucketSSEConfig(ctx, z, bucket, config)
}

// DeleteBucketSSEConfig deletes bucket encryption config on given bucket
func (z *xlZones) DeleteBucketSSEConfig(ctx context.Context, bucket string) error {
	return removeBucketSSEConfig(ctx, z, bucket)
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (z *xlZones) IsNotificationSupported() bool {
	return true
}

// IsListenBucketSupported returns whether listen bucket notification is applicable for this layer.
func (z *xlZones) IsListenBucketSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (z *xlZones) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (z *xlZones) IsCompressionSupported() bool {
	return true
}

// DeleteBucket - deletes a bucket on all zones simultaneously,
// even if one of the zones fail to delete buckets, we proceed to
// undo a successful operation.
func (z *xlZones) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
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

	if forceDelete {
		for _, err := range errs {
			if err != nil {
				if _, ok := err.(InsufficientWriteQuorum); ok {
					undoDeleteBucketZones(bucket, z.zones, errs)
				}

				return err
			}
		}

		return nil
	}

	// For any write quorum failure, we undo all the delete buckets operation
	// by creating all the buckets again.
	for _, err := range errs {
		if err != nil {
			if _, ok := err.(InsufficientWriteQuorum); ok {
				undoDeleteBucketZones(bucket, z.zones, errs)
			}
			return err
		}
	}

	// Success.
	return nil
}

// This function is used to undo a successful DeleteBucket operation.
func undoDeleteBucketZones(bucket string, zones []*xlSets, errs []error) {
	g := errgroup.WithNErrs(len(zones))

	// Undo previous delete bucket on all underlying zones.
	for index := range zones {
		index := index
		g.Go(func() error {
			if errs[index] == nil {
				return zones[index].MakeBucketWithLocation(GlobalContext, bucket, "")
			}
			return nil
		}, index)
	}

	g.Wait()
}

// List all buckets from one of the zones, we are not doing merge
// sort here just for simplification. As per design it is assumed
// that all buckets are present on all zones.
func (z *xlZones) ListBuckets(ctx context.Context) (buckets []BucketInfo, err error) {
	if z.SingleZone() {
		return z.zones[0].ListBuckets(ctx)
	}
	for _, zone := range z.zones {
		buckets, err := zone.ListBuckets(ctx)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		return buckets, nil
	}
	return buckets, InsufficientReadQuorum{}
}

func (z *xlZones) ReloadFormat(ctx context.Context, dryRun bool) error {
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

func (z *xlZones) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
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

func (z *xlZones) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (madmin.HealResultItem, error) {
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
func (z *xlZones) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo) error {
	if err := checkListObjsArgs(ctx, bucket, prefix, "", z); err != nil {
		// Upon error close the channel.
		close(results)
		return err
	}

	var zonesEntryChs [][]FileInfoCh

	for _, zone := range z.zones {
		zonesEntryChs = append(zonesEntryChs,
			zone.startMergeWalks(ctx, bucket, prefix, "", true, ctx.Done()))
	}

	var zoneDrivesPerSet []int
	for _, zone := range z.zones {
		zoneDrivesPerSet = append(zoneDrivesPerSet, zone.drivesPerSet)
	}

	var zonesEntriesInfos [][]FileInfo
	var zonesEntriesValid [][]bool
	for _, entryChs := range zonesEntryChs {
		zonesEntriesInfos = append(zonesEntriesInfos, make([]FileInfo, len(entryChs)))
		zonesEntriesValid = append(zonesEntriesValid, make([]bool, len(entryChs)))
	}

	go func() {
		defer close(results)

		for {
			entry, quorumCount, zoneIndex, ok := leastEntryZone(zonesEntryChs,
				zonesEntriesInfos, zonesEntriesValid)
			if !ok {
				return
			}

			if quorumCount >= zoneDrivesPerSet[zoneIndex]/2 {
				results <- entry.ToObjectInfo() // Read quorum exists proceed
			}

			// skip entries which do not have quorum
		}
	}()

	return nil
}

type healObjectFn func(string, string) error

func (z *xlZones) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, healObject healObjectFn) error {
	var zonesEntryChs [][]FileInfoCh

	endWalkCh := make(chan struct{})
	defer close(endWalkCh)

	for _, zone := range z.zones {
		zonesEntryChs = append(zonesEntryChs,
			zone.startMergeWalks(ctx, bucket, prefix, "", true, endWalkCh))
	}

	var zoneDrivesPerSet []int
	for _, zone := range z.zones {
		zoneDrivesPerSet = append(zoneDrivesPerSet, zone.drivesPerSet)
	}

	var zonesEntriesInfos [][]FileInfo
	var zonesEntriesValid [][]bool
	for _, entryChs := range zonesEntryChs {
		zonesEntriesInfos = append(zonesEntriesInfos, make([]FileInfo, len(entryChs)))
		zonesEntriesValid = append(zonesEntriesValid, make([]bool, len(entryChs)))
	}

	for {
		entry, quorumCount, zoneIndex, ok := leastEntryZone(zonesEntryChs, zonesEntriesInfos, zonesEntriesValid)
		if !ok {
			break
		}

		if quorumCount == zoneDrivesPerSet[zoneIndex] && opts.ScanMode == madmin.HealNormalScan {
			// Skip good entries.
			continue
		}

		// Wait and proceed if there are active requests
		waitForLowHTTPReq(int32(zoneDrivesPerSet[zoneIndex]))

		if err := healObject(bucket, entry.Name); err != nil {
			return toObjectErr(err, bucket, entry.Name)
		}
	}

	return nil
}

func (z *xlZones) HealObject(ctx context.Context, bucket, object string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	// Lock the object before healing. Use read lock since healing
	// will only regenerate parts & xl.json of outdated disks.
	objectLock := z.NewNSLock(ctx, bucket, object)
	if err := objectLock.GetRLock(globalHealingTimeout); err != nil {
		return madmin.HealResultItem{}, err
	}
	defer objectLock.RUnlock()

	if z.SingleZone() {
		return z.zones[0].HealObject(ctx, bucket, object, opts)
	}
	for _, zone := range z.zones {
		result, err := zone.HealObject(ctx, bucket, object, opts)
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

func (z *xlZones) ListBucketsHeal(ctx context.Context) ([]BucketInfo, error) {
	var healBuckets []BucketInfo
	for _, zone := range z.zones {
		bucketsInfo, err := zone.ListBucketsHeal(ctx)
		if err != nil {
			continue
		}
		healBuckets = append(healBuckets, bucketsInfo...)
	}
	return healBuckets, nil
}

// GetMetrics - no op
func (z *xlZones) GetMetrics(ctx context.Context) (*Metrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &Metrics{}, NotImplemented{}
}

// IsReady - Returns true if first zone returns true
func (z *xlZones) IsReady(ctx context.Context) bool {
	return z.zones[0].IsReady(ctx)
}

// PutObjectTag - replace or add tags to an existing object
func (z *xlZones) PutObjectTag(ctx context.Context, bucket, object string, tags string) error {
	if z.SingleZone() {
		return z.zones[0].PutObjectTag(ctx, bucket, object, tags)
	}
	for _, zone := range z.zones {
		err := zone.PutObjectTag(ctx, bucket, object, tags)
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

// DeleteObjectTag - delete object tags from an existing object
func (z *xlZones) DeleteObjectTag(ctx context.Context, bucket, object string) error {
	if z.SingleZone() {
		return z.zones[0].DeleteObjectTag(ctx, bucket, object)
	}
	for _, zone := range z.zones {
		err := zone.DeleteObjectTag(ctx, bucket, object)
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

// GetObjectTag - get object tags from an existing object
func (z *xlZones) GetObjectTag(ctx context.Context, bucket, object string) (tagging.Tagging, error) {
	if z.SingleZone() {
		return z.zones[0].GetObjectTag(ctx, bucket, object)
	}
	for _, zone := range z.zones {
		tags, err := zone.GetObjectTag(ctx, bucket, object)
		if err != nil {
			if isErrBucketNotFound(err) {
				continue
			}
			return tags, err
		}
		return tags, nil
	}
	return tagging.Tagging{}, BucketNotFound{
		Bucket: bucket,
	}
}
