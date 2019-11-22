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

	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/lifecycle"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
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
		z.HealBucket(ctx, bucket.Name, false, false)
	}
}

// Initialize new zone of erasure sets.
func newXLZones(endpointZones EndpointZones) (ObjectLayer, error) {
	var (
		deploymentID string
		err          error

		formats = make([]*formatXLV3, len(endpointZones))
		z       = &xlZones{zones: make([]*xlSets, len(endpointZones))}
	)
	for i, ep := range endpointZones {
		formats[i], err = waitForFormatXL(endpointZones.FirstLocal(), ep.Endpoints,
			ep.SetCount, ep.DrivesPerSet, deploymentID)
		if err != nil {
			return nil, err
		}
		if deploymentID == "" {
			deploymentID = formats[i].ID
		}
	}
	for i, ep := range endpointZones {
		z.zones[i], err = newXLSets(ep.Endpoints, formats[i], ep.SetCount, ep.DrivesPerSet)
		if err != nil {
			return nil, err
		}
	}
	z.quickHealBuckets(context.Background())
	return z, nil
}

func (z *xlZones) NewNSLock(ctx context.Context, bucket string, object string) RWLocker {
	return z.zones[0].NewNSLock(ctx, bucket, object)
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
			storageInfos[index] = z.zones[index].StorageInfo(ctx)
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

func (z *xlZones) StorageInfo(ctx context.Context) StorageInfo {
	if z.SingleZone() {
		return z.zones[0].StorageInfo(ctx)
	}

	var storageInfo StorageInfo

	storageInfos := make([]StorageInfo, len(z.zones))
	g := errgroup.WithNErrs(len(z.zones))
	for index := range z.zones {
		index := index
		g.Go(func() error {
			storageInfos[index] = z.zones[index].StorageInfo(ctx)
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

// This function is used to undo a successful MakeBucket operation.
func undoMakeBucketZones(bucket string, zones []*xlSets, errs []error) {
	g := errgroup.WithNErrs(len(zones))

	// Undo previous make bucket entry on all underlying zones.
	for index := range zones {
		index := index
		g.Go(func() error {
			if errs[index] == nil {
				return zones[index].DeleteBucket(context.Background(), bucket)
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

	var objectLocks = make([]RWLocker, len(objects))
	for i := range objects {
		if derrs[i] != nil {
			continue
		}

		// Acquire a write lock before deleting the object.
		objectLocks[i] = z.NewNSLock(ctx, bucket, objects[i])
		if derrs[i] = objectLocks[i].GetLock(globalOperationTimeout); derrs[i] != nil {
			continue
		}

		defer objectLocks[i].Unlock()
	}

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

	recursive := true
	for _, zone := range z.zones {
		endWalkCh := make(chan struct{})
		defer close(endWalkCh)
		zonesEntryChs = append(zonesEntryChs,
			zone.startMergeWalks(ctx, bucket, prefix, "", recursive, endWalkCh))
	}

	var objInfos []ObjectInfo
	var eof bool
	var prevPrefix string

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
		if len(objInfos) == maxKeys {
			break
		}
		result, quorumCount, zoneIndex, ok := leastEntryZone(zonesEntryChs, zonesEntriesInfos, zonesEntriesValid)
		if !ok {
			eof = true
			break
		}
		rquorum := result.Quorum
		// Quorum is zero for all directories.
		if rquorum == 0 {
			// Choose N/2 quorum for directory entries.
			rquorum = zoneDrivesPerSet[zoneIndex] / 2
		}
		if quorumCount < rquorum {
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

func (z *xlZones) listObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int, heal bool) (ListObjectsInfo, error) {
	loi := ListObjectsInfo{}

	if err := checkListObjsArgs(ctx, bucket, prefix, marker, delimiter, z); err != nil {
		return loi, err
	}

	// Marker is set validate pre-condition.
	if marker != "" {
		// Marker not common with prefix is not implemented. Send an empty response
		if !hasPrefix(marker, prefix) {
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
		// "heal" option passed can be ignored as the heal-listing does not send non-standard delimiter.
		return z.listObjectsNonSlash(ctx, bucket, prefix, marker, delimiter, maxKeys)
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == SlashSeparator {
		recursive = false
	}

	var zonesEntryChs [][]FileInfoCh
	var zonesEndWalkCh []chan struct{}

	for _, zone := range z.zones {
		entryChs, endWalkCh := zone.pool.Release(listParams{bucket, recursive, marker, prefix, heal})
		if entryChs == nil {
			endWalkCh = make(chan struct{})
			entryChs = zone.startMergeWalks(ctx, bucket, prefix, marker, recursive, endWalkCh)
		}
		zonesEntryChs = append(zonesEntryChs, entryChs)
		zonesEndWalkCh = append(zonesEndWalkCh, endWalkCh)
	}

	var zoneDrivesPerSet []int
	for _, zone := range z.zones {
		zoneDrivesPerSet = append(zoneDrivesPerSet, zone.drivesPerSet)
	}

	entries := mergeZonesEntriesCh(zonesEntryChs, maxKeys, zoneDrivesPerSet, heal)
	if len(entries.Files) == 0 {
		return loi, nil
	}

	loi.IsTruncated = entries.IsTruncated
	if loi.IsTruncated {
		loi.NextMarker = entries.Files[len(entries.Files)-1].Name
	}

	for _, entry := range entries.Files {
		var objInfo ObjectInfo
		if hasSuffix(entry.Name, SlashSeparator) {
			if !recursive {
				loi.Prefixes = append(loi.Prefixes, entry.Name)
				continue
			}
			objInfo = ObjectInfo{
				Bucket: bucket,
				Name:   entry.Name,
				IsDir:  true,
			}
		} else {
			objInfo = ObjectInfo{
				IsDir:           false,
				Bucket:          bucket,
				Name:            entry.Name,
				ModTime:         entry.ModTime,
				Size:            entry.Size,
				ContentType:     entry.Metadata["content-type"],
				ContentEncoding: entry.Metadata["content-encoding"],
			}

			// Extract etag from metadata.
			objInfo.ETag = extractETag(entry.Metadata)

			// All the parts per object.
			objInfo.Parts = entry.Parts

			// etag/md5Sum has already been extracted. We need to
			// remove to avoid it from appearing as part of
			// response headers. e.g, X-Minio-* or X-Amz-*.
			objInfo.UserDefined = cleanMetadata(entry.Metadata)

			// Update storage class
			if sc, ok := entry.Metadata[xhttp.AmzStorageClass]; ok {
				objInfo.StorageClass = sc
			} else {
				objInfo.StorageClass = globalMinioDefaultStorageClass
			}
		}
		loi.Objects = append(loi.Objects, objInfo)
	}
	if loi.IsTruncated {
		for i, zone := range z.zones {
			zone.pool.Set(listParams{bucket, recursive, loi.NextMarker, prefix, heal}, zonesEntryChs[i],
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
func mergeZonesEntriesCh(zonesEntryChs [][]FileInfoCh, maxKeys int, zoneDrives []int, heal bool) (entries FilesInfo) {
	var i = 0
	var zonesEntriesInfos [][]FileInfo
	var zonesEntriesValid [][]bool
	for _, entryChs := range zonesEntryChs {
		zonesEntriesInfos = append(zonesEntriesInfos, make([]FileInfo, len(entryChs)))
		zonesEntriesValid = append(zonesEntriesValid, make([]bool, len(entryChs)))
	}
	for {
		fi, quorumCount, zoneIndex, valid := leastEntryZone(zonesEntryChs, zonesEntriesInfos, zonesEntriesValid)
		if !valid {
			// We have reached EOF across all entryChs, break the loop.
			break
		}
		rquorum := fi.Quorum
		// Quorum is zero for all directories.
		if rquorum == 0 {
			// Choose N/2 quoroum for directory entries.
			rquorum = zoneDrives[zoneIndex] / 2
		}

		if heal {
			// When healing is enabled, we should
			// list only objects which need healing.
			if quorumCount == zoneDrives[zoneIndex] {
				// Skip good entries.
				continue
			}
		} else {
			// Regular listing, we skip entries not in quorum.
			if quorumCount < rquorum {
				// Skip entries which do not have quorum.
				continue
			}
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

	return z.listObjects(ctx, bucket, prefix, marker, delimiter, maxKeys, false)
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
func (z *xlZones) DeleteBucket(ctx context.Context, bucket string) error {
	if z.SingleZone() {
		return z.zones[0].DeleteBucket(ctx, bucket)
	}
	g := errgroup.WithNErrs(len(z.zones))

	// Delete buckets in parallel across all zones.
	for index := range z.zones {
		index := index
		g.Go(func() error {
			return z.zones[index].DeleteBucket(ctx, bucket)
		}, index)
	}

	errs := g.Wait()
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
				return zones[index].MakeBucketWithLocation(context.Background(), bucket, "")
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
	for _, zone := range z.zones {
		result, err := zone.HealFormat(ctx, dryRun)
		if err != nil && err != errNoHealRequired {
			logger.LogIf(ctx, err)
			continue
		}
		r.DiskCount += result.DiskCount
		r.SetCount += result.SetCount
		r.Before.Drives = append(r.Before.Drives, result.Before.Drives...)
		r.After.Drives = append(r.After.Drives, result.After.Drives...)
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

func (z *xlZones) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	if z.SingleZone() {
		return z.zones[0].ListObjectsHeal(ctx, bucket, prefix, marker, delimiter, maxKeys)
	}
	return z.listObjects(ctx, bucket, prefix, marker, delimiter, maxKeys, true)
}

func (z *xlZones) HealObjects(ctx context.Context, bucket, prefix string, healObjectFn func(string, string) error) error {
	for _, zone := range z.zones {
		if err := zone.HealObjects(ctx, bucket, prefix, healObjectFn); err != nil {
			return err
		}
	}
	return nil
}

func (z *xlZones) HealObject(ctx context.Context, bucket, object string, dryRun, remove bool, scanMode madmin.HealScanMode) (madmin.HealResultItem, error) {
	// Lock the object before healing. Use read lock since healing
	// will only regenerate parts & xl.json of outdated disks.
	objectLock := z.NewNSLock(ctx, bucket, object)
	if err := objectLock.GetRLock(globalHealingTimeout); err != nil {
		return madmin.HealResultItem{}, err
	}
	defer objectLock.RUnlock()

	if z.SingleZone() {
		return z.zones[0].HealObject(ctx, bucket, object, dryRun, remove, scanMode)
	}
	for _, zone := range z.zones {
		result, err := zone.HealObject(ctx, bucket, object, dryRun, remove, scanMode)
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
