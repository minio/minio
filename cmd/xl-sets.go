/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"hash/crc32"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bpool"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// setsStorageAPI is encapsulated type for Close()
type setsStorageAPI [][]StorageAPI

func (s setsStorageAPI) Close() error {
	for i := 0; i < len(s); i++ {
		for _, disk := range s[i] {
			if disk == nil {
				continue
			}
			disk.Close()
		}
	}
	return nil
}

// xlSets implements ObjectLayer combining a static list of erasure coded
// object sets. NOTE: There is no dynamic scaling allowed or intended in
// current design.
type xlSets struct {
	sets []*xlObjects

	// Reference format.
	format *formatXLV3

	// xlDisks mutex to lock xlDisks.
	xlDisksMu sync.RWMutex

	// Re-ordered list of disks per set.
	xlDisks setsStorageAPI

	// List of endpoints provided on the command line.
	endpoints EndpointList

	// Total number of sets and the number of disks per set.
	setCount, drivesPerSet int

	// Done channel to control monitoring loop.
	disksConnectDoneCh chan struct{}

	// Distribution algorithm of choice.
	distributionAlgo string

	// Pack level listObjects pool management.
	listPool *TreeWalkPool
}

// isConnected - checks if the endpoint is connected or not.
func (s *xlSets) isConnected(endpoint Endpoint) bool {
	s.xlDisksMu.RLock()
	defer s.xlDisksMu.RUnlock()

	for i := 0; i < s.setCount; i++ {
		for j := 0; j < s.drivesPerSet; j++ {
			if s.xlDisks[i][j] == nil {
				continue
			}
			var endpointStr string
			if endpoint.IsLocal {
				endpointStr = endpoint.Path
			} else {
				endpointStr = endpoint.String()
			}
			if s.xlDisks[i][j].String() != endpointStr {
				continue
			}
			return s.xlDisks[i][j].IsOnline()
		}
	}
	return false
}

// Initializes a new StorageAPI from the endpoint argument, returns
// StorageAPI and also `format` which exists on the disk.
func connectEndpoint(endpoint Endpoint) (StorageAPI, *formatXLV3, error) {
	disk, err := newStorageAPI(endpoint)
	if err != nil {
		return nil, nil, err
	}

	format, err := loadFormatXL(disk)
	if err != nil {
		// Close the internal connection to avoid connection leaks.
		disk.Close()
		return nil, nil, err
	}

	return disk, format, nil
}

// findDiskIndex - returns the i,j'th position of the input `format` against the reference
// format, after successful validation.
func findDiskIndex(refFormat, format *formatXLV3) (int, int, error) {
	if err := formatXLV3Check(refFormat, format); err != nil {
		return 0, 0, err
	}

	if format.XL.This == offlineDiskUUID {
		return -1, -1, fmt.Errorf("diskID: %s is offline", format.XL.This)
	}

	for i := 0; i < len(refFormat.XL.Sets); i++ {
		for j := 0; j < len(refFormat.XL.Sets[0]); j++ {
			if refFormat.XL.Sets[i][j] == format.XL.This {
				return i, j, nil
			}
		}
	}

	return -1, -1, fmt.Errorf("diskID: %s not found", format.XL.This)
}

// Re initializes all disks based on the reference format, this function is
// only used by HealFormat and ReloadFormat calls.
func (s *xlSets) reInitDisks(refFormat *formatXLV3, storageDisks []StorageAPI, formats []*formatXLV3) [][]StorageAPI {
	xlDisks := make([][]StorageAPI, s.setCount)
	for i := 0; i < len(refFormat.XL.Sets); i++ {
		xlDisks[i] = make([]StorageAPI, s.drivesPerSet)
	}
	for k := range storageDisks {
		if storageDisks[k] == nil || formats[k] == nil {
			continue
		}
		i, j, err := findDiskIndex(refFormat, formats[k])
		if err != nil {
			reqInfo := (&logger.ReqInfo{}).AppendTags("storageDisk", storageDisks[i].String())
			ctx := logger.SetReqInfo(context.Background(), reqInfo)
			logger.LogIf(ctx, err)
			continue
		}
		xlDisks[i][j] = storageDisks[k]
	}
	return xlDisks
}

// connectDisksWithQuorum is same as connectDisks but waits
// for quorum number of formatted disks to be online in
// any given sets.
func (s *xlSets) connectDisksWithQuorum() {
	var onlineDisks int
	for onlineDisks < len(s.endpoints)/2 {
		for _, endpoint := range s.endpoints {
			if s.isConnected(endpoint) {
				continue
			}
			disk, format, err := connectEndpoint(endpoint)
			if err != nil {
				printEndpointError(endpoint, err)
				continue
			}
			i, j, err := findDiskIndex(s.format, format)
			if err != nil {
				// Close the internal connection to avoid connection leaks.
				disk.Close()
				printEndpointError(endpoint, err)
				continue
			}
			s.xlDisks[i][j] = disk
			onlineDisks++
		}
		// Sleep for a while - so that we don't go into
		// 100% CPU when half the disks are online.
		time.Sleep(500 * time.Millisecond)
	}
}

// connectDisks - attempt to connect all the endpoints, loads format
// and re-arranges the disks in proper position.
func (s *xlSets) connectDisks() {
	for _, endpoint := range s.endpoints {
		if s.isConnected(endpoint) {
			continue
		}
		disk, format, err := connectEndpoint(endpoint)
		if err != nil {
			printEndpointError(endpoint, err)
			continue
		}
		i, j, err := findDiskIndex(s.format, format)
		if err != nil {
			// Close the internal connection to avoid connection leaks.
			disk.Close()
			printEndpointError(endpoint, err)
			continue
		}
		s.xlDisksMu.Lock()
		s.xlDisks[i][j] = disk
		s.xlDisksMu.Unlock()
	}
}

// monitorAndConnectEndpoints this is a monitoring loop to keep track of disconnected
// endpoints by reconnecting them and making sure to place them into right position in
// the set topology, this monitoring happens at a given monitoring interval.
func (s *xlSets) monitorAndConnectEndpoints(monitorInterval time.Duration) {
	ticker := time.NewTicker(monitorInterval)
	// Stop the timer.
	defer ticker.Stop()

	for {
		select {
		case <-GlobalServiceDoneCh:
			return
		case <-s.disksConnectDoneCh:
			return
		case <-ticker.C:
			s.connectDisks()
		}
	}
}

// GetDisks returns a closure for a given set, which provides list of disks per set.
func (s *xlSets) GetDisks(setIndex int) func() []StorageAPI {
	return func() []StorageAPI {
		s.xlDisksMu.Lock()
		defer s.xlDisksMu.Unlock()
		disks := make([]StorageAPI, s.drivesPerSet)
		copy(disks, s.xlDisks[setIndex])
		return disks
	}
}

const defaultMonitorConnectEndpointInterval = time.Second * 10 // Set to 10 secs.

// Initialize new set of erasure coded sets.
func newXLSets(endpoints EndpointList, format *formatXLV3, setCount int, drivesPerSet int) (ObjectLayer, error) {

	// Initialize the XL sets instance.
	s := &xlSets{
		sets:               make([]*xlObjects, setCount),
		xlDisks:            make([][]StorageAPI, setCount),
		endpoints:          endpoints,
		setCount:           setCount,
		drivesPerSet:       drivesPerSet,
		format:             format,
		disksConnectDoneCh: make(chan struct{}),
		distributionAlgo:   format.XL.DistributionAlgo,
		listPool:           NewTreeWalkPool(globalLookupTimeout),
	}

	mutex := newNSLock(globalIsDistXL)

	// Initialize byte pool once for all sets, bpool size is set to
	// setCount * drivesPerSet with each memory upto blockSizeV1.
	bp := bpool.NewBytePoolCap(setCount*drivesPerSet, blockSizeV1, blockSizeV1*2)

	for i := 0; i < len(format.XL.Sets); i++ {
		s.xlDisks[i] = make([]StorageAPI, drivesPerSet)

		// Initialize xl objects for a given set.
		s.sets[i] = &xlObjects{
			getDisks: s.GetDisks(i),
			nsMutex:  mutex,
			bp:       bp,
		}
		go s.sets[i].cleanupStaleMultipartUploads(context.Background(), GlobalMultipartCleanupInterval, GlobalMultipartExpiry, GlobalServiceDoneCh)
	}

	// Connect disks right away, but wait until we have `format.json` quorum.
	s.connectDisksWithQuorum()

	// Start the disk monitoring and connect routine.
	go s.monitorAndConnectEndpoints(defaultMonitorConnectEndpointInterval)

	return s, nil
}

// StorageInfo - combines output of StorageInfo across all erasure coded object sets.
func (s *xlSets) StorageInfo(ctx context.Context) StorageInfo {
	var storageInfo StorageInfo
	storageInfo.Backend.Type = BackendErasure
	for _, set := range s.sets {
		lstorageInfo := set.StorageInfo(ctx)
		storageInfo.Used = storageInfo.Used + lstorageInfo.Used
		storageInfo.Total = storageInfo.Total + lstorageInfo.Total
		storageInfo.Available = storageInfo.Available + lstorageInfo.Available
		storageInfo.Backend.OnlineDisks = storageInfo.Backend.OnlineDisks + lstorageInfo.Backend.OnlineDisks
		storageInfo.Backend.OfflineDisks = storageInfo.Backend.OfflineDisks + lstorageInfo.Backend.OfflineDisks
	}

	scData, scParity := getRedundancyCount(standardStorageClass, s.drivesPerSet)
	storageInfo.Backend.StandardSCData = scData
	storageInfo.Backend.StandardSCParity = scParity

	rrSCData, rrSCparity := getRedundancyCount(reducedRedundancyStorageClass, s.drivesPerSet)
	storageInfo.Backend.RRSCData = rrSCData
	storageInfo.Backend.RRSCParity = rrSCparity

	storageInfo.Backend.Sets = make([][]madmin.DriveInfo, s.setCount)
	for i := range storageInfo.Backend.Sets {
		storageInfo.Backend.Sets[i] = make([]madmin.DriveInfo, s.drivesPerSet)
	}

	storageDisks, err := initStorageDisks(s.endpoints)
	if err != nil {
		return storageInfo
	}
	defer closeStorageDisks(storageDisks)

	formats, sErrs := loadFormatXLAll(storageDisks)

	drivesInfo := formatsToDrivesInfo(s.endpoints, formats, sErrs)
	refFormat, err := getFormatXLInQuorum(formats)
	if err != nil {
		// Ignore errors here, since this call cannot do anything at
		// this point. too many disks are down already.
		return storageInfo
	}

	// fill all the available/online endpoints
	for _, drive := range drivesInfo {
		if drive.UUID == "" {
			continue
		}
		for i := range refFormat.XL.Sets {
			for j, driveUUID := range refFormat.XL.Sets[i] {
				if driveUUID == drive.UUID {
					storageInfo.Backend.Sets[i][j] = drive
				}
			}
		}
	}

	// fill all the offline, missing endpoints as well.
	for _, drive := range drivesInfo {
		if drive.UUID == "" {
			for i := range storageInfo.Backend.Sets {
				for j := range storageInfo.Backend.Sets[i] {
					if storageInfo.Backend.Sets[i][j].Endpoint == drive.Endpoint {
						continue
					}
					if storageInfo.Backend.Sets[i][j].Endpoint == "" {
						storageInfo.Backend.Sets[i][j] = drive
						break
					}
				}
			}
		}
	}

	return storageInfo
}

// Shutdown shutsdown all erasure coded sets in parallel
// returns error upon first error.
func (s *xlSets) Shutdown(ctx context.Context) error {
	g := errgroup.WithNErrs(len(s.sets))

	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].Shutdown(ctx)
		}, index)
	}

	for _, err := range g.Wait() {
		if err != nil {
			return err
		}
	}

	return nil
}

// MakeBucketLocation - creates a new bucket across all sets simultaneously
// even if one of the sets fail to create buckets, we proceed to undo a
// successful operation.
func (s *xlSets) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	g := errgroup.WithNErrs(len(s.sets))

	// Create buckets in parallel across all sets.
	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].MakeBucketWithLocation(ctx, bucket, location)
		}, index)
	}

	errs := g.Wait()
	// Upon even a single write quorum error we undo all previously created buckets.
	for _, err := range errs {
		if err != nil {
			if _, ok := err.(InsufficientWriteQuorum); ok {
				undoMakeBucketSets(bucket, s.sets, errs)
			}
			return err
		}
	}

	// Success.
	return nil
}

// This function is used to undo a successful MakeBucket operation.
func undoMakeBucketSets(bucket string, sets []*xlObjects, errs []error) {
	g := errgroup.WithNErrs(len(sets))

	// Undo previous make bucket entry on all underlying sets.
	for index := range sets {
		index := index
		if errs[index] == nil {
			g.Go(func() error {
				return sets[index].DeleteBucket(context.Background(), bucket)
			}, index)
		}
	}

	// Wait for all delete bucket to finish.
	g.Wait()
}

// hashes the key returning an integer based on the input algorithm.
// This function currently supports
// - CRCMOD
// - all new algos.
func crcHashMod(key string, cardinality int) int {
	if cardinality <= 0 {
		return -1
	}
	keyCrc := crc32.Checksum([]byte(key), crc32.IEEETable)
	return int(keyCrc % uint32(cardinality))
}

func hashKey(algo string, key string, cardinality int) int {
	switch algo {
	case formatXLVersionV2DistributionAlgo:
		return crcHashMod(key, cardinality)
	default:
		// Unknown algorithm returns -1, also if cardinality is lesser than 0.
		return -1
	}
}

// Returns always a same erasure coded set for a given input.
func (s *xlSets) getHashedSet(input string) (set *xlObjects) {
	return s.sets[hashKey(s.distributionAlgo, input, len(s.sets))]
}

// GetBucketInfo - returns bucket info from one of the erasure coded set.
func (s *xlSets) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error) {
	return s.getHashedSet(bucket).GetBucketInfo(ctx, bucket)
}

// ListObjectsV2 lists all objects in bucket filtered by prefix
func (s *xlSets) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	loi, err := s.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return result, err
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

// SetBucketPolicy persist the new policy on the bucket.
func (s *xlSets) SetBucketPolicy(ctx context.Context, bucket string, policy *policy.Policy) error {
	return savePolicyConfig(ctx, s, bucket, policy)
}

// GetBucketPolicy will return a policy on a bucket
func (s *xlSets) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	return getPolicyConfig(s, bucket)
}

// DeleteBucketPolicy deletes all policies on bucket
func (s *xlSets) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return removePolicyConfig(ctx, s, bucket)
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (s *xlSets) IsNotificationSupported() bool {
	return s.getHashedSet("").IsNotificationSupported()
}

// IsListenBucketSupported returns whether listen bucket notification is applicable for this layer.
func (s *xlSets) IsListenBucketSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (s *xlSets) IsEncryptionSupported() bool {
	return s.getHashedSet("").IsEncryptionSupported()
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (s *xlSets) IsCompressionSupported() bool {
	return s.getHashedSet("").IsCompressionSupported()
}

// DeleteBucket - deletes a bucket on all sets simultaneously,
// even if one of the sets fail to delete buckets, we proceed to
// undo a successful operation.
func (s *xlSets) DeleteBucket(ctx context.Context, bucket string) error {
	g := errgroup.WithNErrs(len(s.sets))

	// Delete buckets in parallel across all sets.
	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].DeleteBucket(ctx, bucket)
		}, index)
	}

	errs := g.Wait()
	// For any write quorum failure, we undo all the delete buckets operation
	// by creating all the buckets again.
	for _, err := range errs {
		if err != nil {
			if _, ok := err.(InsufficientWriteQuorum); ok {
				undoDeleteBucketSets(bucket, s.sets, errs)
			}
			return err
		}
	}

	// Delete all bucket metadata.
	deleteBucketMetadata(ctx, bucket, s)

	// Success.
	return nil
}

// This function is used to undo a successful DeleteBucket operation.
func undoDeleteBucketSets(bucket string, sets []*xlObjects, errs []error) {
	g := errgroup.WithNErrs(len(sets))

	// Undo previous delete bucket on all underlying sets.
	for index := range sets {
		index := index
		if errs[index] == nil {
			g.Go(func() error {
				return sets[index].MakeBucketWithLocation(context.Background(), bucket, "")
			}, index)
		}
	}

	g.Wait()
}

// List all buckets from one of the set, we are not doing merge
// sort here just for simplification. As per design it is assumed
// that all buckets are present on all sets.
func (s *xlSets) ListBuckets(ctx context.Context) (buckets []BucketInfo, err error) {
	// Always lists from the same set signified by the empty string.
	return s.getHashedSet("").ListBuckets(ctx)
}

// --- Object Operations ---

// GetObjectNInfo - returns object info and locked object ReadCloser
func (s *xlSets) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	return s.getHashedSet(object).GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
}

// GetObject - reads an object from the hashedSet based on the object name.
func (s *xlSets) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) error {
	return s.getHashedSet(object).GetObject(ctx, bucket, object, startOffset, length, writer, etag, opts)
}

// PutObject - writes an object to hashedSet based on the object name.
func (s *xlSets) PutObject(ctx context.Context, bucket string, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	return s.getHashedSet(object).PutObject(ctx, bucket, object, data, opts)
}

// GetObjectInfo - reads object metadata from the hashedSet based on the object name.
func (s *xlSets) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	return s.getHashedSet(object).GetObjectInfo(ctx, bucket, object, opts)
}

// DeleteObject - deletes an object from the hashedSet based on the object name.
func (s *xlSets) DeleteObject(ctx context.Context, bucket string, object string) (err error) {
	return s.getHashedSet(object).DeleteObject(ctx, bucket, object)
}

// CopyObject - copies objects from one hashedSet to another hashedSet, on server side.
func (s *xlSets) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	srcSet := s.getHashedSet(srcObject)
	destSet := s.getHashedSet(destObject)

	// Check if this request is only metadata update.
	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(destBucket, destObject))
	if cpSrcDstSame && srcInfo.metadataOnly {
		return srcSet.CopyObject(ctx, srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, dstOpts)
	}

	if !cpSrcDstSame {
		objectDWLock := destSet.nsMutex.NewNSLock(destBucket, destObject)
		if err := objectDWLock.GetLock(globalObjectTimeout); err != nil {
			return objInfo, err
		}
		defer objectDWLock.Unlock()
	}
	putOpts := ObjectOptions{ServerSideEncryption: dstOpts.ServerSideEncryption, UserDefined: srcInfo.UserDefined}
	return destSet.putObject(ctx, destBucket, destObject, srcInfo.PutObjReader, putOpts)
}

// Returns function "listDir" of the type listDirFunc.
// isLeaf - is used by listDir function to check if an entry is a leaf or non-leaf entry.
// disks - used for doing disk.ListDir(). Sets passes set of disks.
func listDirSetsFactory(ctx context.Context, sets ...*xlObjects) ListDirFunc {
	listDirInternal := func(bucket, prefixDir, prefixEntry string, disks []StorageAPI) (mergedEntries []string) {
		var diskEntries = make([][]string, len(disks))
		var wg sync.WaitGroup
		for index, disk := range disks {
			if disk == nil {
				continue
			}
			wg.Add(1)
			go func(index int, disk StorageAPI) {
				defer wg.Done()
				diskEntries[index], _ = disk.ListDir(bucket, prefixDir, -1, xlMetaJSONFile)
			}(index, disk)
		}

		wg.Wait()

		// Find elements in entries which are not in mergedEntries
		for _, entries := range diskEntries {
			var newEntries []string

			for _, entry := range entries {
				idx := sort.SearchStrings(mergedEntries, entry)
				// if entry is already present in mergedEntries don't add.
				if idx < len(mergedEntries) && mergedEntries[idx] == entry {
					continue
				}
				newEntries = append(newEntries, entry)
			}

			if len(newEntries) > 0 {
				// Merge the entries and sort it.
				mergedEntries = append(mergedEntries, newEntries...)
				sort.Strings(mergedEntries)
			}
		}

		return mergedEntries
	}

	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (mergedEntries []string) {
		for _, set := range sets {
			var newEntries []string
			// Find elements in entries which are not in mergedEntries
			for _, entry := range listDirInternal(bucket, prefixDir, prefixEntry, set.getLoadBalancedDisks()) {
				idx := sort.SearchStrings(mergedEntries, entry)
				// if entry is already present in mergedEntries don't add.
				if idx < len(mergedEntries) && mergedEntries[idx] == entry {
					continue
				}
				newEntries = append(newEntries, entry)
			}

			if len(newEntries) > 0 {
				// Merge the entries and sort it.
				mergedEntries = append(mergedEntries, newEntries...)
				sort.Strings(mergedEntries)
			}
		}
		return filterMatchingPrefix(mergedEntries, prefixEntry)
	}
	return listDir
}

// ListObjects - implements listing of objects across sets, each set is independently
// listed and subsequently merge lexically sorted inside listDirSetsFactory(). Resulting
// value through the walk channel receives the data properly lexically sorted.
func (s *xlSets) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	listDir := listDirSetsFactory(ctx, s.sets...)

	var getObjectInfoDirs []func(context.Context, string, string) (ObjectInfo, error)
	// Verify prefixes in all sets.
	for _, set := range s.sets {
		getObjectInfoDirs = append(getObjectInfoDirs, set.getObjectInfoDir)
	}

	var getObjectInfo = func(ctx context.Context, bucket string, entry string) (ObjectInfo, error) {
		return s.getHashedSet(entry).getObjectInfo(ctx, bucket, entry)
	}

	return listObjects(ctx, s, bucket, prefix, marker, delimiter, maxKeys, s.listPool, listDir, getObjectInfo, getObjectInfoDirs...)
}

func (s *xlSets) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	// In list multipart uploads we are going to treat input prefix as the object,
	// this means that we are not supporting directory navigation.
	return s.getHashedSet(prefix).ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// Initiate a new multipart upload on a hashedSet based on object name.
func (s *xlSets) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (uploadID string, err error) {
	return s.getHashedSet(object).NewMultipartUpload(ctx, bucket, object, opts)
}

// Copies a part of an object from source hashedSet to destination hashedSet.
func (s *xlSets) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int,
	startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (partInfo PartInfo, err error) {
	destSet := s.getHashedSet(destObject)

	return destSet.PutObjectPart(ctx, destBucket, destObject, uploadID, partID, NewPutObjReader(srcInfo.Reader, nil, nil), dstOpts)
}

// PutObjectPart - writes part of an object to hashedSet based on the object name.
func (s *xlSets) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (info PartInfo, err error) {
	return s.getHashedSet(object).PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
}

// ListObjectParts - lists all uploaded parts to an object in hashedSet.
func (s *xlSets) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (result ListPartsInfo, err error) {
	return s.getHashedSet(object).ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
}

// Aborts an in-progress multipart operation on hashedSet based on the object name.
func (s *xlSets) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	return s.getHashedSet(object).AbortMultipartUpload(ctx, bucket, object, uploadID)
}

// CompleteMultipartUpload - completes a pending multipart transaction, on hashedSet based on object name.
func (s *xlSets) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	return s.getHashedSet(object).CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
}

/*

All disks online
-----------------
- All Unformatted - format all and return success.
- Some Unformatted - format all and return success.
- Any JBOD inconsistent - return failure
- Some are corrupt (missing format.json) - return failure
- Any unrecognized disks - return failure

Some disks are offline and we have quorum.
-----------------
- Some unformatted - format all and return success,
  treat disks offline as corrupted.
- Any JBOD inconsistent - return failure
- Some are corrupt (missing format.json)
- Any unrecognized disks - return failure

No read quorum
-----------------
failure for all cases.

// Pseudo code for managing `format.json`.

// Generic checks.
if (no quorum) return error
if (any disk is corrupt) return error // Always error
if (jbod inconsistent) return error // Always error.
if (disks not recognized) // Always error.

// Specific checks.
if (all disks online)
  if (all disks return format.json)
     if (jbod consistent)
        if (all disks recognized)
          return
  else
     if (all disks return format.json not found)
        return error
     else (some disks return format.json not found)
        (heal format)
        return
     fi
   fi
else
   if (some disks return format.json not found)
        // Offline disks are marked as dead.
        (heal format) // Offline disks should be marked as dead.
        return success
   fi
fi
*/

func formatsToDrivesInfo(endpoints EndpointList, formats []*formatXLV3, sErrs []error) (beforeDrives []madmin.DriveInfo) {
	// Existing formats are available (i.e. ok), so save it in
	// result, also populate disks to be healed.
	for i, format := range formats {
		drive := endpoints.GetString(i)
		switch {
		case format != nil:
			beforeDrives = append(beforeDrives, madmin.DriveInfo{
				UUID:     format.XL.This,
				Endpoint: drive,
				State:    madmin.DriveStateOk,
			})
		case sErrs[i] == errUnformattedDisk:
			beforeDrives = append(beforeDrives, madmin.DriveInfo{
				UUID:     "",
				Endpoint: drive,
				State:    madmin.DriveStateMissing,
			})
		case sErrs[i] == errCorruptedFormat:
			beforeDrives = append(beforeDrives, madmin.DriveInfo{
				UUID:     "",
				Endpoint: drive,
				State:    madmin.DriveStateCorrupt,
			})
		default:
			beforeDrives = append(beforeDrives, madmin.DriveInfo{
				UUID:     "",
				Endpoint: drive,
				State:    madmin.DriveStateOffline,
			})
		}
	}

	return beforeDrives
}

// Reloads the format from the disk, usually called by a remote peer notifier while
// healing in a distributed setup.
func (s *xlSets) ReloadFormat(ctx context.Context, dryRun bool) (err error) {
	// Acquire lock on format.json
	formatLock := s.getHashedSet(formatConfigFile).nsMutex.NewNSLock(minioMetaBucket, formatConfigFile)
	if err = formatLock.GetRLock(globalHealingTimeout); err != nil {
		return err
	}
	defer formatLock.RUnlock()

	storageDisks, err := initStorageDisks(s.endpoints)
	if err != nil {
		return err
	}
	defer func(storageDisks []StorageAPI) {
		if err != nil {
			closeStorageDisks(storageDisks)
		}
	}(storageDisks)

	formats, sErrs := loadFormatXLAll(storageDisks)
	if err = checkFormatXLValues(formats); err != nil {
		return err
	}

	for index, sErr := range sErrs {
		if sErr != nil {
			// Look for acceptable heal errors, for any other
			// errors we should simply quit and return.
			if _, ok := formatHealErrors[sErr]; !ok {
				return fmt.Errorf("Disk %s: %s", s.endpoints[index], sErr)
			}
		}
	}

	refFormat, err := getFormatXLInQuorum(formats)
	if err != nil {
		return err
	}

	// kill the monitoring loop such that we stop writing
	// to indicate that we will re-initialize everything
	// with new format.
	s.disksConnectDoneCh <- struct{}{}

	// Replace the new format.
	s.format = refFormat

	s.xlDisksMu.Lock()
	{
		// Close all existing disks.
		s.xlDisks.Close()

		// Re initialize disks, after saving the new reference format.
		s.xlDisks = s.reInitDisks(refFormat, storageDisks, formats)
	}
	s.xlDisksMu.Unlock()

	// Restart monitoring loop to monitor reformatted disks again.
	go s.monitorAndConnectEndpoints(defaultMonitorConnectEndpointInterval)

	return nil
}

// If it is a single node XL and all disks are root disks, it is most likely a test setup, else it is a production setup.
// On a test setup we allow creation of format.json on root disks to help with dev/testing.
func isTestSetup(infos []DiskInfo, errs []error) bool {
	rootDiskCount := 0
	for i := range errs {
		if errs[i] != nil {
			// On error it is safer to assume that this is not a test setup.
			return false
		}
		if infos[i].RootDisk {
			rootDiskCount++
		}
	}
	// It is a test setup if all disks are root disks.
	return rootDiskCount == len(infos)
}

func getAllDiskInfos(storageDisks []StorageAPI) ([]DiskInfo, []error) {
	infos := make([]DiskInfo, len(storageDisks))
	errs := make([]error, len(storageDisks))
	var wg sync.WaitGroup
	for i := range storageDisks {
		if storageDisks[i] == nil {
			errs[i] = errDiskNotFound
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			infos[i], errs[i] = storageDisks[i].DiskInfo()
		}(i)
	}
	wg.Wait()
	return infos, errs
}

// Mark root disks as down so as not to heal them.
func markRootDisksAsDown(storageDisks []StorageAPI) {
	infos, errs := getAllDiskInfos(storageDisks)
	if isTestSetup(infos, errs) {
		// Allow healing of disks for test setups to help with testing.
		return
	}
	for i := range storageDisks {
		if errs[i] != nil {
			storageDisks[i] = nil
			continue
		}
		if infos[i].RootDisk {
			// We should not heal on root disk. i.e in a situation where the minio-administrator has unmounted a
			// defective drive we should not heal a path on the root disk.
			storageDisks[i] = nil
		}
	}
}

// HealFormat - heals missing `format.json` on fresh unformatted disks.
// TODO: In future support corrupted disks missing format.json but has erasure
// coded data in it.
func (s *xlSets) HealFormat(ctx context.Context, dryRun bool) (res madmin.HealResultItem, err error) {
	// Acquire lock on format.json
	formatLock := s.getHashedSet(formatConfigFile).nsMutex.NewNSLock(minioMetaBucket, formatConfigFile)
	if err = formatLock.GetLock(globalHealingTimeout); err != nil {
		return madmin.HealResultItem{}, err
	}
	defer formatLock.Unlock()

	storageDisks, err := initStorageDisks(s.endpoints)
	if err != nil {
		return madmin.HealResultItem{}, err
	}

	defer func(storageDisks []StorageAPI) {
		if err != nil {
			closeStorageDisks(storageDisks)
		}
	}(storageDisks)

	markRootDisksAsDown(storageDisks)

	formats, sErrs := loadFormatXLAll(storageDisks)
	if err = checkFormatXLValues(formats); err != nil {
		return madmin.HealResultItem{}, err
	}

	// Prepare heal-result
	res = madmin.HealResultItem{
		Type:      madmin.HealItemMetadata,
		Detail:    "disk-format",
		DiskCount: s.setCount * s.drivesPerSet,
		SetCount:  s.setCount,
	}

	// Fetch all the drive info status.
	beforeDrives := formatsToDrivesInfo(s.endpoints, formats, sErrs)

	res.After.Drives = make([]madmin.HealDriveInfo, len(beforeDrives))
	res.Before.Drives = make([]madmin.HealDriveInfo, len(beforeDrives))
	// Copy "after" drive state too from before.
	for k, v := range beforeDrives {
		res.Before.Drives[k] = madmin.HealDriveInfo(v)
		res.After.Drives[k] = madmin.HealDriveInfo(v)
	}

	for index, sErr := range sErrs {
		if sErr != nil {
			// Look for acceptable heal errors, for any other
			// errors we should simply quit and return.
			if _, ok := formatHealErrors[sErr]; !ok {
				return res, fmt.Errorf("Disk %s: %s", s.endpoints[index], sErr)
			}
		}
	}

	if !hasAnyErrorsUnformatted(sErrs) {
		// No unformatted disks found disks are either offline
		// or online, no healing is required.
		return res, errNoHealRequired
	}

	// All disks are unformatted, return quorum error.
	if shouldInitXLDisks(sErrs) {
		return res, errXLReadQuorum
	}

	refFormat, err := getFormatXLInQuorum(formats)
	if err != nil {
		return res, err
	}

	// Mark all UUIDs which might be offline, use list
	// of formats to mark them appropriately.
	markUUIDsOffline(refFormat, formats)

	// Initialize a new set of set formats which will be written to disk.
	newFormatSets := newHealFormatSets(refFormat, s.setCount, s.drivesPerSet, formats, sErrs)

	// Look for all offline/unformatted disks in our reference format,
	// such that we can fill them up with new UUIDs, this looping also
	// ensures that the replaced disks allocated evenly across all sets.
	// Making sure that the redundancy is not lost.
	for i := range refFormat.XL.Sets {
		for j := range refFormat.XL.Sets[i] {
			if refFormat.XL.Sets[i][j] == offlineDiskUUID {
				for l := range newFormatSets[i] {
					if newFormatSets[i][l] == nil {
						continue
					}
					if newFormatSets[i][l].XL.This == "" {
						newFormatSets[i][l].XL.This = mustGetUUID()
						refFormat.XL.Sets[i][j] = newFormatSets[i][l].XL.This
						for m, v := range res.After.Drives {
							if v.Endpoint == s.endpoints.GetString(i*s.drivesPerSet+l) {
								res.After.Drives[m].UUID = newFormatSets[i][l].XL.This
								res.After.Drives[m].State = madmin.DriveStateOk
							}
						}
						break
					}
				}
			}
		}
	}

	if !dryRun {
		var tmpNewFormats = make([]*formatXLV3, s.setCount*s.drivesPerSet)
		for i := range newFormatSets {
			for j := range newFormatSets[i] {
				if newFormatSets[i][j] == nil {
					continue
				}
				tmpNewFormats[i*s.drivesPerSet+j] = newFormatSets[i][j]
				tmpNewFormats[i*s.drivesPerSet+j].XL.Sets = refFormat.XL.Sets
			}
		}

		// Initialize meta volume, if volume already exists ignores it, all disks which
		// are not found are ignored as well.
		if err = initFormatXLMetaVolume(storageDisks, tmpNewFormats); err != nil {
			return madmin.HealResultItem{}, fmt.Errorf("Unable to initialize '.minio.sys' meta volume, %s", err)
		}

		// Save formats `format.json` across all disks.
		if err = saveFormatXLAll(ctx, storageDisks, tmpNewFormats); err != nil {
			return madmin.HealResultItem{}, err
		}

		// kill the monitoring loop such that we stop writing
		// to indicate that we will re-initialize everything
		// with new format.
		s.disksConnectDoneCh <- struct{}{}

		// Replace with new reference format.
		s.format = refFormat

		s.xlDisksMu.Lock()
		{
			// Disconnect/relinquish all existing disks.
			s.xlDisks.Close()

			// Re initialize disks, after saving the new reference format.
			s.xlDisks = s.reInitDisks(refFormat, storageDisks, tmpNewFormats)
		}
		s.xlDisksMu.Unlock()

		// Restart our monitoring loop to start monitoring newly formatted disks.
		go s.monitorAndConnectEndpoints(defaultMonitorConnectEndpointInterval)
	}

	return res, nil
}

// HealBucket - heals inconsistent buckets and bucket metadata on all sets.
func (s *xlSets) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (result madmin.HealResultItem, err error) {
	bucketLock := globalNSMutex.NewNSLock(bucket, "")
	if err := bucketLock.GetLock(globalHealingTimeout); err != nil {
		return result, err
	}
	defer bucketLock.Unlock()

	// Initialize heal result info
	result = madmin.HealResultItem{
		Type:      madmin.HealItemBucket,
		Bucket:    bucket,
		DiskCount: s.setCount * s.drivesPerSet,
		SetCount:  s.setCount,
	}

	for _, s := range s.sets {
		var healResult madmin.HealResultItem
		healResult, err = s.HealBucket(ctx, bucket, dryRun, remove)
		if err != nil {
			return result, err
		}
		result.Before.Drives = append(result.Before.Drives, healResult.Before.Drives...)
		result.After.Drives = append(result.After.Drives, healResult.After.Drives...)
	}

	for _, endpoint := range s.endpoints {
		var foundBefore bool
		for _, v := range result.Before.Drives {
			if endpoint.IsLocal {
				if v.Endpoint == endpoint.Path {
					foundBefore = true
				}
			} else {
				if v.Endpoint == endpoint.String() {
					foundBefore = true
				}
			}
		}
		if !foundBefore {
			result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: endpoint.String(),
				State:    madmin.DriveStateOffline,
			})
		}
		var foundAfter bool
		for _, v := range result.After.Drives {
			if endpoint.IsLocal {
				if v.Endpoint == endpoint.Path {
					foundAfter = true
				}
			} else {
				if v.Endpoint == endpoint.String() {
					foundAfter = true
				}
			}
		}
		if !foundAfter {
			result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: endpoint.String(),
				State:    madmin.DriveStateOffline,
			})
		}
	}

	// Check if we had quorum to write, if not return an appropriate error.
	_, afterDriveOnline := result.GetOnlineCounts()
	if afterDriveOnline < ((s.setCount*s.drivesPerSet)/2)+1 {
		return result, toObjectErr(errXLWriteQuorum, bucket)
	}

	return result, nil
}

// HealObject - heals inconsistent object on a hashedSet based on object name.
func (s *xlSets) HealObject(ctx context.Context, bucket, object string, dryRun, remove bool, scanMode madmin.HealScanMode) (madmin.HealResultItem, error) {
	return s.getHashedSet(object).HealObject(ctx, bucket, object, dryRun, remove, scanMode)
}

// Lists all buckets which need healing.
func (s *xlSets) ListBucketsHeal(ctx context.Context) ([]BucketInfo, error) {
	listBuckets := []BucketInfo{}
	var healBuckets = map[string]BucketInfo{}
	for _, set := range s.sets {
		buckets, _, err := listAllBuckets(set.getDisks())
		if err != nil {
			return nil, err
		}
		for _, currBucket := range buckets {
			healBuckets[currBucket.Name] = BucketInfo(currBucket)
		}
	}
	for _, bucketInfo := range healBuckets {
		listBuckets = append(listBuckets, bucketInfo)
	}
	return listBuckets, nil
}

// HealObjects - Heal all objects recursively at a specified prefix, any
// dangling objects deleted as well automatically.
func (s *xlSets) HealObjects(ctx context.Context, bucket, prefix string, healObjectFn func(string, string) error) (err error) {
	recursive := true

	endWalkCh := make(chan struct{})
	listDir := listDirSetsFactory(ctx, s.sets...)
	walkResultCh := startTreeWalk(ctx, bucket, prefix, "", recursive, listDir, endWalkCh)
	for {
		walkResult, ok := <-walkResultCh
		if !ok {
			break
		}
		if err := healObjectFn(bucket, strings.TrimSuffix(walkResult.entry, slashSeparator+xlMetaJSONFile)); err != nil {
			return toObjectErr(err, bucket, walkResult.entry)
		}
		if walkResult.end {
			break
		}
	}

	return nil
}
