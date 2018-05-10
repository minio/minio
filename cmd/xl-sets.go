/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bpool"
	"github.com/minio/minio/pkg/hash"
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
	listPool *treeWalkPool
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
		case <-globalServiceDoneCh:
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
		listPool:           newTreeWalkPool(globalLookupTimeout),
	}

	mutex := newNSLock(globalIsDistXL)
	for i := 0; i < len(format.XL.Sets); i++ {
		s.xlDisks[i] = make([]StorageAPI, drivesPerSet)

		// Initialize xl objects for a given set.
		s.sets[i] = &xlObjects{
			getDisks: s.GetDisks(i),
			nsMutex:  mutex,
			bp:       bpool.NewBytePoolCap(setCount*drivesPerSet, blockSizeV1, blockSizeV1*2),
		}
		go s.sets[i].cleanupStaleMultipartUploads(context.Background(), globalMultipartCleanupInterval, globalMultipartExpiry, globalServiceDoneCh)
	}

	// Connect disks right away.
	s.connectDisks()

	// Initialize notification system.
	if err := globalNotificationSys.Init(s); err != nil {
		return nil, fmt.Errorf("Unable to initialize notification system. %v", err)
	}

	// Initialize policy system.
	if err := globalPolicySys.Init(s); err != nil {
		return nil, fmt.Errorf("Unable to initialize policy system. %v", err)
	}

	// Start the disk monitoring and connect routine.
	go s.monitorAndConnectEndpoints(defaultMonitorConnectEndpointInterval)

	return s, nil
}

// StorageInfo - combines output of StorageInfo across all erasure coded object sets.
func (s *xlSets) StorageInfo(ctx context.Context) StorageInfo {
	var storageInfo StorageInfo
	storageInfo.Backend.Type = Erasure
	for _, set := range s.sets {
		lstorageInfo := set.StorageInfo(ctx)
		storageInfo.Total = storageInfo.Total + lstorageInfo.Total
		storageInfo.Free = storageInfo.Free + lstorageInfo.Free
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
	}
	// Unknown algorithm returns -1, also if cardinality is lesser than 0.
	return -1
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
	loi, err := s.ListObjects(ctx, bucket, prefix, continuationToken, delimiter, maxKeys)
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
	return savePolicyConfig(s, bucket, policy)
}

// GetBucketPolicy will return a policy on a bucket
func (s *xlSets) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	return GetPolicyConfig(s, bucket)
}

// DeleteBucketPolicy deletes all policies on bucket
func (s *xlSets) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return removePolicyConfig(ctx, s, bucket)
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (s *xlSets) IsNotificationSupported() bool {
	return s.getHashedSet("").IsNotificationSupported()
}

// IsEncryptionSupported returns whether server side encryption is applicable for this layer.
func (s *xlSets) IsEncryptionSupported() bool {
	return s.getHashedSet("").IsEncryptionSupported()
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

// GetObject - reads an object from the hashedSet based on the object name.
func (s *xlSets) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) error {
	return s.getHashedSet(object).GetObject(ctx, bucket, object, startOffset, length, writer, etag)
}

// PutObject - writes an object to hashedSet based on the object name.
func (s *xlSets) PutObject(ctx context.Context, bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	return s.getHashedSet(object).PutObject(ctx, bucket, object, data, metadata)
}

// GetObjectInfo - reads object metadata from the hashedSet based on the object name.
func (s *xlSets) GetObjectInfo(ctx context.Context, bucket, object string) (objInfo ObjectInfo, err error) {
	return s.getHashedSet(object).GetObjectInfo(ctx, bucket, object)
}

// DeleteObject - deletes an object from the hashedSet based on the object name.
func (s *xlSets) DeleteObject(ctx context.Context, bucket string, object string) (err error) {
	return s.getHashedSet(object).DeleteObject(ctx, bucket, object)
}

// CopyObject - copies objects from one hashedSet to another hashedSet, on server side.
func (s *xlSets) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo) (objInfo ObjectInfo, err error) {
	srcSet := s.getHashedSet(srcObject)
	destSet := s.getHashedSet(destObject)

	// Check if this request is only metadata update.
	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(destBucket, destObject))
	if cpSrcDstSame && srcInfo.metadataOnly {
		return srcSet.CopyObject(ctx, srcBucket, srcObject, destBucket, destObject, srcInfo)
	}

	// Hold write lock on destination since in both cases
	// - if source and destination are same
	// - if source and destination are different
	// it is the sole mutating state.
	objectDWLock := destSet.nsMutex.NewNSLock(destBucket, destObject)
	if err := objectDWLock.GetLock(globalObjectTimeout); err != nil {
		return objInfo, err
	}
	defer objectDWLock.Unlock()
	// if source and destination are different, we have to hold
	// additional read lock as well to protect against writes on
	// source.
	if !cpSrcDstSame {
		// Hold read locks on source object only if we are
		// going to read data from source object.
		objectSRLock := srcSet.nsMutex.NewNSLock(srcBucket, srcObject)
		if err := objectSRLock.GetRLock(globalObjectTimeout); err != nil {
			return objInfo, err
		}
		defer objectSRLock.RUnlock()
	}

	go func() {
		if gerr := srcSet.getObject(ctx, srcBucket, srcObject, 0, srcInfo.Size, srcInfo.Writer, srcInfo.ETag); gerr != nil {
			if gerr = srcInfo.Writer.Close(); gerr != nil {
				logger.LogIf(ctx, gerr)
			}
			return
		}
		// Close writer explicitly signalling we wrote all data.
		if gerr := srcInfo.Writer.Close(); gerr != nil {
			logger.LogIf(ctx, gerr)
			return
		}
	}()

	return destSet.putObject(ctx, destBucket, destObject, srcInfo.Reader, srcInfo.UserDefined)
}

// Returns function "listDir" of the type listDirFunc.
// isLeaf - is used by listDir function to check if an entry is a leaf or non-leaf entry.
// disks - used for doing disk.ListDir(). Sets passes set of disks.
func listDirSetsFactory(ctx context.Context, isLeaf isLeafFunc, isLeafDir isLeafDirFunc, treeWalkIgnoredErrs []error, sets ...[]StorageAPI) listDirFunc {
	listDirInternal := func(bucket, prefixDir, prefixEntry string, disks []StorageAPI) (mergedEntries []string, err error) {
		for _, disk := range disks {
			if disk == nil {
				continue
			}

			var entries []string
			var newEntries []string
			entries, err = disk.ListDir(bucket, prefixDir, -1)
			if err != nil {
				// For any reason disk was deleted or goes offline, continue
				// and list from other disks if possible.
				if IsErrIgnored(err, treeWalkIgnoredErrs...) {
					continue
				}
				logger.LogIf(ctx, err)
				return nil, err
			}

			// Find elements in entries which are not in mergedEntries
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
		return mergedEntries, nil
	}

	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (mergedEntries []string, delayIsLeaf bool, err error) {
		for _, disks := range sets {
			var entries []string
			entries, err = listDirInternal(bucket, prefixDir, prefixEntry, disks)
			if err != nil {
				return nil, false, err
			}

			var newEntries []string
			// Find elements in entries which are not in mergedEntries
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
		mergedEntries, delayIsLeaf = filterListEntries(bucket, prefixDir, mergedEntries, prefixEntry, isLeaf)
		return mergedEntries, delayIsLeaf, nil
	}
	return listDir
}

// ListObjects - implements listing of objects across sets, each set is independently
// listed and subsequently merge lexically sorted inside listDirSetsFactory(). Resulting
// value through the walk channel receives the data properly lexically sorted.
func (s *xlSets) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	var result ListObjectsInfo
	// validate all the inputs for listObjects
	if err := checkListObjsArgs(ctx, bucket, prefix, marker, delimiter, s); err != nil {
		return result, err
	}

	var objInfos []ObjectInfo
	var eof bool
	var nextMarker string

	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	walkResultCh, endWalkCh := s.listPool.Release(listParams{bucket, recursive, marker, prefix, false})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		isLeaf := func(bucket, entry string) bool {
			entry = strings.TrimSuffix(entry, slashSeparator)
			// Verify if we are at the leaf, a leaf is where we
			// see `xl.json` inside a directory.
			return s.getHashedSet(entry).isObject(bucket, entry)
		}

		isLeafDir := func(bucket, entry string) bool {
			return s.getHashedSet(entry).isObjectDir(bucket, entry)
		}

		var setDisks = make([][]StorageAPI, len(s.sets))
		for _, set := range s.sets {
			setDisks = append(setDisks, set.getLoadBalancedDisks())
		}

		listDir := listDirSetsFactory(ctx, isLeaf, isLeafDir, xlTreeWalkIgnoredErrs, setDisks...)
		walkResultCh = startTreeWalk(ctx, bucket, prefix, marker, recursive, listDir, isLeaf, isLeafDir, endWalkCh)
	}

	for i := 0; i < maxKeys; {
		walkResult, ok := <-walkResultCh
		if !ok {
			// Closed channel.
			eof = true
			break
		}

		// For any walk error return right away.
		if walkResult.err != nil {
			return result, toObjectErr(walkResult.err, bucket, prefix)
		}

		var objInfo ObjectInfo
		var err error
		if hasSuffix(walkResult.entry, slashSeparator) {
			objInfo, err = s.getHashedSet(walkResult.entry).getObjectInfoDir(ctx, bucket, walkResult.entry)
		} else {
			objInfo, err = s.getHashedSet(walkResult.entry).getObjectInfo(ctx, bucket, walkResult.entry)
		}
		if err != nil {
			// Ignore errFileNotFound as the object might have got
			// deleted in the interim period of listing and getObjectInfo(),
			// ignore quorum error as it might be an entry from an outdated disk.
			if IsErrIgnored(err, []error{
				errFileNotFound,
				errXLReadQuorum,
			}...) {
				continue
			}
			return result, toObjectErr(err, bucket, prefix)
		}
		nextMarker = objInfo.Name
		objInfos = append(objInfos, objInfo)
		i++
		if walkResult.end {
			eof = true
			break
		}
	}

	params := listParams{bucket, recursive, nextMarker, prefix, false}
	if !eof {
		s.listPool.Set(params, walkResultCh, endWalkCh)
	}

	result = ListObjectsInfo{IsTruncated: !eof}
	for _, objInfo := range objInfos {
		result.NextMarker = objInfo.Name
		if objInfo.IsDir && delimiter == slashSeparator {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}
	return result, nil
}

func (s *xlSets) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	// In list multipart uploads we are going to treat input prefix as the object,
	// this means that we are not supporting directory navigation.
	return s.getHashedSet(prefix).ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// Initiate a new multipart upload on a hashedSet based on object name.
func (s *xlSets) NewMultipartUpload(ctx context.Context, bucket, object string, metadata map[string]string) (uploadID string, err error) {
	return s.getHashedSet(object).NewMultipartUpload(ctx, bucket, object, metadata)
}

// Copies a part of an object from source hashedSet to destination hashedSet.
func (s *xlSets) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int,
	startOffset int64, length int64, srcInfo ObjectInfo) (partInfo PartInfo, err error) {

	srcSet := s.getHashedSet(srcObject)
	destSet := s.getHashedSet(destObject)

	go func() {
		if gerr := srcSet.GetObject(ctx, srcBucket, srcObject, startOffset, length, srcInfo.Writer, srcInfo.ETag); gerr != nil {
			if gerr = srcInfo.Writer.Close(); gerr != nil {
				logger.LogIf(ctx, gerr)
				return
			}
		}
		if gerr := srcInfo.Writer.Close(); gerr != nil {
			logger.LogIf(ctx, gerr)
			return
		}
	}()

	return destSet.PutObjectPart(ctx, destBucket, destObject, uploadID, partID, srcInfo.Reader)
}

// PutObjectPart - writes part of an object to hashedSet based on the object name.
func (s *xlSets) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *hash.Reader) (info PartInfo, err error) {
	return s.getHashedSet(object).PutObjectPart(ctx, bucket, object, uploadID, partID, data)
}

// ListObjectParts - lists all uploaded parts to an object in hashedSet.
func (s *xlSets) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error) {
	return s.getHashedSet(object).ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts)
}

// Aborts an in-progress multipart operation on hashedSet based on the object name.
func (s *xlSets) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	return s.getHashedSet(object).AbortMultipartUpload(ctx, bucket, object, uploadID)
}

// CompleteMultipartUpload - completes a pending multipart transaction, on hashedSet based on object name.
func (s *xlSets) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart) (objInfo ObjectInfo, err error) {
	return s.getHashedSet(object).CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts)
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
		res.Before.Drives[k] = madmin.HealDriveInfo{
			UUID:     v.UUID,
			Endpoint: v.Endpoint,
			State:    v.State,
		}
		res.After.Drives[k] = madmin.HealDriveInfo{
			UUID:     v.UUID,
			Endpoint: v.Endpoint,
			State:    v.State,
		}
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
func (s *xlSets) HealBucket(ctx context.Context, bucket string, dryRun bool) (results []madmin.HealResultItem, err error) {
	bucketLock := globalNSMutex.NewNSLock(bucket, "")
	if err := bucketLock.GetLock(globalHealingTimeout); err != nil {
		return nil, err
	}
	defer bucketLock.Unlock()

	// Initialize heal result info
	res := madmin.HealResultItem{
		Type:      madmin.HealItemBucket,
		Bucket:    bucket,
		DiskCount: s.setCount * s.drivesPerSet,
		SetCount:  s.setCount,
	}

	for _, s := range s.sets {
		var setResults []madmin.HealResultItem
		setResults, _ = s.HealBucket(ctx, bucket, dryRun)
		for _, setResult := range setResults {
			if setResult.Type == madmin.HealItemBucket {
				for _, v := range setResult.Before.Drives {
					res.Before.Drives = append(res.Before.Drives, v)
				}
				for _, v := range setResult.After.Drives {
					res.After.Drives = append(res.After.Drives, v)
				}
				continue
			}
			results = append(results, setResult)
		}
	}

	for _, endpoint := range s.endpoints {
		var foundBefore bool
		for _, v := range res.Before.Drives {
			if v.Endpoint == endpoint.String() {
				foundBefore = true
			}
		}
		if !foundBefore {
			res.Before.Drives = append(res.Before.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: endpoint.String(),
				State:    madmin.DriveStateOffline,
			})
		}
		var foundAfter bool
		for _, v := range res.After.Drives {
			if v.Endpoint == endpoint.String() {
				foundAfter = true
			}
		}
		if !foundAfter {
			res.After.Drives = append(res.After.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: endpoint.String(),
				State:    madmin.DriveStateOffline,
			})
		}
	}

	// Check if we had quorum to write, if not return an appropriate error.
	_, afterDriveOnline := res.GetOnlineCounts()
	if afterDriveOnline < ((s.setCount*s.drivesPerSet)/2)+1 {
		return nil, toObjectErr(errXLWriteQuorum, bucket)
	}

	results = append(results, res)

	return results, nil
}

// HealObject - heals inconsistent object on a hashedSet based on object name.
func (s *xlSets) HealObject(ctx context.Context, bucket, object string, dryRun bool) (madmin.HealResultItem, error) {
	return s.getHashedSet(object).HealObject(ctx, bucket, object, dryRun)
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
			healBuckets[currBucket.Name] = BucketInfo{
				Name:    currBucket.Name,
				Created: currBucket.Created,
			}
		}
	}
	for _, bucketInfo := range healBuckets {
		listBuckets = append(listBuckets, bucketInfo)
	}
	return listBuckets, nil
}

// Returns function "listDir" of the type listDirFunc.
// isLeaf - is used by listDir function to check if an entry is a leaf or non-leaf entry.
// disks - used for doing disk.ListDir(). Sets passes set of disks.
func listDirSetsHealFactory(isLeaf isLeafFunc, sets ...[]StorageAPI) listDirFunc {
	listDirInternal := func(bucket, prefixDir, prefixEntry string, disks []StorageAPI) (mergedEntries []string, err error) {
		for _, disk := range disks {
			if disk == nil {
				continue
			}
			var entries []string
			var newEntries []string
			entries, err = disk.ListDir(bucket, prefixDir, -1)
			if err != nil {
				continue
			}

			// Filter entries that have the prefix prefixEntry.
			entries = filterMatchingPrefix(entries, prefixEntry)

			// isLeaf() check has to happen here so that
			// trailing "/" for objects can be removed.
			for i, entry := range entries {
				if isLeaf(bucket, pathJoin(prefixDir, entry)) {
					entries[i] = strings.TrimSuffix(entry, slashSeparator)
				}
			}

			// Find elements in entries which are not in mergedEntries
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
		return mergedEntries, nil

	}

	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (mergedEntries []string, delayIsLeaf bool, err error) {
		for _, disks := range sets {
			var entries []string
			entries, err = listDirInternal(bucket, prefixDir, prefixEntry, disks)
			if err != nil {
				return nil, false, err
			}

			var newEntries []string
			// Find elements in entries which are not in mergedEntries
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
		return mergedEntries, false, nil
	}
	return listDir
}

// listObjectsHeal - wrapper function implemented over file tree walk.
func (s *xlSets) listObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	// "heal" true for listObjectsHeal() and false for listObjects()
	walkResultCh, endWalkCh := s.listPool.Release(listParams{bucket, recursive, marker, prefix, true})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		isLeaf := func(bucket, entry string) bool {
			entry = strings.TrimSuffix(entry, slashSeparator)
			// Verify if we are at the leaf, a leaf is where we
			// see `xl.json` inside a directory.
			return s.getHashedSet(entry).isObject(bucket, entry)
		}

		isLeafDir := func(bucket, entry string) bool {
			return s.getHashedSet(entry).isObjectDir(bucket, entry)
		}

		var setDisks = make([][]StorageAPI, len(s.sets))
		for _, set := range s.sets {
			setDisks = append(setDisks, set.getLoadBalancedDisks())
		}

		listDir := listDirSetsHealFactory(isLeaf, setDisks...)
		walkResultCh = startTreeWalk(ctx, bucket, prefix, marker, recursive, listDir, nil, isLeafDir, endWalkCh)
	}

	var objInfos []ObjectInfo
	var eof bool
	var nextMarker string
	for i := 0; i < maxKeys; {
		walkResult, ok := <-walkResultCh
		if !ok {
			// Closed channel.
			eof = true
			break
		}
		// For any walk error return right away.
		if walkResult.err != nil {
			return loi, toObjectErr(walkResult.err, bucket, prefix)
		}
		var objInfo ObjectInfo
		objInfo.Bucket = bucket
		objInfo.Name = walkResult.entry
		nextMarker = objInfo.Name
		objInfos = append(objInfos, objInfo)
		i++
		if walkResult.end {
			eof = true
			break
		}
	}

	params := listParams{bucket, recursive, nextMarker, prefix, true}
	if !eof {
		s.listPool.Set(params, walkResultCh, endWalkCh)
	}

	result := ListObjectsInfo{IsTruncated: !eof}
	for _, objInfo := range objInfos {
		result.NextMarker = objInfo.Name
		result.Objects = append(result.Objects, objInfo)
	}
	return result, nil
}

// This is not implemented yet, will be implemented later to comply with Admin API refactor.
func (s *xlSets) ListObjectsHeal(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	if err = checkListObjsArgs(ctx, bucket, prefix, marker, delimiter, s); err != nil {
		return loi, err
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return loi, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter' along
	// with the prefix. On a flat namespace with 'prefix' as '/'
	// we don't have any entries, since all the keys are of form 'keyName/...'
	if delimiter == slashSeparator && prefix == slashSeparator {
		return loi, nil
	}

	// Over flowing count - reset to maxObjectList.
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}

	// Initiate a list operation, if successful filter and return quickly.
	listObjInfo, err := s.listObjectsHeal(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err == nil {
		// We got the entries successfully return.
		return listObjInfo, nil
	}

	// Return error at the end.
	return loi, toObjectErr(err, bucket, prefix)
}

// ListLocks from all sets, aggregate them and return.
func (s *xlSets) ListLocks(ctx context.Context, bucket, prefix string, duration time.Duration) (lockInfo []VolumeLockInfo, err error) {
	for _, set := range s.sets {
		var setLockInfo []VolumeLockInfo
		setLockInfo, err = set.ListLocks(ctx, bucket, prefix, duration)
		if err != nil {
			return nil, err
		}
		lockInfo = append(lockInfo, setLockInfo...)
	}
	return lockInfo, nil
}

// Clear all requested locks on all sets.
func (s *xlSets) ClearLocks(ctx context.Context, lockInfo []VolumeLockInfo) error {
	for _, set := range s.sets {
		set.ClearLocks(ctx, lockInfo)
	}
	return nil
}
