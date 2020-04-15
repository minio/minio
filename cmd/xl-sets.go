/*
 * MinIO Cloud Storage, (C) 2018-2019 MinIO, Inc.
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
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/config/storageclass"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bpool"
	bucketsse "github.com/minio/minio/pkg/bucket/encryption"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/bucket/object/tagging"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/dsync"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// setsStorageAPI is encapsulated type for Close()
type setsStorageAPI [][]StorageAPI

// setsDsyncLockers is encapsulated type for Close()
type setsDsyncLockers [][]dsync.NetLocker

func (s setsStorageAPI) Copy() [][]StorageAPI {
	copyS := make(setsStorageAPI, len(s))
	for i, disks := range s {
		copyS[i] = append(copyS[i], disks...)
	}
	return copyS
}

// Information of a new disk connection
type diskConnectInfo struct {
	setIndex int
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

	// Distributed locker clients.
	xlLockers setsDsyncLockers

	// List of endpoints provided on the command line.
	endpoints Endpoints

	// String version of all the endpoints, an optimization
	// to avoid url.String() conversion taking CPU on
	// large disk setups.
	endpointStrings []string

	// Total number of sets and the number of disks per set.
	setCount, drivesPerSet int

	disksConnectEvent chan diskConnectInfo

	// Done channel to control monitoring loop.
	disksConnectDoneCh chan struct{}

	// Distribution algorithm of choice.
	distributionAlgo string

	// Merge tree walk
	pool       *MergeWalkPool
	poolSplunk *MergeWalkPool

	mrfMU      sync.Mutex
	mrfUploads map[string]int
}

func isEndpointConnected(diskMap map[string]StorageAPI, endpoint string) bool {
	disk := diskMap[endpoint]
	if disk == nil {
		return false
	}
	return disk.IsOnline()
}

func (s *xlSets) getOnlineDisksCount() int {
	s.xlDisksMu.RLock()
	defer s.xlDisksMu.RUnlock()
	count := 0
	for i := 0; i < s.setCount; i++ {
		for j := 0; j < s.drivesPerSet; j++ {
			disk := s.xlDisks[i][j]
			if disk == nil {
				continue
			}
			if !disk.IsOnline() {
				continue
			}
			count++
		}
	}
	return count
}

func (s *xlSets) getDiskMap() map[string]StorageAPI {
	diskMap := make(map[string]StorageAPI)

	s.xlDisksMu.RLock()
	defer s.xlDisksMu.RUnlock()

	for i := 0; i < s.setCount; i++ {
		for j := 0; j < s.drivesPerSet; j++ {
			disk := s.xlDisks[i][j]
			if disk == nil {
				continue
			}
			if !disk.IsOnline() {
				continue
			}
			diskMap[disk.String()] = disk
		}
	}
	return diskMap
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

// findDiskIndex - returns the i,j'th position of the input `diskID` against the reference
// format, after successful validation.
//   - i'th position is the set index
//   - j'th position is the disk index in the current set
func findDiskIndexByDiskID(refFormat *formatXLV3, diskID string) (int, int, error) {
	if diskID == offlineDiskUUID {
		return -1, -1, fmt.Errorf("diskID: %s is offline", diskID)
	}
	for i := 0; i < len(refFormat.XL.Sets); i++ {
		for j := 0; j < len(refFormat.XL.Sets[0]); j++ {
			if refFormat.XL.Sets[i][j] == diskID {
				return i, j, nil
			}
		}
	}

	return -1, -1, fmt.Errorf("diskID: %s not found", diskID)
}

// findDiskIndex - returns the i,j'th position of the input `format` against the reference
// format, after successful validation.
//   - i'th position is the set index
//   - j'th position is the disk index in the current set
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

// connectDisks - attempt to connect all the endpoints, loads format
// and re-arranges the disks in proper position.
func (s *xlSets) connectDisks() {
	var wg sync.WaitGroup
	diskMap := s.getDiskMap()
	for i, endpoint := range s.endpoints {
		if isEndpointConnected(diskMap, s.endpointStrings[i]) {
			continue
		}
		wg.Add(1)
		go func(endpoint Endpoint) {
			defer wg.Done()
			disk, format, err := connectEndpoint(endpoint)
			if err != nil {
				printEndpointError(endpoint, err)
				return
			}
			setIndex, diskIndex, err := findDiskIndex(s.format, format)
			if err != nil {
				// Close the internal connection to avoid connection leaks.
				disk.Close()
				printEndpointError(endpoint, err)
				return
			}
			disk.SetDiskID(format.XL.This)
			s.xlDisksMu.Lock()
			if s.xlDisks[setIndex][diskIndex] != nil {
				s.xlDisks[setIndex][diskIndex].Close()
			}
			s.xlDisks[setIndex][diskIndex] = disk
			s.xlDisksMu.Unlock()
			go func(setIndex int) {
				// Send a new disk connect event with a timeout
				select {
				case s.disksConnectEvent <- diskConnectInfo{setIndex: setIndex}:
				case <-time.After(100 * time.Millisecond):
				}
			}(setIndex)
		}(endpoint)
	}
	wg.Wait()
}

// monitorAndConnectEndpoints this is a monitoring loop to keep track of disconnected
// endpoints by reconnecting them and making sure to place them into right position in
// the set topology, this monitoring happens at a given monitoring interval.
func (s *xlSets) monitorAndConnectEndpoints(ctx context.Context, monitorInterval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.disksConnectDoneCh:
			return
		case <-time.After(monitorInterval):
			s.connectDisks()
		}
	}
}

func (s *xlSets) GetLockers(setIndex int) func() []dsync.NetLocker {
	return func() []dsync.NetLocker {
		lockers := make([]dsync.NetLocker, s.drivesPerSet)
		copy(lockers, s.xlLockers[setIndex])
		return lockers
	}
}

// GetDisks returns a closure for a given set, which provides list of disks per set.
func (s *xlSets) GetDisks(setIndex int) func() []StorageAPI {
	return func() []StorageAPI {
		s.xlDisksMu.RLock()
		defer s.xlDisksMu.RUnlock()
		disks := make([]StorageAPI, s.drivesPerSet)
		copy(disks, s.xlDisks[setIndex])
		return disks
	}
}

const defaultMonitorConnectEndpointInterval = time.Second * 10 // Set to 10 secs.

// Initialize new set of erasure coded sets.
func newXLSets(ctx context.Context, endpoints Endpoints, storageDisks []StorageAPI, format *formatXLV3, setCount int, drivesPerSet int) (*xlSets, error) {
	endpointStrings := make([]string, len(endpoints))
	for i, endpoint := range endpoints {
		if endpoint.IsLocal {
			endpointStrings[i] = endpoint.Path
		} else {
			endpointStrings[i] = endpoint.String()
		}
	}

	// Initialize the XL sets instance.
	s := &xlSets{
		sets:               make([]*xlObjects, setCount),
		xlDisks:            make([][]StorageAPI, setCount),
		xlLockers:          make([][]dsync.NetLocker, setCount),
		endpoints:          endpoints,
		endpointStrings:    endpointStrings,
		setCount:           setCount,
		drivesPerSet:       drivesPerSet,
		format:             format,
		disksConnectEvent:  make(chan diskConnectInfo),
		disksConnectDoneCh: make(chan struct{}),
		distributionAlgo:   format.XL.DistributionAlgo,
		pool:               NewMergeWalkPool(globalMergeLookupTimeout),
		poolSplunk:         NewMergeWalkPool(globalMergeLookupTimeout),
		mrfUploads:         make(map[string]int),
	}

	mutex := newNSLock(globalIsDistXL)

	// Initialize byte pool once for all sets, bpool size is set to
	// setCount * drivesPerSet with each memory upto blockSizeV1.
	bp := bpool.NewBytePoolCap(setCount*drivesPerSet, blockSizeV1, blockSizeV1*2)

	for i := 0; i < setCount; i++ {
		s.xlDisks[i] = make([]StorageAPI, drivesPerSet)
		s.xlLockers[i] = make([]dsync.NetLocker, drivesPerSet)

		var endpoints Endpoints
		for j := 0; j < drivesPerSet; j++ {
			endpoints = append(endpoints, s.endpoints[i*drivesPerSet+j])
			// Rely on endpoints list to initialize, init lockers and available disks.
			s.xlLockers[i][j] = newLockAPI(s.endpoints[i*drivesPerSet+j])
			disk := storageDisks[i*drivesPerSet+j]
			if disk == nil {
				continue
			}
			diskID, derr := disk.GetDiskID()
			if derr != nil {
				disk.Close()
				continue
			}
			m, n, err := findDiskIndexByDiskID(format, diskID)
			if err != nil {
				disk.Close()
				continue
			}
			s.xlDisks[m][n] = disk
		}

		// Initialize xl objects for a given set.
		s.sets[i] = &xlObjects{
			getDisks:    s.GetDisks(i),
			getLockers:  s.GetLockers(i),
			endpoints:   endpoints,
			nsMutex:     mutex,
			bp:          bp,
			mrfUploadCh: make(chan partialUpload, 10000),
		}

		go s.sets[i].cleanupStaleMultipartUploads(ctx,
			GlobalMultipartCleanupInterval, GlobalMultipartExpiry, ctx.Done())
	}

	// Start the disk monitoring and connect routine.
	go s.monitorAndConnectEndpoints(ctx, defaultMonitorConnectEndpointInterval)
	go s.maintainMRFList()
	go s.healMRFRoutine()

	return s, nil
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (s *xlSets) NewNSLock(ctx context.Context, bucket string, objects ...string) RWLocker {
	if len(objects) == 1 {
		return s.getHashedSet(objects[0]).NewNSLock(ctx, bucket, objects...)
	}
	return s.getHashedSet("").NewNSLock(ctx, bucket, objects...)
}

// StorageInfo - combines output of StorageInfo across all erasure coded object sets.
func (s *xlSets) StorageInfo(ctx context.Context, local bool) StorageInfo {
	var storageInfo StorageInfo

	storageInfos := make([]StorageInfo, len(s.sets))
	storageInfo.Backend.Type = BackendErasure

	g := errgroup.WithNErrs(len(s.sets))
	for index := range s.sets {
		index := index
		g.Go(func() error {
			storageInfos[index] = s.sets[index].StorageInfo(ctx, local)
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
	}

	scParity := globalStorageClass.GetParityForSC(storageclass.STANDARD)
	if scParity == 0 {
		scParity = s.drivesPerSet / 2
	}
	storageInfo.Backend.StandardSCData = s.drivesPerSet - scParity
	storageInfo.Backend.StandardSCParity = scParity

	rrSCParity := globalStorageClass.GetParityForSC(storageclass.RRS)
	storageInfo.Backend.RRSCData = s.drivesPerSet - rrSCParity
	storageInfo.Backend.RRSCParity = rrSCParity

	storageInfo.Backend.Sets = make([][]madmin.DriveInfo, s.setCount)
	for i := range storageInfo.Backend.Sets {
		storageInfo.Backend.Sets[i] = make([]madmin.DriveInfo, s.drivesPerSet)
	}

	if local {
		// if local is true, we don't need to read format.json
		return storageInfo
	}

	s.xlDisksMu.RLock()
	storageDisks := s.xlDisks.Copy()
	s.xlDisksMu.RUnlock()

	for i := 0; i < s.setCount; i++ {
		for j := 0; j < s.drivesPerSet; j++ {
			if storageDisks[i][j] == nil {
				storageInfo.Backend.Sets[i][j] = madmin.DriveInfo{
					State:    madmin.DriveStateOffline,
					Endpoint: s.endpointStrings[i*s.drivesPerSet+j],
				}
				continue
			}
			diskID, err := storageDisks[i][j].GetDiskID()
			if err != nil {
				if err == errUnformattedDisk {
					storageInfo.Backend.Sets[i][j] = madmin.DriveInfo{
						State:    madmin.DriveStateUnformatted,
						Endpoint: storageDisks[i][j].String(),
						UUID:     "",
					}
				} else {
					storageInfo.Backend.Sets[i][j] = madmin.DriveInfo{
						State:    madmin.DriveStateCorrupt,
						Endpoint: storageDisks[i][j].String(),
						UUID:     "",
					}
				}
				continue
			}
			storageInfo.Backend.Sets[i][j] = madmin.DriveInfo{
				State:    madmin.DriveStateOk,
				Endpoint: storageDisks[i][j].String(),
				UUID:     diskID,
			}
		}
	}

	return storageInfo
}

func (s *xlSets) CrawlAndGetDataUsage(ctx context.Context, updates chan<- DataUsageInfo) error {
	return NotImplemented{}
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
		g.Go(func() error {
			if errs[index] == nil {
				return sets[index].DeleteBucket(GlobalContext, bucket, false)
			}
			return nil
		}, index)
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
func (s *xlSets) getHashedSetIndex(input string) int {
	return hashKey(s.distributionAlgo, input, len(s.sets))
}

// Returns always a same erasure coded set for a given input.
func (s *xlSets) getHashedSet(input string) (set *xlObjects) {
	return s.sets[s.getHashedSetIndex(input)]
}

// GetBucketInfo - returns bucket info from one of the erasure coded set.
func (s *xlSets) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error) {
	return s.getHashedSet("").GetBucketInfo(ctx, bucket)
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

// SetBucketLifecycle sets lifecycle on bucket
func (s *xlSets) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *lifecycle.Lifecycle) error {
	return saveLifecycleConfig(ctx, s, bucket, lifecycle)
}

// GetBucketLifecycle will get lifecycle on bucket
func (s *xlSets) GetBucketLifecycle(ctx context.Context, bucket string) (*lifecycle.Lifecycle, error) {
	return getLifecycleConfig(s, bucket)
}

// DeleteBucketLifecycle deletes all lifecycle on bucket
func (s *xlSets) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	return removeLifecycleConfig(ctx, s, bucket)
}

// GetBucketSSEConfig returns bucket encryption config on given bucket
func (s *xlSets) GetBucketSSEConfig(ctx context.Context, bucket string) (*bucketsse.BucketSSEConfig, error) {
	return getBucketSSEConfig(s, bucket)
}

// SetBucketSSEConfig sets bucket encryption config on given bucket
func (s *xlSets) SetBucketSSEConfig(ctx context.Context, bucket string, config *bucketsse.BucketSSEConfig) error {
	return saveBucketSSEConfig(ctx, s, bucket, config)
}

// DeleteBucketSSEConfig deletes bucket encryption config on given bucket
func (s *xlSets) DeleteBucketSSEConfig(ctx context.Context, bucket string) error {
	return removeBucketSSEConfig(ctx, s, bucket)
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
func (s *xlSets) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	g := errgroup.WithNErrs(len(s.sets))

	// Delete buckets in parallel across all sets.
	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].DeleteBucket(ctx, bucket, forceDelete)
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
		g.Go(func() error {
			if errs[index] == nil {
				return sets[index].MakeBucketWithLocation(GlobalContext, bucket, "")
			}
			return nil
		}, index)
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

// DeleteObjects - bulk delete of objects
// Bulk delete is only possible within one set. For that purpose
// objects are group by set first, and then bulk delete is invoked
// for each set, the error response of each delete will be returned
func (s *xlSets) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	type delObj struct {
		// Set index associated to this object
		setIndex int
		// Original index from the list of arguments
		// where this object is passed
		origIndex int
		// Object name
		name string
	}

	// Transform []delObj to the list of object names
	toNames := func(delObjs []delObj) []string {
		names := make([]string, len(delObjs))
		for i, obj := range delObjs {
			names[i] = obj.name
		}
		return names
	}

	// The result of delete operation on all passed objects
	var delErrs = make([]error, len(objects))

	// A map between a set and its associated objects
	var objSetMap = make(map[int][]delObj)

	// Group objects by set index
	for i, object := range objects {
		index := s.getHashedSetIndex(object)
		objSetMap[index] = append(objSetMap[index], delObj{setIndex: index, origIndex: i, name: object})
	}

	// Invoke bulk delete on objects per set and save
	// the result of the delete operation
	for _, objsGroup := range objSetMap {
		errs, err := s.getHashedSet(objsGroup[0].name).DeleteObjects(ctx, bucket, toNames(objsGroup))
		if err != nil {
			return nil, err
		}
		for i, obj := range objsGroup {
			delErrs[obj.origIndex] = errs[i]
		}
	}

	return delErrs, nil
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

	putOpts := ObjectOptions{ServerSideEncryption: dstOpts.ServerSideEncryption, UserDefined: srcInfo.UserDefined}
	return destSet.putObject(ctx, destBucket, destObject, srcInfo.PutObjReader, putOpts)
}

// FileInfoCh - file info channel
type FileInfoCh struct {
	Ch    chan FileInfo
	Prev  FileInfo
	Valid bool
}

// Pop - pops a cached entry if any, or from the cached channel.
func (f *FileInfoCh) Pop() (fi FileInfo, ok bool) {
	if f.Valid {
		f.Valid = false
		return f.Prev, true
	} // No cached entries found, read from channel
	f.Prev, ok = <-f.Ch
	return f.Prev, ok
}

// Push - cache an entry, for Pop() later.
func (f *FileInfoCh) Push(fi FileInfo) {
	f.Prev = fi
	f.Valid = true
}

// Calculate least entry across multiple FileInfo channels,
// returns the least common entry and the total number of times
// we found this entry. Additionally also returns a boolean
// to indicate if the caller needs to call this function
// again to list the next entry. It is callers responsibility
// if the caller wishes to list N entries to call leastEntry
// N times until this boolean is 'false'.
func leastEntry(entryChs []FileInfoCh, entries []FileInfo, entriesValid []bool) (FileInfo, int, bool) {
	for i := range entryChs {
		entries[i], entriesValid[i] = entryChs[i].Pop()
	}

	var isTruncated = false
	for _, valid := range entriesValid {
		if !valid {
			continue
		}
		isTruncated = true
		break
	}

	var lentry FileInfo
	var found bool
	for i, valid := range entriesValid {
		if !valid {
			continue
		}
		if !found {
			lentry = entries[i]
			found = true
			continue
		}
		if entries[i].Name < lentry.Name {
			lentry = entries[i]
		}
	}

	// We haven't been able to find any least entry,
	// this would mean that we don't have valid entry.
	if !found {
		return lentry, 0, isTruncated
	}

	leastEntryCount := 0
	for i, valid := range entriesValid {
		if !valid {
			continue
		}

		// Entries are duplicated across disks,
		// we should simply skip such entries.
		if lentry.Name == entries[i].Name && lentry.ModTime.Equal(entries[i].ModTime) {
			leastEntryCount++
			continue
		}

		// Push all entries which are lexically higher
		// and will be returned later in Pop()
		entryChs[i].Push(entries[i])
	}

	return lentry, leastEntryCount, isTruncated
}

// mergeEntriesCh - merges FileInfo channel to entries upto maxKeys.
func mergeEntriesCh(entryChs []FileInfoCh, maxKeys int, ndisks int) (entries FilesInfo) {
	var i = 0
	entriesInfos := make([]FileInfo, len(entryChs))
	entriesValid := make([]bool, len(entryChs))
	for {
		fi, quorumCount, valid := leastEntry(entryChs, entriesInfos, entriesValid)
		if !valid {
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
			entries.IsTruncated = isTruncated(entryChs, entriesInfos, entriesValid)
			break
		}
	}
	return entries
}

func isTruncated(entryChs []FileInfoCh, entries []FileInfo, entriesValid []bool) bool {
	for i := range entryChs {
		entries[i], entriesValid[i] = entryChs[i].Pop()
	}

	var isTruncated = false
	for _, valid := range entriesValid {
		if !valid {
			continue
		}
		isTruncated = true
		break
	}
	for i := range entryChs {
		if entriesValid[i] {
			entryChs[i].Push(entries[i])
		}
	}
	return isTruncated
}

func (s *xlSets) startMergeWalks(ctx context.Context, bucket, prefix, marker string, recursive bool, endWalkCh <-chan struct{}) []FileInfoCh {
	return s.startMergeWalksN(ctx, bucket, prefix, marker, recursive, endWalkCh, -1)
}

// Starts a walk channel across all disks and returns a slice of
// FileInfo channels which can be read from.
func (s *xlSets) startMergeWalksN(ctx context.Context, bucket, prefix, marker string, recursive bool, endWalkCh <-chan struct{}, ndisks int) []FileInfoCh {
	var entryChs []FileInfoCh
	var success int
	for _, set := range s.sets {
		// Reset for the next erasure set.
		success = ndisks
		for _, disk := range set.getLoadBalancedDisks() {
			if disk == nil {
				// Disk can be offline
				continue
			}
			entryCh, err := disk.Walk(bucket, prefix, marker, recursive, xlMetaJSONFile, readMetadata, endWalkCh)
			if err != nil {
				// Disk walk returned error, ignore it.
				continue
			}
			entryChs = append(entryChs, FileInfoCh{
				Ch: entryCh,
			})
			success--
			if success == 0 {
				break
			}
		}
	}
	return entryChs
}

// Starts a walk channel across all disks and returns a slice of
// FileInfo channels which can be read from.
func (s *xlSets) startSplunkMergeWalksN(ctx context.Context, bucket, prefix, marker string, endWalkCh <-chan struct{}, ndisks int) []FileInfoCh {
	var entryChs []FileInfoCh
	var success int
	for _, set := range s.sets {
		// Reset for the next erasure set.
		success = ndisks
		for _, disk := range set.getLoadBalancedDisks() {
			if disk == nil {
				// Disk can be offline
				continue
			}
			entryCh, err := disk.WalkSplunk(bucket, prefix, marker, endWalkCh)
			if err != nil {
				// Disk walk returned error, ignore it.
				continue
			}
			entryChs = append(entryChs, FileInfoCh{
				Ch: entryCh,
			})
			success--
			if success == 0 {
				break
			}
		}
	}
	return entryChs
}

func (s *xlSets) listObjectsNonSlash(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	endWalkCh := make(chan struct{})
	defer close(endWalkCh)

	const ndisks = 3
	entryChs := s.startMergeWalksN(GlobalContext, bucket, prefix, "", true, endWalkCh, ndisks)

	var objInfos []ObjectInfo
	var eof bool
	var prevPrefix string

	entriesValid := make([]bool, len(entryChs))
	entries := make([]FileInfo, len(entryChs))
	for {
		if len(objInfos) == maxKeys {
			break
		}

		result, quorumCount, ok := leastEntry(entryChs, entries, entriesValid)
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

// ListObjects - implements listing of objects across disks, each disk is independently
// walked and merged at this layer. Resulting value through the merge process sends
// the data in lexically sorted order.
// If partialQuorumOnly is set only objects that does not have full quorum is returned.
func (s *xlSets) listObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	if err = checkListObjsArgs(ctx, bucket, prefix, marker, s); err != nil {
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
		// "heal" option passed can be ignored as the heal-listing does not send non-standard delimiter.
		return s.listObjectsNonSlash(ctx, bucket, prefix, marker, delimiter, maxKeys)
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == SlashSeparator {
		recursive = false
	}

	const ndisks = 3

	entryChs, endWalkCh := s.pool.Release(listParams{bucket: bucket, recursive: recursive, marker: marker, prefix: prefix})
	if entryChs == nil {
		endWalkCh = make(chan struct{})
		// start file tree walk across at most randomly 3 disks in a set.
		entryChs = s.startMergeWalksN(GlobalContext, bucket, prefix, marker, recursive, endWalkCh, ndisks)
	}

	entries := mergeEntriesCh(entryChs, maxKeys, ndisks)
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
			loi.Prefixes = append(loi.Prefixes, entry.Name)
			continue
		}
		loi.Objects = append(loi.Objects, objInfo)
	}
	if loi.IsTruncated {
		s.pool.Set(listParams{bucket, recursive, loi.NextMarker, prefix}, entryChs, endWalkCh)
	}
	return loi, nil
}

// ListObjects - implements listing of objects across disks, each disk is indepenently
// walked and merged at this layer. Resulting value through the merge process sends
// the data in lexically sorted order.
func (s *xlSets) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	return s.listObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
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

func formatsToDrivesInfo(endpoints Endpoints, formats []*formatXLV3, sErrs []error) (beforeDrives []madmin.DriveInfo) {
	beforeDrives = make([]madmin.DriveInfo, len(endpoints))
	// Existing formats are available (i.e. ok), so save it in
	// result, also populate disks to be healed.
	for i, format := range formats {
		drive := endpoints.GetString(i)
		var state = madmin.DriveStateCorrupt
		switch {
		case format != nil:
			state = madmin.DriveStateOk
		case sErrs[i] == errUnformattedDisk:
			state = madmin.DriveStateMissing
		case sErrs[i] == errDiskNotFound:
			state = madmin.DriveStateOffline
		}
		beforeDrives[i] = madmin.DriveInfo{
			UUID: func() string {
				if format != nil {
					return format.XL.This
				}
				return ""
			}(),
			Endpoint: drive,
			State:    state,
		}
	}

	return beforeDrives
}

// Reloads the format from the disk, usually called by a remote peer notifier while
// healing in a distributed setup.
func (s *xlSets) ReloadFormat(ctx context.Context, dryRun bool) (err error) {
	storageDisks, errs := initStorageDisksWithErrors(s.endpoints)
	for i, err := range errs {
		if err != nil && err != errDiskNotFound {
			return fmt.Errorf("Disk %s: %w", s.endpoints[i], err)
		}
	}
	defer func(storageDisks []StorageAPI) {
		if err != nil {
			closeStorageDisks(storageDisks)
		}
	}(storageDisks)

	formats, sErrs := loadFormatXLAll(storageDisks, false)
	if err = checkFormatXLValues(formats, s.drivesPerSet); err != nil {
		return err
	}

	for index, sErr := range sErrs {
		if sErr != nil {
			// Look for acceptable heal errors, for any other
			// errors we should simply quit and return.
			if _, ok := formatHealErrors[sErr]; !ok {
				return fmt.Errorf("Disk %s: %w", s.endpoints[index], sErr)
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

	// Replace with new reference format.
	s.format = refFormat

	// Close all existing disks and reconnect all the disks.
	s.xlDisksMu.Lock()
	for _, disk := range storageDisks {
		if disk == nil {
			continue
		}

		diskID, err := disk.GetDiskID()
		if err != nil {
			disk.Close()
			continue
		}

		m, n, err := findDiskIndexByDiskID(refFormat, diskID)
		if err != nil {
			disk.Close()
			continue
		}

		if s.xlDisks[m][n] != nil {
			s.xlDisks[m][n].Close()
		}

		s.xlDisks[m][n] = disk
	}
	s.xlDisksMu.Unlock()

	// Restart monitoring loop to monitor reformatted disks again.
	go s.monitorAndConnectEndpoints(GlobalContext, defaultMonitorConnectEndpointInterval)

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
	g := errgroup.WithNErrs(len(storageDisks))
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			var err error
			if storageDisks[index] != nil {
				infos[index], err = storageDisks[index].DiskInfo()
			} else {
				// Disk not found.
				err = errDiskNotFound
			}
			return err
		}, index)
	}
	return infos, g.Wait()
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
func (s *xlSets) HealFormat(ctx context.Context, dryRun bool) (res madmin.HealResultItem, err error) {
	storageDisks, errs := initStorageDisksWithErrors(s.endpoints)
	for i, derr := range errs {
		if derr != nil && derr != errDiskNotFound {
			return madmin.HealResultItem{}, fmt.Errorf("Disk %s: %w", s.endpoints[i], derr)
		}
	}

	defer func(storageDisks []StorageAPI) {
		if err != nil {
			closeStorageDisks(storageDisks)
		}
	}(storageDisks)

	markRootDisksAsDown(storageDisks)

	formats, sErrs := loadFormatXLAll(storageDisks, true)
	if err = checkFormatXLValues(formats, s.drivesPerSet); err != nil {
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
				return res, fmt.Errorf("Disk %s: %w", s.endpoints[index], sErr)
			}
		}
	}

	if countErrs(sErrs, errUnformattedDisk) == 0 {
		// No unformatted disks found disks are either offline
		// or online, no healing is required.
		return res, errNoHealRequired
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

		// Disconnect/relinquish all existing disks, lockers and reconnect the disks, lockers.
		s.xlDisksMu.Lock()
		for _, disk := range storageDisks {
			if disk == nil {
				continue
			}

			diskID, err := disk.GetDiskID()
			if err != nil {
				disk.Close()
				continue
			}

			m, n, err := findDiskIndexByDiskID(refFormat, diskID)
			if err != nil {
				disk.Close()
				continue
			}

			if s.xlDisks[m][n] != nil {
				s.xlDisks[m][n].Close()
			}

			s.xlDisks[m][n] = disk
		}
		s.xlDisksMu.Unlock()

		// Restart our monitoring loop to start monitoring newly formatted disks.
		go s.monitorAndConnectEndpoints(GlobalContext, defaultMonitorConnectEndpointInterval)
	}

	return res, nil
}

// HealBucket - heals inconsistent buckets and bucket metadata on all sets.
func (s *xlSets) HealBucket(ctx context.Context, bucket string, dryRun, remove bool) (result madmin.HealResultItem, err error) {
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

	for i := range s.endpoints {
		var foundBefore bool
		for _, v := range result.Before.Drives {
			if s.endpointStrings[i] == v.Endpoint {
				foundBefore = true
			}
		}
		if !foundBefore {
			result.Before.Drives = append(result.Before.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: s.endpointStrings[i],
				State:    madmin.DriveStateOffline,
			})
		}
		var foundAfter bool
		for _, v := range result.After.Drives {
			if s.endpointStrings[i] == v.Endpoint {
				foundAfter = true
			}
		}
		if !foundAfter {
			result.After.Drives = append(result.After.Drives, madmin.HealDriveInfo{
				UUID:     "",
				Endpoint: s.endpointStrings[i],
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
func (s *xlSets) HealObject(ctx context.Context, bucket, object string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	return s.getHashedSet(object).HealObject(ctx, bucket, object, opts)
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

// Walk a bucket, optionally prefix recursively, until we have returned
// all the content to objectInfo channel, it is callers responsibility
// to allocate a receive channel for ObjectInfo, upon any unhandled
// error walker returns error. Optionally if context.Done() is received
// then Walk() stops the walker.
func (s *xlSets) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo) error {
	if err := checkListObjsArgs(ctx, bucket, prefix, "", s); err != nil {
		// Upon error close the channel.
		close(results)
		return err
	}

	entryChs := s.startMergeWalks(ctx, bucket, prefix, "", true, ctx.Done())

	entriesValid := make([]bool, len(entryChs))
	entries := make([]FileInfo, len(entryChs))

	go func() {
		defer close(results)

		for {
			entry, quorumCount, ok := leastEntry(entryChs, entries, entriesValid)
			if !ok {
				return
			}

			if quorumCount >= s.drivesPerSet/2 {
				results <- entry.ToObjectInfo() // Read quorum exists proceed
			}
			// skip entries which do not have quorum
		}
	}()

	return nil
}

// HealObjects - Heal all objects recursively at a specified prefix, any
// dangling objects deleted as well automatically.
func (s *xlSets) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, healObject healObjectFn) error {
	endWalkCh := make(chan struct{})
	defer close(endWalkCh)

	entryChs := s.startMergeWalks(ctx, bucket, prefix, "", true, endWalkCh)

	entriesValid := make([]bool, len(entryChs))
	entries := make([]FileInfo, len(entryChs))
	for {
		entry, quorumCount, ok := leastEntry(entryChs, entries, entriesValid)
		if !ok {
			break
		}

		if quorumCount == s.drivesPerSet && opts.ScanMode == madmin.HealNormalScan {
			// Skip good entries.
			continue
		}

		// Wait and proceed if there are active requests
		waitForLowHTTPReq(int32(s.drivesPerSet))

		if err := healObject(bucket, entry.Name); err != nil {
			return toObjectErr(err, bucket, entry.Name)
		}
	}

	return nil
}

// PutObjectTag - replace or add tags to an existing object
func (s *xlSets) PutObjectTag(ctx context.Context, bucket, object string, tags string) error {
	return s.getHashedSet(object).PutObjectTag(ctx, bucket, object, tags)
}

// DeleteObjectTag - delete object tags from an existing object
func (s *xlSets) DeleteObjectTag(ctx context.Context, bucket, object string) error {
	return s.getHashedSet(object).DeleteObjectTag(ctx, bucket, object)
}

// GetObjectTag - get object tags from an existing object
func (s *xlSets) GetObjectTag(ctx context.Context, bucket, object string) (tagging.Tagging, error) {
	return s.getHashedSet(object).GetObjectTag(ctx, bucket, object)
}

// GetMetrics - no op
func (s *xlSets) GetMetrics(ctx context.Context) (*Metrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &Metrics{}, NotImplemented{}
}

// IsReady - Returns true if atleast n/2 disks (read quorum) are online
func (s *xlSets) IsReady(_ context.Context) bool {
	s.xlDisksMu.RLock()
	defer s.xlDisksMu.RUnlock()

	var activeDisks int
	for i := 0; i < s.setCount; i++ {
		for j := 0; j < s.drivesPerSet; j++ {
			if s.xlDisks[i][j] == nil {
				continue
			}
			if s.xlDisks[i][j].IsOnline() {
				activeDisks++
			}
			// Return true if read quorum is available.
			if activeDisks >= len(s.endpoints)/2 {
				return true
			}
		}
	}
	// Disks are not ready
	return false
}

// maintainMRFList gathers the list of successful partial uploads
// from all underlying xl sets and puts them in a global map which
// should not have more than 10000 entries.
func (s *xlSets) maintainMRFList() {
	var agg = make(chan partialUpload, 10000)
	for i, xl := range s.sets {
		go func(c <-chan partialUpload, setIndex int) {
			for msg := range c {
				msg.failedSet = setIndex
				select {
				case agg <- msg:
				default:
				}
			}
		}(xl.mrfUploadCh, i)
	}

	for fUpload := range agg {
		s.mrfMU.Lock()
		if len(s.mrfUploads) > 10000 {
			s.mrfMU.Unlock()
			continue
		}
		s.mrfUploads[pathJoin(fUpload.bucket, fUpload.object)] = fUpload.failedSet
		s.mrfMU.Unlock()
	}
}

// healMRFRoutine monitors new disks connection, sweep the MRF list
// to find objects related to the new disk that needs to be healed.
func (s *xlSets) healMRFRoutine() {
	// Wait until background heal state is initialized
	var bgSeq *healSequence
	for {
		if globalBackgroundHealState == nil {
			time.Sleep(time.Second)
			continue
		}
		var ok bool
		bgSeq, ok = globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
		if ok {
			break
		}
		time.Sleep(time.Second)
	}

	for e := range s.disksConnectEvent {
		// Get the list of objects related the xl set
		// to which the connected disk belongs.
		var mrfUploads []string
		s.mrfMU.Lock()
		for k, v := range s.mrfUploads {
			if v == e.setIndex {
				mrfUploads = append(mrfUploads, k)
			}
		}
		s.mrfMU.Unlock()

		// Heal objects
		for _, u := range mrfUploads {
			// Send an object to be healed with a timeout
			select {
			case bgSeq.sourceCh <- healSource{path: u}:
			case <-time.After(100 * time.Millisecond):
			}

			s.mrfMU.Lock()
			delete(s.mrfUploads, u)
			s.mrfMU.Unlock()
		}
	}
}
