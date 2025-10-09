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
	"hash/crc32"
	"math/rand"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/dchest/siphash"
	"github.com/google/uuid"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/pkg/v3/sync/errgroup"
	"github.com/puzpuzpuz/xsync/v3"
)

// setsDsyncLockers is encapsulated type for Close()
type setsDsyncLockers [][]dsync.NetLocker

// erasureSets implements ObjectLayer combining a static list of erasure coded
// object sets. NOTE: There is no dynamic scaling allowed or intended in
// current design.
type erasureSets struct {
	sets []*erasureObjects

	// Reference format.
	format *formatErasureV3

	// erasureDisks mutex to lock erasureDisks.
	erasureDisksMu sync.RWMutex

	// Re-ordered list of disks per set.
	erasureDisks [][]StorageAPI

	// Distributed locker clients.
	erasureLockers setsDsyncLockers

	// Distributed lock owner (constant per running instance).
	erasureLockOwner string

	// List of endpoints provided on the command line.
	endpoints PoolEndpoints

	// String version of all the endpoints, an optimization
	// to avoid url.String() conversion taking CPU on
	// large disk setups.
	endpointStrings []string

	// Total number of sets and the number of disks per set.
	setCount, setDriveCount int
	defaultParityCount      int

	poolIndex int

	// Distribution algorithm of choice.
	distributionAlgo string
	deploymentID     [16]byte

	lastConnectDisksOpTime time.Time
}

var staleUploadsCleanupIntervalChangedCh = make(chan struct{})

func (s *erasureSets) getDiskMap() map[Endpoint]StorageAPI {
	diskMap := make(map[Endpoint]StorageAPI)

	s.erasureDisksMu.RLock()
	defer s.erasureDisksMu.RUnlock()

	for i := range s.setCount {
		for j := 0; j < s.setDriveCount; j++ {
			disk := s.erasureDisks[i][j]
			if disk == OfflineDisk {
				continue
			}
			if !disk.IsOnline() {
				continue
			}
			diskMap[disk.Endpoint()] = disk
		}
	}
	return diskMap
}

// Initializes a new StorageAPI from the endpoint argument, returns
// StorageAPI and also `format` which exists on the disk.
func connectEndpoint(endpoint Endpoint) (StorageAPI, *formatErasureV3, error) {
	disk, err := newStorageAPI(endpoint, storageOpts{
		cleanUp:     false,
		healthCheck: false,
	})
	if err != nil {
		return nil, nil, err
	}

	format, err := loadFormatErasure(disk, false)
	if err != nil {
		disk.Close()
		return nil, nil, fmt.Errorf("Drive: %s returned %w", disk, err) // make sure to '%w' to wrap the error
	}

	disk.Close()
	disk, err = newStorageAPI(endpoint, storageOpts{
		cleanUp:     true,
		healthCheck: true,
	})
	if err != nil {
		return nil, nil, err
	}

	return disk, format, nil
}

// findDiskIndex - returns the i,j'th position of the input `diskID` against the reference
// format, after successful validation.
//   - i'th position is the set index
//   - j'th position is the disk index in the current set
func findDiskIndexByDiskID(refFormat *formatErasureV3, diskID string) (int, int, error) {
	if diskID == "" {
		return -1, -1, errDiskNotFound
	}
	if diskID == offlineDiskUUID {
		return -1, -1, fmt.Errorf("DriveID: %s is offline", diskID)
	}
	for i := range len(refFormat.Erasure.Sets) {
		for j := 0; j < len(refFormat.Erasure.Sets[0]); j++ {
			if refFormat.Erasure.Sets[i][j] == diskID {
				return i, j, nil
			}
		}
	}

	return -1, -1, fmt.Errorf("DriveID: %s not found", diskID)
}

// findDiskIndex - returns the i,j'th position of the input `format` against the reference
// format, after successful validation.
//   - i'th position is the set index
//   - j'th position is the disk index in the current set
func findDiskIndex(refFormat, format *formatErasureV3) (int, int, error) {
	if err := formatErasureV3Check(refFormat, format); err != nil {
		return 0, 0, err
	}

	if format.Erasure.This == offlineDiskUUID {
		return -1, -1, fmt.Errorf("DriveID: %s is offline", format.Erasure.This)
	}

	for i := range len(refFormat.Erasure.Sets) {
		for j := 0; j < len(refFormat.Erasure.Sets[0]); j++ {
			if refFormat.Erasure.Sets[i][j] == format.Erasure.This {
				return i, j, nil
			}
		}
	}

	return -1, -1, fmt.Errorf("DriveID: %s not found", format.Erasure.This)
}

// Legacy returns 'true' if distribution algo is CRCMOD
func (s *erasureSets) Legacy() (ok bool) {
	return s.distributionAlgo == formatErasureVersionV2DistributionAlgoV1
}

// connectDisks - attempt to connect all the endpoints, loads format
// and re-arranges the disks in proper position.
func (s *erasureSets) connectDisks(log bool) {
	defer func() {
		s.lastConnectDisksOpTime = time.Now()
	}()

	var wg sync.WaitGroup
	diskMap := s.getDiskMap()
	for _, endpoint := range s.endpoints.Endpoints {
		cdisk := diskMap[endpoint]
		if cdisk != nil && cdisk.IsOnline() {
			if s.lastConnectDisksOpTime.IsZero() {
				continue
			}

			// An online-disk means its a valid disk but it may be a re-connected disk
			// we verify that here based on LastConn(), however we make sure to avoid
			// putting it back into the s.erasureDisks by re-placing the disk again.
			_, setIndex, _ := cdisk.GetDiskLoc()
			if setIndex != -1 {
				continue
			}
		}
		if cdisk != nil {
			// Close previous offline disk.
			cdisk.Close()
		}

		wg.Add(1)
		go func(endpoint Endpoint) {
			defer wg.Done()
			disk, format, err := connectEndpoint(endpoint)
			if err != nil {
				if endpoint.IsLocal && errors.Is(err, errUnformattedDisk) {
					globalBackgroundHealState.pushHealLocalDisks(endpoint)
				} else if !errors.Is(err, errDriveIsRoot) {
					if log {
						printEndpointError(endpoint, err, true)
					}
				}
				return
			}
			if disk.IsLocal() {
				h := disk.Healing()
				if h != nil && !h.Finished {
					globalBackgroundHealState.pushHealLocalDisks(disk.Endpoint())
				}
			}
			s.erasureDisksMu.Lock()
			setIndex, diskIndex, err := findDiskIndex(s.format, format)
			if err != nil {
				printEndpointError(endpoint, err, false)
				disk.Close()
				s.erasureDisksMu.Unlock()
				return
			}

			if currentDisk := s.erasureDisks[setIndex][diskIndex]; currentDisk != nil {
				if !reflect.DeepEqual(currentDisk.Endpoint(), disk.Endpoint()) {
					err = fmt.Errorf("Detected unexpected drive ordering refusing to use the drive: expecting %s, found %s, refusing to use the drive",
						currentDisk.Endpoint(), disk.Endpoint())
					printEndpointError(endpoint, err, false)
					disk.Close()
					s.erasureDisksMu.Unlock()
					return
				}
				s.erasureDisks[setIndex][diskIndex].Close()
			}

			disk.SetDiskID(format.Erasure.This)
			s.erasureDisks[setIndex][diskIndex] = disk

			if disk.IsLocal() {
				globalLocalDrivesMu.Lock()
				if globalIsDistErasure {
					globalLocalSetDrives[s.poolIndex][setIndex][diskIndex] = disk
				}
				globalLocalDrivesMap[disk.Endpoint().String()] = disk
				globalLocalDrivesMu.Unlock()
			}
			s.erasureDisksMu.Unlock()
		}(endpoint)
	}

	wg.Wait()
}

// monitorAndConnectEndpoints this is a monitoring loop to keep track of disconnected
// endpoints by reconnecting them and making sure to place them into right position in
// the set topology, this monitoring happens at a given monitoring interval.
func (s *erasureSets) monitorAndConnectEndpoints(ctx context.Context, monitorInterval time.Duration) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	time.Sleep(time.Duration(r.Float64() * float64(time.Second)))

	// Pre-emptively connect the disks if possible.
	s.connectDisks(false)

	monitor := time.NewTimer(monitorInterval)
	defer monitor.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-monitor.C:
			if serverDebugLog {
				console.Debugln("running drive monitoring")
			}

			s.connectDisks(true)

			// Reset the timer for next interval
			monitor.Reset(monitorInterval)
		}
	}
}

func (s *erasureSets) GetLockers(setIndex int) func() ([]dsync.NetLocker, string) {
	return func() ([]dsync.NetLocker, string) {
		lockers := make([]dsync.NetLocker, len(s.erasureLockers[setIndex]))
		copy(lockers, s.erasureLockers[setIndex])
		return lockers, s.erasureLockOwner
	}
}

func (s *erasureSets) GetEndpointStrings(setIndex int) func() []string {
	return func() []string {
		eps := make([]string, s.setDriveCount)
		copy(eps, s.endpointStrings[setIndex*s.setDriveCount:setIndex*s.setDriveCount+s.setDriveCount])
		return eps
	}
}

func (s *erasureSets) GetEndpoints(setIndex int) func() []Endpoint {
	return func() []Endpoint {
		eps := make([]Endpoint, s.setDriveCount)
		copy(eps, s.endpoints.Endpoints[setIndex*s.setDriveCount:setIndex*s.setDriveCount+s.setDriveCount])
		return eps
	}
}

// GetDisks returns a closure for a given set, which provides list of disks per set.
func (s *erasureSets) GetDisks(setIndex int) func() []StorageAPI {
	return func() []StorageAPI {
		s.erasureDisksMu.RLock()
		defer s.erasureDisksMu.RUnlock()
		disks := make([]StorageAPI, s.setDriveCount)
		copy(disks, s.erasureDisks[setIndex])
		return disks
	}
}

// defaultMonitorConnectEndpointInterval is the interval to monitor endpoint connections.
// Must be bigger than defaultMonitorNewDiskInterval.
const defaultMonitorConnectEndpointInterval = defaultMonitorNewDiskInterval + time.Second*5

// Initialize new set of erasure coded sets.
func newErasureSets(ctx context.Context, endpoints PoolEndpoints, storageDisks []StorageAPI, format *formatErasureV3, defaultParityCount, poolIdx int) (*erasureSets, error) {
	setCount := len(format.Erasure.Sets)
	setDriveCount := len(format.Erasure.Sets[0])

	endpointStrings := make([]string, len(endpoints.Endpoints))
	for i, endpoint := range endpoints.Endpoints {
		endpointStrings[i] = endpoint.String()
	}

	// Initialize the erasure sets instance.
	s := &erasureSets{
		sets:               make([]*erasureObjects, setCount),
		erasureDisks:       make([][]StorageAPI, setCount),
		erasureLockers:     make([][]dsync.NetLocker, setCount),
		erasureLockOwner:   globalLocalNodeName,
		endpoints:          endpoints,
		endpointStrings:    endpointStrings,
		setCount:           setCount,
		setDriveCount:      setDriveCount,
		defaultParityCount: defaultParityCount,
		format:             format,
		distributionAlgo:   format.Erasure.DistributionAlgo,
		deploymentID:       uuid.MustParse(format.ID),
		poolIndex:          poolIdx,
	}

	mutex := newNSLock(globalIsDistErasure)

	for i := range setCount {
		s.erasureDisks[i] = make([]StorageAPI, setDriveCount)
	}

	erasureLockers := map[string]dsync.NetLocker{}
	for _, endpoint := range endpoints.Endpoints {
		if _, ok := erasureLockers[endpoint.Host]; !ok {
			erasureLockers[endpoint.Host] = newLockAPI(endpoint)
		}
	}

	var wg sync.WaitGroup
	var lk sync.Mutex
	for i := range setCount {
		lockerEpSet := set.NewStringSet()
		for j := range setDriveCount {
			wg.Add(1)
			go func(i int, endpoint Endpoint) {
				defer wg.Done()

				lk.Lock()
				// Only add lockers only one per endpoint and per erasure set.
				if locker, ok := erasureLockers[endpoint.Host]; ok && !lockerEpSet.Contains(endpoint.Host) {
					lockerEpSet.Add(endpoint.Host)
					s.erasureLockers[i] = append(s.erasureLockers[i], locker)
				}
				lk.Unlock()
			}(i, endpoints.Endpoints[i*setDriveCount+j])
		}
	}
	wg.Wait()

	for i := range setCount {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var innerWg sync.WaitGroup
			for j := range setDriveCount {
				disk := storageDisks[i*setDriveCount+j]
				if disk == nil {
					continue
				}

				if disk.IsLocal() && globalIsDistErasure {
					globalLocalDrivesMu.RLock()
					ldisk := globalLocalSetDrives[poolIdx][i][j]
					if ldisk == nil {
						globalLocalDrivesMu.RUnlock()
						continue
					}
					disk.Close()
					disk = ldisk
					globalLocalDrivesMu.RUnlock()
				}

				innerWg.Add(1)
				go func(disk StorageAPI, i, j int) {
					defer innerWg.Done()
					diskID, err := disk.GetDiskID()
					if err != nil {
						if !errors.Is(err, errUnformattedDisk) {
							bootLogIf(ctx, err)
						}
						return
					}
					if diskID == "" {
						return
					}
					s.erasureDisks[i][j] = disk
				}(disk, i, j)
			}

			innerWg.Wait()

			// Initialize erasure objects for a given set.
			s.sets[i] = &erasureObjects{
				setIndex:           i,
				poolIndex:          poolIdx,
				setDriveCount:      setDriveCount,
				defaultParityCount: defaultParityCount,
				getDisks:           s.GetDisks(i),
				getLockers:         s.GetLockers(i),
				getEndpoints:       s.GetEndpoints(i),
				getEndpointStrings: s.GetEndpointStrings(i),
				nsMutex:            mutex,
			}
		}(i)
	}

	wg.Wait()

	// start cleanup stale uploads go-routine.
	go s.cleanupStaleUploads(ctx)

	// start cleanup of deleted objects.
	go s.cleanupDeletedObjects(ctx)

	// Start the disk monitoring and connect routine.
	if !globalIsTesting {
		go s.monitorAndConnectEndpoints(ctx, defaultMonitorConnectEndpointInterval)
	}

	return s, nil
}

// cleanup ".trash/" folder every 5m minutes with sufficient sleep cycles, between each
// deletes a dynamic sleeper is used with a factor of 10 ratio with max delay between
// deletes to be 2 seconds.
func (s *erasureSets) cleanupDeletedObjects(ctx context.Context) {
	timer := time.NewTimer(globalAPIConfig.getDeleteCleanupInterval())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			var wg sync.WaitGroup
			for _, set := range s.sets {
				wg.Add(1)
				go func(set *erasureObjects) {
					defer wg.Done()
					if set == nil {
						return
					}
					set.cleanupDeletedObjects(ctx)
				}(set)
			}
			wg.Wait()

			// Reset for the next interval
			timer.Reset(globalAPIConfig.getDeleteCleanupInterval())
		}
	}
}

func (s *erasureSets) cleanupStaleUploads(ctx context.Context) {
	timer := time.NewTimer(globalAPIConfig.getStaleUploadsCleanupInterval())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			var wg sync.WaitGroup
			for _, set := range s.sets {
				wg.Add(1)
				go func(set *erasureObjects) {
					defer wg.Done()
					if set == nil {
						return
					}
					set.cleanupStaleUploads(ctx)
				}(set)
			}
			wg.Wait()
		case <-staleUploadsCleanupIntervalChangedCh:
		}

		// Reset for the next interval
		timer.Reset(globalAPIConfig.getStaleUploadsCleanupInterval())
	}
}

type auditObjectOp struct {
	Name string `json:"name"`
	Pool int    `json:"poolId"`
	Set  int    `json:"setId"`
}

func (op auditObjectOp) String() string {
	// Flatten the auditObjectOp
	return fmt.Sprintf("name=%s,pool=%d,set=%d", op.Name, op.Pool, op.Set)
}

// Add erasure set information to the current context
func auditObjectErasureSet(ctx context.Context, api, object string, set *erasureObjects) {
	if len(logger.AuditTargets()) == 0 {
		return
	}

	op := auditObjectOp{
		Name: decodeDirObject(object),
		Pool: set.poolIndex + 1,
		Set:  set.setIndex + 1,
	}

	logger.GetReqInfo(ctx).AppendTags(api, op.String())
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (s *erasureSets) NewNSLock(bucket string, objects ...string) RWLocker {
	return s.sets[0].NewNSLock(bucket, objects...)
}

// SetDriveCount returns the current drives per set.
func (s *erasureSets) SetDriveCount() int {
	return s.setDriveCount
}

// ParityCount returns the default parity count used while erasure
// coding objects
func (s *erasureSets) ParityCount() int {
	return s.defaultParityCount
}

// StorageInfo - combines output of StorageInfo across all erasure coded object sets.
func (s *erasureSets) StorageInfo(ctx context.Context) StorageInfo {
	var storageInfo madmin.StorageInfo

	storageInfos := make([]madmin.StorageInfo, len(s.sets))

	g := errgroup.WithNErrs(len(s.sets))
	for index := range s.sets {
		g.Go(func() error {
			storageInfos[index] = s.sets[index].StorageInfo(ctx)
			return nil
		}, index)
	}

	// Wait for the go routines.
	g.Wait()

	for _, lstorageInfo := range storageInfos {
		storageInfo.Disks = append(storageInfo.Disks, lstorageInfo.Disks...)
	}

	return storageInfo
}

// StorageInfo - combines output of StorageInfo across all erasure coded object sets.
func (s *erasureSets) LocalStorageInfo(ctx context.Context, metrics bool) StorageInfo {
	var storageInfo StorageInfo

	storageInfos := make([]StorageInfo, len(s.sets))

	g := errgroup.WithNErrs(len(s.sets))
	for index := range s.sets {
		g.Go(func() error {
			storageInfos[index] = s.sets[index].LocalStorageInfo(ctx, metrics)
			return nil
		}, index)
	}

	// Wait for the go routines.
	g.Wait()

	for _, lstorageInfo := range storageInfos {
		storageInfo.Disks = append(storageInfo.Disks, lstorageInfo.Disks...)
	}

	return storageInfo
}

// Shutdown shutsdown all erasure coded sets in parallel
// returns error upon first error.
func (s *erasureSets) Shutdown(ctx context.Context) error {
	g := errgroup.WithNErrs(len(s.sets))

	for index := range s.sets {
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

// hashes the key returning an integer based on the input algorithm.
// This function currently supports
// - CRCMOD
// - SIPMOD
// - all new algos.
func sipHashMod(key string, cardinality int, id [16]byte) int {
	if cardinality <= 0 {
		return -1
	}
	// use the faster version as per siphash docs
	// https://github.com/dchest/siphash#usage
	k0, k1 := binary.LittleEndian.Uint64(id[0:8]), binary.LittleEndian.Uint64(id[8:16])
	sum64 := siphash.Hash(k0, k1, []byte(key))
	return int(sum64 % uint64(cardinality))
}

func crcHashMod(key string, cardinality int) int {
	if cardinality <= 0 {
		return -1
	}
	keyCrc := crc32.Checksum([]byte(key), crc32.IEEETable)
	return int(keyCrc % uint32(cardinality))
}

func hashKey(algo string, key string, cardinality int, id [16]byte) int {
	switch algo {
	case formatErasureVersionV2DistributionAlgoV1:
		return crcHashMod(key, cardinality)
	case formatErasureVersionV3DistributionAlgoV2, formatErasureVersionV3DistributionAlgoV3:
		return sipHashMod(key, cardinality, id)
	default:
		// Unknown algorithm returns -1, also if cardinality is lesser than 0.
		return -1
	}
}

// Returns always a same erasure coded set for a given input.
func (s *erasureSets) getHashedSetIndex(input string) int {
	return hashKey(s.distributionAlgo, input, len(s.sets), s.deploymentID)
}

// Returns always a same erasure coded set for a given input.
func (s *erasureSets) getHashedSet(input string) (set *erasureObjects) {
	return s.sets[s.getHashedSetIndex(input)]
}

// listDeletedBuckets lists deleted buckets from all disks.
func listDeletedBuckets(ctx context.Context, storageDisks []StorageAPI, delBuckets *xsync.MapOf[string, VolInfo], readQuorum int) error {
	g := errgroup.WithNErrs(len(storageDisks))
	for index := range storageDisks {
		g.Go(func() error {
			if storageDisks[index] == nil {
				// we ignore disk not found errors
				return nil
			}
			volsInfo, err := storageDisks[index].ListDir(ctx, "", minioMetaBucket, pathJoin(bucketMetaPrefix, deletedBucketsPrefix), -1)
			if err != nil {
				if errors.Is(err, errFileNotFound) {
					return nil
				}
				return err
			}
			for _, volName := range volsInfo {
				vi, err := storageDisks[index].StatVol(ctx, pathJoin(minioMetaBucket, bucketMetaPrefix, deletedBucketsPrefix, volName))
				if err == nil {
					vi.Name = strings.TrimSuffix(volName, SlashSeparator)
					delBuckets.Store(volName, vi)
				}
			}
			return nil
		}, index)
	}
	return reduceReadQuorumErrs(ctx, g.Wait(), bucketMetadataOpIgnoredErrs, readQuorum)
}

// --- Object Operations ---

// GetObjectNInfo - returns object info and locked object ReadCloser
func (s *erasureSets) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, err error) {
	set := s.getHashedSet(object)
	return set.GetObjectNInfo(ctx, bucket, object, rs, h, opts)
}

// PutObject - writes an object to hashedSet based on the object name.
func (s *erasureSets) PutObject(ctx context.Context, bucket string, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	set := s.getHashedSet(object)
	return set.PutObject(ctx, bucket, object, data, opts)
}

// GetObjectInfo - reads object metadata from the hashedSet based on the object name.
func (s *erasureSets) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	set := s.getHashedSet(object)
	return set.GetObjectInfo(ctx, bucket, object, opts)
}

func (s *erasureSets) deletePrefix(ctx context.Context, bucket string, prefix string) error {
	var wg sync.WaitGroup
	wg.Add(len(s.sets))
	for _, s := range s.sets {
		go func(s *erasureObjects) {
			defer wg.Done()
			// This is a force delete, no reason to throw errors.
			s.DeleteObject(ctx, bucket, prefix, ObjectOptions{DeletePrefix: true})
		}(s)
	}
	wg.Wait()
	return nil
}

// DeleteObject - deletes an object from the hashedSet based on the object name.
func (s *erasureSets) DeleteObject(ctx context.Context, bucket string, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if opts.DeletePrefix && !opts.DeletePrefixObject {
		err := s.deletePrefix(ctx, bucket, object)
		return ObjectInfo{}, err
	}
	set := s.getHashedSet(object)
	return set.DeleteObject(ctx, bucket, object, opts)
}

// DeleteObjects - bulk delete of objects
// Bulk delete is only possible within one set. For that purpose
// objects are group by set first, and then bulk delete is invoked
// for each set, the error response of each delete will be returned
func (s *erasureSets) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	type delObj struct {
		// Set index associated to this object
		setIndex int
		// Original index from the list of arguments
		// where this object is passed
		origIndex int
		// object to delete
		object ObjectToDelete
	}

	// Transform []delObj to the list of object names
	toNames := func(delObjs []delObj) []ObjectToDelete {
		objs := make([]ObjectToDelete, len(delObjs))
		for i, obj := range delObjs {
			objs[i] = obj.object
		}
		return objs
	}

	// The result of delete operation on all passed objects
	delErrs := make([]error, len(objects))

	// The result of delete objects
	delObjects := make([]DeletedObject, len(objects))

	// A map between a set and its associated objects
	objSetMap := make(map[int][]delObj)

	// Group objects by set index
	for i, object := range objects {
		index := s.getHashedSetIndex(object.ObjectName)
		objSetMap[index] = append(objSetMap[index], delObj{setIndex: index, origIndex: i, object: object})
	}

	// Invoke bulk delete on objects per set and save
	// the result of the delete operation
	var wg sync.WaitGroup
	var mu sync.Mutex
	wg.Add(len(objSetMap))
	for setIdx, objsGroup := range objSetMap {
		go func(set *erasureObjects, group []delObj) {
			defer wg.Done()
			dobjects, errs := set.DeleteObjects(ctx, bucket, toNames(group), opts)
			mu.Lock()
			defer mu.Unlock()
			for i, obj := range group {
				delErrs[obj.origIndex] = errs[i]
				delObjects[obj.origIndex] = dobjects[i]
			}
		}(s.sets[setIdx], objsGroup)
	}
	wg.Wait()

	return delObjects, delErrs
}

// CopyObject - copies objects from one hashedSet to another hashedSet, on server side.
func (s *erasureSets) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	srcSet := s.getHashedSet(srcObject)
	dstSet := s.getHashedSet(dstObject)

	cpSrcDstSame := srcSet == dstSet
	// Check if this request is only metadata update.
	if cpSrcDstSame && srcInfo.metadataOnly {
		// Version ID is set for the destination and source == destination version ID.
		// perform an in-place update.
		if dstOpts.VersionID != "" && srcOpts.VersionID == dstOpts.VersionID {
			srcInfo.Reader.Close() // We are not interested in the reader stream at this point close it.
			return srcSet.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		}
		// Destination is not versioned and source version ID is empty
		// perform an in-place update.
		if !dstOpts.Versioned && srcOpts.VersionID == "" {
			srcInfo.Reader.Close() // We are not interested in the reader stream at this point close it.
			return srcSet.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		}
		// CopyObject optimization where we don't create an entire copy
		// of the content, instead we add a reference, we disallow legacy
		// objects to be self referenced in this manner so make sure
		// that we actually create a new dataDir for legacy objects.
		if dstOpts.Versioned && srcOpts.VersionID != dstOpts.VersionID && !srcInfo.Legacy {
			srcInfo.versionOnly = true
			srcInfo.Reader.Close() // We are not interested in the reader stream at this point close it.
			return srcSet.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		}
	}

	putOpts := ObjectOptions{
		ServerSideEncryption:       dstOpts.ServerSideEncryption,
		UserDefined:                srcInfo.UserDefined,
		Versioned:                  dstOpts.Versioned,
		VersionID:                  dstOpts.VersionID,
		MTime:                      dstOpts.MTime,
		EncryptFn:                  dstOpts.EncryptFn,
		WantChecksum:               dstOpts.WantChecksum,
		WantServerSideChecksumType: dstOpts.WantServerSideChecksumType,
	}

	return dstSet.putObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, putOpts)
}

func (s *erasureSets) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	// In list multipart uploads we are going to treat input prefix as the object,
	// this means that we are not supporting directory navigation.
	set := s.getHashedSet(prefix)
	return set.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// Initiate a new multipart upload on a hashedSet based on object name.
func (s *erasureSets) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (res *NewMultipartUploadResult, err error) {
	set := s.getHashedSet(object)
	return set.NewMultipartUpload(ctx, bucket, object, opts)
}

// PutObjectPart - writes part of an object to hashedSet based on the object name.
func (s *erasureSets) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (info PartInfo, err error) {
	set := s.getHashedSet(object)
	return set.PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
}

// GetMultipartInfo - return multipart metadata info uploaded at hashedSet.
func (s *erasureSets) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (result MultipartInfo, err error) {
	set := s.getHashedSet(object)
	return set.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
}

// ListObjectParts - lists all uploaded parts to an object in hashedSet.
func (s *erasureSets) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (result ListPartsInfo, err error) {
	set := s.getHashedSet(object)
	return set.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
}

// Aborts an in-progress multipart operation on hashedSet based on the object name.
func (s *erasureSets) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) error {
	set := s.getHashedSet(object)
	return set.AbortMultipartUpload(ctx, bucket, object, uploadID, opts)
}

// CompleteMultipartUpload - completes a pending multipart transaction, on hashedSet based on object name.
func (s *erasureSets) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	set := s.getHashedSet(object)
	return set.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
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

func formatsToDrivesInfo(endpoints Endpoints, formats []*formatErasureV3, sErrs []error) (beforeDrives []madmin.HealDriveInfo) {
	beforeDrives = make([]madmin.HealDriveInfo, len(endpoints))
	// Existing formats are available (i.e. ok), so save it in
	// result, also populate disks to be healed.
	for i, format := range formats {
		drive := endpoints.GetString(i)
		state := madmin.DriveStateCorrupt
		switch {
		case format != nil:
			state = madmin.DriveStateOk
		case sErrs[i] == errUnformattedDisk:
			state = madmin.DriveStateMissing
		case sErrs[i] == errDiskNotFound:
			state = madmin.DriveStateOffline
		}
		beforeDrives[i] = madmin.HealDriveInfo{
			UUID: func() string {
				if format != nil {
					return format.Erasure.This
				}
				return ""
			}(),
			Endpoint: drive,
			State:    state,
		}
	}

	return beforeDrives
}

// HealFormat - heals missing `format.json` on fresh unformatted disks.
func (s *erasureSets) HealFormat(ctx context.Context, dryRun bool) (res madmin.HealResultItem, err error) {
	storageDisks, _ := initStorageDisksWithErrors(s.endpoints.Endpoints, storageOpts{
		cleanUp:     false,
		healthCheck: false,
	})

	defer func(storageDisks []StorageAPI) {
		if err != nil {
			closeStorageDisks(storageDisks...)
		}
	}(storageDisks)

	formats, sErrs := loadFormatErasureAll(storageDisks, true)
	if err = checkFormatErasureValues(formats, storageDisks, s.setDriveCount); err != nil {
		return madmin.HealResultItem{}, err
	}

	refFormat, err := getFormatErasureInQuorum(formats)
	if err != nil {
		return res, err
	}

	// Prepare heal-result
	res = madmin.HealResultItem{
		Type:      madmin.HealItemMetadata,
		Detail:    "disk-format",
		DiskCount: s.setCount * s.setDriveCount,
		SetCount:  s.setCount,
	}

	// Fetch all the drive info status.
	beforeDrives := formatsToDrivesInfo(s.endpoints.Endpoints, formats, sErrs)

	res.After.Drives = make([]madmin.HealDriveInfo, len(beforeDrives))
	res.Before.Drives = make([]madmin.HealDriveInfo, len(beforeDrives))
	// Copy "after" drive state too from before.
	for k, v := range beforeDrives {
		res.Before.Drives[k] = v
		res.After.Drives[k] = v
	}

	if countErrs(sErrs, errUnformattedDisk) == 0 {
		return res, errNoHealRequired
	}

	if !reflect.DeepEqual(s.format, refFormat) {
		// Format is corrupted and unrecognized by the running instance.
		healingLogIf(ctx, fmt.Errorf("Unable to heal the newly replaced drives due to format.json inconsistencies, please engage MinIO support for further assistance: %w",
			errCorruptedFormat))
		return res, errCorruptedFormat
	}

	formatOpID := mustGetUUID()

	// Initialize a new set of set formats which will be written to disk.
	newFormatSets, currentDisksInfo := newHealFormatSets(refFormat, s.setCount, s.setDriveCount, formats, sErrs)

	if !dryRun {
		tmpNewFormats := make([]*formatErasureV3, s.setCount*s.setDriveCount)
		for i := range newFormatSets {
			for j := range newFormatSets[i] {
				if newFormatSets[i][j] == nil {
					continue
				}
				res.After.Drives[i*s.setDriveCount+j].UUID = newFormatSets[i][j].Erasure.This
				res.After.Drives[i*s.setDriveCount+j].State = madmin.DriveStateOk
				tmpNewFormats[i*s.setDriveCount+j] = newFormatSets[i][j]
			}
		}

		// Save new formats `format.json` on unformatted disks.
		for index, format := range tmpNewFormats {
			if storageDisks[index] == nil || format == nil {
				continue
			}
			if err := saveFormatErasure(storageDisks[index], format, formatOpID); err != nil {
				healingLogIf(ctx, fmt.Errorf("Drive %s failed to write updated 'format.json': %v", storageDisks[index], err))
				storageDisks[index].Close()
				tmpNewFormats[index] = nil // this disk failed to write new format
			}
		}

		s.erasureDisksMu.Lock()

		for index, format := range tmpNewFormats {
			if format == nil {
				continue
			}

			m, n, err := findDiskIndexByDiskID(refFormat, format.Erasure.This)
			if err != nil {
				healingLogIf(ctx, err)
				continue
			}

			if s.erasureDisks[m][n] != nil {
				s.erasureDisks[m][n].Close()
			}

			if disk := storageDisks[index]; disk != nil {
				if disk.IsLocal() {
					xldisk, ok := disk.(*xlStorageDiskIDCheck)
					if ok {
						_, commonDeletes := calcCommonWritesDeletes(currentDisksInfo[m], (s.setDriveCount+1)/2)
						xldisk.totalDeletes.Store(commonDeletes)
						xldisk.storage.setDeleteAttribute(commonDeletes)

						if globalDriveMonitoring {
							go xldisk.monitorDiskWritable(xldisk.diskCtx)
						}
					}
				} else {
					disk.Close() // Close the remote storage client, re-initialize with healthchecks.
					disk, err = newStorageRESTClient(disk.Endpoint(), true, globalGrid.Load())
					if err != nil {
						continue
					}
				}

				s.erasureDisks[m][n] = disk

				if disk.IsLocal() {
					globalLocalDrivesMu.Lock()
					if globalIsDistErasure {
						globalLocalSetDrives[s.poolIndex][m][n] = disk
					}
					globalLocalDrivesMap[disk.Endpoint().String()] = disk
					globalLocalDrivesMu.Unlock()
				}
			}
		}

		s.erasureDisksMu.Unlock()
	}

	return res, nil
}

// HealObject - heals inconsistent object on a hashedSet based on object name.
func (s *erasureSets) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	return s.getHashedSet(object).HealObject(ctx, bucket, object, versionID, opts)
}

// PutObjectMetadata - replace or add metadata to an existing object/version
func (s *erasureSets) PutObjectMetadata(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	er := s.getHashedSet(object)
	return er.PutObjectMetadata(ctx, bucket, object, opts)
}

// DecomTieredObject - moves tiered object to another pool during decommissioning.
func (s *erasureSets) DecomTieredObject(ctx context.Context, bucket, object string, fi FileInfo, opts ObjectOptions) error {
	er := s.getHashedSet(object)
	return er.DecomTieredObject(ctx, bucket, object, fi, opts)
}

// PutObjectTags - replace or add tags to an existing object
func (s *erasureSets) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) (ObjectInfo, error) {
	er := s.getHashedSet(object)
	return er.PutObjectTags(ctx, bucket, object, tags, opts)
}

// DeleteObjectTags - delete object tags from an existing object
func (s *erasureSets) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	er := s.getHashedSet(object)
	return er.DeleteObjectTags(ctx, bucket, object, opts)
}

// GetObjectTags - get object tags from an existing object
func (s *erasureSets) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	er := s.getHashedSet(object)
	return er.GetObjectTags(ctx, bucket, object, opts)
}

// TransitionObject - transition object content to target tier.
func (s *erasureSets) TransitionObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return s.getHashedSet(object).TransitionObject(ctx, bucket, object, opts)
}

// RestoreTransitionedObject - restore transitioned object content locally on this cluster.
func (s *erasureSets) RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return s.getHashedSet(object).RestoreTransitionedObject(ctx, bucket, object, opts)
}

// CheckAbandonedParts - check object for abandoned parts.
func (s *erasureSets) CheckAbandonedParts(ctx context.Context, bucket, object string, opts madmin.HealOpts) error {
	return s.getHashedSet(object).checkAbandonedParts(ctx, bucket, object, opts)
}
