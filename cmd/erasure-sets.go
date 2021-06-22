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
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/dchest/siphash"
	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/bpool"
	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/env"
)

// setsDsyncLockers is encapsulated type for Close()
type setsDsyncLockers [][]dsync.NetLocker

const envMinioDeleteCleanupInterval = "MINIO_DELETE_CLEANUP_INTERVAL"

// erasureSets implements ObjectLayer combining a static list of erasure coded
// object sets. NOTE: There is no dynamic scaling allowed or intended in
// current design.
type erasureSets struct {
	GatewayUnsupported

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
	endpoints Endpoints

	// String version of all the endpoints, an optimization
	// to avoid url.String() conversion taking CPU on
	// large disk setups.
	endpointStrings []string

	// Total number of sets and the number of disks per set.
	setCount, setDriveCount int
	defaultParityCount      int

	poolIndex int

	// A channel to send the set index to the MRF when
	// any disk belonging to that set is connected
	setReconnectEvent chan int

	// Distribution algorithm of choice.
	distributionAlgo string
	deploymentID     [16]byte

	disksStorageInfoCache timedValue

	mrfMU                  sync.Mutex
	mrfOperations          map[healSource]int
	lastConnectDisksOpTime time.Time
}

// Return false if endpoint is not connected or has been reconnected after last check
func isEndpointConnectionStable(diskMap map[Endpoint]StorageAPI, endpoint Endpoint, lastCheck time.Time) bool {
	disk := diskMap[endpoint]
	if disk == nil {
		return false
	}
	if !disk.IsOnline() {
		return false
	}
	if disk.LastConn().After(lastCheck) {
		return false
	}
	return true
}

func (s *erasureSets) getDiskMap() map[Endpoint]StorageAPI {
	diskMap := make(map[Endpoint]StorageAPI)

	s.erasureDisksMu.RLock()
	defer s.erasureDisksMu.RUnlock()

	for i := 0; i < s.setCount; i++ {
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
	disk, err := newStorageAPIWithoutHealthCheck(endpoint)
	if err != nil {
		return nil, nil, err
	}

	format, err := loadFormatErasure(disk)
	if err != nil {
		if errors.Is(err, errUnformattedDisk) {
			info, derr := disk.DiskInfo(context.TODO())
			if derr != nil && info.RootDisk {
				return nil, nil, fmt.Errorf("Disk: %s returned %w", disk, derr) // make sure to '%w' to wrap the error
			}
		}
		return nil, nil, fmt.Errorf("Disk: %s returned %w", disk, err) // make sure to '%w' to wrap the error
	}

	return disk, format, nil
}

// findDiskIndex - returns the i,j'th position of the input `diskID` against the reference
// format, after successful validation.
//   - i'th position is the set index
//   - j'th position is the disk index in the current set
func findDiskIndexByDiskID(refFormat *formatErasureV3, diskID string) (int, int, error) {
	if diskID == offlineDiskUUID {
		return -1, -1, fmt.Errorf("diskID: %s is offline", diskID)
	}
	for i := 0; i < len(refFormat.Erasure.Sets); i++ {
		for j := 0; j < len(refFormat.Erasure.Sets[0]); j++ {
			if refFormat.Erasure.Sets[i][j] == diskID {
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
func findDiskIndex(refFormat, format *formatErasureV3) (int, int, error) {
	if err := formatErasureV3Check(refFormat, format); err != nil {
		return 0, 0, err
	}

	if format.Erasure.This == offlineDiskUUID {
		return -1, -1, fmt.Errorf("diskID: %s is offline", format.Erasure.This)
	}

	for i := 0; i < len(refFormat.Erasure.Sets); i++ {
		for j := 0; j < len(refFormat.Erasure.Sets[0]); j++ {
			if refFormat.Erasure.Sets[i][j] == format.Erasure.This {
				return i, j, nil
			}
		}
	}

	return -1, -1, fmt.Errorf("diskID: %s not found", format.Erasure.This)
}

// connectDisks - attempt to connect all the endpoints, loads format
// and re-arranges the disks in proper position.
func (s *erasureSets) connectDisks() {
	defer func() {
		s.lastConnectDisksOpTime = time.Now()
	}()

	var wg sync.WaitGroup
	var setsJustConnected = make([]bool, s.setCount)
	diskMap := s.getDiskMap()
	for _, endpoint := range s.endpoints {
		if isEndpointConnectionStable(diskMap, endpoint, s.lastConnectDisksOpTime) {
			continue
		}
		wg.Add(1)
		go func(endpoint Endpoint) {
			defer wg.Done()
			disk, format, err := connectEndpoint(endpoint)
			if err != nil {
				if endpoint.IsLocal && errors.Is(err, errUnformattedDisk) {
					globalBackgroundHealState.pushHealLocalDisks(endpoint)
					logger.Info(fmt.Sprintf("Found unformatted drive %s, attempting to heal...", endpoint))
				} else {
					printEndpointError(endpoint, err, true)
				}
				return
			}
			if disk.IsLocal() && disk.Healing() != nil {
				globalBackgroundHealState.pushHealLocalDisks(disk.Endpoint())
				logger.Info(fmt.Sprintf("Found the drive %s that needs healing, attempting to heal...", disk))
			}
			s.erasureDisksMu.RLock()
			setIndex, diskIndex, err := findDiskIndex(s.format, format)
			s.erasureDisksMu.RUnlock()
			if err != nil {
				printEndpointError(endpoint, err, false)
				return
			}

			s.erasureDisksMu.Lock()
			if s.erasureDisks[setIndex][diskIndex] != nil {
				s.erasureDisks[setIndex][diskIndex].Close()
			}
			if disk.IsLocal() {
				disk.SetDiskID(format.Erasure.This)
				s.erasureDisks[setIndex][diskIndex] = disk
			} else {
				// Enable healthcheck disk for remote endpoint.
				disk, err = newStorageAPI(endpoint)
				if err != nil {
					printEndpointError(endpoint, err, false)
					return
				}
				disk.SetDiskID(format.Erasure.This)
				s.erasureDisks[setIndex][diskIndex] = disk
			}
			disk.SetDiskLoc(s.poolIndex, setIndex, diskIndex)
			s.endpointStrings[setIndex*s.setDriveCount+diskIndex] = disk.String()
			setsJustConnected[setIndex] = true
			s.erasureDisksMu.Unlock()
		}(endpoint)
	}

	wg.Wait()

	go func() {
		idler := time.NewTimer(100 * time.Millisecond)
		defer idler.Stop()

		for setIndex, justConnected := range setsJustConnected {
			if !justConnected {
				continue
			}

			// Send a new set connect event with a timeout
			idler.Reset(100 * time.Millisecond)
			select {
			case s.setReconnectEvent <- setIndex:
			case <-idler.C:
			}
		}
	}()
}

// monitorAndConnectEndpoints this is a monitoring loop to keep track of disconnected
// endpoints by reconnecting them and making sure to place them into right position in
// the set topology, this monitoring happens at a given monitoring interval.
func (s *erasureSets) monitorAndConnectEndpoints(ctx context.Context, monitorInterval time.Duration) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	time.Sleep(time.Duration(r.Float64() * float64(time.Second)))

	// Pre-emptively connect the disks if possible.
	s.connectDisks()

	monitor := time.NewTimer(monitorInterval)
	defer monitor.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-monitor.C:
			// Reset the timer once fired for required interval.
			monitor.Reset(monitorInterval)

			if serverDebugLog {
				console.Debugln("running disk monitoring")
			}

			s.connectDisks()
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

func (s *erasureSets) GetEndpoints(setIndex int) func() []string {
	return func() []string {
		s.erasureDisksMu.RLock()
		defer s.erasureDisksMu.RUnlock()

		eps := make([]string, s.setDriveCount)
		for i := 0; i < s.setDriveCount; i++ {
			eps[i] = s.endpointStrings[setIndex*s.setDriveCount+i]
		}
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
func newErasureSets(ctx context.Context, endpoints Endpoints, storageDisks []StorageAPI, format *formatErasureV3, defaultParityCount, poolIdx int) (*erasureSets, error) {
	setCount := len(format.Erasure.Sets)
	setDriveCount := len(format.Erasure.Sets[0])

	endpointStrings := make([]string, len(endpoints))

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
		setReconnectEvent:  make(chan int),
		distributionAlgo:   format.Erasure.DistributionAlgo,
		deploymentID:       uuid.MustParse(format.ID),
		mrfOperations:      make(map[healSource]int),
		poolIndex:          poolIdx,
	}

	mutex := newNSLock(globalIsDistErasure)

	// Number of buffers, max 2GB
	n := (2 * humanize.GiByte) / (blockSizeV2 * 2)

	// Initialize byte pool once for all sets, bpool size is set to
	// setCount * setDriveCount with each memory upto blockSizeV2.
	bp := bpool.NewBytePoolCap(n, blockSizeV2, blockSizeV2*2)

	// Initialize byte pool for all sets, bpool size is set to
	// setCount * setDriveCount with each memory upto blockSizeV1
	//
	// Number of buffers, max 10GiB
	m := (10 * humanize.GiByte) / (blockSizeV1 * 2)

	bpOld := bpool.NewBytePoolCap(m, blockSizeV1, blockSizeV1*2)

	for i := 0; i < setCount; i++ {
		s.erasureDisks[i] = make([]StorageAPI, setDriveCount)
	}

	var erasureLockers = map[string]dsync.NetLocker{}
	for _, endpoint := range endpoints {
		if _, ok := erasureLockers[endpoint.Host]; !ok {
			erasureLockers[endpoint.Host] = newLockAPI(endpoint)
		}
	}

	for i := 0; i < setCount; i++ {
		var lockerEpSet = set.NewStringSet()
		for j := 0; j < setDriveCount; j++ {
			endpoint := endpoints[i*setDriveCount+j]
			// Only add lockers only one per endpoint and per erasure set.
			if locker, ok := erasureLockers[endpoint.Host]; ok && !lockerEpSet.Contains(endpoint.Host) {
				lockerEpSet.Add(endpoint.Host)
				s.erasureLockers[i] = append(s.erasureLockers[i], locker)
			}
			disk := storageDisks[i*setDriveCount+j]
			if disk == nil {
				continue
			}
			diskID, derr := disk.GetDiskID()
			if derr != nil {
				continue
			}
			m, n, err := findDiskIndexByDiskID(format, diskID)
			if err != nil {
				continue
			}
			disk.SetDiskLoc(s.poolIndex, m, n)
			s.endpointStrings[m*setDriveCount+n] = disk.String()
			s.erasureDisks[m][n] = disk
		}

		// Initialize erasure objects for a given set.
		s.sets[i] = &erasureObjects{
			setIndex:              i,
			poolIndex:             poolIdx,
			setDriveCount:         setDriveCount,
			defaultParityCount:    defaultParityCount,
			getDisks:              s.GetDisks(i),
			getLockers:            s.GetLockers(i),
			getEndpoints:          s.GetEndpoints(i),
			deletedCleanupSleeper: newDynamicSleeper(10, 2*time.Second),
			nsMutex:               mutex,
			bp:                    bp,
			bpOld:                 bpOld,
			mrfOpCh:               make(chan partialOperation, 10000),
		}
	}

	// cleanup ".trash/" folder every 5m minutes with sufficient sleep cycles, between each
	// deletes a dynamic sleeper is used with a factor of 10 ratio with max delay between
	// deletes to be 2 seconds.
	deletedObjectsCleanupInterval, err := time.ParseDuration(env.Get(envMinioDeleteCleanupInterval, "5m"))
	if err != nil {
		return nil, err
	}

	// start cleanup stale uploads go-routine.
	go s.cleanupStaleUploads(ctx, GlobalStaleUploadsCleanupInterval, GlobalStaleUploadsExpiry)

	// start cleanup of deleted objects.
	go s.cleanupDeletedObjects(ctx, deletedObjectsCleanupInterval)

	// Start the disk monitoring and connect routine.
	go s.monitorAndConnectEndpoints(ctx, defaultMonitorConnectEndpointInterval)
	go s.maintainMRFList()
	go s.healMRFRoutine()

	return s, nil
}

func (s *erasureSets) cleanupDeletedObjects(ctx context.Context, cleanupInterval time.Duration) {
	timer := time.NewTimer(cleanupInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// Reset for the next interval
			timer.Reset(cleanupInterval)

			for _, set := range s.sets {
				set.cleanupDeletedObjects(ctx)
			}
		}
	}
}

func (s *erasureSets) cleanupStaleUploads(ctx context.Context, cleanupInterval, expiry time.Duration) {
	timer := time.NewTimer(cleanupInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// Reset for the next interval
			timer.Reset(cleanupInterval)

			for _, set := range s.sets {
				set.cleanupStaleUploads(ctx, expiry)
			}
		}
	}
}

const objectErasureMapKey = "objectErasureMap"

type auditObjectOp struct {
	Pool  int      `json:"poolId"`
	Set   int      `json:"setId"`
	Disks []string `json:"disks"`
}

type auditObjectErasureMap struct {
	sync.Map
}

// Define how to marshal auditObjectErasureMap so it can be
// printed in the audit webhook notification request.
func (a *auditObjectErasureMap) MarshalJSON() ([]byte, error) {
	mapCopy := make(map[string]auditObjectOp)
	a.Range(func(k, v interface{}) bool {
		mapCopy[k.(string)] = v.(auditObjectOp)
		return true
	})
	return json.Marshal(mapCopy)
}

func auditObjectErasureSet(ctx context.Context, object string, set *erasureObjects) {
	if len(logger.AuditTargets) == 0 {
		return
	}

	object = decodeDirObject(object)

	op := auditObjectOp{
		Pool:  set.poolIndex + 1,
		Set:   set.setIndex + 1,
		Disks: set.getEndpoints(),
	}

	var objectErasureSetTag *auditObjectErasureMap
	reqInfo := logger.GetReqInfo(ctx)
	for _, kv := range reqInfo.GetTags() {
		if kv.Key == objectErasureMapKey {
			objectErasureSetTag = kv.Val.(*auditObjectErasureMap)
			break
		}
	}

	if objectErasureSetTag == nil {
		objectErasureSetTag = &auditObjectErasureMap{}
	}

	objectErasureSetTag.Store(object, op)
	reqInfo.SetTags(objectErasureMapKey, objectErasureSetTag)
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (s *erasureSets) NewNSLock(bucket string, objects ...string) RWLocker {
	if len(objects) == 1 {
		return s.getHashedSet(objects[0]).NewNSLock(bucket, objects...)
	}
	return s.getHashedSet("").NewNSLock(bucket, objects...)
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

// StorageUsageInfo - combines output of StorageInfo across all erasure coded object sets.
// This only returns disk usage info for ServerPools to perform placement decision, this call
// is not implemented in Object interface and is not meant to be used by other object
// layer implementations.
func (s *erasureSets) StorageUsageInfo(ctx context.Context) StorageInfo {
	storageUsageInfo := func() StorageInfo {
		var storageInfo StorageInfo
		storageInfos := make([]StorageInfo, len(s.sets))
		storageInfo.Backend.Type = madmin.Erasure

		g := errgroup.WithNErrs(len(s.sets))
		for index := range s.sets {
			index := index
			g.Go(func() error {
				// ignoring errors on purpose
				storageInfos[index], _ = s.sets[index].StorageInfo(ctx)
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

	s.disksStorageInfoCache.Once.Do(func() {
		s.disksStorageInfoCache.TTL = time.Second
		s.disksStorageInfoCache.Update = func() (interface{}, error) {
			return storageUsageInfo(), nil
		}
	})

	v, _ := s.disksStorageInfoCache.Get()
	return v.(StorageInfo)
}

// StorageInfo - combines output of StorageInfo across all erasure coded object sets.
func (s *erasureSets) StorageInfo(ctx context.Context) (StorageInfo, []error) {
	var storageInfo madmin.StorageInfo

	storageInfos := make([]madmin.StorageInfo, len(s.sets))
	storageInfoErrs := make([][]error, len(s.sets))

	g := errgroup.WithNErrs(len(s.sets))
	for index := range s.sets {
		index := index
		g.Go(func() error {
			storageInfos[index], storageInfoErrs[index] = s.sets[index].StorageInfo(ctx)
			return nil
		}, index)
	}

	// Wait for the go routines.
	g.Wait()

	for _, lstorageInfo := range storageInfos {
		storageInfo.Disks = append(storageInfo.Disks, lstorageInfo.Disks...)
	}

	errs := make([]error, 0, len(s.sets)*s.setDriveCount)
	for i := range s.sets {
		errs = append(errs, storageInfoErrs[i]...)
	}

	return storageInfo, errs
}

// StorageInfo - combines output of StorageInfo across all erasure coded object sets.
func (s *erasureSets) LocalStorageInfo(ctx context.Context) (StorageInfo, []error) {
	var storageInfo StorageInfo

	storageInfos := make([]StorageInfo, len(s.sets))
	storageInfoErrs := make([][]error, len(s.sets))

	g := errgroup.WithNErrs(len(s.sets))
	for index := range s.sets {
		index := index
		g.Go(func() error {
			storageInfos[index], storageInfoErrs[index] = s.sets[index].LocalStorageInfo(ctx)
			return nil
		}, index)
	}

	// Wait for the go routines.
	g.Wait()

	for _, lstorageInfo := range storageInfos {
		storageInfo.Disks = append(storageInfo.Disks, lstorageInfo.Disks...)
	}

	var errs []error
	for i := range s.sets {
		errs = append(errs, storageInfoErrs[i]...)
	}

	return storageInfo, errs
}

// Shutdown shutsdown all erasure coded sets in parallel
// returns error upon first error.
func (s *erasureSets) Shutdown(ctx context.Context) error {
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
	select {
	case _, ok := <-s.setReconnectEvent:
		if ok {
			close(s.setReconnectEvent)
		}
	default:
		close(s.setReconnectEvent)
	}
	return nil
}

// MakeBucketLocation - creates a new bucket across all sets simultaneously,
// then return the first encountered error
func (s *erasureSets) MakeBucketWithLocation(ctx context.Context, bucket string, opts BucketOptions) error {
	g := errgroup.WithNErrs(len(s.sets))

	// Create buckets in parallel across all sets.
	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].MakeBucketWithLocation(ctx, bucket, opts)
		}, index)
	}

	errs := g.Wait()

	// Return the first encountered error
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	// Success.
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

// GetBucketInfo - returns bucket info from one of the erasure coded set.
func (s *erasureSets) GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error) {
	return s.getHashedSet("").GetBucketInfo(ctx, bucket)
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (s *erasureSets) IsNotificationSupported() bool {
	return s.getHashedSet("").IsNotificationSupported()
}

// IsListenSupported returns whether listen bucket notification is applicable for this layer.
func (s *erasureSets) IsListenSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (s *erasureSets) IsEncryptionSupported() bool {
	return s.getHashedSet("").IsEncryptionSupported()
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (s *erasureSets) IsCompressionSupported() bool {
	return s.getHashedSet("").IsCompressionSupported()
}

func (s *erasureSets) IsTaggingSupported() bool {
	return true
}

// DeleteBucket - deletes a bucket on all sets simultaneously,
// even if one of the sets fail to delete buckets, we proceed to
// undo a successful operation.
func (s *erasureSets) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	g := errgroup.WithNErrs(len(s.sets))

	// Delete buckets in parallel across all sets.
	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].DeleteBucket(ctx, bucket, forceDelete)
		}, index)
	}

	errs := g.Wait()
	// For any failure, we attempt undo all the delete buckets operation
	// by creating buckets again on all sets which were successfully deleted.
	for _, err := range errs {
		if err != nil {
			undoDeleteBucketSets(ctx, bucket, s.sets, errs)
			return err
		}
	}

	// Success.
	return nil
}

// This function is used to undo a successful DeleteBucket operation.
func undoDeleteBucketSets(ctx context.Context, bucket string, sets []*erasureObjects, errs []error) {
	g := errgroup.WithNErrs(len(sets))

	// Undo previous delete bucket on all underlying sets.
	for index := range sets {
		index := index
		g.Go(func() error {
			if errs[index] == nil {
				return sets[index].MakeBucketWithLocation(ctx, bucket, BucketOptions{})
			}
			return nil
		}, index)
	}

	g.Wait()
}

// List all buckets from one of the set, we are not doing merge
// sort here just for simplification. As per design it is assumed
// that all buckets are present on all sets.
func (s *erasureSets) ListBuckets(ctx context.Context) (buckets []BucketInfo, err error) {
	var listBuckets []BucketInfo
	var healBuckets = map[string]VolInfo{}
	for _, set := range s.sets {
		// lists all unique buckets across drives.
		if err := listAllBuckets(ctx, set.getDisks(), healBuckets); err != nil {
			return nil, err
		}
	}

	for _, v := range healBuckets {
		listBuckets = append(listBuckets, BucketInfo(v))
	}

	sort.Slice(listBuckets, func(i, j int) bool {
		return listBuckets[i].Name < listBuckets[j].Name
	})

	return listBuckets, nil
}

// --- Object Operations ---

// GetObjectNInfo - returns object info and locked object ReadCloser
func (s *erasureSets) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	set := s.getHashedSet(object)
	auditObjectErasureSet(ctx, object, set)
	return set.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
}

func (s *erasureSets) parentDirIsObject(ctx context.Context, bucket, parent string) bool {
	if parent == "." {
		return false
	}
	return s.getHashedSet(parent).parentDirIsObject(ctx, bucket, parent)
}

// PutObject - writes an object to hashedSet based on the object name.
func (s *erasureSets) PutObject(ctx context.Context, bucket string, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	set := s.getHashedSet(object)
	auditObjectErasureSet(ctx, object, set)
	opts.ParentIsObject = s.parentDirIsObject
	return set.PutObject(ctx, bucket, object, data, opts)
}

// GetObjectInfo - reads object metadata from the hashedSet based on the object name.
func (s *erasureSets) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	set := s.getHashedSet(object)
	auditObjectErasureSet(ctx, object, set)
	return set.GetObjectInfo(ctx, bucket, object, opts)
}

func (s *erasureSets) deletePrefix(ctx context.Context, bucket string, prefix string) error {
	for _, s := range s.sets {
		_, err := s.DeleteObject(ctx, bucket, prefix, ObjectOptions{DeletePrefix: true})
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteObject - deletes an object from the hashedSet based on the object name.
func (s *erasureSets) DeleteObject(ctx context.Context, bucket string, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	set := s.getHashedSet(object)
	auditObjectErasureSet(ctx, object, set)

	if opts.DeletePrefix {
		err := s.deletePrefix(ctx, bucket, object)
		return ObjectInfo{}, err
	}
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
	var delErrs = make([]error, len(objects))

	// The result of delete objects
	var delObjects = make([]DeletedObject, len(objects))

	// A map between a set and its associated objects
	var objSetMap = make(map[int][]delObj)

	// Group objects by set index
	for i, object := range objects {
		index := s.getHashedSetIndex(object.ObjectName)
		objSetMap[index] = append(objSetMap[index], delObj{setIndex: index, origIndex: i, object: object})
	}

	// Invoke bulk delete on objects per set and save
	// the result of the delete operation
	for _, objsGroup := range objSetMap {
		set := s.getHashedSet(objsGroup[0].object.ObjectName)
		dobjects, errs := set.DeleteObjects(ctx, bucket, toNames(objsGroup), opts)
		for i, obj := range objsGroup {
			delErrs[obj.origIndex] = errs[i]
			delObjects[obj.origIndex] = dobjects[i]
			if errs[i] == nil {
				auditObjectErasureSet(ctx, obj.object.ObjectName, set)
			}
		}
	}

	return delObjects, delErrs
}

// CopyObject - copies objects from one hashedSet to another hashedSet, on server side.
func (s *erasureSets) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (objInfo ObjectInfo, err error) {
	srcSet := s.getHashedSet(srcObject)
	dstSet := s.getHashedSet(dstObject)

	auditObjectErasureSet(ctx, dstObject, dstSet)

	cpSrcDstSame := srcSet == dstSet
	// Check if this request is only metadata update.
	if cpSrcDstSame && srcInfo.metadataOnly {
		// Version ID is set for the destination and source == destination version ID.
		// perform an in-place update.
		if dstOpts.VersionID != "" && srcOpts.VersionID == dstOpts.VersionID {
			return srcSet.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		}
		// Destination is not versioned and source version ID is empty
		// perform an in-place update.
		if !dstOpts.Versioned && srcOpts.VersionID == "" {
			return srcSet.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		}
		// CopyObject optimization where we don't create an entire copy
		// of the content, instead we add a reference, we disallow legacy
		// objects to be self referenced in this manner so make sure
		// that we actually create a new dataDir for legacy objects.
		if dstOpts.Versioned && srcOpts.VersionID != dstOpts.VersionID && !srcInfo.Legacy {
			srcInfo.versionOnly = true
			return srcSet.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		}
	}

	putOpts := ObjectOptions{
		ServerSideEncryption: dstOpts.ServerSideEncryption,
		UserDefined:          srcInfo.UserDefined,
		Versioned:            dstOpts.Versioned,
		VersionID:            dstOpts.VersionID,
		MTime:                dstOpts.MTime,
	}

	return dstSet.putObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, putOpts)
}

func (s *erasureSets) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	// In list multipart uploads we are going to treat input prefix as the object,
	// this means that we are not supporting directory navigation.
	set := s.getHashedSet(prefix)
	auditObjectErasureSet(ctx, prefix, set)
	return set.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// Initiate a new multipart upload on a hashedSet based on object name.
func (s *erasureSets) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (uploadID string, err error) {
	set := s.getHashedSet(object)
	auditObjectErasureSet(ctx, object, set)
	return set.NewMultipartUpload(ctx, bucket, object, opts)
}

// Copies a part of an object from source hashedSet to destination hashedSet.
func (s *erasureSets) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int,
	startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (partInfo PartInfo, err error) {
	destSet := s.getHashedSet(destObject)
	auditObjectErasureSet(ctx, destObject, destSet)
	return destSet.PutObjectPart(ctx, destBucket, destObject, uploadID, partID, NewPutObjReader(srcInfo.Reader), dstOpts)
}

// PutObjectPart - writes part of an object to hashedSet based on the object name.
func (s *erasureSets) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (info PartInfo, err error) {
	set := s.getHashedSet(object)
	auditObjectErasureSet(ctx, object, set)
	return set.PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
}

// GetMultipartInfo - return multipart metadata info uploaded at hashedSet.
func (s *erasureSets) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (result MultipartInfo, err error) {
	set := s.getHashedSet(object)
	auditObjectErasureSet(ctx, object, set)
	return set.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
}

// ListObjectParts - lists all uploaded parts to an object in hashedSet.
func (s *erasureSets) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (result ListPartsInfo, err error) {
	set := s.getHashedSet(object)
	auditObjectErasureSet(ctx, object, set)
	return set.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
}

// Aborts an in-progress multipart operation on hashedSet based on the object name.
func (s *erasureSets) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) error {
	set := s.getHashedSet(object)
	auditObjectErasureSet(ctx, object, set)
	return set.AbortMultipartUpload(ctx, bucket, object, uploadID, opts)
}

// CompleteMultipartUpload - completes a pending multipart transaction, on hashedSet based on object name.
func (s *erasureSets) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	set := s.getHashedSet(object)
	auditObjectErasureSet(ctx, object, set)
	opts.ParentIsObject = s.parentDirIsObject
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
		var state = madmin.DriveStateCorrupt
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

// If it is a single node Erasure and all disks are root disks, it is most likely a test setup, else it is a production setup.
// On a test setup we allow creation of format.json on root disks to help with dev/testing.
func isTestSetup(infos []DiskInfo, errs []error) bool {
	rootDiskCount := 0
	for i := range errs {
		if errs[i] == nil || errs[i] == errUnformattedDisk {
			if infos[i].RootDisk {
				rootDiskCount++
			}
		}
	}
	// It is a test setup if all disks are root disks in quorum.
	return rootDiskCount >= len(infos)/2+1
}

func getHealDiskInfos(storageDisks []StorageAPI, errs []error) ([]DiskInfo, []error) {
	infos := make([]DiskInfo, len(storageDisks))
	g := errgroup.WithNErrs(len(storageDisks))
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if errs[index] != nil && errs[index] != errUnformattedDisk {
				return errs[index]
			}
			if storageDisks[index] == nil {
				return errDiskNotFound
			}
			var err error
			infos[index], err = storageDisks[index].DiskInfo(context.TODO())
			return err
		}, index)
	}
	return infos, g.Wait()
}

// Mark root disks as down so as not to heal them.
func markRootDisksAsDown(storageDisks []StorageAPI, errs []error) {
	var infos []DiskInfo
	infos, errs = getHealDiskInfos(storageDisks, errs)
	if !isTestSetup(infos, errs) {
		for i := range storageDisks {
			if storageDisks[i] != nil && infos[i].RootDisk {
				// We should not heal on root disk. i.e in a situation where the minio-administrator has unmounted a
				// defective drive we should not heal a path on the root disk.
				logger.Info("Disk `%s` the same as the system root disk.\n"+
					"Disk will not be used. Please supply a separate disk and restart the server.",
					storageDisks[i].String())
				storageDisks[i] = nil
			}
		}
	}
}

// HealFormat - heals missing `format.json` on fresh unformatted disks.
func (s *erasureSets) HealFormat(ctx context.Context, dryRun bool) (res madmin.HealResultItem, err error) {
	storageDisks, errs := initStorageDisksWithErrorsWithoutHealthCheck(s.endpoints)
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

	formats, sErrs := loadFormatErasureAll(storageDisks, true)
	if err = checkFormatErasureValues(formats, storageDisks, s.setDriveCount); err != nil {
		return madmin.HealResultItem{}, err
	}

	// Mark all root disks down
	markRootDisksAsDown(storageDisks, sErrs)

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
	beforeDrives := formatsToDrivesInfo(s.endpoints, formats, sErrs)

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

	// Initialize a new set of set formats which will be written to disk.
	newFormatSets := newHealFormatSets(refFormat, s.setCount, s.setDriveCount, formats, sErrs)

	if !dryRun {
		var tmpNewFormats = make([]*formatErasureV3, s.setCount*s.setDriveCount)
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
		if err = saveUnformattedFormat(ctx, storageDisks, tmpNewFormats); err != nil {
			return madmin.HealResultItem{}, err
		}

		s.erasureDisksMu.Lock()

		for index, format := range tmpNewFormats {
			if format == nil {
				continue
			}

			m, n, err := findDiskIndexByDiskID(refFormat, format.Erasure.This)
			if err != nil {
				continue
			}

			if s.erasureDisks[m][n] != nil {
				s.erasureDisks[m][n].Close()
			}
			storageDisks[index].SetDiskLoc(s.poolIndex, m, n)
			s.erasureDisks[m][n] = storageDisks[index]
			s.endpointStrings[m*s.setDriveCount+n] = storageDisks[index].String()
		}

		// Replace reference format with what was loaded from disks.
		s.format = refFormat

		s.erasureDisksMu.Unlock()
	}

	return res, nil
}

// HealBucket - heals inconsistent buckets and bucket metadata on all sets.
func (s *erasureSets) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (result madmin.HealResultItem, err error) {
	// Initialize heal result info
	result = madmin.HealResultItem{
		Type:      madmin.HealItemBucket,
		Bucket:    bucket,
		DiskCount: s.setCount * s.setDriveCount,
		SetCount:  s.setCount,
	}

	for _, set := range s.sets {
		var healResult madmin.HealResultItem
		healResult, err = set.HealBucket(ctx, bucket, opts)
		if err != nil {
			return result, toObjectErr(err, bucket)
		}
		result.Before.Drives = append(result.Before.Drives, healResult.Before.Drives...)
		result.After.Drives = append(result.After.Drives, healResult.After.Drives...)
	}

	// Check if we had quorum to write, if not return an appropriate error.
	_, afterDriveOnline := result.GetOnlineCounts()
	if afterDriveOnline < ((s.setCount*s.setDriveCount)/2)+1 {
		return result, toObjectErr(errErasureWriteQuorum, bucket)
	}

	return result, nil
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

// maintainMRFList gathers the list of successful partial uploads
// from all underlying er.sets and puts them in a global map which
// should not have more than 10000 entries.
func (s *erasureSets) maintainMRFList() {
	var agg = make(chan partialOperation, 10000)
	for i, er := range s.sets {
		go func(c <-chan partialOperation, setIndex int) {
			for msg := range c {
				msg.failedSet = setIndex
				select {
				case agg <- msg:
				default:
				}
			}
		}(er.mrfOpCh, i)
	}

	for fOp := range agg {
		s.mrfMU.Lock()
		if len(s.mrfOperations) > 10000 {
			s.mrfMU.Unlock()
			continue
		}
		s.mrfOperations[healSource{
			bucket:    fOp.bucket,
			object:    fOp.object,
			versionID: fOp.versionID,
			opts:      &madmin.HealOpts{Remove: true},
		}] = fOp.failedSet
		s.mrfMU.Unlock()
	}
}

// healMRFRoutine monitors new disks connection, sweep the MRF list
// to find objects related to the new disk that needs to be healed.
func (s *erasureSets) healMRFRoutine() {
	// Wait until background heal state is initialized
	bgSeq := mustGetHealSequence(GlobalContext)

	for setIndex := range s.setReconnectEvent {
		// Get the list of objects related the er.set
		// to which the connected disk belongs.
		var mrfOperations []healSource
		s.mrfMU.Lock()
		for k, v := range s.mrfOperations {
			if v == setIndex {
				mrfOperations = append(mrfOperations, k)
			}
		}
		s.mrfMU.Unlock()

		// Heal objects
		for _, u := range mrfOperations {
			waitForLowHTTPReq(globalHealConfig.IOCount, globalHealConfig.Sleep)

			// Send an object to background heal
			bgSeq.sourceCh <- u

			s.mrfMU.Lock()
			delete(s.mrfOperations, u)
			s.mrfMU.Unlock()
		}
	}
}

// TransitionObject - transition object content to target tier.
func (s *erasureSets) TransitionObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return s.getHashedSet(object).TransitionObject(ctx, bucket, object, opts)
}

// RestoreTransitionedObject - restore transitioned object content locally on this cluster.
func (s *erasureSets) RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return s.getHashedSet(object).RestoreTransitionedObject(ctx, bucket, object, opts)
}
