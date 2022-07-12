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
	"sort"
	"strings"
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
)

// setsDsyncLockers is encapsulated type for Close()
type setsDsyncLockers [][]dsync.NetLocker

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
	endpoints PoolEndpoints

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

	lastConnectDisksOpTime time.Time
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
				return nil, nil, fmt.Errorf("Disk: %s is a root disk", disk)
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
	if diskID == "" {
		return -1, -1, errDiskNotFound
	}
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
	diskMap := s.getDiskMap()
	setsJustConnected := make([]bool, s.setCount)
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
				// Recently disconnected disks must go to MRF
				setsJustConnected[setIndex] = cdisk.LastConn().After(s.lastConnectDisksOpTime)
				continue
			}
		}

		wg.Add(1)
		go func(endpoint Endpoint) {
			defer wg.Done()
			disk, format, err := connectEndpoint(endpoint)
			if err != nil {
				if endpoint.IsLocal && errors.Is(err, errUnformattedDisk) {
					globalBackgroundHealState.pushHealLocalDisks(endpoint)
				} else {
					printEndpointError(endpoint, err, true)
				}
				return
			}
			if disk.IsLocal() && disk.Healing() != nil {
				globalBackgroundHealState.pushHealLocalDisks(disk.Endpoint())
			}
			s.erasureDisksMu.RLock()
			setIndex, diskIndex, err := findDiskIndex(s.format, format)
			s.erasureDisksMu.RUnlock()
			if err != nil {
				printEndpointError(endpoint, err, false)
				disk.Close()
				return
			}

			s.erasureDisksMu.Lock()
			if currentDisk := s.erasureDisks[setIndex][diskIndex]; currentDisk != nil {
				if !reflect.DeepEqual(currentDisk.Endpoint(), disk.Endpoint()) {
					err = fmt.Errorf("Detected unexpected disk ordering refusing to use the disk: expecting %s, found %s, refusing to use the disk",
						currentDisk.Endpoint(), disk.Endpoint())
					printEndpointError(endpoint, err, false)
					disk.Close()
					s.erasureDisksMu.Unlock()
					return
				}
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
					s.erasureDisksMu.Unlock()
					return
				}
				disk.SetDiskID(format.Erasure.This)
				s.erasureDisks[setIndex][diskIndex] = disk
			}
			disk.SetDiskLoc(s.poolIndex, setIndex, diskIndex)
			setsJustConnected[setIndex] = true // disk just went online we treat it is as MRF event
			s.erasureDisksMu.Unlock()
		}(endpoint)
	}

	wg.Wait()

	go func() {
		for setIndex, justConnected := range setsJustConnected {
			if !justConnected {
				continue
			}
			globalMRFState.newSetReconnected(s.poolIndex, setIndex)
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
			if serverDebugLog {
				console.Debugln("running disk monitoring")
			}

			s.connectDisks()

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

func (s *erasureSets) GetEndpoints(setIndex int) func() []Endpoint {
	return func() []Endpoint {
		s.erasureDisksMu.RLock()
		defer s.erasureDisksMu.RUnlock()

		eps := make([]Endpoint, s.setDriveCount)
		for i := 0; i < s.setDriveCount; i++ {
			eps[i] = s.endpoints.Endpoints[setIndex*s.setDriveCount+i]
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
		setReconnectEvent:  make(chan int),
		distributionAlgo:   format.Erasure.DistributionAlgo,
		deploymentID:       uuid.MustParse(format.ID),
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

	erasureLockers := map[string]dsync.NetLocker{}
	for _, endpoint := range endpoints.Endpoints {
		if _, ok := erasureLockers[endpoint.Host]; !ok {
			erasureLockers[endpoint.Host] = newLockAPI(endpoint)
		}
	}

	for i := 0; i < setCount; i++ {
		lockerEpSet := set.NewStringSet()
		for j := 0; j < setDriveCount; j++ {
			endpoint := endpoints.Endpoints[i*setDriveCount+j]
			// Only add lockers only one per endpoint and per erasure set.
			if locker, ok := erasureLockers[endpoint.Host]; ok && !lockerEpSet.Contains(endpoint.Host) {
				lockerEpSet.Add(endpoint.Host)
				s.erasureLockers[i] = append(s.erasureLockers[i], locker)
			}
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < setCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			var innerWg sync.WaitGroup
			for j := 0; j < setDriveCount; j++ {
				disk := storageDisks[i*setDriveCount+j]
				if disk == nil {
					continue
				}
				innerWg.Add(1)
				go func(disk StorageAPI, i, j int) {
					defer innerWg.Done()
					diskID, err := disk.GetDiskID()
					if err != nil {
						if !errors.Is(err, errUnformattedDisk) {
							logger.LogIf(ctx, err)
						}
						return
					}
					if diskID == "" {
						return
					}
					m, n, err := findDiskIndexByDiskID(format, diskID)
					if err != nil {
						logger.LogIf(ctx, err)
						return
					}
					if m != i || n != j {
						logger.LogIf(ctx, fmt.Errorf("Detected unexpected disk ordering refusing to use the disk - poolID: %s, found disk mounted at (set=%s, disk=%s) expected mount at (set=%s, disk=%s): %s(%s)", humanize.Ordinal(poolIdx+1), humanize.Ordinal(m+1), humanize.Ordinal(n+1), humanize.Ordinal(i+1), humanize.Ordinal(j+1), disk, diskID))
						s.erasureDisks[i][j] = &unrecognizedDisk{storage: disk}
						return
					}
					disk.SetDiskLoc(s.poolIndex, m, n)
					s.endpointStrings[m*setDriveCount+n] = disk.String()
					s.erasureDisks[m][n] = disk
				}(disk, i, j)
			}
			innerWg.Wait()

			// Initialize erasure objects for a given set.
			s.sets[i] = &erasureObjects{
				setIndex:              i,
				poolIndex:             poolIdx,
				setDriveCount:         setDriveCount,
				defaultParityCount:    defaultParityCount,
				getDisks:              s.GetDisks(i),
				getLockers:            s.GetLockers(i),
				getEndpoints:          s.GetEndpoints(i),
				deletedCleanupSleeper: newDynamicSleeper(10, 2*time.Second, false),
				nsMutex:               mutex,
				bp:                    bp,
				bpOld:                 bpOld,
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
					set.cleanupStaleUploads(ctx, globalAPIConfig.getStaleUploadsExpiry())
				}(set)
			}
			wg.Wait()

			// Reset for the next interval
			timer.Reset(globalAPIConfig.getStaleUploadsCleanupInterval())
		}
	}
}

type auditObjectOp struct {
	Name  string   `json:"name"`
	Pool  int      `json:"poolId"`
	Set   int      `json:"setId"`
	Disks []string `json:"disks"`
}

// Add erasure set information to the current context
func auditObjectErasureSet(ctx context.Context, object string, set *erasureObjects) {
	if len(logger.AuditTargets()) == 0 {
		return
	}

	object = decodeDirObject(object)
	endpoints := set.getEndpoints()
	disksEndpoints := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		disksEndpoints = append(disksEndpoints, endpoint.String())
	}

	op := auditObjectOp{
		Name:  object,
		Pool:  set.poolIndex + 1,
		Set:   set.setIndex + 1,
		Disks: disksEndpoints,
	}

	logger.GetReqInfo(ctx).AppendTags("objectLocation", op)
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
func (s *erasureSets) MakeBucketWithLocation(ctx context.Context, bucket string, opts MakeBucketOptions) error {
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
func (s *erasureSets) GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (bucketInfo BucketInfo, err error) {
	return s.getHashedSet("").GetBucketInfo(ctx, bucket, opts)
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
func (s *erasureSets) DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	g := errgroup.WithNErrs(len(s.sets))

	// Delete buckets in parallel across all sets.
	for index := range s.sets {
		index := index
		g.Go(func() error {
			return s.sets[index].DeleteBucket(ctx, bucket, opts)
		}, index)
	}

	errs := g.Wait()
	// For any failure, we attempt undo all the delete buckets operation
	// by creating buckets again on all sets which were successfully deleted.
	for _, err := range errs {
		if err != nil && !opts.NoRecreate {
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
				return sets[index].MakeBucketWithLocation(ctx, bucket, MakeBucketOptions{})
			}
			return nil
		}, index)
	}

	g.Wait()
}

// List all buckets from one of the set, we are not doing merge
// sort here just for simplification. As per design it is assumed
// that all buckets are present on all sets.
func (s *erasureSets) ListBuckets(ctx context.Context, opts BucketOptions) (buckets []BucketInfo, err error) {
	var listBuckets []BucketInfo
	healBuckets := map[string]VolInfo{}
	for _, set := range s.sets {
		// lists all unique buckets across drives.
		if err := listAllBuckets(ctx, set.getDisks(), healBuckets, s.defaultParityCount); err != nil {
			return nil, err
		}
	}

	// include deleted buckets in listBuckets output
	deletedBuckets := map[string]VolInfo{}

	if opts.Deleted {
		for _, set := range s.sets {
			// lists all unique buckets across drives.
			if err := listDeletedBuckets(ctx, set.getDisks(), deletedBuckets, s.defaultParityCount); err != nil {
				return nil, err
			}
		}
	}
	for _, v := range healBuckets {
		bi := BucketInfo{
			Name:    v.Name,
			Created: v.Created,
		}
		if vi, ok := deletedBuckets[v.Name]; ok {
			bi.Deleted = vi.Created
		}
		listBuckets = append(listBuckets, bi)
	}
	for _, v := range deletedBuckets {
		if _, ok := healBuckets[v.Name]; !ok {
			listBuckets = append(listBuckets, BucketInfo{
				Name:    v.Name,
				Deleted: v.Created,
			})
		}
	}

	sort.Slice(listBuckets, func(i, j int) bool {
		return listBuckets[i].Name < listBuckets[j].Name
	})

	return listBuckets, nil
}

// listDeletedBuckets lists deleted buckets from all disks.
func listDeletedBuckets(ctx context.Context, storageDisks []StorageAPI, delBuckets map[string]VolInfo, readQuorum int) error {
	g := errgroup.WithNErrs(len(storageDisks))
	var mu sync.Mutex
	for index := range storageDisks {
		index := index
		g.Go(func() error {
			if storageDisks[index] == nil {
				// we ignore disk not found errors
				return nil
			}
			volsInfo, err := storageDisks[index].ListDir(ctx, minioMetaBucket, pathJoin(bucketMetaPrefix, deletedBucketsPrefix), -1)
			if err != nil {
				if err == errFileNotFound {
					return nil
				}
				return err
			}
			for _, volName := range volsInfo {
				mu.Lock()
				if _, ok := delBuckets[volName]; !ok {
					vi, err := storageDisks[index].StatVol(ctx, pathJoin(minioMetaBucket, bucketMetaPrefix, deletedBucketsPrefix, volName))
					if err != nil {
						return err
					}
					bkt := strings.TrimPrefix(vi.Name, pathJoin(minioMetaBucket, bucketMetaPrefix, deletedBucketsPrefix))
					bkt = strings.TrimPrefix(bkt, slashSeparator)
					bkt = strings.TrimSuffix(bkt, slashSeparator)
					vi.Name = bkt
					delBuckets[bkt] = vi
				}
				mu.Unlock()
			}
			return nil
		}, index)
	}
	return reduceReadQuorumErrs(ctx, g.Wait(), bucketMetadataOpIgnoredErrs, readQuorum)
}

// --- Object Operations ---

// GetObjectNInfo - returns object info and locked object ReadCloser
func (s *erasureSets) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	set := s.getHashedSet(object)
	return set.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
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
	if opts.DeletePrefix {
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
	return set.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// Initiate a new multipart upload on a hashedSet based on object name.
func (s *erasureSets) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (uploadID string, err error) {
	set := s.getHashedSet(object)
	return set.NewMultipartUpload(ctx, bucket, object, opts)
}

// Copies a part of an object from source hashedSet to destination hashedSet.
func (s *erasureSets) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int,
	startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions,
) (partInfo PartInfo, err error) {
	destSet := s.getHashedSet(destObject)
	return destSet.PutObjectPart(ctx, destBucket, destObject, uploadID, partID, NewPutObjReader(srcInfo.Reader), dstOpts)
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
	if globalIsCICD {
		// Do nothing
		return
	}
	infos, ierrs := getHealDiskInfos(storageDisks, errs)
	for i := range storageDisks {
		if ierrs[i] != nil && ierrs[i] != errUnformattedDisk {
			storageDisks[i] = nil
			continue
		}
		if storageDisks[i] != nil && infos[i].RootDisk {
			// We should not heal on root disk. i.e in a situation where the minio-administrator has unmounted a
			// defective drive we should not heal a path on the root disk.
			logger.LogIf(GlobalContext, fmt.Errorf("Disk `%s` is part of root disk, will not be used", storageDisks[i]))
			storageDisks[i] = nil
		}
	}
}

// HealFormat - heals missing `format.json` on fresh unformatted disks.
func (s *erasureSets) HealFormat(ctx context.Context, dryRun bool) (res madmin.HealResultItem, err error) {
	storageDisks, _ := initStorageDisksWithErrorsWithoutHealthCheck(s.endpoints.Endpoints)

	defer func(storageDisks []StorageAPI) {
		if err != nil {
			closeStorageDisks(storageDisks...)
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

	// Initialize a new set of set formats which will be written to disk.
	newFormatSets := newHealFormatSets(refFormat, s.setCount, s.setDriveCount, formats, sErrs)

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
			if err := saveFormatErasure(storageDisks[index], format, true); err != nil {
				logger.LogIf(ctx, fmt.Errorf("Disk %s failed to write updated 'format.json': %v", storageDisks[index], err))
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
				logger.LogIf(ctx, err)
				continue
			}

			if s.erasureDisks[m][n] != nil {
				s.erasureDisks[m][n].Close()
			}

			if storageDisks[index] != nil {
				storageDisks[index].SetDiskLoc(s.poolIndex, m, n)
				s.erasureDisks[m][n] = storageDisks[index]
			}
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
		healResult, err := set.HealBucket(ctx, bucket, opts)
		if err != nil {
			return result, toObjectErr(err, bucket)
		}
		result.Before.Drives = append(result.Before.Drives, healResult.Before.Drives...)
		result.After.Drives = append(result.After.Drives, healResult.After.Drives...)
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

// TransitionObject - transition object content to target tier.
func (s *erasureSets) TransitionObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return s.getHashedSet(object).TransitionObject(ctx, bucket, object, opts)
}

// RestoreTransitionedObject - restore transitioned object content locally on this cluster.
func (s *erasureSets) RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return s.getHashedSet(object).RestoreTransitionedObject(ctx, bucket, object, opts)
}
