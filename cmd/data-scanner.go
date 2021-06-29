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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config/heal"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/console"
)

const (
	dataScannerSleepPerFolder     = time.Millisecond                 // Time to wait between folders.
	dataUsageUpdateDirCycles      = 16                               // Visit all folders every n cycles.
	dataScannerCompactLeastObject = 500                              // Compact when there is less than this many objects in a branch.
	dataScannerCompactAtChildren  = 10000                            // Compact when there are this many children in a branch.
	dataScannerCompactAtFolders   = dataScannerCompactAtChildren / 4 // Compact when this many subfolders in a single folder.
	dataScannerStartDelay         = 1 * time.Minute                  // Time to wait on startup and between cycles.

	healDeleteDangling    = true
	healFolderIncludeProb = 32  // Include a clean folder one in n cycles.
	healObjectSelectProb  = 512 // Overall probability of a file being scanned; one in n.
)

var (
	globalHealConfig   heal.Config
	globalHealConfigMu sync.Mutex

	dataScannerLeaderLockTimeout = newDynamicTimeout(30*time.Second, 10*time.Second)
	// Sleeper values are updated when config is loaded.
	scannerSleeper = newDynamicSleeper(10, 10*time.Second)
	scannerCycle   = &safeDuration{
		t: dataScannerStartDelay,
	}
)

// initDataScanner will start the scanner in the background.
func initDataScanner(ctx context.Context, objAPI ObjectLayer) {
	go runDataScanner(ctx, objAPI)
}

type safeDuration struct {
	sync.Mutex
	t time.Duration
}

func (s *safeDuration) Update(t time.Duration) {
	s.Lock()
	defer s.Unlock()
	s.t = t
}

func (s *safeDuration) Get() time.Duration {
	s.Lock()
	defer s.Unlock()
	return s.t
}

// runDataScanner will start a data scanner.
// The function will block until the context is canceled.
// There should only ever be one scanner running per cluster.
func runDataScanner(pctx context.Context, objAPI ObjectLayer) {
	// Make sure only 1 scanner is running on the cluster.
	locker := objAPI.NewNSLock(minioMetaBucket, "runDataScanner.lock")
	var ctx context.Context
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		lkctx, err := locker.GetLock(pctx, dataScannerLeaderLockTimeout)
		if err != nil {
			time.Sleep(time.Duration(r.Float64() * float64(scannerCycle.Get())))
			continue
		}
		ctx = lkctx.Context()
		defer lkctx.Cancel()
		break
		// No unlock for "leader" lock.
	}

	// Load current bloom cycle
	nextBloomCycle := intDataUpdateTracker.current() + 1

	br, err := objAPI.GetObjectNInfo(ctx, dataUsageBucket, dataUsageBloomName, nil, http.Header{}, readLock, ObjectOptions{})
	if err != nil {
		if !isErrObjectNotFound(err) && !isErrBucketNotFound(err) {
			logger.LogIf(ctx, err)
		}
	} else {
		if br.ObjInfo.Size == 8 {
			if err = binary.Read(br, binary.LittleEndian, &nextBloomCycle); err != nil {
				logger.LogIf(ctx, err)
			}
		}
		br.Close()
	}

	scannerTimer := time.NewTimer(scannerCycle.Get())
	defer scannerTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-scannerTimer.C:
			// Reset the timer for next cycle.
			scannerTimer.Reset(scannerCycle.Get())

			if intDataUpdateTracker.debug {
				console.Debugln("starting scanner cycle")
			}

			// Wait before starting next cycle and wait on startup.
			results := make(chan madmin.DataUsageInfo, 1)
			go storeDataUsageInBackend(ctx, objAPI, results)
			bf, err := globalNotificationSys.updateBloomFilter(ctx, nextBloomCycle)
			logger.LogIf(ctx, err)
			err = objAPI.NSScanner(ctx, bf, results)
			logger.LogIf(ctx, err)
			if err == nil {
				// Store new cycle...
				nextBloomCycle++
				var tmp [8]byte
				binary.LittleEndian.PutUint64(tmp[:], nextBloomCycle)
				r, err := hash.NewReader(bytes.NewReader(tmp[:]), int64(len(tmp)), "", "", int64(len(tmp)))
				if err != nil {
					logger.LogIf(ctx, err)
					continue
				}

				_, err = objAPI.PutObject(ctx, dataUsageBucket, dataUsageBloomName, NewPutObjReader(r), ObjectOptions{})
				if !isErrBucketNotFound(err) {
					logger.LogIf(ctx, err)
				}
			}
		}
	}
}

type cachedFolder struct {
	name              string
	parent            *dataUsageHash
	objectHealProbDiv uint32
}

type folderScanner struct {
	root        string
	getSize     getSizeFn
	oldCache    dataUsageCache
	newCache    dataUsageCache
	updateCache dataUsageCache
	withFilter  *bloomFilter

	dataUsageScannerDebug bool
	healFolderInclude     uint32 // Include a clean folder one in n cycles.
	healObjectSelect      uint32 // Do a heal check on an object once every n cycles. Must divide into healFolderInclude

	disks []StorageAPI

	// If set updates will be sent regularly to this channel.
	// Will not be closed when returned.
	updates    chan<- dataUsageEntry
	lastUpdate time.Time
}

// Cache structure and compaction:
//
// A cache structure will be kept with a tree of usages.
// The cache is a tree structure where each keeps track of its children.
//
// An uncompacted branch contains a count of the files only directly at the
// branch level, and contains link to children branches or leaves.
//
// The leaves are "compacted" based on a number of properties.
// A compacted leaf contains the totals of all files beneath it.
//
// A leaf is only scanned once every dataUsageUpdateDirCycles,
// rarer if the bloom filter for the path is clean and no lifecycles are applied.
// Skipped leaves have their totals transferred from the previous cycle.
//
// A clean leaf will be included once every healFolderIncludeProb for partial heal scans.
// When selected there is a one in healObjectSelectProb that any object will be chosen for heal scan.
//
// Compaction happens when either:
//
// 1) The folder (and subfolders) contains less than dataScannerCompactLeastObject objects.
// 2) The folder itself contains more than dataScannerCompactAtFolders folders.
// 3) The folder only contains objects and no subfolders.
//
// A bucket root will never be compacted.
//
// Furthermore if a has more than dataScannerCompactAtChildren recursive children (uncompacted folders)
// the tree will be recursively scanned and the branches with the least number of objects will be
// compacted until the limit is reached.
//
// This ensures that any branch will never contain an unreasonable amount of other branches,
// and also that small branches with few objects don't take up unreasonable amounts of space.
// This keeps the cache size at a reasonable size for all buckets.
//
// Whenever a branch is scanned, it is assumed that it will be un-compacted
// before it hits any of the above limits.
// This will make the branch rebalance itself when scanned if the distribution of objects has changed.

// scanDataFolder will scanner the basepath+cache.Info.Name and return an updated cache.
// The returned cache will always be valid, but may not be updated from the existing.
// Before each operation sleepDuration is called which can be used to temporarily halt the scanner.
// If the supplied context is canceled the function will return at the first chance.
func scanDataFolder(ctx context.Context, basePath string, cache dataUsageCache, getSize getSizeFn) (dataUsageCache, error) {
	t := UTCNow()

	logPrefix := color.Green("data-usage: ")
	logSuffix := color.Blue("- %v + %v", basePath, cache.Info.Name)
	if intDataUpdateTracker.debug {
		defer func() {
			console.Debugf(logPrefix+" Scanner time: %v %s\n", time.Since(t), logSuffix)
		}()

	}

	switch cache.Info.Name {
	case "", dataUsageRoot:
		return cache, errors.New("internal error: root scan attempted")
	}

	s := folderScanner{
		root:                  basePath,
		getSize:               getSize,
		oldCache:              cache,
		newCache:              dataUsageCache{Info: cache.Info},
		updateCache:           dataUsageCache{Info: cache.Info},
		dataUsageScannerDebug: intDataUpdateTracker.debug,
		healFolderInclude:     0,
		healObjectSelect:      0,
		updates:               cache.Info.updates,
	}

	// Add disks for set healing.
	if len(cache.Disks) > 0 {
		objAPI, ok := newObjectLayerFn().(*erasureServerPools)
		if ok {
			s.disks = objAPI.GetDisksID(cache.Disks...)
			if len(s.disks) != len(cache.Disks) {
				console.Debugf(logPrefix+"Missing disks, want %d, found %d. Cannot heal. %s\n", len(cache.Disks), len(s.disks), logSuffix)
				s.disks = s.disks[:0]
			}
		}
	}

	// Enable healing in XL mode.
	if globalIsErasure && !cache.Info.SkipHealing {
		// Include a clean folder one in n cycles.
		s.healFolderInclude = healFolderIncludeProb
		// Do a heal check on an object once every n cycles. Must divide into healFolderInclude
		s.healObjectSelect = healObjectSelectProb
	}
	if len(cache.Info.BloomFilter) > 0 {
		s.withFilter = &bloomFilter{BloomFilter: &bloom.BloomFilter{}}
		_, err := s.withFilter.ReadFrom(bytes.NewReader(cache.Info.BloomFilter))
		if err != nil {
			logger.LogIf(ctx, err, logPrefix+"Error reading bloom filter")
			s.withFilter = nil
		}
	}
	if s.dataUsageScannerDebug {
		console.Debugf(logPrefix+"Start scanning. Bloom filter: %v %s\n", s.withFilter != nil, logSuffix)
	}

	done := ctx.Done()
	if s.dataUsageScannerDebug {
		console.Debugf(logPrefix+"Cycle: %v, Entries: %v %s\n", cache.Info.NextCycle, len(cache.Cache), logSuffix)
	}

	// Read top level in bucket.
	select {
	case <-done:
		return cache, ctx.Err()
	default:
	}
	root := dataUsageEntry{}
	folder := cachedFolder{name: cache.Info.Name, objectHealProbDiv: 1}
	err := s.scanFolder(ctx, folder, &root)
	if err != nil {
		// No useful information...
		return cache, err
	}

	if s.dataUsageScannerDebug {
		console.Debugf(logPrefix+"Finished scanner, %v entries (%+v) %s \n", len(s.newCache.Cache), *s.newCache.sizeRecursive(s.newCache.Info.Name), logSuffix)
	}
	s.newCache.Info.LastUpdate = UTCNow()
	s.newCache.Info.NextCycle++
	return s.newCache, nil
}

// sendUpdate() should be called on a regular basis when the newCache contains more recent total than previously.
// May or may not send an update upstream.
func (f *folderScanner) sendUpdate() {
	// Send at most an update every minute.
	if f.updates == nil || time.Since(f.lastUpdate) < time.Minute {
		return
	}
	if flat := f.updateCache.sizeRecursive(f.newCache.Info.Name); flat != nil {
		select {
		case f.updates <- *flat:
		default:
		}
		f.lastUpdate = time.Now()
	}
}

// scanFolder will scan the provided folder.
// Files found in the folders will be added to f.newCache.
// If final is provided folders will be put into f.newFolders or f.existingFolders.
// If final is not provided the folders found are returned from the function.
func (f *folderScanner) scanFolder(ctx context.Context, folder cachedFolder, into *dataUsageEntry) error {
	done := ctx.Done()
	scannerLogPrefix := color.Green("folder-scanner:")
	thisHash := hashPath(folder.name)
	// Store initial compaction state.
	wasCompacted := into.Compacted

	for {
		select {
		case <-done:
			return ctx.Err()
		default:
		}
		existing, ok := f.oldCache.Cache[thisHash.Key()]
		var abandonedChildren dataUsageHashMap
		if !into.Compacted {
			abandonedChildren = f.oldCache.findChildrenCopy(thisHash)
		}

		// If there are lifecycle rules for the prefix, remove the filter.
		filter := f.withFilter
		_, prefix := path2BucketObjectWithBasePath(f.root, folder.name)
		var activeLifeCycle *lifecycle.Lifecycle
		if f.oldCache.Info.lifeCycle != nil && f.oldCache.Info.lifeCycle.HasActiveRules(prefix, true) {
			if f.dataUsageScannerDebug {
				console.Debugf(scannerLogPrefix+" Prefix %q has active rules\n", prefix)
			}
			activeLifeCycle = f.oldCache.Info.lifeCycle
			filter = nil
		}
		// If there are replication rules for the prefix, remove the filter.
		var replicationCfg replicationConfig
		if !f.oldCache.Info.replication.Empty() && f.oldCache.Info.replication.Config.HasActiveRules(prefix, true) {
			replicationCfg = f.oldCache.Info.replication
			filter = nil
		}
		// Check if we can skip it due to bloom filter...
		if filter != nil && ok && existing.Compacted {
			// If folder isn't in filter and we have data, skip it completely.
			if folder.name != dataUsageRoot && !filter.containsDir(folder.name) {
				if f.healObjectSelect == 0 || !thisHash.mod(f.oldCache.Info.NextCycle, f.healFolderInclude/folder.objectHealProbDiv) {
					f.newCache.copyWithChildren(&f.oldCache, thisHash, folder.parent)
					f.updateCache.copyWithChildren(&f.oldCache, thisHash, folder.parent)
					if f.dataUsageScannerDebug {
						console.Debugf(scannerLogPrefix+" Skipping non-updated folder: %v\n", folder.name)
					}
					return nil
				}
				if f.dataUsageScannerDebug {
					console.Debugf(scannerLogPrefix+" Adding non-updated folder to heal check: %v\n", folder.name)
				}
				// If probability was already scannerHealFolderInclude, keep it.
				folder.objectHealProbDiv = f.healFolderInclude
			}
		}
		scannerSleeper.Sleep(ctx, dataScannerSleepPerFolder)

		var existingFolders, newFolders []cachedFolder
		var foundObjects bool
		err := readDirFn(path.Join(f.root, folder.name), func(entName string, typ os.FileMode) error {
			// Parse
			entName = pathClean(path.Join(folder.name, entName))
			if entName == "" {
				if f.dataUsageScannerDebug {
					console.Debugf(scannerLogPrefix+" no bucket (%s,%s)\n", f.root, entName)
				}
				return errDoneForNow
			}
			bucket, prefix := path2BucketObjectWithBasePath(f.root, entName)
			if bucket == "" {
				if f.dataUsageScannerDebug {
					console.Debugf(scannerLogPrefix+" no bucket (%s,%s)\n", f.root, entName)
				}
				return errDoneForNow
			}

			if isReservedOrInvalidBucket(bucket, false) {
				if f.dataUsageScannerDebug {
					console.Debugf(scannerLogPrefix+" invalid bucket: %v, entry: %v\n", bucket, entName)
				}
				return errDoneForNow
			}

			select {
			case <-done:
				return errDoneForNow
			default:
			}

			if typ&os.ModeDir != 0 {
				h := hashPath(entName)
				_, exists := f.oldCache.Cache[h.Key()]

				this := cachedFolder{name: entName, parent: &thisHash, objectHealProbDiv: folder.objectHealProbDiv}
				delete(abandonedChildren, h.Key()) // h.Key() already accounted for.
				if exists {
					existingFolders = append(existingFolders, this)
					f.updateCache.copyWithChildren(&f.oldCache, h, &thisHash)
				} else {
					newFolders = append(newFolders, this)
				}
				return nil
			}

			// Dynamic time delay.
			wait := scannerSleeper.Timer(ctx)

			// Get file size, ignore errors.
			item := scannerItem{
				Path:        path.Join(f.root, entName),
				Typ:         typ,
				bucket:      bucket,
				prefix:      path.Dir(prefix),
				objectName:  path.Base(entName),
				debug:       f.dataUsageScannerDebug,
				lifeCycle:   activeLifeCycle,
				replication: replicationCfg,
				heal:        thisHash.mod(f.oldCache.Info.NextCycle, f.healObjectSelect/folder.objectHealProbDiv) && globalIsErasure,
			}
			// if the drive belongs to an erasure set
			// that is already being healed, skip the
			// healing attempt on this drive.
			item.heal = item.heal && f.healObjectSelect > 0

			sz, err := f.getSize(item)
			if err == errSkipFile {
				wait() // wait to proceed to next entry.

				return nil
			}
			// successfully read means we have a valid object.
			foundObjects = true
			// Remove filename i.e is the meta file to construct object name
			item.transformMetaDir()

			// Object already accounted for, remove from heal map,
			// simply because getSize() function already heals the
			// object.
			delete(abandonedChildren, path.Join(item.bucket, item.objectPath()))

			into.addSizes(sz)
			into.Objects++

			wait() // wait to proceed to next entry.

			return nil
		})
		if err != nil {
			return err
		}

		if foundObjects && globalIsErasure {
			// If we found an object in erasure mode, we skip subdirs (only datadirs)...
			break
		}

		// If we have many subfolders, compact ourself.
		if !into.Compacted &&
			f.newCache.Info.Name != folder.name &&
			len(existingFolders)+len(newFolders) >= dataScannerCompactAtFolders {
			into.Compacted = true
			newFolders = append(newFolders, existingFolders...)
			existingFolders = nil
			if f.dataUsageScannerDebug {
				console.Debugf(scannerLogPrefix+" Preemptively compacting: %v, entries: %v\n", folder.name, len(existingFolders)+len(newFolders))
			}
		}

		scanFolder := func(folder cachedFolder) {
			if contextCanceled(ctx) {
				return
			}
			dst := into
			if !into.Compacted {
				dst = &dataUsageEntry{Compacted: false}
			}
			if err := f.scanFolder(ctx, folder, dst); err != nil {
				logger.LogIf(ctx, err)
				return
			}
			if !into.Compacted {
				into.addChild(dataUsageHash(folder.name))
			}
			// We scanned a folder, optionally send update.
			f.sendUpdate()
		}

		// Scan new...
		for _, folder := range newFolders {
			h := hashPath(folder.name)
			// Add new folders to the update tree so totals update for these.
			if !into.Compacted {
				var foundAny bool
				parent := thisHash
				for parent != hashPath(f.updateCache.Info.Name) {
					e := f.updateCache.find(parent.Key())
					if e == nil || e.Compacted {
						foundAny = true
						break
					}
					if next := f.updateCache.searchParent(parent); next == nil {
						foundAny = true
						break
					} else {
						parent = *next
					}
				}
				if !foundAny {
					// Add non-compacted empty entry.
					f.updateCache.replaceHashed(h, &thisHash, dataUsageEntry{})
				}
			}
			scanFolder(folder)
			// Add new folders if this is new and we don't have existing.
			if !into.Compacted {
				parent := f.updateCache.find(thisHash.Key())
				if parent != nil && !parent.Compacted {
					f.updateCache.deleteRecursive(h)
					f.updateCache.copyWithChildren(&f.newCache, h, &thisHash)
				}
			}
		}

		// Scan existing...
		for _, folder := range existingFolders {
			h := hashPath(folder.name)
			// Check if we should skip scanning folder...
			// We can only skip if we are not indexing into a compacted destination
			// and the entry itself is compacted.
			if !into.Compacted && f.oldCache.isCompacted(h) {
				if !h.mod(f.oldCache.Info.NextCycle, dataUsageUpdateDirCycles) {
					if f.healObjectSelect == 0 || !h.mod(f.oldCache.Info.NextCycle, f.healFolderInclude/folder.objectHealProbDiv) {
						// Transfer and add as child...
						f.newCache.copyWithChildren(&f.oldCache, h, folder.parent)
						into.addChild(h)
						continue
					}
					folder.objectHealProbDiv = dataUsageUpdateDirCycles
				}
			}
			scanFolder(folder)
		}

		// Scan for healing
		if f.healObjectSelect == 0 || len(abandonedChildren) == 0 {
			// If we are not heal scanning, return now.
			break
		}

		objAPI, ok := newObjectLayerFn().(*erasureServerPools)
		if !ok || len(f.disks) == 0 {
			break
		}

		bgSeq, found := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
		if !found {
			break
		}

		// Whatever remains in 'abandonedChildren' are folders at this level
		// that existed in the previous run but wasn't found now.
		//
		// This may be because of 2 reasons:
		//
		// 1) The folder/object was deleted.
		// 2) We come from another disk and this disk missed the write.
		//
		// We therefore perform a heal check.
		// If that doesn't bring it back we remove the folder and assume it was deleted.
		// This means that the next run will not look for it.
		// How to resolve results.
		resolver := metadataResolutionParams{
			dirQuorum: getReadQuorum(len(f.disks)),
			objQuorum: getReadQuorum(len(f.disks)),
			bucket:    "",
		}

		healObjectsPrefix := color.Green("healObjects:")
		for k := range abandonedChildren {
			bucket, prefix := path2BucketObject(k)
			if f.dataUsageScannerDebug {
				console.Debugf(scannerLogPrefix+" checking disappeared folder: %v/%v\n", bucket, prefix)
			}

			resolver.bucket = bucket

			foundObjs := false
			dangling := false
			ctx, cancel := context.WithCancel(ctx)

			err := listPathRaw(ctx, listPathRawOptions{
				disks:          f.disks,
				bucket:         bucket,
				path:           prefix,
				recursive:      true,
				reportNotFound: true,
				minDisks:       len(f.disks), // We want full consistency.
				// Weird, maybe transient error.
				agreed: func(entry metaCacheEntry) {
					if f.dataUsageScannerDebug {
						console.Debugf(healObjectsPrefix+" got agreement: %v\n", entry.name)
					}
				},
				// Some disks have data for this.
				partial: func(entries metaCacheEntries, nAgreed int, errs []error) {
					if f.dataUsageScannerDebug {
						console.Debugf(healObjectsPrefix+" got partial, %d agreed, errs: %v\n", nAgreed, errs)
					}

					// agreed value less than expected quorum
					dangling = nAgreed < resolver.objQuorum || nAgreed < resolver.dirQuorum

					entry, ok := entries.resolve(&resolver)
					if !ok {
						for _, err := range errs {
							if err != nil {
								return
							}
						}

						// If no errors, queue it for healing.
						entry, _ = entries.firstFound()
					}

					if f.dataUsageScannerDebug {
						console.Debugf(healObjectsPrefix+" resolved to: %v, dir: %v\n", entry.name, entry.isDir())
					}

					if entry.isDir() {
						return
					}

					// wait on timer per object.
					wait := scannerSleeper.Timer(ctx)

					// We got an entry which we should be able to heal.
					fiv, err := entry.fileInfoVersions(bucket)
					if err != nil {
						wait()
						err := bgSeq.queueHealTask(healSource{
							bucket:    bucket,
							object:    entry.name,
							versionID: "",
						}, madmin.HealItemObject)
						if !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
							logger.LogIf(ctx, err)
						}
						foundObjs = foundObjs || err == nil
						return
					}

					for _, ver := range fiv.Versions {
						// Sleep and reset.
						wait()
						wait = scannerSleeper.Timer(ctx)

						err := bgSeq.queueHealTask(healSource{
							bucket:    bucket,
							object:    fiv.Name,
							versionID: ver.VersionID,
						}, madmin.HealItemObject)
						if !isErrObjectNotFound(err) && !isErrVersionNotFound(err) {
							logger.LogIf(ctx, err)
						}
						foundObjs = foundObjs || err == nil
					}
				},
				// Too many disks failed.
				finished: func(errs []error) {
					if f.dataUsageScannerDebug {
						console.Debugf(healObjectsPrefix+" too many errors: %v\n", errs)
					}
					cancel()
				},
			})

			if f.dataUsageScannerDebug && err != nil && err != errFileNotFound {
				console.Debugf(healObjectsPrefix+" checking returned value %v (%T)\n", err, err)
			}

			// If we found one or more disks with this folder, delete it.
			if err == nil && dangling {
				if f.dataUsageScannerDebug {
					console.Debugf(healObjectsPrefix+" deleting dangling directory %s\n", prefix)
				}

				// wait on timer per object.
				wait := scannerSleeper.Timer(ctx)

				objAPI.HealObjects(ctx, bucket, prefix, madmin.HealOpts{
					Recursive: true,
					Remove:    healDeleteDangling,
				}, func(bucket, object, versionID string) error {
					// Wait for each heal as per scanner frequency.
					wait()
					wait = scannerSleeper.Timer(ctx)
					return bgSeq.queueHealTask(healSource{
						bucket:    bucket,
						object:    object,
						versionID: versionID,
					}, madmin.HealItemObject)
				})
			}

			// Add unless healing returned an error.
			if foundObjs {
				this := cachedFolder{name: k, parent: &thisHash, objectHealProbDiv: 1}
				scanFolder(this)
			}
		}
		break
	}
	if !wasCompacted {
		f.newCache.replaceHashed(thisHash, folder.parent, *into)
	}

	if !into.Compacted && f.newCache.Info.Name != folder.name {
		flat := f.newCache.sizeRecursive(thisHash.Key())
		flat.Compacted = true
		var compact bool
		if flat.Objects < dataScannerCompactLeastObject {
			if f.dataUsageScannerDebug && flat.Objects > 1 {
				// Disabled, rather chatty:
				//console.Debugf(scannerLogPrefix+" Only %d objects, compacting %s -> %+v\n", flat.Objects, folder.name, flat)
			}
			compact = true
		} else {
			// Compact if we only have objects as children...
			compact = true
			for k := range into.Children {
				if v, ok := f.newCache.Cache[k]; ok {
					if len(v.Children) > 0 || v.Objects > 1 {
						compact = false
						break
					}
				}
			}
			if f.dataUsageScannerDebug && compact {
				// Disabled, rather chatty:
				//console.Debugf(scannerLogPrefix+" Only objects (%d), compacting %s -> %+v\n", flat.Objects, folder.name, flat)
			}
		}
		if compact {
			f.newCache.deleteRecursive(thisHash)
			f.newCache.replaceHashed(thisHash, folder.parent, *flat)
		}

	}
	// Compact if too many children...
	if !into.Compacted {
		f.newCache.reduceChildrenOf(thisHash, dataScannerCompactAtChildren, f.newCache.Info.Name != folder.name)
	}
	if _, ok := f.updateCache.Cache[thisHash.Key()]; !wasCompacted && ok {
		// Replace if existed before.
		if flat := f.newCache.sizeRecursive(thisHash.Key()); flat != nil {
			f.updateCache.deleteRecursive(thisHash)
			f.updateCache.replaceHashed(thisHash, folder.parent, *flat)
		}
	}

	return nil
}

// scannerItem represents each file while walking.
type scannerItem struct {
	Path string
	Typ  os.FileMode

	bucket      string // Bucket.
	prefix      string // Only the prefix if any, does not have final object name.
	objectName  string // Only the object name without prefixes.
	lifeCycle   *lifecycle.Lifecycle
	replication replicationConfig
	heal        bool // Has the object been selected for heal check?
	debug       bool
}

type sizeSummary struct {
	totalSize      int64
	versions       uint64
	replicatedSize int64
	pendingSize    int64
	failedSize     int64
	replicaSize    int64
	pendingCount   uint64
	failedCount    uint64
}

type getSizeFn func(item scannerItem) (sizeSummary, error)

// transformMetaDir will transform a directory to prefix/file.ext
func (i *scannerItem) transformMetaDir() {
	split := strings.Split(i.prefix, SlashSeparator)
	if len(split) > 1 {
		i.prefix = path.Join(split[:len(split)-1]...)
	} else {
		i.prefix = ""
	}
	// Object name is last element
	i.objectName = split[len(split)-1]
}

// actionMeta contains information used to apply actions.
type actionMeta struct {
	oi         ObjectInfo
	bitRotScan bool // indicates if bitrot check was requested.
}

var applyActionsLogPrefix = color.Green("applyActions:")

func (i *scannerItem) applyHealing(ctx context.Context, o ObjectLayer, meta actionMeta) (size int64) {
	if i.debug {
		if meta.oi.VersionID != "" {
			console.Debugf(applyActionsLogPrefix+" heal checking: %v/%v v(%s)\n", i.bucket, i.objectPath(), meta.oi.VersionID)
		} else {
			console.Debugf(applyActionsLogPrefix+" heal checking: %v/%v\n", i.bucket, i.objectPath())
		}
	}
	healOpts := madmin.HealOpts{Remove: healDeleteDangling}
	if meta.bitRotScan {
		healOpts.ScanMode = madmin.HealDeepScan
	}
	res, err := o.HealObject(ctx, i.bucket, i.objectPath(), meta.oi.VersionID, healOpts)
	if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
		return 0
	}
	if err != nil && !errors.Is(err, NotImplemented{}) {
		logger.LogIf(ctx, err)
		return 0
	}
	return res.ObjectSize
}

func (i *scannerItem) applyLifecycle(ctx context.Context, o ObjectLayer, meta actionMeta) (applied bool, size int64) {
	size, err := meta.oi.GetActualSize()
	if i.debug {
		logger.LogIf(ctx, err)
	}
	if i.lifeCycle == nil {
		if i.debug {
			// disabled, very chatty:
			// console.Debugf(applyActionsLogPrefix+" no lifecycle rules to apply: %q\n", i.objectPath())
		}
		return false, size
	}

	versionID := meta.oi.VersionID
	action := i.lifeCycle.ComputeAction(
		lifecycle.ObjectOpts{
			Name:                   i.objectPath(),
			UserTags:               meta.oi.UserTags,
			ModTime:                meta.oi.ModTime,
			VersionID:              meta.oi.VersionID,
			DeleteMarker:           meta.oi.DeleteMarker,
			IsLatest:               meta.oi.IsLatest,
			NumVersions:            meta.oi.NumVersions,
			SuccessorModTime:       meta.oi.SuccessorModTime,
			RestoreOngoing:         meta.oi.RestoreOngoing,
			RestoreExpires:         meta.oi.RestoreExpires,
			TransitionStatus:       meta.oi.TransitionStatus,
			RemoteTiersImmediately: globalDebugRemoteTiersImmediately,
		})
	if i.debug {
		if versionID != "" {
			console.Debugf(applyActionsLogPrefix+" lifecycle: %q (version-id=%s), Initial scan: %v\n", i.objectPath(), versionID, action)
		} else {
			console.Debugf(applyActionsLogPrefix+" lifecycle: %q Initial scan: %v\n", i.objectPath(), action)
		}
	}
	switch action {
	case lifecycle.DeleteAction, lifecycle.DeleteVersionAction:
	case lifecycle.TransitionAction, lifecycle.TransitionVersionAction:
	case lifecycle.DeleteRestoredAction, lifecycle.DeleteRestoredVersionAction:
	default:
		// No action.
		if i.debug {
			console.Debugf(applyActionsLogPrefix+" object not expirable: %q\n", i.objectPath())
		}
		return false, size
	}

	obj, err := o.GetObjectInfo(ctx, i.bucket, i.objectPath(), ObjectOptions{
		VersionID: versionID,
	})
	if err != nil {
		switch err.(type) {
		case MethodNotAllowed: // This happens usually for a delete marker
			if !obj.DeleteMarker { // if this is not a delete marker log and return
				// Do nothing - heal in the future.
				logger.LogIf(ctx, err)
				return false, size
			}
		case ObjectNotFound, VersionNotFound:
			// object not found or version not found return 0
			return false, 0
		default:
			// All other errors proceed.
			logger.LogIf(ctx, err)
			return false, size
		}
	}

	action = evalActionFromLifecycle(ctx, *i.lifeCycle, obj, i.debug)
	if action != lifecycle.NoneAction {
		applied = applyLifecycleAction(ctx, action, o, obj)
	}

	if applied {
		switch action {
		case lifecycle.TransitionAction, lifecycle.TransitionVersionAction:
			return true, size
		}
		// For all other lifecycle actions that remove data
		return true, 0
	}

	return false, size
}

// applyActions will apply lifecycle checks on to a scanned item.
// The resulting size on disk will always be returned.
// The metadata will be compared to consensus on the object layer before any changes are applied.
// If no metadata is supplied, -1 is returned if no action is taken.
func (i *scannerItem) applyActions(ctx context.Context, o ObjectLayer, meta actionMeta, sizeS *sizeSummary) int64 {
	applied, size := i.applyLifecycle(ctx, o, meta)
	// For instance, an applied lifecycle means we remove/transitioned an object
	// from the current deployment, which means we don't have to call healing
	// routine even if we are asked to do via heal flag.
	if !applied {
		if i.heal {
			size = i.applyHealing(ctx, o, meta)
		}
		// replicate only if lifecycle rules are not applied.
		i.healReplication(ctx, o, meta.oi.Clone(), sizeS)
	}
	return size
}

func evalActionFromLifecycle(ctx context.Context, lc lifecycle.Lifecycle, obj ObjectInfo, debug bool) (action lifecycle.Action) {
	lcOpts := lifecycle.ObjectOpts{
		Name:                   obj.Name,
		UserTags:               obj.UserTags,
		ModTime:                obj.ModTime,
		VersionID:              obj.VersionID,
		DeleteMarker:           obj.DeleteMarker,
		IsLatest:               obj.IsLatest,
		NumVersions:            obj.NumVersions,
		SuccessorModTime:       obj.SuccessorModTime,
		RestoreOngoing:         obj.RestoreOngoing,
		RestoreExpires:         obj.RestoreExpires,
		TransitionStatus:       obj.TransitionStatus,
		RemoteTiersImmediately: globalDebugRemoteTiersImmediately,
	}

	action = lc.ComputeAction(lcOpts)
	if debug {
		console.Debugf(applyActionsLogPrefix+" lifecycle: Secondary scan: %v\n", action)
	}

	if action == lifecycle.NoneAction {
		return action
	}

	switch action {
	case lifecycle.DeleteVersionAction, lifecycle.DeleteRestoredVersionAction:
		// Defensive code, should never happen
		if obj.VersionID == "" {
			return lifecycle.NoneAction
		}
		if rcfg, _ := globalBucketObjectLockSys.Get(obj.Bucket); rcfg.LockEnabled {
			locked := enforceRetentionForDeletion(ctx, obj)
			if locked {
				if debug {
					if obj.VersionID != "" {
						console.Debugf(applyActionsLogPrefix+" lifecycle: %s v(%s) is locked, not deleting\n", obj.Name, obj.VersionID)
					} else {
						console.Debugf(applyActionsLogPrefix+" lifecycle: %s is locked, not deleting\n", obj.Name)
					}
				}
				return lifecycle.NoneAction
			}
		}
	}

	return action
}

func applyTransitionAction(ctx context.Context, action lifecycle.Action, objLayer ObjectLayer, obj ObjectInfo) bool {
	srcOpts := ObjectOptions{}
	if obj.TransitionStatus == "" {
		srcOpts.Versioned = globalBucketVersioningSys.Enabled(obj.Bucket)
		srcOpts.VersionID = obj.VersionID
		// mark transition as pending
		obj.UserDefined[ReservedMetadataPrefixLower+TransitionStatus] = lifecycle.TransitionPending
		obj.metadataOnly = true // Perform only metadata updates.
		if obj.DeleteMarker {
			return false
		}
	}
	globalTransitionState.queueTransitionTask(obj)
	return true

}

func applyExpiryOnTransitionedObject(ctx context.Context, objLayer ObjectLayer, obj ObjectInfo, restoredObject bool) bool {
	lcOpts := lifecycle.ObjectOpts{
		Name:             obj.Name,
		UserTags:         obj.UserTags,
		ModTime:          obj.ModTime,
		VersionID:        obj.VersionID,
		DeleteMarker:     obj.DeleteMarker,
		IsLatest:         obj.IsLatest,
		NumVersions:      obj.NumVersions,
		SuccessorModTime: obj.SuccessorModTime,
		RestoreOngoing:   obj.RestoreOngoing,
		RestoreExpires:   obj.RestoreExpires,
		TransitionStatus: obj.TransitionStatus,
	}

	action := expireObj
	if restoredObject {
		action = expireRestoredObj
	}
	if err := expireTransitionedObject(ctx, objLayer, &obj, lcOpts, action); err != nil {
		if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
			return false
		}
		logger.LogIf(ctx, err)
		return false
	}
	// Notification already sent in *expireTransitionedObject*, just return 'true' here.
	return true
}

func applyExpiryOnNonTransitionedObjects(ctx context.Context, objLayer ObjectLayer, obj ObjectInfo, applyOnVersion bool) bool {
	opts := ObjectOptions{}

	if applyOnVersion {
		opts.VersionID = obj.VersionID
	}
	if opts.VersionID == "" {
		opts.Versioned = globalBucketVersioningSys.Enabled(obj.Bucket)
	}

	obj, err := objLayer.DeleteObject(ctx, obj.Bucket, obj.Name, opts)
	if err != nil {
		if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
			return false
		}
		// Assume it is still there.
		logger.LogIf(ctx, err)
		return false
	}

	// Send audit for the lifecycle delete operation
	auditLogLifecycle(ctx, obj.Bucket, obj.Name)

	eventName := event.ObjectRemovedDelete
	if obj.DeleteMarker {
		eventName = event.ObjectRemovedDeleteMarkerCreated
	}

	// Notify object deleted event.
	sendEvent(eventArgs{
		EventName:  eventName,
		BucketName: obj.Bucket,
		Object:     obj,
		Host:       "Internal: [ILM-EXPIRY]",
	})

	return true
}

// Apply object, object version, restored object or restored object version action on the given object
func applyExpiryRule(ctx context.Context, objLayer ObjectLayer, obj ObjectInfo, restoredObject, applyOnVersion bool) bool {
	if obj.TransitionStatus != "" {
		return applyExpiryOnTransitionedObject(ctx, objLayer, obj, restoredObject)
	}
	return applyExpiryOnNonTransitionedObjects(ctx, objLayer, obj, applyOnVersion)
}

// Perform actions (removal or transitioning of objects), return true the action is successfully performed
func applyLifecycleAction(ctx context.Context, action lifecycle.Action, objLayer ObjectLayer, obj ObjectInfo) (success bool) {
	switch action {
	case lifecycle.DeleteVersionAction, lifecycle.DeleteAction:
		success = applyExpiryRule(ctx, objLayer, obj, false, action == lifecycle.DeleteVersionAction)
	case lifecycle.DeleteRestoredAction, lifecycle.DeleteRestoredVersionAction:
		success = applyExpiryRule(ctx, objLayer, obj, true, action == lifecycle.DeleteRestoredVersionAction)
	case lifecycle.TransitionAction, lifecycle.TransitionVersionAction:
		success = applyTransitionAction(ctx, action, objLayer, obj)
	}
	return
}

// objectPath returns the prefix and object name.
func (i *scannerItem) objectPath() string {
	return path.Join(i.prefix, i.objectName)
}

// healReplication will heal a scanned item that has failed replication.
func (i *scannerItem) healReplication(ctx context.Context, o ObjectLayer, oi ObjectInfo, sizeS *sizeSummary) {
	existingObjResync := i.replication.Resync(ctx, oi)
	if oi.DeleteMarker || !oi.VersionPurgeStatus.Empty() {
		// heal delete marker replication failure or versioned delete replication failure
		if oi.ReplicationStatus == replication.Pending ||
			oi.ReplicationStatus == replication.Failed ||
			oi.VersionPurgeStatus == Failed || oi.VersionPurgeStatus == Pending {
			i.healReplicationDeletes(ctx, o, oi, existingObjResync)
			return
		}
		// if replication status is Complete on DeleteMarker and existing object resync required
		if existingObjResync && oi.ReplicationStatus == replication.Completed {
			i.healReplicationDeletes(ctx, o, oi, existingObjResync)
			return
		}
		return
	}
	roi := ReplicateObjectInfo{ObjectInfo: oi, OpType: replication.HealReplicationType}
	if existingObjResync {
		roi.OpType = replication.ExistingObjectReplicationType
		roi.ResetID = i.replication.ResetID
	}
	switch oi.ReplicationStatus {
	case replication.Pending:
		sizeS.pendingCount++
		sizeS.pendingSize += oi.Size
		globalReplicationPool.queueReplicaTask(roi)
		return
	case replication.Failed:
		sizeS.failedSize += oi.Size
		sizeS.failedCount++
		globalReplicationPool.queueReplicaTask(roi)
		return
	case replication.Completed, "COMPLETE":
		sizeS.replicatedSize += oi.Size
	case replication.Replica:
		sizeS.replicaSize += oi.Size
	}
	if existingObjResync {
		globalReplicationPool.queueReplicaTask(roi)
	}
}

// healReplicationDeletes will heal a scanned deleted item that failed to replicate deletes.
func (i *scannerItem) healReplicationDeletes(ctx context.Context, o ObjectLayer, oi ObjectInfo, existingObject bool) {
	// handle soft delete and permanent delete failures here.
	if oi.DeleteMarker || !oi.VersionPurgeStatus.Empty() {
		versionID := ""
		dmVersionID := ""
		if oi.VersionPurgeStatus.Empty() {
			dmVersionID = oi.VersionID
		} else {
			versionID = oi.VersionID
		}
		doi := DeletedObjectReplicationInfo{
			DeletedObject: DeletedObject{
				ObjectName:                    oi.Name,
				DeleteMarkerVersionID:         dmVersionID,
				VersionID:                     versionID,
				DeleteMarkerReplicationStatus: string(oi.ReplicationStatus),
				DeleteMarkerMTime:             DeleteMarkerMTime{oi.ModTime},
				DeleteMarker:                  oi.DeleteMarker,
				VersionPurgeStatus:            oi.VersionPurgeStatus,
			},
			Bucket: oi.Bucket,
		}
		if existingObject {
			doi.OpType = replication.ExistingObjectReplicationType
			doi.ResetID = i.replication.ResetID
		}
		globalReplicationPool.queueReplicaDeleteTask(doi)
	}
}

type dynamicSleeper struct {
	mu sync.RWMutex

	// Sleep factor
	factor float64

	// maximum sleep cap,
	// set to <= 0 to disable.
	maxSleep time.Duration

	// Don't sleep at all, if time taken is below this value.
	// This is to avoid too small costly sleeps.
	minSleep time.Duration

	// cycle will be closed
	cycle chan struct{}
}

// newDynamicSleeper
func newDynamicSleeper(factor float64, maxWait time.Duration) *dynamicSleeper {
	return &dynamicSleeper{
		factor:   factor,
		cycle:    make(chan struct{}),
		maxSleep: maxWait,
		minSleep: 100 * time.Microsecond,
	}
}

// Timer returns a timer that has started.
// When the returned function is called it will wait.
func (d *dynamicSleeper) Timer(ctx context.Context) func() {
	t := time.Now()
	return func() {
		doneAt := time.Now()
		for {
			// Grab current values
			d.mu.RLock()
			minWait, maxWait := d.minSleep, d.maxSleep
			factor := d.factor
			cycle := d.cycle
			d.mu.RUnlock()
			elapsed := doneAt.Sub(t)
			// Don't sleep for really small amount of time
			wantSleep := time.Duration(float64(elapsed) * factor)
			if wantSleep <= minWait {
				return
			}
			if maxWait > 0 && wantSleep > maxWait {
				wantSleep = maxWait
			}
			timer := time.NewTimer(wantSleep)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
				return
			case <-cycle:
				if !timer.Stop() {
					// We expired.
					<-timer.C
					return
				}
			}
		}
	}
}

// Sleep sleeps the specified time multiplied by the sleep factor.
// If the factor is updated the sleep will be done again with the new factor.
func (d *dynamicSleeper) Sleep(ctx context.Context, base time.Duration) {
	for {
		// Grab current values
		d.mu.RLock()
		minWait, maxWait := d.minSleep, d.maxSleep
		factor := d.factor
		cycle := d.cycle
		d.mu.RUnlock()
		// Don't sleep for really small amount of time
		wantSleep := time.Duration(float64(base) * factor)
		if wantSleep <= minWait {
			return
		}
		if maxWait > 0 && wantSleep > maxWait {
			wantSleep = maxWait
		}
		timer := time.NewTimer(wantSleep)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			return
		case <-cycle:
			if !timer.Stop() {
				// We expired.
				<-timer.C
				return
			}
		}
	}
}

// Update the current settings and cycle all waiting.
// Parameters are the same as in the contructor.
func (d *dynamicSleeper) Update(factor float64, maxWait time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if math.Abs(d.factor-factor) < 1e-10 && d.maxSleep == maxWait {
		return nil
	}
	// Update values and cycle waiting.
	close(d.cycle)
	d.factor = factor
	d.maxSleep = maxWait
	d.cycle = make(chan struct{})
	return nil
}

// ILMExpiryActivity - activity trail for ILM expiry
const ILMExpiryActivity = "ilm:expiry"

func auditLogLifecycle(ctx context.Context, bucket, object string) {
	auditLogInternal(ctx, bucket, object, AuditLogOptions{
		Trigger: ILMExpiryActivity,
		APIName: "s3:ExpireObject",
	})
}
