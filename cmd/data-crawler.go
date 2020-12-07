/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/config/heal"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/bucket/replication"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
	"github.com/willf/bloom"
)

const (
	dataCrawlSleepPerFolder  = time.Millisecond // Time to wait between folders.
	dataCrawlStartDelay      = 5 * time.Minute  // Time to wait on startup and between cycles.
	dataUsageUpdateDirCycles = 16               // Visit all folders every n cycles.

	healDeleteDangling    = true
	healFolderIncludeProb = 32  // Include a clean folder one in n cycles.
	healObjectSelectProb  = 512 // Overall probability of a file being scanned; one in n.
)

var (
	globalHealConfig   heal.Config
	globalHealConfigMu sync.Mutex

	dataCrawlerLeaderLockTimeout = newDynamicTimeout(30*time.Second, 10*time.Second)
	// Sleeper values are updated when config is loaded.
	crawlerSleeper = newDynamicSleeper(10, 10*time.Second)
)

// initDataCrawler will start the crawler unless disabled.
func initDataCrawler(ctx context.Context, objAPI ObjectLayer) {
	if env.Get(envDataUsageCrawlConf, config.EnableOn) == config.EnableOn {
		go runDataCrawler(ctx, objAPI)
	}
}

// runDataCrawler will start a data crawler.
// The function will block until the context is canceled.
// There should only ever be one crawler running per cluster.
func runDataCrawler(ctx context.Context, objAPI ObjectLayer) {
	// Make sure only 1 crawler is running on the cluster.
	locker := objAPI.NewNSLock(minioMetaBucket, "runDataCrawler.lock")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		err := locker.GetLock(ctx, dataCrawlerLeaderLockTimeout)
		if err != nil {
			time.Sleep(time.Duration(r.Float64() * float64(dataCrawlStartDelay)))
			continue
		}
		break
		// No unlock for "leader" lock.
	}

	// Load current bloom cycle
	nextBloomCycle := intDataUpdateTracker.current() + 1
	var buf bytes.Buffer
	err := objAPI.GetObject(ctx, dataUsageBucket, dataUsageBloomName, 0, -1, &buf, "", ObjectOptions{})
	if err != nil {
		if !isErrObjectNotFound(err) && !isErrBucketNotFound(err) {
			logger.LogIf(ctx, err)
		}
	} else {
		if buf.Len() == 8 {
			nextBloomCycle = binary.LittleEndian.Uint64(buf.Bytes())
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(dataCrawlStartDelay):
			// Wait before starting next cycle and wait on startup.
			results := make(chan DataUsageInfo, 1)
			go storeDataUsageInBackend(ctx, objAPI, results)
			bf, err := globalNotificationSys.updateBloomFilter(ctx, nextBloomCycle)
			logger.LogIf(ctx, err)
			err = objAPI.CrawlAndGetDataUsage(ctx, bf, results)
			close(results)
			logger.LogIf(ctx, err)
			if err == nil {
				// Store new cycle...
				nextBloomCycle++
				var tmp [8]byte
				binary.LittleEndian.PutUint64(tmp[:], nextBloomCycle)
				r, err := hash.NewReader(bytes.NewReader(tmp[:]), int64(len(tmp)), "", "", int64(len(tmp)), false)
				if err != nil {
					logger.LogIf(ctx, err)
					continue
				}

				_, err = objAPI.PutObject(ctx, dataUsageBucket, dataUsageBloomName, NewPutObjReader(r, nil, nil), ObjectOptions{})
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
	root       string
	getSize    getSizeFn
	oldCache   dataUsageCache
	newCache   dataUsageCache
	withFilter *bloomFilter

	dataUsageCrawlDebug bool
	healFolderInclude   uint32 // Include a clean folder one in n cycles.
	healObjectSelect    uint32 // Do a heal check on an object once every n cycles. Must divide into healFolderInclude

	newFolders      []cachedFolder
	existingFolders []cachedFolder
	disks           []StorageAPI
}

// crawlDataFolder will crawl the basepath+cache.Info.Name and return an updated cache.
// The returned cache will always be valid, but may not be updated from the existing.
// Before each operation sleepDuration is called which can be used to temporarily halt the crawler.
// If the supplied context is canceled the function will return at the first chance.
func crawlDataFolder(ctx context.Context, basePath string, cache dataUsageCache, getSize getSizeFn) (dataUsageCache, error) {
	t := UTCNow()

	logPrefix := color.Green("data-usage: ")
	logSuffix := color.Blue(" - %v + %v", basePath, cache.Info.Name)
	if intDataUpdateTracker.debug {
		defer func() {
			logger.Info(logPrefix+" Crawl time: %v"+logSuffix, time.Since(t))
		}()

	}

	switch cache.Info.Name {
	case "", dataUsageRoot:
		return cache, errors.New("internal error: root scan attempted")
	}

	s := folderScanner{
		root:                basePath,
		getSize:             getSize,
		oldCache:            cache,
		newCache:            dataUsageCache{Info: cache.Info},
		newFolders:          nil,
		existingFolders:     nil,
		dataUsageCrawlDebug: intDataUpdateTracker.debug,
		healFolderInclude:   0,
		healObjectSelect:    0,
	}

	// Add disks for set healing.
	if len(cache.Disks) > 0 {
		objAPI, ok := newObjectLayerFn().(*erasureServerPools)
		if ok {
			s.disks = objAPI.GetDisksID(cache.Disks...)
			if len(s.disks) != len(cache.Disks) {
				logger.Info(logPrefix+"Missing disks, want %d, found %d. Cannot heal."+logSuffix, len(cache.Disks), len(s.disks))
				s.disks = s.disks[:0]
			}
		}
	}

	// Enable healing in XL mode.
	if globalIsErasure {
		// Include a clean folder one in n cycles.
		s.healFolderInclude = healFolderIncludeProb
		// Do a heal check on an object once every n cycles. Must divide into healFolderInclude
		s.healObjectSelect = healObjectSelectProb
	}
	if len(cache.Info.BloomFilter) > 0 {
		s.withFilter = &bloomFilter{BloomFilter: &bloom.BloomFilter{}}
		_, err := s.withFilter.ReadFrom(bytes.NewBuffer(cache.Info.BloomFilter))
		if err != nil {
			logger.LogIf(ctx, err, logPrefix+"Error reading bloom filter")
			s.withFilter = nil
		}
	}
	if s.dataUsageCrawlDebug {
		logger.Info(logPrefix+"Start crawling. Bloom filter: %v"+logSuffix, s.withFilter != nil)
	}

	done := ctx.Done()
	var flattenLevels = 2

	if s.dataUsageCrawlDebug {
		logger.Info(logPrefix+"Cycle: %v, Entries: %v"+logSuffix, cache.Info.NextCycle, len(cache.Cache))
	}

	// Always scan flattenLevels deep. Cache root is level 0.
	todo := []cachedFolder{{name: cache.Info.Name, objectHealProbDiv: 1}}
	for i := 0; i < flattenLevels; i++ {
		if s.dataUsageCrawlDebug {
			logger.Info(logPrefix+"Level %v, scanning %v directories."+logSuffix, i, len(todo))
		}
		select {
		case <-done:
			return cache, ctx.Err()
		default:
		}
		var err error
		todo, err = s.scanQueuedLevels(ctx, todo, i == flattenLevels-1)
		if err != nil {
			// No useful information...
			return cache, err
		}
	}

	if s.dataUsageCrawlDebug {
		logger.Info(logPrefix+"New folders: %v"+logSuffix, s.newFolders)
	}

	// Add new folders first
	for _, folder := range s.newFolders {
		select {
		case <-done:
			return s.newCache, ctx.Err()
		default:
		}
		du, err := s.deepScanFolder(ctx, folder)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		if du == nil {
			logger.Info(logPrefix + "no disk usage provided" + logSuffix)
			continue
		}

		s.newCache.replace(folder.name, "", *du)
		// Add to parent manually
		if folder.parent != nil {
			parent := s.newCache.Cache[folder.parent.Key()]
			parent.addChildString(folder.name)
		}
	}

	if s.dataUsageCrawlDebug {
		logger.Info(logPrefix+"Existing folders: %v"+logSuffix, len(s.existingFolders))
	}

	// Do selective scanning of existing folders.
	for _, folder := range s.existingFolders {
		select {
		case <-done:
			return s.newCache, ctx.Err()
		default:
		}
		h := hashPath(folder.name)
		if !h.mod(s.oldCache.Info.NextCycle, dataUsageUpdateDirCycles) {
			if !h.mod(s.oldCache.Info.NextCycle, s.healFolderInclude/folder.objectHealProbDiv) {
				s.newCache.replaceHashed(h, folder.parent, s.oldCache.Cache[h.Key()])
				continue
			} else {
				folder.objectHealProbDiv = s.healFolderInclude
			}
			folder.objectHealProbDiv = dataUsageUpdateDirCycles
		}
		if s.withFilter != nil {
			_, prefix := path2BucketObjectWithBasePath(basePath, folder.name)
			if s.oldCache.Info.lifeCycle == nil || !s.oldCache.Info.lifeCycle.HasActiveRules(prefix, true) {
				// If folder isn't in filter, skip it completely.
				if !s.withFilter.containsDir(folder.name) {
					if !h.mod(s.oldCache.Info.NextCycle, s.healFolderInclude/folder.objectHealProbDiv) {
						if s.dataUsageCrawlDebug {
							logger.Info(logPrefix+"Skipping non-updated folder: %v"+logSuffix, folder)
						}
						s.newCache.replaceHashed(h, folder.parent, s.oldCache.Cache[h.Key()])
						continue
					} else {
						if s.dataUsageCrawlDebug {
							logger.Info(logPrefix+"Adding non-updated folder to heal check: %v"+logSuffix, folder.name)
						}
						// Update probability of including objects
						folder.objectHealProbDiv = s.healFolderInclude
					}
				}
			}
		}

		// Update on this cycle...
		du, err := s.deepScanFolder(ctx, folder)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		if du == nil {
			logger.LogIf(ctx, errors.New("data-usage: no disk usage provided"))
			continue
		}
		s.newCache.replaceHashed(h, folder.parent, *du)
	}
	if s.dataUsageCrawlDebug {
		logger.Info(logPrefix+"Finished crawl, %v entries"+logSuffix, len(s.newCache.Cache))
	}
	s.newCache.Info.LastUpdate = UTCNow()
	s.newCache.Info.NextCycle++
	return s.newCache, nil
}

// scanQueuedLevels will scan the provided folders.
// Files found in the folders will be added to f.newCache.
// If final is provided folders will be put into f.newFolders or f.existingFolders.
// If final is not provided the folders found are returned from the function.
func (f *folderScanner) scanQueuedLevels(ctx context.Context, folders []cachedFolder, final bool) ([]cachedFolder, error) {
	var nextFolders []cachedFolder
	done := ctx.Done()
	for _, folder := range folders {
		select {
		case <-done:
			return nil, ctx.Err()
		default:
		}
		thisHash := hashPath(folder.name)
		existing := f.oldCache.findChildrenCopy(thisHash)

		// If there are lifecycle rules for the prefix, remove the filter.
		filter := f.withFilter
		var activeLifeCycle *lifecycle.Lifecycle
		if f.oldCache.Info.lifeCycle != nil {
			_, prefix := path2BucketObjectWithBasePath(f.root, folder.name)
			if f.oldCache.Info.lifeCycle.HasActiveRules(prefix, true) {
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("folder-scanner:")+" Prefix %q has active rules", prefix)
				}
				activeLifeCycle = f.oldCache.Info.lifeCycle
				filter = nil
			}
		}
		if _, ok := f.oldCache.Cache[thisHash.Key()]; filter != nil && ok {
			// If folder isn't in filter and we have data, skip it completely.
			if folder.name != dataUsageRoot && !filter.containsDir(folder.name) {
				if !thisHash.mod(f.oldCache.Info.NextCycle, f.healFolderInclude/folder.objectHealProbDiv) {
					f.newCache.copyWithChildren(&f.oldCache, thisHash, folder.parent)
					if f.dataUsageCrawlDebug {
						logger.Info(color.Green("folder-scanner:")+" Skipping non-updated folder: %v", folder.name)
					}
					continue
				} else {
					if f.dataUsageCrawlDebug {
						logger.Info(color.Green("folder-scanner:")+" Adding non-updated folder to heal check: %v", folder.name)
					}
					// If probability was already crawlerHealFolderInclude, keep it.
					folder.objectHealProbDiv = f.healFolderInclude
				}
			}
		}
		crawlerSleeper.Sleep(ctx, dataCrawlSleepPerFolder)

		cache := dataUsageEntry{}

		err := readDirFn(path.Join(f.root, folder.name), func(entName string, typ os.FileMode) error {
			// Parse
			entName = path.Clean(path.Join(folder.name, entName))
			bucket, prefix := path2BucketObjectWithBasePath(f.root, entName)
			if bucket == "" {
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("folder-scanner:")+" no bucket (%s,%s)", f.root, entName)
				}
				return nil
			}

			if isReservedOrInvalidBucket(bucket, false) {
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("folder-scanner:")+" invalid bucket: %v, entry: %v", bucket, entName)
				}
				return nil
			}

			select {
			case <-done:
				return ctx.Err()
			default:
			}

			if typ&os.ModeDir != 0 {
				h := hashPath(entName)
				_, exists := f.oldCache.Cache[h.Key()]
				cache.addChildString(entName)

				this := cachedFolder{name: entName, parent: &thisHash, objectHealProbDiv: folder.objectHealProbDiv}
				delete(existing, h.Key())
				cache.addChild(h)
				if final {
					if exists {
						f.existingFolders = append(f.existingFolders, this)
					} else {
						f.newFolders = append(f.newFolders, this)
					}
				} else {
					nextFolders = append(nextFolders, this)
				}
				return nil
			}

			// Dynamic time delay.
			wait := crawlerSleeper.Timer(ctx)

			// Get file size, ignore errors.
			item := crawlItem{
				Path:       path.Join(f.root, entName),
				Typ:        typ,
				bucket:     bucket,
				prefix:     path.Dir(prefix),
				objectName: path.Base(entName),
				debug:      f.dataUsageCrawlDebug,
				lifeCycle:  activeLifeCycle,
				heal:       thisHash.mod(f.oldCache.Info.NextCycle, f.healObjectSelect/folder.objectHealProbDiv),
			}
			sizeSummary, err := f.getSize(item)

			wait()
			if err == errSkipFile {
				return nil
			}
			logger.LogIf(ctx, err)
			cache.addSizes(sizeSummary)
			cache.Objects++
			cache.ObjSizes.add(sizeSummary.totalSize)

			return nil
		})
		if err != nil {
			return nil, err
		}

		if f.healObjectSelect == 0 {
			// If we are not scanning, return now.
			f.newCache.replaceHashed(thisHash, folder.parent, cache)
			continue
		}

		objAPI, ok := newObjectLayerFn().(*erasureServerPools)
		if !ok || len(f.disks) == 0 {
			continue
		}

		bgSeq, found := globalBackgroundHealState.getHealSequenceByToken(bgHealingUUID)
		if !found {
			continue
		}

		// Whatever remains in 'existing' are folders at this level
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

		for k := range existing {
			bucket, prefix := path2BucketObject(k)
			if f.dataUsageCrawlDebug {
				logger.Info(color.Green("folder-scanner:")+" checking disappeared folder: %v/%v", bucket, prefix)
			}

			// Dynamic time delay.
			wait := crawlerSleeper.Timer(ctx)
			resolver.bucket = bucket

			foundObjs := false
			dangling := true
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
					if f.dataUsageCrawlDebug {
						logger.Info(color.Green("healObjects:")+" got agreement: %v", entry.name)
					}
					if entry.isObject() {
						dangling = false
					}
				},
				// Some disks have data for this.
				partial: func(entries metaCacheEntries, nAgreed int, errs []error) {
					if f.dataUsageCrawlDebug {
						logger.Info(color.Green("healObjects:")+" got partial, %d agreed, errs: %v", nAgreed, errs)
					}
					// Sleep and reset.
					wait()
					wait = crawlerSleeper.Timer(ctx)
					entry, ok := entries.resolve(&resolver)
					if !ok {
						for _, err := range errs {
							if err != nil {
								// Not all disks are ready, do nothing for now.
								dangling = false
								return
							}
						}

						// If no errors, queue it for healing.
						entry, _ = entries.firstFound()
					}

					if f.dataUsageCrawlDebug {
						logger.Info(color.Green("healObjects:")+" resolved to: %v, dir: %v", entry.name, entry.isDir())
					}
					if entry.isDir() {
						return
					}
					dangling = false
					// We got an entry which we should be able to heal.
					fiv, err := entry.fileInfoVersions(bucket)
					if err != nil {
						err := bgSeq.queueHealTask(healSource{
							bucket:    bucket,
							object:    entry.name,
							versionID: "",
						}, madmin.HealItemObject)
						logger.LogIf(ctx, err)
						foundObjs = foundObjs || err == nil
						return
					}
					for _, ver := range fiv.Versions {
						// Sleep and reset.
						wait()
						wait = crawlerSleeper.Timer(ctx)
						err := bgSeq.queueHealTask(healSource{
							bucket:    bucket,
							object:    fiv.Name,
							versionID: ver.VersionID,
						}, madmin.HealItemObject)
						logger.LogIf(ctx, err)
						foundObjs = foundObjs || err == nil
					}
				},
				// Too many disks failed.
				finished: func(errs []error) {
					if f.dataUsageCrawlDebug {
						logger.Info(color.Green("healObjects:")+" too many errors: %v", errs)
					}
					dangling = false
					cancel()
				},
			})

			if f.dataUsageCrawlDebug && err != nil && err != errFileNotFound {
				logger.Info(color.Green("healObjects:")+" checking returned value %v (%T)", err, err)
			}

			// If we found one or more disks with this folder, delete it.
			if err == nil && dangling {
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("healObjects:")+" deleting dangling directory %s", prefix)
				}
				// If we have quorum, found directories, but no objects, issue heal to delete the dangling.
				objAPI.HealObjects(ctx, bucket, prefix, madmin.HealOpts{Recursive: true, Remove: true},
					func(bucket, object, versionID string) error {
						// Wait for each heal as per crawler frequency.
						wait()
						wait = crawlerSleeper.Timer(ctx)
						return bgSeq.queueHealTask(healSource{
							bucket:    bucket,
							object:    object,
							versionID: versionID,
						}, madmin.HealItemObject)
					})
			}

			wait()

			// Add unless healing returned an error.
			if foundObjs {
				this := cachedFolder{name: k, parent: &thisHash, objectHealProbDiv: folder.objectHealProbDiv}
				cache.addChild(hashPath(k))
				if final {
					f.existingFolders = append(f.existingFolders, this)
				} else {
					nextFolders = append(nextFolders, this)
				}
			}
		}
		f.newCache.replaceHashed(thisHash, folder.parent, cache)
	}
	return nextFolders, nil
}

// deepScanFolder will deep scan a folder and return the size if no error occurs.
func (f *folderScanner) deepScanFolder(ctx context.Context, folder cachedFolder) (*dataUsageEntry, error) {
	var cache dataUsageEntry

	done := ctx.Done()

	var addDir func(entName string, typ os.FileMode) error
	var dirStack = []string{f.root, folder.name}

	addDir = func(entName string, typ os.FileMode) error {
		select {
		case <-done:
			return ctx.Err()
		default:
		}

		if typ&os.ModeDir != 0 {
			dirStack = append(dirStack, entName)
			err := readDirFn(path.Join(dirStack...), addDir)
			dirStack = dirStack[:len(dirStack)-1]
			crawlerSleeper.Sleep(ctx, dataCrawlSleepPerFolder)
			return err
		}

		// Dynamic time delay.
		wait := crawlerSleeper.Timer(ctx)

		// Get file size, ignore errors.
		dirStack = append(dirStack, entName)
		fileName := path.Join(dirStack...)
		dirStack = dirStack[:len(dirStack)-1]

		bucket, prefix := path2BucketObjectWithBasePath(f.root, fileName)
		var activeLifeCycle *lifecycle.Lifecycle
		if f.oldCache.Info.lifeCycle != nil {
			if f.oldCache.Info.lifeCycle.HasActiveRules(prefix, false) {
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("folder-scanner:")+" Prefix %q has active rules", prefix)
				}
				activeLifeCycle = f.oldCache.Info.lifeCycle
			}
		}

		sizeSummary, err := f.getSize(
			crawlItem{
				Path:       fileName,
				Typ:        typ,
				bucket:     bucket,
				prefix:     path.Dir(prefix),
				objectName: path.Base(entName),
				debug:      f.dataUsageCrawlDebug,
				lifeCycle:  activeLifeCycle,
				heal:       hashPath(path.Join(prefix, entName)).mod(f.oldCache.Info.NextCycle, f.healObjectSelect/folder.objectHealProbDiv),
			})

		// Wait to throttle IO
		wait()

		if err == errSkipFile {
			return nil
		}
		logger.LogIf(ctx, err)
		cache.addSizes(sizeSummary)
		cache.Objects++
		cache.ObjSizes.add(sizeSummary.totalSize)
		return nil
	}
	err := readDirFn(path.Join(dirStack...), addDir)
	if err != nil {
		return nil, err
	}
	return &cache, nil
}

// crawlItem represents each file while walking.
type crawlItem struct {
	Path string
	Typ  os.FileMode

	bucket     string // Bucket.
	prefix     string // Only the prefix if any, does not have final object name.
	objectName string // Only the object name without prefixes.
	lifeCycle  *lifecycle.Lifecycle
	heal       bool // Has the object been selected for heal check?
	debug      bool
}

type sizeSummary struct {
	totalSize      int64
	replicatedSize int64
	pendingSize    int64
	failedSize     int64
	replicaSize    int64
}

type getSizeFn func(item crawlItem) (sizeSummary, error)

// transformMetaDir will transform a directory to prefix/file.ext
func (i *crawlItem) transformMetaDir() {
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
	oi               ObjectInfo
	successorModTime time.Time // The modtime of the successor version
	numVersions      int       // The number of versions of this object
}

// applyActions will apply lifecycle checks on to a scanned item.
// The resulting size on disk will always be returned.
// The metadata will be compared to consensus on the object layer before any changes are applied.
// If no metadata is supplied, -1 is returned if no action is taken.
func (i *crawlItem) applyActions(ctx context.Context, o ObjectLayer, meta actionMeta) (size int64) {
	size, err := meta.oi.GetActualSize()
	if i.debug {
		logger.LogIf(ctx, err)
	}
	if i.heal {
		if i.debug {
			logger.Info(color.Green("applyActions:")+" heal checking: %v/%v v%s", i.bucket, i.objectPath(), meta.oi.VersionID)
		}
		res, err := o.HealObject(ctx, i.bucket, i.objectPath(), meta.oi.VersionID, madmin.HealOpts{Remove: healDeleteDangling})
		if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
			return 0
		}
		if err != nil && !errors.Is(err, NotImplemented{}) {
			logger.LogIf(ctx, err)
			return 0
		}
		size = res.ObjectSize
	}
	if i.lifeCycle == nil {
		return size
	}

	versionID := meta.oi.VersionID
	action := i.lifeCycle.ComputeAction(
		lifecycle.ObjectOpts{
			Name:             i.objectPath(),
			UserTags:         meta.oi.UserTags,
			ModTime:          meta.oi.ModTime,
			VersionID:        meta.oi.VersionID,
			DeleteMarker:     meta.oi.DeleteMarker,
			IsLatest:         meta.oi.IsLatest,
			NumVersions:      meta.numVersions,
			SuccessorModTime: meta.successorModTime,
			RestoreOngoing:   meta.oi.RestoreOngoing,
			RestoreExpires:   meta.oi.RestoreExpires,
			TransitionStatus: meta.oi.TransitionStatus,
		})
	if i.debug {
		logger.Info(color.Green("applyActions:")+" lifecycle: %q (version-id=%s), Initial scan: %v", i.objectPath(), versionID, action)
	}
	switch action {
	case lifecycle.DeleteAction, lifecycle.DeleteVersionAction:
	case lifecycle.TransitionAction, lifecycle.TransitionVersionAction:
	case lifecycle.DeleteRestoredAction, lifecycle.DeleteRestoredVersionAction:
	default:
		// No action.
		return size
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
				return size
			}
		case ObjectNotFound:
			// object not found return 0
			return 0
		default:
			// All other errors proceed.
			logger.LogIf(ctx, err)
			return size
		}
	}
	size = obj.Size

	// Recalculate action.
	lcOpts := lifecycle.ObjectOpts{
		Name:             i.objectPath(),
		UserTags:         obj.UserTags,
		ModTime:          obj.ModTime,
		VersionID:        obj.VersionID,
		DeleteMarker:     obj.DeleteMarker,
		IsLatest:         obj.IsLatest,
		NumVersions:      meta.numVersions,
		SuccessorModTime: meta.successorModTime,
		RestoreOngoing:   obj.RestoreOngoing,
		RestoreExpires:   obj.RestoreExpires,
		TransitionStatus: obj.TransitionStatus,
	}
	action = i.lifeCycle.ComputeAction(lcOpts)

	if i.debug {
		logger.Info(color.Green("applyActions:")+" lifecycle: Secondary scan: %v", action)
	}
	switch action {
	case lifecycle.DeleteAction, lifecycle.DeleteVersionAction:
	case lifecycle.TransitionAction, lifecycle.TransitionVersionAction:
	case lifecycle.DeleteRestoredAction, lifecycle.DeleteRestoredVersionAction:
	default:
		// No action.
		return size
	}

	opts := ObjectOptions{}
	switch action {
	case lifecycle.DeleteVersionAction, lifecycle.DeleteRestoredVersionAction:
		// Defensive code, should never happen
		if obj.VersionID == "" {
			return size
		}
		if rcfg, _ := globalBucketObjectLockSys.Get(i.bucket); rcfg.LockEnabled {
			locked := enforceRetentionForDeletion(ctx, obj)
			if locked {
				if i.debug {
					logger.Info(color.Green("applyActions:")+" lifecycle: %s is locked, not deleting", i.objectPath())
				}
				return size
			}
		}
		opts.VersionID = obj.VersionID
	case lifecycle.DeleteAction, lifecycle.DeleteRestoredAction:
		opts.Versioned = globalBucketVersioningSys.Enabled(i.bucket)
	case lifecycle.TransitionAction, lifecycle.TransitionVersionAction:
		if obj.TransitionStatus == "" {
			opts.Versioned = globalBucketVersioningSys.Enabled(obj.Bucket)
			opts.VersionID = obj.VersionID
			opts.TransitionStatus = lifecycle.TransitionPending
			if _, err = o.DeleteObject(ctx, obj.Bucket, obj.Name, opts); err != nil {
				// Assume it is still there.
				logger.LogIf(ctx, err)
				return size
			}
		}
		globalTransitionState.queueTransitionTask(obj)
		return 0
	}
	if obj.TransitionStatus != "" {
		if err := deleteTransitionedObject(ctx, o, i.bucket, i.objectPath(), lcOpts, action, false); err != nil {
			logger.LogIf(ctx, err)
			return size
		}
	} else {
		obj, err = o.DeleteObject(ctx, i.bucket, i.objectPath(), opts)
		if err != nil {
			// Assume it is still there.
			logger.LogIf(ctx, err)
			return size
		}
	}

	eventName := event.ObjectRemovedDelete
	if obj.DeleteMarker {
		eventName = event.ObjectRemovedDeleteMarkerCreated
	}

	// Notify object deleted event.
	sendEvent(eventArgs{
		EventName:  eventName,
		BucketName: i.bucket,
		Object:     obj,
		Host:       "Internal: [ILM-EXPIRY]",
	})
	return 0
}

// objectPath returns the prefix and object name.
func (i *crawlItem) objectPath() string {
	return path.Join(i.prefix, i.objectName)
}

// healReplication will heal a scanned item that has failed replication.
func (i *crawlItem) healReplication(ctx context.Context, o ObjectLayer, meta actionMeta, sizeS *sizeSummary) {
	if meta.oi.DeleteMarker || !meta.oi.VersionPurgeStatus.Empty() {
		//heal delete marker replication failure or versioned delete replication failure
		if meta.oi.ReplicationStatus == replication.Pending ||
			meta.oi.ReplicationStatus == replication.Failed ||
			meta.oi.VersionPurgeStatus == Failed || meta.oi.VersionPurgeStatus == Pending {
			i.healReplicationDeletes(ctx, o, meta)
			return
		}
	}
	switch meta.oi.ReplicationStatus {
	case replication.Pending:
		sizeS.pendingSize += meta.oi.Size
		globalReplicationState.queueReplicaTask(meta.oi)
	case replication.Failed:
		sizeS.failedSize += meta.oi.Size
		globalReplicationState.queueReplicaTask(meta.oi)
	case replication.Complete:
		sizeS.replicatedSize += meta.oi.Size
	case replication.Replica:
		sizeS.replicaSize += meta.oi.Size
	}
}

// healReplicationDeletes will heal a scanned deleted item that failed to replicate deletes.
func (i *crawlItem) healReplicationDeletes(ctx context.Context, o ObjectLayer, meta actionMeta) {
	// handle soft delete and permanent delete failures here.
	if meta.oi.DeleteMarker || !meta.oi.VersionPurgeStatus.Empty() {
		versionID := ""
		dmVersionID := ""
		if meta.oi.VersionPurgeStatus.Empty() {
			dmVersionID = meta.oi.VersionID
		} else {
			versionID = meta.oi.VersionID
		}
		globalReplicationState.queueReplicaDeleteTask(DeletedObjectVersionInfo{
			DeletedObject: DeletedObject{
				ObjectName:                    meta.oi.Name,
				DeleteMarkerVersionID:         dmVersionID,
				VersionID:                     versionID,
				DeleteMarkerReplicationStatus: string(meta.oi.ReplicationStatus),
				DeleteMarkerMTime:             DeleteMarkerMTime{meta.oi.ModTime},
				DeleteMarker:                  meta.oi.DeleteMarker,
				VersionPurgeStatus:            meta.oi.VersionPurgeStatus,
			},
			Bucket: meta.oi.Bucket,
		})
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
