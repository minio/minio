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
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/config/crawler"
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
	dataCrawlSleepDefMult    = 10.0             // Default multiplier for waits between operations.
	dataCrawlStartDelay      = 5 * time.Minute  // Time to wait on startup and between cycles.
	dataUsageUpdateDirCycles = 16               // Visit all folders every n cycles.

	healDeleteDangling    = true
	healFolderIncludeProb = 32  // Include a clean folder one in n cycles.
	healObjectSelectProb  = 512 // Overall probability of a file being scanned; one in n.
)

var (
	globalCrawlerConfig          crawler.Config
	dataCrawlerLeaderLockTimeout = newDynamicTimeout(30*time.Second, 10*time.Second)
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
	locker := objAPI.NewNSLock(ctx, minioMetaBucket, "runDataCrawler.lock")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		err := locker.GetLock(dataCrawlerLeaderLockTimeout)
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
		case <-time.NewTimer(dataCrawlStartDelay).C:
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
	root               string
	getSize            getSizeFn
	oldCache           dataUsageCache
	newCache           dataUsageCache
	withFilter         *bloomFilter
	waitForLowActiveIO func()

	dataUsageCrawlMult  float64
	dataUsageCrawlDebug bool
	healFolderInclude   uint32 // Include a clean folder one in n cycles.
	healObjectSelect    uint32 // Do a heal check on an object once every n cycles. Must divide into healFolderInclude

	newFolders      []cachedFolder
	existingFolders []cachedFolder
}

// crawlDataFolder will crawl the basepath+cache.Info.Name and return an updated cache.
// The returned cache will always be valid, but may not be updated from the existing.
// Before each operation waitForLowActiveIO is called which can be used to temporarily halt the crawler.
// If the supplied context is canceled the function will return at the first chance.
func crawlDataFolder(ctx context.Context, basePath string, cache dataUsageCache, waitForLowActiveIO func(), getSize getSizeFn) (dataUsageCache, error) {
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

	delayMult, err := strconv.ParseFloat(env.Get(envDataUsageCrawlDelay, "10.0"), 64)
	if err != nil {
		logger.LogIf(ctx, err)
		delayMult = dataCrawlSleepDefMult
	}

	s := folderScanner{
		root:                basePath,
		getSize:             getSize,
		oldCache:            cache,
		newCache:            dataUsageCache{Info: cache.Info},
		waitForLowActiveIO:  waitForLowActiveIO,
		newFolders:          nil,
		existingFolders:     nil,
		dataUsageCrawlMult:  delayMult,
		dataUsageCrawlDebug: intDataUpdateTracker.debug,
		healFolderInclude:   0,
		healObjectSelect:    0,
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
		f.waitForLowActiveIO()
		sleepDuration(dataCrawlSleepPerFolder, f.dataUsageCrawlMult)

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
			f.waitForLowActiveIO()
			// Dynamic time delay.
			t := UTCNow()

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
			size, err := f.getSize(item)

			sleepDuration(time.Since(t), f.dataUsageCrawlMult)
			if err == errSkipFile {
				return nil
			}
			logger.LogIf(ctx, err)
			cache.Size += size
			cache.Objects++
			cache.ObjSizes.add(size)

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

		objAPI := newObjectLayerFn()
		if objAPI == nil {
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
		for k := range existing {
			f.waitForLowActiveIO()
			bucket, prefix := path2BucketObject(k)
			if f.dataUsageCrawlDebug {
				logger.Info(color.Green("folder-scanner:")+" checking disappeared folder: %v/%v", bucket, prefix)
			}

			err = objAPI.HealObjects(ctx, bucket, prefix, madmin.HealOpts{Recursive: true, Remove: healDeleteDangling},
				func(bucket, object, versionID string) error {
					return bgSeq.queueHealTask(healSource{
						bucket:    bucket,
						object:    object,
						versionID: versionID,
					}, madmin.HealItemObject)
				})

			if f.dataUsageCrawlDebug && err != nil {
				logger.Info(color.Green("healObjects:")+" checking returned value %v", err)
			}

			// Add unless healing returned an error.
			if err == nil {
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

		f.waitForLowActiveIO()
		if typ&os.ModeDir != 0 {
			dirStack = append(dirStack, entName)
			err := readDirFn(path.Join(dirStack...), addDir)
			dirStack = dirStack[:len(dirStack)-1]
			sleepDuration(dataCrawlSleepPerFolder, f.dataUsageCrawlMult)
			return err
		}

		// Dynamic time delay.
		t := UTCNow()

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

		size, err := f.getSize(
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

		// Don't sleep for really small amount of time
		sleepDuration(time.Since(t), f.dataUsageCrawlMult)

		if err == errSkipFile {
			return nil
		}
		logger.LogIf(ctx, err)
		cache.Size += size
		cache.Objects++
		cache.ObjSizes.add(size)
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

type getSizeFn func(item crawlItem) (int64, error)

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
		})
	if i.debug {
		logger.Info(color.Green("applyActions:")+" lifecycle: %q (version-id=%s), Initial scan: %v", i.objectPath(), versionID, action)
	}
	switch action {
	case lifecycle.DeleteAction, lifecycle.DeleteVersionAction:
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
	action = i.lifeCycle.ComputeAction(
		lifecycle.ObjectOpts{
			Name:             i.objectPath(),
			UserTags:         obj.UserTags,
			ModTime:          obj.ModTime,
			VersionID:        obj.VersionID,
			DeleteMarker:     obj.DeleteMarker,
			IsLatest:         obj.IsLatest,
			NumVersions:      meta.numVersions,
			SuccessorModTime: meta.successorModTime,
		})
	if i.debug {
		logger.Info(color.Green("applyActions:")+" lifecycle: Secondary scan: %v", action)
	}
	switch action {
	case lifecycle.DeleteAction, lifecycle.DeleteVersionAction:
	default:
		// No action.
		return size
	}

	opts := ObjectOptions{}
	switch action {
	case lifecycle.DeleteVersionAction:
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
	case lifecycle.DeleteAction:
		opts.Versioned = globalBucketVersioningSys.Enabled(i.bucket)
	}

	obj, err = o.DeleteObject(ctx, i.bucket, i.objectPath(), opts)
	if err != nil {
		// Assume it is still there.
		logger.LogIf(ctx, err)
		return size
	}

	// Notify object deleted event.
	sendEvent(eventArgs{
		EventName:  event.ObjectRemovedDelete,
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

// sleepDuration multiplies the duration d by x and sleeps if is more than 100 micro seconds.
// sleep is limited to max 1 second.
func sleepDuration(d time.Duration, x float64) {
	// Don't sleep for really small amount of time
	if d := time.Duration(float64(d) * x); d > time.Microsecond*100 {
		if d > time.Second {
			d = time.Second
		}
		time.Sleep(d)
	}
}

// healReplication will heal a scanned item that has failed replication.
func (i *crawlItem) healReplication(ctx context.Context, o ObjectLayer, meta actionMeta) {
	if meta.oi.ReplicationStatus == replication.Pending ||
		meta.oi.ReplicationStatus == replication.Failed {
		globalReplicationState.queueReplicaTask(meta.oi)
	}
}
