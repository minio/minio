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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/cmd/config"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/hash"
	"github.com/willf/bloom"
)

const (
	envDataUsageCrawlConf  = "MINIO_DISK_USAGE_CRAWL_ENABLE"
	envDataUsageCrawlDelay = "MINIO_DISK_USAGE_CRAWL_DELAY"
	envDataUsageCrawlDebug = "MINIO_DISK_USAGE_CRAWL_DEBUG"

	dataUsageRoot   = SlashSeparator
	dataUsageBucket = minioMetaBucket + SlashSeparator + bucketMetaPrefix

	dataUsageObjName         = ".usage.json"
	dataUsageCacheName       = ".usage-cache.bin"
	dataUsageBloomName       = ".bloomcycle.bin"
	dataUsageSleepPerFolder  = 1 * time.Millisecond
	dataUsageSleepDefMult    = 10.0
	dataUsageUpdateDirCycles = 16
	// FIXME: Set back to 5 mins when testing is done.
	dataUsageStartDelay = 5 * time.Second // Time to wait on startup and between cycles.
)

// initDataUsageStats will start the crawler unless disabled.
func initDataUsageStats(ctx context.Context, objAPI ObjectLayer) {
	if env.Get(envDataUsageCrawlConf, config.EnableOn) == config.EnableOn {
		go runDataUsageInfo(ctx, objAPI)
	}
}

func runDataUsageInfo(ctx context.Context, objAPI ObjectLayer) {
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
		case <-time.NewTimer(dataUsageStartDelay).C:
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
				if nextBloomCycle%dataUpdateTrackerResetEvery == 0 {
					if intDataUpdateTracker.debug {
						logger.Info(color.Green("runDataUsageInfo:") + " Resetting bloom filter for next runs.")
					}
					nextBloomCycle++
				}
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

// storeDataUsageInBackend will store all objects sent on the gui channel until closed.
func storeDataUsageInBackend(ctx context.Context, objAPI ObjectLayer, gui <-chan DataUsageInfo) {
	for dataUsageInfo := range gui {
		dataUsageJSON, err := json.Marshal(dataUsageInfo)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		size := int64(len(dataUsageJSON))
		r, err := hash.NewReader(bytes.NewReader(dataUsageJSON), size, "", "", size, false)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}

		_, err = objAPI.PutObject(ctx, dataUsageBucket, dataUsageObjName, NewPutObjReader(r, nil, nil), ObjectOptions{})
		if !isErrBucketNotFound(err) {
			logger.LogIf(ctx, err)
		}
	}
}

func loadDataUsageFromBackend(ctx context.Context, objAPI ObjectLayer) (DataUsageInfo, error) {
	var dataUsageInfoJSON bytes.Buffer

	err := objAPI.GetObject(ctx, dataUsageBucket, dataUsageObjName, 0, -1, &dataUsageInfoJSON, "", ObjectOptions{})
	if err != nil {
		if isErrObjectNotFound(err) || isErrBucketNotFound(err) {
			return DataUsageInfo{}, nil
		}
		return DataUsageInfo{}, toObjectErr(err, dataUsageBucket, dataUsageObjName)
	}

	var dataUsageInfo DataUsageInfo
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err = json.Unmarshal(dataUsageInfoJSON.Bytes(), &dataUsageInfo)
	if err != nil {
		return DataUsageInfo{}, err
	}

	// For forward compatibility reasons, we need to add this code.
	if len(dataUsageInfo.BucketsUsage) == 0 {
		dataUsageInfo.BucketsUsage = make(map[string]BucketUsageInfo, len(dataUsageInfo.BucketSizes))
		for bucket, size := range dataUsageInfo.BucketSizes {
			dataUsageInfo.BucketsUsage[bucket] = BucketUsageInfo{Size: size}
		}
	}

	// For backward compatibility reasons, we need to add this code.
	if len(dataUsageInfo.BucketSizes) == 0 {
		dataUsageInfo.BucketSizes = make(map[string]uint64, len(dataUsageInfo.BucketsUsage))
		for bucket, bui := range dataUsageInfo.BucketsUsage {
			dataUsageInfo.BucketSizes[bucket] = bui.Size
		}
	}

	return dataUsageInfo, nil
}

// Item represents each file while walking.
type Item struct {
	Path string
	Typ  os.FileMode

	bucket     string // Bucket.
	prefix     string // Only the prefix if any, does not have final object name.
	objectName string // Only the object name without prefixes.
	lifeCycle  *lifecycle.Lifecycle
	debug      bool
}

type getSizeFn func(item Item) (int64, error)

// actionMeta contains information used to apply actions.
type actionMeta struct {
	oi      ObjectInfo
	trustOI bool // Set true if oi can be trusted and has been read with quorum.
	meta    map[string]string
}

// transformMetaDir will transform a directory to prefix/file.ext
func (i *Item) transformMetaDir() {
	split := strings.Split(i.prefix, SlashSeparator)
	if len(split) > 1 {
		i.prefix = path.Join(split[:len(split)-1]...)
	} else {
		i.prefix = ""
	}
	// Object name is last element
	i.objectName = split[len(split)-1]
}

// applyActions will apply lifecycle checks on to a scanned item.
// The resulting size on disk will always be returned.
// The metadata will be compared to consensus on the object layer before any changes are applied.
// If no metadata is supplied, -1 is returned if no action is taken.
func (i *Item) applyActions(ctx context.Context, o ObjectLayer, meta actionMeta) (size int64) {
	size, err := meta.oi.GetActualSize()
	if i.debug {
		logger.LogIf(ctx, err)
	}
	if i.lifeCycle == nil {
		// logger.Info(color.Green("applyActions:")+" %q: no lifecycle", i.objectPath())
		return size
	}

	action := i.lifeCycle.ComputeAction(i.objectPath(), meta.meta[xhttp.AmzObjectTagging], meta.oi.ModTime)
	logger.Info(color.Green("applyActions:")+" lifecycle: %q, initial scan: %v", i.objectPath(), action)
	switch action {
	case lifecycle.DeleteAction:
	default:
		// No action.
		return size
	}

	// These (expensive) operations should only run on items we are likely to delete.
	// Load to ensure that we have the correct version and not an unsynced version.
	if !meta.trustOI {
		obj, err := o.GetObjectInfo(ctx, i.bucket, i.objectPath(), ObjectOptions{})
		if err != nil {
			// Do nothing - heal in the future.
			logger.LogIf(ctx, err)
			return size
		}
		size = obj.Size

		// Recalculate action.
		action = i.lifeCycle.ComputeAction(i.objectPath(), obj.UserTags, obj.ModTime)
		logger.Info(color.Green("applyActions:")+" lifecycle: secondary scan: %v", action)
		switch action {
		case lifecycle.DeleteAction:
		default:
			// No action.
			return size
		}
	}

	err = o.DeleteObject(ctx, i.bucket, i.objectPath())
	if err != nil {
		// Assume it is still there.
		logger.LogIf(ctx, err)
		return size
	}

	// Notify object deleted event.
	sendEvent(eventArgs{
		EventName:  event.ObjectRemovedDelete,
		BucketName: i.bucket,
		Object: ObjectInfo{
			Name: i.objectPath(),
		},
		Host: "Internal: [ILM-EXPIRY]",
	})
	return 0
}

// objectPath returns the prefix and object name.
func (i *Item) objectPath() string {
	return path.Join(i.prefix, i.objectName)
}

type cachedFolder struct {
	name   string
	parent *dataUsageHash
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

	newFolders      []cachedFolder
	existingFolders []cachedFolder
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

		// If there are lifecycle rules for the prefix, remove the filter.
		filter := f.withFilter
		var activeLifeCycle *lifecycle.Lifecycle
		if f.oldCache.Info.lifeCycle != nil && filter != nil {
			_, prefix := path2BucketObjectWithBasePath(f.root, folder.name)
			if f.oldCache.Info.lifeCycle.HasActiveRules(prefix, true) {
				logger.Info(color.Green("data-usage:")+" Prefix %q has active rules", prefix)
				activeLifeCycle = f.oldCache.Info.lifeCycle
				filter = nil
			}
		}

		if _, ok := f.oldCache.Cache[thisHash.Key()]; filter != nil && ok {
			// If folder isn't in filter and we have data, skip it completely.
			if folder.name != dataUsageRoot && !filter.containsDir(folder.name) {
				f.newCache.copyWithChildren(&f.oldCache, thisHash, folder.parent)
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("data-usage:")+" Skipping non-updated folder: %v", folder.name)
				}
				continue
			}
		}
		f.waitForLowActiveIO()
		sleepDuration(dataUsageSleepPerFolder, f.dataUsageCrawlMult)

		cache := dataUsageEntry{}

		err := readDirFn(path.Join(f.root, folder.name), func(entName string, typ os.FileMode) error {
			// Parse
			entName = path.Clean(path.Join(folder.name, entName))
			bucket, prefix := path2BucketObjectWithBasePath(f.root, entName)
			if bucket == "" {
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("data-usage:")+" no bucket (%s,%s)", f.root, entName)
				}
				return nil
			}

			if isReservedOrInvalidBucket(bucket, false) {
				if f.dataUsageCrawlDebug {
					logger.Info(color.Green("data-usage:")+" invalid bucket: %v, entry: %v", bucket, entName)
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

				this := cachedFolder{name: entName, parent: &thisHash}
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
			item := Item{
				Path:       path.Join(f.root, entName),
				Typ:        typ,
				bucket:     bucket,
				prefix:     path.Dir(prefix),
				objectName: path.Base(entName),
				debug:      f.dataUsageCrawlDebug,
				lifeCycle:  activeLifeCycle,
			}
			size, err := f.getSize(item)

			sleepDuration(time.Since(t), f.dataUsageCrawlMult)
			if err == errSkipFile || err == errFileNotFound {
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
		f.newCache.replaceHashed(thisHash, folder.parent, cache)
	}
	return nextFolders, nil
}

// deepScanFolder will deep scan a folder and return the size if no error occurs.
func (f *folderScanner) deepScanFolder(ctx context.Context, folder string) (*dataUsageEntry, error) {
	var cache dataUsageEntry

	done := ctx.Done()

	var addDir func(entName string, typ os.FileMode) error
	var dirStack = []string{f.root, folder}

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
			sleepDuration(dataUsageSleepPerFolder, f.dataUsageCrawlMult)
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
				logger.Info(color.Green("data-usage:")+" Prefix %q has active rules", prefix)
				activeLifeCycle = f.oldCache.Info.lifeCycle
			}
		}

		size, err := f.getSize(
			Item{
				Path:       fileName,
				Typ:        typ,
				bucket:     bucket,
				prefix:     path.Dir(prefix),
				objectName: path.Base(entName),
				debug:      f.dataUsageCrawlDebug,
				lifeCycle:  activeLifeCycle,
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

// updateUsage will crawl the basepath+cache.Info.Name and return an updated cache.
// The returned cache will always be valid, but may not be updated from the existing.
// Before each operation waitForLowActiveIO is called which can be used to temporarily halt the crawler.
// If the supplied context is canceled the function will return at the first chance.
func updateUsage(ctx context.Context, basePath string, cache dataUsageCache, waitForLowActiveIO func(), getSize getSizeFn) (dataUsageCache, error) {
	t := UTCNow()

	dataUsageDebug := env.Get(envDataUsageCrawlDebug, config.EnableOff) == config.EnableOn
	logPrefix := color.Green("data-usage: ")
	logSuffix := color.Blue(" - %v + %v", basePath, cache.Info.Name)
	if dataUsageDebug {
		defer func() {
			logger.Info(logPrefix+" Crawl time: %v"+logSuffix, time.Since(t))
		}()

	}

	if cache.Info.Name == "" {
		cache.Info.Name = dataUsageRoot
	}

	delayMult, err := strconv.ParseFloat(env.Get(envDataUsageCrawlDelay, "10.0"), 64)
	if err != nil {
		logger.LogIf(ctx, err)
		delayMult = dataUsageSleepDefMult
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
		dataUsageCrawlDebug: dataUsageDebug,
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
	var flattenLevels = 3

	// If we are scanning inside a bucket reduce depth by 1.
	if cache.Info.Name != dataUsageRoot {
		flattenLevels--
	}

	if s.dataUsageCrawlDebug {
		logger.Info(logPrefix+"Cycle: %v, Entries: %v"+logSuffix, cache.Info.NextCycle, len(cache.Cache))
	}

	// Always scan flattenLevels deep. Cache root is level 0.
	todo := []cachedFolder{{name: cache.Info.Name}}
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
		du, err := s.deepScanFolder(ctx, folder.name)
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
			s.newCache.replaceHashed(h, folder.parent, s.oldCache.Cache[h.Key()])
			continue
		}

		if s.withFilter != nil {
			_, prefix := path2BucketObjectWithBasePath(basePath, folder.name)
			if s.oldCache.Info.lifeCycle != nil && s.oldCache.Info.lifeCycle.HasActiveRules(prefix, true) {

				logger.Info(color.Green("data-usage:")+" Prefix %q has active rules", prefix)
				// If folder isn't in filter, skip it completely.
				if !s.withFilter.containsDir(folder.name) {
					if s.dataUsageCrawlDebug {
						logger.Info(logPrefix+"Skipping non-updated folder: %v"+logSuffix, folder)
					}
					s.newCache.replaceHashed(h, folder.parent, s.oldCache.Cache[h.Key()])
					continue
				}
			}
		}

		// Update on this cycle...
		du, err := s.deepScanFolder(ctx, folder.name)
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
