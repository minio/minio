/*
 * (C) 2017 David Gore <dvstate@gmail.com>
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
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/NebulousLabs/Sia/api"
	"github.com/minio/minio-go/pkg/policy"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// SiaCacheLayer represents the cache layer between Minio and Sia network
type SiaCacheLayer struct {
	SiadAddress       string       // Address of siad daemon API. (e.g., "127.0.0.1:9980")
	CacheDir          string       // Cache directory for downloads.
	DbFile            string       // Name and path of database file.
	ManagerDelaySec   int64        // How many seconds to delay between cache/db management operations. (SIA_MANAGER_DELAY_SEC)
	UploadCheckFreqMs int64        // How many milliseconds to wait between checks to see if file uploaded to Sia. (SIA_UPLOAD_CHECK_FREQ_MS)
	MaxCacheSizeBytes int64        // Maximum size of cache directory in bytes. (SIA_CACHE_MAX_SIZE_BYTES)
	CacheTicker       *time.Ticker // Ticker for cache management.
	Db                *bolt.DB     // The database object.
	DbMutex           *sync.Mutex  // Mutex for protecting database access.
	BackgroundUpload  bool         // Whether or not to upload to Sia in background
	DebugMode         bool         // Whether or not debug mode is enabled.
}

// SiaBucketInfo stores info about buckets stored in Sia cache layer
type SiaBucketInfo struct {
	Name    string    // Name of bucket
	Created time.Time // Time of bucket creation
	Policy  string    // Policies for bucket
}

// SiaObjectInfo stores info about objects stored in Sia
type SiaObjectInfo struct {
	Bucket        string    // Name of bucket object is stored in
	Name          string    // Name of object
	Size          int64     // Size of the object in bytes
	Queued        time.Time // Time object was queued for upload to Sia
	Uploaded      time.Time // Time object was successfully uploaded to Sia
	PurgeAfter    int64     // If no downloads in this many seconds, purge from cache. Always in cache if value is 0.
	CachedFetches int64     // The total number of times the object has been fetched from cache
	SiaFetches    int64     // The total number of times the object has been fetched from Sia network
	LastFetch     time.Time // The time of the last fetch request for the object
	SrcFile       string    // The absolute path of the original source file
	Deleted       int64     // Whether object is marked for delete. 0: Not Deleted, 1: Deleted
	Cached        int64     // Whether object is currently in cache. 0: Not Cached, 1: Cached
}

// SiaFileInfo implements os.FileInfo interface and provides file information
type SiaFileInfo struct {
	FileName    string
	FileSize    int64
	FileModTime time.Time
	FileMode    os.FileMode
	FileIsDir   bool
	FileSys     interface{}
}

// Name returns the name of the file
func (o SiaFileInfo) Name() string {
	return o.FileName
}

// Size returns the size of the file in bytes
func (o SiaFileInfo) Size() int64 {
	return o.FileSize
}

// ModTime returns the modification time
func (o SiaFileInfo) ModTime() time.Time {
	return o.FileModTime
}

// Mode returns the file permissions
func (o SiaFileInfo) Mode() os.FileMode {
	return o.FileMode
}

// IsDir returns whether the file is a directory
func (o SiaFileInfo) IsDir() bool {
	return o.FileIsDir
}

// Sys returns system interface
func (o SiaFileInfo) Sys() interface{} {
	return o.FileSys
}

// newSiaCacheLayer creates a new Sia cache layer
func newSiaCacheLayer(siadAddress string, cacheDir string, dbFile string, debug bool) (*SiaCacheLayer, error) {
	cache := &SiaCacheLayer{
		SiadAddress:       siadAddress,
		CacheDir:          cacheDir,
		DbFile:            dbFile,
		DebugMode:         debug,
		ManagerDelaySec:   30,
		UploadCheckFreqMs: 3000,
		MaxCacheSizeBytes: 10000000000,
		CacheTicker:       nil,
		Db:                nil,
		DbMutex:           &sync.Mutex{},
	}

	cache.loadSiaEnv()

	return cache, nil
}

// Start will start running the Cache Layer
func (cache *SiaCacheLayer) Start() *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.Start")

	cache.DbMutex = &sync.Mutex{}

	cache.ensureCacheDirExists()

	// Open and initialize database
	err := cache.dbOpenDatabase()
	if err != nil {
		return err
	}

	// Start the cache management process
	cache.CacheTicker = time.NewTicker(time.Second * time.Duration(cache.ManagerDelaySec))
	go func() {
		for _ = range cache.CacheTicker.C {
			cache.manager()
		}
	}()

	return nil
}

// Stop will stop the SiaCacheLayer
func (cache *SiaCacheLayer) Stop() {
	cache.debugmsg("SiaCacheLayer.Stop")

	// Stop cache management process
	cache.CacheTicker.Stop()

	// Close the database
	cache.dbCloseDatabase()
}

// InsertBucket will attempt to insert a new bucket
func (cache *SiaCacheLayer) InsertBucket(bucket string) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.InsertBucket")

	return cache.dbInsertBucket(bucket)
}

// DeleteBucket will attempt to delete an existing bucket
func (cache *SiaCacheLayer) DeleteBucket(bucket string) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.DeleteBucket")

	// Do NOT delete if objects exist in bucket.
	objects, serr := cache.ListObjects(bucket)
	if serr != nil {
		return serr
	}
	if (len(objects) > 0) {
		return siaErrorBucketNotEmpty
	}

	return cache.dbDeleteBucket(bucket)
}

// ListBuckets will return a list of all existing buckets
func (cache *SiaCacheLayer) ListBuckets() (buckets []SiaBucketInfo, e *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.ListBuckets")

	return cache.dbListBuckets()
}

// DeleteObject will attempt to delete the object from Sia
func (cache *SiaCacheLayer) DeleteObject(bucket string, objectName string) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.DeleteObject")

	err := cache.dbUpdateObjectDeletedStatus(bucket, objectName, 1)
	if err != nil {
		return err
	}

	// Tell Sia daemon to delete the object
	var siaObj = cache.getSiaObjectName(bucket, objectName)

	derr := post(cache.SiadAddress, "/renter/delete/"+siaObj, "")
	if derr != nil {
		return &SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	return cache.dbDeleteObject(bucket, objectName)
}

// PutObject will attempt to put an object on Sia
func (cache *SiaCacheLayer) PutObject(bucket string, objectName string, size int64, purgeAfter int64, srcFile string) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.PutObject")

	// Before inserting to DB, there is a very rare chance that the object already exists in DB
	// from a failed upload and Minio crashed or was killed before DB updated to reflect. So just in case
	// we will check if the object exists and has a not uploaded status. If so, we will delete that
	// record and then continue as normal.
	objInfo, e := cache.GetObjectInfo(bucket, objectName)
	if e == nil {
		// Object does exist. If uploaded, return error. If not uploaded, delete it and continue.
		if objInfo.Uploaded.Unix() > 0 {
			return siaErrorObjectAlreadyExists
		}
		e = cache.dbDeleteObject(bucket, objectName)
		if e != nil {
			return e
		}
	}

	err := cache.dbInsertObject(bucket, objectName, size, time.Now().Unix(), 0, purgeAfter, srcFile, 1)
	if err != nil {
		return err
	}

	// Tell Sia daemon to upload the object
	siaObj := cache.getSiaObjectName(bucket, objectName)
	derr := post(cache.SiadAddress, "/renter/upload/"+siaObj, "source="+srcFile)
	if derr != nil {
		cache.dbDeleteObject(bucket, objectName)
		return &SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	// Need to wait for upload to complete unless background uploading is enabled
	if (!cache.BackgroundUpload) {
		err = cache.waitTillSiaUploadCompletes(siaObj)
		if err != nil {
			cache.dbDeleteObject(bucket, objectName)
			return err
		}

		// Mark object as uploaded
		err = cache.dbUpdateObjectUploadedStatus(bucket, objectName, 1)
		if err != nil {
			cache.dbDeleteObject(bucket, objectName)
			return err
		}
	}

	return nil
}

// ListObjects will return a list of existing objects in the bucket provided
func (cache *SiaCacheLayer) ListObjects(bucket string) (objects []SiaObjectInfo, e *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.ListObjects")

	return cache.dbListObjects(bucket)
}

// GuaranteeObjectIsInCache will guarantee that the specified object exists in the local cache
func (cache *SiaCacheLayer) GuaranteeObjectIsInCache(bucket string, objectName string) *SiaServiceError {
	defer cache.timeTrack(time.Now(), "GuaranteeObjectIsInCache")
	cache.debugmsg("SiaCacheLayer.GuaranteeObjectIsInCache")

	// Minio filesystem layer may request files from .minio.sys bucket
	// If we get a request for Minio, we'll pass back success and let Minio deal with it.
	if bucket == ".minio.sys" {
		return nil
	}

	// Make sure object exists in database
	objInfo, err := cache.GetObjectInfo(bucket, objectName)
	if err != nil {
		return err
	}

	// Is file already in cache?
	_, serr := os.Stat(objInfo.SrcFile)
	if serr == nil {
		// File exists in cache
		err = cache.dbUpdateCachedStatus(bucket, objectName, 1)
		if err != nil {
			return err
		}
		// Increment cached fetch count and update last_fetch
		return cache.dbUpdateCachedFetches(bucket, objectName, objInfo.CachedFetches+1)
	}
	// Object not in cache, must download from Sia.
	// First, though, make sure the file was completely uploaded to Sia.
	if objInfo.Uploaded == time.Unix(0, 0) {
		// File never completed uploading, or was never marked as uploaded in database.
		// Neither of these cases should happen, but just in case.
		return siaErrorUnknown
	}

	// Make sure bucket path exists in cache directory
	cache.ensureCacheBucketDirExists(bucket)

	// Make sure enough space exists in cache
	err = cache.guaranteeCacheSpace(objInfo.Size)
	if err != nil {
		return err
	}

	// Increment fetch count and update last_fetch BEFORE requesting d/l from Sia.
	// This will prevent the cache manager from removing the partially downloaded file.
	err = cache.dbUpdateSiaFetches(bucket, objectName, objInfo.SiaFetches+1)
	if err != nil {
		return err
	}

	var siaObj = cache.getSiaObjectName(bucket, objectName)
	derr := get(cache.SiadAddress, "/renter/download/"+siaObj+"?destination="+url.QueryEscape(objInfo.SrcFile))
	if derr != nil {
		cache.debugmsg(fmt.Sprintf("Error: %s", derr))
		return &SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	// After successfully downloading to the cache, make sure the cached flag of the object is set.
	return cache.dbUpdateCachedStatus(bucket, objectName, 1)
}

// GetObjectInfo will return object information for the object specified
func (cache *SiaCacheLayer) GetObjectInfo(bucket string, objectName string) (objInfo SiaObjectInfo, e *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.GetObjectInfo")

	return cache.dbGetObjectInfo(bucket, objectName)
}

// SetBucketPolicies sets policy on bucket
func (cache *SiaCacheLayer) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.SetBucketPolicies")
	res, _ := json.Marshal(&policyInfo)
	return cache.dbUpdateBucketPolicies(bucket, string(res))
}

// GetBucketPolicies will get policy on bucket
func (cache *SiaCacheLayer) GetBucketPolicies(bucket string) (bal policy.BucketAccessPolicy, e *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.GetBucketPolicies")

	return cache.dbGetBucketPolicies(bucket)
}

// DeleteBucketPolicies deletes all policies on bucket
func (cache *SiaCacheLayer) DeleteBucketPolicies(bucket string) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.DeleteBucketPolicies")

	return cache.dbUpdateBucketPolicies(bucket, "")
}

// Runs periodically to manage the database and cache
func (cache *SiaCacheLayer) manager() {
	cache.debugmsg("SiaCacheLayer.manager")
	// Check to see if any files in database have completed uploading to Sia.
	// If so, update uploaded timestamp in database.
	err := cache.checkSiaUploads()
	if err != nil {
		fmt.Println("Error in DB/Cache Management Process:")
		fmt.Println(err)
	}

	// Remove files from cache that have not been uploaded or fetched in purge_after seconds.
	err = cache.purgeCache()
	if err != nil {
		fmt.Println("Error in DB/Cache Management Process:")
		fmt.Println(err)
	}

	// Check cache disk usage
	err = cache.guaranteeCacheSpace(0)
	if err != nil {
		fmt.Println("Error in DB/Cache Management Process:")
		fmt.Println(err)
	}

}

// Purge older, infrequently accessed files from cache.
// This function is less strict and doesn't consider max space quota.
func (cache *SiaCacheLayer) purgeCache() *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.purgeCache")
	buckets, err := cache.ListBuckets()
	if err != nil {
		return err
	}

	for _, bucket := range buckets {
		objects, err := cache.ListObjects(bucket.Name)
		if err != nil {
			return err
		}

		for _, object := range objects {
			// Only remove an object from cache here if:
			// 1. Object is cached
			// 1. Object was uploaded over PurgeAfter seconds ago
			// 2. Object hasn't been fetched in over PurgeAfter seconds
			if object.Cached == 1 && object.Uploaded != time.Unix(0, 0) {
				sinceUploaded := time.Now().Unix() - object.Uploaded.Unix()
				sinceFetched := time.Now().Unix() - object.LastFetch.Unix()
				if sinceUploaded > object.PurgeAfter && sinceFetched > object.PurgeAfter {
					err = cache.removeFromCache(object)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (cache *SiaCacheLayer) removeFromCache(objInfo SiaObjectInfo) *SiaServiceError {
	cache.debugmsg(fmt.Sprintf("removeFromCache: %s", objInfo.SrcFile))

	// If file doesn't exist in cache, it's falsely labelled. Update and return.
	_, err := os.Stat(objInfo.SrcFile)
	if err != nil {
		return cache.dbUpdateCachedStatus(objInfo.Bucket, objInfo.Name, 0)
	}

	err = os.Remove(objInfo.SrcFile)
	if err != nil {
		// File exists but couldn't be deleted. Permission issue?
		return siaErrorFailedToDeleteCachedFile
	}

	return cache.dbUpdateCachedStatus(objInfo.Bucket, objInfo.Name, 0)
}

func (cache *SiaCacheLayer) checkSiaUploads() *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.checkSiaUploads")
	// Get list of all uploading objects
	objs, err := cache.dbListUploadingObjects()
	if err != nil {
		return err
	}

	// Get list of all renter files
	var rf api.RenterFiles
	derr := getAPI(cache.SiadAddress, "/renter/files", &rf)
	if derr != nil {
		return &SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	// If uploading object is available on Sia, update database
	for _, obj := range objs {
		var siaObj = cache.getSiaObjectName(obj.Bucket, obj.Name)
		for _, file := range rf.Files {
			if file.SiaPath == siaObj && file.Available {
				cache.debugmsg(fmt.Sprintf("  Upload to Sia completed: %s", obj.Name))
				err = cache.dbUpdateObjectUploadedStatus(obj.Bucket, obj.Name, 1)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (cache *SiaCacheLayer) waitTillSiaUploadCompletes(siaObj string) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.waitTillSiaUploadCompletes")
	complete := false
	for !complete {
		avail, e := cache.isSiaFileAvailable(siaObj)
		if e != nil {
			return e
		}

		if avail {
			return nil
		}
		time.Sleep(time.Duration(cache.UploadCheckFreqMs) * time.Millisecond)
	}

	return nil
}

func (cache *SiaCacheLayer) isSiaFileAvailable(siaObj string) (bool, *SiaServiceError) {
	cache.debugmsg(fmt.Sprintf("SiaCacheLayer.isSiaFileAvailable: %s", siaObj))

	var rf api.RenterFiles
	err := getAPI(cache.SiadAddress, "/renter/files", &rf)
	if err != nil {
		return false, &SiaServiceError{Code: "SiaErrorDaemon", Message: err.Error()}
	}

	for _, file := range rf.Files {
		cache.debugmsg(fmt.Sprintf("  Renter file: %s", file.SiaPath))
		if file.SiaPath == siaObj {
			return file.Available, nil
		}
	}

	return false, &SiaServiceError{Code: "SiaErrorDaemon", Message: "File not in Sia renter list"}
}

func (cache *SiaCacheLayer) guaranteeCacheSpace(cacheNeeded int64) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.guaranteeCacheSpace")

	avail, e := cache.getCacheAvailable()
	if e != nil {
		return e
	}

	cache.debugmsg(fmt.Sprintf("  Cache space available: %d\n", avail))
	for avail < cacheNeeded {
		e = cache.forceDeleteOldestCacheFile()
		if e != nil {
			return e
		}

		avail, e = cache.getCacheAvailable()
		if e != nil {
			return e
		}
		cache.debugmsg(fmt.Sprintf("  Cache space available: %d\n", avail))
	}

	return nil
}

func (cache *SiaCacheLayer) getSiaObjectName(bucket string, objectName string) string {
	reg, _ := regexp.Compile("[^a-zA-Z0-9.]+")

	cleanedName := reg.ReplaceAllString(objectName, "+")
	return bucket + "/" + cleanedName
}

func (cache *SiaCacheLayer) forceDeleteOldestCacheFile() *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.forceDeleteOldestCacheFile")

	buckets, serr := cache.dbListBuckets()
	if serr != nil {
		return serr
	}

	var objToDelete *SiaObjectInfo;
	objToDelete = nil

	for _, bkt := range buckets {
		objs, serr := cache.dbListObjects(bkt.Name)
		if serr != nil {
			return serr
		}

		for _, obj := range objs {
			if obj.Uploaded.Unix() > 0 && obj.Cached == 1 {
				if objToDelete == nil || obj.LastFetch.Unix() < objToDelete.LastFetch.Unix() {
					objToDelete = &obj
				}
			}
		}
	}

	if objToDelete == nil {
		return siaErrorUnableToClearAnyCachedFiles
	}

	// Make certain cached item exists, then delete it.
	_, err := os.Stat(objToDelete.SrcFile)
	if err != nil {
		// Item does NOT exist in cache. Could have been deleted manually by user.
		// Update the cached flag and return. (Returning failure would stop cache manager.)
		return cache.dbUpdateCachedStatus(objToDelete.Bucket, objToDelete.Name, 0)
	}

	err = os.Remove(objToDelete.SrcFile)
	if err != nil {
		return siaErrorUnableToClearAnyCachedFiles
	}

	err = cache.dbUpdateCachedStatus(objToDelete.Bucket, objToDelete.Name, 0)
	if err != nil {
		return siaErrorUnableToClearAnyCachedFiles
	}
	return nil
}

func (cache *SiaCacheLayer) getCacheUsed() (int64, *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.getCacheUsed")

	var size int64
	size = 0
	err := filepath.Walk(cache.CacheDir, func(_ string, info os.FileInfo, e error) error {
		if !info.IsDir() {
			cache.debugmsg(fmt.Sprintf("  %s: %d", info.Name(), info.Size()))
			size += info.Size()
		}
		return e
	})
	if err != nil {
		return 0, siaErrorDeterminingCacheSize
	}
	return size, nil
}

func (cache *SiaCacheLayer) getCacheAvailable() (int64, *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.getCacheAvailable")

	used, serr := cache.getCacheUsed()
	return (cache.MaxCacheSizeBytes - used), serr
}

func (cache *SiaCacheLayer) ensureCacheDirExists() {
	cache.debugmsg("SiaCacheLayer.ensureCacheDirExists")
	// Make sure cache directory exists
	os.Mkdir(cache.CacheDir, 0744)
}

func (cache *SiaCacheLayer) ensureCacheBucketDirExists(bucket string) {
	cache.debugmsg("SiaCacheLayer.ensureCacheBucketDirExists")
	os.Mkdir(filepath.Join(cache.CacheDir, bucket), 0744)
}

func (cache *SiaCacheLayer) debugmsg(str string) {
	if cache.DebugMode {
		fmt.Println(str)
	}
}

func (cache *SiaCacheLayer) timeTrack(start time.Time, name string) {
	if cache.DebugMode {
		elapsed := time.Since(start)
		fmt.Printf("%s took %s\n", name, elapsed)
	}
}

// Attempt to load Sia config from ENV
func (cache *SiaCacheLayer) loadSiaEnv() {
	tmp := os.Getenv("SIA_MANAGER_DELAY_SEC")
	if tmp != "" {
		i, err := strconv.ParseInt(tmp, 10, 64)
		if err == nil {
			cache.ManagerDelaySec = i
		}
	}

	tmp = os.Getenv("SIA_UPLOAD_CHECK_FREQ_MS")
	if tmp != "" {
		i, err := strconv.ParseInt(tmp, 10, 64)
		if err == nil {
			cache.UploadCheckFreqMs = i
		}
	}

	tmp = os.Getenv("SIA_CACHE_MAX_SIZE_BYTES")
	if tmp != "" {
		i, err := strconv.ParseInt(tmp, 10, 64)
		if err == nil {
			cache.MaxCacheSizeBytes = i
		}
	}

	tmp = os.Getenv("SIA_BACKGROUND_UPLOAD")
	if tmp != "" {
		i, err := strconv.ParseInt(tmp, 10, 64)
		if err == nil {
			if i == 0 {
				cache.BackgroundUpload = false
			} else {
				cache.BackgroundUpload = true
			}
		}
	}
}
