package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/NebulousLabs/Sia/api"
	_ "github.com/mattn/go-sqlite3"
	"github.com/minio/minio-go/pkg/policy"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type SiaCacheLayer struct {
	SiadAddress       string       // Address of siad daemon API. (e.g., "127.0.0.1:9980")
	CacheDir          string       // Cache directory for downloads.
	DbFile            string       // Name and path of Sqlite database file.
	ManagerDelaySec   int64        // How many seconds to delay between cache/db management operations. (SIA_MANAGER_DELAY_SEC)
	UploadCheckFreqMs int64        // How many milliseconds to wait between checks to see if file uploaded to Sia. (SIA_UPLOAD_CHECK_FREQ_MS)
	MaxCacheSizeBytes int64        // Maximum size of cache directory in bytes. (SIA_CACHE_MAX_SIZE_BYTES)
	CacheTicker       *time.Ticker // Ticker for cache management.
	Db                *sql.DB      // The database object.
	DbMutex           *sync.Mutex  // Mutex for protecting database access.
	DebugMode         bool         // Whether or not debug mode is enabled.
}

type SiaBucketInfo struct {
	Name    string    // Name of bucket
	Created time.Time // Time of bucket creation
}

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

type SiaServiceError struct {
	Code    string
	Message string
}

func (e SiaServiceError) Error() string {
	return fmt.Sprintf("Sia Error: %s",
		e.Message)
}

//Also: SiaErrorDaemon for daemon errors

var siaSuccess = SiaServiceError{
	Code:    "SiaSuccess",
	Message: "",
}
var siaErrorUnableToClearAnyCachedFiles = SiaServiceError{
	Code:    "SiaErrorUnableToClearAnyCachedFiles",
	Message: "Unable to clear any files from cache.",
}
var siaErrorDeterminingCacheSize = SiaServiceError{
	Code:    "SiaErrorDeterminingCacheSize",
	Message: "Unable to determine total size of files in cache.",
}
var siaErrorDatabaseDeleteError = SiaServiceError{
	Code:    "SiaErrorDatabaseDeleteError",
	Message: "Failed to delete a record in the cache database.",
}
var siaErrorDatabaseCreateError = SiaServiceError{
	Code:    "SiaErrorDatabaseCreateError",
	Message: "Failed to create a table in the cache database.",
}
var siaErrorDatabaseCantBeOpened = SiaServiceError{
	Code:    "SiaErrorDatabaseCantBeOpened",
	Message: "The cache database could not be opened.",
}
var siaErrorDatabaseInsertError = SiaServiceError{
	Code:    "SiaErrorDatabaseInsertError",
	Message: "Failed to insert a record in the cache database.",
}
var siaErrorDatabaseUpdateError = SiaServiceError{
	Code:    "SiaErrorDatabaseUpdateError",
	Message: "Failed to update a record in the cache database.",
}
var siaErrorDatabaseSelectError = SiaServiceError{
	Code:    "SiaErrorDatabaseSelectError",
	Message: "Failed to select records in the cache database.",
}
var siaErrorUnknown = SiaServiceError{
	Code:    "SiaErrorUnknown",
	Message: "An unknown error has occured.",
}
var siaErrorFailedToDeleteCachedFile = SiaServiceError{
	Code:    "SiaFailedToDeleteCachedFile",
	Message: "Failed to delete cached file. Check permissions.",
}
var siaErrorObjectDoesNotExistInBucket = SiaServiceError{
	Code:    "SiaErrorObjectDoesNotExistInBucket",
	Message: "Object does not exist in bucket.",
}
var siaErrorObjectAlreadyExists = SiaServiceError{
	Code:    "SiaErrorObjectAlreadyExists",
	Message: "Object does not exist in bucket.",
}
var siaErrorInvalidBucketPolicy = SiaServiceError{
	Code:    "SiaErrorInvalidBucketPolicy",
	Message: "An invalid bucket policy has been specified.",
}

// Need to implement os.FileInfo interface on SiaObjectInfo for convenience.
type SiaFileInfo struct {
	FileName    string
	FileSize    int64
	FileModTime time.Time
	FileMode    os.FileMode
	FileIsDir   bool
	FileSys     interface{}
}

func (o SiaFileInfo) Name() string {
	return o.FileName
}
func (o SiaFileInfo) Size() int64 {
	return o.FileSize
}
func (o SiaFileInfo) ModTime() time.Time {
	return o.FileModTime
}
func (o SiaFileInfo) Mode() os.FileMode {
	return o.FileMode
}
func (o SiaFileInfo) IsDir() bool {
	return o.FileIsDir
}
func (o SiaFileInfo) Sys() interface{} {
	return o.FileSys
}

func newSiaCacheLayer(siadAddress string, cacheDir string, dbFile string, debug bool) (*SiaCacheLayer, error) {
	return &SiaCacheLayer{
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
	}, nil
}

// Called to start running the Cache Layer
func (cache *SiaCacheLayer) Start() SiaServiceError {
	cache.debugmsg("SiaCacheLayer.Start")
	cache.loadSiaEnv()

	cache.DbMutex = &sync.Mutex{}

	cache.ensureCacheDirExists()

	// Open and initialize database
	err := cache.initDatabase()
	if err != siaSuccess {
		return err
	}

	// Start the cache management process
	cache.CacheTicker = time.NewTicker(time.Second * time.Duration(cache.ManagerDelaySec))
	go func() {
		for _ = range cache.CacheTicker.C {
			cache.manager()
		}
	}()

	return siaSuccess
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
	fmt.Printf("SIA_MANAGER_DELAY_SEC: %d\n", cache.ManagerDelaySec)

	tmp = os.Getenv("SIA_UPLOAD_CHECK_FREQ_MS")
	if tmp != "" {
		i, err := strconv.ParseInt(tmp, 10, 64)
		if err == nil {
			cache.UploadCheckFreqMs = i
		}
	}
	fmt.Printf("SIA_UPLOAD_CHECK_FREQ_MS: %d\n", cache.UploadCheckFreqMs)

	tmp = os.Getenv("SIA_CACHE_MAX_SIZE_BYTES")
	if tmp != "" {
		i, err := strconv.ParseInt(tmp, 10, 64)
		if err == nil {
			cache.MaxCacheSizeBytes = i
		}
	}
	fmt.Printf("SIA_CACHE_MAX_SIZE_BYTES: %d\n", cache.MaxCacheSizeBytes)
}

// Called to stop the SiaBridge
func (cache *SiaCacheLayer) Stop() {
	cache.debugmsg("SiaCacheLayer.Stop")
	// Stop cache management process
	cache.CacheTicker.Stop()

	// Close the database
	cache.lockDB()
	cache.Db.Close()
	cache.unlockDB()
}

func (cache *SiaCacheLayer) lockDB() {
	//cache.debugmsg("SiaCacheLayer.LockDB")
	cache.DbMutex.Lock()
}

func (cache *SiaCacheLayer) unlockDB() {
	//cache.debugmsg("SiaCacheLayer.UnlockDB")
	cache.DbMutex.Unlock()
}

func (cache *SiaCacheLayer) InsertBucket(bucket string) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.InsertBucket")

	cache.lockDB()
	defer cache.unlockDB()

	stmt, err := cache.Db.Prepare("INSERT INTO buckets(name, created, policy) values(?,?,?)")
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	defer stmt.Close()

	policyInfo := policy.BucketAccessPolicy{}
	res, _ := json.Marshal(&policyInfo)
	_, err = stmt.Exec(bucket, time.Now().Unix(), string(res))
	if err != nil {
		cache.debugmsg(err.Error())
		return siaErrorDatabaseInsertError
	}

	return siaSuccess
}

func (cache *SiaCacheLayer) DeleteBucket(bucket string) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.DeleteBucket")
	// First, should delete all objects in the bucket
	objects, err := cache.ListObjects(bucket)
	if err != siaSuccess {
		return err
	}
	for _, object := range objects {
		err = cache.DeleteObject(bucket, object.Name)
		if err != siaSuccess {
			return err
		}
	}

	cache.lockDB()
	defer cache.unlockDB()

	// Now delete the actual bucket
	stmt, e := cache.Db.Prepare("DELETE FROM buckets WHERE name=?")
	if e != nil {
		return siaErrorDatabaseDeleteError
	}

	defer stmt.Close()

	_, e = stmt.Exec(bucket)
	if e != nil {
		return siaErrorDatabaseDeleteError
	}

	return siaSuccess
}

// List all buckets
func (cache *SiaCacheLayer) ListBuckets() (buckets []SiaBucketInfo, e SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.ListBuckets")

	cache.lockDB()
	defer cache.unlockDB()

	rows, err := cache.Db.Query("SELECT name,created FROM buckets")
	if err != nil {
		return buckets, siaErrorDatabaseSelectError
	}

	defer rows.Close()

	var name string
	var created int64

	for rows.Next() {
		err = rows.Scan(&name, &created)
		if err != nil {
			return buckets, siaErrorDatabaseSelectError
		}

		buckets = append(buckets, SiaBucketInfo{
			Name:    name,
			Created: time.Unix(created, 0),
		})
	}

	rows.Close()

	return buckets, siaSuccess
}

// Deletes the object
func (cache *SiaCacheLayer) DeleteObject(bucket string, objectName string) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.DeleteObject")
	err := cache.markObjectDeleted(bucket, objectName)
	if err != siaSuccess {
		return err
	}

	// Tell Sia daemon to delete the object
	var siaObj = cache.getSiaObjectName(bucket, objectName)

	derr := post(cache.SiadAddress, "/renter/delete/"+siaObj, "")
	if derr != nil {
		return SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	return cache.deleteObjectFromDb(bucket, objectName)
}

func (cache *SiaCacheLayer) PutObject(bucket string, objectName string, size int64, purge_after int64, src_file string) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.PutObject")

	// First, make sure space exists in cache
	err := cache.guaranteeCacheSpace(size)
	if err != siaSuccess {
		return err
	}

	// Before inserting to DB, there is a very rare chance that the object already exists in DB
	// from a failed upload and Minio crashed or was killed before DB updated to reflect. So just in case
	// we will check if the object exists and has a not uploaded status. If so, we will delete that
	// record and then continue as normal.
	objInfo, e := cache.GetObjectInfo(bucket, objectName)
	if e == siaSuccess {
		// Object does exist. If uploaded, return error. If not uploaded, delete it and continue.
		if objInfo.Uploaded.Unix() > 0 {
			return siaErrorObjectAlreadyExists
		} else {
			e = cache.deleteObjectFromDb(bucket, objectName)
			if e != siaSuccess {
				return e
			}
		}
	}

	err = cache.insertObjectToDb(bucket, objectName, size, time.Now().Unix(), 0, purge_after, src_file, 1)
	if err != siaSuccess {
		return err
	}

	// Tell Sia daemon to upload the object
	siaObj := cache.getSiaObjectName(bucket, objectName)
	derr := post(cache.SiadAddress, "/renter/upload/"+siaObj, "source="+src_file)
	if derr != nil {
		cache.deleteObjectFromDb(bucket, objectName)
		return SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	// Need to wait for upload to complete
	err = cache.waitTillSiaUploadCompletes(siaObj)
	if err != siaSuccess {
		cache.deleteObjectFromDb(bucket, objectName)
		return err
	}

	// Mark object as uploaded
	err = cache.markObjectUploaded(bucket, objectName)
	if err != siaSuccess {
		cache.deleteObjectFromDb(bucket, objectName)
		return err
	}

	return siaSuccess
}

// Returns a list of objects in the bucket provided
func (cache *SiaCacheLayer) ListObjects(bucket string) (objects []SiaObjectInfo, e SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.listUploadingObjects")

	cache.lockDB()
	defer cache.unlockDB()

	rows, err := cache.Db.Query("SELECT name,size,queued,uploaded,purge_after,cached_fetches,sia_fetches,last_fetch,src_file,deleted,cached FROM objects WHERE bucket=?", bucket)
	if err != nil {
		return objects, siaErrorDatabaseSelectError
	}

	defer rows.Close()

	var name string
	var size int64
	var queued int64
	var uploaded int64
	var purge_after int64
	var cached_fetches int64
	var sia_fetches int64
	var last_fetch int64
	var src_file string
	var deleted int64
	var cached int64

	for rows.Next() {
		err = rows.Scan(&name, &size, &queued, &uploaded, &purge_after, &cached_fetches, &sia_fetches, &last_fetch, &src_file, &deleted, &cached)
		if err != nil {
			return objects, siaErrorDatabaseSelectError
		}

		objects = append(objects, SiaObjectInfo{
			Bucket:        bucket,
			Name:          name,
			Size:          size,
			Queued:        time.Unix(queued, 0),
			Uploaded:      time.Unix(uploaded, 0),
			PurgeAfter:    purge_after,
			CachedFetches: cached_fetches,
			SiaFetches:    sia_fetches,
			LastFetch:     time.Unix(last_fetch, 0),
			SrcFile:       src_file,
			Deleted:       deleted,
			Cached:        cached,
		})
	}

	return objects, siaSuccess
}

func (cache *SiaCacheLayer) GuaranteeObjectIsInCache(bucket string, objectName string) SiaServiceError {
	defer cache.timeTrack(time.Now(), "GuaranteeObjectIsInCache")
	cache.debugmsg("SiaCacheLayer.GuaranteeObjectIsInCache")
	// Minio filesystem layer may request files from .minio.sys bucket
	// If we get a request for Minio, we'll pass back success and let Minio deal with it.
	if bucket == ".minio.sys" {
		return siaSuccess
	}

	// Make sure object exists in database
	objInfo, err := cache.GetObjectInfo(bucket, objectName)
	if err != siaSuccess {
		return err
	}

	// Is file already in cache?
	_, serr := os.Stat(objInfo.SrcFile)
	if serr == nil {
		// File exists in cache
		err = cache.updateCachedStatus(bucket, objectName, 1)
		if err != siaSuccess {
			return err
		}
		// Increment cached fetch count and update last_fetch
		return cache.updateCachedFetches(bucket, objectName, objInfo.CachedFetches+1)
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
	if err != siaSuccess {
		return err
	}

	// Increment fetch count and update last_fetch BEFORE requesting d/l from Sia.
	// This will prevent the cache manager from removing the partially downloaded file.
	err = cache.updateSiaFetches(bucket, objectName, objInfo.SiaFetches+1)
	if err != siaSuccess {
		return err
	}

	var siaObj = cache.getSiaObjectName(bucket, objectName)
	cache.debugmsg(fmt.Sprintf("GET %s %s %s\n", cache.SiadAddress, siaObj, objInfo.SrcFile))
	derr := get(cache.SiadAddress, "/renter/download/"+siaObj+"?destination="+url.QueryEscape(objInfo.SrcFile))
	if derr != nil {
		cache.debugmsg(fmt.Sprintf("Error: %s", derr))
		return SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	// After successfully downloading to the cache, make sure the cached flag of the object is set.
	return cache.updateCachedStatus(bucket, objectName, 1)
}

// Returns info for the provided object
func (cache *SiaCacheLayer) GetObjectInfo(bucket string, objectName string) (objInfo SiaObjectInfo, e SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.GetObjectInfo")

	cache.lockDB()
	defer cache.unlockDB()

	// Query the database
	var size int64
	var queued int64
	var uploaded int64
	var purge_after int64
	var cached_fetches int64
	var sia_fetches int64
	var last_fetch int64
	var src_file string
	var deleted int64
	var cached int64
	err := cache.Db.QueryRow("SELECT size,queued,uploaded,purge_after,cached_fetches,sia_fetches,last_fetch,src_file,deleted,cached FROM objects WHERE name=? AND bucket=?", objectName, bucket).Scan(&size, &queued, &uploaded, &purge_after, &cached_fetches, &sia_fetches, &last_fetch, &src_file, &deleted, &cached)
	switch {
	case err == sql.ErrNoRows:
		return objInfo, siaErrorObjectDoesNotExistInBucket
	case err != nil:
		// An error occured
		return objInfo, siaErrorDatabaseSelectError
	default:
		// Object exists
		objInfo.Bucket = bucket
		objInfo.Name = objectName
		objInfo.Size = size
		objInfo.Queued = time.Unix(queued, 0)
		objInfo.Uploaded = time.Unix(uploaded, 0)
		objInfo.PurgeAfter = purge_after
		objInfo.CachedFetches = cached_fetches
		objInfo.SiaFetches = sia_fetches
		objInfo.LastFetch = time.Unix(last_fetch, 0)
		objInfo.SrcFile = src_file
		objInfo.Deleted = deleted
		objInfo.Cached = cached
		return objInfo, siaSuccess
	}

	// Shouldn't happen, but just in case
	return objInfo, siaErrorUnknown
}

// SetBucketPolicies sets policy on bucket
func (cache *SiaCacheLayer) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.SetBucketPolicies")
	res, _ := json.Marshal(&policyInfo)
	return cache.updateBucketPolicy(bucket, string(res))
}

// GetBucketPolicies will get policy on bucket
func (cache *SiaCacheLayer) GetBucketPolicies(bucket string) (bal policy.BucketAccessPolicy, e SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.GetBucketPolicies")

	cache.lockDB()
	defer cache.unlockDB()

	// Query the database
	var bktPolicy string
	err := cache.Db.QueryRow("SELECT policy FROM buckets WHERE name=?", bucket).Scan(&bktPolicy)
	switch {
	case err == sql.ErrNoRows:
		return bal, siaErrorDatabaseSelectError // Bucket does not exist
	case err != nil:
		return bal, siaErrorDatabaseSelectError // An error occured
	default:
		res := policy.BucketAccessPolicy{}
		json.Unmarshal([]byte(bktPolicy), &res)
		fmt.Println("Returning success!")
		return res, siaSuccess
	}

	// Shouldn't happen, but just in case
	return bal, siaErrorUnknown
}

// DeleteBucketPolicies deletes all policies on bucket
func (cache *SiaCacheLayer) DeleteBucketPolicies(bucket string) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.DeleteBucketPolicies")

	return cache.updateBucketPolicy(bucket, "")
}

func (cache *SiaCacheLayer) updateBucketPolicy(bucket string, bktPolicy string) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.updateBucketPolicy")

	cache.lockDB()
	defer cache.unlockDB()

	stmt, err := cache.Db.Prepare("UPDATE buckets SET policy=? WHERE name=?")
	if err != nil {
		cache.debugmsg(err.Error())
		return siaErrorDatabaseUpdateError
	}

	defer stmt.Close()

	_, err = stmt.Exec(bktPolicy, bucket)
	if err != nil {
		cache.debugmsg(err.Error())
		return siaErrorDatabaseUpdateError
	}

	return siaSuccess
}

// Runs periodically to manage the database and cache
func (cache *SiaCacheLayer) manager() {
	cache.debugmsg("SiaCacheLayer.manager")
	// Check to see if any files in database have completed uploading to Sia.
	// If so, update uploaded timestamp in database.
	err := cache.checkSiaUploads()
	if err != siaSuccess {
		fmt.Println("Error in DB/Cache Management Process:")
		fmt.Println(err)
	}

	// Remove files from cache that have not been uploaded or fetched in purge_after seconds.
	err = cache.purgeCache()
	if err != siaSuccess {
		fmt.Println("Error in DB/Cache Management Process:")
		fmt.Println(err)
	}

	// Check cache disk usage
	err = cache.guaranteeCacheSpace(0)
	if err != siaSuccess {
		fmt.Println("Error in DB/Cache Management Process:")
		fmt.Println(err)
	}

}

// Purge older, infrequently accessed files from cache.
// This function is less strict and doesn't consider max space quota.
func (cache *SiaCacheLayer) purgeCache() SiaServiceError {
	cache.debugmsg("SiaCacheLayer.purgeCache")
	buckets, err := cache.ListBuckets()
	if err != siaSuccess {
		return err
	}

	for _, bucket := range buckets {
		objects, err := cache.ListObjects(bucket.Name)
		if err != siaSuccess {
			return err
		}

		for _, object := range objects {
			// Only remove an object from cache here if:
			// 1. Object is cached
			// 1. Object was uploaded over PurgeAfter seconds ago
			// 2. Object hasn't been fetched in over PurgeAfter seconds
			if object.Cached == 1 && object.Uploaded != time.Unix(0, 0) {
				since_uploaded := time.Now().Unix() - object.Uploaded.Unix()
				since_fetched := time.Now().Unix() - object.LastFetch.Unix()
				if since_uploaded > object.PurgeAfter && since_fetched > object.PurgeAfter {
					err = cache.removeFromCache(object)
					if err != siaSuccess {
						return err
					}
				}
			}
		}
	}
	return siaSuccess
}

func (cache *SiaCacheLayer) removeFromCache(objInfo SiaObjectInfo) SiaServiceError {
	cache.debugmsg(fmt.Sprintf("removeFromCache: %s", objInfo.SrcFile))

	// If file doesn't exist in cache, it's falsely labelled. Update and return.
	_, err := os.Stat(objInfo.SrcFile)
	if err != nil {
		return cache.markObjectCached(objInfo.Bucket, objInfo.Name, 0)
	}

	err = os.Remove(objInfo.SrcFile)
	if err != nil {
		// File exists but couldn't be deleted. Permission issue?
		return siaErrorFailedToDeleteCachedFile
	}

	return cache.markObjectCached(objInfo.Bucket, objInfo.Name, 0)
}

func (cache *SiaCacheLayer) checkSiaUploads() SiaServiceError {
	cache.debugmsg("SiaCacheLayer.checkSiaUploads")
	// Get list of all uploading objects
	objs, err := cache.listUploadingObjects()
	if err != siaSuccess {
		return err
	}

	// Get list of all renter files
	var rf api.RenterFiles
	derr := getAPI(cache.SiadAddress, "/renter/files", &rf)
	if derr != nil {
		return SiaServiceError{Code: "SiaErrorDaemon", Message: derr.Error()}
	}

	// If uploading object is available on Sia, update database
	for _, obj := range objs {
		var siaObj = cache.getSiaObjectName(obj.Bucket, obj.Name)
		for _, file := range rf.Files {
			if file.SiaPath == siaObj && file.Available {
				err = cache.markObjectUploaded(obj.Bucket, obj.Name)
				if err != siaSuccess {
					return err
				}
			}
		}
	}

	return siaSuccess
}

func (cache *SiaCacheLayer) markObjectUploaded(bucket string, objectName string) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.markObjectUploaded")

	cache.lockDB()
	defer cache.unlockDB()

	stmt, err := cache.Db.Prepare("UPDATE objects SET uploaded=? WHERE bucket=? AND name=?")
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	defer stmt.Close()

	_, err = stmt.Exec(time.Now().Unix(), bucket, objectName)
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	return siaSuccess
}

func (cache *SiaCacheLayer) markObjectDeleted(bucket string, objectName string) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.markObjectDeleted")

	cache.lockDB()
	defer cache.unlockDB()

	stmt, err := cache.Db.Prepare("UPDATE objects SET deleted=1 WHERE bucket=? AND name=?")
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	defer stmt.Close()

	_, err = stmt.Exec(bucket, objectName)
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	return siaSuccess
}

func (cache *SiaCacheLayer) markObjectCached(bucket string, objectName string, status int) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.markObjectCached")

	cache.lockDB()
	defer cache.unlockDB()

	stmt, err := cache.Db.Prepare("UPDATE objects SET cached=? WHERE bucket=? AND name=?")
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	defer stmt.Close()

	_, err = stmt.Exec(status, bucket, objectName)
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	return siaSuccess
}

func (cache *SiaCacheLayer) initDatabase() SiaServiceError {
	cache.debugmsg("SiaCacheLayer.initDatabase")

	cache.lockDB()
	defer cache.unlockDB()

	// Open the database
	var e error
	cache.Db, e = sql.Open("sqlite3", cache.DbFile)
	if e != nil {
		return siaErrorDatabaseCantBeOpened
	}

	// Make sure buckets table exists
	stmt, err := cache.Db.Prepare("CREATE TABLE IF NOT EXISTS buckets(name TEXT PRIMARY KEY, created INTEGER, policy TEXT)")
	if err != nil {
		return siaErrorDatabaseCreateError
	}

	defer stmt.Close()

	_, err = stmt.Exec()
	if err != nil {
		return siaErrorDatabaseCreateError
	}

	// Make sure objects table exists
	stmt2, e := cache.Db.Prepare("CREATE TABLE IF NOT EXISTS objects(bucket TEXT, name TEXT, size INTEGER, queued INTEGER, uploaded INTEGER, purge_after INTEGER, cached_fetches INTEGER, sia_fetches INTEGER, last_fetch INTEGER, src_file TEXT, deleted INTEGER, cached INTEGER, PRIMARY KEY(bucket,name) )")
	if e != nil {
		return siaErrorDatabaseCreateError
	}

	defer stmt2.Close()

	_, err = stmt2.Exec()
	if err != nil {
		return siaErrorDatabaseCreateError
	}

	return siaSuccess
}

func (cache *SiaCacheLayer) bucketExists(bucket string) (exists bool, e SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.bucketExists")

	cache.lockDB()
	defer cache.unlockDB()

	// Query the database
	var name string
	err := cache.Db.QueryRow("SELECT name FROM buckets WHERE name=?", bucket).Scan(&name)
	switch {
	case err == sql.ErrNoRows:
		return false, siaSuccess // Bucket does not exist
	case err != nil:
		return false, siaErrorDatabaseSelectError // An error occured
	default:
		if name == bucket {
			return true, siaSuccess // Bucket exists
		}
	}

	// Shouldn't happen, but just in case
	return false, siaErrorUnknown
}

func (cache *SiaCacheLayer) objectExists(bucket string, objectName string) (exists bool, e SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.objectExists")

	cache.lockDB()
	defer cache.unlockDB()

	// Query the database
	var bkt string
	var name string
	err := cache.Db.QueryRow("SELECT bucket,name FROM objects WHERE bucket=? AND name=?",
		bucket, objectName).Scan(&bkt, &name)
	switch {
	case err == sql.ErrNoRows:
		return false, siaSuccess // Object does not exist
	case err != nil:
		return false, siaErrorDatabaseSelectError // An error occured
	default:
		if bkt == bucket && name == objectName {
			return true, siaSuccess // Object exists
		}
	}

	// Shouldn't happen, but just in case
	return false, siaErrorUnknown
}

// Updates last_fetch time and sets the cached_fetches quantity
func (cache *SiaCacheLayer) updateCachedFetches(bucket string, objectName string, fetches int64) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.updateCachedFetches")

	cache.lockDB()
	defer cache.unlockDB()

	stmt, err := cache.Db.Prepare("UPDATE objects SET last_fetch=?, cached_fetches=? WHERE bucket=? AND name=?")
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	defer stmt.Close()

	_, err = stmt.Exec(time.Now().Unix(), fetches, bucket, objectName)
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	return siaSuccess
}

// Updates the cached status of the object
func (cache *SiaCacheLayer) updateCachedStatus(bucket string, objectName string, status int64) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.updateCachedFetches")

	cache.lockDB()
	defer cache.unlockDB()

	stmt, err := cache.Db.Prepare("UPDATE objects SET cached=? WHERE bucket=? AND name=?")
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	defer stmt.Close()

	_, err = stmt.Exec(status, bucket, objectName)
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	return siaSuccess
}

func (cache *SiaCacheLayer) updateSiaFetches(bucket string, objectName string, fetches int64) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.updateSiaFetches")

	cache.lockDB()
	defer cache.unlockDB()

	stmt, err := cache.Db.Prepare("UPDATE objects SET last_fetch=?, sia_fetches=? WHERE bucket=? AND name=?")
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	defer stmt.Close()

	_, err = stmt.Exec(time.Now().Unix(), fetches, bucket, objectName)
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	return siaSuccess
}

func (cache *SiaCacheLayer) listUploadingObjects() (objects []SiaObjectInfo, e SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.listUploadingObjects")

	cache.lockDB()
	defer cache.unlockDB()

	rows, err := cache.Db.Query("SELECT bucket,name,size,queued,purge_after,cached_fetches,sia_fetches,last_fetch,deleted,cached FROM objects WHERE uploaded=0")
	if err != nil {
		return objects, siaErrorDatabaseSelectError
	}

	var bucket string
	var name string
	var size int64
	var queued int64
	var purge_after int64
	var cached_fetches int64
	var sia_fetches int64
	var last_fetch int64
	var deleted int64
	var cached int64

	for rows.Next() {
		err = rows.Scan(&bucket, &name, &size, &queued, &purge_after, &cached_fetches, &sia_fetches, &last_fetch, &deleted, &cached)
		if err != nil {
			return objects, siaErrorDatabaseSelectError
		}

		objects = append(objects, SiaObjectInfo{
			Bucket:        bucket,
			Name:          name,
			Size:          size,
			Queued:        time.Unix(queued, 0),
			Uploaded:      time.Unix(0, 0),
			PurgeAfter:    purge_after,
			CachedFetches: cached_fetches,
			SiaFetches:    sia_fetches,
			LastFetch:     time.Unix(last_fetch, 0),
			Deleted:       deleted,
			Cached:        cached,
		})
	}

	rows.Close()

	return objects, siaSuccess
}

func (cache *SiaCacheLayer) waitTillSiaUploadCompletes(siaObj string) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.waitTillSiaUploadCompletes")
	complete := false
	for !complete {
		avail, e := cache.isSiaFileAvailable(siaObj)
		if e != siaSuccess {
			return e
		}

		if avail {
			return siaSuccess
		}
		time.Sleep(time.Duration(cache.UploadCheckFreqMs) * time.Millisecond)
	}

	return siaSuccess
}

func (cache *SiaCacheLayer) isSiaFileAvailable(siaObj string) (bool, SiaServiceError) {
	cache.debugmsg(fmt.Sprintf("SiaCacheLayer.isSiaFileAvailable: %s", siaObj))
	var rf api.RenterFiles
	err := getAPI(cache.SiadAddress, "/renter/files", &rf)
	if err != nil {
		return false, SiaServiceError{Code: "SiaErrorDaemon", Message: err.Error()}
	}

	for _, file := range rf.Files {
		cache.debugmsg(fmt.Sprintf("  Renter file: %s", file.SiaPath))
		if file.SiaPath == siaObj {
			return file.Available, siaSuccess
		}
	}

	return false, SiaServiceError{Code: "SiaErrorDaemon", Message: "File not in Sia renter list"}
}

func (cache *SiaCacheLayer) insertObjectToDb(bucket string, objectName string, size int64, queued int64, uploaded int64, purge_after int64, src_file string, cached int64) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.insertObjectToDb")

	cache.lockDB()
	defer cache.unlockDB()

	stmt, err := cache.Db.Prepare("INSERT INTO objects(bucket, name, size, queued, uploaded, purge_after, cached_fetches, sia_fetches, last_fetch, src_file, deleted, cached) values(?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	defer stmt.Close()

	_, err = stmt.Exec(bucket,
		objectName,
		size,
		queued,
		uploaded,
		purge_after,
		0,
		0,
		0,
		src_file,
		0,
		cached)
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	return siaSuccess
}

func (cache *SiaCacheLayer) deleteObjectFromDb(bucket string, objectName string) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.deleteObjectFromDb")

	cache.lockDB()
	defer cache.unlockDB()

	stmt, err := cache.Db.Prepare("DELETE FROM objects WHERE bucket=? AND name=?")
	if err != nil {
		return siaErrorDatabaseDeleteError
	}

	defer stmt.Close()

	_, err = stmt.Exec(bucket, objectName)
	if err != nil {
		return siaErrorDatabaseDeleteError
	}
	return siaSuccess
}

func (cache *SiaCacheLayer) guaranteeCacheSpace(extraSpace int64) SiaServiceError {
	cache.debugmsg("SiaCacheLayer.guaranteeCacheSpace")
	cs, e := cache.getCacheSize()
	if e != siaSuccess {
		return e
	}

	space_needed := int64(cache.MaxCacheSizeBytes) - extraSpace
	for cs > space_needed {
		e = cache.forceDeleteOldestCacheFile()
		if e != siaSuccess {
			return e
		}

		cs, e = cache.getCacheSize()
		if e != siaSuccess {
			return e
		}
	}

	return siaSuccess
}

func (cache *SiaCacheLayer) getSiaObjectName(bucket string, objectName string) string {
	reg, _ := regexp.Compile("[^a-zA-Z0-9.]+")

	cleanedName := reg.ReplaceAllString(objectName, "+")
	return bucket + "/" + cleanedName
}

func (cache *SiaCacheLayer) forceDeleteOldestCacheFile() SiaServiceError {
	cache.debugmsg("SiaCacheLayer.forceDeleteOldestCacheFile")

	cache.lockDB()

	var bucket string
	var objectName string
	var src_file string
	err := cache.Db.QueryRow("SELECT bucket,name,src_file FROM objects WHERE uploaded>0 AND cached=1 ORDER BY last_fetch DESC LIMIT 1").Scan(&bucket, &objectName, &src_file)
	cache.unlockDB()

	switch {
	case err == sql.ErrNoRows:
		return siaErrorUnableToClearAnyCachedFiles
	case err != nil:
		// An error occured
		return siaErrorUnableToClearAnyCachedFiles
	default:
		// Make certain cached item exists, then delete it.
		_, err = os.Stat(src_file)
		if err != nil {
			// Item does NOT exist in cache. Could have been deleted manually by user.
			// Update the cached flag and return. (Returning failure would stop cache manager.)
			return cache.markObjectCached(bucket, objectName, 0)
		}

		err = os.Remove(src_file)
		if err != nil {
			return siaErrorUnableToClearAnyCachedFiles
		}

		err = cache.markObjectCached(bucket, objectName, 0)
		if err != nil {
			return siaErrorUnableToClearAnyCachedFiles
		}
		return siaSuccess
	}

	return siaErrorUnableToClearAnyCachedFiles // Shouldn't happen
}

func (cache *SiaCacheLayer) getCacheSize() (int64, SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.getCacheSize")
	var size int64
	err := filepath.Walk(cache.CacheDir, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	if err != nil {
		return 0, siaErrorDeterminingCacheSize
	}
	return size, siaSuccess
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
		log.Printf("%s took %s\n", name, elapsed)
	}
}
