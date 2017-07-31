package cmd

import (
	"fmt"
	"time"
	"os"
	"sync"
	"strconv"
	"regexp"
	"net/url"
	"path/filepath"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/NebulousLabs/Sia/api"
	"github.com/minio/minio-go/pkg/policy"
	"encoding/json"
)

// How many seconds to delay between cache/db management operations
var SIA_MANAGER_DELAY_SEC = 30

// How many milliseconds to wait between checks to see if file uploaded
var SIA_UPLOAD_CHECK_FREQ_MS = 3000

var SIA_CACHE_MAX_SIZE_BYTES = int64(10000000000) // 10GB

// Global ticker for cache management
var g_cache_ticker *time.Ticker

// Global database object
var g_db *sql.DB

// Mutex for DB access
var g_db_mutex = &sync.Mutex{}

type SiaCacheLayer struct {
	SiadAddress string 	// Address of siad daemon API. (e.g., "127.0.0.1:9980")
	CacheDir string 	// Cache directory for downloads
	DbFile string 		// Name and path of Sqlite database file
}


type SiaBucketInfo struct {
	Name string 				// Name of bucket
	Created time.Time   		// Time of bucket creation
}

type SiaObjectInfo struct {
	Bucket string 		// Name of bucket object is stored in
	Name string 		// Name of object
	Size int64			// Size of the object in bytes
	Queued time.Time 	// Time object was queued for upload to Sia
	Uploaded time.Time 	// Time object was successfully uploaded to Sia
	PurgeAfter int64    // If no downloads in this many seconds, purge from cache.
	                    // Always in cache if value is 0.
	CachedFetches int64	// The total number of times the object has been fetched from cache
	SiaFetches int64 	// The total number of times the object has been fetched from Sia network
	LastFetch time.Time // The time of the last fetch request for the object
	SrcFile string 		// The absolute path of the original source file
	Deleted int64       // Whether object is marked for delete. 0: Not Deleted, 1: Deleted
	Cached int64        // Whether object is currently in cache. 0: Not Cached, 1: Cached
}

type SiaServiceError struct {
	Code                      string
	Message                   string
}

func (e SiaServiceError) Error() string {
	return fmt.Sprintf("Sia Error: %s",
		e.Message)
}

//Also: SiaErrorDaemon for daemon errors

var siaSuccess = SiaServiceError{
	Code:       "SiaSuccess",
	Message:    "",
}
var siaErrorUnableToClearAnyCachedFiles = SiaServiceError{
	Code:       "SiaErrorUnableToClearAnyCachedFiles",
	Message:    "Unable to clear any files from cache.",
}
var siaErrorDeterminingCacheSize = SiaServiceError{
	Code:       "SiaErrorDeterminingCacheSize",
	Message:    "Unable to determine total size of files in cache.",
}
var siaErrorDatabaseDeleteError = SiaServiceError{
	Code:       "SiaErrorDatabaseDeleteError",
	Message:    "Failed to delete a record in the cache database.",
}
var siaErrorDatabaseCreateError = SiaServiceError{
	Code:       "SiaErrorDatabaseCreateError",
	Message:    "Failed to create a table in the cache database.",
}
var siaErrorDatabaseCantBeOpened = SiaServiceError{
	Code:       "SiaErrorDatabaseCantBeOpened",
	Message:    "The cache database could not be opened.",
}
var siaErrorDatabaseInsertError = SiaServiceError{
	Code:       "SiaErrorDatabaseInsertError",
	Message:    "Failed to insert a record in the cache database.",
}
var siaErrorDatabaseUpdateError = SiaServiceError{
	Code:       "SiaErrorDatabaseUpdateError",
	Message:    "Failed to update a record in the cache database.",
}
var siaErrorDatabaseSelectError = SiaServiceError{
	Code:       "SiaErrorDatabaseSelectError",
	Message:    "Failed to select records in the cache database.",
}
var siaErrorUnknown = SiaServiceError{
	Code:       "SiaErrorUnknown",
	Message:    "An unknown error has occured.",
}
var siaErrorFailedToDeleteCachedFile = SiaServiceError{
	Code:       "SiaFailedToDeleteCachedFile",
	Message:    "Failed to delete cached file. Check permissions.",
}
var siaErrorObjectDoesNotExistInBucket = SiaServiceError{
	Code:       "SiaErrorObjectDoesNotExistInBucket",
	Message:    "Object does not exist in bucket.",
}
var siaErrorObjectAlreadyExists = SiaServiceError{
	Code:       "SiaErrorObjectAlreadyExists",
	Message:    "Object does not exist in bucket.",
}
var siaErrorInvalidBucketPolicy = SiaServiceError{
	Code:       "SiaErrorInvalidBucketPolicy",
	Message:    "An invalid bucket policy has been specified.",
}

// Need to implement os.FileInfo interface on SiaObjectInfo for convenience.
type SiaFileInfo struct {
	FileName string
	FileSize int64
	FileModTime time.Time
	FileMode os.FileMode
	FileIsDir bool
	FileSys interface {}
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

// Called to start running the SiaBridge
func (b *SiaCacheLayer) Start() SiaServiceError {
	debugmsg("SiaCacheLayer.Start")
	b.loadSiaEnv()

	b.ensureCacheDirExists()

	// Open and initialize database
	err := b.initDatabase() 
	if err != siaSuccess {
		return err
	}

	// Start the cache management process
	g_cache_ticker = time.NewTicker(time.Second * time.Duration(SIA_MANAGER_DELAY_SEC))
    go func() {
        for _ = range g_cache_ticker.C {
        	b.manager()
        }
    }()

    return siaSuccess
}

// Attempt to load Sia config from ENV
func (b *SiaCacheLayer) loadSiaEnv() {
	tmp := os.Getenv("SIA_MANAGER_DELAY_SEC")
	if tmp != "" {
		i, err := strconv.Atoi(tmp)
		if err == nil {
			SIA_MANAGER_DELAY_SEC = i
		}
	}
	fmt.Printf("SIA_MANAGER_DELAY_SEC: %d\n", SIA_MANAGER_DELAY_SEC)

	tmp = os.Getenv("SIA_UPLOAD_CHECK_FREQ_MS")
	if tmp != "" {
		i, err := strconv.Atoi(tmp)
		if err == nil {
			SIA_UPLOAD_CHECK_FREQ_MS = i
		}
	}
	fmt.Printf("SIA_UPLOAD_CHECK_FREQ_MS: %d\n", SIA_UPLOAD_CHECK_FREQ_MS)

	tmp = os.Getenv("SIA_CACHE_MAX_SIZE_BYTES")
	if tmp != "" {
		i, err := strconv.ParseInt(tmp, 10, 64)
		if err == nil {
			SIA_CACHE_MAX_SIZE_BYTES = i
		}
	}
	fmt.Printf("SIA_CACHE_MAX_SIZE_BYTES: %d\n", SIA_CACHE_MAX_SIZE_BYTES)
}

// Called to stop the SiaBridge
func (b *SiaCacheLayer) Stop() {
	debugmsg("SiaCacheLayer.Stop")
	// Stop cache management process
	g_cache_ticker.Stop()

	// Close the database
	b.lockDB()
	g_db.Close()
	b.unlockDB()
}

func (b *SiaCacheLayer) lockDB() {
	//debugmsg("SiaCacheLayer.LockDB")
	g_db_mutex.Lock()
}

func (b *SiaCacheLayer) unlockDB() {
	//debugmsg("SiaCacheLayer.UnlockDB")
	g_db_mutex.Unlock()
}

func (b *SiaCacheLayer) InsertBucket(bucket string) SiaServiceError {
	debugmsg("SiaCacheLayer.InsertBucket")

	b.lockDB()
	defer b.unlockDB()
	
	stmt, err := g_db.Prepare("INSERT INTO buckets(name, created, policy) values(?,?,?)")
    if err != nil {
    	return siaErrorDatabaseInsertError
    }

    defer stmt.Close()

    policyInfo := policy.BucketAccessPolicy{}
    res, _ := json.Marshal(&policyInfo)
    _, err = stmt.Exec(bucket, time.Now().Unix(), string(res))
    if err != nil {
    	debugmsg(err.Error())
    	return siaErrorDatabaseInsertError
    }

    return siaSuccess
}

func (b *SiaCacheLayer) DeleteBucket(bucket string) SiaServiceError {
	debugmsg("SiaCacheLayer.DeleteBucket")
	// First, should delete all objects in the bucket
	objects, err := b.ListObjects(bucket)
	if err != siaSuccess {
		return err
	}
	for _, object := range objects {
		err = b.DeleteObject(bucket, object.Name)
		if err != siaSuccess {
			return err
		}
	}

	b.lockDB()
	defer b.unlockDB()

	// Now delete the actual bucket
	stmt, e := g_db.Prepare("DELETE FROM buckets WHERE name=?")
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
func (b *SiaCacheLayer) ListBuckets() (buckets []SiaBucketInfo, e SiaServiceError) {
	debugmsg("SiaCacheLayer.ListBuckets")

	b.lockDB()
	defer b.unlockDB()

	rows, err := g_db.Query("SELECT name,created FROM buckets")
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
    		Name:		name,
    		Created: 	time.Unix(created, 0),
    	})
    }

    rows.Close()

	return buckets, siaSuccess
}

// Deletes the object
func (b *SiaCacheLayer) DeleteObject(bucket string, objectName string) SiaServiceError {
	debugmsg("SiaCacheLayer.DeleteObject")
	err := b.markObjectDeleted(bucket, objectName)
	if err != siaSuccess {
		return err
	}

    // Tell Sia daemon to delete the object
	var siaObj = b.getSiaObjectName(bucket, objectName)
	
	derr := post(b.SiadAddress, "/renter/delete/"+siaObj, "")
	if derr != nil {
		return SiaServiceError{ Code: "SiaErrorDaemon", Message: derr.Error(), }
	}

	return b.deleteObjectFromDb(bucket, objectName)
}

func (b *SiaCacheLayer) PutObject(bucket string, objectName string, size int64, purge_after int64, src_file string) SiaServiceError {
	debugmsg("SiaCacheLayer.PutObject")
	
	// First, make sure space exists in cache
	err := b.guaranteeCacheSpace(size)
	if err != siaSuccess {
		return err
	}

	// Before inserting to DB, there is a very rare chance that the object already exists in DB 
	// from a failed upload and Minio crashed or was killed before DB updated to reflect. So just in case 
	// we will check if the object exists and has a not uploaded status. If so, we will delete that 
	// record and then continue as normal.
	objInfo, e := b.GetObjectInfo(bucket, objectName)
	if e == siaSuccess {
		// Object does exist. If uploaded, return error. If not uploaded, delete it and continue.
		if objInfo.Uploaded.Unix() > 0 {
			return siaErrorObjectAlreadyExists
		} else {
			e = b.deleteObjectFromDb(bucket, objectName)
			if e != siaSuccess {
				return e
			}
		}
	}

	err = b.insertObjectToDb(bucket, objectName, size, time.Now().Unix(), 0, purge_after, src_file, 1)
	if err != siaSuccess {
		return err
	}

	// Tell Sia daemon to upload the object
	siaObj := b.getSiaObjectName(bucket, objectName)
	derr := post(b.SiadAddress, "/renter/upload/"+siaObj, "source="+src_file)
	if derr != nil {
		b.deleteObjectFromDb(bucket, objectName)
		return SiaServiceError{ Code: "SiaErrorDaemon", Message: derr.Error(), }
	}

	// Need to wait for upload to complete
	err = b.waitTillSiaUploadCompletes(siaObj)
	if err != siaSuccess {
		b.deleteObjectFromDb(bucket, objectName)
		return err
	}

	// Mark object as uploaded
	err = b.markObjectUploaded(bucket, objectName)
	if err != siaSuccess {
		b.deleteObjectFromDb(bucket, objectName)
		return err
	}

	return siaSuccess
}

// Returns a list of objects in the bucket provided
func (b *SiaCacheLayer) ListObjects(bucket string) (objects []SiaObjectInfo, e SiaServiceError) {
	debugmsg("SiaCacheLayer.listUploadingObjects")

	b.lockDB()
	defer b.unlockDB()

	rows, err := g_db.Query("SELECT name,size,queued,uploaded,purge_after,cached_fetches,sia_fetches,last_fetch,src_file,deleted,cached FROM objects WHERE bucket=?",bucket)
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
        	Bucket:			bucket,
    		Name:			name,
    		Size:       	size,
    		Queued:         time.Unix(queued, 0),
    		Uploaded: 		time.Unix(uploaded, 0),
    		PurgeAfter:		purge_after,
    		CachedFetches:	cached_fetches,
    		SiaFetches:		sia_fetches,
    		LastFetch:  	time.Unix(last_fetch, 0),
    		SrcFile:		src_file,
    		Deleted:		deleted,
    		Cached:         cached,
    	})
    }

	return objects, siaSuccess
}

func (b *SiaCacheLayer) GuaranteeObjectIsInCache(bucket string, objectName string) SiaServiceError {
	defer b.timeTrack(time.Now(), "GuaranteeObjectIsInCache")
	debugmsg("SiaCacheLayer.GuaranteeObjectIsInCache")
	// Minio filesystem layer may request files from .minio.sys bucket
	// If we get a request for Minio, we'll pass back success and let Minio deal with it.
	if bucket == ".minio.sys" {
		return siaSuccess
	}
	
	// Make sure object exists in database
	objInfo, err := b.GetObjectInfo(bucket, objectName)
	if err != siaSuccess {
		return err
	}

	// Is file already in cache?
	_, serr := os.Stat(objInfo.SrcFile);
	if serr == nil {
		// File exists in cache
		err = b.updateCachedStatus(bucket, objectName, 1)
		if err != siaSuccess {
			return err
		}
		// Increment cached fetch count and update last_fetch
    	return b.updateCachedFetches(bucket, objectName, objInfo.CachedFetches+1)
	}
	// Object not in cache, must download from Sia.
    // First, though, make sure the file was completely uploaded to Sia.
    if objInfo.Uploaded == time.Unix(0,0) {
    	// File never completed uploading, or was never marked as uploaded in database.
    	// Neither of these cases should happen, but just in case.
    	return siaErrorUnknown
    }

    // Make sure bucket path exists in cache directory
    b.ensureCacheBucketDirExists(bucket)

	// Make sure enough space exists in cache
	err = b.guaranteeCacheSpace(objInfo.Size)
	if err != siaSuccess {
		return err
	}

	// Increment fetch count and update last_fetch BEFORE requesting d/l from Sia.
	// This will prevent the cache manager from removing the partially downloaded file.
	err = b.updateSiaFetches(bucket, objectName, objInfo.SiaFetches+1)
	if err != siaSuccess {
		return err
	}

	var siaObj = b.getSiaObjectName(bucket, objectName)
	debugmsg(fmt.Sprintf("GET %s %s %s\n", b.SiadAddress, siaObj, objInfo.SrcFile))
	derr := get(b.SiadAddress, "/renter/download/" + siaObj + "?destination=" + url.QueryEscape(objInfo.SrcFile))
	if derr != nil {
		debugmsg(fmt.Sprintf("Error: %s", derr))
		return SiaServiceError{ Code: "SiaErrorDaemon", Message: derr.Error(), }
	}

	// After successfully downloading to the cache, make sure the cached flag of the object is set.
	return b.updateCachedStatus(bucket, objectName, 1)
}


// Returns info for the provided object
func (b *SiaCacheLayer) GetObjectInfo(bucket string, objectName string) (objInfo SiaObjectInfo, e SiaServiceError) {
	debugmsg("SiaCacheLayer.GetObjectInfo")

	b.lockDB()
	defer b.unlockDB()

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
	err := g_db.QueryRow("SELECT size,queued,uploaded,purge_after,cached_fetches,sia_fetches,last_fetch,src_file,deleted,cached FROM objects WHERE name=? AND bucket=?", objectName, bucket).Scan(&size,&queued,&uploaded,&purge_after,&cached_fetches,&sia_fetches,&last_fetch,&src_file,&deleted,&cached)
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
		objInfo.Queued = time.Unix(queued,0)
		objInfo.Uploaded = time.Unix(uploaded,0)
		objInfo.PurgeAfter = purge_after
		objInfo.CachedFetches = cached_fetches
		objInfo.SiaFetches = sia_fetches
		objInfo.LastFetch = time.Unix(last_fetch,0)
		objInfo.SrcFile = src_file
		objInfo.Deleted = deleted
		objInfo.Cached = cached
		return objInfo, siaSuccess 	
	}

	// Shouldn't happen, but just in case
	return objInfo, siaErrorUnknown
}

// SetBucketPolicies sets policy on bucket
func (b *SiaCacheLayer) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) SiaServiceError {
	debugmsg("SiaCacheLayer.SetBucketPolicies")
	res, _ := json.Marshal(&policyInfo)
    return b.updateBucketPolicy(bucket, string(res))
}

// GetBucketPolicies will get policy on bucket
func (b *SiaCacheLayer) GetBucketPolicies(bucket string) (bal policy.BucketAccessPolicy, e SiaServiceError) {
	debugmsg("SiaCacheLayer.GetBucketPolicies")

	b.lockDB()
	defer b.unlockDB()

	// Query the database
	var bktPolicy string
	err := g_db.QueryRow("SELECT policy FROM buckets WHERE name=?", bucket).Scan(&bktPolicy)
	switch {
	case err == sql.ErrNoRows:
	   return bal, siaErrorDatabaseSelectError		// Bucket does not exist
	case err != nil:
	   return bal, siaErrorDatabaseSelectError 		// An error occured
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
func (b *SiaCacheLayer) DeleteBucketPolicies(bucket string) SiaServiceError {
	debugmsg("SiaCacheLayer.DeleteBucketPolicies")
	
	return b.updateBucketPolicy(bucket, "")
}

func (b *SiaCacheLayer) updateBucketPolicy(bucket string, bktPolicy string) SiaServiceError {
	debugmsg("SiaCacheLayer.updateBucketPolicy")

	b.lockDB()
	defer b.unlockDB()

	stmt, err := g_db.Prepare("UPDATE buckets SET policy=? WHERE name=?")
    if err != nil {
    	debugmsg(err.Error())
    	return siaErrorDatabaseUpdateError
    }

    defer stmt.Close()

    _, err = stmt.Exec(bktPolicy, bucket)
    if err != nil {
    	debugmsg(err.Error())
    	return siaErrorDatabaseUpdateError
    }

    return siaSuccess
}

// Runs periodically to manage the database and cache
func (b *SiaCacheLayer) manager() {
	debugmsg("SiaCacheLayer.manager")
	// Check to see if any files in database have completed uploading to Sia.
	// If so, update uploaded timestamp in database.
	err := b.checkSiaUploads()
	if err != siaSuccess {
		fmt.Println("Error in DB/Cache Management Process:")
		fmt.Println(err)
	}

	// Remove files from cache that have not been uploaded or fetched in purge_after seconds.
	err = b.purgeCache()
	if err != siaSuccess {
		fmt.Println("Error in DB/Cache Management Process:")
		fmt.Println(err)
	}

	// Check cache disk usage
	err = b.guaranteeCacheSpace(0)
	if err != siaSuccess {
		fmt.Println("Error in DB/Cache Management Process:")
		fmt.Println(err)
	}

}

// Purge older, infrequently accessed files from cache.
// This function is less strict and doesn't consider max space quota.
func (b *SiaCacheLayer) purgeCache() SiaServiceError {
	debugmsg("SiaCacheLayer.purgeCache")
	buckets, err := b.ListBuckets()
	if err != siaSuccess {
		return err
	}

	for _, bucket := range buckets {
		objects, err := b.ListObjects(bucket.Name)
		if err != siaSuccess {
			return err
		}

		for _, object := range objects {
			// Only remove an object from cache here if:
			// 1. Object is cached
			// 1. Object was uploaded over PurgeAfter seconds ago
			// 2. Object hasn't been fetched in over PurgeAfter seconds
			if object.Cached == 1 && object.Uploaded != time.Unix(0,0) {
				since_uploaded := time.Now().Unix() - object.Uploaded.Unix()
				since_fetched := time.Now().Unix() - object.LastFetch.Unix()
				if since_uploaded > object.PurgeAfter && since_fetched > object.PurgeAfter {
					err = b.removeFromCache(object)
					if err != siaSuccess {
						return err
					}
				}
			}
		}
	}
	return siaSuccess
}

func (b *SiaCacheLayer) removeFromCache(objInfo SiaObjectInfo) SiaServiceError {
	debugmsg(fmt.Sprintf("removeFromCache: %s", objInfo.SrcFile))

	// If file doesn't exist in cache, it's falsely labelled. Update and return.
	_, err := os.Stat(objInfo.SrcFile)
	if err != nil {
		return b.markObjectCached(objInfo.Bucket, objInfo.Name, 0)
	}

	err = os.Remove(objInfo.SrcFile)
	if err != nil {
		// File exists but couldn't be deleted. Permission issue?
		return siaErrorFailedToDeleteCachedFile
	}

	return b.markObjectCached(objInfo.Bucket, objInfo.Name, 0)
}

func (b *SiaCacheLayer) checkSiaUploads() SiaServiceError {
	debugmsg("SiaCacheLayer.checkSiaUploads")
	// Get list of all uploading objects
	objs, err := b.listUploadingObjects()
	if err != siaSuccess {
		return err
	}

	// Get list of all renter files
	var rf api.RenterFiles
	derr := getAPI(b.SiadAddress, "/renter/files", &rf)
	if derr != nil {
		return SiaServiceError{ Code: "SiaErrorDaemon", Message: derr.Error(), }
	}

	// If uploading object is available on Sia, update database
	for _, obj := range objs {
		var siaObj = b.getSiaObjectName(obj.Bucket, obj.Name)
		for _, file := range rf.Files {
			if file.SiaPath == siaObj && file.Available {
				err = b.markObjectUploaded(obj.Bucket, obj.Name)
				if err != siaSuccess {
					return err
				}
			}
		}
	}

	return siaSuccess
}

func (b *SiaCacheLayer) markObjectUploaded(bucket string, objectName string) SiaServiceError {
	debugmsg("SiaCacheLayer.markObjectUploaded")

	b.lockDB()
	defer b.unlockDB()

	stmt, err := g_db.Prepare("UPDATE objects SET uploaded=? WHERE bucket=? AND name=?")
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

func (b *SiaCacheLayer) markObjectDeleted(bucket string, objectName string) SiaServiceError {
	debugmsg("SiaCacheLayer.markObjectDeleted")
	
	b.lockDB()
	defer b.unlockDB()

	stmt, err := g_db.Prepare("UPDATE objects SET deleted=1 WHERE bucket=? AND name=?")
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

func (b *SiaCacheLayer) markObjectCached(bucket string, objectName string, status int) SiaServiceError {
	debugmsg("SiaCacheLayer.markObjectCached")

	b.lockDB()
	defer b.unlockDB()

	stmt, err := g_db.Prepare("UPDATE objects SET cached=? WHERE bucket=? AND name=?")
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

func (b *SiaCacheLayer) initDatabase() SiaServiceError {
	debugmsg("SiaCacheLayer.initDatabase")

	b.lockDB()
	defer b.unlockDB()

	// Open the database
	var e error
	g_db, e = sql.Open("sqlite3", b.DbFile)
	if e != nil {
		return siaErrorDatabaseCantBeOpened
	}

	// Make sure buckets table exists
	stmt, err := g_db.Prepare("CREATE TABLE IF NOT EXISTS buckets(name TEXT PRIMARY KEY, created INTEGER, policy TEXT)")
    if err != nil {
    	return siaErrorDatabaseCreateError
    }

    defer stmt.Close()

	_, err = stmt.Exec()
    if err != nil {
    	return siaErrorDatabaseCreateError
    }

	// Make sure objects table exists
    stmt2, e := g_db.Prepare("CREATE TABLE IF NOT EXISTS objects(bucket TEXT, name TEXT, size INTEGER, queued INTEGER, uploaded INTEGER, purge_after INTEGER, cached_fetches INTEGER, sia_fetches INTEGER, last_fetch INTEGER, src_file TEXT, deleted INTEGER, cached INTEGER, PRIMARY KEY(bucket,name) )")
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

func (b *SiaCacheLayer) bucketExists(bucket string) (exists bool, e SiaServiceError) {
	debugmsg("SiaCacheLayer.bucketExists")

	b.lockDB()
	defer b.unlockDB()

	// Query the database
	var name string
	err := g_db.QueryRow("SELECT name FROM buckets WHERE name=?", bucket).Scan(&name)
	switch {
	case err == sql.ErrNoRows:
	   return false, siaSuccess		// Bucket does not exist
	case err != nil:
	   return false, siaErrorDatabaseSelectError 		// An error occured
	default:
		if name == bucket {
			return true, siaSuccess 	// Bucket exists
		}
	}

	// Shouldn't happen, but just in case
	return false, siaErrorUnknown
}

func (b *SiaCacheLayer) objectExists(bucket string, objectName string) (exists bool, e SiaServiceError) {
	debugmsg("SiaCacheLayer.objectExists")

	b.lockDB()
	defer b.unlockDB()

	// Query the database
	var bkt string
	var name string
	err := g_db.QueryRow("SELECT bucket,name FROM objects WHERE bucket=? AND name=?", 
							bucket, objectName).Scan(&bkt,&name)
	switch {
	case err == sql.ErrNoRows:
	   return false, siaSuccess		// Object does not exist
	case err != nil:
	   return false, siaErrorDatabaseSelectError 		// An error occured
	default:
		if bkt == bucket && name == objectName {
			return true, siaSuccess 	// Object exists
		}
	}

	// Shouldn't happen, but just in case
	return false, siaErrorUnknown
}

// Updates last_fetch time and sets the cached_fetches quantity
func (b *SiaCacheLayer) updateCachedFetches(bucket string, objectName string, fetches int64) SiaServiceError {
	debugmsg("SiaCacheLayer.updateCachedFetches")

	b.lockDB()
	defer b.unlockDB()

	stmt, err := g_db.Prepare("UPDATE objects SET last_fetch=?, cached_fetches=? WHERE bucket=? AND name=?")
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
func (b *SiaCacheLayer) updateCachedStatus(bucket string, objectName string, status int64) SiaServiceError {
	debugmsg("SiaCacheLayer.updateCachedFetches")

	b.lockDB()
	defer b.unlockDB()

	stmt, err := g_db.Prepare("UPDATE objects SET cached=? WHERE bucket=? AND name=?")
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

func (b *SiaCacheLayer) updateSiaFetches(bucket string, objectName string, fetches int64) SiaServiceError {
	debugmsg("SiaCacheLayer.updateSiaFetches")

	b.lockDB()
	defer b.unlockDB()

	stmt, err := g_db.Prepare("UPDATE objects SET last_fetch=?, sia_fetches=? WHERE bucket=? AND name=?")
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

func (b *SiaCacheLayer) listUploadingObjects() (objects []SiaObjectInfo, e SiaServiceError) {
	debugmsg("SiaCacheLayer.listUploadingObjects")

	b.lockDB()
	defer b.unlockDB()

	rows, err := g_db.Query("SELECT bucket,name,size,queued,purge_after,cached_fetches,sia_fetches,last_fetch,deleted,cached FROM objects WHERE uploaded=0")
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
        	Bucket:			bucket,
    		Name:			name,
    		Size:       	size,
    		Queued:         time.Unix(queued, 0),
    		Uploaded: 		time.Unix(0, 0),
    		PurgeAfter:		purge_after,
    		CachedFetches:	cached_fetches,
    		SiaFetches:		sia_fetches,
    		LastFetch:  	time.Unix(last_fetch, 0),
    		Deleted:		deleted,
    		Cached:         cached,
    	})
    }

    rows.Close()

	return objects, siaSuccess
}

func (b *SiaCacheLayer) waitTillSiaUploadCompletes(siaObj string) SiaServiceError {
	debugmsg("SiaCacheLayer.waitTillSiaUploadCompletes")
	complete := false
	for !complete {
		avail, e := b.isSiaFileAvailable(siaObj)
		if e != siaSuccess {
			return e
		}

		if avail {
			return siaSuccess
		}
		time.Sleep(time.Duration(SIA_UPLOAD_CHECK_FREQ_MS) * time.Millisecond)
	}

	return siaSuccess
}

func (b *SiaCacheLayer) isSiaFileAvailable(siaObj string) (bool, SiaServiceError) {
	debugmsg(fmt.Sprintf("SiaCacheLayer.isSiaFileAvailable: %s", siaObj))
	var rf api.RenterFiles
	err := getAPI(b.SiadAddress, "/renter/files", &rf)
	if err != nil {
		return false, SiaServiceError{ Code: "SiaErrorDaemon", Message: err.Error(), }
	}

	for _, file := range rf.Files {
		debugmsg(fmt.Sprintf("  Renter file: %s", file.SiaPath))
		if file.SiaPath == siaObj {
			return file.Available, siaSuccess
		}
	}

	return false, SiaServiceError{ Code: "SiaErrorDaemon", Message: "File not in Sia renter list", }
}

func (b *SiaCacheLayer) insertObjectToDb(bucket string, objectName string, size int64, queued int64, uploaded int64, purge_after int64, src_file string, cached int64) SiaServiceError {
	debugmsg("SiaCacheLayer.insertObjectToDb")

	b.lockDB()
	defer b.unlockDB()

	stmt, err := g_db.Prepare("INSERT INTO objects(bucket, name, size, queued, uploaded, purge_after, cached_fetches, sia_fetches, last_fetch, src_file, deleted, cached) values(?,?,?,?,?,?,?,?,?,?,?,?)")
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

func (b *SiaCacheLayer) deleteObjectFromDb(bucket string, objectName string) SiaServiceError {
	debugmsg("SiaCacheLayer.deleteObjectFromDb")

	b.lockDB()
	defer b.unlockDB()

	stmt, err := g_db.Prepare("DELETE FROM objects WHERE bucket=? AND name=?")
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

func (b *SiaCacheLayer) guaranteeCacheSpace(extraSpace int64) SiaServiceError {
	debugmsg("SiaCacheLayer.guaranteeCacheSpace")
	cs, e := b.getCacheSize()
	if e != siaSuccess {
		return e
	}

	space_needed := int64(SIA_CACHE_MAX_SIZE_BYTES) - extraSpace
	for cs > space_needed {
		e = b.forceDeleteOldestCacheFile()
		if e != siaSuccess {
			return e
		}

		cs, e = b.getCacheSize()
		if e != siaSuccess {
			return e
		}
	}

	return siaSuccess
}

func (b *SiaCacheLayer) getSiaObjectName(bucket string, objectName string) string {
	reg, _ := regexp.Compile("[^a-zA-Z0-9]+")
    
    cleanedName := reg.ReplaceAllString(objectName, "+")
	return bucket + "/" + cleanedName
}

func (b *SiaCacheLayer) forceDeleteOldestCacheFile() SiaServiceError {
	debugmsg("SiaCacheLayer.forceDeleteOldestCacheFile")

	b.lockDB()

	var bucket string
	var objectName string
	var src_file string
	err := g_db.QueryRow("SELECT bucket,name,src_file FROM objects WHERE uploaded>0 AND cached=1 ORDER BY last_fetch DESC LIMIT 1").Scan(&bucket,&objectName,&src_file)
	b.unlockDB()
	
	switch {
	case err == sql.ErrNoRows:
		return siaErrorUnableToClearAnyCachedFiles;
	case err != nil:
		// An error occured
		return siaErrorUnableToClearAnyCachedFiles
	default:
		// Make certain cached item exists, then delete it.
		_, err = os.Stat(src_file)
		if err != nil {
			// Item does NOT exist in cache. Could have been deleted manually by user.
			// Update the cached flag and return. (Returning failure would stop cache manager.)
			return b.markObjectCached(bucket, objectName, 0)
		}
		
		err = os.Remove(src_file)
		if err != nil {
			return siaErrorUnableToClearAnyCachedFiles
		}

		err = b.markObjectCached(bucket, objectName, 0)
		if err != nil {
			return siaErrorUnableToClearAnyCachedFiles
		}
		return siaSuccess
	}

	return siaErrorUnableToClearAnyCachedFiles // Shouldn't happen
}

func (b *SiaCacheLayer) getCacheSize() (int64, SiaServiceError) {
	debugmsg("SiaCacheLayer.getCacheSize")
    var size int64
    err := filepath.Walk(b.CacheDir, func(_ string, info os.FileInfo, err error) error {
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

func (b *SiaCacheLayer) ensureCacheDirExists() {
	debugmsg("SiaCacheLayer.ensureCacheDirExists")
	// Make sure cache directory exists
	os.Mkdir(b.CacheDir, 0744)
}

func (b* SiaCacheLayer) ensureCacheBucketDirExists(bucket string) {
	debugmsg("SiaCacheLayer.ensureCacheBucketDirExists")
	os.Mkdir(filepath.Join(b.CacheDir, bucket), 0744)
}

func debugmsg(str string) {
	if SIA_DEBUG > 0 {
		fmt.Println(str)
	}
}

func (b* SiaCacheLayer) timeTrack(start time.Time, name string) {
	if SIA_DEBUG > 0 {
	    elapsed := time.Since(start)
	    log.Printf("%s took %s\n", name, elapsed)
	}
}