package cmd

import (
	"fmt"
	"time"
	"os"
	"strconv"
	"path/filepath"
	"errors"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/NebulousLabs/Sia/api"
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

type SiaCacheLayer struct {
	SiadAddress string 	// Address of siad daemon API. (e.g., "127.0.0.1:9980")
	CacheDir string 	// Cache directory for downloads
	DbFile string 		// Name and path of Sqlite database file
}


type SiaBucketInfo struct {
	Name string 		// Name of bucket
	Created time.Time   // Time of bucket creation
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
	Message:    "Failed to delete a record in the cache database",
}
var siaErrorDatabaseInsertError = SiaServiceError{
	Code:       "SiaErrorDatabaseInsertError",
	Message:    "Failed to insert a record in the cache database",
}
var siaErrorDatabaseUpdateError = SiaServiceError{
	Code:       "SiaErrorDatabaseUpdateError",
	Message:    "Failed to update a record in the cache database",
}

// Called to start running the SiaBridge
func (b *SiaCacheLayer) Start() error {
	debugmsg("SiaCacheLayer.Start")
	b.loadSiaEnv()

	b.ensureCacheDirExists()

	// Open and initialize database
	err := b.initDatabase() 
	if err != nil {
		return err
	}

	// Start the cache management process
	g_cache_ticker = time.NewTicker(time.Second * time.Duration(SIA_MANAGER_DELAY_SEC))
    go func() {
        for _ = range g_cache_ticker.C {
        	b.manager()
        }
    }()

    return nil
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
	g_db.Close()
}

func (b *SiaCacheLayer) InsertBucket(bucket string) error {
	debugmsg("SiaCacheLayer.InsertBucket")
	stmt, err := g_db.Prepare("INSERT INTO buckets(name, created) values(?,?)")
    if err != nil {
    	return err
    }

    _, err = stmt.Exec(bucket, time.Now().Unix())
    if err != nil {
    	return err
    }

    return nil
}

func (b *SiaCacheLayer) DeleteBucket(bucket string) error {
	debugmsg("SiaCacheLayer.DeleteBucket")
	stmt, err := g_db.Prepare("DELETE FROM buckets WHERE name=?")
    if err != nil {
    	return err
    }
	_, err = stmt.Exec(bucket)
    if err != nil {
    	return err
    }

	return nil
}

// List all buckets
func (b *SiaCacheLayer) ListBuckets() (buckets []SiaBucketInfo, e error) {
	debugmsg("SiaCacheLayer.ListBuckets")
	rows, err := g_db.Query("SELECT * FROM buckets")
    if err != nil {
    	return buckets, err
    }

    var name string
    var created int64

    for rows.Next() {
        err = rows.Scan(&name, &created)
        if err != nil {
        	return buckets, err
        }

        buckets = append(buckets, SiaBucketInfo{
    		Name:		name,
    		Created: 	time.Unix(created, 0),
    	})
    }

    rows.Close()

	return buckets, nil
}

// Deletes the object
func (b *SiaCacheLayer) DeleteObject(bucket string, objectName string) error {
	debugmsg("SiaCacheLayer.DeleteObject")
	err := b.markObjectDeleted(bucket, objectName)
	if err != nil {
		return err
	}

    // Tell Sia daemon to delete the object
	var siaObj = bucket + "/" + objectName
	
	err = post(b.SiadAddress, "/renter/delete/"+siaObj, "")
	if err != nil {
		return err
	}

	return b.deleteObjectFromDb(bucket, objectName)
}

func (b *SiaCacheLayer) PutObject(bucket string, objectName string, size int64, purge_after int64, src_file string) error {
	debugmsg("SiaCacheLayer.PutObject")
	
	// First, make sure space exists in cache
	err := b.guaranteeCacheSpace(size)
	if err != nil {
		return err
	}

	// Before inserting to DB, there is a very rare chance that the object already exists in DB 
	// from a failed upload and Minio crashed or was killed before DB updated to reflect. So just in case 
	// we will check if the object exists and has a not uploaded status. If so, we will delete that 
	// record and then continue as normal.
	objInfo, e := b.GetObjectInfo(bucket, objectName)
	if e == nil {
		// Object does exist. If uploaded, return error. If not uploaded, delete it and continue.
		if objInfo.Uploaded.Unix() > 0 {
			return errors.New("Object already exists")
		} else {
			e = b.deleteObjectFromDb(bucket, objectName)
			if e != nil {
				return e
			}
		}
	}

	err = b.insertObjectToDb(bucket, objectName, size, time.Now().Unix(), 0, purge_after, src_file, 1)
	if err != nil {
		return err
	}

	// Tell Sia daemon to upload the object
	siaObj := bucket + "/" + objectName
	err = post(b.SiadAddress, "/renter/upload/"+siaObj, "source="+src_file)
	if err != nil {
		b.deleteObjectFromDb(bucket, objectName)
		return err
	}

	// Need to wait for upload to complete
	err = b.waitTillSiaUploadCompletes(siaObj)
	if err != nil {
		b.deleteObjectFromDb(bucket, objectName)
		return err
	}

	// Mark object as uploaded
	err = b.markObjectUploaded(bucket, objectName)
	if err != nil {
		b.deleteObjectFromDb(bucket, objectName)
		return err
	}

	return nil
}

// Returns a list of objects in the bucket provided
func (b *SiaCacheLayer) ListObjects(bucket string) (objects []SiaObjectInfo, e error) {
	debugmsg("SiaCacheLayer.listUploadingObjects")
	rows, err := g_db.Query("SELECT name,size,queued,uploaded,purge_after,cached_fetches,sia_fetches,last_fetch,src_file,deleted,cached FROM objects WHERE bucket=?",bucket)
    if err != nil {
    	return objects, err
    }

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
        	return objects, err
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

    rows.Close()

	return objects, nil
}

func (b *SiaCacheLayer) GuaranteeObjectIsInCache(bucket string, objectName string) error {
	defer b.timeTrack(time.Now(), "GuaranteeObjectIsInCache")
	debugmsg("SiaCacheLayer.GuaranteeObjectIsInCache")
	// Make sure object exists in database
	objInfo, err := b.GetObjectInfo(bucket, objectName)
	if err != nil {
		return err
	}

	// Is file already in cache?
	_, err = os.Stat(objInfo.SrcFile);
	if err == nil {
		// File exists in cache
		// Increment cached fetch count
    	return b.updateCachedFetches(bucket, objectName, objInfo.CachedFetches+1)
	}
	// Object not in cache, must download from Sia.
    // First, though, make sure the file was completely uploaded to Sia.
    if objInfo.Uploaded == time.Unix(0,0) {
    	// File never completed uploaded, or was never marked as uploaded in database
    	return errors.New("Attempting to download incomplete file from Sia")
    }

    // Make sure bucket path exists in cache directory
    b.ensureCacheBucketDirExists(bucket)

	// Make sure enough space exists in cache
	err = b.guaranteeCacheSpace(objInfo.Size)
	if err != nil {
		return err
	}

	var siaObj = bucket + "/" + objectName
	err = get(b.SiadAddress, "/renter/download/" + siaObj + "?destination=" + objInfo.SrcFile)
	if err != nil {
		return err
	}

	// Increment cached fetch count
   	return b.updateSiaFetches(bucket, objectName, objInfo.SiaFetches+1)
}


// Returns info for the provided object
func (b *SiaCacheLayer) GetObjectInfo(bucket string, objectName string) (objInfo SiaObjectInfo, e error) {
	debugmsg("SiaCacheLayer.GetObjectInfo")
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
		return objInfo, errors.New("Object does not exist in bucket")
	case err != nil:
		// An error occured
		return objInfo, err
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
		return objInfo, nil 	
	}

	// Shouldn't happen, but just in case
	return objInfo, errors.New("Unknown error in GetObjectInfo()")
}

// Runs periodically to manage the database and cache
func (b *SiaCacheLayer) manager() {
	debugmsg("SiaCacheLayer.manager")
	// Check to see if any files in database have completed uploading to Sia.
	// If so, update uploaded timestamp in database.
	err := b.checkSiaUploads()
	if err != nil {
		fmt.Println("Error in DB/Cache Management Process:")
		fmt.Println(err)
	}

	// Remove files from cache that have not been uploaded or fetched in purge_after seconds.
	err = b.purgeCache()
	if err != nil {
		fmt.Println("Error in DB/Cache Management Process:")
		fmt.Println(err)
	}

	// Check cache disk usage
	err = b.guaranteeCacheSpace(0)
	if err != nil {
		fmt.Println("Error in DB/Cache Management Process:")
		fmt.Println(err)
	}

}

func (b *SiaCacheLayer) purgeCache() error {
	debugmsg("SiaCacheLayer.purgeCache")
	buckets, err := b.ListBuckets()
	if err != nil {
		return err
	}

	for _, bucket := range buckets {
		objects, err := b.ListObjects(bucket.Name)
		if err != nil {
			return err
		}

		for _, object := range objects {
			if object.Uploaded != time.Unix(0,0) {
				since_uploaded := time.Now().Unix() - object.Uploaded.Unix()
				since_fetched := time.Now().Unix() - object.LastFetch.Unix()
				if since_uploaded > object.PurgeAfter && since_fetched > object.PurgeAfter {
					var siaObj = object.Bucket + "/" + object.Name
					var cachedFile = filepath.Join(b.CacheDir,siaObj)
					os.Remove(abs(cachedFile))
				}
			}
		}
	}
	return nil
}

func (b *SiaCacheLayer) checkSiaUploads() error {
	debugmsg("SiaCacheLayer.checkSiaUploads")
	// Get list of all uploading objects
	objs, err := b.listUploadingObjects()
	if err != nil {
		return err
	}

	// Get list of all renter files
	var rf api.RenterFiles
	err = getAPI(b.SiadAddress, "/renter/files", &rf)
	if err != nil {
		return err
	}

	// If uploading object is available on Sia, update database
	for _, obj := range objs {
		var siaObj = obj.Bucket + "/" + obj.Name
		for _, file := range rf.Files {
			if file.SiaPath == siaObj && file.Available {
				err = b.markObjectUploaded(obj.Bucket, obj.Name)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (b *SiaCacheLayer) markObjectUploaded(bucket string, objectName string) error {
	debugmsg("SiaCacheLayer.markObjectUploaded")
	stmt, err := g_db.Prepare("UPDATE objects SET uploaded=? WHERE bucket=? AND name=?")
    if err != nil {
    	return err
    }

    _, err = stmt.Exec(time.Now().Unix(), bucket, objectName)
    if err != nil {
    	return err
    }

    return nil
}

func (b *SiaCacheLayer) markObjectDeleted(bucket string, objectName string) error {
	debugmsg("SiaCacheLayer.markObjectDeleted")
	stmt, err := g_db.Prepare("UPDATE objects SET deleted=1 WHERE bucket=? AND name=?")
    if err != nil {
    	return err
    }

    _, err = stmt.Exec(bucket, objectName)
    if err != nil {
    	return err
    }

    return nil
}

func (b *SiaCacheLayer) markObjectCached(bucket string, objectName string, status int) error {
	debugmsg("SiaCacheLayer.markObjectCached")
	stmt, err := g_db.Prepare("UPDATE objects SET cached=? WHERE bucket=? AND name=?")
    if err != nil {
    	return err
    }

    _, err = stmt.Exec(status, bucket, objectName)
    if err != nil {
    	return err
    }

    return nil
}

func (b *SiaCacheLayer) initDatabase() error {
	debugmsg("SiaCacheLayer.initDatabase")
	// Open the database
	var e error
	g_db, e = sql.Open("sqlite3", b.DbFile)
	if e != nil {
		return e
	}

	// Make sure buckets table exists
	stmt, err := g_db.Prepare("CREATE TABLE IF NOT EXISTS buckets(name TEXT PRIMARY KEY, created INTEGER)")
    if err != nil {
    	return err
    }
	_, err = stmt.Exec()
    if err != nil {
    	return err
    }

	// Make sure objects table exists
    stmt, err = g_db.Prepare("CREATE TABLE IF NOT EXISTS objects(bucket TEXT, name TEXT, size INTEGER, queued INTEGER, uploaded INTEGER, purge_after INTEGER, cached_fetches INTEGER, sia_fetches INTEGER, last_fetch INTEGER, src_file TEXT, deleted INTEGER, cached INTEGER, PRIMARY KEY(bucket,name) )")
    if err != nil {
    	return err
    }
	_, err = stmt.Exec()
    if err != nil {
    	return err
    }

	return nil
}

func (b *SiaCacheLayer) bucketExists(bucket string) (exists bool, e error) {
	debugmsg("SiaCacheLayer.bucketExists")
	// Query the database
	var name string
	err := g_db.QueryRow("SELECT name FROM buckets WHERE name=?", bucket).Scan(&name)
	switch {
	case err == sql.ErrNoRows:
	   return false, nil		// Bucket does not exist
	case err != nil:
	   return false, err 		// An error occured
	default:
		if name == bucket {
			return true, nil 	// Bucket exists
		}
	}

	// Shouldn't happen, but just in case
	return false, errors.New("Unknown error in bucketExists()")	
}

func (b *SiaCacheLayer) objectExists(bucket string, objectName string) (exists bool, e error) {
	debugmsg("SiaCacheLayer.objectExists")
	// Query the database
	var bkt string
	var name string
	err := g_db.QueryRow("SELECT bucket,name FROM objects WHERE bucket=? AND name=?", 
							bucket, objectName).Scan(&bkt,&name)
	switch {
	case err == sql.ErrNoRows:
	   return false, nil		// Bucket does not exist
	case err != nil:
	   return false, err 		// An error occured
	default:
		if bkt == bucket && name == objectName {
			return true, nil 	// Object exists
		}
	}

	// Shouldn't happen, but just in case
	return false, errors.New("Unknown error in objectExists()")
}

func (b *SiaCacheLayer) updateCachedFetches(bucket string, objectName string, fetches int64) error {
	debugmsg("SiaCacheLayer.updateCachedFetches")
	stmt, err := g_db.Prepare("UPDATE objects SET cached_fetches=? WHERE bucket=? AND name=?")
    if err != nil {
    	return err
    }

    _, err = stmt.Exec(fetches, bucket, objectName)
    if err != nil {
    	return err
    }

    return nil
}

func (b *SiaCacheLayer) updateSiaFetches(bucket string, objectName string, fetches int64) error {
	debugmsg("SiaCacheLayer.updateSiaFetches")
	stmt, err := g_db.Prepare("UPDATE objects SET sia_fetches=? WHERE bucket=? AND name=?")
    if err != nil {
    	return err
    }

    _, err = stmt.Exec(fetches, bucket, objectName)
    if err != nil {
    	return err
    }

    return nil
}

func (b *SiaCacheLayer) listUploadingObjects() (objects []SiaObjectInfo, e error) {
	debugmsg("SiaCacheLayer.listUploadingObjects")
	rows, err := g_db.Query("SELECT bucket,name,size,queued,purge_after,cached_fetches,sia_fetches,last_fetch,deleted,cached FROM objects WHERE uploaded=0")
    if err != nil {
    	return objects, err
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
        	return objects, err
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

	return objects, nil
}

func (b *SiaCacheLayer) waitTillSiaUploadCompletes(siaObj string) error {
	debugmsg("SiaCacheLayer.waitTillSiaUploadCompletes")
	complete := false
	for !complete {
		avail, e := b.isSiaFileAvailable(siaObj)
		if e != nil {
			return e
		}

		if avail {
			return nil
		}
		time.Sleep(time.Duration(SIA_UPLOAD_CHECK_FREQ_MS) * time.Millisecond)
	}

	return nil
}

func (b *SiaCacheLayer) isSiaFileAvailable(siaObj string) (bool, error) {
	debugmsg("SiaCacheLayer.isSiaFileAvailable")
	var rf api.RenterFiles
	err := getAPI(b.SiadAddress, "/renter/files", &rf)
	if err != nil {
		return false, err
	}

	for _, file := range rf.Files {
		if file.SiaPath == siaObj {
			return file.Available, nil
		}
	}

	return false, errors.New("File not in Sia renter list")
}

func (b *SiaCacheLayer) insertObjectToDb(bucket string, objectName string, size int64, queued int64, uploaded int64, purge_after int64, src_file string, cached int64) error {
	debugmsg("SiaCacheLayer.insertObjectToDb")
	stmt, err := g_db.Prepare("INSERT INTO objects(bucket, name, size, queued, uploaded, purge_after, cached_fetches, sia_fetches, last_fetch, src_file, deleted, cached) values(?,?,?,?,?,?,?,?,?,?,?,?)")
    if err != nil {
    	return siaErrorDatabaseInsertError
    }

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

    return nil
}

func (b *SiaCacheLayer) deleteObjectFromDb(bucket string, objectName string) error {
	debugmsg("SiaCacheLayer.deleteObjectFromDb")
	stmt, err := g_db.Prepare("DELETE FROM objects WHERE bucket=? AND name=?")
    if err != nil {
    	return siaErrorDatabaseDeleteError
    }
	_, err = stmt.Exec(bucket, objectName)
    if err != nil {
    	return siaErrorDatabaseDeleteError
    }
    return nil
}

func (b *SiaCacheLayer) guaranteeCacheSpace(extraSpace int64) error {
	debugmsg("SiaCacheLayer.guaranteeCacheSpace")
	cs, e := b.getCacheSize()
	if e != nil {
		return e
	}

	space_needed := int64(SIA_CACHE_MAX_SIZE_BYTES) - extraSpace
	for cs > space_needed {
		e = b.forceDeleteOldestCacheFile()
		if e != nil {
			return e
		}

		cs, e = b.getCacheSize()
		if e != nil {
			return e
		}
	}

	return nil
}

func (b *SiaCacheLayer) forceDeleteOldestCacheFile() error {
	debugmsg("SiaCacheLayer.forceDeleteOldestCacheFile")
	var bucket string
	var objectName string
	err := g_db.QueryRow("SELECT bucket,name FROM objects WHERE uploaded>0 AND cached=1 ORDER BY last_fetch DESC LIMIT 1").Scan(&bucket,&objectName)
	switch {
	case err == sql.ErrNoRows:
		return siaErrorUnableToClearAnyCachedFiles;
	case err != nil:
		// An error occured
		return siaErrorUnableToClearAnyCachedFiles
	default:
		// Cached item exists. Delete it.
		siaObj := bucket + "/" + objectName
		var cachedFile = filepath.Join(b.CacheDir,siaObj)
		err = os.Remove(abs(cachedFile))
		if err != nil {
			return siaErrorUnableToClearAnyCachedFiles
		}
		err = b.markObjectCached(bucket, objectName, 0)
		if err != nil {
			return siaErrorUnableToClearAnyCachedFiles
		}
		return nil
	}

	return siaErrorUnableToClearAnyCachedFiles // Shouldn't happen
}

func (b *SiaCacheLayer) getCacheSize() (int64, error) {
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
    return size, nil
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