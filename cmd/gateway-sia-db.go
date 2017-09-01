/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"strconv"
	"time"

	"github.com/boltdb/bolt"
	"github.com/minio/minio-go/pkg/policy"
)

func (cache *SiaCacheLayer) dbLockDB() {
	//cache.debugmsg("SiaCacheLayer.dbLockDB")
	cache.DbMutex.Lock()
}

func (cache *SiaCacheLayer) dbUnlockDB() {
	//cache.debugmsg("SiaCacheLayer.dbUnlockDB")
	cache.DbMutex.Unlock()
}

func (cache *SiaCacheLayer) dbBucketName(bucket string) string {
	return string("bucket_") + bucket
}

// dbInitDatabase will open and prepare the database
func (cache *SiaCacheLayer) dbOpenDatabase() *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.dbOpenDatabase")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	var err error
	cache.Db, err = bolt.Open(cache.DbFile, 0600, nil)
	if err != nil {
		return siaErrorDatabaseCantBeOpened
	}

	return nil
}

// dbCloseDatabase will open and prepare the database
func (cache *SiaCacheLayer) dbCloseDatabase() *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.dbCloseDatabase")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	cache.Db.Close()

	return nil
}

// dbInsertBucket will attempt to insert a new bucket into the database
func (cache *SiaCacheLayer) dbInsertBucket(bucket string) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.dbInsertBucket")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	// Start a transaction
	tx, err := cache.Db.Begin(true)
	if err != nil {
		return siaErrorDatabaseInsertError
	}
	defer tx.Rollback()

	b, err := tx.CreateBucket([]byte(cache.dbBucketName(bucket)))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = b.Put([]byte("name"), []byte(bucket))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = b.Put([]byte("created"), []byte(strconv.FormatInt(time.Now().Unix(), 10)))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	policyInfo := policy.BucketAccessPolicy{}
	res, _ := json.Marshal(&policyInfo)
	if err != nil {
		cache.debugmsg(err.Error())
		return siaErrorDatabaseInsertError
	}
	err = b.Put([]byte("policy"), []byte(string(res)))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return siaErrorDatabaseInsertError
	}

	return nil
}

// dbDeleteBucket will attempt to delete the bucket from the database
func (cache *SiaCacheLayer) dbDeleteBucket(bucket string) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.dbDeleteBucket")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	// Now delete the actual bucket

	// Start a transaction
	tx, err := cache.Db.Begin(true)
	if err != nil {
		return siaErrorDatabaseDeleteError
	}
	defer tx.Rollback()

	err = tx.DeleteBucket([]byte(cache.dbBucketName(bucket)))
	if err != nil {
		return siaErrorDatabaseDeleteError
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return siaErrorDatabaseDeleteError
	}

	return nil
}

// dbListBuckets will attempt to return a list of all existing buckets
func (cache *SiaCacheLayer) dbListBuckets() (buckets []SiaBucketInfo, e *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.dbListBuckets")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	var bucketNames []string
	err := cache.Db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			bucketNames = append(bucketNames, string(name))
			return nil
		})
	})
	if err != nil {
		return buckets, siaErrorDatabaseSelectError
	}

	for _, bucketName := range bucketNames {

		var created int64
		var pol string
		var name string
		err = cache.Db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucketName))
			if b == nil {
				return siaErrorDatabaseSelectError
			}
			vCreated := b.Get([]byte("created"))
			vPolicy := b.Get([]byte("policy"))
			name = string(b.Get([]byte("name")))

			cache.debugmsg(fmt.Sprintf("  %s", name))

			i, err := strconv.ParseInt(string(vCreated), 10, 64)
			if err != nil {
				return siaErrorDatabaseSelectError
			}
			created = i

			pol = string(vPolicy)

			return nil
		})

		if err != nil {
			return buckets, siaErrorDatabaseSelectError
		}

		buckets = append(buckets, SiaBucketInfo{
			Name:    name,
			Created: time.Unix(created, 0),
			Policy:  pol,
		})

	}

	return buckets, nil
}

func (cache *SiaCacheLayer) dbInsertObject(bucket string, objectName string, size int64, queued int64, uploaded int64, purgeAfter int64, srcFile string, cached int64) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.dbInsertObject")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	// Start a transaction
	tx, err := cache.Db.Begin(true)
	if err != nil {
		return siaErrorDatabaseInsertError
	}
	defer tx.Rollback()

	bktBucket := tx.Bucket([]byte(cache.dbBucketName(bucket)))
	if bktBucket == nil {
		return siaErrorDatabaseInsertError
	}

	bktObjects, err := bktBucket.CreateBucketIfNotExists([]byte("objects"))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	bktObj, err := bktObjects.CreateBucketIfNotExists([]byte(objectName))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = bktObj.Put([]byte("name"), []byte(objectName))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = bktObj.Put([]byte("bucket"), []byte(bucket))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = bktObj.Put([]byte("size"), []byte(strconv.FormatInt(size, 10)))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = bktObj.Put([]byte("queued"), []byte(strconv.FormatInt(queued, 10)))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = bktObj.Put([]byte("uploaded"), []byte(strconv.FormatInt(uploaded, 10)))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = bktObj.Put([]byte("purgeAfter"), []byte(strconv.FormatInt(purgeAfter, 10)))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = bktObj.Put([]byte("cachedFetches"), []byte("0"))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = bktObj.Put([]byte("siaFetches"), []byte("0"))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = bktObj.Put([]byte("lastFetch"), []byte("0"))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = bktObj.Put([]byte("srcFile"), []byte(srcFile))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = bktObj.Put([]byte("deleted"), []byte("0"))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	err = bktObj.Put([]byte("cached"), []byte(strconv.FormatInt(cached, 10)))
	if err != nil {
		return siaErrorDatabaseInsertError
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return siaErrorDatabaseInsertError
	}

	return nil
}

func (cache *SiaCacheLayer) dbDeleteObject(bucket string, objectName string) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.dbDeleteObject")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	// Start a transaction
	tx, err := cache.Db.Begin(true)
	if err != nil {
		return siaErrorDatabaseDeleteError
	}
	defer tx.Rollback()

	bktBucket := tx.Bucket([]byte(cache.dbBucketName(bucket)))
	if bktBucket == nil {
		return siaErrorDatabaseDeleteError
	}

	bktObjects := bktBucket.Bucket([]byte("objects"))
	if bktObjects == nil {
		return siaErrorDatabaseDeleteError
	}

	err = bktObjects.DeleteBucket([]byte(objectName))
	if err != nil {
		return siaErrorDatabaseDeleteError
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return siaErrorDatabaseDeleteError
	}

	return nil
}

func (cache *SiaCacheLayer) dbListObjects(bucket string) (objects []SiaObjectInfo, e *SiaServiceError) {
	cache.debugmsg(fmt.Sprintf("SiaCacheLayer.dbListObjects bucket: %s", bucket))

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	err := cache.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(cache.dbBucketName(bucket)))
		if b == nil {
			cache.debugmsg("SiaCacheLayer.dbListObjects1")
			return siaErrorDatabaseSelectError
		}
		bktObjects := b.Bucket([]byte("objects"))
		if bktObjects == nil {
			return nil
		}

		bktObjects.ForEach(func(k []byte, v []byte) error {
			bo := bktObjects.Bucket(k)
			soi, serr := cache.boltPopulateSiaObjectInfo(bo)
			if serr != nil {
				cache.debugmsg("SiaCacheLayer.dbListObjects2")
				return serr
			}
			objects = append(objects, soi)
			return nil
		})
		return nil
	})
	if err != nil {
		cache.debugmsg("SiaCacheLayer.dbListObjects3")
		return objects, siaErrorDatabaseSelectError
	}

	return objects, nil
}

// dbGetObjectInfo will attempt to return object information for the object specified
func (cache *SiaCacheLayer) dbGetObjectInfo(bucket string, objectName string) (objInfo SiaObjectInfo, e *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.dbGetObjectInfo")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	err := cache.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(cache.dbBucketName(bucket)))
		if b == nil {
			return siaErrorDatabaseSelectError
		}
		bktObjects := b.Bucket([]byte("objects"))
		if bktObjects == nil {
			return siaErrorDatabaseSelectError
		}

		bktObj := bktObjects.Bucket([]byte(objectName))
		if bktObj == nil {
			return siaErrorDatabaseSelectError
		}

		soi, serr := cache.boltPopulateSiaObjectInfo(bktObj)
		if serr != nil {
			return serr
		}
		objInfo = soi
		return nil
	})
	if err != nil {
		return objInfo, siaErrorDatabaseSelectError
	}

	return objInfo, nil
}

func (cache *SiaCacheLayer) dbDoesBucketExist(bucket string) (exists bool, e *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.dbDoesBucketExist")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	err := cache.Db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(cache.dbBucketName(bucket)))
		if bkt != nil {
			exists = true
		} else {
			exists = false
		}
		return nil
	})
	if err != nil {
		return false, siaErrorUnknown
	}

	return exists, nil
}

func (cache *SiaCacheLayer) dbDoesObjectExist(bucket string, objectName string) (exists bool, e *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.dbDoesObjectExist")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	err := cache.Db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(cache.dbBucketName(bucket)))
		if bkt == nil {
			exists = false
			return nil
		}

		bktObjects := bkt.Bucket([]byte("objects"))
		if bktObjects == nil {
			exists = false
			return nil
		}

		bktObj := bktObjects.Bucket([]byte(objectName))
		if bktObj == nil {
			exists = false
		} else {
			exists = true
		}
		return nil
	})
	if err != nil {
		return false, siaErrorUnknown
	}

	return exists, nil
}

// dbGetBucketPolicies will attempt to get the policies for the bucket provided
func (cache *SiaCacheLayer) dbGetBucketPolicies(bucket string) (bal policy.BucketAccessPolicy, e *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.dbGetBucketPolicies")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	err := cache.Db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(cache.dbBucketName(bucket)))
		if bkt == nil {
			return siaErrorDatabaseSelectError
		}

		policy := bkt.Get([]byte("policy"))
		json.Unmarshal(policy, &bal)

		return nil
	})
	if err != nil {
		return bal, siaErrorDatabaseSelectError
	}

	return bal, nil
}

// dbUpdateBucketPolicies will attempt to update the policies for the bucket provided
func (cache *SiaCacheLayer) dbUpdateBucketPolicies(bucket string, policy string) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.dbUpdateBucketPolicies")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	// Start a transaction
	tx, err := cache.Db.Begin(true)
	if err != nil {
		return siaErrorDatabaseUpdateError
	}
	defer tx.Rollback()

	bktBucket := tx.Bucket([]byte(cache.dbBucketName(bucket)))
	if bktBucket == nil {
		return siaErrorDatabaseUpdateError
	}

	err = bktBucket.Put([]byte("policy"), []byte(policy))
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return siaErrorDatabaseUpdateError
	}

	return nil
}

func (cache *SiaCacheLayer) dbUpdateObjectUploadedStatus(bucket string, objectName string, uploaded int64) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.dbMarkObjectUploaded")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	// Start a transaction
	tx, err := cache.Db.Begin(true)
	if err != nil {
		return siaErrorDatabaseUpdateError
	}
	defer tx.Rollback()

	bktBucket := tx.Bucket([]byte(cache.dbBucketName(bucket)))
	if bktBucket == nil {
		return siaErrorDatabaseUpdateError
	}

	bktObjects := bktBucket.Bucket([]byte("objects"))
	if bktObjects == nil {
		return siaErrorDatabaseUpdateError
	}

	bktObj := bktObjects.Bucket([]byte(objectName))
	if bktObj == nil {
		return siaErrorDatabaseUpdateError
	}

	err = bktObj.Put([]byte("uploaded"), []byte(strconv.FormatInt(uploaded, 10)))
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return siaErrorDatabaseUpdateError
	}

	return nil
}

func (cache *SiaCacheLayer) dbUpdateObjectDeletedStatus(bucket string, objectName string, deleted int64) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.dbMarkObjectDeleted")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	// Start a transaction
	tx, err := cache.Db.Begin(true)
	if err != nil {
		return siaErrorDatabaseUpdateError
	}
	defer tx.Rollback()

	bktBucket := tx.Bucket([]byte(cache.dbBucketName(bucket)))
	if bktBucket == nil {
		return siaErrorDatabaseUpdateError
	}

	bktObjects := bktBucket.Bucket([]byte("objects"))
	if bktObjects == nil {
		return siaErrorDatabaseUpdateError
	}

	bktObj := bktObjects.Bucket([]byte(objectName))
	if bktObj == nil {
		return siaErrorDatabaseUpdateError
	}

	err = bktObj.Put([]byte("deleted"), []byte(strconv.FormatInt(deleted, 10)))
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return siaErrorDatabaseUpdateError
	}

	return nil
}

// dbUpdateCachedFetches will attempt to update last_fetch time and set the cached_fetches quantity
func (cache *SiaCacheLayer) dbUpdateCachedFetches(bucket string, objectName string, fetches int64) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.dbUpdateCachedFetches")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	// Start a transaction
	tx, err := cache.Db.Begin(true)
	if err != nil {
		return siaErrorDatabaseUpdateError
	}
	defer tx.Rollback()

	bktBucket := tx.Bucket([]byte(cache.dbBucketName(bucket)))
	if bktBucket == nil {
		return siaErrorDatabaseUpdateError
	}

	bktObjects := bktBucket.Bucket([]byte("objects"))
	if bktObjects == nil {
		return siaErrorDatabaseUpdateError
	}

	bktObj := bktObjects.Bucket([]byte(objectName))
	if bktObj == nil {
		return siaErrorDatabaseUpdateError
	}

	err = bktObj.Put([]byte("lastFetch"), []byte(strconv.FormatInt(time.Now().Unix(), 10)))
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	err = bktObj.Put([]byte("cachedFetches"), []byte(strconv.FormatInt(fetches, 10)))
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return siaErrorDatabaseUpdateError
	}

	return nil
}

// dbUpdateCachedStatus will attempt to update the cached status of the object
func (cache *SiaCacheLayer) dbUpdateCachedStatus(bucket string, objectName string, status int64) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.dbUpdateCachedStatus")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	// Start a transaction
	tx, err := cache.Db.Begin(true)
	if err != nil {
		return siaErrorDatabaseUpdateError
	}
	defer tx.Rollback()

	bktBucket := tx.Bucket([]byte(cache.dbBucketName(bucket)))
	if bktBucket == nil {
		return siaErrorDatabaseUpdateError
	}

	bktObjects := bktBucket.Bucket([]byte("objects"))
	if bktObjects == nil {
		return siaErrorDatabaseUpdateError
	}

	bktObj := bktObjects.Bucket([]byte(objectName))
	if bktObj == nil {
		return siaErrorDatabaseUpdateError
	}

	err = bktObj.Put([]byte("cached"), []byte(strconv.FormatInt(status, 10)))
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return siaErrorDatabaseUpdateError
	}

	return nil
}

// dbUpdateSiaFetches will attempt to update the number of sia fetches for the object
func (cache *SiaCacheLayer) dbUpdateSiaFetches(bucket string, objectName string, fetches int64) *SiaServiceError {
	cache.debugmsg("SiaCacheLayer.dbUpdateSiaFetches")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	// Start a transaction
	tx, err := cache.Db.Begin(true)
	if err != nil {
		return siaErrorDatabaseUpdateError
	}
	defer tx.Rollback()

	bktBucket := tx.Bucket([]byte(cache.dbBucketName(bucket)))
	if bktBucket == nil {
		return siaErrorDatabaseUpdateError
	}

	bktObjects := bktBucket.Bucket([]byte("objects"))
	if bktObjects == nil {
		return siaErrorDatabaseUpdateError
	}

	bktObj := bktObjects.Bucket([]byte(objectName))
	if bktObj == nil {
		return siaErrorDatabaseUpdateError
	}

	err = bktObj.Put([]byte("lastFetch"), []byte(strconv.FormatInt(time.Now().Unix(), 10)))
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	err = bktObj.Put([]byte("siaFetches"), []byte(strconv.FormatInt(fetches, 10)))
	if err != nil {
		return siaErrorDatabaseUpdateError
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return siaErrorDatabaseUpdateError
	}

	return nil
}

// dbListUploadingObjects will attempt to list all objects being uploaded to Sia
func (cache *SiaCacheLayer) dbListUploadingObjects() (objects []SiaObjectInfo, e *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.dbListUploadingObjects")

	cache.dbLockDB()
	defer cache.dbUnlockDB()

	err := cache.Db.View(func(tx *bolt.Tx) error {
		tx.ForEach(func(bucket []byte, b *bolt.Bucket) error {
			bktObjects := b.Bucket([]byte("objects"))
			if bktObjects == nil {
				return siaErrorDatabaseSelectError
			}
			bktObjects.ForEach(func(k []byte, v []byte) error {
				bo := bktObjects.Bucket(k)
				vUp := bo.Get([]byte("uploaded"))
				if string(vUp) == "0" {
					soi, serr := cache.boltPopulateSiaObjectInfo(bo)
					if serr != nil {
						return serr
					}
					objects = append(objects, soi)
				}
				return nil
			})
			return nil
		})
		return nil
	})
	if err != nil {
		return objects, siaErrorDatabaseSelectError
	}

	return objects, nil
}

// boltPopulateSiaObjectInfo is a BoltDB helper function to populate a SiaObjectInfo structure from a given BoltDB bucket
func (cache *SiaCacheLayer) boltPopulateSiaObjectInfo(bo *bolt.Bucket) (soi SiaObjectInfo, e *SiaServiceError) {
	cache.debugmsg("SiaCacheLayer.boltPopulateSiaObjectInfo")

	soi.Bucket = string(bo.Get([]byte("bucket")))
	soi.Name = string(bo.Get([]byte("name")))
	soi.SrcFile = string(bo.Get([]byte("srcFile")))

	sz, err := strconv.ParseInt(string(bo.Get([]byte("size"))), 10, 64)
	if err != nil {
		return soi, siaErrorDatabaseSelectError
	}
	soi.Size = sz

	queued, err := strconv.ParseInt(string(bo.Get([]byte("queued"))), 10, 64)
	if err != nil {
		return soi, siaErrorDatabaseSelectError
	}
	soi.Queued = time.Unix(queued, 0)

	uploaded, err := strconv.ParseInt(string(bo.Get([]byte("uploaded"))), 10, 64)
	if err != nil {
		return soi, siaErrorDatabaseSelectError
	}
	soi.Uploaded = time.Unix(uploaded, 0)

	lastFetch, err := strconv.ParseInt(string(bo.Get([]byte("lastFetch"))), 10, 64)
	if err != nil {
		return soi, siaErrorDatabaseSelectError
	}
	soi.LastFetch = time.Unix(lastFetch, 0)

	purgeAfter, err := strconv.ParseInt(string(bo.Get([]byte("purgeAfter"))), 10, 64)
	if err != nil {
		return soi, siaErrorDatabaseSelectError
	}
	soi.PurgeAfter = purgeAfter

	cachedFetches, err := strconv.ParseInt(string(bo.Get([]byte("cachedFetches"))), 10, 64)
	if err != nil {
		return soi, siaErrorDatabaseSelectError
	}
	soi.CachedFetches = cachedFetches

	siaFetches, err := strconv.ParseInt(string(bo.Get([]byte("siaFetches"))), 10, 64)
	if err != nil {
		return soi, siaErrorDatabaseSelectError
	}
	soi.SiaFetches = siaFetches

	deleted, err := strconv.ParseInt(string(bo.Get([]byte("deleted"))), 10, 64)
	if err != nil {
		return soi, siaErrorDatabaseSelectError
	}
	soi.Deleted = deleted

	cached, err := strconv.ParseInt(string(bo.Get([]byte("cached"))), 10, 64)
	if err != nil {
		return soi, siaErrorDatabaseSelectError
	}
	soi.Cached = cached

	return soi, nil
}
