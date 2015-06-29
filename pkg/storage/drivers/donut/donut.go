/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"io/ioutil"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/storage/donut"
	"github.com/minio/minio/pkg/storage/drivers"
	"github.com/minio/minio/pkg/storage/trove"
	"github.com/minio/minio/pkg/utils/log"
)

type storedBucket struct {
	bucketMetadata   drivers.BucketMetadata
	objectMetadata   map[string]drivers.ObjectMetadata
	partMetadata     map[string]drivers.PartMetadata
	multiPartSession map[string]multiPartSession
}

type multiPartSession struct {
	totalParts int
	uploadID   string
	initiated  time.Time
}

const (
	totalBuckets = 100
)

// donutDriver - creates a new single disk drivers driver using donut
type donutDriver struct {
	donut            donut.Donut
	paths            []string
	lock             *sync.RWMutex
	storedBuckets    map[string]storedBucket
	objects          *trove.Cache
	multiPartObjects *trove.Cache
	maxSize          uint64
	expiration       time.Duration
}

// This is a dummy nodeDiskMap which is going to be deprecated soon
// once the Management API is standardized, and we have way of adding
// and removing disks. This is useful for now to take inputs from CLI
func createNodeDiskMap(paths []string) map[string][]string {
	if len(paths) == 1 {
		nodes := make(map[string][]string)
		nodes["localhost"] = make([]string, 16)
		for i := 0; i < len(nodes["localhost"]); i++ {
			diskPath := filepath.Join(paths[0], strconv.Itoa(i))
			if _, err := os.Stat(diskPath); err != nil {
				if os.IsNotExist(err) {
					os.MkdirAll(diskPath, 0700)
				}
			}
			nodes["localhost"][i] = diskPath
		}
		return nodes
	}
	diskPaths := make([]string, len(paths))
	nodes := make(map[string][]string)
	for i, p := range paths {
		diskPath := filepath.Join(p, strconv.Itoa(i))
		if _, err := os.Stat(diskPath); err != nil {
			if os.IsNotExist(err) {
				os.MkdirAll(diskPath, 0700)
			}
		}
		diskPaths[i] = diskPath
	}
	nodes["localhost"] = diskPaths
	return nodes
}

func initialize(d *donutDriver) error {
	// Soon to be user configurable, when Management API is available
	// we should remove "default" to something which is passed down
	// from configuration paramters
	var err error
	d.donut, err = donut.NewDonut("default", createNodeDiskMap(d.paths))
	if err != nil {
		return iodine.New(err, nil)
	}
	buckets, err := d.donut.ListBuckets()
	if err != nil {
		return iodine.New(err, nil)
	}
	for bucketName, metadata := range buckets {
		d.lock.RLock()
		storedBucket := d.storedBuckets[bucketName]
		d.lock.RUnlock()
		if len(storedBucket.multiPartSession) == 0 {
			storedBucket.multiPartSession = make(map[string]multiPartSession)
		}
		if len(storedBucket.objectMetadata) == 0 {
			storedBucket.objectMetadata = make(map[string]drivers.ObjectMetadata)
		}
		if len(storedBucket.partMetadata) == 0 {
			storedBucket.partMetadata = make(map[string]drivers.PartMetadata)
		}
		storedBucket.bucketMetadata = drivers.BucketMetadata{
			Name:    metadata.Name,
			Created: metadata.Created,
			ACL:     drivers.BucketACL(metadata.ACL),
		}
		d.lock.Lock()
		d.storedBuckets[bucketName] = storedBucket
		d.lock.Unlock()
	}
	return nil
}

// NewDriver instantiate a donut driver
func NewDriver(paths []string, maxSize uint64, expiration time.Duration) (drivers.Driver, error) {
	driver := new(donutDriver)
	driver.storedBuckets = make(map[string]storedBucket)
	driver.objects = trove.NewCache(maxSize, expiration)
	driver.maxSize = maxSize
	driver.expiration = expiration
	driver.multiPartObjects = trove.NewCache(0, time.Duration(0))
	driver.lock = new(sync.RWMutex)

	driver.objects.OnExpired = driver.expiredObject
	driver.multiPartObjects.OnExpired = driver.expiredPart

	// set up memory expiration
	driver.objects.ExpireObjects(time.Second * 5)

	driver.paths = paths
	driver.lock = new(sync.RWMutex)

	err := initialize(driver)
	return driver, err
}

func (d donutDriver) expiredObject(a ...interface{}) {
	cacheStats := d.objects.Stats()
	log.Printf("CurrentSize: %d, CurrentItems: %d, TotalExpirations: %d",
		cacheStats.Bytes, cacheStats.Items, cacheStats.Expired)
	key := a[0].(string)
	// loop through all buckets
	for bucket, storedBucket := range d.storedBuckets {
		delete(storedBucket.objectMetadata, key)
		// remove bucket if no objects found anymore
		if len(storedBucket.objectMetadata) == 0 {
			if time.Since(d.storedBuckets[bucket].bucketMetadata.Created) > d.expiration {
				delete(d.storedBuckets, bucket)
			}
		}
	}
	go debug.FreeOSMemory()
}

// byBucketName is a type for sorting bucket metadata by bucket name
type byBucketName []drivers.BucketMetadata

func (b byBucketName) Len() int           { return len(b) }
func (b byBucketName) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byBucketName) Less(i, j int) bool { return b[i].Name < b[j].Name }

// ListBuckets returns a list of buckets
func (d donutDriver) ListBuckets() (results []drivers.BucketMetadata, err error) {
	if d.donut == nil {
		return nil, iodine.New(drivers.InternalError{}, nil)
	}
	for _, storedBucket := range d.storedBuckets {
		results = append(results, storedBucket.bucketMetadata)
	}
	sort.Sort(byBucketName(results))
	return results, nil
}

// CreateBucket creates a new bucket
func (d donutDriver) CreateBucket(bucketName, acl string) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if len(d.storedBuckets) == totalBuckets {
		return iodine.New(drivers.TooManyBuckets{Bucket: bucketName}, nil)
	}
	if d.donut == nil {
		return iodine.New(drivers.InternalError{}, nil)
	}
	if !drivers.IsValidBucketACL(acl) {
		return iodine.New(drivers.InvalidACL{ACL: acl}, nil)
	}
	if drivers.IsValidBucket(bucketName) {
		if strings.TrimSpace(acl) == "" {
			acl = "private"
		}
		if err := d.donut.MakeBucket(bucketName, acl); err != nil {
			switch iodine.ToError(err).(type) {
			case donut.BucketExists:
				return iodine.New(drivers.BucketExists{Bucket: bucketName}, nil)
			}
			return iodine.New(err, nil)
		}
		var newBucket = storedBucket{}
		newBucket.objectMetadata = make(map[string]drivers.ObjectMetadata)
		newBucket.multiPartSession = make(map[string]multiPartSession)
		newBucket.partMetadata = make(map[string]drivers.PartMetadata)
		metadata, err := d.donut.GetBucketMetadata(bucketName)
		if err != nil {
			return iodine.New(err, nil)
		}
		newBucket.bucketMetadata = drivers.BucketMetadata{
			Name:    metadata.Name,
			Created: metadata.Created,
			ACL:     drivers.BucketACL(metadata.ACL),
		}
		d.storedBuckets[bucketName] = newBucket
		return nil
	}
	return iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, nil)
}

// GetBucketMetadata retrieves an bucket's metadata
func (d donutDriver) GetBucketMetadata(bucketName string) (drivers.BucketMetadata, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if d.donut == nil {
		return drivers.BucketMetadata{}, iodine.New(drivers.InternalError{}, nil)
	}
	if !drivers.IsValidBucket(bucketName) {
		return drivers.BucketMetadata{}, drivers.BucketNameInvalid{Bucket: bucketName}
	}
	if d.storedBuckets[bucketName].bucketMetadata.Name != "" {
		return d.storedBuckets[bucketName].bucketMetadata, nil
	}
	metadata, err := d.donut.GetBucketMetadata(bucketName)
	if err != nil {
		return drivers.BucketMetadata{}, iodine.New(drivers.BucketNotFound{Bucket: bucketName}, nil)
	}
	bucketMetadata := drivers.BucketMetadata{
		Name:    metadata.Name,
		Created: metadata.Created,
		ACL:     drivers.BucketACL(metadata.ACL),
	}
	return bucketMetadata, nil
}

// SetBucketMetadata sets bucket's metadata
func (d donutDriver) SetBucketMetadata(bucketName, acl string) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.donut == nil {
		return iodine.New(drivers.InternalError{}, nil)
	}
	if !drivers.IsValidBucket(bucketName) {
		return iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, nil)
	}
	if strings.TrimSpace(acl) == "" {
		acl = "private"
	}
	bucketMetadata := make(map[string]string)
	bucketMetadata["acl"] = acl
	err := d.donut.SetBucketMetadata(bucketName, bucketMetadata)
	if err != nil {
		return iodine.New(drivers.BucketNotFound{Bucket: bucketName}, nil)
	}
	storedBucket := d.storedBuckets[bucketName]
	storedBucket.bucketMetadata.ACL = drivers.BucketACL(acl)
	d.storedBuckets[bucketName] = storedBucket
	return nil
}

// GetObject retrieves an object and writes it to a writer
func (d donutDriver) GetObject(w io.Writer, bucketName, objectName string) (int64, error) {
	if d.donut == nil {
		return 0, iodine.New(drivers.InternalError{}, nil)
	}
	if !drivers.IsValidBucket(bucketName) {
		return 0, iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, nil)
	}
	if !drivers.IsValidObjectName(objectName) {
		return 0, iodine.New(drivers.ObjectNameInvalid{Object: objectName}, nil)
	}
	if _, ok := d.storedBuckets[bucketName]; ok == false {
		return 0, iodine.New(drivers.BucketNotFound{Bucket: bucketName}, nil)
	}
	d.lock.RLock()
	defer d.lock.RUnlock()
	objectKey := bucketName + "/" + objectName
	data, ok := d.objects.Get(objectKey)
	if !ok {
		reader, size, err := d.donut.GetObject(bucketName, objectName)
		if err != nil {
			switch iodine.ToError(err).(type) {
			case donut.BucketNotFound:
				return 0, iodine.New(drivers.BucketNotFound{Bucket: bucketName}, nil)
			case donut.ObjectNotFound:
				return 0, iodine.New(drivers.ObjectNotFound{
					Bucket: bucketName,
					Object: objectName,
				}, nil)
			default:
				return 0, iodine.New(drivers.InternalError{}, nil)
			}
		}
		pw := newProxyWriter(w)
		n, err := io.CopyN(pw, reader, size)
		if err != nil {
			return 0, iodine.New(err, nil)
		}
		// Save in memory for future reads
		d.objects.Set(objectKey, pw.writtenBytes)
		// free up
		pw.writtenBytes = nil
		go debug.FreeOSMemory()
		return n, nil
	}
	written, err := io.Copy(w, bytes.NewBuffer(data))
	if err != nil {
		return 0, iodine.New(err, nil)
	}
	return written, nil
}

// GetPartialObject retrieves an object range and writes it to a writer
func (d donutDriver) GetPartialObject(w io.Writer, bucketName, objectName string, start, length int64) (int64, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if d.donut == nil {
		return 0, iodine.New(drivers.InternalError{}, nil)
	}
	errParams := map[string]string{
		"bucketName": bucketName,
		"objectName": objectName,
		"start":      strconv.FormatInt(start, 10),
		"length":     strconv.FormatInt(length, 10),
	}
	if !drivers.IsValidBucket(bucketName) {
		return 0, iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, errParams)
	}
	if !drivers.IsValidObjectName(objectName) {
		return 0, iodine.New(drivers.ObjectNameInvalid{Object: objectName}, errParams)
	}
	if start < 0 {
		return 0, iodine.New(drivers.InvalidRange{
			Start:  start,
			Length: length,
		}, errParams)
	}
	if _, ok := d.storedBuckets[bucketName]; ok == false {
		return 0, iodine.New(drivers.BucketNotFound{Bucket: bucketName}, nil)
	}
	objectKey := bucketName + "/" + objectName
	data, ok := d.objects.Get(objectKey)
	if !ok {
		reader, size, err := d.donut.GetObject(bucketName, objectName)
		if err != nil {
			switch iodine.ToError(err).(type) {
			case donut.BucketNotFound:
				return 0, iodine.New(drivers.BucketNotFound{Bucket: bucketName}, nil)
			case donut.ObjectNotFound:
				return 0, iodine.New(drivers.ObjectNotFound{
					Bucket: bucketName,
					Object: objectName,
				}, nil)
			default:
				return 0, iodine.New(drivers.InternalError{}, nil)
			}
		}
		defer reader.Close()
		if start > size || (start+length-1) > size {
			return 0, iodine.New(drivers.InvalidRange{
				Start:  start,
				Length: length,
			}, errParams)
		}
		_, err = io.CopyN(ioutil.Discard, reader, start)
		if err != nil {
			return 0, iodine.New(err, errParams)
		}
		n, err := io.CopyN(w, reader, length)
		if err != nil {
			return 0, iodine.New(err, errParams)
		}
		return n, nil
	}
	written, err := io.CopyN(w, bytes.NewBuffer(data[start:]), length)
	return written, iodine.New(err, nil)
}

// GetObjectMetadata retrieves an object's metadata
func (d donutDriver) GetObjectMetadata(bucketName, objectName string) (drivers.ObjectMetadata, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	errParams := map[string]string{
		"bucketName": bucketName,
		"objectName": objectName,
	}
	if d.donut == nil {
		return drivers.ObjectMetadata{}, iodine.New(drivers.InternalError{}, errParams)
	}
	if !drivers.IsValidBucket(bucketName) {
		return drivers.ObjectMetadata{}, iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, errParams)
	}
	if !drivers.IsValidObjectName(objectName) {
		return drivers.ObjectMetadata{}, iodine.New(drivers.ObjectNameInvalid{Object: objectName}, errParams)
	}
	if _, ok := d.storedBuckets[bucketName]; ok {
		storedBucket := d.storedBuckets[bucketName]
		objectKey := bucketName + "/" + objectName
		if object, ok := storedBucket.objectMetadata[objectKey]; ok {
			return object, nil
		}
	}
	metadata, err := d.donut.GetObjectMetadata(bucketName, objectName)
	if err != nil {
		return drivers.ObjectMetadata{}, iodine.New(drivers.ObjectNotFound{
			Bucket: bucketName,
			Object: objectName,
		}, errParams)
	}
	objectMetadata := drivers.ObjectMetadata{
		Bucket: bucketName,
		Key:    objectName,

		ContentType: metadata.Metadata["contentType"],
		Created:     metadata.Created,
		Md5:         metadata.MD5Sum,
		Size:        metadata.Size,
	}
	return objectMetadata, nil
}

type byObjectName []drivers.ObjectMetadata

func (b byObjectName) Len() int           { return len(b) }
func (b byObjectName) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byObjectName) Less(i, j int) bool { return b[i].Key < b[j].Key }

// ListObjects - returns list of objects
func (d donutDriver) ListObjects(bucketName string, resources drivers.BucketResourcesMetadata) ([]drivers.ObjectMetadata, drivers.BucketResourcesMetadata, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	errParams := map[string]string{
		"bucketName": bucketName,
	}
	if d.donut == nil {
		return nil, drivers.BucketResourcesMetadata{}, iodine.New(drivers.InternalError{}, errParams)
	}
	if !drivers.IsValidBucket(bucketName) {
		return nil, drivers.BucketResourcesMetadata{}, iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, nil)
	}
	if !drivers.IsValidObjectName(resources.Prefix) {
		return nil, drivers.BucketResourcesMetadata{}, iodine.New(drivers.ObjectNameInvalid{Object: resources.Prefix}, nil)
	}
	listObjects, err := d.donut.ListObjects(bucketName, resources.Prefix, resources.Marker, resources.Delimiter, resources.Maxkeys)
	if err != nil {
		return nil, drivers.BucketResourcesMetadata{}, iodine.New(err, errParams)
	}
	resources.CommonPrefixes = listObjects.CommonPrefixes
	resources.IsTruncated = listObjects.IsTruncated
	var results []drivers.ObjectMetadata
	for _, objMetadata := range listObjects.Objects {
		metadata := drivers.ObjectMetadata{
			Key:     objMetadata.Object,
			Created: objMetadata.Created,
			Size:    objMetadata.Size,
		}
		results = append(results, metadata)
	}
	sort.Sort(byObjectName(results))
	if resources.IsTruncated && resources.IsDelimiterSet() {
		resources.NextMarker = results[len(results)-1].Key
	}
	return results, resources, nil
}

type proxyWriter struct {
	writer       io.Writer
	writtenBytes []byte
}

func (r *proxyWriter) Write(p []byte) (n int, err error) {
	n, err = r.writer.Write(p)
	if err != nil {
		return
	}
	r.writtenBytes = append(r.writtenBytes, p[0:n]...)
	return
}

func newProxyWriter(w io.Writer) *proxyWriter {
	return &proxyWriter{writer: w, writtenBytes: nil}
}

// CreateObject creates a new object
func (d donutDriver) CreateObject(bucketName, objectName, contentType, expectedMD5Sum string, size int64, reader io.Reader) (string, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	errParams := map[string]string{
		"bucketName":  bucketName,
		"objectName":  objectName,
		"contentType": contentType,
	}
	if d.donut == nil {
		return "", iodine.New(drivers.InternalError{}, errParams)
	}
	// TODO - Should be able to write bigger than cache
	if size > int64(d.maxSize) {
		generic := drivers.GenericObjectError{Bucket: bucketName, Object: objectName}
		return "", iodine.New(drivers.EntityTooLarge{
			GenericObjectError: generic,
			Size:               strconv.FormatInt(size, 10),
			MaxSize:            strconv.FormatUint(d.maxSize, 10),
		}, nil)
	}
	if !drivers.IsValidBucket(bucketName) {
		return "", iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, nil)
	}
	if !drivers.IsValidObjectName(objectName) {
		return "", iodine.New(drivers.ObjectNameInvalid{Object: objectName}, nil)
	}
	storedBucket := d.storedBuckets[bucketName]
	// get object key
	objectKey := bucketName + "/" + objectName
	if _, ok := storedBucket.objectMetadata[objectKey]; ok == true {
		return "", iodine.New(drivers.ObjectExists{Bucket: bucketName, Object: objectName}, nil)
	}
	if strings.TrimSpace(contentType) == "" {
		contentType = "application/octet-stream"
	}
	metadata := make(map[string]string)
	metadata["contentType"] = strings.TrimSpace(contentType)
	metadata["contentLength"] = strconv.FormatInt(size, 10)
	if strings.TrimSpace(expectedMD5Sum) != "" {
		expectedMD5SumBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
		if err != nil {
			return "", iodine.New(drivers.InvalidDigest{Md5: expectedMD5Sum}, nil)
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}
	objMetadata, err := d.donut.PutObject(bucketName, objectName, expectedMD5Sum, reader, metadata)
	if err != nil {
		switch iodine.ToError(err).(type) {
		case donut.BadDigest:
			return "", iodine.New(drivers.BadDigest{Md5: expectedMD5Sum, Bucket: bucketName, Key: objectName}, nil)
		}
		return "", iodine.New(err, errParams)
	}
	newObject := drivers.ObjectMetadata{
		Bucket: bucketName,
		Key:    objectName,

		ContentType: objMetadata.Metadata["contentType"],
		Created:     objMetadata.Created,
		Md5:         objMetadata.MD5Sum,
		Size:        objMetadata.Size,
	}
	storedBucket.objectMetadata[objectKey] = newObject
	d.storedBuckets[bucketName] = storedBucket
	return newObject.Md5, nil
}
