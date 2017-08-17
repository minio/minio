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
	"errors"
	"fmt"
	"github.com/minio/minio-go/pkg/policy"
	"path/filepath"
	"io"
	"os"
	"strconv"
)

type siaObjects struct {
	Cache              *SiaCacheLayer // Cache layer.
	Fs                 *fsObjects     // Filesystem layer.
	PurgeCacheAfterSec int64          // How many seconds to wait to purge object from SiaBridge cache.
	SiadAddress        string         // Address and port of Sia Daemon.
	CacheDir           string         // Name of cache directory.
	DbFile             string         // Name of cache database file.
	DebugMode          bool           // Whether or not debug mode is enabled.
}

// Convert Sia errors to minio object layer errors.
func siaToObjectError(err *SiaServiceError, params ...string) (e error) {
	if err == nil {
		return nil
	}

	switch err.Code {
	case "SiaErrorDaemon":
		return fmt.Errorf("Sia Daemon: %s", err.Message)
	case "SiaErrorUnableToClearAnyCachedFiles":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorDeterminingCacheSize":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorDatabaseDeleteError":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorDatabaseCreateError":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorDatabaseCantBeOpened":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorDatabaseInsertError":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorDatabaseUpdateError":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorDatabaseSelectError":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaFailedToDeleteCachedFile":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorObjectDoesNotExistInBucket":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorObjectAlreadyExists":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorUnknown":
		return fmt.Errorf("Sia: %s", err.Message)
	default:
		return fmt.Errorf("Sia: %s", err.Message)
	}
}

// newSiaGateway returns Sia gatewaylayer
func newSiaGateway(host string) (GatewayLayer, error) {
	sia := &siaObjects{
		PurgeCacheAfterSec: 24 * 60 * 60,
		SiadAddress:        host,
		CacheDir:           ".sia_cache",
		DbFile:             ".sia.db",
		DebugMode:          false,
	}

	sia.loadSiaEnv()

	// If host is specified on command line, override the ENV
	if host != "" {
		sia.SiadAddress = host
	}

	// If SiadAddress not provided on command line or ENV, default to:
    if sia.SiadAddress == "" {
    	sia.SiadAddress = "127.0.0.1:9980"
    }

	// Create the filesystem layer
	f, err := newFSObjectLayer(sia.CacheDir)
	if err != nil {
		return nil, err
	}
	// Create the Sia cache layer
	sia.Cache, err = newSiaCacheLayer(sia.SiadAddress, sia.CacheDir, sia.DbFile, sia.DebugMode)
	if err != nil {
		return nil, err
	}

	fmt.Printf("\nSia Gateway Configuration:\n")
	fmt.Printf("  Debug Mode: %v\n", sia.DebugMode)
	fmt.Printf("  Sia Daemon API Address: %s\n", sia.SiadAddress)
	fmt.Printf("  Cache Directory: %s\n", sia.CacheDir)
	fmt.Printf("  Purge Cache After: %ds\n", sia.PurgeCacheAfterSec)
	fmt.Printf("  Cache Database File: %s\n", sia.DbFile)
	fmt.Printf("  Cache Manager Delay: %ds\n", sia.Cache.ManagerDelaySec)
	fmt.Printf("  Cache Max Size: %d bytes\n", sia.Cache.MaxCacheSizeBytes)
	fmt.Printf("  Upload Check Frequency: %dms\n", sia.Cache.UploadCheckFreqMs)
	fmt.Printf("  Background Uploading: %v\n\n", sia.Cache.BackgroundUpload)

	// Start the Sia cache layer
	siaErr := sia.Cache.Start()
	if siaErr != nil {
		panic(siaErr)
	}

	fso, ok := f.(*fsObjects)
	if !ok {
		return nil, errors.New("Invalid type")
	}
	sia.Fs = fso

	return sia, nil
}

// Attempt to load Sia config from ENV
func (s *siaObjects) loadSiaEnv() {
	tmp := os.Getenv("SIA_CACHE_DIR")
	if tmp != "" {
		s.CacheDir = tmp
	}

	tmp = os.Getenv("SIA_CACHE_PURGE_AFTER_SEC")
	if tmp != "" {
		i, err := strconv.ParseInt(tmp, 10, 64)
		if err == nil {
			s.PurgeCacheAfterSec = i
		}
	}

	tmp = os.Getenv("SIA_DAEMON_ADDR")
	if tmp != "" {
		s.SiadAddress = tmp
	}

	tmp = os.Getenv("SIA_DB_FILE")
	if tmp != "" {
		s.DbFile = tmp
	}

	tmp = os.Getenv("SIA_DEBUG")
	if tmp != "" {
		i, err := strconv.ParseInt(tmp, 10, 64)
		if err == nil {
			if i == 0 {
				s.DebugMode = false
			} else {
				s.DebugMode = true
			}
		}
	}
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (s *siaObjects) Shutdown() error {
	s.debugmsg("Gateway.Shutdown")

	// Stop the Sia caching layer
	s.Cache.Stop()

	return nil
}

// StorageInfo is not relevant to Sia backend.
func (s *siaObjects) StorageInfo() (si StorageInfo) {
	s.debugmsg("Gateway.StorageInfo")
	return si
}

// MakeBucket creates a new container on Sia backend.
func (s *siaObjects) MakeBucketWithLocation(bucket, location string) error {
	s.debugmsg(fmt.Sprintf("Gateway.MakeBucketWithLocation(%s, %s)", bucket, location))
	err := s.Fs.MakeBucketWithLocation(bucket, location)
	if err != nil {
		return err
	}

	siaErr := s.Cache.InsertBucket(bucket)
	return siaToObjectError(siaErr)
}

// GetBucketInfo gets bucket metadata.
func (s *siaObjects) GetBucketInfo(bucket string) (bi BucketInfo, e error) {
	s.debugmsg("Gateway.GetBucketInfo")
	return s.Fs.GetBucketInfo(bucket)
}

// ListBuckets lists all Sia buckets
func (s *siaObjects) ListBuckets() (buckets []BucketInfo, e error) {
	s.debugmsg("Gateway.ListBuckets")
	return s.Fs.ListBuckets()
}

// DeleteBucket deletes a bucket on Sia
func (s *siaObjects) DeleteBucket(bucket string) error {
	s.debugmsg("Gateway.DeleteBucket")
	siaErr := s.Cache.DeleteBucket(bucket)
	if siaErr != nil {
		return siaToObjectError(siaErr)
	}

	return s.Fs.DeleteBucket(bucket)
}

func (s *siaObjects) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	s.debugmsg("Gateway.ListObjects")
	sobjs, siaErr := s.Cache.ListObjects(bucket)
	if siaErr != nil {
		return loi, siaToObjectError(siaErr)
	}

	loi.IsTruncated = false
	loi.NextMarker = ""

	for _, sobj := range sobjs {
		etag, err := s.Fs.getObjectETag(bucket, sobj.Name)
		if err != nil {
			return loi, err
		}

		loi.Objects = append(loi.Objects, ObjectInfo{
			Bucket:  bucket,
			Name:    sobj.Name,
			ModTime: sobj.Queued,
			Size:    int64(sobj.Size),
			ETag:    etag,
			//ContentType:     object.Properties.ContentType,
			//ContentEncoding: object.Properties.ContentEncoding,
		})
	}

	return loi, nil
}

func (s *siaObjects) ListObjectsV2(bucket, prefix, continuationToken string, fetchOwner bool, delimiter string, maxKeys int) (loi ListObjectsV2Info, e error) {
	s.debugmsg("Gateway.ListObjectsV2")
	return loi, nil
}

func (s *siaObjects) GetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer) error {
	s.debugmsg(fmt.Sprintf("Gateway.GetObject %s %s startOffset: %d, length: %d", bucket, key, startOffset, length))

	// Only deal with cache on first chunk
	if startOffset == 0 {
		siaErr := s.Cache.GuaranteeObjectIsInCache(bucket, key)
		if siaErr != nil {
			return siaToObjectError(siaErr)
		}
	}

	return s.Fs.GetObject(bucket, key, startOffset, length, writer)
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (s *siaObjects) GetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	s.debugmsg("Gateway.GetObjectInfo")

	// Can't delegate to object layer. File may not be on local filesystem.
	// Pull the info from cache database.
	// Use size from cache database instead of checking filesystem.

	sobjInfo, siaErr := s.Cache.GetObjectInfo(bucket, object)
	if siaErr != nil {
		return objInfo, siaToObjectError(siaErr)
	}

	fsMeta := fsMetaV1{}
	fsMetaPath := pathJoin(s.Fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fsMetaJSONFile)

	// Read `fs.json` to perhaps contend with
	// parallel Put() operations.
	rlk, err := s.Fs.rwPool.Open(fsMetaPath)
	if err == nil {
		// Read from fs metadata only if it exists.
		defer s.Fs.rwPool.Close(fsMetaPath)
		if _, rerr := fsMeta.ReadFrom(rlk.LockedFile); rerr != nil {
			// `fs.json` can be empty due to previously failed
			// PutObject() transaction, if we arrive at such
			// a situation we just ignore and continue.
			if errorCause(rerr) != io.EOF {
				return objInfo, toObjectErr(rerr, bucket, object)
			}
		}
	}

	// Ignore if `fs.json` is not available, this is true for pre-existing data.
	if err != nil && err != errFileNotFound {
		return objInfo, toObjectErr(traceError(err), bucket, object)
	}

	// SiaFileInfo object is passed in, which implements os.FileInfo interface.
	// This allows the following function to read file size from it without checking file.
	siaFileInfo := SiaFileInfo{
		FileName:    sobjInfo.Name,
		FileSize:    sobjInfo.Size,
		FileModTime: sobjInfo.Queued,
		FileIsDir:   false,
	}
	return fsMeta.ToObjectInfo(bucket, object, siaFileInfo), nil

	/*

		etag, err := s.Fs.getObjectETag(bucket, sobj.Name)
		if err != nil {
			return err
		}

		objInfo = ObjectInfo{
			Bucket:      bucket,
			//UserDefined: meta,
			ETag:        etag,
			ModTime:     sobjInfo.Queued,
			Name:        object,
			Size:        int64(sobjInfo.Size),
		}

		return objInfo, nil*/
}

// PutObject creates a new object with the incoming data,
func (s *siaObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, e error) {
	s.debugmsg("Gateway.PutObject")

	// First, make sure space exists in cache
	serr := s.Cache.guaranteeCacheSpace(size)
	if serr != nil {
		return objInfo, siaToObjectError(serr)
	}

	oi, e := s.Fs.PutObject(bucket, object, size, data, metadata, sha256sum)
	if e != nil {
		return objInfo, e
	}

	absCacheDir, e := filepath.Abs(s.CacheDir)
	if e != nil {
		return objInfo, e
	}

	srcFile := pathJoin(absCacheDir, bucket, object)
	siaErr := s.Cache.PutObject(bucket, object, size, int64(s.PurgeCacheAfterSec), srcFile)
	// If put fails to the cache layer, then delete from object layer
	if siaErr != nil {
		s.Fs.DeleteObject(bucket, object)
		return objInfo, siaToObjectError(siaErr, bucket, object)
	}
	return oi, e
}

// CopyObject copies a blob from source container to destination container.
func (s *siaObjects) CopyObject(srcBucket string, srcObject string, destBucket string, destObject string, metadata map[string]string) (objInfo ObjectInfo, e error) {
	s.debugmsg("Gateway.CopyObject")
	return s.Fs.CopyObject(srcBucket, srcObject, destBucket, destObject, metadata)
}

// DeleteObject deletes a blob in bucket
func (s *siaObjects) DeleteObject(bucket string, object string) error {
	s.debugmsg("Gateway.DeleteObject")
	siaErr := s.Cache.DeleteObject(bucket, object)
	if siaErr != nil {
		return siaToObjectError(siaErr)
	}

	return s.Fs.DeleteObject(bucket, object)
}

// ListMultipartUploads lists all multipart uploads.
func (s *siaObjects) ListMultipartUploads(bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
	s.debugmsg("Gateway.ListMultipartUploads")
	return s.Fs.ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// NewMultipartUpload upload object in multiple parts
func (s *siaObjects) NewMultipartUpload(bucket string, object string, metadata map[string]string) (uploadID string, err error) {
	s.debugmsg("Gateway.NewMultipartUpload")
	return s.Fs.NewMultipartUpload(bucket, object, metadata)
}

// CopyObjectPart copy part of object to other bucket and object
func (s *siaObjects) CopyObjectPart(srcBucket string, srcObject string, destBucket string, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error) {
	s.debugmsg("Gateway.CopyObjectPart")
	return s.Fs.CopyObjectPart(srcBucket, srcObject, destBucket, destObject, uploadID, partID, startOffset, length)
}

// PutObjectPart puts a part of object in bucket
func (s *siaObjects) PutObjectPart(bucket string, object string, uploadID string, partID int, size int64, data io.Reader, md5Hex string, sha256sum string) (pi PartInfo, e error) {
	s.debugmsg("Gateway.PutObjectPart")
	return s.Fs.PutObjectPart(bucket, object, uploadID, partID, size, data, md5Hex, sha256sum)
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (s *siaObjects) ListObjectParts(bucket string, object string, uploadID string, partNumberMarker int, maxParts int) (lpi ListPartsInfo, e error) {
	s.debugmsg("Gateway.ListObjectParts")
	return s.Fs.ListObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (s *siaObjects) AbortMultipartUpload(bucket string, object string, uploadID string) error {
	s.debugmsg("Gateway.AbortMultipartUpload")
	return s.Fs.AbortMultipartUpload(bucket, object, uploadID)
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (s *siaObjects) CompleteMultipartUpload(bucket string, object string, uploadID string, uploadedParts []completePart) (oi ObjectInfo, e error) {
	s.debugmsg("Gateway.CompleteMultipartUpload")
	oi, err := s.Fs.CompleteMultipartUpload(bucket, object, uploadID, uploadedParts)
	if err != nil {
		return oi, err
	}

	absCacheDir, err := filepath.Abs(s.CacheDir)
	if err != nil {
		return oi, err
	}

	srcFile := pathJoin(absCacheDir, bucket, object)
	fi, err := os.Stat(srcFile)
	if err != nil {
		return oi, err
	}

	siaErr := s.Cache.PutObject(bucket, object, fi.Size(), int64(s.PurgeCacheAfterSec), srcFile)
	// If put fails to the cache layer, then delete from object layer
	if siaErr != nil {
		s.Fs.DeleteObject(bucket, object)
		return oi, siaToObjectError(siaErr)
	}

	return oi, nil
}

// SetBucketPolicies sets policy on bucket
func (s *siaObjects) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	s.debugmsg("Gateway.SetBucketPolicies")

	return siaToObjectError(s.Cache.SetBucketPolicies(bucket, policyInfo))
}

// GetBucketPolicies will get policy on bucket
func (s *siaObjects) GetBucketPolicies(bucket string) (bal policy.BucketAccessPolicy, e error) {
	s.debugmsg("Gateway.GetBucketPolicies")

	bp, siaErr := s.Cache.GetBucketPolicies(bucket)
	return bp, siaToObjectError(siaErr)
}

// DeleteBucketPolicies deletes all policies on bucket
func (s *siaObjects) DeleteBucketPolicies(bucket string) error {
	s.debugmsg("Gateway.DeleteBucketPolicies")
	return siaToObjectError(s.Cache.DeleteBucketPolicies(bucket))
}

func (s *siaObjects) debugmsg(str string) {
	if s.DebugMode {
		fmt.Println(str)
	}
}
