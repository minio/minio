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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/minio/minio-go/pkg/policy"
)

type siaObjects struct {
	Fs          *fsObjects // Filesystem layer.
	Daemon      *SiaDaemon // Daemon layer.
	SiadAddress string     // Address and port of Sia Daemon.
	CacheDir    string     // Temporary storage location for file transfers.
	SiaRootDir  string     // Root directory to store files on Sia.
	DebugMode   bool       // Whether or not debug mode is enabled.
}

// Convert Sia errors to minio object layer errors.
func siaToObjectError(err *SiaServiceError, params ...string) (e error) {
	if err == nil {
		return nil
	}

	switch err.Code {
	case "SiaErrorUnknown":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorObjectDoesNotExistInBucket":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorObjectAlreadyExists":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorInvalidObjectName":
		return fmt.Errorf("Sia: %s", err.Message)
	case "SiaErrorDaemon":
		return fmt.Errorf("Sia Daemon: %s", err.Message)
	default:
		return fmt.Errorf("Sia: %s", err.Message)
	}
}

// newSiaGateway returns Sia gatewaylayer
func newSiaGateway(host string) (GatewayLayer, error) {
	sia := &siaObjects{
		SiadAddress: host,
		DebugMode:   false,
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

	fmt.Printf("\nSia Gateway Configuration:\n")
	fmt.Printf("  Debug Mode: %v\n", sia.DebugMode)
	fmt.Printf("  Sia Daemon API Address: %s\n", sia.SiadAddress)
	fmt.Printf("  Sia Root Directory: %s\n", sia.SiaRootDir)
	fmt.Printf("  Sia Cache Directory: %s\n", sia.CacheDir)

	// Create the daemon layer
	d, err := newSiaDaemon(sia.SiadAddress, sia.CacheDir, sia.SiaRootDir, sia.DebugMode)
	if err != nil {
		return nil, err
	}
	sia.Daemon = d

	// Create the filesystem layer
	f, err := newFSObjectLayer(sia.CacheDir)
	if err != nil {
		return nil, err
	}

	fso, ok := f.(*fsObjects)
	if !ok {
		return nil, errors.New("Invalid type")
	}
	sia.Fs = fso

	// Sync Sia buckets with Filesystem buckets
	err = sia.syncBuckets()
	if err != nil {
		return sia, err
	}

	return sia, nil
}

// loadSiaEnv will attempt to load Sia config from ENV
func (s *siaObjects) loadSiaEnv() {
	tmp := os.Getenv("SIA_DAEMON_ADDR")
	if tmp != "" {
		s.SiadAddress = tmp
	}

	tmp = os.Getenv("SIA_CACHE_DIR")
	if tmp != "" {
		s.CacheDir = tmp
	} else {
		s.CacheDir = ".sia_cache"
	}

	tmp = os.Getenv("SIA_ROOT_DIR")
	if tmp != "" {
		s.SiaRootDir = tmp
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

// syncBuckets will attempt to sync Sia buckets with filesystem buckets
func (s *siaObjects) syncBuckets() error {
	s.debugmsg("Gateway.syncBuckets")

	// We use the filesystem layer to store new buckets, since Sia doesn't use "buckets".
	// However, we should check Sia network to detect existing buckets, and add those
	// locally if they don't already exist.

	sia_buckets, siaErr := s.Daemon.ListBuckets()
	if siaErr != nil {
		return siaToObjectError(siaErr)
	}

	fs_buckets, err := s.Fs.ListBuckets()
	if err != nil {
		return err
	}

	for _, siaBkt := range sia_buckets {
		found := false
		for _, fsBkt := range fs_buckets {
			if siaBkt.Name == fsBkt.Name {
				found = true
				continue
			}
		}
		if !found {
			s.debugmsg(fmt.Sprintf("    s.Fs.MakeBucketWithLocation - bucket: %s", siaBkt.Name))
			err = s.Fs.MakeBucketWithLocation(siaBkt.Name, "")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (s *siaObjects) Shutdown() error {
	s.debugmsg("Gateway.Shutdown")
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
	return s.Fs.MakeBucketWithLocation(bucket, location)
}

// GetBucketInfo gets bucket metadata.
func (s *siaObjects) GetBucketInfo(bucket string) (bi BucketInfo, e error) {
	s.debugmsg("Gateway.GetBucketInfo")
	return s.Fs.GetBucketInfo(bucket)
}

// ListBuckets lists all Sia buckets
func (s *siaObjects) ListBuckets() (buckets []BucketInfo, e error) {
	s.debugmsg("Gateway.ListBuckets")

	// Sync'd Sia buckets with filesystem buckets at startup

	fs_buckets, err := s.Fs.ListBuckets()
	if err != nil {
		return buckets, err
	}

	return fs_buckets, nil
}

// DeleteBucket deletes a bucket on Sia
func (s *siaObjects) DeleteBucket(bucket string) error {
	s.debugmsg("Gateway.DeleteBucket")
	return s.Fs.DeleteBucket(bucket)
}

func (s *siaObjects) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	s.debugmsg("Gateway.ListObjects")
	siaObjs, siaErr := s.Daemon.ListRenterFiles(bucket)
	if siaErr != nil {
		return loi, siaToObjectError(siaErr)
	}

	loi.IsTruncated = false
	loi.NextMarker = ""

	var root string
	if s.Daemon.SiaRootDir == "" {
		root = ""
	} else {
		root = s.Daemon.SiaRootDir + "/"
	}

	for _, siaObj := range siaObjs {

		pre := root + bucket + "/"
		name := strings.TrimPrefix(siaObj.SiaPath, pre)

		etag, err := s.Fs.getObjectETag(bucket, name)
		if err != nil {
			return loi, err
		}

		loi.Objects = append(loi.Objects, ObjectInfo{
			Bucket: bucket,
			Name:   name,
			//ModTime: sobj.Queued,
			Size: int64(siaObj.Filesize),
			ETag: etag,
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

	// Only deal with daemon on first chunk
	if startOffset == 0 {
		siaErr := s.Daemon.GuaranteeObjectIsInCache(bucket, key)
		if siaErr != nil {
			return siaToObjectError(siaErr)
		}
	}

	s.debugmsg("    s.Fs.GetObject")
	err := s.Fs.GetObject(bucket, key, startOffset, length, writer)

	// Delete the cached copy when done
	s.debugmsg("    s.Fs.DeleteObject")
	s.Fs.DeleteObject(bucket, key)

	return err
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (s *siaObjects) GetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	s.debugmsg("Gateway.GetObjectInfo")

	// Can't delegate to object layer. File may not be on local filesystem.

	sobjInfo, siaErr := s.Daemon.GetObjectInfo(bucket, object)
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
		FileName:  object,
		FileSize:  int64(sobjInfo.Filesize),
		FileIsDir: false,
	}
	return fsMeta.ToObjectInfo(bucket, object, siaFileInfo), nil
}

// PutObject creates a new object with the incoming data,
func (s *siaObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, e error) {
	s.debugmsg("Gateway.PutObject")

	// Check the object's name first
	if !s.Daemon.ApproveObjectName(object) {
		return objInfo, siaToObjectError(siaErrorInvalidObjectName, bucket, object)
	}

	// Temporarily copy to local filesystem
	s.debugmsg("    s.Fs.PutObject")
	oi, e := s.Fs.PutObject(bucket, object, size, data, metadata, sha256sum)
	if e != nil {
		return objInfo, e
	}

	// Upload to Sia
	srcFile := pathJoin(s.CacheDir, bucket, object)
	srcFile, e = filepath.Abs(srcFile)
	if e != nil {
		return objInfo, e
	}
	siaErr := s.Daemon.PutObject(bucket, object, size, srcFile)

	// Delete the temporary copy
	s.debugmsg("    s.Fs.PutObject")
	s.Fs.DeleteObject(bucket, object)

	if siaErr != nil {
		return objInfo, siaToObjectError(siaErr, bucket, object)
	}
	return oi, e
}

// CopyObject copies a blob from source container to destination container.
func (s *siaObjects) CopyObject(srcBucket string, srcObject string, destBucket string, destObject string, metadata map[string]string) (objInfo ObjectInfo, e error) {
	s.debugmsg("Gateway.CopyObject")
	return objInfo, siaErrorNotImplemented
}

// DeleteObject deletes a blob in bucket
func (s *siaObjects) DeleteObject(bucket string, object string) error {
	s.debugmsg("Gateway.DeleteObject")
	siaErr := s.Daemon.DeleteObject(bucket, object)
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

	siaErr := s.Daemon.PutObject(bucket, object, fi.Size(), srcFile)

	// Delete temporary copy
	s.Fs.DeleteObject(bucket, object)

	if siaErr != nil {
		return oi, siaToObjectError(siaErr)
	}

	return oi, nil
}

// SetBucketPolicies sets policy on bucket
func (s *siaObjects) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	s.debugmsg("Gateway.SetBucketPolicies")

	// Per Harshavardhana on 10/19/2017, if Sia doesn't support policies, gateway shouldn't either.
	return siaToObjectError(siaErrorNotImplemented)
}

// GetBucketPolicies will get policy on bucket
func (s *siaObjects) GetBucketPolicies(bucket string) (bal policy.BucketAccessPolicy, e error) {
	s.debugmsg("Gateway.GetBucketPolicies")

	// Per Harshavardhana on 10/19/2017, if Sia doesn't support policies, gateway shouldn't either.
	return bal, siaToObjectError(siaErrorNotImplemented)
}

// DeleteBucketPolicies deletes all policies on bucket
func (s *siaObjects) DeleteBucketPolicies(bucket string) error {
	s.debugmsg("Gateway.DeleteBucketPolicies")

	// Per Harshavardhana on 10/19/2017, if Sia doesn't support policies, gateway shouldn't either.
	return siaToObjectError(siaErrorNotImplemented)
}

func (s *siaObjects) debugmsg(str string) {
	if s.DebugMode {
		fmt.Println(str)
	}
}
