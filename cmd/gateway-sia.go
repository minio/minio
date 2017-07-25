package cmd

import (
	"io"
	"os"
	"fmt"
	"strconv"

	"github.com/minio/minio-go/pkg/policy"
)

// How many seconds to wait to purge object from SiaBridge cache
var SIA_CACHE_PURGE_AFTER_SEC = 24*60*60 // 24 hours
var SIA_DAEMON_ADDR = "127.0.0.1:9980"
var SIA_CACHE_DIR = ".sia_cache"
var SIA_DB_FILE = ".sia.db"
var SIA_DEBUG = 0

type siaObjects struct {
	siacl *SiaCacheLayer
	fs ObjectLayer
}



// newSiaGateway returns Sia gatewaylayer
func newSiaGateway(host string) (GatewayLayer, error) {
	loadSiaEnv()

	// Create the filesystem layer
	f, err := newFSObjectLayer(SIA_CACHE_DIR)
	if err != nil {
		return nil, err
	}
	// Create the Sia cache layer
	s := &SiaCacheLayer{SIA_DAEMON_ADDR, SIA_CACHE_DIR, SIA_DB_FILE}

	// Start the Sia cache layer
	err = s.Start()
	if err != nil {
		panic(err)
	}

	return &siaObjects{
		siacl:	s,
		fs:     f,
	}, nil
}

// Attempt to load Sia config from ENV
func loadSiaEnv() {
	tmp := os.Getenv("SIA_CACHE_DIR")
	if tmp != "" {
		SIA_CACHE_DIR = tmp
	}
	fmt.Printf("SIA_CACHE_DIR: %s\n", SIA_CACHE_DIR)

	tmp = os.Getenv("SIA_CACHE_PURGE_AFTER_SEC")
	if tmp != "" {
		i, err := strconv.Atoi(tmp)
		if err == nil {
			SIA_CACHE_PURGE_AFTER_SEC = i
		}
	}
	fmt.Printf("SIA_CACHE_PURGE_AFTER_SEC: %d\n", SIA_CACHE_PURGE_AFTER_SEC)

	tmp = os.Getenv("SIA_DAEMON_ADDR")
	if tmp != "" {
		SIA_DAEMON_ADDR = tmp
	}
	fmt.Printf("SIA_DAEMON_ADDR: %s\n", SIA_DAEMON_ADDR)

	tmp = os.Getenv("SIA_DB_FILE")
	if tmp != "" {
		SIA_DB_FILE = tmp
	}
	fmt.Printf("SIA_DB_FILE: %s\n", SIA_DB_FILE)

	tmp = os.Getenv("SIA_DEBUG")
	if tmp != "" {
		i, err := strconv.Atoi(tmp)
		if err == nil {
			SIA_DEBUG = i
		}
	}
	fmt.Printf("SIA_DEBUG: %d\n", SIA_DEBUG)
}


// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (s *siaObjects) Shutdown() error {
	debugmsg("Gateway.Shutdown")

	// Stop the Sia caching layer
	s.siacl.Stop()

	return nil
}

// StorageInfo is not relevant to Sia backend.
func (s *siaObjects) StorageInfo() (si StorageInfo) {
	debugmsg("Gateway.StorageInfo")
	return si
}

// MakeBucket creates a new container on Sia backend.
func (s *siaObjects) MakeBucketWithLocation(bucket, location string) error {
	debugmsg(fmt.Sprintf("Gateway.MakeBucketWithLocation(%s, %s)", bucket, location))
	err := s.fs.MakeBucketWithLocation(bucket, location)
	if err != nil {
		return err
	}

	return s.siacl.InsertBucket(bucket)
}

// GetBucketInfo gets bucket metadata.
func (s *siaObjects) GetBucketInfo(bucket string) (bi BucketInfo, e error) {
	debugmsg("Gateway.GetBucketInfo")
	return s.fs.GetBucketInfo(bucket)
}

// ListBuckets lists all Sia buckets
func (s *siaObjects) ListBuckets() (buckets []BucketInfo, e error) {
	debugmsg("Gateway.ListBuckets")
	return s.fs.ListBuckets()
}

// DeleteBucket deletes a bucket on Sia
func (s *siaObjects) DeleteBucket(bucket string) error {
	debugmsg("Gateway.DeleteBucket")
	err := s.siacl.DeleteBucket(bucket)
	if err != nil {
		return err
	}

	return s.fs.DeleteBucket(bucket)
}

func (s *siaObjects) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	debugmsg("Gateway.ListObjects")
	sobjs, err := s.siacl.ListObjects(bucket)
	if err != nil {
		return loi, err
	}

	loi.IsTruncated = false
	loi.NextMarker = ""

	for _, sobj := range sobjs {
		loi.Objects = append(loi.Objects, ObjectInfo{
			Bucket:          bucket,
			Name:            sobj.Name,
			ModTime:         sobj.Queued,
			Size:            int64(sobj.Size),
			//ETag:            azureToS3ETag(object.Properties.Etag),
			//ContentType:     object.Properties.ContentType,
			//ContentEncoding: object.Properties.ContentEncoding,
		})
	}

return loi, nil
}

func (s *siaObjects) ListObjectsV2(bucket, prefix, continuationToken string, fetchOwner bool, delimiter string, maxKeys int) (loi ListObjectsV2Info, e error) {
	debugmsg("Gateway.ListObjectsV2")
	return loi, nil
}

func (s *siaObjects) GetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer) error {
	debugmsg("Gateway.GetObject")
	err := s.siacl.GuaranteeObjectIsInCache(bucket, key)
	if err != nil {
		return err
	}

	return s.fs.GetObject(bucket, key, startOffset, length, writer)
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (s *siaObjects) GetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	debugmsg("Gateway.GetObjectInfo")
	return s.fs.GetObjectInfo(bucket, object)
}

// PutObject creates a new object with the incoming data,
func (s *siaObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, e error) {
	debugmsg("Gateway.PutObject")
	oi, e := s.fs.PutObject(bucket, object, size, data, metadata, sha256sum)
	if e != nil {
		return oi, e
	}

	src_file := pathJoin(abs(SIA_CACHE_DIR), bucket, object)
	e = s.siacl.PutObject(bucket, object, size, int64(SIA_CACHE_PURGE_AFTER_SEC), src_file)
	// If put fails to the cache layer, then delete from object layer
	if e != nil {
		s.fs.DeleteObject(bucket, object)
	}
	return oi, e
}

// CopyObject copies a blob from source container to destination container.
func (s *siaObjects) CopyObject(srcBucket string, srcObject string, destBucket string, destObject string, metadata map[string]string) (objInfo ObjectInfo, e error) {
	debugmsg("Gateway.CopyObject")
	return s.fs.CopyObject(srcBucket, srcObject, destBucket, destObject, metadata)
}

// DeleteObject deletes a blob in bucket
func (s *siaObjects) DeleteObject(bucket string, object string) error {
	debugmsg("Gateway.DeleteObject")
	err := s.siacl.DeleteObject(bucket, object)
	if err != nil {
		return err
	}

	return s.fs.DeleteObject(bucket, object)
}

// ListMultipartUploads lists all multipart uploads.
func (s *siaObjects) ListMultipartUploads(bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
	debugmsg("Gateway.ListMultipartUploads")
	return s.fs.ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// NewMultipartUpload upload object in multiple parts
func (s *siaObjects) NewMultipartUpload(bucket string, object string, metadata map[string]string) (uploadID string, err error) {
	debugmsg("Gateway.NewMultipartUpload")
	return s.fs.NewMultipartUpload(bucket, object, metadata)
}

// CopyObjectPart copy part of object to other bucket and object
func (s *siaObjects) CopyObjectPart(srcBucket string, srcObject string, destBucket string, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error) {
	debugmsg("Gateway.CopyObjectPart")
	return s.fs.CopyObjectPart(srcBucket, srcObject, destBucket, destObject, uploadID, partID, startOffset, length)
}

// PutObjectPart puts a part of object in bucket
func (s *siaObjects) PutObjectPart(bucket string, object string, uploadID string, partID int, size int64, data io.Reader, md5Hex string, sha256sum string) (pi PartInfo, e error) {
	debugmsg("Gateway.PutObjectPart")
	return s.fs.PutObjectPart(bucket, object, uploadID, partID, size, data, md5Hex, sha256sum)
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (s *siaObjects) ListObjectParts(bucket string, object string, uploadID string, partNumberMarker int, maxParts int) (lpi ListPartsInfo, e error) {
	debugmsg("Gateway.ListObjectParts")
	return s.fs.ListObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (s *siaObjects) AbortMultipartUpload(bucket string, object string, uploadID string) error {
	debugmsg("Gateway.AbortMultipartUpload")
	return s.fs.AbortMultipartUpload(bucket, object, uploadID)
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (s *siaObjects) CompleteMultipartUpload(bucket string, object string, uploadID string, uploadedParts []completePart) (oi ObjectInfo, e error) {
	debugmsg("Gateway.CompleteMultipartUpload")
	oi, err := s.fs.CompleteMultipartUpload(bucket, object, uploadID, uploadedParts)
	if err != nil {
		return oi, err
	}

	src_file := pathJoin(abs(SIA_CACHE_DIR), bucket, object)
	fi, err := os.Stat(src_file)
	if err != nil {
		return oi, err
	}

	err = s.siacl.PutObject(bucket, object, fi.Size(), int64(SIA_CACHE_PURGE_AFTER_SEC), src_file)
	// If put fails to the cache layer, then delete from object layer
	if err != nil {
		s.fs.DeleteObject(bucket, object)
	}

	return oi, nil
}

// SetBucketPolicies sets policy on bucket
func (s *siaObjects) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	debugmsg("Gateway.SetBucketPolicies")
	//return s.fs.SetBucketPolicies(bucket, policyInfo)
	return nil
}

// GetBucketPolicies will get policy on bucket
func (s *siaObjects) GetBucketPolicies(bucket string) (policy.BucketAccessPolicy, error) {
	debugmsg("Gateway.GetBucketPolicies")
	//return s.fs.GetBucketPolicies(bucket)
	return policy.BucketAccessPolicy{}, nil
}

// DeleteBucketPolicies deletes all policies on bucket
func (s *siaObjects) DeleteBucketPolicies(bucket string) error {
	debugmsg("Gateway.DeleteBucketPolicies")
	//return s.fs.DeleteBucketPolciies(bucket)
	return nil
}
