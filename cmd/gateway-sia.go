package cmd

import (
	"io"
	"fmt"
	"time"

	"github.com/minio/minio-go/pkg/policy"
	"github.com/dvstate/siabridge/bridge"
)

// How many seconds to wait to purge object from SiaBridge cache
const PURGE_AFTER_SEC = 24*60*60 // 24 hours

type siaObjects struct {
	siab *bridge.SiaBridge
}

// newSiaGateway returns Sia gatewaylayer
func newSiaGateway(host string) (GatewayLayer, error) {
	fmt.Printf("newSiaGateway host: %s", host)

	b := &bridge.SiaBridge{"127.0.0.1:9980",
								  ".sia_cache",
	                              "siabridge.db"}

	err := b.Start()
	if err != nil {
		panic(err)
	}

	return &siaObjects{
		siab:	b,
	}, nil
}


// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (s *siaObjects) Shutdown() error {
	s.siab.Stop()

	return nil
}

// StorageInfo is not relevant to Sia backend.
func (s *siaObjects) StorageInfo() (si StorageInfo) {
	return si
}

// MakeBucket creates a new container on Sia backend.
func (s *siaObjects) MakeBucketWithLocation(bucket, location string) error {
	return s.siab.CreateBucket(bucket)
}

// GetBucketInfo gets bucket metadata..
func (s *siaObjects) GetBucketInfo(bucket string) (bi BucketInfo, e error) {
	sbi, err := s.siab.GetBucketInfo(bucket)
	if err != nil {
		return bi, err
	}

	bi.Name = bucket
	bi.Created = sbi.Created
	return bi, nil
}

// ListBuckets lists all Sia buckets
func (s *siaObjects) ListBuckets() (buckets []BucketInfo, e error) {
	sbkts, err := s.siab.ListBuckets()
	if err != nil {
		return buckets, err
	}

	for _, sbkt := range sbkts {
		buckets = append(buckets, BucketInfo{
			Name: sbkt.Name,
			Created: sbkt.Created,
		})
	}

	return buckets, nil
}

// DeleteBucket deletes a bucket on Sia
func (s *siaObjects) DeleteBucket(bucket string) error {
	return s.siab.DeleteBucket(bucket)
}

// ListObjects lists all blobs in Sia bucket filtered by prefix
func (s *siaObjects) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	//fmt.Printf("ListObjects bucket: %s, prefix: %s, marker: %s, delimiter: %s, maxKeys: %d\n", bucket, prefix, marker, delimiter, maxKeys)

	sobjs, err := s.siab.ListObjects(bucket)
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

// ListObjectsV2 lists all blobs in S3 bucket filtered by prefix
func (s *siaObjects) ListObjectsV2(bucket, prefix, continuationToken string, fetchOwner bool, delimiter string, maxKeys int) (loi ListObjectsV2Info, e error) {
	fmt.Println("ListObjectsV2")
	return loi, nil
}

func (s *siaObjects) GetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer) error {
	return s.siab.GetObject(bucket, key, writer)
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (s *siaObjects) GetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	sobjInfo, err := s.siab.GetObjectInfo(bucket, object)
	if err != nil {
		return objInfo, err
	}

	objInfo.Bucket = sobjInfo.Bucket
	objInfo.Name = sobjInfo.Name
	objInfo.Size = sobjInfo.Size
	objInfo.ModTime = sobjInfo.Queued

	return objInfo, nil
}

// PutObject creates a new object with the incoming data,
func (s *siaObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, e error) {
	err := s.siab.PutObjectFromReader(data, bucket, object, size, PURGE_AFTER_SEC)
	if err != nil {
		return objInfo, err
	}

	objInfo.Bucket = bucket
	objInfo.Name = object
	objInfo.Size = size
	objInfo.ModTime = time.Now()
	return objInfo, nil
}

// CopyObject copies a blob from source container to destination container.
func (s *siaObjects) CopyObject(srcBucket string, srcObject string, destBucket string, destObject string, metadata map[string]string) (objInfo ObjectInfo, e error) {
	return objInfo, nil
}

// DeleteObject deletes a blob in bucket
func (s *siaObjects) DeleteObject(bucket string, object string) error {
	return s.siab.DeleteObject(bucket, object)
}

// ListMultipartUploads lists all multipart uploads.
func (s *siaObjects) ListMultipartUploads(bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
	return lmi, nil
}

// NewMultipartUpload upload object in multiple parts
func (s *siaObjects) NewMultipartUpload(bucket string, object string, metadata map[string]string) (uploadID string, err error) {
	return uploadID, nil
}

// CopyObjectPart copy part of object to other bucket and object
func (s *siaObjects) CopyObjectPart(srcBucket string, srcObject string, destBucket string, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error) {
	return info, nil
}

// PutObjectPart puts a part of object in bucket
func (s *siaObjects) PutObjectPart(bucket string, object string, uploadID string, partID int, size int64, data io.Reader, md5Hex string, sha256sum string) (pi PartInfo, e error) {
	return pi, nil
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (s *siaObjects) ListObjectParts(bucket string, object string, uploadID string, partNumberMarker int, maxParts int) (lpi ListPartsInfo, e error) {
	return lpi, nil
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (s *siaObjects) AbortMultipartUpload(bucket string, object string, uploadID string) error {
	return nil
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (s *siaObjects) CompleteMultipartUpload(bucket string, object string, uploadID string, uploadedParts []completePart) (oi ObjectInfo, e error) {
	return oi, nil
}

// SetBucketPolicies sets policy on bucket
func (s *siaObjects) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	return nil
}

// GetBucketPolicies will get policy on bucket
func (s *siaObjects) GetBucketPolicies(bucket string) (policy.BucketAccessPolicy, error) {
	return policy.BucketAccessPolicy{}, nil
}

// DeleteBucketPolicies deletes all policies on bucket
func (s *siaObjects) DeleteBucketPolicies(bucket string) error {
	return nil
}
