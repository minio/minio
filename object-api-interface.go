package main

import (
	"io"

	"github.com/minio/minio/pkg/probe"
)

// ObjectAPI interface.
type ObjectAPI interface {
	// Bucket resource API.
	DeleteBucket(bucket string) *probe.Error
	ListBuckets() ([]BucketInfo, *probe.Error)
	MakeBucket(bucket string) *probe.Error
	GetBucketInfo(bucket string) (BucketInfo, *probe.Error)

	// Bucket query API.
	ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, *probe.Error)
	ListMultipartUploads(bucket, objectPrefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, *probe.Error)

	// Object resource API.
	GetObject(bucket, object string, startOffset int64) (io.ReadCloser, *probe.Error)
	GetObjectInfo(bucket, object string) (ObjectInfo, *probe.Error)
	PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (ObjectInfo, *probe.Error)
	DeleteObject(bucket, object string) *probe.Error

	// Object query API.
	NewMultipartUpload(bucket, object string) (string, *probe.Error)
	PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, *probe.Error)
	ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, *probe.Error)
	CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (ObjectInfo, *probe.Error)
	AbortMultipartUpload(bucket, object, uploadID string) *probe.Error
}
