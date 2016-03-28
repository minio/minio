package main

import (
	"io"

	"github.com/minio/minio/pkg/probe"
)

// Backend api interface.
type Backend interface {
	// Bucket level API.
	MakeBucket(bucket string) *probe.Error
	ListBuckets() ([]BucketInfo, *probe.Error)
	GetBucketInfo(bucket string) (BucketInfo, *probe.Error)
	DeleteBucket(bucket string) *probe.Error
	ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, *probe.Error)

	// Object level API.
	GetObject(w io.Writer, bucket, object string, start, length int64) (int64, *probe.Error)
	GetObjectInfo(bucket, object string) (ObjectInfo, *probe.Error)
	CreateObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (ObjectInfo, *probe.Error)
	DeleteObject(bucket, object string) *probe.Error

	// Multipart level API.
	NewMultipartUpload(bucket, object string) (string, *probe.Error)
	CreateObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, metadata map[string]string) (string, *probe.Error)
	CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (ObjectInfo, *probe.Error)
	ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, *probe.Error)
	ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, *probe.Error)
	AbortMultipartUpload(bucket, object, uploadID string) *probe.Error
}
