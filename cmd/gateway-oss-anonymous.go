package cmd

import (
	"io"

	"github.com/minio/minio/pkg/hash"
)

// AnonPutObject creates a new object anonymously with the incoming data,
func (l *ossObjects) AnonPutObject(bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	return ossPutObject(l.anonClient, bucket, object, data, metadata)
}

// AnonGetObject - Get object anonymously
func (l *ossObjects) AnonGetObject(bucket, key string, startOffset, length int64, writer io.Writer) error {
	return ossGetObject(l.anonClient, bucket, key, startOffset, length, writer)
}

// AnonGetObjectInfo - Get object info anonymously
func (l *ossObjects) AnonGetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	return ossGetObjectInfo(l.anonClient, bucket, object)
}

// AnonListObjects lists objects anonymously.
func (l *ossObjects) AnonListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	return ossListObjects(l.anonClient, bucket, prefix, marker, delimiter, maxKeys)
}

// AnonListObjectsV2 lists objects in V2 mode, anonymously.
func (l *ossObjects) AnonListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (loi ListObjectsV2Info, err error) {
	return ossListObjectsV2(l.anonClient, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
}

// AnonGetBucketInfo gets bucket metadata anonymously.
func (l *ossObjects) AnonGetBucketInfo(bucket string) (bi BucketInfo, err error) {
	return ossGeBucketInfo(l.anonClient, bucket)
}
