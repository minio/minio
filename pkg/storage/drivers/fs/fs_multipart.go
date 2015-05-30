package filesystem

import (
	"errors"
	"io"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/storage/drivers"
)

func (fs *fsDriver) ListMultipartUploads(bucket string, resources drivers.BucketMultipartResourcesMetadata) (drivers.BucketMultipartResourcesMetadata, error) {
	return drivers.BucketMultipartResourcesMetadata{}, iodine.New(errors.New("Not Implemented"), nil)
}

func (fs *fsDriver) NewMultipartUpload(bucket, key, contentType string) (string, error) {
	return "", iodine.New(errors.New("Not Implemented"), nil)
}

func (fs *fsDriver) CreateObjectPart(bucket, key, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	return "", iodine.New(errors.New("Not Implemented"), nil)
}

func (fs *fsDriver) CompleteMultipartUpload(bucket, key, uploadID string, parts map[int]string) (string, error) {
	return "", iodine.New(errors.New("Not Implemented"), nil)
}

func (fs *fsDriver) ListObjectParts(bucket, key string, resources drivers.ObjectResourcesMetadata) (drivers.ObjectResourcesMetadata, error) {
	return drivers.ObjectResourcesMetadata{}, iodine.New(errors.New("Not Implemented"), nil)
}

func (fs *fsDriver) AbortMultipartUpload(bucket, key, uploadID string) error {
	return iodine.New(errors.New("Not Implemented"), nil)
}
