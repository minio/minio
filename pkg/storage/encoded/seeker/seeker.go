package seeker

import (
	"io"

	"github.com/minio-io/minio/pkg/encoding/erasure"
	"github.com/minio-io/minio/pkg/storage"
)

type Seeker interface {
	ListBuckets() ([]storage.BucketMetadata, error)

	GetReader(bucket, object string, chunk uint, part uint8) (io.Reader, error)
	// TODO this should probably write async and return via a channel. For now it blocks.
	Write(bucket, object string, chunk int, part uint8, length int, params erasure.EncoderParams, reader io.Reader) error

	GetObjectMetadata(bucket string, object string, prefix string) (storage.ObjectMetadata, error)
	ListObjects(bucket string, resources storage.BucketResourcesMetadata) ([]storage.ObjectMetadata, storage.BucketResourcesMetadata, error)

	SetPolicy(bucket string, policy interface{}) error
	GetPolicy(bucket string) (interface{}, error)

	CreateBucket(bucket string) error
}
