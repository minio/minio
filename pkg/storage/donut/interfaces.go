package donut

import (
	"io"
)

// Collection of Donut specification interfaces

// Donut interface
type Donut interface {
	CreateBucket(bucket string) error
	GetObjectReader(bucket, object string) (io.ReadCloser, error)
	GetObjectWriter(bucket, object string) (ObjectWriter, error)
	GetObjectMetadata(bucket, object string) (map[string]string, error)
	ListBuckets() ([]string, error)
	ListObjects(bucket string) ([]string, error)
}

// Bucket interface
type Bucket interface {
	GetNodes() ([]string, error)
	AddNode(nodeID, bucketID string) error
}

// Node interface
type Node interface {
	CreateBucket(bucket string) error
	GetBuckets() ([]string, error)
	GetDonutMetadata(bucket, object string) (map[string]string, error)
	GetMetadata(bucket, object string) (map[string]string, error)
	GetReader(bucket, object string) (io.ReadCloser, error)
	GetWriter(bucket, object string) (Writer, error)
	ListObjects(bucket string) ([]string, error)
}

// ObjectWriter interface
type ObjectWriter interface {
	Close() error
	CloseWithError(error) error
	GetMetadata() (map[string]string, error)
	SetMetadata(map[string]string) error
	Write([]byte) (int, error)
}

// Writer interface
type Writer interface {
	ObjectWriter

	GetDonutMetadata() (map[string]string, error)
	SetDonutMetadata(map[string]string) error
}
