package donut

import "io"

// INTERFACES

// Donut interface
type Donut interface {
	CreateBucket(bucket string) error
	GetBuckets() ([]string, error)
	GetObject(bucket, object string) (io.ReadCloser, error)
	GetObjectMetadata(bucket, object string) (map[string]string, error)
	GetObjectWriter(bucket, object string) (ObjectWriter, error)
}

// Bucket is an interface for managing buckets
type Bucket interface {
	GetObject(object string) (io.Reader, error)
	GetObjectMetadata(object string) (map[string]string, error)
	GetObjectWriter(object string) (ObjectWriter, error)
	GetObjects() ([]string, error)
}

// Disk is an interface for managing disks
type Disk interface {
	GetBuckets(object string) ([]string, error)
}

// ObjectWriter is an interface for writing new objects
type ObjectWriter interface {
	Write([]byte) error
	Close() error
	CloseWithError(error) error

	SetMetadata(map[string]string)
	GetMetadata() map[string]string
}

// InternalObjectWriter is an interface for use internally to donut
type InternalObjectWriter interface {
	ObjectWriter

	SetDonutMetadata(map[string]string)
	GetDonutMetadata() map[string]string
}
