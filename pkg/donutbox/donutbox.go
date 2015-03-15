package donutbox

import "io"

// DonutBox is an interface specifying how the storage driver should interact with its underlying system.
type DonutBox interface {
	// system operations
	ListBuckets() ([]string, error)

	// bucket operations
	CreateBucket(bucket string) error
	ListObjectsInBucket(bucket, prefix string) ([]string, error)
	GetBucketMetadata(bucket string) (map[string]string, error)
	SetBucketMetadata(bucket string, metadata map[string]string) error

	// object operations
	GetObjectWriter(bucket, object string, column, blockSize uint) *io.PipeWriter
	GetObjectReader(bucket, object string, column int) (io.Reader, error)
	SetObjectMetadata(bucket, object string, metadata map[string]string) error
	GetObjectMetadata(bucket, object string) (map[string]string, error)
}

// Result is a result for async tasks
type Result struct {
	Err error
}
