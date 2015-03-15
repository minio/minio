package donutbox

import "io"

// DonutBox is an interface specifying how the storage driver should interact with its underlying system.
type DonutBox interface {
	// system operations
	ListBuckets() ([]string, error)

	// bucket operations
	CreateBucket(bucket string) error
	ListObjects(bucket, prefix string) ([]string, error)
	GetBucketMetadata(bucket, name string) (io.Reader, error)
	SetBucketMetadata(bucket, name string, metadata io.Reader) error

	// object operations
	GetObjectWriter(bucket, object string, column, blockSize uint) (io.WriteCloser, <-chan Result, error)
	GetObjectReader(bucket, object string, column int) (io.Reader, error)
	StoreObjectMetadata(bucket, object, name string, reader io.Reader) error
	GetObjectMetadata(bucket, object, name string) (io.Reader, error)
}

// Result is a result for async tasks
type Result struct {
	Err error
}
