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
	GetObjectWriter(bucket, object string, column, blockSize uint) (*NewObject, error)
	GetObjectReader(bucket, object string, column uint) (io.Reader, error)
	GetObjectMetadata(bucket, object string, column uint) (map[string]string, error)
}

// Result is a result for async tasks
type Result struct {
	Err error
}

// CreateNewObject creates a new object wrapping a writer. Clients are not expected to use this directly. This is exposed for storage drivers.
func CreateNewObject(writer *io.PipeWriter) *NewObject {
	return &NewObject{writer: writer}
}

// NewObject wraps a writer and allows setting metadata. On a successful close, the object is committed by the backend.
type NewObject struct {
	metadata map[string]string
	writer   *io.PipeWriter
}

// Write data
func (newObject *NewObject) Write(data []byte) (int, error) {
	return newObject.writer.Write(data)
}

// SetMetadata sets metadata for an object
func (newObject *NewObject) SetMetadata(metadata map[string]string) {
	newMetadata := make(map[string]string)
	for k, v := range metadata {
		newMetadata[k] = v
	}
	newObject.metadata = newMetadata
}

// Close and commit the object
func (newObject *NewObject) Close() error {
	return newObject.writer.Close()
}

// CloseWithError closes the object with an error, causing the backend to abandon the object
func (newObject *NewObject) CloseWithError(err error) error {
	return newObject.writer.CloseWithError(err)
}

// GetMetadata returns a copy of the metadata set metadata
func (newObject *NewObject) GetMetadata() map[string]string {
	newMetadata := make(map[string]string)
	if newMetadata != nil {
		for k, v := range newObject.metadata {
			newMetadata[k] = v
		}
	}
	return newMetadata
}
