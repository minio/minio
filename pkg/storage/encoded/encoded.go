package encoded

import (
	"errors"
	"io"

	mstorage "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/storage/encoded/seeker"
)

// Encoded FS Storage
type Storage struct {
	Seeker seeker.Seeker
}

// Start inmemory object server
func Start(seeker seeker.Seeker) (chan<- string, <-chan error, mstorage.Storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	go start(ctrlChannel, errorChannel)
	return ctrlChannel, errorChannel, &Storage{Seeker: seeker}
}

func start(ctrlChannel <-chan string, errorChannel chan<- error) {
}

// Bucket Operations
func (storage *Storage) ListBuckets() ([]mstorage.BucketMetadata, error) {
	return storage.Seeker.ListBuckets()
}

// Store a bucket
func (storage *Storage) StoreBucket(bucket string) error {
	return storage.Seeker.CreateBucket(bucket)

}

// Store a bucket policy
func (storage *Storage) StoreBucketPolicy(bucket string, policy interface{}) error {
	return storage.Seeker.SetPolicy(bucket, policy)
}

// Get a bucket policy
func (storage *Storage) GetBucketPolicy(bucket string) (interface{}, error) {
	return storage.Seeker.GetPolicy(bucket)
}

// Object Operations
func (storage *Storage) CopyObjectToWriter(w io.Writer, bucket string, object string) (int64, error) {
	return 0, errors.New("Not Implemented")
}

// Get object metadata
func (storage *Storage) GetObjectMetadata(bucket string, object string, prefix string) (mstorage.ObjectMetadata, error) {
	return storage.Seeker.GetObjectMetadata(bucket, object, prefix)

}

// Lists objects
func (storage *Storage) ListObjects(bucket string, resources mstorage.BucketResourcesMetadata) ([]mstorage.ObjectMetadata, mstorage.BucketResourcesMetadata, error) {
	return nil, mstorage.BucketResourcesMetadata{}, errors.New("Not Implemented")
}

// Stores an object
func (storage *Storage) StoreObject(bucket string, key string, contentType string, data io.Reader) error {
	return errors.New("Not Implemented")
}
