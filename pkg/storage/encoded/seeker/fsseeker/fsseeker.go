package fsseeker

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path"
	"strconv"

	encoder "github.com/minio-io/minio/pkg/encoding/erasure"
	"github.com/minio-io/minio/pkg/storage/donut/erasure"
	"github.com/minio-io/minio/pkg/storage/donut/fragment/fragment1"

	"github.com/minio-io/minio/pkg/storage"
)

type Seeker struct {
	Root string
}

// lists buckets
func (seeker Seeker) ListBuckets() ([]storage.BucketMetadata, error) {
	return nil, errors.New("Not Implemented")
}

// gets reader
func (seeker Seeker) GetReader(bucket, object string, chunk uint, index uint8) (io.Reader, error) {
	return nil, errors.New("Not Implemented")
}

// Write a fragment
func (seeker Seeker) Write(bucket, object string, chunk int, part uint8, length int, params encoder.EncoderParams, reader io.Reader) error {
	// stage object
	// create new file

	key := object + "$" + strconv.Itoa(chunk)

	var technique erasure.EncoderTechnique
	switch {
	case params.Technique == encoder.Cauchy:
		technique = erasure.Cauchy
	case params.Technique == encoder.Vandermonde:
		technique = erasure.Vandermonde
	default:
		errors.New("Unknown technique")
	}

	var erasureBuffer bytes.Buffer
	err := erasure.Write(&erasureBuffer, key, part, uint32(length), params.K, params.M, technique, reader)
	if err != nil {
		return err
	}

	target, err := os.OpenFile(path.Join(seeker.Root, key), os.O_WRONLY|os.O_CREATE, 0600)
	defer target.Close()
	if err != nil {
		return err
	}

	fragment1.Write(target, &erasureBuffer, uint64(erasureBuffer.Len()))
	// TODO verify write
	return err
}

// Get's object metadata
func (seeker Seeker) GetObjectMetadata(bucket string, object string) (storage.ObjectMetadata, error) {
	return storage.ObjectMetadata{}, errors.New("Not Implemented")
}

// Lists objects
func (seeker Seeker) ListObjects(bucket string, resources storage.BucketResourcesMetadata) ([]storage.ObjectMetadata, storage.BucketResourcesMetadata, error) {
	return nil, storage.BucketResourcesMetadata{}, errors.New("Not Implemented")
}

// Sets bucket policy
func (seeker Seeker) SetPolicy(bucket string, policy interface{}) error {
	return errors.New("Not Implemented")
}

// Gets a bucket policy
func (seeker Seeker) GetPolicy(bucket string) (interface{}, error) {
	return nil, errors.New("Not Implemented")
}

// Creates a bucket
func (seeker Seeker) CreateBucket(bucket string) error {
	return errors.New("Not Implemented")
}
