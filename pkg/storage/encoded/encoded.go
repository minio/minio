package encoded

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"errors"
	"io"
	"io/ioutil"

	"github.com/minio-io/minio/pkg/encoding/erasure"
	mstorage "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/storage/encoded/seeker"
	"github.com/minio-io/minio/pkg/utils/split"
)

// Encoded FS Storage
type Storage struct {
	Seeker seeker.Seeker
}

type ObjectHeader struct {
	Bucket     string
	Key        string
	Length     uint64
	Md5        []byte
	ChunkCount int
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
func (storage *Storage) StoreBucketPolicy(bucket string, policy mstorage.BucketPolicy) error {
	return storage.Seeker.SetPolicy(bucket, policy)
}

// Get a bucket policy
func (storage *Storage) GetBucketPolicy(bucket string) (mstorage.BucketPolicy, error) {
	return storage.Seeker.GetPolicy(bucket)
}

// Object Operations
func (storage *Storage) CopyObjectToWriter(w io.Writer, bucket string, object string) (int64, error) {
	// read object 0
	var headerBytes [][]byte
	headerLength := 0
	for i := 0; i < 16; i++ {
		header, reader, err := storage.Seeker.GetReader(bucket, object, 0, uint8(i))
		headerLength = int(header.OriginalLength)
		if err != nil {
			return 0, err
		}
		part, err := ioutil.ReadAll(reader)
		if err != nil {
			return 0, err
		}
		headerBytes = append(headerBytes, part)
	}

	params, err := erasure.ParseEncoderParams(8, 8, erasure.Cauchy)
	if err != nil {
		return 0, err
	}

	encoder := erasure.NewEncoder(params)
	headerGob, err := encoder.Decode(headerBytes, headerLength)
	if err != nil {
		return 0, err
	}

	objectHeader := ObjectHeader{}

	headerDecoder := gob.NewDecoder(bytes.NewBuffer(headerGob))
	err = headerDecoder.Decode(&objectHeader)
	if err != nil {
		return 0, err
	}

	// extract number of parts from object 0
	for chunkId := 0; chunkId < objectHeader.ChunkCount; chunkId++ {

	}

	// read all parts

	// reconstruct object

	// stream back to w
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
func (storage *Storage) StoreObject(bucket string, key string, contentType string, reader io.Reader) error {
	// split object 10M
	splits := split.Stream(reader, 10*1024*1024)
	totalLength := uint64(0)
	// send each split to encoder
	params, err := erasure.ParseEncoderParams(8, 8, erasure.Cauchy)
	if err != nil {
		return err
	}
	encoder := erasure.NewEncoder(params)
	chunkId := 1

	hash := md5.New()
	for chunk := range splits {
		if chunk.Err != nil {
			return err
		}
		totalLength = totalLength + uint64(len(chunk.Data))
		hash.Write(chunk.Data)
		// compute isal
		encodedData, length := encoder.Encode(chunk.Data)
		// write erasure layer
		for index, data := range encodedData {
			err := storage.Seeker.Write(bucket, key, chunkId, uint8(index), length, *params, bytes.NewBuffer(data))
			if err != nil {
				return err
			}
		}

		// write to fragment
		chunkId = chunkId + 1
	}
	// create header
	header := ObjectHeader{
		Bucket:     bucket,
		Key:        key,
		Length:     totalLength,
		Md5:        hash.Sum(nil),
		ChunkCount: chunkId,
	}
	var headerBuffer bytes.Buffer
	headerEncoder := gob.NewEncoder(&headerBuffer)
	err = headerEncoder.Encode(header)
	if err != nil {
		return err
	}
	encodedHeader, length := encoder.Encode(headerBuffer.Bytes())
	for index, data := range encodedHeader {
		err = storage.Seeker.Write(bucket, key, 0, uint8(index), length, *params, bytes.NewBuffer(data))
		if err != nil {
			return err
		}
	}
	return err
}
