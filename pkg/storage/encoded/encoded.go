/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package encoded

import (
	"bytes"
	"encoding/hex"
	"errors"
	"github.com/minio-io/minio/pkg/donutbox"
	"github.com/minio-io/minio/pkg/encoding/erasure"
	"github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/utils/split"
	"io"
	"strconv"
	"time"
)

// StorageDriver creates a new single disk storage driver using donut without encoding.
type StorageDriver struct {
	donutBox donutbox.DonutBox
}

// Start a single disk subsystem
func Start(donutBox donutbox.DonutBox) (chan<- string, <-chan error, storage.Storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	s := new(StorageDriver)
	s.donutBox = donutBox
	go start(ctrlChannel, errorChannel, s)
	return ctrlChannel, errorChannel, s
}

func start(ctrlChannel <-chan string, errorChannel chan<- error, s *StorageDriver) {
	close(errorChannel)
}

// ListBuckets returns a list of buckets
func (diskStorage StorageDriver) ListBuckets() ([]storage.BucketMetadata, error) {
	return nil, errors.New("Not Implemented")
}

// CreateBucket creates a new bucket
func (diskStorage StorageDriver) CreateBucket(bucket string) error {
	return diskStorage.donutBox.CreateBucket(bucket)
}

// CreateBucketPolicy sets a bucket's access policy
func (diskStorage StorageDriver) CreateBucketPolicy(bucket string, p storage.BucketPolicy) error {
	return errors.New("Not Implemented")
}

// GetBucketPolicy returns a bucket's access policy
func (diskStorage StorageDriver) GetBucketPolicy(bucket string) (storage.BucketPolicy, error) {
	return storage.BucketPolicy{}, errors.New("Not Implemented")
}

// GetObject retrieves an object and writes it to a writer
func (diskStorage StorageDriver) GetObject(target io.Writer, bucket, key string) (int64, error) {
	metadata, err := diskStorage.donutBox.GetObjectMetadata(bucket, key, 0)
	k, err := strconv.Atoi(metadata["erasureK"])
	if err != nil {
		return 0, errors.New("Cannot parse erasureK")
	}
	m, err := strconv.Atoi(metadata["erasureM"])
	if err != nil {
		return 0, errors.New("Cannot parse erasureM")
	}
	columnCount := k + m
	blockSize, err := strconv.Atoi(metadata["blockSize"])
	if err != nil {
		return 0, errors.New("Cannot parse blockSize")
	}
	size, err := strconv.Atoi(metadata["size"])
	if err != nil {
		return 0, errors.New("Cannot parse length")
	}
	chunkCount := size/blockSize + 1
	var readers []io.Reader
	for column := 0; column < columnCount; column++ {
		reader, err := diskStorage.donutBox.GetObjectReader(bucket, key, uint(column))
		if err != nil {
			return 0, err
		}
		readers = append(readers, reader)
	}

	totalWritten := int64(size)
	totalRemaining := int64(size)
	if err != err {
		return 0, err
	}
	params, err := erasure.ParseEncoderParams(8, 8, erasure.Cauchy)
	decoder := erasure.NewEncoder(params)
	for chunk := 0; chunk < chunkCount; chunk++ {
		blocks := make([][]byte, columnCount)
		for column := 0; column < columnCount; column++ {
			var block bytes.Buffer
			limitReader := io.LimitReader(readers[column], int64(blockSize))
			_, err := io.Copy(&block, limitReader)
			if err != nil {
				return totalWritten, err
			}
			blocks[column] = block.Bytes()
		}
		curBlockSize := blockSize
		if totalRemaining < int64(blockSize) {
			curBlockSize = int(totalRemaining)
		}
		original, err := decoder.Decode(blocks, curBlockSize)
		if err != nil {
			return totalWritten, err
		}
		curWritten, err := io.Copy(target, bytes.NewBuffer(original))
		totalRemaining = totalRemaining - curWritten
		if err != nil {
			return totalWritten, err
		}
	}

	return totalWritten, nil
}

// GetPartialObject retrieves an object and writes it to a writer
func (diskStorage StorageDriver) GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error) {
	return 0, errors.New("Not Implemented")
}

// GetObjectMetadata retrieves an object's metadata
func (diskStorage StorageDriver) GetObjectMetadata(bucket, key string, prefix string) (storage.ObjectMetadata, error) {
	metadata, err := diskStorage.donutBox.GetObjectMetadata(bucket, key, 0)
	if err != nil {
		return storage.ObjectMetadata{}, err
	}
	created, err := time.Parse(metadata["created"], time.RFC3339Nano)
	size, err := strconv.ParseInt(metadata["size"], 10, 64)
	objectMetadata := storage.ObjectMetadata{
		Bucket:      bucket,
		Key:         key,
		ContentType: metadata["contentType"],
		Created:     created,
		ETag:        hex.EncodeToString([]byte(metadata["md5"])),
		Size:        size,
	}
	return objectMetadata, nil
}

// ListObjects lists objects
func (diskStorage StorageDriver) ListObjects(bucket string, resources storage.BucketResourcesMetadata) ([]storage.ObjectMetadata, storage.BucketResourcesMetadata, error) {
	return nil, storage.BucketResourcesMetadata{}, errors.New("Not Implemented")
}

// CreateObject creates a new object
func (diskStorage StorageDriver) CreateObject(bucketKey string, objectKey string, contentType string, reader io.Reader) error {
	blockSize := 10 * 1024 * 1024
	// split stream
	splitStream := split.Stream(reader, uint64(blockSize))
	writers := make([]*donutbox.NewObject, 16)
	for i := 0; i < 16; i++ {
		newWriter, err := diskStorage.donutBox.GetObjectWriter(bucketKey, objectKey, uint(i), uint(blockSize))
		if err != nil {
			closeAllWritersWithError(writers, err)
			return err
		}
		writers[i] = newWriter
	}
	totalLength := uint64(0)
	chunkCount := 0
	for chunk := range splitStream {
		params, err := erasure.ParseEncoderParams(8, 8, erasure.Cauchy)
		if err != nil {
			return err
		}
		totalLength = totalLength + uint64(len(chunk.Data))
		chunkCount = chunkCount + 1
		encoder := erasure.NewEncoder(params)
		if chunk.Err == nil {
			parts, _ := encoder.Encode(chunk.Data)
			for index, part := range parts {
				if _, err := writers[index].Write(part); err != nil {
					closeAllWritersWithError(writers, err)
					return err
				}
			}
		} else {
			closeAllWritersWithError(writers, chunk.Err)
			return chunk.Err
		}
		// encode data
		// write
	}
	// close connections

	metadataObj := storage.ObjectMetadata{
		Bucket: bucketKey,
		Key:    objectKey,

		ContentType: contentType,
		Created:     time.Now(),
		ETag:        "md5",
		Size:        int64(totalLength),
	}

	metadata := createMetadata(metadataObj, blockSize, 8, 8, "Cauchy")

	for column := uint(0); column < 16; column++ {
		writers[column].SetMetadata(metadata)
	}

	// TODO capture errors in writers, enough should pass before returning
	closeAllWriters(writers)

	return nil
}

func closeAllWriters(writers []*donutbox.NewObject) {
	for _, writer := range writers {
		if writer != nil {
			writer.Close()
		}
	}
}

func closeAllWritersWithError(writers []*donutbox.NewObject, err error) {
	for _, writer := range writers {
		if writer != nil {
			writer.CloseWithError(err)
		}
	}
}

func createMetadata(metadataObject storage.ObjectMetadata, blockSize int, k, m uint8, technique string) map[string]string {
	metadata := make(map[string]string)
	metadata["bucket"] = metadataObject.Bucket
	metadata["key"] = metadataObject.Key
	metadata["contentType"] = metadataObject.ContentType
	metadata["created"] = metadataObject.Created.Format(time.RFC3339Nano)
	metadata["md5"] = metadataObject.ETag
	metadata["size"] = strconv.FormatInt(metadataObject.Size, 10)

	metadata["blockSize"] = strconv.FormatUint(uint64(blockSize), 10)
	metadata["erasureK"] = strconv.FormatUint(uint64(k), 10)
	metadata["erasureM"] = strconv.FormatUint(uint64(m), 10)
	metadata["erasureTechnique"] = technique
	return metadata
}
