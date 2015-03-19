/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donutstorage

import (
	"bytes"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"crypto/md5"
	"encoding/hex"

	"github.com/minio-io/minio/pkg/donutbox"
	"github.com/minio-io/minio/pkg/encoding/erasure"
	"github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/utils/split"
)

// DonutDriver creates a new single disk storage driver using donut without encoding.
type DonutDriver struct {
	donutBox donutbox.DonutBox
}

const (
	blockSize = 10 * 1024 * 1024
)

// Start a single disk subsystem
func Start(donutBox donutbox.DonutBox) (chan<- string, <-chan error, storage.Storage) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	s := new(DonutDriver)
	s.donutBox = donutBox
	go start(ctrlChannel, errorChannel, s)
	return ctrlChannel, errorChannel, s
}

func start(ctrlChannel <-chan string, errorChannel chan<- error, s *DonutDriver) {
	close(errorChannel)
}

// ListBuckets returns a list of buckets
func (donutStorage DonutDriver) ListBuckets() (results []storage.BucketMetadata, err error) {
	buckets, err := donutStorage.donutBox.ListBuckets()
	if err != nil {
		return nil, err
	}
	sort.Strings(buckets)
	for _, bucket := range buckets {
		metadata, err := donutStorage.donutBox.GetBucketMetadata(bucket)
		if err != nil {
			return nil, err
		}

		created, err := time.Parse(time.RFC3339Nano, metadata["created"])
		if err != nil {
			return nil, err
		}
		bucketMetadata := storage.BucketMetadata{
			Name:    bucket,
			Created: created,
		}
		results = append(results, bucketMetadata)
	}
	return results, err
}

// CreateBucket creates a new bucket
func (donutStorage DonutDriver) CreateBucket(bucket string) error {
	err := donutStorage.donutBox.CreateBucket(bucket)
	if err != nil {
		return err
	}
	metadataBucket := storage.BucketMetadata{
		Name:    bucket,
		Created: time.Now(),
	}
	metadata := createBucketMetadata(metadataBucket)
	err = donutStorage.donutBox.SetBucketMetadata(bucket, metadata)
	if err != nil {
		return err
	}
	return nil
}

// GetBucketMetadata retrieves an bucket's metadata
func (donutStorage DonutDriver) GetBucketMetadata(bucket string) (storage.BucketMetadata, error) {
	metadata, err := donutStorage.donutBox.GetBucketMetadata(bucket)
	if err != nil {
		return storage.BucketMetadata{}, err
	}
	created, err := time.Parse(time.RFC3339Nano, metadata["created"])
	bucketMetadata := storage.BucketMetadata{
		Name:    bucket,
		Created: created,
	}
	return bucketMetadata, nil
}

// CreateBucketPolicy sets a bucket's access policy
func (donutStorage DonutDriver) CreateBucketPolicy(bucket string, p storage.BucketPolicy) error {
	return errors.New("Not Implemented")
}

// GetBucketPolicy returns a bucket's access policy
func (donutStorage DonutDriver) GetBucketPolicy(bucket string) (storage.BucketPolicy, error) {
	return storage.BucketPolicy{}, errors.New("Not Implemented")
}

// GetObject retrieves an object and writes it to a writer
func (donutStorage DonutDriver) GetObject(target io.Writer, bucket, key string) (int64, error) {
	metadata, err := donutStorage.donutBox.GetObjectMetadata(bucket, key, 0)
	if err != nil {
		// TODO strongly type and properly handle error cases
		return 0, storage.ObjectNotFound{Bucket: bucket, Object: key}
	}
	k, err := strconv.Atoi(metadata["erasureK"])
	if err != nil {
		return 0, errors.New("Cannot parse erasureK")
	}
	m, err := strconv.Atoi(metadata["erasureM"])
	if err != nil {
		return 0, errors.New("Cannot parse erasureM")
	}
	columnCount := k + m
	bs, err := strconv.Atoi(metadata["blockSize"])
	if err != nil {
		return 0, errors.New("Cannot parse blockSize")
	}
	size, err := strconv.Atoi(metadata["size"])
	if err != nil {
		return 0, errors.New("Cannot parse length")
	}
	chunkCount := size/bs + 1
	var readers []io.Reader
	for column := 0; column < columnCount; column++ {
		reader, err := donutStorage.donutBox.GetObjectReader(bucket, key, uint(column))
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
func (donutStorage DonutDriver) GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error) {
	return 0, errors.New("Not Implemented")
}

// GetObjectMetadata retrieves an object's metadata
func (donutStorage DonutDriver) GetObjectMetadata(bucket, key string, prefix string) (storage.ObjectMetadata, error) {
	metadata, err := donutStorage.donutBox.GetObjectMetadata(bucket, key, 0)
	if err != nil {
		return storage.ObjectMetadata{}, err
	}
	created, err := time.Parse(time.RFC3339Nano, metadata["created"])
	size, err := strconv.ParseInt(metadata["size"], 10, 64)
	objectMetadata := storage.ObjectMetadata{
		Bucket:      bucket,
		Key:         key,
		ContentType: metadata["contentType"],
		Created:     created,
		Md5:         metadata["md5"],
		Size:        size,
	}
	return objectMetadata, nil
}

// ListObjects lists objects
func (donutStorage DonutDriver) ListObjects(bucket string, resources storage.BucketResourcesMetadata) ([]storage.ObjectMetadata, storage.BucketResourcesMetadata, error) {
	objects, err := donutStorage.donutBox.ListObjectsInBucket(bucket, resources.Prefix)
	if err != nil {
		return nil, storage.BucketResourcesMetadata{}, err
	}
	var results []storage.ObjectMetadata
	sort.Strings(objects)
	for _, object := range withoutDelimiter(objects, resources.Prefix, resources.Delimiter) {
		if len(results) < resources.Maxkeys {
			objectMetadata, err := donutStorage.GetObjectMetadata(bucket, object, "")
			if err != nil {
				return nil, storage.BucketResourcesMetadata{}, err
			}
			results = append(results, objectMetadata)
		} else {
			resources.IsTruncated = true
		}
	}
	if resources.Delimiter != "" {
		objects = trimPrefixWithDelimiter(objects, resources.Prefix, resources.Delimiter)
		objects = beforeDelimiter(objects, resources.Delimiter)
		resources.CommonPrefixes = objects
	}
	return results, resources, nil
}

func appendUniq(slice []string, i string) []string {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}

func withoutDelimiter(inputs []string, prefix, delim string) (results []string) {
	if delim == "" {
		return inputs
	}
	for _, input := range inputs {
		input = strings.TrimPrefix(input, prefix)
		if !strings.Contains(input, delim) {
			results = appendUniq(results, prefix+input)
		}
	}
	return results
}

func trimPrefixWithDelimiter(inputs []string, prefix, delim string) (results []string) {
	for _, input := range inputs {
		input = strings.TrimPrefix(input, prefix)
		if strings.Contains(input, delim) {
			results = appendUniq(results, input)
		}
	}
	return results
}

func beforeDelimiter(inputs []string, delim string) (results []string) {
	for _, input := range inputs {
		results = appendUniq(results, strings.Split(input, delim)[0]+delim)
	}
	return results
}

// CreateObject creates a new object
func (donutStorage DonutDriver) CreateObject(bucketKey, objectKey, contentType, md5sum string, reader io.Reader) error {
	// set defaults
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentType = strings.TrimSpace(contentType)
	// split stream
	splitStream := split.Stream(reader, uint64(blockSize))
	writers := make([]*donutbox.NewObject, 16)
	for i := 0; i < 16; i++ {
		newWriter, err := donutStorage.donutBox.GetObjectWriter(bucketKey, objectKey, uint(i))
		if err != nil {
			closeAllWritersWithError(writers, err)
			return err
		}
		writers[i] = newWriter
	}
	totalLength := uint64(0)
	chunkCount := 0
	hasher := md5.New()
	for chunk := range splitStream {
		params, err := erasure.ParseEncoderParams(8, 8, erasure.Cauchy)
		if err != nil {
			return err
		}
		hasher.Write(chunk.Data)
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
		Md5:         hex.EncodeToString(hasher.Sum(nil)),
		Size:        int64(totalLength),
	}

	metadata := createObjectMetadata(metadataObj, blockSize, 8, 8, "Cauchy")

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

func createBucketMetadata(metadataBucket storage.BucketMetadata) map[string]string {
	metadata := make(map[string]string)
	metadata["bucket"] = metadataBucket.Name
	metadata["created"] = metadataBucket.Created.Format(time.RFC3339Nano)
	return metadata
}

func createObjectMetadata(metadataObject storage.ObjectMetadata, blockSize int, k, m uint8, technique string) map[string]string {
	metadata := make(map[string]string)
	metadata["bucket"] = metadataObject.Bucket
	metadata["key"] = metadataObject.Key
	metadata["contentType"] = metadataObject.ContentType
	metadata["created"] = metadataObject.Created.Format(time.RFC3339Nano)
	metadata["md5"] = metadataObject.Md5
	metadata["size"] = strconv.FormatInt(metadataObject.Size, 10)
	metadata["blockSize"] = strconv.FormatUint(uint64(blockSize), 10)
	metadata["erasureK"] = strconv.FormatUint(uint64(k), 10)
	metadata["erasureM"] = strconv.FormatUint(uint64(m), 10)
	metadata["erasureTechnique"] = technique
	return metadata
}
