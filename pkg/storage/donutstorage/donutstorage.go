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
	"errors"
	"github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/storage/donut"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Storage creates a new single disk storage driver using donut without encoding.
type Storage struct {
	donut donut.Donut
}

const (
	blockSize = 10 * 1024 * 1024
)

// Start a single disk subsystem
func Start(path string) (chan<- string, <-chan error, storage.Storage) {

	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	s := new(Storage)

	// TODO donut driver should be passed in as Start param and driven by config
	var err error
	s.donut, err = donut.NewDonutDriver(path)
	if err != nil {
		errorChannel <- err
	}

	go start(ctrlChannel, errorChannel, s)
	return ctrlChannel, errorChannel, s
}

func start(ctrlChannel <-chan string, errorChannel chan<- error, s *Storage) {
	close(errorChannel)
}

// ListBuckets returns a list of buckets
func (donutStorage Storage) ListBuckets() (results []storage.BucketMetadata, err error) {
	buckets, err := donutStorage.donut.ListBuckets()
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets {
		if err != nil {
			return nil, err
		}
		result := storage.BucketMetadata{
			Name: bucket,
			// TODO Add real created date
			Created: time.Now(),
		}
		results = append(results, result)
	}
	return results, nil
}

// CreateBucket creates a new bucket
func (donutStorage Storage) CreateBucket(bucket string) error {
	return donutStorage.donut.CreateBucket(bucket)
}

// GetBucketMetadata retrieves an bucket's metadata
func (donutStorage Storage) GetBucketMetadata(bucket string) (storage.BucketMetadata, error) {
	return storage.BucketMetadata{}, errors.New("Not Implemented")
}

// CreateBucketPolicy sets a bucket's access policy
func (donutStorage Storage) CreateBucketPolicy(bucket string, p storage.BucketPolicy) error {
	return errors.New("Not Implemented")
}

// GetBucketPolicy returns a bucket's access policy
func (donutStorage Storage) GetBucketPolicy(bucket string) (storage.BucketPolicy, error) {
	return storage.BucketPolicy{}, errors.New("Not Implemented")
}

// GetObject retrieves an object and writes it to a writer
func (donutStorage Storage) GetObject(target io.Writer, bucket, key string) (int64, error) {
	reader, err := donutStorage.donut.GetObject(bucket, key)
	if err != nil {
		return 0, storage.ObjectNotFound{
			Bucket: bucket,
			Object: key,
		}
	}
	return io.Copy(target, reader)
}

// GetPartialObject retrieves an object and writes it to a writer
func (donutStorage Storage) GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error) {
	return 0, errors.New("Not Implemented")
}

// GetObjectMetadata retrieves an object's metadata
func (donutStorage Storage) GetObjectMetadata(bucket, key string, prefix string) (storage.ObjectMetadata, error) {
	metadata, err := donutStorage.donut.GetObjectMetadata(bucket, key)
	created, err := time.Parse(time.RFC3339Nano, metadata["sys.created"])
	if err != nil {
		return storage.ObjectMetadata{}, err
	}
	size, err := strconv.ParseInt(metadata["sys.size"], 10, 64)
	if err != nil {
		return storage.ObjectMetadata{}, err
	}
	objectMetadata := storage.ObjectMetadata{
		Bucket: bucket,
		Key:    key,

		ContentType: metadata["contentType"],
		Created:     created,
		Md5:         metadata["sys.md5"],
		Size:        size,
	}
	return objectMetadata, nil
}

// ListObjects lists objects
func (donutStorage Storage) ListObjects(bucket string, resources storage.BucketResourcesMetadata) ([]storage.ObjectMetadata, storage.BucketResourcesMetadata, error) {
	// TODO Fix IsPrefixSet && IsDelimiterSet and use them
	objects, err := donutStorage.donut.ListObjects(bucket)
	if err != nil {
		return nil, storage.BucketResourcesMetadata{}, err
	}
	sort.Strings(objects)
	if resources.Prefix != "" {
		objects = filterPrefix(objects, resources.Prefix)
		objects = removePrefix(objects, resources.Prefix)
	}
	if resources.Maxkeys <= 0 || resources.Maxkeys > 1000 {
		resources.Maxkeys = 1000
	}

	var actualObjects []string
	var commonPrefixes []string
	if strings.TrimSpace(resources.Delimiter) != "" {
		actualObjects = filterDelimited(objects, resources.Delimiter)
		commonPrefixes = filterNotDelimited(objects, resources.Delimiter)
		commonPrefixes = extractDir(commonPrefixes, resources.Delimiter)
		commonPrefixes = uniqueObjects(commonPrefixes)
		resources.CommonPrefixes = commonPrefixes
	} else {
		actualObjects = objects
	}

	var results []storage.ObjectMetadata
	for _, object := range actualObjects {
		if len(results) >= resources.Maxkeys {
			resources.IsTruncated = true
			break
		}
		metadata, err := donutStorage.GetObjectMetadata(bucket, resources.Prefix+object, "")
		if err != nil {
			return nil, storage.BucketResourcesMetadata{}, err
		}
		results = append(results, metadata)
	}
	return results, resources, nil
}

func filterPrefix(objects []string, prefix string) []string {
	var results []string
	for _, object := range objects {
		if strings.HasPrefix(object, prefix) {
			results = append(results, object)
		}
	}
	return results
}

func removePrefix(objects []string, prefix string) []string {
	var results []string
	for _, object := range objects {
		results = append(results, strings.TrimPrefix(object, prefix))
	}
	return results
}

func filterDelimited(objects []string, delim string) []string {
	var results []string
	for _, object := range objects {
		if !strings.Contains(object, delim) {
			results = append(results, object)
		}
	}
	return results
}
func filterNotDelimited(objects []string, delim string) []string {
	var results []string
	for _, object := range objects {
		if strings.Contains(object, delim) {
			results = append(results, object)
		}
	}
	return results
}

func extractDir(objects []string, delim string) []string {
	var results []string
	for _, object := range objects {
		parts := strings.Split(object, delim)
		results = append(results, parts[0]+"/")
	}
	return results
}

func uniqueObjects(objects []string) []string {
	objectMap := make(map[string]string)
	for _, v := range objects {
		objectMap[v] = v
	}
	var results []string
	for k := range objectMap {
		results = append(results, k)
	}
	sort.Strings(results)
	return results
}

// CreateObject creates a new object
func (donutStorage Storage) CreateObject(bucketKey, objectKey, contentType, expectedMd5sum string, reader io.Reader) error {
	writer, err := donutStorage.donut.GetObjectWriter(bucketKey, objectKey)
	if err != nil {
		return err
	}
	if _, err := io.Copy(writer, reader); err != nil {
		return err
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentType = strings.TrimSpace(contentType)
	metadata := make(map[string]string)
	metadata["bucket"] = bucketKey
	metadata["object"] = objectKey
	metadata["contentType"] = contentType
	if err = writer.SetMetadata(metadata); err != nil {
		return err
	}
	return writer.Close()
}
