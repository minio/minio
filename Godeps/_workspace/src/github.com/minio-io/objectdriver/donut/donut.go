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

package donut

import (
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"io/ioutil"

	"github.com/minio-io/iodine"
	"github.com/minio-io/minio/pkg/storage/donut"
	"github.com/minio-io/minio/pkg/utils/log"
	"github.com/minio-io/objectdriver"
)

// donutDriver - creates a new single disk drivers driver using donut
type donutDriver struct {
	donut donut.Donut
}

const (
	blockSize = 10 * 1024 * 1024
)

// Start a single disk subsystem
func Start(path string) (chan<- string, <-chan error, drivers.Driver) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	s := new(donutDriver)

	// TODO donut driver should be passed in as Start param and driven by config
	var err error
	s.donut, err = donut.NewDonut(path)
	err = iodine.New(err, map[string]string{"path": path})
	if err != nil {
		log.Error.Println(err)
	}

	go start(ctrlChannel, errorChannel, s)
	return ctrlChannel, errorChannel, s
}

func start(ctrlChannel <-chan string, errorChannel chan<- error, s *donutDriver) {
	close(errorChannel)
}

// ListBuckets returns a list of buckets
func (d donutDriver) ListBuckets() (results []drivers.BucketMetadata, err error) {
	buckets, err := d.donut.ListBuckets()
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets {
		result := drivers.BucketMetadata{
			Name: bucket,
			// TODO Add real created date
			Created: time.Now(),
		}
		results = append(results, result)
	}
	return results, nil
}

// CreateBucket creates a new bucket
func (d donutDriver) CreateBucket(bucket string) error {
	return d.donut.CreateBucket(bucket)
}

// GetBucketMetadata retrieves an bucket's metadata
func (d donutDriver) GetBucketMetadata(bucket string) (drivers.BucketMetadata, error) {
	return drivers.BucketMetadata{}, errors.New("Not Implemented")
}

// CreateBucketPolicy sets a bucket's access policy
func (d donutDriver) CreateBucketPolicy(bucket string, p drivers.BucketPolicy) error {
	return errors.New("Not Implemented")
}

// GetBucketPolicy returns a bucket's access policy
func (d donutDriver) GetBucketPolicy(bucket string) (drivers.BucketPolicy, error) {
	return drivers.BucketPolicy{}, errors.New("Not Implemented")
}

// GetObject retrieves an object and writes it to a writer
func (d donutDriver) GetObject(target io.Writer, bucket, key string) (int64, error) {
	reader, err := d.donut.GetObjectReader(bucket, key)
	if err != nil {
		return 0, drivers.ObjectNotFound{
			Bucket: bucket,
			Object: key,
		}
	}
	return io.Copy(target, reader)
}

// GetPartialObject retrieves an object range and writes it to a writer
func (d donutDriver) GetPartialObject(w io.Writer, bucket, object string, start, length int64) (int64, error) {
	// TODO more efficient get partial object with proper donut support
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
		"start":  strconv.FormatInt(start, 10),
		"length": strconv.FormatInt(length, 10),
	}
	reader, err := d.donut.GetObjectReader(bucket, object)
	if err != nil {
		return 0, iodine.New(err, errParams)
	}
	_, err = io.CopyN(ioutil.Discard, reader, start)
	if err != nil {
		return 0, iodine.New(err, errParams)
	}
	n, err := io.CopyN(w, reader, length)
	return n, iodine.New(err, errParams)
}

// GetObjectMetadata retrieves an object's metadata
func (d donutDriver) GetObjectMetadata(bucket, key string, prefix string) (drivers.ObjectMetadata, error) {
	metadata, err := d.donut.GetObjectMetadata(bucket, key)
	if err != nil {
		// return ObjectNotFound quickly on an error, API needs this to handle invalid requests
		return drivers.ObjectMetadata{}, drivers.ObjectNotFound{
			Bucket: bucket,
			Object: key,
		}
	}
	created, err := time.Parse(time.RFC3339Nano, metadata["sys.created"])
	if err != nil {
		return drivers.ObjectMetadata{}, err
	}
	size, err := strconv.ParseInt(metadata["sys.size"], 10, 64)
	if err != nil {
		return drivers.ObjectMetadata{}, err
	}
	objectMetadata := drivers.ObjectMetadata{
		Bucket: bucket,
		Key:    key,

		ContentType: metadata["contentType"],
		Created:     created,
		Md5:         metadata["sys.md5"],
		Size:        size,
	}
	return objectMetadata, nil
}

// ListObjects - returns list of objects
func (d donutDriver) ListObjects(bucket string, resources drivers.BucketResourcesMetadata) ([]drivers.ObjectMetadata, drivers.BucketResourcesMetadata, error) {
	// TODO Fix IsPrefixSet && IsDelimiterSet and use them
	objects, err := d.donut.ListObjects(bucket)
	if err != nil {
		return nil, drivers.BucketResourcesMetadata{}, err
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

	var results []drivers.ObjectMetadata
	for _, object := range actualObjects {
		if len(results) >= resources.Maxkeys {
			resources.IsTruncated = true
			break
		}
		metadata, err := d.GetObjectMetadata(bucket, resources.Prefix+object, "")
		if err != nil {
			return nil, drivers.BucketResourcesMetadata{}, err
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
func (d donutDriver) CreateObject(bucketKey, objectKey, contentType, expectedMd5sum string, reader io.Reader) error {
	writer, err := d.donut.GetObjectWriter(bucketKey, objectKey)
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
