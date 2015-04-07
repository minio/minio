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
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"io/ioutil"

	"github.com/minio-io/donut"
	"github.com/minio-io/iodine"
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

// This is a dummy nodeDiskMap which is going to be deprecated soon
// once the Management API is standardized, this map is useful for now
// to show multi disk API correctness behavior.
//
// This should be obtained from donut configuration file
func createNodeDiskMap(p string) map[string][]string {
	nodes := make(map[string][]string)
	nodes["localhost"] = make([]string, 16)
	for i := 0; i < len(nodes["localhost"]); i++ {
		diskPath := path.Join(p, strconv.Itoa(i))
		if _, err := os.Stat(diskPath); err != nil {
			if os.IsNotExist(err) {
				os.MkdirAll(diskPath, 0700)
			}
		}
		nodes["localhost"][i] = diskPath
	}
	return nodes
}

// Start a single disk subsystem
func Start(path string) (chan<- string, <-chan error, drivers.Driver) {
	ctrlChannel := make(chan string)
	errorChannel := make(chan error)
	errParams := map[string]string{"path": path}

	// Soon to be user configurable, when Management API
	// is finished we remove "default" to something
	// which is passed down from configuration
	donut, err := donut.NewDonut("default", createNodeDiskMap(path))
	if err != nil {
		err = iodine.New(err, errParams)
		log.Error.Println(err)
	}

	s := new(donutDriver)
	s.donut = donut

	go start(ctrlChannel, errorChannel, s)
	return ctrlChannel, errorChannel, s
}

func start(ctrlChannel <-chan string, errorChannel chan<- error, s *donutDriver) {
	close(errorChannel)
}

// byBucketName is a type for sorting bucket metadata by bucket name
type byBucketName []drivers.BucketMetadata

func (b byBucketName) Len() int           { return len(b) }
func (b byBucketName) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byBucketName) Less(i, j int) bool { return b[i].Name < b[j].Name }

// ListBuckets returns a list of buckets
func (d donutDriver) ListBuckets() (results []drivers.BucketMetadata, err error) {
	buckets, err := d.donut.ListBuckets()
	if err != nil {
		return nil, err
	}
	for name := range buckets {
		result := drivers.BucketMetadata{
			Name: name,
			// TODO Add real created date
			Created: time.Now(),
		}
		results = append(results, result)
	}
	sort.Sort(byBucketName(results))
	return results, nil
}

// CreateBucket creates a new bucket
func (d donutDriver) CreateBucket(bucketName string) error {
	if drivers.IsValidBucket(bucketName) && !strings.Contains(bucketName, ".") {
		return d.donut.MakeBucket(bucketName)
	}
	return errors.New("Invalid bucket")
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
func (d donutDriver) GetObject(target io.Writer, bucketName, objectName string) (int64, error) {
	errParams := map[string]string{
		"bucketName": bucketName,
		"objectName": objectName,
	}
	if bucketName == "" || strings.TrimSpace(bucketName) == "" {
		return 0, iodine.New(errors.New("invalid argument"), errParams)
	}
	if objectName == "" || strings.TrimSpace(objectName) == "" {
		return 0, iodine.New(errors.New("invalid argument"), errParams)
	}
	buckets, err := d.donut.ListBuckets()
	if err != nil {
		return 0, iodine.New(err, nil)
	}
	if _, ok := buckets[bucketName]; !ok {
		return 0, drivers.BucketNotFound{Bucket: bucketName}
	}
	reader, size, err := buckets[bucketName].GetObject(objectName)
	if err != nil {
		return 0, drivers.ObjectNotFound{
			Bucket: bucketName,
			Object: objectName,
		}
	}
	n, err := io.CopyN(target, reader, size)
	return n, iodine.New(err, errParams)
}

// GetPartialObject retrieves an object range and writes it to a writer
func (d donutDriver) GetPartialObject(w io.Writer, bucketName, objectName string, start, length int64) (int64, error) {
	// TODO more efficient get partial object with proper donut support
	errParams := map[string]string{
		"bucketName": bucketName,
		"objectName": objectName,
		"start":      strconv.FormatInt(start, 10),
		"length":     strconv.FormatInt(length, 10),
	}

	if bucketName == "" || strings.TrimSpace(bucketName) == "" {
		return 0, iodine.New(errors.New("invalid argument"), errParams)
	}
	if objectName == "" || strings.TrimSpace(objectName) == "" {
		return 0, iodine.New(errors.New("invalid argument"), errParams)
	}
	if start < 0 {
		return 0, iodine.New(errors.New("invalid argument"), errParams)
	}
	buckets, err := d.donut.ListBuckets()
	if err != nil {
		return 0, iodine.New(err, nil)
	}
	if _, ok := buckets[bucketName]; !ok {
		return 0, drivers.BucketNotFound{Bucket: bucketName}
	}
	reader, size, err := buckets[bucketName].GetObject(objectName)
	defer reader.Close()
	if err != nil {
		return 0, drivers.ObjectNotFound{
			Bucket: bucketName,
			Object: objectName,
		}
	}
	if start > size || (start+length-1) > size {
		return 0, iodine.New(errors.New("invalid range"), errParams)
	}
	_, err = io.CopyN(ioutil.Discard, reader, start)
	if err != nil {
		return 0, iodine.New(err, errParams)
	}
	n, err := io.CopyN(w, reader, length)
	return n, iodine.New(err, errParams)
}

// GetObjectMetadata retrieves an object's metadata
func (d donutDriver) GetObjectMetadata(bucketName, objectName, prefixName string) (drivers.ObjectMetadata, error) {
	errParams := map[string]string{
		"bucketName": bucketName,
		"objectName": objectName,
		"prefixName": prefixName,
	}
	buckets, err := d.donut.ListBuckets()
	if err != nil {
		return drivers.ObjectMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := buckets[bucketName]; !ok {
		return drivers.ObjectMetadata{}, drivers.BucketNotFound{Bucket: bucketName}
	}
	objectList, err := buckets[bucketName].ListObjects()
	if err != nil {
		return drivers.ObjectMetadata{}, iodine.New(err, errParams)
	}
	object, ok := objectList[objectName]
	if !ok {
		// return ObjectNotFound quickly on an error, API needs this to handle invalid requests
		return drivers.ObjectMetadata{}, drivers.ObjectNotFound{
			Bucket: bucketName,
			Object: objectName,
		}
	}
	donutObjectMetadata, err := object.GetDonutObjectMetadata()
	if err != nil {
		// return ObjectNotFound quickly on an error, API needs this to handle invalid requests
		return drivers.ObjectMetadata{}, drivers.ObjectNotFound{
			Bucket: bucketName,
			Object: objectName,
		}
	}
	objectMetadata, err := object.GetObjectMetadata()
	if err != nil {
		// return ObjectNotFound quickly on an error, API needs this to handle invalid requests
		return drivers.ObjectMetadata{}, drivers.ObjectNotFound{
			Bucket: bucketName,
			Object: objectName,
		}
	}
	created, err := time.Parse(time.RFC3339Nano, donutObjectMetadata["created"])
	if err != nil {
		return drivers.ObjectMetadata{}, iodine.New(err, nil)
	}
	size, err := strconv.ParseInt(donutObjectMetadata["size"], 10, 64)
	if err != nil {
		return drivers.ObjectMetadata{}, iodine.New(err, nil)
	}
	driversObjectMetadata := drivers.ObjectMetadata{
		Bucket: bucketName,
		Key:    objectName,

		ContentType: objectMetadata["contentType"],
		Created:     created,
		Md5:         donutObjectMetadata["md5"],
		Size:        size,
	}
	return driversObjectMetadata, nil
}

type byObjectKey []drivers.ObjectMetadata

func (b byObjectKey) Len() int           { return len(b) }
func (b byObjectKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byObjectKey) Less(i, j int) bool { return b[i].Key < b[j].Key }

// ListObjects - returns list of objects
func (d donutDriver) ListObjects(bucketName string, resources drivers.BucketResourcesMetadata) ([]drivers.ObjectMetadata, drivers.BucketResourcesMetadata, error) {
	errParams := map[string]string{
		"bucketName": bucketName,
	}
	buckets, err := d.donut.ListBuckets()
	if err != nil {
		return nil, drivers.BucketResourcesMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := buckets[bucketName]; !ok {
		return nil, drivers.BucketResourcesMetadata{}, drivers.BucketNotFound{Bucket: bucketName}
	}
	objectList, err := buckets[bucketName].ListObjects()
	if err != nil {
		return nil, drivers.BucketResourcesMetadata{}, iodine.New(err, errParams)
	}
	var objects []string
	for key := range objectList {
		objects = append(objects, key)
	}
	sort.Strings(objects)

	if resources.Maxkeys <= 0 || resources.Maxkeys > 1000 {
		resources.Maxkeys = 1000
	}
	// Populate filtering mode
	resources.Mode = drivers.GetMode(resources)
	// filter objects based on resources.Prefix and resources.Delimiter
	actualObjects, commonPrefixes := d.filter(objects, resources)
	resources.CommonPrefixes = commonPrefixes

	var results []drivers.ObjectMetadata
	for _, objectName := range actualObjects {
		if len(results) >= resources.Maxkeys {
			resources.IsTruncated = true
			break
		}
		if _, ok := objectList[objectName]; !ok {
			return nil, drivers.BucketResourcesMetadata{}, iodine.New(errors.New("object corrupted"), errParams)
		}
		objectMetadata, err := objectList[objectName].GetDonutObjectMetadata()
		if err != nil {
			return nil, drivers.BucketResourcesMetadata{}, iodine.New(err, errParams)
		}
		t, err := time.Parse(time.RFC3339Nano, objectMetadata["created"])
		if err != nil {
			return nil, drivers.BucketResourcesMetadata{}, iodine.New(err, nil)
		}
		size, err := strconv.ParseInt(objectMetadata["size"], 10, 64)
		if err != nil {
			return nil, drivers.BucketResourcesMetadata{}, iodine.New(err, nil)
		}
		metadata := drivers.ObjectMetadata{
			Key:     objectName,
			Created: t,
			Size:    size,
		}
		results = append(results, metadata)
	}
	sort.Sort(byObjectKey(results))
	return results, resources, nil
}

// CreateObject creates a new object
func (d donutDriver) CreateObject(bucketName, objectName, contentType, expectedMd5sum string, reader io.Reader) error {
	errParams := map[string]string{
		"bucketName":  bucketName,
		"objectName":  objectName,
		"contentType": contentType,
	}
	if bucketName == "" || strings.TrimSpace(bucketName) == "" {
		return iodine.New(errors.New("invalid argument"), errParams)
	}
	if objectName == "" || strings.TrimSpace(objectName) == "" {
		return iodine.New(errors.New("invalid argument"), errParams)
	}
	buckets, err := d.donut.ListBuckets()
	if err != nil {
		return iodine.New(err, errParams)
	}
	if _, ok := buckets[bucketName]; !ok {
		return drivers.BucketNotFound{Bucket: bucketName}
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentType = strings.TrimSpace(contentType)
	err = buckets[bucketName].PutObject(objectName, contentType, reader)
	if err != nil {
		return iodine.New(err, errParams)
	}
	// handle expectedMd5sum
	return nil
}
