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
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"io/ioutil"

	"github.com/minio-io/minio/pkg/iodine"
	"github.com/minio-io/minio/pkg/storage/donut"
	"github.com/minio-io/minio/pkg/storage/drivers"
	"github.com/minio-io/minio/pkg/utils/log"
)

// donutDriver - creates a new single disk drivers driver using donut
type donutDriver struct {
	donut donut.Donut
	path  string
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
	s.path = path

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
	for _, name := range buckets {
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
func (d donutDriver) CreateBucket(bucketName, acl string) error {
	if !drivers.IsValidBucketACL(acl) {
		return iodine.New(drivers.InvalidACL{ACL: acl}, nil)
	}
	if drivers.IsValidBucket(bucketName) && !strings.Contains(bucketName, ".") {
		return d.donut.MakeBucket(bucketName, acl)
	}
	return iodine.New(drivers.BucketNameInvalid{Bucket: bucketName}, nil)
}

// GetBucketMetadata retrieves an bucket's metadata
func (d donutDriver) GetBucketMetadata(bucketName string) (drivers.BucketMetadata, error) {
	if !drivers.IsValidBucket(bucketName) || strings.Contains(bucketName, ".") {
		return drivers.BucketMetadata{}, drivers.BucketNameInvalid{Bucket: bucketName}
	}
	metadata, err := d.donut.GetBucketMetadata(bucketName)
	if err != nil {
		return drivers.BucketMetadata{}, drivers.BucketNotFound{Bucket: bucketName}
	}
	created, err := time.Parse(time.RFC3339Nano, metadata["created"])
	if err != nil {
		return drivers.BucketMetadata{}, iodine.New(err, nil)
	}
	acl, ok := metadata["acl"]
	if !ok {
		return drivers.BucketMetadata{}, iodine.New(drivers.BackendCorrupted{Path: d.path}, nil)
	}
	bucketMetadata := drivers.BucketMetadata{
		Name:    metadata["name"],
		Created: created,
		ACL:     drivers.BucketACL(acl),
	}
	return bucketMetadata, nil
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
	reader, size, err := d.donut.GetObject(bucketName, objectName)
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
	reader, size, err := d.donut.GetObject(bucketName, objectName)
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
	if err != nil {
		return 0, iodine.New(err, errParams)
	}
	return n, nil
}

// GetObjectMetadata retrieves an object's metadata
func (d donutDriver) GetObjectMetadata(bucketName, objectName, prefixName string) (drivers.ObjectMetadata, error) {
	errParams := map[string]string{
		"bucketName": bucketName,
		"objectName": objectName,
		"prefixName": prefixName,
	}
	metadata, err := d.donut.GetObjectMetadata(bucketName, objectName)
	if err != nil {
		return drivers.ObjectMetadata{}, drivers.ObjectNotFound{
			Bucket: bucketName,
			Object: objectName,
		}
	}
	created, err := time.Parse(time.RFC3339Nano, metadata["created"])
	if err != nil {
		return drivers.ObjectMetadata{}, iodine.New(err, errParams)
	}
	size, err := strconv.ParseInt(metadata["size"], 10, 64)
	if err != nil {
		return drivers.ObjectMetadata{}, iodine.New(err, errParams)
	}
	objectMetadata := drivers.ObjectMetadata{
		Bucket: bucketName,
		Key:    objectName,

		ContentType: metadata["contentType"],
		Created:     created,
		Md5:         metadata["md5"],
		Size:        size,
	}
	return objectMetadata, nil
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
	actualObjects, commonPrefixes, isTruncated, err := d.donut.ListObjects(bucketName,
		resources.Prefix,
		resources.Marker,
		resources.Delimiter,
		resources.Maxkeys)
	if err != nil {
		return nil, drivers.BucketResourcesMetadata{}, iodine.New(err, errParams)
	}
	resources.CommonPrefixes = commonPrefixes
	resources.IsTruncated = isTruncated

	var results []drivers.ObjectMetadata
	for _, objectName := range actualObjects {
		objectMetadata, err := d.donut.GetObjectMetadata(bucketName, objectName)
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
func (d donutDriver) CreateObject(bucketName, objectName, contentType, expectedMD5Sum string, reader io.Reader) error {
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
	if strings.TrimSpace(contentType) == "" {
		contentType = "application/octet-stream"
	}
	metadata := make(map[string]string)
	metadata["contentType"] = strings.TrimSpace(contentType)

	if strings.TrimSpace(expectedMD5Sum) != "" {
		expectedMD5SumBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(expectedMD5Sum))
		if err != nil {
			return iodine.New(err, nil)
		}
		expectedMD5Sum = hex.EncodeToString(expectedMD5SumBytes)
	}

	err := d.donut.PutObject(bucketName, objectName, expectedMD5Sum, ioutil.NopCloser(reader), metadata)
	if err != nil {
		return iodine.New(err, errParams)
	}
	return nil
}
