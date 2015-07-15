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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/minio/minio/pkg/donut/disk"
	"github.com/minio/minio/pkg/iodine"
)

// config files used inside Donut
const (
	// donut system config
	donutConfig = "donutConfig.json"

	// bucket, object metadata
	bucketMetadataConfig = "bucketMetadata.json"
	objectMetadataConfig = "objectMetadata.json"

	// versions
	objectMetadataVersion = "1.0.0"
	bucketMetadataVersion = "1.0.0"
)

/// v1 API functions

// makeBucket - make a new bucket
func (donut API) makeBucket(bucket string, acl BucketACL) error {
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return iodine.New(InvalidArgument{}, nil)
	}
	return donut.makeDonutBucket(bucket, acl.String())
}

// getBucketMetadata - get bucket metadata
func (donut API) getBucketMetadata(bucketName string) (BucketMetadata, error) {
	if err := donut.listDonutBuckets(); err != nil {
		return BucketMetadata{}, iodine.New(err, nil)
	}
	if _, ok := donut.buckets[bucketName]; !ok {
		return BucketMetadata{}, iodine.New(BucketNotFound{Bucket: bucketName}, nil)
	}
	metadata, err := donut.getDonutBucketMetadata()
	if err != nil {
		return BucketMetadata{}, iodine.New(err, nil)
	}
	return metadata.Buckets[bucketName], nil
}

// setBucketMetadata - set bucket metadata
func (donut API) setBucketMetadata(bucketName string, bucketMetadata map[string]string) error {
	if err := donut.listDonutBuckets(); err != nil {
		return iodine.New(err, nil)
	}
	metadata, err := donut.getDonutBucketMetadata()
	if err != nil {
		return iodine.New(err, nil)
	}
	oldBucketMetadata := metadata.Buckets[bucketName]
	acl, ok := bucketMetadata["acl"]
	if !ok {
		return iodine.New(InvalidArgument{}, nil)
	}
	oldBucketMetadata.ACL = BucketACL(acl)
	metadata.Buckets[bucketName] = oldBucketMetadata
	return donut.setDonutBucketMetadata(metadata)
}

// listBuckets - return list of buckets
func (donut API) listBuckets() (map[string]BucketMetadata, error) {
	if err := donut.listDonutBuckets(); err != nil {
		return nil, iodine.New(err, nil)
	}
	metadata, err := donut.getDonutBucketMetadata()
	if err != nil {
		// intentionally left out the error when Donut is empty
		// but we need to revisit this area in future - since we need
		// to figure out between acceptable and unacceptable errors
		return make(map[string]BucketMetadata), nil
	}
	if metadata == nil {
		return make(map[string]BucketMetadata), nil
	}
	return metadata.Buckets, nil
}

// listObjects - return list of objects
func (donut API) listObjects(bucket, prefix, marker, delimiter string, maxkeys int) (ListObjectsResults, error) {
	errParams := map[string]string{
		"bucket":    bucket,
		"prefix":    prefix,
		"marker":    marker,
		"delimiter": delimiter,
		"maxkeys":   strconv.Itoa(maxkeys),
	}
	if err := donut.listDonutBuckets(); err != nil {
		return ListObjectsResults{}, iodine.New(err, errParams)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return ListObjectsResults{}, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	listObjects, err := donut.buckets[bucket].ListObjects(prefix, marker, delimiter, maxkeys)
	if err != nil {
		return ListObjectsResults{}, iodine.New(err, errParams)
	}
	return listObjects, nil
}

// putObject - put object
func (donut API) putObject(bucket, object, expectedMD5Sum string, reader io.Reader, metadata map[string]string, signature *Signature) (ObjectMetadata, error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return ObjectMetadata{}, iodine.New(InvalidArgument{}, errParams)
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return ObjectMetadata{}, iodine.New(InvalidArgument{}, errParams)
	}
	if err := donut.listDonutBuckets(); err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return ObjectMetadata{}, iodine.New(BucketNotFound{Bucket: bucket}, nil)
	}
	bucketMeta, err := donut.getDonutBucketMetadata()
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := bucketMeta.Buckets[bucket].BucketObjects[object]; ok {
		return ObjectMetadata{}, iodine.New(ObjectExists{Object: object}, errParams)
	}
	objMetadata, err := donut.buckets[bucket].WriteObject(object, reader, expectedMD5Sum, metadata, signature)
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	bucketMeta.Buckets[bucket].BucketObjects[object] = struct{}{}
	if err := donut.setDonutBucketMetadata(bucketMeta); err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	return objMetadata, nil
}

// getObject - get object
func (donut API) getObject(bucket, object string) (reader io.ReadCloser, size int64, err error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return nil, 0, iodine.New(InvalidArgument{}, errParams)
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return nil, 0, iodine.New(InvalidArgument{}, errParams)
	}
	if err := donut.listDonutBuckets(); err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return nil, 0, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	return donut.buckets[bucket].ReadObject(object)
}

// getObjectMetadata - get object metadata
func (donut API) getObjectMetadata(bucket, object string) (ObjectMetadata, error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if err := donut.listDonutBuckets(); err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return ObjectMetadata{}, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	bucketMeta, err := donut.getDonutBucketMetadata()
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := bucketMeta.Buckets[bucket].BucketObjects[object]; !ok {
		return ObjectMetadata{}, iodine.New(ObjectNotFound{Object: object}, errParams)
	}
	objectMetadata, err := donut.buckets[bucket].GetObjectMetadata(object)
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, nil)
	}
	return objectMetadata, nil
}

//// internal functions

// getBucketMetadataWriters -
func (donut API) getBucketMetadataWriters() ([]io.WriteCloser, error) {
	var writers []io.WriteCloser
	for _, node := range donut.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		writers = make([]io.WriteCloser, len(disks))
		for order, dd := range disks {
			bucketMetaDataWriter, err := dd.CreateFile(filepath.Join(donut.config.DonutName, bucketMetadataConfig))
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			writers[order] = bucketMetaDataWriter
		}
	}
	return writers, nil
}

// getBucketMetadataReaders - readers are returned in map rather than slice
func (donut API) getBucketMetadataReaders() (map[int]io.ReadCloser, error) {
	readers := make(map[int]io.ReadCloser)
	var disks map[int]disk.Disk
	var err error
	for _, node := range donut.nodes {
		disks, err = node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
	}
	var bucketMetaDataReader io.ReadCloser
	for order, dsk := range disks {
		bucketMetaDataReader, err = dsk.OpenFile(filepath.Join(donut.config.DonutName, bucketMetadataConfig))
		if err != nil {
			continue
		}
		readers[order] = bucketMetaDataReader
	}
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return readers, nil
}

// setDonutBucketMetadata -
func (donut API) setDonutBucketMetadata(metadata *AllBuckets) error {
	writers, err := donut.getBucketMetadataWriters()
	if err != nil {
		return iodine.New(err, nil)
	}
	for _, writer := range writers {
		jenc := json.NewEncoder(writer)
		if err := jenc.Encode(metadata); err != nil {
			CleanupWritersOnError(writers)
			return iodine.New(err, nil)
		}
	}
	for _, writer := range writers {
		writer.Close()
	}
	return nil
}

// getDonutBucketMetadata -
func (donut API) getDonutBucketMetadata() (*AllBuckets, error) {
	metadata := &AllBuckets{}
	var err error
	readers, err := donut.getBucketMetadataReaders()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	for _, reader := range readers {
		defer reader.Close()
	}
	for _, reader := range readers {
		jenc := json.NewDecoder(reader)
		if err = jenc.Decode(metadata); err == nil {
			return metadata, nil
		}
	}
	return nil, iodine.New(err, nil)
}

// makeDonutBucket -
func (donut API) makeDonutBucket(bucketName, acl string) error {
	if err := donut.listDonutBuckets(); err != nil {
		return iodine.New(err, nil)
	}
	if _, ok := donut.buckets[bucketName]; ok {
		return iodine.New(BucketExists{Bucket: bucketName}, nil)
	}
	bucket, bucketMetadata, err := newBucket(bucketName, acl, donut.config.DonutName, donut.nodes)
	if err != nil {
		return iodine.New(err, nil)
	}
	nodeNumber := 0
	donut.buckets[bucketName] = bucket
	for _, node := range donut.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return iodine.New(err, nil)
		}
		for order, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", bucketName, nodeNumber, order)
			err := disk.MakeDir(filepath.Join(donut.config.DonutName, bucketSlice))
			if err != nil {
				return iodine.New(err, nil)
			}
		}
		nodeNumber = nodeNumber + 1
	}
	metadata, err := donut.getDonutBucketMetadata()
	if err != nil {
		if os.IsNotExist(iodine.ToError(err)) {
			metadata := new(AllBuckets)
			metadata.Buckets = make(map[string]BucketMetadata)
			metadata.Buckets[bucketName] = bucketMetadata
			err = donut.setDonutBucketMetadata(metadata)
			if err != nil {
				return iodine.New(err, nil)
			}
			return nil
		}
		return iodine.New(err, nil)
	}
	metadata.Buckets[bucketName] = bucketMetadata
	err = donut.setDonutBucketMetadata(metadata)
	if err != nil {
		return iodine.New(err, nil)
	}
	return nil
}

// listDonutBuckets -
func (donut API) listDonutBuckets() error {
	var disks map[int]disk.Disk
	var err error
	for _, node := range donut.nodes {
		disks, err = node.ListDisks()
		if err != nil {
			return iodine.New(err, nil)
		}
	}
	var dirs []os.FileInfo
	for _, disk := range disks {
		dirs, err = disk.ListDir(donut.config.DonutName)
		if err == nil {
			break
		}
	}
	// if all disks are missing then return error
	if err != nil {
		return iodine.New(err, nil)
	}
	for _, dir := range dirs {
		splitDir := strings.Split(dir.Name(), "$")
		if len(splitDir) < 3 {
			return iodine.New(CorruptedBackend{Backend: dir.Name()}, nil)
		}
		bucketName := splitDir[0]
		// we dont need this once we cache from makeDonutBucket()
		bucket, _, err := newBucket(bucketName, "private", donut.config.DonutName, donut.nodes)
		if err != nil {
			return iodine.New(err, nil)
		}
		donut.buckets[bucketName] = bucket
	}
	return nil
}
