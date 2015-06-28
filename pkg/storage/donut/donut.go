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
	"sync"

	"github.com/minio/minio/pkg/iodine"
)

// donut struct internal data
type donut struct {
	name    string
	buckets map[string]bucket
	nodes   map[string]node
	lock    *sync.RWMutex
}

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

// attachDonutNode - wrapper function to instantiate a new node for associatedt donut
// based on the provided configuration
func (dt donut) attachDonutNode(hostname string, disks []string) error {
	if err := dt.AttachNode(hostname, disks); err != nil {
		return iodine.New(err, nil)
	}
	return nil
}

// NewDonut - instantiate a new donut
func NewDonut(donutName string, nodeDiskMap map[string][]string) (Donut, error) {
	if donutName == "" || len(nodeDiskMap) == 0 {
		return nil, iodine.New(InvalidArgument{}, nil)
	}
	nodes := make(map[string]node)
	buckets := make(map[string]bucket)
	d := donut{
		name:    donutName,
		nodes:   nodes,
		buckets: buckets,
		lock:    new(sync.RWMutex),
	}
	for k, v := range nodeDiskMap {
		if len(v) == 0 {
			return nil, iodine.New(InvalidDisksArgument{}, nil)
		}
		err := d.attachDonutNode(k, v)
		if err != nil {
			return nil, iodine.New(err, nil)
		}
	}
	return d, nil
}

// MakeBucket - make a new bucket
func (dt donut) MakeBucket(bucket, acl string) error {
	dt.lock.Lock()
	defer dt.lock.Unlock()
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return iodine.New(InvalidArgument{}, nil)
	}
	return dt.makeDonutBucket(bucket, acl)
}

// GetBucketMetadata - get bucket metadata
func (dt donut) GetBucketMetadata(bucketName string) (BucketMetadata, error) {
	dt.lock.RLock()
	defer dt.lock.RUnlock()
	if err := dt.listDonutBuckets(); err != nil {
		return BucketMetadata{}, iodine.New(err, nil)
	}
	if _, ok := dt.buckets[bucketName]; !ok {
		return BucketMetadata{}, iodine.New(BucketNotFound{Bucket: bucketName}, nil)
	}
	metadata, err := dt.getDonutBucketMetadata()
	if err != nil {
		return BucketMetadata{}, iodine.New(err, nil)
	}
	return metadata.Buckets[bucketName], nil
}

// SetBucketMetadata - set bucket metadata
func (dt donut) SetBucketMetadata(bucketName string, bucketMetadata map[string]string) error {
	dt.lock.Lock()
	defer dt.lock.Unlock()
	if err := dt.listDonutBuckets(); err != nil {
		return iodine.New(err, nil)
	}
	metadata, err := dt.getDonutBucketMetadata()
	if err != nil {
		return iodine.New(err, nil)
	}
	oldBucketMetadata := metadata.Buckets[bucketName]
	acl, ok := bucketMetadata["acl"]
	if !ok {
		return iodine.New(InvalidArgument{}, nil)
	}
	oldBucketMetadata.ACL = acl
	metadata.Buckets[bucketName] = oldBucketMetadata
	return dt.setDonutBucketMetadata(metadata)
}

// ListBuckets - return list of buckets
func (dt donut) ListBuckets() (map[string]BucketMetadata, error) {
	dt.lock.RLock()
	defer dt.lock.RUnlock()
	if err := dt.listDonutBuckets(); err != nil {
		return nil, iodine.New(err, nil)
	}
	metadata, err := dt.getDonutBucketMetadata()
	if err != nil {
		// intentionally left out the error when Donut is empty
		// but we need to revisit this area in future - since we need
		// to figure out between acceptable and unacceptable errors
		return make(map[string]BucketMetadata), nil
	}
	return metadata.Buckets, nil
}

// ListObjects - return list of objects
func (dt donut) ListObjects(bucket, prefix, marker, delimiter string, maxkeys int) (ListObjects, error) {
	dt.lock.RLock()
	defer dt.lock.RUnlock()
	errParams := map[string]string{
		"bucket":    bucket,
		"prefix":    prefix,
		"marker":    marker,
		"delimiter": delimiter,
		"maxkeys":   strconv.Itoa(maxkeys),
	}
	if err := dt.listDonutBuckets(); err != nil {
		return ListObjects{}, iodine.New(err, errParams)
	}
	if _, ok := dt.buckets[bucket]; !ok {
		return ListObjects{}, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	listObjects, err := dt.buckets[bucket].ListObjects(prefix, marker, delimiter, maxkeys)
	if err != nil {
		return ListObjects{}, iodine.New(err, errParams)
	}
	return listObjects, nil
}

// PutObject - put object
func (dt donut) PutObject(bucket, object, expectedMD5Sum string, reader io.Reader, metadata map[string]string) (string, error) {
	dt.lock.Lock()
	defer dt.lock.Unlock()
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return "", iodine.New(InvalidArgument{}, errParams)
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return "", iodine.New(InvalidArgument{}, errParams)
	}
	if err := dt.listDonutBuckets(); err != nil {
		return "", iodine.New(err, errParams)
	}
	if _, ok := dt.buckets[bucket]; !ok {
		return "", iodine.New(BucketNotFound{Bucket: bucket}, nil)
	}
	bucketMeta, err := dt.getDonutBucketMetadata()
	if err != nil {
		return "", iodine.New(err, errParams)
	}
	if _, ok := bucketMeta.Buckets[bucket].BucketObjects[object]; ok {
		return "", iodine.New(ObjectExists{Object: object}, errParams)
	}
	md5sum, err := dt.buckets[bucket].WriteObject(object, reader, expectedMD5Sum, metadata)
	if err != nil {
		return "", iodine.New(err, errParams)
	}
	bucketMeta.Buckets[bucket].BucketObjects[object] = 1
	if err := dt.setDonutBucketMetadata(bucketMeta); err != nil {
		return "", iodine.New(err, errParams)
	}
	return md5sum, nil
}

// GetObject - get object
func (dt donut) GetObject(bucket, object string) (reader io.ReadCloser, size int64, err error) {
	dt.lock.RLock()
	defer dt.lock.RUnlock()
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
	if err := dt.listDonutBuckets(); err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	if _, ok := dt.buckets[bucket]; !ok {
		return nil, 0, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	return dt.buckets[bucket].ReadObject(object)
}

// GetObjectMetadata - get object metadata
func (dt donut) GetObjectMetadata(bucket, object string) (ObjectMetadata, error) {
	dt.lock.RLock()
	defer dt.lock.RUnlock()
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if err := dt.listDonutBuckets(); err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := dt.buckets[bucket]; !ok {
		return ObjectMetadata{}, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	bucketMeta, err := dt.getDonutBucketMetadata()
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := bucketMeta.Buckets[bucket].BucketObjects[object]; !ok {
		return ObjectMetadata{}, iodine.New(ObjectNotFound{Object: object}, errParams)
	}
	objectMetadata, err := dt.buckets[bucket].GetObjectMetadata(object)
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, nil)
	}
	return objectMetadata, nil
}

// getDiskWriters -
func (dt donut) getBucketMetadataWriters() ([]io.WriteCloser, error) {
	var writers []io.WriteCloser
	for _, node := range dt.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		writers = make([]io.WriteCloser, len(disks))
		for order, d := range disks {
			bucketMetaDataWriter, err := d.CreateFile(filepath.Join(dt.name, bucketMetadataConfig))
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			writers[order] = bucketMetaDataWriter
		}
	}
	return writers, nil
}

func (dt donut) getBucketMetadataReaders() ([]io.ReadCloser, error) {
	var readers []io.ReadCloser
	for _, node := range dt.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		readers = make([]io.ReadCloser, len(disks))
		for order, d := range disks {
			bucketMetaDataReader, err := d.OpenFile(filepath.Join(dt.name, bucketMetadataConfig))
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			readers[order] = bucketMetaDataReader
		}
	}
	return readers, nil
}

//
func (dt donut) setDonutBucketMetadata(metadata *AllBuckets) error {
	writers, err := dt.getBucketMetadataWriters()
	if err != nil {
		return iodine.New(err, nil)
	}
	for _, writer := range writers {
		defer writer.Close()
	}
	for _, writer := range writers {
		jenc := json.NewEncoder(writer)
		if err := jenc.Encode(metadata); err != nil {
			return iodine.New(err, nil)
		}
	}
	return nil
}

func (dt donut) getDonutBucketMetadata() (*AllBuckets, error) {
	metadata := new(AllBuckets)
	readers, err := dt.getBucketMetadataReaders()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	for _, reader := range readers {
		defer reader.Close()
	}
	for _, reader := range readers {
		jenc := json.NewDecoder(reader)
		if err := jenc.Decode(metadata); err != nil {
			return nil, iodine.New(err, nil)
		}
		return metadata, nil
	}
	return nil, iodine.New(InvalidArgument{}, nil)
}

func (dt donut) makeDonutBucket(bucketName, acl string) error {
	if err := dt.listDonutBuckets(); err != nil {
		return iodine.New(err, nil)
	}
	if _, ok := dt.buckets[bucketName]; ok {
		return iodine.New(BucketExists{Bucket: bucketName}, nil)
	}
	bucket, bucketMetadata, err := newBucket(bucketName, acl, dt.name, dt.nodes)
	if err != nil {
		return iodine.New(err, nil)
	}
	nodeNumber := 0
	dt.buckets[bucketName] = bucket
	for _, node := range dt.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return iodine.New(err, nil)
		}
		for order, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", bucketName, nodeNumber, order)
			err := disk.MakeDir(filepath.Join(dt.name, bucketSlice))
			if err != nil {
				return iodine.New(err, nil)
			}
		}
		nodeNumber = nodeNumber + 1
	}
	metadata, err := dt.getDonutBucketMetadata()
	if err != nil {
		if os.IsNotExist(iodine.ToError(err)) {
			metadata := new(AllBuckets)
			metadata.Buckets = make(map[string]BucketMetadata)
			metadata.Buckets[bucketName] = bucketMetadata
			err = dt.setDonutBucketMetadata(metadata)
			if err != nil {
				return iodine.New(err, nil)
			}
			return nil
		}
		return iodine.New(err, nil)
	}
	metadata.Buckets[bucketName] = bucketMetadata
	err = dt.setDonutBucketMetadata(metadata)
	if err != nil {
		return iodine.New(err, nil)
	}
	return nil
}

func (dt donut) listDonutBuckets() error {
	for _, node := range dt.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return iodine.New(err, nil)
		}
		for _, disk := range disks {
			dirs, err := disk.ListDir(dt.name)
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
				bucket, _, err := newBucket(bucketName, "private", dt.name, dt.nodes)
				if err != nil {
					return iodine.New(err, nil)
				}
				dt.buckets[bucketName] = bucket
			}
		}
	}
	return nil
}
