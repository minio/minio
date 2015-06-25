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
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/storage/donut/disk"
)

// donut struct internal data
type donut struct {
	name    string
	buckets map[string]bucket
	nodes   map[string]Node
	lock    *sync.RWMutex
}

// config files used inside Donut
const (
	// donut object metadata and config
	donutObjectMetadataConfig = "donutObjectMetadata.json"
	donutConfig               = "donutMetadata.json"

	// bucket, object metadata
	bucketMetadataConfig = "bucketMetadata.json"
	objectMetadataConfig = "objectMetadata.json"

	// versions
	objectMetadataVersion      = "1.0"
	donutObjectMetadataVersion = "1.0"
)

// attachDonutNode - wrapper function to instantiate a new node for associated donut
// based on the provided configuration
func (d donut) attachDonutNode(hostname string, disks []string) error {
	node, err := NewNode(hostname)
	if err != nil {
		return iodine.New(err, nil)
	}
	donutName := d.name
	for i, d := range disks {
		// Order is necessary for maps, keep order number separately
		newDisk, err := disk.New(d)
		if err != nil {
			return iodine.New(err, nil)
		}
		if err := newDisk.MakeDir(donutName); err != nil {
			return iodine.New(err, nil)
		}
		if err := node.AttachDisk(newDisk, i); err != nil {
			return iodine.New(err, nil)
		}
	}
	if err := d.AttachNode(node); err != nil {
		return iodine.New(err, nil)
	}
	return nil
}

// NewDonut - instantiate a new donut
func NewDonut(donutName string, nodeDiskMap map[string][]string) (Donut, error) {
	if donutName == "" || len(nodeDiskMap) == 0 {
		return nil, iodine.New(InvalidArgument{}, nil)
	}
	nodes := make(map[string]Node)
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
func (d donut) MakeBucket(bucket, acl string) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return iodine.New(InvalidArgument{}, nil)
	}
	return d.makeDonutBucket(bucket, acl)
}

// GetBucketMetadata - get bucket metadata
func (d donut) GetBucketMetadata(bucket string) (map[string]string, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	err := d.getDonutBuckets()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return nil, iodine.New(BucketNotFound{Bucket: bucket}, nil)
	}
	metadata, err := d.getDonutBucketMetadata()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return metadata[bucket], nil
}

// SetBucketMetadata - set bucket metadata
func (d donut) SetBucketMetadata(bucket string, bucketMetadata map[string]string) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	err := d.getDonutBuckets()
	if err != nil {
		return iodine.New(err, nil)
	}
	metadata, err := d.getDonutBucketMetadata()
	if err != nil {
		return iodine.New(err, nil)
	}
	oldBucketMetadata := metadata[bucket]
	// TODO ignore rest of the keys for now, only mutable data is "acl"
	oldBucketMetadata["acl"] = bucketMetadata["acl"]
	metadata[bucket] = oldBucketMetadata
	return d.setDonutBucketMetadata(metadata)
}

// ListBuckets - return list of buckets
func (d donut) ListBuckets() (metadata map[string]map[string]string, err error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	err = d.getDonutBuckets()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	dummyMetadata := make(map[string]map[string]string)
	metadata, err = d.getDonutBucketMetadata()
	if err != nil {
		// intentionally left out the error when Donut is empty
		// but we need to revisit this area in future - since we need
		// to figure out between acceptable and unacceptable errors
		return dummyMetadata, nil
	}
	return metadata, nil
}

// ListObjects - return list of objects
func (d donut) ListObjects(bucket, prefix, marker, delimiter string, maxkeys int) ([]string, []string, bool, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	errParams := map[string]string{
		"bucket":    bucket,
		"prefix":    prefix,
		"marker":    marker,
		"delimiter": delimiter,
		"maxkeys":   strconv.Itoa(maxkeys),
	}
	err := d.getDonutBuckets()
	if err != nil {
		return nil, nil, false, iodine.New(err, errParams)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return nil, nil, false, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	objectList, err := d.buckets[bucket].ListObjects()
	if err != nil {
		return nil, nil, false, iodine.New(err, errParams)
	}
	var donutObjects []string
	for objectName := range objectList {
		donutObjects = append(donutObjects, objectName)
	}
	if maxkeys <= 0 {
		maxkeys = 1000
	}
	if strings.TrimSpace(prefix) != "" {
		donutObjects = filterPrefix(donutObjects, prefix)
		donutObjects = removePrefix(donutObjects, prefix)
	}

	var actualObjects []string
	var actualPrefixes []string
	var isTruncated bool
	if strings.TrimSpace(delimiter) != "" {
		actualObjects = filterDelimited(donutObjects, delimiter)
		actualPrefixes = filterNotDelimited(donutObjects, delimiter)
		actualPrefixes = extractDir(actualPrefixes, delimiter)
		actualPrefixes = uniqueObjects(actualPrefixes)
	} else {
		actualObjects = donutObjects
	}

	sort.Strings(actualObjects)
	var newActualObjects []string
	switch {
	case marker != "":
		for _, objectName := range actualObjects {
			if objectName > marker {
				newActualObjects = append(newActualObjects, objectName)
			}
		}
	default:
		newActualObjects = actualObjects
	}

	var results []string
	var commonPrefixes []string
	for _, objectName := range newActualObjects {
		if len(results) >= maxkeys {
			isTruncated = true
			break
		}
		results = appendUniq(results, prefix+objectName)
	}
	for _, commonPrefix := range actualPrefixes {
		commonPrefixes = appendUniq(commonPrefixes, prefix+commonPrefix)
	}
	sort.Strings(results)
	sort.Strings(commonPrefixes)
	return results, commonPrefixes, isTruncated, nil
}

// PutObject - put object
func (d donut) PutObject(bucket, object, expectedMD5Sum string, reader io.ReadCloser, metadata map[string]string) (string, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
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
	err := d.getDonutBuckets()
	if err != nil {
		return "", iodine.New(err, errParams)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return "", iodine.New(BucketNotFound{Bucket: bucket}, nil)
	}
	objectList, err := d.buckets[bucket].ListObjects()
	if err != nil {
		return "", iodine.New(err, nil)
	}
	for objectName := range objectList {
		if objectName == object {
			return "", iodine.New(ObjectExists{Object: object}, nil)
		}
	}
	md5sum, err := d.buckets[bucket].WriteObject(object, reader, expectedMD5Sum, metadata)
	if err != nil {
		return "", iodine.New(err, errParams)
	}
	return md5sum, nil
}

// GetObject - get object
func (d donut) GetObject(bucket, object string) (reader io.ReadCloser, size int64, err error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
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
	err = d.getDonutBuckets()
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return nil, 0, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	return d.buckets[bucket].ReadObject(object)
}

// GetObjectMetadata - get object metadata
func (d donut) GetObjectMetadata(bucket, object string) (map[string]string, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	err := d.getDonutBuckets()
	if err != nil {
		return nil, iodine.New(err, errParams)
	}
	if _, ok := d.buckets[bucket]; !ok {
		return nil, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	objectList, err := d.buckets[bucket].ListObjects()
	if err != nil {
		return nil, iodine.New(err, errParams)
	}
	donutObject, ok := objectList[object]
	if !ok {
		return nil, iodine.New(ObjectNotFound{Object: object}, errParams)
	}
	return donutObject.GetObjectMetadata()
}

// getDiskWriters -
func (d donut) getBucketMetadataWriters() ([]io.WriteCloser, error) {
	var writers []io.WriteCloser
	for _, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		writers = make([]io.WriteCloser, len(disks))
		for order, disk := range disks {
			bucketMetaDataWriter, err := disk.CreateFile(filepath.Join(d.name, bucketMetadataConfig))
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			writers[order] = bucketMetaDataWriter
		}
	}
	return writers, nil
}

func (d donut) getBucketMetadataReaders() ([]io.ReadCloser, error) {
	var readers []io.ReadCloser
	for _, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		readers = make([]io.ReadCloser, len(disks))
		for order, disk := range disks {
			bucketMetaDataReader, err := disk.OpenFile(filepath.Join(d.name, bucketMetadataConfig))
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			readers[order] = bucketMetaDataReader
		}
	}
	return readers, nil
}

//
func (d donut) setDonutBucketMetadata(metadata map[string]map[string]string) error {
	writers, err := d.getBucketMetadataWriters()
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

func (d donut) getDonutBucketMetadata() (map[string]map[string]string, error) {
	metadata := make(map[string]map[string]string)
	readers, err := d.getBucketMetadataReaders()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	for _, reader := range readers {
		defer reader.Close()
	}
	for _, reader := range readers {
		jenc := json.NewDecoder(reader)
		if err := jenc.Decode(&metadata); err != nil {
			return nil, iodine.New(err, nil)
		}
	}
	return metadata, nil
}

func (d donut) makeDonutBucket(bucketName, acl string) error {
	err := d.getDonutBuckets()
	if err != nil {
		return iodine.New(err, nil)
	}
	if _, ok := d.buckets[bucketName]; ok {
		return iodine.New(BucketExists{Bucket: bucketName}, nil)
	}
	bucket, bucketMetadata, err := newBucket(bucketName, acl, d.name, d.nodes)
	if err != nil {
		return iodine.New(err, nil)
	}
	nodeNumber := 0
	d.buckets[bucketName] = bucket
	for _, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return iodine.New(err, nil)
		}
		for order, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", bucketName, nodeNumber, order)
			err := disk.MakeDir(filepath.Join(d.name, bucketSlice))
			if err != nil {
				return iodine.New(err, nil)
			}
		}
		nodeNumber = nodeNumber + 1
	}
	metadata, err := d.getDonutBucketMetadata()
	if err != nil {
		err = iodine.ToError(err)
		if os.IsNotExist(err) {
			metadata := make(map[string]map[string]string)
			metadata[bucketName] = bucketMetadata
			err = d.setDonutBucketMetadata(metadata)
			if err != nil {
				return iodine.New(err, nil)
			}
			return nil
		}
		return iodine.New(err, nil)
	}
	metadata[bucketName] = bucketMetadata
	err = d.setDonutBucketMetadata(metadata)
	if err != nil {
		return iodine.New(err, nil)
	}
	return nil
}

func (d donut) getDonutBuckets() error {
	for _, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return iodine.New(err, nil)
		}
		for _, disk := range disks {
			dirs, err := disk.ListDir(d.name)
			if err != nil {
				return iodine.New(err, nil)
			}
			for _, dir := range dirs {
				splitDir := strings.Split(dir.Name(), "$")
				if len(splitDir) < 3 {
					return iodine.New(CorruptedBackend{Backend: dir.Name()}, nil)
				}
				bucketName := splitDir[0]
				// we dont need this NewBucket once we cache from makeDonutBucket()
				bucket, _, err := newBucket(bucketName, "private", d.name, d.nodes)
				if err != nil {
					return iodine.New(err, nil)
				}
				d.buckets[bucketName] = bucket
			}
		}
	}
	return nil
}
