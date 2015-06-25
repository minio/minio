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
	"strings"

	"github.com/minio/minio/pkg/iodine"
)

/// This file contains all the internal functions used by Object interface

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
	bucket, bucketMetadata, err := NewBucket(bucketName, acl, d.name, d.nodes)
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
				bucket, _, err := NewBucket(bucketName, "private", d.name, d.nodes)
				if err != nil {
					return iodine.New(err, nil)
				}
				d.buckets[bucketName] = bucket
			}
		}
	}
	return nil
}
