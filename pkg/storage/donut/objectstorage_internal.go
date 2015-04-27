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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/minio-io/minio/pkg/iodine"
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
		for _, disk := range disks {
			bucketMetaDataWriter, err := disk.MakeFile(path.Join(d.name, bucketMetadataConfig))
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			writers[disk.GetOrder()] = bucketMetaDataWriter
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
		for _, disk := range disks {
			bucketMetaDataReader, err := disk.OpenFile(path.Join(d.name, bucketMetadataConfig))
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			readers[disk.GetOrder()] = bucketMetaDataReader
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
		return iodine.New(errors.New("bucket exists"), nil)
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
		for _, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", bucketName, nodeNumber, disk.GetOrder())
			err := disk.MakeDir(path.Join(d.name, bucketSlice))
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
		} else {
			return iodine.New(err, nil)
		}
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
					return iodine.New(errors.New("corrupted backend"), nil)
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
