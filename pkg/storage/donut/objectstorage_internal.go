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
	"fmt"
	"path"
	"strings"

	"github.com/minio-io/iodine"
)

func (d donut) makeDonutBucket(bucketName string) error {
	err := d.getDonutBuckets()
	if err != nil {
		return iodine.New(err, nil)
	}
	if _, ok := d.buckets[bucketName]; ok {
		return iodine.New(errors.New("bucket exists"), nil)
	}
	bucket, err := NewBucket(bucketName, d.name, d.nodes)
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
				bucket, err := NewBucket(bucketName, d.name, d.nodes)
				if err != nil {
					return iodine.New(err, nil)
				}
				d.buckets[bucketName] = bucket
			}
		}
	}
	return nil
}
