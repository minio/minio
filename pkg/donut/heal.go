/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"path/filepath"

	"github.com/minio/minio/pkg/donut/disk"
	"github.com/minio/minio/pkg/probe"
)

// healBuckets heal bucket slices
func (donut API) healBuckets() *probe.Error {
	if err := donut.listDonutBuckets(); err != nil {
		return err.Trace()
	}
	bucketMetadata, err := donut.getDonutBucketMetadata()
	if err != nil {
		return err.Trace()
	}
	disks := make(map[int]disk.Disk)
	for _, node := range donut.nodes {
		nDisks, err := node.ListDisks()
		if err != nil {
			return err.Trace()
		}
		for k, v := range nDisks {
			disks[k] = v
		}
	}
	for order, disk := range disks {
		if disk.IsUsable() {
			disk.MakeDir(donut.config.DonutName)
			bucketMetadataWriter, err := disk.CreateFile(filepath.Join(donut.config.DonutName, bucketMetadataConfig))
			if err != nil {
				return err.Trace()
			}
			defer bucketMetadataWriter.Close()
			jenc := json.NewEncoder(bucketMetadataWriter)
			if err := jenc.Encode(bucketMetadata); err != nil {
				return probe.NewError(err)
			}
			for bucket := range bucketMetadata.Buckets {
				bucketSlice := fmt.Sprintf("%s$0$%d", bucket, order) // TODO handle node slices
				err := disk.MakeDir(filepath.Join(donut.config.DonutName, bucketSlice))
				if err != nil {
					return err.Trace()
				}
			}
		}
	}
	return nil
}
