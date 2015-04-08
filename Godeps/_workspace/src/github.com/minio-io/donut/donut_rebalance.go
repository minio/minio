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
	"fmt"
	"os"
	"strings"

	"github.com/minio-io/iodine"
)

// Rebalance -
func (d donut) Rebalance() error {
	var totalOffSetLength int
	var newDisks []Disk
	var existingDirs []os.FileInfo
	for _, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return iodine.New(err, nil)
		}
		totalOffSetLength = len(disks)
		fmt.Println(totalOffSetLength)
		for _, disk := range disks {
			dirs, err := disk.ListDir(d.name)
			if err != nil {
				return iodine.New(err, nil)
			}
			if len(dirs) == 0 {
				newDisks = append(newDisks, disk)
			}
			existingDirs = append(existingDirs, dirs...)
		}
	}
	for _, dir := range existingDirs {
		splits := strings.Split(dir.Name(), "$")
		bucketName, segment, offset := splits[0], splits[1], splits[2]
		fmt.Println(bucketName, segment, offset)
	}
	return nil
}
