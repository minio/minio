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

	"github.com/minio-io/iodine"
)

type donut struct {
	name    string
	buckets map[string]Bucket
	nodes   map[string]Node
}

const (
	donutObjectMetadataConfig = "donutObjectMetadata.json"
	objectMetadataConfig      = "objectMetadata.json"
	donutConfig               = "donutMetadata.json"
)

// attachDonutNode - wrapper function to instantiate a new node for associated donut
// based on the configuration
func (d donut) attachDonutNode(hostname string, disks []string) error {
	node, err := NewNode(hostname)
	if err != nil {
		return iodine.New(err, nil)
	}
	for i, disk := range disks {
		// Order is necessary for maps, keep order number separately
		newDisk, err := NewDisk(disk, i)
		if err != nil {
			return iodine.New(err, nil)
		}
		if err := newDisk.MakeDir(d.name); err != nil {
			return iodine.New(err, nil)
		}
		if err := node.AttachDisk(newDisk); err != nil {
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
		return nil, iodine.New(errors.New("invalid argument"), nil)
	}
	nodes := make(map[string]Node)
	buckets := make(map[string]Bucket)
	d := donut{
		name:    donutName,
		nodes:   nodes,
		buckets: buckets,
	}
	for k, v := range nodeDiskMap {
		if len(v) == 0 {
			return nil, iodine.New(errors.New("invalid number of disks per node"), nil)
		}
		err := d.attachDonutNode(k, v)
		if err != nil {
			return nil, iodine.New(err, nil)
		}
	}
	return d, nil
}
