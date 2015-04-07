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

import "errors"

type donut struct {
	name    string
	buckets map[string]Bucket
	nodes   map[string]Node
}

// attachDonutNode - wrapper function to instantiate a new node for associated donut
// based on the configuration
func (d donut) attachDonutNode(hostname string, disks []string) error {
	node, err := NewNode(hostname)
	if err != nil {
		return err
	}
	for i, disk := range disks {
		// Order is necessary for maps, keep order number separately
		newDisk, err := NewDisk(disk, i)
		if err != nil {
			return err
		}
		if err := newDisk.MakeDir(d.name); err != nil {
			return err
		}
		if err := node.AttachDisk(newDisk); err != nil {
			return err
		}
	}
	if err := d.AttachNode(node); err != nil {
		return err
	}
	return nil
}

// NewDonut - instantiate a new donut
func NewDonut(donutName string, nodeDiskMap map[string][]string) (Donut, error) {
	if donutName == "" || len(nodeDiskMap) == 0 {
		return nil, errors.New("invalid argument")
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
			return nil, errors.New("invalid number of disks per node")
		}
		err := d.attachDonutNode(k, v)
		if err != nil {
			return nil, err
		}
	}
	return d, nil
}
