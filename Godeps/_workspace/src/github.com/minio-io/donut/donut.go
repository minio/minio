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
	"path"
	"strings"
)

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

func (d donut) MakeBucket(bucketName string) error {
	if bucketName == "" || strings.TrimSpace(bucketName) == "" {
		return errors.New("invalid argument")
	}
	if _, ok := d.buckets[bucketName]; ok {
		return errors.New("bucket exists")
	}
	bucket, err := NewBucket(bucketName, d.name, d.nodes)
	if err != nil {
		return err
	}
	nodeNumber := 0
	d.buckets[bucketName] = bucket
	for _, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return err
		}
		for _, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", bucketName, nodeNumber, disk.GetOrder())
			err := disk.MakeDir(path.Join(d.name, bucketSlice))
			if err != nil {
				return err
			}
		}
		nodeNumber = nodeNumber + 1
	}
	return nil
}

func (d donut) ListBuckets() (map[string]Bucket, error) {
	for _, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, err
		}
		for _, disk := range disks {
			dirs, err := disk.ListDir(d.name)
			if err != nil {
				return nil, err
			}
			for _, dir := range dirs {
				splitDir := strings.Split(dir.Name(), "$")
				if len(splitDir) < 3 {
					return nil, errors.New("corrupted backend")
				}
				bucketName := splitDir[0]
				// we dont need this NewBucket once we cache these
				bucket, err := NewBucket(bucketName, d.name, d.nodes)
				if err != nil {
					return nil, err
				}
				d.buckets[bucketName] = bucket
			}
		}
	}
	return d.buckets, nil
}

func (d donut) Heal() error {
	return errors.New("Not Implemented")
}

func (d donut) Info() (nodeDiskMap map[string][]string, err error) {
	nodeDiskMap = make(map[string][]string)
	for nodeName, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, err
		}
		diskList := make([]string, len(disks))
		for diskName, disk := range disks {
			diskList[disk.GetOrder()] = diskName
		}
		nodeDiskMap[nodeName] = diskList
	}
	return nodeDiskMap, nil
}

func (d donut) AttachNode(node Node) error {
	if node == nil {
		return errors.New("invalid argument")
	}
	d.nodes[node.GetNodeName()] = node
	return nil
}
func (d donut) DetachNode(node Node) error {
	delete(d.nodes, node.GetNodeName())
	return nil
}

func (d donut) SaveConfig() error {
	nodeDiskMap := make(map[string][]string)
	for hostname, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return err
		}
		for _, disk := range disks {
			donutConfigPath := path.Join(d.name, donutConfig)
			donutConfigWriter, err := disk.MakeFile(donutConfigPath)
			defer donutConfigWriter.Close()
			if err != nil {
				return err
			}
			nodeDiskMap[hostname][disk.GetOrder()] = disk.GetPath()
			jenc := json.NewEncoder(donutConfigWriter)
			if err := jenc.Encode(nodeDiskMap); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d donut) LoadConfig() error {
	return errors.New("Not Implemented")
}
