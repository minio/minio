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
	"path/filepath"

	"github.com/minio/minio/pkg/donut/disk"
	"github.com/minio/minio/pkg/iodine"
)

// Info - return info about donut configuration
func (donut API) Info() (nodeDiskMap map[string][]string, err error) {
	nodeDiskMap = make(map[string][]string)
	for nodeName, node := range donut.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		diskList := make([]string, len(disks))
		for diskOrder, disk := range disks {
			diskList[diskOrder] = disk.GetPath()
		}
		nodeDiskMap[nodeName] = diskList
	}
	return nodeDiskMap, nil
}

// AttachNode - attach node
func (donut API) AttachNode(hostname string, disks []string) error {
	if hostname == "" || len(disks) == 0 {
		return iodine.New(InvalidArgument{}, nil)
	}
	node, err := newNode(hostname)
	if err != nil {
		return iodine.New(err, nil)
	}
	donut.nodes[hostname] = node
	for i, d := range disks {
		newDisk, err := disk.New(d)
		if err != nil {
			continue
		}
		if err := newDisk.MakeDir(donut.config.DonutName); err != nil {
			return iodine.New(err, nil)
		}
		if err := node.AttachDisk(newDisk, i); err != nil {
			return iodine.New(err, nil)
		}
	}
	return nil
}

// DetachNode - detach node
func (donut API) DetachNode(hostname string) error {
	delete(donut.nodes, hostname)
	return nil
}

// SaveConfig - save donut configuration
func (donut API) SaveConfig() error {
	nodeDiskMap := make(map[string][]string)
	for hostname, node := range donut.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return iodine.New(err, nil)
		}
		for order, disk := range disks {
			donutConfigPath := filepath.Join(donut.config.DonutName, donutConfig)
			donutConfigWriter, err := disk.CreateFile(donutConfigPath)
			defer donutConfigWriter.Close()
			if err != nil {
				return iodine.New(err, nil)
			}
			nodeDiskMap[hostname][order] = disk.GetPath()
			jenc := json.NewEncoder(donutConfigWriter)
			if err := jenc.Encode(nodeDiskMap); err != nil {
				return iodine.New(err, nil)
			}
		}
	}
	return nil
}

// LoadConfig - load configuration
func (donut API) LoadConfig() error {
	return iodine.New(NotImplemented{Function: "LoadConfig"}, nil)
}
