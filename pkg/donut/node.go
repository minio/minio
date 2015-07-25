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
	"github.com/minio/minio/pkg/donut/disk"
	"github.com/minio/minio/pkg/iodine"
)

// node struct internal
type node struct {
	hostname string
	disks    map[int]disk.Disk
}

// newNode - instantiates a new node
func newNode(hostname string) (node, error) {
	if hostname == "" {
		return node{}, iodine.New(InvalidArgument{}, nil)
	}
	disks := make(map[int]disk.Disk)
	n := node{
		hostname: hostname,
		disks:    disks,
	}
	return n, nil
}

// GetHostname - return hostname
func (n node) GetHostname() string {
	return n.hostname
}

// ListDisks - return number of disks
func (n node) ListDisks() (map[int]disk.Disk, error) {
	return n.disks, nil
}

// AttachDisk - attach a disk
func (n node) AttachDisk(disk disk.Disk, diskOrder int) error {
	if diskOrder < 0 {
		return iodine.New(InvalidArgument{}, nil)
	}
	n.disks[diskOrder] = disk
	return nil
}

// DetachDisk - detach a disk
func (n node) DetachDisk(diskOrder int) error {
	delete(n.disks, diskOrder)
	return nil
}

// SaveConfig - save node configuration
func (n node) SaveConfig() error {
	return iodine.New(NotImplemented{Function: "SaveConfig"}, nil)
}

// LoadConfig - load node configuration from saved configs
func (n node) LoadConfig() error {
	return iodine.New(NotImplemented{Function: "LoadConfig"}, nil)
}
