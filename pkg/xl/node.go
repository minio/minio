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

package xl

import (
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/xl/block"
)

// node struct internal
type node struct {
	hostname string
	disks    map[int]block.Block
}

// newNode - instantiates a new node
func newNode(hostname string) (node, *probe.Error) {
	if hostname == "" {
		return node{}, probe.NewError(InvalidArgument{})
	}
	disks := make(map[int]block.Block)
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
func (n node) ListDisks() (map[int]block.Block, *probe.Error) {
	return n.disks, nil
}

// AttachDisk - attach a disk
func (n node) AttachDisk(disk block.Block, diskOrder int) *probe.Error {
	if diskOrder < 0 {
		return probe.NewError(InvalidArgument{})
	}
	n.disks[diskOrder] = disk
	return nil
}

// DetachDisk - detach a disk
func (n node) DetachDisk(diskOrder int) *probe.Error {
	delete(n.disks, diskOrder)
	return nil
}

// SaveConfig - save node configuration
func (n node) SaveConfig() *probe.Error {
	return probe.NewError(NotImplemented{Function: "SaveConfig"})
}

// LoadConfig - load node configuration from saved configs
func (n node) LoadConfig() *probe.Error {
	return probe.NewError(NotImplemented{Function: "LoadConfig"})
}
