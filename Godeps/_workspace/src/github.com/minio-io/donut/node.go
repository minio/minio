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
)

type node struct {
	hostname string
	disks    map[string]Disk
}

// NewNode - instantiates a new node
func NewNode(hostname string) (Node, error) {
	if hostname == "" {
		return nil, errors.New("invalid argument")
	}
	disks := make(map[string]Disk)
	n := node{
		hostname: hostname,
		disks:    disks,
	}
	return n, nil
}

func (n node) GetNodeName() string {
	return n.hostname
}

func (n node) ListDisks() (map[string]Disk, error) {
	return n.disks, nil
}

func (n node) AttachDisk(disk Disk) error {
	if disk == nil {
		return errors.New("Invalid argument")
	}
	n.disks[disk.GetPath()] = disk
	return nil
}

func (n node) DetachDisk(disk Disk) error {
	delete(n.disks, disk.GetPath())
	return nil
}

func (n node) SaveConfig() error {
	return errors.New("Not Implemented")
}

func (n node) LoadConfig() error {
	return errors.New("Not Implemented")
}
