/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

// !build linux,amd64

package scsi

import (
	"errors"
	"io/ioutil"
	"path"
)

// NOTE : supporting virtio based scsi devices
//        is out of scope for this implementation

type Scsi struct {
	Disk        string
	Scsiattrmap map[string][]byte
	Diskattrmap map[string][]byte
}

type Devices struct {
	List []Scsi
}

func (d *Devices) getInfo(disk string) (map[string][]byte, error) {
	var diskAttrsList []string
	var diskQueueAttrs []string
	aggrAttrMap := make(map[string][]byte)

	sysfs_block_dev := path.Join(SYSFS_BLOCK + disk)
	sysfs_block_dev_queue := path.Join(sysfs_block_dev + "/queue")

	scsiFiles, err := ioutil.ReadDir(sysfs_block_dev)
	if err != nil {
		return nil, err
	}

	scsiQueueFiles, err := ioutil.ReadDir(sysfs_block_dev_queue)
	if err != nil {
		return nil, err
	}

	for _, sf := range scsiFiles {
		if sf.IsDir() {
			continue
		}
		// Skip symlinks
		if !sf.Mode().IsRegular() {
			continue
		}
		// Skip, not readable, write-only
		if sf.Mode().Perm() == 128 {
			continue
		}
		diskAttrsList = append(diskAttrsList, sf.Name())
	}

	for _, sf := range scsiQueueFiles {
		if sf.IsDir() {
			continue
		}
		// Skip symlinks
		if !sf.Mode().IsRegular() {
			continue
		}
		// Skip, not readable, write-only
		if sf.Mode().Perm() == 128 {
			continue
		}
		diskQueueAttrs = append(diskQueueAttrs, sf.Name())
	}

	if len(diskAttrsList) == 0 {
		return nil, errors.New("No disk attributes found")
	}

	if len(diskQueueAttrs) == 0 {
		return nil, errors.New("No disk queue attributes found")
	}

	diskAttrMap := getattrs(sysfs_block_dev, diskAttrsList)
	diskQueueAttrMap := getattrs(sysfs_block_dev_queue, diskQueueAttrs)

	for k, v := range diskAttrMap {
		aggrAttrMap[k] = v
	}

	for k, v := range diskQueueAttrMap {
		aggrAttrMap[k] = v
	}

	return aggrAttrMap, nil
}

func (d *Devices) Get() error {
	var scsidevices []string
	var scsiAttrList []string

	sysFiles, err := ioutil.ReadDir(SYSFS_SCSI_DEVICES)
	if err != nil {
		return err
	}

	scsidevices = filterdevices(sysFiles)
	if len(scsidevices) == 0 {
		return errors.New("No scsi devices found on the system")
	}

	for _, scsi := range scsidevices {
		var _scsi Scsi
		scsiAttrPath := path.Join(SYSFS_SCSI_DEVICES, scsi, "/")
		scsiAttrs, err := ioutil.ReadDir(scsiAttrPath)
		if err != nil {
			return err
		}
		scsiBlockPath := path.Join(SYSFS_SCSI_DEVICES, scsi, "/block")
		scsidevList, err := ioutil.ReadDir(scsiBlockPath)
		if err != nil {
			return err
		}

		if len(scsidevList) > 1 {
			return errors.New("Scsi address points to multiple block devices")
		}

		_scsi.Disk = UDEV + scsidevList[0].Name()
		for _, sa := range scsiAttrs {
			// Skip directories
			if sa.IsDir() {
				continue
			}
			// Skip symlinks
			if !sa.Mode().IsRegular() {
				continue
			}
			// Skip, not readable, write-only
			if sa.Mode().Perm() == 128 {
				continue
			}
			scsiAttrList = append(scsiAttrList, sa.Name())
		}

		if len(scsiAttrList) == 0 {
			return errors.New("No scsi attributes found")
		}
		attrMap := getattrs(scsiAttrPath, scsiAttrList)
		_scsi.Scsiattrmap = attrMap
		_scsi.Diskattrmap, err = d.getInfo(scsidevList[0].Name())
		if err != nil {
			return err
		}
		d.List = append(d.List, _scsi)
	}
	return nil
}
