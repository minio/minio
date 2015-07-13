// +build linux,amd64

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

package scsi

import (
	"errors"
	"io/ioutil"
	"path/filepath"

	"github.com/minio/minio/pkg/iodine"
)

// NOTE : supporting virtio based scsi devices is out of scope for this implementation

// Get get disk scsi params
func (d Disks) Get(disk string) Attributes {
	return d[disk]
}

// Len return len of total disks
func (d Disks) Len() int {
	return len(d)
}

// GetDisks - get system devices list
func GetDisks() (Disks, error) {
	var scsidevices []string
	var scsiAttrList []string
	d := Disks{}

	sysFiles, err := ioutil.ReadDir(sysFSDEVICES)
	if err != nil {
		// may be an amazon instance, ignore this
		return Disks{}, nil
	}

	scsidevices = filterdisks(sysFiles)
	if len(scsidevices) == 0 {
		// may be a docker instance, ignore this
		return Disks{}, nil
	}

	for _, s := range scsidevices {
		scsiAttrPath := filepath.Join(sysFSDEVICES, s)
		scsiAttrs, err := ioutil.ReadDir(scsiAttrPath)
		if err != nil {
			return Disks{}, iodine.New(err, nil)
		}
		scsiBlockPath := filepath.Join(sysFSDEVICES, s, sysFSBLOCK)
		scsidevList, err := ioutil.ReadDir(scsiBlockPath)
		if err != nil {
			return Disks{}, iodine.New(err, nil)
		}
		if len(scsidevList) > 1 {
			return Disks{}, iodine.New(errors.New("Scsi address points to multiple block devices"), nil)
		}
		device := filepath.Join(udev, scsidevList[0].Name())
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
			return Disks{}, iodine.New(errors.New("No scsi attributes found"), nil)
		}
		d[device] = getattrs(scsiAttrPath, scsiAttrList)
	}
	return d, nil
}
