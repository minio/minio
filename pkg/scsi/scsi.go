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

type _Scsi struct {
	device  string
	attrMap map[string][]byte
}

type Devices struct {
	List []_Scsi
}

func (d *Devices) Get() error {
	var scsidevices []string
	var scsiAttrList []string

	sysfs := path.Join(SYSFS_SCSI_DEVICES)
	sysFiles, err := ioutil.ReadDir(sysfs)
	if err != nil {
		return err
	}

	scsidevices = filterdevices(sysFiles)
	if len(scsidevices) == 0 {
		return errors.New("No scsi devices found on the system")
	}

	for _, scsi := range scsidevices {
		var _scsi _Scsi
		scsiAttrPath := path.Join(sysfs, scsi, "/")
		scsiAttrs, err := ioutil.ReadDir(scsiAttrPath)
		if err != nil {
			return err
		}
		scsiBlockPath := path.Join(sysfs, scsi, SYSFS_BLOCK)
		scsidevList, err := ioutil.ReadDir(scsiBlockPath)
		if err != nil {
			return err
		}

		if len(scsidevList) > 1 {
			return errors.New("Scsi address points to multiple block devices")
		}

		_scsi.device = UDEV + scsidevList[0].Name()
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
		_scsi.attrMap = attrMap
		d.List = append(d.List, _scsi)
	}
	return nil
}
