// +build linux

/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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

package smart

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"regexp"
	"strings"
	"unsafe"

	"github.com/dswarbrick/smart/drivedb"
	"github.com/dswarbrick/smart/ioctl"
	"github.com/dswarbrick/smart/scsi"
	"github.com/dswarbrick/smart/utils"

	"gopkg.in/yaml.v2"
)

// GetInfo - gets info about device
func GetInfo(device string) (Info, error) {
	info := Info{
		Device: device,
	}

	var db drivedb.DriveDb
	dec := yaml.NewDecoder(bytes.NewBuffer(MustAsset("drivedb.yaml")))

	err := dec.Decode(&db)
	if err != nil {
		return info, err
	}

	for i, d := range db.Drives {
		db.Drives[i].CompiledRegexp, _ = regexp.Compile(d.ModelRegex)
	}

	if strings.HasPrefix(device, "/dev/nvme") {
		d := NewNVMeDevice(device)
		if err := d.Open(); err != nil {
			return info, err
		}
		nvmeInfo, err := getNvmeInfo(d)
		if err != nil {
			return info, err
		}
		info.Nvme = nvmeInfo
		return info, nil
	}

	d, err := scsi.OpenSCSIAutodetect(device)
	if err != nil {
		return info, err
	}

	switch dev := d.(type) {
	case *scsi.SCSIDevice:
		scsiInfo, err := getScsiInfo(dev)
		if err != nil {
			return info, err
		}
		info.Scsi = scsiInfo
	case *scsi.SATDevice:
		ataInfo, err := getAtaInfo(dev)
		if err != nil {
			return info, err
		}
		info.Ata = ataInfo
	}

	return info, nil
}

func getNvmeInfo(d *NVMeDevice) (*NvmeInfo, error) {
	buf := make([]byte, 4096)
	nvmeInfo := &NvmeInfo{}

	cmd := nvmePassthruCommand{
		opcode:  NvmeAdminIdentify,
		nsid:    0, // Namespace 0, since we are identifying the controller
		addr:    uint64(uintptr(unsafe.Pointer(&buf[0]))),
		dataLen: uint32(len(buf)),
		cdw10:   1, // Identify controller
	}

	if err := ioctl.Ioctl(uintptr(d.fd), nvmeIoctlAdminCmd, uintptr(unsafe.Pointer(&cmd))); err != nil {
		return nvmeInfo, err
	}

	var controller nvmeIdentController
	binary.Read(bytes.NewBuffer(buf[:]), utils.NativeEndian, &controller)

	nvmeInfo.VendorID = strings.TrimSpace(fmt.Sprintf("%#04x", controller.VendorID))
	nvmeInfo.ModelNum = strings.TrimSpace(fmt.Sprintf("%s", controller.ModelNumber))
	nvmeInfo.SerialNum = strings.TrimSpace(fmt.Sprintf("%s", controller.SerialNumber))
	nvmeInfo.FirmwareVersion = strings.TrimSpace(fmt.Sprintf("%s", controller.Firmware))
	nvmeInfo.MaxDataTransferPages = 1 << controller.Mdts

	buf2 := make([]byte, 512)

	// Read SMART log
	if err := d.readLogPage(0x02, &buf2); err != nil {
		return nvmeInfo, err
	}

	var sl nvmeSMARTLog
	binary.Read(bytes.NewBuffer(buf2[:]), utils.NativeEndian, &sl)

	unitsRead := le128ToBigInt(sl.DataUnitsRead)
	unitsWritten := le128ToBigInt(sl.DataUnitsWritten)

	nvmeInfo.CriticalWarning = fmt.Sprintf("%x", sl.CritWarning)
	nvmeInfo.Temperature = fmt.Sprintf("%d Celsius",
		((uint16(sl.Temperature[1])<<8)|uint16(sl.Temperature[0]))-273) // Kelvin to degrees Celsius
	nvmeInfo.SpareAvailable = fmt.Sprintf("%d%%", sl.AvailSpare)
	nvmeInfo.SpareThreshold = fmt.Sprintf("%d%%", sl.SpareThresh)
	nvmeInfo.DataUnitsReadBytes = unitsRead
	nvmeInfo.DataUnitsWrittenBytes = unitsWritten
	nvmeInfo.HostReadCommands = le128ToBigInt(sl.HostReads)
	nvmeInfo.HostWriteCommands = le128ToBigInt(sl.HostWrites)
	nvmeInfo.ControllerBusyTime = le128ToBigInt(sl.CtrlBusyTime)
	nvmeInfo.PowerCycles = le128ToBigInt(sl.PowerCycles)
	nvmeInfo.PowerOnHours = le128ToBigInt(sl.PowerOnHours)
	nvmeInfo.UnsafeShutdowns = le128ToBigInt(sl.UnsafeShutdowns)
	nvmeInfo.MediaAndDataIntegrityErrors = le128ToBigInt(sl.MediaErrors)

	return nvmeInfo, nil
}

func getScsiInfo(d *scsi.SCSIDevice) (*ScsiInfo, error) {
	return &ScsiInfo{}, nil
}

func getAtaInfo(d *scsi.SATDevice) (*AtaInfo, error) {
	return &AtaInfo{}, nil
}
