// +build linux

// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
	"github.com/minio/madmin-go"

	"gopkg.in/yaml.v2"
)

// GetInfo - gets info about device
func GetInfo(device string) (madmin.SmartInfo, error) {
	info := madmin.SmartInfo{
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

func getNvmeInfo(d *NVMeDevice) (*madmin.SmartNvmeInfo, error) {
	buf := make([]byte, 4096)
	nvmeInfo := &madmin.SmartNvmeInfo{}

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

func getScsiInfo(d *scsi.SCSIDevice) (*madmin.SmartScsiInfo, error) {
	return &madmin.SmartScsiInfo{}, nil
}

func getAtaInfo(d *scsi.SATDevice) (*madmin.SmartAtaInfo, error) {
	return &madmin.SmartAtaInfo{}, nil
}
