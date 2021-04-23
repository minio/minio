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

import "math/big"

// Defined in <linux/nvme_ioctl.h>
//nolint:structcheck,deadcode
type nvmePassthruCommand struct {
	opcode      uint8
	flags       uint8
	rsvd1       uint16
	nsid        uint32
	cdw2        uint32
	cdw3        uint32
	metadata    uint64
	addr        uint64
	metadataLen uint32
	dataLen     uint32
	cdw10       uint32
	cdw11       uint32
	cdw12       uint32
	cdw13       uint32
	cdw14       uint32
	cdw15       uint32
	timeoutMS   uint32
	result      uint32
} // 72 bytes

type nvmeIdentPowerState struct {
	MaxPower        uint16 // Centiwatts
	Rsvd2           uint8
	Flags           uint8
	EntryLat        uint32 // Microseconds
	ExitLat         uint32 // Microseconds
	ReadTput        uint8
	ReadLat         uint8
	WriteTput       uint8
	WriteLat        uint8
	IdlePower       uint16
	IdleScale       uint8
	Rsvd19          uint8
	ActivePower     uint16
	ActiveWorkScale uint8
	Rsvd23          [9]byte
}

//nolint:deadcode
type nvmeIdentController struct {
	VendorID     uint16                  // PCI Vendor ID
	Ssvid        uint16                  // PCI Subsystem Vendor ID
	SerialNumber [20]byte                // Serial Number
	ModelNumber  [40]byte                // Model Number
	Firmware     [8]byte                 // Firmware Revision
	Rab          uint8                   // Recommended Arbitration Burst
	IEEE         [3]byte                 // IEEE OUI Identifier
	Cmic         uint8                   // Controller Multi-Path I/O and Namespace Sharing Capabilities
	Mdts         uint8                   // Maximum Data Transfer Size
	Cntlid       uint16                  // Controller ID
	Ver          uint32                  // Version
	Rtd3r        uint32                  // RTD3 Resume Latency
	Rtd3e        uint32                  // RTD3 Entry Latency
	Oaes         uint32                  // Optional Asynchronous Events Supported
	Rsvd96       [160]byte               // ...
	Oacs         uint16                  // Optional Admin Command Support
	ACL          uint8                   // Abort Command Limit
	Aerl         uint8                   // Asynchronous Event Request Limit
	Frmw         uint8                   // Firmware Updates
	Lpa          uint8                   // Log Page Attributes
	Elpe         uint8                   // Error Log Page Entries
	Npss         uint8                   // Number of Power States Support
	Avscc        uint8                   // Admin Vendor Specific Command Configuration
	Apsta        uint8                   // Autonomous Power State Transition Attributes
	Wctemp       uint16                  // Warning Composite Temperature Threshold
	Cctemp       uint16                  // Critical Composite Temperature Threshold
	Mtfa         uint16                  // Maximum Time for Firmware Activation
	Hmpre        uint32                  // Host Memory Buffer Preferred Size
	Hmmin        uint32                  // Host Memory Buffer Minimum Size
	Tnvmcap      [16]byte                // Total NVM Capacity
	Unvmcap      [16]byte                // Unallocated NVM Capacity
	Rpmbs        uint32                  // Replay Protected Memory Block Support
	Rsvd316      [196]byte               // ...
	Sqes         uint8                   // Submission Queue Entry Size
	Cqes         uint8                   // Completion Queue Entry Size
	Rsvd514      [2]byte                 // (defined in NVMe 1.3 spec)
	Nn           uint32                  // Number of Namespaces
	Oncs         uint16                  // Optional NVM Command Support
	Fuses        uint16                  // Fused Operation Support
	Fna          uint8                   // Format NVM Attributes
	Vwc          uint8                   // Volatile Write Cache
	Awun         uint16                  // Atomic Write Unit Normal
	Awupf        uint16                  // Atomic Write Unit Power Fail
	Nvscc        uint8                   // NVM Vendor Specific Command Configuration
	Rsvd531      uint8                   // ...
	Acwu         uint16                  // Atomic Compare & Write Unit
	Rsvd534      [2]byte                 // ...
	Sgls         uint32                  // SGL Support
	Rsvd540      [1508]byte              // ...
	Psd          [32]nvmeIdentPowerState // Power State Descriptors
	Vs           [1024]byte              // Vendor Specific
} // 4096 bytes

type nvmeLBAF struct {
	Ms uint16
	Ds uint8
	Rp uint8
}

//nolint:deadcode
type nvmeIdentNamespace struct {
	Nsze    uint64
	Ncap    uint64
	Nuse    uint64
	Nsfeat  uint8
	Nlbaf   uint8
	Flbas   uint8
	Mc      uint8
	Dpc     uint8
	Dps     uint8
	Nmic    uint8
	Rescap  uint8
	Fpi     uint8
	Rsvd33  uint8
	Nawun   uint16
	Nawupf  uint16
	Nacwu   uint16
	Nabsn   uint16
	Nabo    uint16
	Nabspf  uint16
	Rsvd46  [2]byte
	Nvmcap  [16]byte
	Rsvd64  [40]byte
	Nguid   [16]byte
	EUI64   [8]byte
	Lbaf    [16]nvmeLBAF
	Rsvd192 [192]byte
	Vs      [3712]byte
} // 4096 bytes

//nolint:deadcode
type nvmeSMARTLog struct {
	CritWarning      uint8
	Temperature      [2]uint8
	AvailSpare       uint8
	SpareThresh      uint8
	PercentUsed      uint8
	Rsvd6            [26]byte
	DataUnitsRead    [16]byte
	DataUnitsWritten [16]byte
	HostReads        [16]byte
	HostWrites       [16]byte
	CtrlBusyTime     [16]byte
	PowerCycles      [16]byte
	PowerOnHours     [16]byte
	UnsafeShutdowns  [16]byte
	MediaErrors      [16]byte
	NumErrLogEntries [16]byte
	WarningTempTime  uint32
	CritCompTime     uint32
	TempSensor       [8]uint16
	Rsvd216          [296]byte
} // 512 bytes

// NVMeDevice represents drive data about NVMe drives
//nolint:structcheck
type NVMeDevice struct {
	Name string
	fd   int
}

// Info contains S.M.A.R.T data about the drive
type Info struct {
	Device string `json:"device"`

	Scsi *ScsiInfo `json:"scsi,omitempty"`
	Nvme *NvmeInfo `json:"nvme,omitempty"`
	Ata  *AtaInfo  `json:"ata,omitempty"`

	Error string `json:"error,omitempty"`
}

// AtaInfo contains ATA drive info
type AtaInfo struct {
	LUWWNDeviceID         string `json:"scsiLuWWNDeviceID,omitempty"`
	SerialNum             string `json:"serialNum,omitempty"`
	ModelNum              string `json:"modelNum,omitempty"`
	FirmwareRevision      string `json:"firmwareRevision,omitempty"`
	RotationRate          string `json:"RotationRate,omitempty"`
	ATAMajorVersion       string `json:"MajorVersion,omitempty"`
	ATAMinorVersion       string `json:"MinorVersion,omitempty"`
	SmartSupportAvailable bool   `json:"smartSupportAvailable,omitempty"`
	SmartSupportEnabled   bool   `json:"smartSupportEnabled,omitempty"`
	ErrorLog              string `json:"smartErrorLog,omitempty"`
	Transport             string `json:"transport,omitempty"`
}

// ScsiInfo contains SCSI drive Info
type ScsiInfo struct {
	CapacityBytes int64  `json:"scsiCapacityBytes,omitempty"`
	ModeSenseBuf  string `json:"scsiModeSenseBuf,omitempty"`
	RespLen       int64  `json:"scsirespLen,omitempty"`
	BdLen         int64  `json:"scsiBdLen,omitempty"`
	Offset        int64  `json:"scsiOffset,omitempty"`
	RPM           int64  `json:"sciRpm,omitempty"`
}

// NvmeInfo contains NVMe drive info
type NvmeInfo struct {
	SerialNum       string `json:"serialNum,omitempty"`
	VendorID        string `json:"vendorId,omitempty"`
	FirmwareVersion string `json:"firmwareVersion,omitempty"`
	ModelNum        string `json:"modelNum,omitempty"`
	SpareAvailable  string `json:"spareAvailable,omitempty"`
	SpareThreshold  string `json:"spareThreshold,omitempty"`
	Temperature     string `json:"temperature,omitempty"`
	CriticalWarning string `json:"criticalWarning,omitempty"`

	MaxDataTransferPages        int      `json:"maxDataTransferPages,omitempty"`
	ControllerBusyTime          *big.Int `json:"controllerBusyTime,omitempty"`
	PowerOnHours                *big.Int `json:"powerOnHours,omitempty"`
	PowerCycles                 *big.Int `json:"powerCycles,omitempty"`
	UnsafeShutdowns             *big.Int `json:"unsafeShutdowns,omitempty"`
	MediaAndDataIntegrityErrors *big.Int `json:"mediaAndDataIntgerityErrors,omitempty"`
	DataUnitsReadBytes          *big.Int `json:"dataUnitsReadBytes,omitempty"`
	DataUnitsWrittenBytes       *big.Int `json:"dataUnitsWrittenBytes,omitempty"`
	HostReadCommands            *big.Int `json:"hostReadCommands,omitempty"`
	HostWriteCommands           *big.Int `json:"hostWriteCommands,omitempty"`
}
