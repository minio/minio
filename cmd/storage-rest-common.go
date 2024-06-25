// Copyright (c) 2015-2024 MinIO, Inc.
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

package cmd

//go:generate msgp -file $GOFILE -unexported

const (
	storageRESTVersion       = "v59" // Change ReadOptions inclFreeVersions
	storageRESTVersionPrefix = SlashSeparator + storageRESTVersion
	storageRESTPrefix        = minioReservedBucketPath + "/storage"
)

const (
	storageRESTMethodHealth = "/health"

	storageRESTMethodAppendFile     = "/appendfile"
	storageRESTMethodCreateFile     = "/createfile"
	storageRESTMethodWriteAll       = "/writeall"
	storageRESTMethodReadVersion    = "/readversion"
	storageRESTMethodReadXL         = "/readxl"
	storageRESTMethodReadAll        = "/readall"
	storageRESTMethodReadFile       = "/readfile"
	storageRESTMethodReadFileStream = "/readfilestream"
	storageRESTMethodListDir        = "/listdir"
	storageRESTMethodDeleteVersions = "/deleteverions"
	storageRESTMethodRenameFile     = "/renamefile"
	storageRESTMethodVerifyFile     = "/verifyfile"
	storageRESTMethodStatInfoFile   = "/statfile"
	storageRESTMethodReadMultiple   = "/readmultiple"
	storageRESTMethodCleanAbandoned = "/cleanabandoned"
)

const (
	storageRESTVolume           = "volume"
	storageRESTVolumes          = "volumes"
	storageRESTDirPath          = "dir-path"
	storageRESTFilePath         = "file-path"
	storageRESTVersionID        = "version-id"
	storageRESTReadData         = "read-data"
	storageRESTHealing          = "healing"
	storageRESTTotalVersions    = "total-versions"
	storageRESTSrcVolume        = "source-volume"
	storageRESTSrcPath          = "source-path"
	storageRESTDstVolume        = "destination-volume"
	storageRESTDstPath          = "destination-path"
	storageRESTOffset           = "offset"
	storageRESTLength           = "length"
	storageRESTCount            = "count"
	storageRESTBitrotAlgo       = "bitrot-algo"
	storageRESTBitrotHash       = "bitrot-hash"
	storageRESTDiskID           = "disk-id"
	storageRESTForceDelete      = "force-delete"
	storageRESTGlob             = "glob"
	storageRESTMetrics          = "metrics"
	storageRESTDriveQuorum      = "drive-quorum"
	storageRESTOrigVolume       = "orig-volume"
	storageRESTInclFreeVersions = "incl-free-versions"
)

type nsScannerOptions struct {
	DiskID   string          `msg:"id"`
	ScanMode int             `msg:"m"`
	Cache    *dataUsageCache `msg:"c"`
}

type nsScannerResp struct {
	Update *dataUsageEntry `msg:"u"`
	Final  *dataUsageCache `msg:"f"`
}
