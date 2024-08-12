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
	storageRESTVersion       = "v63" // Introduce RenamePart and ReadParts API
	storageRESTVersionPrefix = SlashSeparator + storageRESTVersion
	storageRESTPrefix        = minioReservedBucketPath + "/storage"
)

const (
	storageRESTMethodHealth = "/health"

	storageRESTMethodAppendFile     = "/afile"
	storageRESTMethodCreateFile     = "/cfile"
	storageRESTMethodWriteAll       = "/wall"
	storageRESTMethodReadVersion    = "/rver"
	storageRESTMethodReadXL         = "/rxl"
	storageRESTMethodReadAll        = "/rall"
	storageRESTMethodReadFile       = "/rfile"
	storageRESTMethodReadFileStream = "/rfilest"
	storageRESTMethodListDir        = "/ls"
	storageRESTMethodDeleteVersions = "/dvers"
	storageRESTMethodRenameFile     = "/rfile"
	storageRESTMethodVerifyFile     = "/vfile"
	storageRESTMethodStatInfoFile   = "/sfile"
	storageRESTMethodReadMultiple   = "/rmpl"
	storageRESTMethodCleanAbandoned = "/cln"
	storageRESTMethodDeleteBulk     = "/dblk"
	storageRESTMethodReadParts      = "/rps"
)

const (
	storageRESTVolume           = "vol"
	storageRESTVolumes          = "vols"
	storageRESTDirPath          = "dpath"
	storageRESTFilePath         = "fp"
	storageRESTVersionID        = "vid"
	storageRESTHealing          = "heal"
	storageRESTTotalVersions    = "tvers"
	storageRESTSrcVolume        = "svol"
	storageRESTSrcPath          = "spath"
	storageRESTDstVolume        = "dvol"
	storageRESTDstPath          = "dpath"
	storageRESTOffset           = "offset"
	storageRESTLength           = "length"
	storageRESTCount            = "count"
	storageRESTBitrotAlgo       = "balg"
	storageRESTBitrotHash       = "bhash"
	storageRESTDiskID           = "did"
	storageRESTForceDelete      = "fdel"
	storageRESTGlob             = "glob"
	storageRESTMetrics          = "metrics"
	storageRESTDriveQuorum      = "dquorum"
	storageRESTOrigVolume       = "ovol"
	storageRESTInclFreeVersions = "incl-fv"
	storageRESTRange            = "rng"
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
