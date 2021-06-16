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

package cmd

const (
	storageRESTVersion       = "v37" // cleanup behavior change at storage layer.
	storageRESTVersionPrefix = SlashSeparator + storageRESTVersion
	storageRESTPrefix        = minioReservedBucketPath + "/storage"
)

const (
	storageRESTMethodHealth      = "/health"
	storageRESTMethodDiskInfo    = "/diskinfo"
	storageRESTMethodNSScanner   = "/nsscanner"
	storageRESTMethodMakeVol     = "/makevol"
	storageRESTMethodMakeVolBulk = "/makevolbulk"
	storageRESTMethodStatVol     = "/statvol"
	storageRESTMethodDeleteVol   = "/deletevol"
	storageRESTMethodListVols    = "/listvols"

	storageRESTMethodAppendFile     = "/appendfile"
	storageRESTMethodCreateFile     = "/createfile"
	storageRESTMethodWriteAll       = "/writeall"
	storageRESTMethodWriteMetadata  = "/writemetadata"
	storageRESTMethodUpdateMetadata = "/updatemetadata"
	storageRESTMethodDeleteVersion  = "/deleteversion"
	storageRESTMethodReadVersion    = "/readversion"
	storageRESTMethodRenameData     = "/renamedata"
	storageRESTMethodCheckParts     = "/checkparts"
	storageRESTMethodCheckFile      = "/checkfile"
	storageRESTMethodReadAll        = "/readall"
	storageRESTMethodReadFile       = "/readfile"
	storageRESTMethodReadFileStream = "/readfilestream"
	storageRESTMethodListDir        = "/listdir"
	storageRESTMethodDeleteFile     = "/deletefile"
	storageRESTMethodDeleteVersions = "/deleteverions"
	storageRESTMethodRenameFile     = "/renamefile"
	storageRESTMethodVerifyFile     = "/verifyfile"
	storageRESTMethodWalkDir        = "/walkdir"
)

const (
	storageRESTVolume         = "volume"
	storageRESTVolumes        = "volumes"
	storageRESTDirPath        = "dir-path"
	storageRESTFilePath       = "file-path"
	storageRESTForceDelMarker = "force-delete-marker"
	storageRESTVersionID      = "version-id"
	storageRESTReadData       = "read-data"
	storageRESTTotalVersions  = "total-versions"
	storageRESTSrcVolume      = "source-volume"
	storageRESTSrcPath        = "source-path"
	storageRESTDstVolume      = "destination-volume"
	storageRESTDstPath        = "destination-path"
	storageRESTOffset         = "offset"
	storageRESTLength         = "length"
	storageRESTCount          = "count"
	storageRESTPrefixFilter   = "prefix"
	storageRESTForwardFilter  = "forward"
	storageRESTRecursive      = "recursive"
	storageRESTReportNotFound = "report-notfound"
	storageRESTBitrotAlgo     = "bitrot-algo"
	storageRESTBitrotHash     = "bitrot-hash"
	storageRESTDiskID         = "disk-id"
	storageRESTForceDelete    = "force-delete"
)
