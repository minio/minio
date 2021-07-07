/*
 * MinIO Cloud Storage, (C) 2018-2019 MinIO, Inc.
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

package cmd

const (
	storageRESTVersion       = "v32" // Inline bugfix needs all servers to be updated
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
	storageRESTMethodStatInfoFile   = "/statfile"
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
