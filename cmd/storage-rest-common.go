/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

const storageRESTVersion = "v2"
const storageRESTPath = minioReservedBucketPath + "/storage/" + storageRESTVersion + "/"

const (
	storageRESTMethodDiskInfo  = "diskinfo"
	storageRESTMethodMakeVol   = "makevol"
	storageRESTMethodStatVol   = "statvol"
	storageRESTMethodDeleteVol = "deletevol"
	storageRESTMethodListVols  = "listvols"

	storageRESTMethodPrepareFile = "preparefile"
	storageRESTMethodAppendFile  = "appendfile"
	storageRESTMethodWriteAll    = "writeall"
	storageRESTMethodStatFile    = "statfile"
	storageRESTMethodReadAll     = "readall"
	storageRESTMethodReadFile    = "readfile"
	storageRESTMethodListDir     = "listdir"
	storageRESTMethodDeleteFile  = "deletefile"
	storageRESTMethodRenameFile  = "renamefile"
)

const (
	storageRESTVolume     = "volume"
	storageRESTDirPath    = "dir-path"
	storageRESTFilePath   = "file-path"
	storageRESTSrcVolume  = "source-volume"
	storageRESTSrcPath    = "source-path"
	storageRESTDstVolume  = "destination-volume"
	storageRESTDstPath    = "destination-path"
	storageRESTOffset     = "offset"
	storageRESTLength     = "length"
	storageRESTCount      = "count"
	storageRESTBitrotAlgo = "bitrot-algo"
	storageRESTBitrotHash = "bitrot-hash"
)
