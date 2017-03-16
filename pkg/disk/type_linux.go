// +build linux

/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package disk

import "strconv"

// fsType2StringMap - list of filesystems supported by donut on linux
var fsType2StringMap = map[string]string{
	"1021994":  "TMPFS",
	"137d":     "EXT",
	"4244":     "HFS",
	"4d44":     "MSDOS",
	"52654973": "REISERFS",
	"5346544e": "NTFS",
	"58465342": "XFS",
	"61756673": "AUFS",
	"6969":     "NFS",
	"ef51":     "EXT2OLD",
	"ef53":     "EXT4",
	"f15f":     "ecryptfs",
}

// getFSType returns the filesystem type of the underlying mounted filesystem
func getFSType(ftype int64) string {
	fsTypeHex := strconv.FormatInt(ftype, 16)
	fsTypeString, ok := fsType2StringMap[fsTypeHex]
	if !ok {
		return "UNKNOWN"
	}
	return fsTypeString
}
