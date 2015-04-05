/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"fmt"
	"strconv"
	"strings"
)

// Convert bytes to human readable string. Like a 2 MB, 64.2 KB, 52 B
func (d disk) formatBytes(i int64) (result string) {
	switch {
	case i > (1024 * 1024 * 1024 * 1024):
		result = fmt.Sprintf("%.02f TB", float64(i)/1024/1024/1024/1024)
	case i > (1024 * 1024 * 1024):
		result = fmt.Sprintf("%.02f GB", float64(i)/1024/1024/1024)
	case i > (1024 * 1024):
		result = fmt.Sprintf("%.02f MB", float64(i)/1024/1024)
	case i > 1024:
		result = fmt.Sprintf("%.02f KB", float64(i)/1024)
	default:
		result = fmt.Sprintf("%d B", i)
	}
	result = strings.Trim(result, " ")
	return
}

var fsType2StringMap = map[string]string{
	"137d":     "EXT",
	"ef51":     "EXT2OLD",
	"ef53":     "EXT4",
	"4244":     "HFS",
	"5346544e": "NTFS",
	"4d44":     "MSDOS",
	"52654973": "REISERFS",
	"6969":     "NFS",
	"01021994": "TMPFS",
	"58465342": "XFS",
}

func (d disk) getFSType(fsType int64) string {
	fsTypeHex := strconv.FormatInt(fsType, 16)
	fsTypeString, ok := fsType2StringMap[fsTypeHex]
	if ok == false {
		return "UNKNOWN"
	}
	return fsTypeString
}
