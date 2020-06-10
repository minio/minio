// +build linux,!appengine

/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

import (
	"bytes"
	"fmt"
	"syscall"
	"unsafe"
)

func direntNamlen(dirent *syscall.Dirent) (uint64, error) {
	const fixedHdr = uint16(unsafe.Offsetof(syscall.Dirent{}.Name))
	nameBuf := (*[unsafe.Sizeof(dirent.Name)]byte)(unsafe.Pointer(&dirent.Name[0]))
	const nameBufLen = uint16(len(nameBuf))
	limit := dirent.Reclen - fixedHdr
	if limit > nameBufLen {
		limit = nameBufLen
	}
	// Avoid bugs in long file names
	// https://github.com/golang/tools/commit/5f9a5413737ba4b4f692214aebee582b47c8be74
	nameLen := bytes.IndexByte(nameBuf[:limit], 0)
	if nameLen < 0 {
		return 0, fmt.Errorf("failed to find terminating 0 byte in dirent")
	}
	return uint64(nameLen), nil
}
