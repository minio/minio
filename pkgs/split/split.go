/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

// +build linux
// amd64

package split

// #include <stdlib.h>
// #include <stdlib.h>
//
// #include "split.h"
import "C"
import (
	"errors"
	"github.com/minio-io/minio/pkgs/strbyteconv"
	"unsafe"
)

type Split struct {
	bytecnt C.ssize_t
	bname   *C.char
}

func (b *Split) GenChunks(bname string, bytestr string) error {
	bytecnt, err := strbyteconv.StringToBytes(bytestr)
	if err != nil {
		return err
	}

	b.bytecnt = C.ssize_t(bytecnt)
	b.bname = C.CString(bname)
	defer C.free(unsafe.Pointer(b.bname))

	value := C.minio_split(b.bname, b.bytecnt)
	if value < 0 {
		return errors.New("File split failed")
	}
	return nil
}
