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

package ioutil

import (
	"errors"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/disk"
)

// ODirectReader - to support O_DIRECT reads for erasure backends.
type ODirectReader struct {
	File      *os.File
	SmallFile bool
	err       error
	buf       []byte
	bufp      *[]byte
	seenRead  bool
}

// Block sizes constant.
const (
	BlockSizeSmall       = 128 * humanize.KiByte // Default r/w block size for smaller objects.
	BlockSizeLarge       = 2 * humanize.MiByte   // Default r/w block size for larger objects.
	BlockSizeReallyLarge = 4 * humanize.MiByte   // Default write block size for objects per shard >= 64MiB
)

// O_DIRECT aligned sync.Pool's
var (
	ODirectPoolXLarge = sync.Pool{
		New: func() interface{} {
			b := disk.AlignedBlock(BlockSizeReallyLarge)
			return &b
		},
	}
	ODirectPoolLarge = sync.Pool{
		New: func() interface{} {
			b := disk.AlignedBlock(BlockSizeLarge)
			return &b
		},
	}
	ODirectPoolSmall = sync.Pool{
		New: func() interface{} {
			b := disk.AlignedBlock(BlockSizeSmall)
			return &b
		},
	}
)

// Invalid argument, unsupported flags such as O_DIRECT
func isSysErrInvalidArg(err error) bool {
	return errors.Is(err, syscall.EINVAL)
}

// Read - Implements Reader interface.
func (o *ODirectReader) Read(buf []byte) (n int, err error) {
	if o.err != nil && (len(o.buf) == 0 || !o.seenRead) {
		return 0, o.err
	}
	if o.buf == nil {
		if o.SmallFile {
			o.bufp = ODirectPoolSmall.Get().(*[]byte)
		} else {
			o.bufp = ODirectPoolLarge.Get().(*[]byte)
		}
	}
	if !o.seenRead {
		o.buf = *o.bufp
		n, err = o.File.Read(o.buf)
		if err != nil && err != io.EOF {
			if isSysErrInvalidArg(err) {
				if err = disk.DisableDirectIO(o.File); err != nil {
					o.err = err
					return n, err
				}
				n, err = o.File.Read(o.buf)
			}
			if err != nil && err != io.EOF {
				o.err = err
				return n, err
			}
		}
		if n == 0 {
			// err is likely io.EOF
			o.err = err
			return n, err
		}
		o.err = err
		o.buf = o.buf[:n]
		o.seenRead = true
	}
	if len(buf) >= len(o.buf) {
		n = copy(buf, o.buf)
		o.seenRead = false
		return n, o.err
	}
	n = copy(buf, o.buf)
	o.buf = o.buf[n:]
	// There is more left in buffer, do not return any EOF yet.
	return n, nil
}

// Close - Release the buffer and close the file.
func (o *ODirectReader) Close() error {
	if o.bufp != nil {
		if o.SmallFile {
			ODirectPoolSmall.Put(o.bufp)
		} else {
			ODirectPoolLarge.Put(o.bufp)
		}
		o.bufp = nil
		o.buf = nil
	}
	o.err = errors.New("internal error: ODirectReader Read after Close")
	return o.File.Close()
}
