// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// http://golang.org/src/os/stat_windows.go

package atime

import (
	"os"
	"syscall"
	"time"
)

func atime(fi os.FileInfo) time.Time {
	return time.Unix(0, fi.Sys().(*syscall.Win32FileAttributeData).LastAccessTime.Nanoseconds())
}
