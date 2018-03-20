// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// http://golang.org/src/os/stat_plan9.go

package atime

import (
	"os"
	"time"
)

func atime(fi os.FileInfo) time.Time {
	return time.Unix(int64(fi.Sys().(*syscall.Dir).Atime), 0)
}
