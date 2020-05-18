// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This code is imported from "golang.org/x/tools/internal/fastwalk",
// only fastwalk.go is imported since we already implement readDir()
// with some little tweaks.

package cmd

import (
	"errors"
	"os"
	"strings"
)

var errSkipFile = errors.New("fastwalk: skip this file")

func readDirFn(dirName string, fn func(entName string, typ os.FileMode) error) error {
	fis, err := readDir(dirName)
	if err != nil {
		if os.IsNotExist(err) || err == errFileNotFound {
			return nil
		}
		return err
	}
	for _, fi := range fis {
		var mode os.FileMode
		if strings.HasSuffix(fi, SlashSeparator) {
			mode |= os.ModeDir
		}

		if err = fn(fi, mode); err != nil {
			return err
		}
	}
	return nil
}
