// +build !windows

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

package certs

import (
	"crypto/x509"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// Possible directories with certificate files, this is an extended
// list from https://golang.org/src/crypto/x509/root_unix.go?#L18
// for k8s platform
var certDirectories = []string{
	"/var/run/secrets/kubernetes.io/serviceaccount",
}

// readUniqueDirectoryEntries is like ioutil.ReadDir but omits
// symlinks that point within the directory.
func readUniqueDirectoryEntries(dir string) ([]os.FileInfo, error) {
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	uniq := fis[:0]
	for _, fi := range fis {
		if !isSameDirSymlink(fi, dir) {
			uniq = append(uniq, fi)
		}
	}
	return uniq, nil
}

// isSameDirSymlink reports whether fi in dir is a symlink with a
// target not containing a slash.
func isSameDirSymlink(fi os.FileInfo, dir string) bool {
	if fi.Mode()&os.ModeSymlink == 0 {
		return false
	}
	target, err := os.Readlink(filepath.Join(dir, fi.Name()))
	return err == nil && !strings.Contains(target, "/")
}

func loadSystemRoots() (*x509.CertPool, error) {
	caPool, err := x509.SystemCertPool()
	if err != nil {
		return caPool, err
	}

	for _, directory := range certDirectories {
		fis, err := readUniqueDirectoryEntries(directory)
		if err != nil {
			if os.IsNotExist(err) || os.IsPermission(err) {
				return caPool, nil
			}
			return caPool, err
		}
		for _, fi := range fis {
			data, err := ioutil.ReadFile(directory + "/" + fi.Name())
			if err == nil {
				caPool.AppendCertsFromPEM(data)
			}
		}
	}
	return caPool, nil
}
