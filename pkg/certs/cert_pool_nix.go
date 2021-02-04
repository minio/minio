// +build !windows

/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
