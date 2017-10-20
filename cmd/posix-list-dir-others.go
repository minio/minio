// +build !linux,!darwin,!openbsd,!freebsd,!netbsd

/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"io"
	"os"
	"path"
	"strings"
)

// Return all the entries at the directory dirPath.
func readDir(dirPath string) (entries []string, err error) {
	d, err := os.Open((dirPath))
	if err != nil {
		// File is really not found.
		if os.IsNotExist(err) {
			return nil, errFileNotFound
		}

		// File path cannot be verified since one of the parents is a file.
		if strings.Contains(err.Error(), "not a directory") {
			return nil, errFileNotFound
		}
		return nil, err
	}
	defer d.Close()

	for {
		// Read 1000 entries.
		fis, err := d.Readdir(1000)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		for _, fi := range fis {
			// Stat symbolic link and follow to get the final value.
			if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
				var st os.FileInfo
				st, err = os.Stat((path.Join(dirPath, fi.Name())))
				if err != nil {
					errorIf(err, "Unable to stat path %s", path.Join(dirPath, fi.Name()))
					continue
				}
				// Append to entries if symbolic link exists and is valid.
				if st.IsDir() {
					entries = append(entries, fi.Name()+slashSeparator)
				} else if st.Mode().IsRegular() {
					entries = append(entries, fi.Name())
				}
				continue
			}
			if fi.Mode().IsDir() {
				// Append "/" instead of "\" so that sorting is achieved as expected.
				entries = append(entries, fi.Name()+slashSeparator)
			} else if fi.Mode().IsRegular() {
				entries = append(entries, fi.Name())
			}
		}
	}
	return entries, nil
}
