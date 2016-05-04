// +build !linux,!darwin,!openbsd,!freebsd,!netbsd,!dragonfly

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

package main

import (
	"io"
	"os"
)

// Return all the entries at the directory dirPath.
func readDir(dirPath string) (entries []string, err error) {
	d, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer d.Close()

	for {
		fis, err := d.Readdir(1000)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		for _, fi := range fis {
			if fi.Mode().IsDir() {
				// append "/" instead of "\" so that sorting is done as expected.
				entries = append(entries, name+slashSeparator)
			} else if fi.Mode().IsRegular() {
				entries = append(entries, name)
			}
		}
	}
	return
}
