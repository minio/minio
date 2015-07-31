/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"fmt"
	"io/ioutil"
	"os"
	"syscall"

	"github.com/minio/minio/pkg/iodine"
)

// isUsable provides a comprehensive way of knowing if the provided mountPath is mounted and writable
func isUsable(mountPath string) (bool, error) {
	mntpoint, err := os.Stat(mountPath)
	if err != nil {
		return false, iodine.New(err, nil)
	}
	parent, err := os.Stat("/")
	if err != nil {
		return false, iodine.New(err, nil)
	}
	mntpointSt := mntpoint.Sys().(*syscall.Stat_t)
	parentSt := parent.Sys().(*syscall.Stat_t)

	if mntpointSt.Dev == parentSt.Dev {
		return false, iodine.New(fmt.Errorf("Not mounted %s", mountPath), nil)
	}
	testFile, err := ioutil.TempFile(mountPath, "writetest-")
	if err != nil {
		return false, iodine.New(err, nil)
	}
	testFileName := testFile.Name()
	// close the file, to avoid leaky fd's
	testFile.Close()
	if err := os.Remove(testFileName); err != nil {
		return false, iodine.New(err, nil)
	}
	return true, nil
}
