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
	"io/ioutil"
	"os"

	"github.com/minio/minio/pkg/probe"
)

// isUsable provides a comprehensive way of knowing if the provided mountPath is mounted and writable
func isUsable(mountPath string) (bool, *probe.Error) {
	_, e := os.Stat(mountPath)
	if e != nil {
		e := os.MkdirAll(mountPath, 0700)
		if e != nil {
			return false, probe.NewError(e)
		}
	}

	testFile, e := ioutil.TempFile(mountPath, "writetest-")
	if e != nil {
		return false, probe.NewError(e)
	}
	defer testFile.Close()

	testFileName := testFile.Name()
	if e := os.Remove(testFileName); e != nil {
		return false, probe.NewError(e)
	}
	return true, nil
}
