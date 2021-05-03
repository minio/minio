// +build windows

/*
 * MinIO Object Storage (c) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"errors"
	"io/ioutil"
	"os"

	"github.com/djherbis/atime"
	"golang.org/x/sys/windows/registry"
)

// Return error if Atime is disabled on the O/S
func checkAtimeSupport(dir string) (err error) {
	file, err := ioutil.TempFile(dir, "prefix")
	if err != nil {
		return
	}
	defer os.Remove(file.Name())
	defer file.Close()
	finfo1, err := os.Stat(file.Name())
	if err != nil {
		return
	}
	atime.Get(finfo1)

	k, err := registry.OpenKey(registry.LOCAL_MACHINE, `SYSTEM\CurrentControlSet\Control\FileSystem`, registry.QUERY_VALUE)
	if err != nil {
		return
	}
	defer k.Close()

	setting, _, err := k.GetIntegerValue("NtfsDisableLastAccessUpdate")
	if err != nil {
		return
	}

	lowSetting := setting & 0xFFFF
	if lowSetting != uint64(0x0000) && lowSetting != uint64(0x0002) {
		return errors.New("Atime not supported")
	}
	return
}
