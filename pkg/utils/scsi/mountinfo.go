// +build linux

/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

package scsi

import (
	"bufio"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/minio/minio/pkg/iodine"
)

var supportedFSType = map[string]bool{
	"ext4":  true,
	"xfs":   true,
	"ext3":  true,
	"btrfs": true,
	"tmpfs": true,
	"nfs":   true,
}

func isSupportedType(t string) bool {
	_, ok := supportedFSType[t]
	return ok
}

// GetMountInfo - get mount info map
func GetMountInfo() (map[string]Mountinfo, error) {
	f, err := os.Open("/etc/mtab")
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	mntEnt := make(map[string]Mountinfo)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		mtabEnt := strings.Split(scanner.Text(), " ")
		mntInfo := Mountinfo{}
		if len(mtabEnt) == 6 {
			var err error
			if !isSupportedType(mtabEnt[2]) {
				continue
			}
			mntInfo.FSName, err = filepath.EvalSymlinks(mtabEnt[0])
			if err != nil {
				continue
			}
			mntInfo.Dir = mtabEnt[1]
			mntInfo.Type = mtabEnt[2]
			mntInfo.Opts = mtabEnt[3]
			mntInfo.Freq, err = strconv.Atoi(mtabEnt[4])
			if err != nil {
				continue
			}
			mntInfo.Passno, err = strconv.Atoi(mtabEnt[5])
			if err != nil {
				continue
			}
			mntEnt[mntInfo.FSName] = mntInfo
		}
	}
	return mntEnt, nil
}

// IsUsable provides a comprehensive way of knowing if the provided mountPath is mounted and writable
func IsUsable(mountPath string) (bool, error) {
	mntpoint, err := os.Stat(mountPath)
	if err != nil {
		return false, iodine.New(err, nil)
	}
	parent, err := os.Stat(filepath.Join(mountPath, ".."))
	if err != nil {
		return false, iodine.New(err, nil)
	}
	mntpointSt := mntpoint.Sys().(*syscall.Stat_t)
	parentSt := parent.Sys().(*syscall.Stat_t)

	if mntpointSt.Dev == parentSt.Dev {
		return false, iodine.New(errors.New("not mounted"), nil)
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
