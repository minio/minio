// +build linux

/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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

package mountinfo

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const (
	// Number of fields per line in /proc/mounts as per the fstab man page.
	expectedNumFieldsPerLine = 6
	// Location of the mount file to use
	procMountsPath = "/proc/mounts"
)

// IsLikelyMountPoint determines if a directory is a mountpoint.
// It is fast but not necessarily ALWAYS correct. If the path is in fact
// a bind mount from one part of a mount to another it will not be detected.
// mkdir /tmp/a /tmp/b; mount --bin /tmp/a /tmp/b; IsLikelyMountPoint("/tmp/b")
// will return false. When in fact /tmp/b is a mount point. If this situation
// if of interest to you, don't use this function...
func IsLikelyMountPoint(file string) bool {
	stat, err := os.Stat(file)
	if err != nil {
		return false
	}

	rootStat, err := os.Lstat(filepath.Dir(strings.TrimSuffix(file, "/")))
	if err != nil {
		return false
	}

	// If the directory has a different device as parent, then it is a mountpoint.
	return stat.Sys().(*syscall.Stat_t).Dev != rootStat.Sys().(*syscall.Stat_t).Dev
}

// CheckCrossDevice - check if any list of paths has any sub-mounts at /proc/mounts.
func CheckCrossDevice(absPaths []string) error {
	return checkCrossDevice(absPaths, procMountsPath)
}

// Check cross device is an internal function.
func checkCrossDevice(absPaths []string, mountsPath string) error {
	mounts, err := readProcMounts(mountsPath)
	if err != nil {
		return err
	}
	for _, path := range absPaths {
		if err := mounts.checkCrossMounts(path); err != nil {
			return err
		}
	}
	return nil
}

// CheckCrossDevice - check if given path has any sub-mounts in the input mounts list.
func (mts mountInfos) checkCrossMounts(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("Invalid argument, path (%s) is expected to be absolute", path)
	}
	var crossMounts mountInfos
	for _, mount := range mts {
		// Add a separator to indicate that this is a proper mount-point.
		// This is to avoid a situation where prefix is '/tmp/fsmount'
		// and mount path is /tmp/fs. In such a scenario we need to check for
		// `/tmp/fs/` to be a common prefix amount other mounts.
		mpath := strings.TrimSuffix(mount.Path, "/") + "/"
		ppath := strings.TrimSuffix(path, "/") + "/"
		if strings.HasPrefix(mpath, ppath) {
			// At this point if the mount point has a common prefix two conditions can happen.
			// - mount.Path matches exact with `path` means we can proceed no error here.
			// - mount.Path doesn't match (means cross-device mount), should error out.
			if mount.Path != path {
				crossMounts = append(crossMounts, mount)
			}
		}
	}
	msg := `Cross-device mounts detected on path (%s) at following locations %s. Export path should not have any sub-mounts, refusing to start.`
	if len(crossMounts) > 0 {
		// if paths didn't match then we do have cross-device mount.
		return fmt.Errorf(msg, path, crossMounts)
	}
	return nil
}

// readProcMounts reads the given mountFilePath (normally /proc/mounts) and produces a hash
// of the contents.  If the out argument is not nil, this fills it with MountPoint structs.
func readProcMounts(mountFilePath string) (mountInfos, error) {
	file, err := os.Open(mountFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return parseMountFrom(file)
}

func parseMountFrom(file io.Reader) (mountInfos, error) {
	var mounts = mountInfos{}
	scanner := bufio.NewReader(file)
	for {
		line, err := scanner.ReadString('\n')
		if err == io.EOF {
			break
		}
		fields := strings.Fields(line)
		if len(fields) != expectedNumFieldsPerLine {
			return nil, fmt.Errorf("wrong number of fields (expected %d, got %d): %s", expectedNumFieldsPerLine, len(fields), line)
		}

		// Freq should be an integer.
		if _, err := strconv.Atoi(fields[4]); err != nil {
			return nil, err
		}

		// Pass should be an integer.
		if _, err := strconv.Atoi(fields[5]); err != nil {
			return nil, err
		}

		mounts = append(mounts, mountInfo{
			Device:  fields[0],
			Path:    fields[1],
			FSType:  fields[2],
			Options: strings.Split(fields[3], ","),
			Freq:    fields[4],
			Pass:    fields[5],
		})
	}
	return mounts, nil
}
