//go:build linux
// +build linux

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
func IsLikelyMountPoint(path string) bool {
	s1, err := os.Lstat(path)
	if err != nil {
		return false
	}

	// A symlink can never be a mount point
	if s1.Mode()&os.ModeSymlink != 0 {
		return false
	}

	s2, err := os.Lstat(filepath.Dir(strings.TrimSuffix(path, "/")))
	if err != nil {
		return false
	}

	// If the directory has a different device as parent, then it is a mountpoint.
	ss1, ok1 := s1.Sys().(*syscall.Stat_t)
	ss2, ok2 := s2.Sys().(*syscall.Stat_t)
	return ok1 && ok2 &&
		// path/.. on a different device as path
		(ss1.Dev != ss2.Dev ||
			// path/.. is the same i-node as path - this check is for bind mounts.
			ss1.Ino == ss2.Ino)
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
	mounts := mountInfos{}
	scanner := bufio.NewReader(file)
	for {
		line, err := scanner.ReadString('\n')
		if err == io.EOF {
			break
		}

		fields := strings.Fields(line)
		if len(fields) != expectedNumFieldsPerLine {
			// ignore incorrect lines.
			continue
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
