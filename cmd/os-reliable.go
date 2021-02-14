/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

struct full_inode_id {
	ulong inode_id;
	ulong inode_supplemental;
};

struct rename_ioctl_pack {
	ulong inode_id;
	ulong inode_supplemental;
	char target_name[256];
};

static int WekaRenameFast(char *src_fullname, char *dst_dir, char *dst_name) {
	int fd, err;
	struct full_inode_id dst_dir_inode;
	struct rename_ioctl_pack dst_ioctl_pack;

	fd = open(dst_dir, O_RDONLY | O_DIRECTORY);
	if (fd < 0) {
		printf("Failed to open dest dir %s\n", dst_dir);
		return -1;
	}

	memset(&dst_dir_inode, 0, sizeof(dst_dir_inode));
	err = ioctl(fd, 'STAT', &dst_dir_inode);
	close(fd);
	if (err) {
		printf("Failed to perform internal ioctl: base %s. Status %d errno %d.\n", dst_dir, err, errno);
		return -1;
	}

	fd = open(src_fullname, O_RDONLY);
	if (fd < 0) {
		printf("Failed to open src file %s\n", src_fullname);
		return -1;
	}

	memset(&dst_ioctl_pack, 0, sizeof(dst_ioctl_pack));
	dst_ioctl_pack.inode_id = dst_dir_inode.inode_id;
	dst_ioctl_pack.inode_supplemental = dst_dir_inode.inode_supplemental;
	strcpy(&dst_ioctl_pack.target_name[0], dst_name);
	err = ioctl(fd, 'RNME', &dst_ioctl_pack);
	close(fd);
	if (err) {
		printf("Failed to perform internal ioctl: target %s. Status %d errno %d.\n", src_fullname, err, errno);
		return -1;
	}

	return err;
}
*/
import "C"
import "unsafe"

import (
	"fmt"
	"os"
	"path"
)

func fsRenameFast(srcPath, dstPath string) (err error){
	dstDir, dstFile := path.Split(dstPath)

	cSrcPath := C.CString(srcPath)
	cDstDir := C.CString(dstDir)
	cDstFile := C.CString(dstFile)

	rv, err := C.WekaRenameFast(cSrcPath, cDstDir, cDstFile)

	C.free(unsafe.Pointer(cSrcPath))
	C.free(unsafe.Pointer(cDstDir))
	C.free(unsafe.Pointer(cDstFile))

	if rv == 0 {
		return nil
	}

	return err;
}

// Wrapper functions to os.RemoveAll, which calls reliableRemoveAll
// this is to ensure that if there is a racy parent directory
// create in between we can simply retry the operation.
func removeAll(dirPath string) (err error) {
	if dirPath == "" {
		return errInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		return err
	}

	if err = reliableRemoveAll(dirPath); err != nil {
		switch {
		case isSysErrNotDir(err):
			// File path cannot be verified since one of
			// the parents is a file.
			return errFileAccessDenied
		case isSysErrPathNotFound(err):
			// This is a special case should be handled only for
			// windows, because windows API does not return "not a
			// directory" error message. Handle this specifically
			// here.
			return errFileAccessDenied
		}
	}
	return err
}

// Reliably retries os.RemoveAll if for some reason os.RemoveAll returns
// syscall.ENOTEMPTY (children has files).
func reliableRemoveAll(dirPath string) (err error) {
	i := 0
	for {
		// Removes all the directories and files.
		if err = os.RemoveAll(dirPath); err != nil {
			// Retry only for the first retryable error.
			if isSysErrNotEmpty(err) && i == 0 {
				i++
				continue
			}
		}
		break
	}
	return err
}

// Wrapper functions to os.MkdirAll, which calls reliableMkdirAll
// this is to ensure that if there is a racy parent directory
// delete in between we can simply retry the operation.
func mkdirAll(dirPath string, mode os.FileMode) (err error) {
	if dirPath == "" {
		return errInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		return err
	}

	if err = reliableMkdirAll(dirPath, mode); err != nil {
		// File path cannot be verified since one of the parents is a file.
		if isSysErrNotDir(err) {
			return errFileAccessDenied
		} else if isSysErrPathNotFound(err) {
			// This is a special case should be handled only for
			// windows, because windows API does not return "not a
			// directory" error message. Handle this specifically here.
			return errFileAccessDenied
		}
	}
	return err
}

// Reliably retries os.MkdirAll if for some reason os.MkdirAll returns
// syscall.ENOENT (parent does not exist).
func reliableMkdirAll(dirPath string, mode os.FileMode) (err error) {
	i := 0
	for {
		// Creates all the parent directories, with mode 0777 mkdir honors system umask.
		if err = os.MkdirAll(dirPath, mode); err != nil {
			// Retry only for the first retryable error.
			if osIsNotExist(err) && i == 0 {
				i++
				continue
			}
		}
		break
	}
	return err
}

// Wrapper function to os.Rename, which calls reliableMkdirAll
// and reliableRenameAll. This is to ensure that if there is a
// racy parent directory delete in between we can simply retry
// the operation.
func renameAll(srcFilePath, dstFilePath string) (err error) {
	if srcFilePath == "" || dstFilePath == "" {
		return errInvalidArgument
	}

	if err = checkPathLength(srcFilePath); err != nil {
		return err
	}
	if err = checkPathLength(dstFilePath); err != nil {
		return err
	}

	if err = reliableRename(srcFilePath, dstFilePath); err != nil {
		switch {
		case isSysErrNotDir(err) && !osIsNotExist(err):
			// Windows can have both isSysErrNotDir(err) and osIsNotExist(err) returning
			// true if the source file path contains an inexistant directory. In that case,
			// we want to return errFileNotFound instead, which will honored in subsequent
			// switch cases
			return errFileAccessDenied
		case isSysErrPathNotFound(err):
			// This is a special case should be handled only for
			// windows, because windows API does not return "not a
			// directory" error message. Handle this specifically here.
			return errFileAccessDenied
		case isSysErrCrossDevice(err):
			return fmt.Errorf("%w (%s)->(%s)", errCrossDeviceLink, srcFilePath, dstFilePath)
		case osIsNotExist(err):
			return errFileNotFound
		case osIsExist(err):
			// This is returned only when destination is a directory and we
			// are attempting a rename from file to directory.
			return errIsNotRegular
		default:
			return err
		}
	}
	return nil
}

// Reliably retries os.RenameAll if for some reason os.RenameAll returns
// syscall.ENOENT (parent does not exist).
func reliableRename(srcFilePath, dstFilePath string) (err error) {
	if err = reliableMkdirAll(path.Dir(dstFilePath), 0777); err != nil {
		return err
	}

	if err = fsRenameFast(srcFilePath, dstFilePath); err == nil {
		return nil;
	}

	i := 0
	for {
		// After a successful parent directory create attempt a renameAll.
		if err = os.Rename(srcFilePath, dstFilePath); err != nil {
			// Retry only for the first retryable error.
			if osIsNotExist(err) && i == 0 {
				i++
				continue
			}
		}
		break
	}
	return err
}
