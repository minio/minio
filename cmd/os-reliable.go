/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"fmt"
	"os"
	"path"
)

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
			if os.IsNotExist(err) && i == 0 {
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
		if isSysErrNotDir(err) {
			return errFileAccessDenied
		} else if isSysErrPathNotFound(err) {
			// This is a special case should be handled only for
			// windows, because windows API does not return "not a
			// directory" error message. Handle this specifically here.
			return errFileAccessDenied
		} else if isSysErrCrossDevice(err) {
			return fmt.Errorf("%s (%s)->(%s)", errCrossDeviceLink, srcFilePath, dstFilePath)
		} else if os.IsNotExist(err) {
			return errFileNotFound
		}
	}
	return err
}

// Reliably retries os.RenameAll if for some reason os.RenameAll returns
// syscall.ENOENT (parent does not exist).
func reliableRename(srcFilePath, dstFilePath string) (err error) {
	i := 0
	for {
		if err = reliableMkdirAll(path.Dir(dstFilePath), 0777); err != nil {
			return err
		}
		// After a successful parent directory create attempt a renameAll.
		if err = os.Rename(srcFilePath, dstFilePath); err != nil {
			// Retry only for the first retryable error.
			if os.IsNotExist(err) && i == 0 {
				i++
				continue
			}
		}
		break
	}
	return err
}
