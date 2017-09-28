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
	"fmt"
	"io"
	"os"
	pathutil "path"
	"runtime"
)

// Removes only the file at given path does not remove
// any parent directories, handles long paths for
// windows automatically.
func fsRemoveFile(filePath string) (err error) {
	if filePath == "" {
		return traceError(errInvalidArgument)
	}

	if err = checkPathLength(filePath); err != nil {
		return traceError(err)
	}

	if err = os.Remove((filePath)); err != nil {
		if os.IsNotExist(err) {
			return traceError(errFileNotFound)
		} else if os.IsPermission(err) {
			return traceError(errFileAccessDenied)
		}
		return traceError(err)
	}

	return nil
}

// Removes all files and folders at a given path, handles
// long paths for windows automatically.
func fsRemoveAll(dirPath string) (err error) {
	if dirPath == "" {
		return traceError(errInvalidArgument)
	}

	if err = checkPathLength(dirPath); err != nil {
		return traceError(err)
	}

	if err = os.RemoveAll(dirPath); err != nil {
		if os.IsPermission(err) {
			return traceError(errVolumeAccessDenied)
		} else if isSysErrNotEmpty(err) {
			return traceError(errVolumeNotEmpty)
		}
		return traceError(err)
	}

	return nil
}

// Removes a directory only if its empty, handles long
// paths for windows automatically.
func fsRemoveDir(dirPath string) (err error) {
	if dirPath == "" {
		return traceError(errInvalidArgument)
	}

	if err = checkPathLength(dirPath); err != nil {
		return traceError(err)
	}

	if err = os.Remove((dirPath)); err != nil {
		if os.IsNotExist(err) {
			return traceError(errVolumeNotFound)
		} else if isSysErrNotEmpty(err) {
			return traceError(errVolumeNotEmpty)
		}
		return traceError(err)
	}

	return nil
}

// Creates a new directory, parent dir should exist
// otherwise returns an error. If directory already
// exists returns an error. Windows long paths
// are handled automatically.
func fsMkdir(dirPath string) (err error) {
	if dirPath == "" {
		return traceError(errInvalidArgument)
	}

	if err = checkPathLength(dirPath); err != nil {
		return traceError(err)
	}

	if err = os.Mkdir((dirPath), 0777); err != nil {
		if os.IsExist(err) {
			return traceError(errVolumeExists)
		} else if os.IsPermission(err) {
			return traceError(errDiskAccessDenied)
		} else if isSysErrNotDir(err) {
			// File path cannot be verified since
			// one of the parents is a file.
			return traceError(errDiskAccessDenied)
		} else if isSysErrPathNotFound(err) {
			// Add specific case for windows.
			return traceError(errDiskAccessDenied)
		}
		return traceError(err)
	}

	return nil
}

func fsStat(statLoc string) (os.FileInfo, error) {
	if statLoc == "" {
		return nil, traceError(errInvalidArgument)
	}
	if err := checkPathLength(statLoc); err != nil {
		return nil, traceError(err)
	}
	fi, err := osStat((statLoc))
	if err != nil {
		return nil, traceError(err)
	}

	return fi, nil
}

// Lookup if directory exists, returns directory
// attributes upon success.
func fsStatDir(statDir string) (os.FileInfo, error) {
	fi, err := fsStat(statDir)
	if err != nil {
		err = errorCause(err)
		if os.IsNotExist(err) {
			return nil, traceError(errVolumeNotFound)
		} else if os.IsPermission(err) {
			return nil, traceError(errVolumeAccessDenied)
		}
		return nil, traceError(err)
	}

	if !fi.IsDir() {
		return nil, traceError(errVolumeAccessDenied)
	}

	return fi, nil
}

// Lookup if file exists, returns file attributes upon success
func fsStatFile(statFile string) (os.FileInfo, error) {
	fi, err := fsStat(statFile)
	if err != nil {
		err = errorCause(err)
		if os.IsNotExist(err) {
			return nil, traceError(errFileNotFound)
		} else if os.IsPermission(err) {
			return nil, traceError(errFileAccessDenied)
		} else if isSysErrNotDir(err) {
			return nil, traceError(errFileAccessDenied)
		} else if isSysErrPathNotFound(err) {
			return nil, traceError(errFileNotFound)
		}
		return nil, traceError(err)
	}
	if fi.IsDir() {
		return nil, traceError(errFileAccessDenied)
	}
	return fi, nil
}

// Opens the file at given path, optionally from an offset. Upon success returns
// a readable stream and the size of the readable stream.
func fsOpenFile(readPath string, offset int64) (io.ReadCloser, int64, error) {
	if readPath == "" || offset < 0 {
		return nil, 0, traceError(errInvalidArgument)
	}
	if err := checkPathLength(readPath); err != nil {
		return nil, 0, traceError(err)
	}

	fr, err := os.Open((readPath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, traceError(errFileNotFound)
		} else if os.IsPermission(err) {
			return nil, 0, traceError(errFileAccessDenied)
		} else if isSysErrNotDir(err) {
			// File path cannot be verified since one of the parents is a file.
			return nil, 0, traceError(errFileAccessDenied)
		} else if isSysErrPathNotFound(err) {
			// Add specific case for windows.
			return nil, 0, traceError(errFileNotFound)
		}
		return nil, 0, traceError(err)
	}

	// Stat to get the size of the file at path.
	st, err := osStat((readPath))
	if err != nil {
		return nil, 0, traceError(err)
	}

	// Verify if its not a regular file, since subsequent Seek is undefined.
	if !st.Mode().IsRegular() {
		return nil, 0, traceError(errIsNotRegular)
	}

	// Seek to the requested offset.
	if offset > 0 {
		_, err = fr.Seek(offset, os.SEEK_SET)
		if err != nil {
			return nil, 0, traceError(err)
		}
	}

	// Success.
	return fr, st.Size(), nil
}

// Creates a file and copies data from incoming reader. Staging buffer is used by io.CopyBuffer.
func fsCreateFile(filePath string, reader io.Reader, buf []byte, fallocSize int64) (int64, error) {
	if filePath == "" || reader == nil {
		return 0, traceError(errInvalidArgument)
	}

	if err := checkPathLength(filePath); err != nil {
		return 0, traceError(err)
	}

	if err := os.MkdirAll(pathutil.Dir(filePath), 0777); err != nil {
		return 0, traceError(err)
	}

	if err := checkDiskFree(pathutil.Dir(filePath), fallocSize); err != nil {
		return 0, traceError(err)
	}

	writer, err := os.OpenFile((filePath), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		// File path cannot be verified since one of the parents is a file.
		if isSysErrNotDir(err) {
			return 0, traceError(errFileAccessDenied)
		}
		return 0, err
	}
	defer writer.Close()

	// Fallocate only if the size is final object is known.
	if fallocSize > 0 {
		if err = fsFAllocate(int(writer.Fd()), 0, fallocSize); err != nil {
			return 0, traceError(err)
		}
	}

	var bytesWritten int64
	if buf != nil {
		bytesWritten, err = io.CopyBuffer(writer, reader, buf)
		if err != nil {
			return 0, traceError(err)
		}
	} else {
		bytesWritten, err = io.Copy(writer, reader)
		if err != nil {
			return 0, traceError(err)
		}
	}
	return bytesWritten, nil
}

// Removes uploadID at destination path.
func fsRemoveUploadIDPath(basePath, uploadIDPath string) error {
	if basePath == "" || uploadIDPath == "" {
		return traceError(errInvalidArgument)
	}
	if err := checkPathLength(basePath); err != nil {
		return traceError(err)
	}
	if err := checkPathLength(uploadIDPath); err != nil {
		return traceError(err)
	}

	// List all the entries in uploadID.
	entries, err := readDir(uploadIDPath)
	if err != nil && err != errFileNotFound {
		return traceError(err)
	}

	// Delete all the entries obtained from previous readdir.
	for _, entryPath := range entries {
		err = fsDeleteFile(basePath, pathJoin(uploadIDPath, entryPath))
		if err != nil && err != errFileNotFound {
			return traceError(err)
		}
	}

	fsRemoveDir(uploadIDPath)
	return nil
}

// fsFAllocate is similar to Fallocate but provides a convenient
// wrapper to handle various operating system specific errors.
func fsFAllocate(fd int, offset int64, len int64) (err error) {
	e := Fallocate(fd, offset, len)
	// Ignore errors when Fallocate is not supported in the current system
	if e != nil && !isSysErrNoSys(e) && !isSysErrOpNotSupported(e) {
		switch {
		case isSysErrNoSpace(e):
			err = errDiskFull
		case isSysErrIO(e):
			err = e
		default:
			// For errors: EBADF, EINTR, EINVAL, ENODEV, EPERM, ESPIPE  and ETXTBSY
			// Appending was failed anyway, returns unexpected error
			err = errUnexpected
		}
		return err
	}

	return nil
}

// Renames source path to destination path, creates all the
// missing parents if they don't exist.
func fsRenameFile(sourcePath, destPath string) error {
	if err := checkPathLength(sourcePath); err != nil {
		return traceError(err)
	}
	if err := checkPathLength(destPath); err != nil {
		return traceError(err)
	}
	// Verify if source path exists.
	if _, err := os.Stat((sourcePath)); err != nil {
		if os.IsNotExist(err) {
			return traceError(errFileNotFound)
		} else if os.IsPermission(err) {
			return traceError(errFileAccessDenied)
		} else if isSysErrPathNotFound(err) {
			return traceError(errFileNotFound)
		} else if isSysErrNotDir(err) {
			// File path cannot be verified since one of the parents is a file.
			return traceError(errFileAccessDenied)
		}
		return traceError(err)
	}
	if err := os.MkdirAll(pathutil.Dir(destPath), 0777); err != nil {
		return traceError(err)
	}
	if err := os.Rename((sourcePath), (destPath)); err != nil {
		if isSysErrCrossDevice(err) {
			return traceError(fmt.Errorf("%s (%s)->(%s)", errCrossDeviceLink, sourcePath, destPath))
		}
		return traceError(err)
	}
	return nil
}

// fsDeleteFile is a wrapper for deleteFile(), after checking the path length.
func fsDeleteFile(basePath, deletePath string) error {
	if err := checkPathLength(basePath); err != nil {
		return traceError(err)
	}

	if err := checkPathLength(deletePath); err != nil {
		return traceError(err)
	}

	return deleteFile(basePath, deletePath)
}

// fsRemoveMeta safely removes a locked file and takes care of Windows special case
func fsRemoveMeta(basePath, deletePath, tmpDir string) error {
	// Special case for windows please read through.
	if runtime.GOOS == globalWindowsOSName {
		// Ordinarily windows does not permit deletion or renaming of files still
		// in use, but if all open handles to that file were opened with FILE_SHARE_DELETE
		// then it can permit renames and deletions of open files.
		//
		// There are however some gotchas with this, and it is worth listing them here.
		// Firstly, Windows never allows you to really delete an open file, rather it is
		// flagged as delete pending and its entry in its directory remains visible
		// (though no new file handles may be opened to it) and when the very last
		// open handle to the file in the system is closed, only then is it truly
		// deleted. Well, actually only sort of truly deleted, because Windows only
		// appears to remove the file entry from the directory, but in fact that
		// entry is merely hidden and actually still exists and attempting to create
		// a file with the same name will return an access denied error. How long it
		// silently exists for depends on a range of factors, but put it this way:
		// if your code loops creating and deleting the same file name as you might
		// when operating a lock file, you're going to see lots of random spurious
		// access denied errors and truly dismal lock file performance compared to POSIX.
		//
		// We work-around these un-POSIX file semantics by taking a dual step to
		// deleting files. Firstly, it renames the file to tmp location into multipartTmpBucket
		// We always open files with FILE_SHARE_DELETE permission enabled, with that
		// flag Windows permits renaming and deletion, and because the name was changed
		// to a very random name somewhere not in its origin directory before deletion,
		// you don't see those unexpected random errors when creating files with the
		// same name as a recently deleted file as you do anywhere else on Windows.
		// Because the file is probably not in its original containing directory any more,
		// deletions of that directory will not fail with "directory not empty" as they
		// otherwise normally would either.

		tmpPath := pathJoin(tmpDir, mustGetUUID())

		fsRenameFile(deletePath, tmpPath)

		// Proceed to deleting the directory if empty
		fsDeleteFile(basePath, pathutil.Dir(deletePath))

		// Finally delete the renamed file.
		return fsDeleteFile(tmpDir, tmpPath)
	}
	return fsDeleteFile(basePath, deletePath)
}
