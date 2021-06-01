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

package cmd

import (
	"context"
	"io"
	"os"
	pathutil "path"
	"runtime"
	"strings"

	"github.com/minio/minio/internal/lock"
	"github.com/minio/minio/internal/logger"
)

// Removes only the file at given path does not remove
// any parent directories, handles long paths for
// windows automatically.
func fsRemoveFile(ctx context.Context, filePath string) (err error) {
	if filePath == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}

	if err = checkPathLength(filePath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err = os.Remove(filePath); err != nil {
		if err = osErrToFileErr(err); err != errFileNotFound {
			logger.LogIf(ctx, err)
		}
	}

	return err
}

// Removes all files and folders at a given path, handles
// long paths for windows automatically.
func fsRemoveAll(ctx context.Context, dirPath string) (err error) {
	if dirPath == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err = removeAll(dirPath); err != nil {
		if osIsPermission(err) {
			logger.LogIf(ctx, errVolumeAccessDenied)
			return errVolumeAccessDenied
		} else if isSysErrNotEmpty(err) {
			logger.LogIf(ctx, errVolumeNotEmpty)
			return errVolumeNotEmpty
		}
		logger.LogIf(ctx, err)
		return err
	}

	return nil
}

// Removes a directory only if its empty, handles long
// paths for windows automatically.
func fsRemoveDir(ctx context.Context, dirPath string) (err error) {
	if dirPath == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err = os.Remove((dirPath)); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrNotEmpty(err) {
			return errVolumeNotEmpty
		}
		logger.LogIf(ctx, err)
		return err
	}

	return nil
}

// Creates a new directory, parent dir should exist
// otherwise returns an error. If directory already
// exists returns an error. Windows long paths
// are handled automatically.
func fsMkdir(ctx context.Context, dirPath string) (err error) {
	if dirPath == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}

	if err = checkPathLength(dirPath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err = os.Mkdir((dirPath), 0777); err != nil {
		switch {
		case osIsExist(err):
			return errVolumeExists
		case osIsPermission(err):
			logger.LogIf(ctx, errDiskAccessDenied)
			return errDiskAccessDenied
		case isSysErrNotDir(err):
			// File path cannot be verified since
			// one of the parents is a file.
			logger.LogIf(ctx, errDiskAccessDenied)
			return errDiskAccessDenied
		case isSysErrPathNotFound(err):
			// Add specific case for windows.
			logger.LogIf(ctx, errDiskAccessDenied)
			return errDiskAccessDenied
		default:
			logger.LogIf(ctx, err)
			return err
		}
	}

	return nil
}

// fsStat is a low level call which validates input arguments
// and checks input length upto supported maximum. Does
// not perform any higher layer interpretation of files v/s
// directories. For higher level interpretation look at
// fsStatFileDir, fsStatFile, fsStatDir.
func fsStat(ctx context.Context, statLoc string) (os.FileInfo, error) {
	if statLoc == "" {
		logger.LogIf(ctx, errInvalidArgument)
		return nil, errInvalidArgument
	}
	if err := checkPathLength(statLoc); err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	fi, err := os.Stat(statLoc)
	if err != nil {
		return nil, err
	}

	return fi, nil
}

// Lookup if volume exists, returns volume attributes upon success.
func fsStatVolume(ctx context.Context, volume string) (os.FileInfo, error) {
	fi, err := fsStat(ctx, volume)
	if err != nil {
		if osIsNotExist(err) {
			return nil, errVolumeNotFound
		} else if osIsPermission(err) {
			return nil, errVolumeAccessDenied
		}
		return nil, err
	}

	if !fi.IsDir() {
		return nil, errVolumeAccessDenied
	}

	return fi, nil
}

// Lookup if directory exists, returns directory attributes upon success.
func fsStatDir(ctx context.Context, statDir string) (os.FileInfo, error) {
	fi, err := fsStat(ctx, statDir)
	if err != nil {
		err = osErrToFileErr(err)
		if err != errFileNotFound {
			logger.LogIf(ctx, err)
		}
		return nil, err
	}
	if !fi.IsDir() {
		return nil, errFileNotFound
	}
	return fi, nil
}

// Lookup if file exists, returns file attributes upon success.
func fsStatFile(ctx context.Context, statFile string) (os.FileInfo, error) {
	fi, err := fsStat(ctx, statFile)
	if err != nil {
		err = osErrToFileErr(err)
		if err != errFileNotFound {
			logger.LogIf(ctx, err)
		}
		return nil, err
	}
	if fi.IsDir() {
		return nil, errFileNotFound
	}
	return fi, nil
}

// Returns if the filePath is a regular file.
func fsIsFile(ctx context.Context, filePath string) bool {
	fi, err := fsStat(ctx, filePath)
	if err != nil {
		return false
	}
	return fi.Mode().IsRegular()
}

// Opens the file at given path, optionally from an offset. Upon success returns
// a readable stream and the size of the readable stream.
func fsOpenFile(ctx context.Context, readPath string, offset int64) (io.ReadCloser, int64, error) {
	if readPath == "" || offset < 0 {
		logger.LogIf(ctx, errInvalidArgument)
		return nil, 0, errInvalidArgument
	}
	if err := checkPathLength(readPath); err != nil {
		logger.LogIf(ctx, err)
		return nil, 0, err
	}

	fr, err := os.Open(readPath)
	if err != nil {
		return nil, 0, osErrToFileErr(err)
	}

	// Stat to get the size of the file at path.
	st, err := fr.Stat()
	if err != nil {
		err = osErrToFileErr(err)
		if err != errFileNotFound {
			logger.LogIf(ctx, err)
		}
		return nil, 0, err
	}

	// Verify if its not a regular file, since subsequent Seek is undefined.
	if !st.Mode().IsRegular() {
		return nil, 0, errIsNotRegular
	}

	// Seek to the requested offset.
	if offset > 0 {
		_, err = fr.Seek(offset, io.SeekStart)
		if err != nil {
			logger.LogIf(ctx, err)
			return nil, 0, err
		}
	}

	// Success.
	return fr, st.Size(), nil
}

// Creates a file and copies data from incoming reader.
func fsCreateFile(ctx context.Context, filePath string, reader io.Reader, fallocSize int64) (int64, error) {
	if filePath == "" || reader == nil {
		logger.LogIf(ctx, errInvalidArgument)
		return 0, errInvalidArgument
	}

	if err := checkPathLength(filePath); err != nil {
		logger.LogIf(ctx, err)
		return 0, err
	}

	if err := mkdirAll(pathutil.Dir(filePath), 0777); err != nil {
		switch {
		case osIsPermission(err):
			return 0, errFileAccessDenied
		case osIsExist(err):
			return 0, errFileAccessDenied
		case isSysErrIO(err):
			return 0, errFaultyDisk
		case isSysErrInvalidArg(err):
			return 0, errUnsupportedDisk
		case isSysErrNoSpace(err):
			return 0, errDiskFull
		}
		return 0, err
	}

	flags := os.O_CREATE | os.O_WRONLY
	if globalFSOSync {
		flags = flags | os.O_SYNC
	}
	writer, err := lock.Open(filePath, flags, 0666)
	if err != nil {
		return 0, osErrToFileErr(err)
	}
	defer writer.Close()

	bytesWritten, err := io.Copy(writer, reader)
	if err != nil {
		logger.LogIf(ctx, err)
		return 0, err
	}

	return bytesWritten, nil
}

// Renames source path to destination path, creates all the
// missing parents if they don't exist.
func fsRenameFile(ctx context.Context, sourcePath, destPath string) error {
	if err := checkPathLength(sourcePath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	if err := checkPathLength(destPath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err := renameAll(sourcePath, destPath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	return nil
}

func deleteFile(basePath, deletePath string, recursive bool) error {
	if basePath == "" || deletePath == "" {
		return nil
	}
	isObjectDir := HasSuffix(deletePath, SlashSeparator)
	basePath = pathutil.Clean(basePath)
	deletePath = pathutil.Clean(deletePath)
	if !strings.HasPrefix(deletePath, basePath) || deletePath == basePath {
		return nil
	}

	var err error
	if recursive {
		os.RemoveAll(deletePath)
	} else {
		err = os.Remove(deletePath)
	}
	if err != nil {
		switch {
		case isSysErrNotEmpty(err):
			// if object is a directory, but if its not empty
			// return FileNotFound to indicate its an empty prefix.
			if isObjectDir {
				return errFileNotFound
			}
			// Ignore errors if the directory is not empty. The server relies on
			// this functionality, and sometimes uses recursion that should not
			// error on parent directories.
			return nil
		case osIsNotExist(err):
			return errFileNotFound
		case osIsPermission(err):
			return errFileAccessDenied
		case isSysErrIO(err):
			return errFaultyDisk
		default:
			return err
		}
	}

	deletePath = pathutil.Dir(deletePath)

	// Delete parent directory obviously not recursively. Errors for
	// parent directories shouldn't trickle down.
	deleteFile(basePath, deletePath, false)

	return nil
}

// fsDeleteFile is a wrapper for deleteFile(), after checking the path length.
func fsDeleteFile(ctx context.Context, basePath, deletePath string) error {
	if err := checkPathLength(basePath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err := checkPathLength(deletePath); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	if err := deleteFile(basePath, deletePath, false); err != nil {
		if err != errFileNotFound {
			logger.LogIf(ctx, err)
		}
		return err
	}
	return nil
}

// fsRemoveMeta safely removes a locked file and takes care of Windows special case
func fsRemoveMeta(ctx context.Context, basePath, deletePath, tmpDir string) error {
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

		fsRenameFile(ctx, deletePath, tmpPath)

		// Proceed to deleting the directory if empty
		fsDeleteFile(ctx, basePath, pathutil.Dir(deletePath))

		// Finally delete the renamed file.
		return fsDeleteFile(ctx, tmpDir, tmpPath)
	}
	return fsDeleteFile(ctx, basePath, deletePath)
}
