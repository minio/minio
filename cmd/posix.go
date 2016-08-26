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
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	slashpath "path"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/minio/minio/pkg/disk"
)

const (
	fsMinSpacePercent = 5
	maxAllowedIOError = 5
)

// posix - implements StorageAPI interface.
type posix struct {
	ioErrCount  int32 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	diskPath    string
	minFreeDisk int64
}

var errFaultyDisk = errors.New("Faulty disk")

// checkPathLength - returns error if given path name length more than 255
func checkPathLength(pathName string) error {
	// Apple OS X path length is limited to 1016
	if runtime.GOOS == "darwin" && len(pathName) > 1016 {
		return errFileNameTooLong
	}

	// Check each path segment length is > 255
	for len(pathName) > 0 && pathName != "." && pathName != "/" {
		dir, file := slashpath.Dir(pathName), slashpath.Base(pathName)

		if len(file) > 255 {
			return errFileNameTooLong
		}

		pathName = dir
	} // Success.
	return nil
}

// isDirEmpty - returns whether given directory is empty or not.
func isDirEmpty(dirname string) bool {
	f, err := os.Open(dirname)
	if err != nil {
		errorIf(err, "Unable to access directory.")
		return false
	}
	defer f.Close()
	// List one entry.
	_, err = f.Readdirnames(1)
	if err != nil {
		if err == io.EOF {
			// Returns true if we have reached EOF, directory is indeed empty.
			return true
		}
		errorIf(err, "Unable to list directory.")
		return false
	}
	// Directory is not empty.
	return false
}

// Initialize a new storage disk.
func newPosix(diskPath string) (StorageAPI, error) {
	if diskPath == "" {
		return nil, errInvalidArgument
	}
	var err error
	// Disallow relative paths, figure out absolute paths.
	diskPath, err = filepath.Abs(diskPath)
	if err != nil {
		return nil, err
	}
	fs := &posix{
		diskPath:    diskPath,
		minFreeDisk: fsMinSpacePercent, // Minimum 5% disk should be free.
	}
	st, err := os.Stat(preparePath(diskPath))
	if err != nil {
		if os.IsNotExist(err) {
			// Disk not found create it.
			err = os.MkdirAll(diskPath, 0777)
			return fs, err
		}
		return fs, err
	}
	if !st.IsDir() {
		return fs, syscall.ENOTDIR
	}
	return fs, nil
}

// getDiskInfo returns given disk information.
func getDiskInfo(diskPath string) (di disk.Info, err error) {
	if err = checkPathLength(diskPath); err == nil {
		di, err = disk.GetInfo(diskPath)
	}

	if os.IsNotExist(err) {
		err = errDiskNotFound
	}

	return di, err
}

// checkDiskFree verifies if disk path has sufficient minimum free disk space and files.
func checkDiskFree(diskPath string, minFreeDisk int64) (err error) {
	di, err := getDiskInfo(diskPath)
	if err != nil {
		return err
	}

	// Remove 5% from total space for cumulative disk
	// space used for journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= minFreeDisk {
		return errDiskFull
	}

	// Some filesystems do not implement a way to provide total inodes available, instead inodes
	// are allocated based on available disk space. For example CephFS, StoreNext CVFS, AzureFile driver.
	// Allow for the available disk to be separately validate and we will validate inodes only if
	// total inodes are provided by the underlying filesystem.
	if di.Files != 0 {
		availableFiles := (float64(di.Ffree) / (float64(di.Files) - (0.05 * float64(di.Files)))) * 100
		if int64(availableFiles) <= minFreeDisk {
			return errDiskFull
		}
	}

	// Success.
	return nil
}

// getVolDir - will convert incoming volume names to
// corresponding valid volume names on the backend in a platform
// compatible way for all operating systems. If volume is not found
// an error is generated.
func (s *posix) getVolDir(volume string) (string, error) {
	if !isValidVolname(volume) {
		return "", errInvalidArgument
	}
	volumeDir := pathJoin(s.diskPath, volume)
	return volumeDir, nil
}

// Make a volume entry.
func (s *posix) MakeVol(volume string) (err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return errFaultyDisk
	}

	// Validate if disk is free.
	if err = checkDiskFree(s.diskPath, s.minFreeDisk); err != nil {
		return err
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}
	// Make a volume entry, with mode 0777 mkdir honors system umask.
	err = os.Mkdir(preparePath(volumeDir), 0777)
	if err != nil {
		if os.IsExist(err) {
			return errVolumeExists
		} else if os.IsPermission(err) {
			return errDiskAccessDenied
		}
		return err
	}
	// Success
	return nil
}

// ListVols - list volumes.
func (s *posix) ListVols() (volsInfo []VolInfo, err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return nil, errFaultyDisk
	}

	volsInfo, err = listVols(s.diskPath)
	if err != nil {
		return nil, err
	}
	for i, vol := range volsInfo {
		volInfo := VolInfo{
			Name:    vol.Name,
			Created: vol.Created,
		}
		volsInfo[i] = volInfo
	}
	return volsInfo, nil
}

// List all the volumes from diskPath.
func listVols(dirPath string) ([]VolInfo, error) {
	if err := checkPathLength(dirPath); err != nil {
		return nil, err
	}
	entries, err := readDir(dirPath)
	if err != nil {
		return nil, errDiskNotFound
	}
	var volsInfo []VolInfo
	for _, entry := range entries {
		if !strings.HasSuffix(entry, slashSeparator) || !isValidVolname(slashpath.Clean(entry)) {
			// Skip if entry is neither a directory not a valid volume name.
			continue
		}
		var fi os.FileInfo
		fi, err = os.Stat(preparePath(pathJoin(dirPath, entry)))
		if err != nil {
			// If the file does not exist, skip the entry.
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		volsInfo = append(volsInfo, VolInfo{
			Name: fi.Name(),
			// As os.Stat() doesn't carry other than ModTime(), use
			// ModTime() as CreatedTime.
			Created: fi.ModTime(),
		})
	}
	return volsInfo, nil
}

// StatVol - get volume info.
func (s *posix) StatVol(volume string) (volInfo VolInfo, err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return VolInfo{}, errFaultyDisk
	}

	// Check disk availability.
	if _, err = getDiskInfo(s.diskPath); err != nil {
		return VolInfo{}, err
	}

	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return VolInfo{}, err
	}
	// Stat a volume entry.
	var st os.FileInfo
	st, err = os.Stat(preparePath(volumeDir))
	if err != nil {
		if os.IsNotExist(err) {
			return VolInfo{}, errVolumeNotFound
		}
		return VolInfo{}, err
	}
	// As os.Stat() doesn't carry other than ModTime(), use ModTime()
	// as CreatedTime.
	createdTime := st.ModTime()
	return VolInfo{
		Name:    volume,
		Created: createdTime,
	}, nil
}

// DeleteVol - delete a volume.
func (s *posix) DeleteVol(volume string) (err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return errFaultyDisk
	}

	// Check disk availability.
	if _, err = getDiskInfo(s.diskPath); err != nil {
		return err
	}

	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}
	err = os.Remove(preparePath(volumeDir))
	if err != nil {
		if os.IsNotExist(err) {
			return errVolumeNotFound
		} else if strings.Contains(err.Error(), "directory is not empty") {
			// On windows the string is slightly different, handle it here.
			return errVolumeNotEmpty
		} else if strings.Contains(err.Error(), "directory not empty") {
			// Hopefully for all other operating systems, this is
			// assumed to be consistent.
			return errVolumeNotEmpty
		}
		return err
	}
	return nil
}

// ListDir - return all the entries at the given directory path.
// If an entry is a directory it will be returned with a trailing "/".
func (s *posix) ListDir(volume, dirPath string) (entries []string, err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return nil, errFaultyDisk
	}

	// Check disk availability.
	if _, err = getDiskInfo(s.diskPath); err != nil {
		return nil, err
	}

	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}
	// Stat a volume entry.
	_, err = os.Stat(preparePath(volumeDir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errVolumeNotFound
		}
		return nil, err
	}
	return readDir(pathJoin(volumeDir, dirPath))
}

// ReadAll reads from r until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because ReadAll is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
// This API is meant to be used on files which have small memory footprint, do
// not use this on large files as it would cause server to crash.
func (s *posix) ReadAll(volume, path string) (buf []byte, err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return nil, errFaultyDisk
	}

	// Check disk availability.
	if _, err = getDiskInfo(s.diskPath); err != nil {
		return nil, err
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}
	// Stat a volume entry.
	_, err = os.Stat(preparePath(volumeDir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errVolumeNotFound
		}
		return nil, err
	}

	// Validate file path length, before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return nil, err
	}

	// Open the file for reading.
	buf, err = ioutil.ReadFile(preparePath(filePath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errFileNotFound
		} else if os.IsPermission(err) {
			return nil, errFileAccessDenied
		} else if pathErr, ok := err.(*os.PathError); ok {
			switch pathErr.Err {
			case syscall.ENOTDIR, syscall.EISDIR:
				return nil, errFileNotFound
			default:
				if strings.Contains(pathErr.Err.Error(), "The handle is invalid") {
					// This case is special and needs to be handled for windows.
					return nil, errFileNotFound
				}
			}
			return nil, pathErr
		}
		return nil, err
	}
	return buf, nil
}

// ReadFile reads exactly len(buf) bytes into buf. It returns the
// number of bytes copied. The error is EOF only if no bytes were
// read. On return, n == len(buf) if and only if err == nil. n == 0
// for io.EOF. Additionally ReadFile also starts reading from an
// offset.
func (s *posix) ReadFile(volume string, path string, offset int64, buf []byte) (n int64, err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return 0, errFaultyDisk
	}

	// Check disk availability.
	if _, err = getDiskInfo(s.diskPath); err != nil {
		return 0, err
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return 0, err
	}
	// Stat a volume entry.
	_, err = os.Stat(preparePath(volumeDir))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, errVolumeNotFound
		}
		return 0, err
	}

	// Validate effective path length before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return 0, err
	}

	// Open the file for reading.
	file, err := os.Open(preparePath(filePath))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, errFileNotFound
		} else if os.IsPermission(err) {
			return 0, errFileAccessDenied
		} else if strings.Contains(err.Error(), "not a directory") {
			return 0, errFileAccessDenied
		}
		return 0, err
	}

	// Close the file descriptor.
	defer file.Close()

	st, err := file.Stat()
	if err != nil {
		return 0, err
	}

	// Verify if its not a regular file, since subsequent Seek is undefined.
	if !st.Mode().IsRegular() {
		return 0, errIsNotRegular
	}

	// Seek to requested offset.
	_, err = file.Seek(offset, os.SEEK_SET)
	if err != nil {
		return 0, err
	}

	// Read full until buffer.
	m, err := io.ReadFull(file, buf)

	// Success.
	return int64(m), err
}

// AppendFile - append a byte array at path, if file doesn't exist at
// path this call explicitly creates it.
func (s *posix) AppendFile(volume, path string, buf []byte) (err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return errFaultyDisk
	}

	// Validate if disk is free.
	if err = checkDiskFree(s.diskPath, s.minFreeDisk); err != nil {
		return err
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}
	// Stat a volume entry.
	_, err = os.Stat(preparePath(volumeDir))
	if err != nil {
		if os.IsNotExist(err) {
			return errVolumeNotFound
		}
		return err
	}
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}
	// Verify if the file already exists and is not of regular type.
	var st os.FileInfo
	if st, err = os.Stat(preparePath(filePath)); err == nil {
		if !st.Mode().IsRegular() {
			return errIsNotRegular
		}
	}
	// Create top level directories if they don't exist.
	// with mode 0777 mkdir honors system umask.
	if err = mkdirAll(filepath.Dir(filePath), 0777); err != nil {
		// File path cannot be verified since one of the parents is a file.
		if strings.Contains(err.Error(), "not a directory") {
			return errFileAccessDenied
		} else if runtime.GOOS == "windows" && strings.Contains(err.Error(), "system cannot find the path specified") {
			// Add specific case for windows.
			return errFileAccessDenied
		}
		return err
	}

	// Creates the named file with mode 0666 (before umask), or starts appending
	// to an existig file.
	w, err := os.OpenFile(preparePath(filePath), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		// File path cannot be verified since one of the parents is a file.
		if strings.Contains(err.Error(), "not a directory") {
			return errFileAccessDenied
		}
		return err
	}

	// Close upon return.
	defer w.Close()

	// Return io.Copy
	_, err = io.Copy(w, bytes.NewReader(buf))
	return err
}

// StatFile - get file info.
func (s *posix) StatFile(volume, path string) (file FileInfo, err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return FileInfo{}, errFaultyDisk
	}

	// Check disk availability.
	if _, err = getDiskInfo(s.diskPath); err != nil {
		return FileInfo{}, err
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return FileInfo{}, err
	}
	// Stat a volume entry.
	_, err = os.Stat(preparePath(volumeDir))
	if err != nil {
		if os.IsNotExist(err) {
			return FileInfo{}, errVolumeNotFound
		}
		return FileInfo{}, err
	}

	filePath := slashpath.Join(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return FileInfo{}, err
	}
	st, err := os.Stat(preparePath(filePath))
	if err != nil {
		// File is really not found.
		if os.IsNotExist(err) {
			return FileInfo{}, errFileNotFound
		}

		// File path cannot be verified since one of the parents is a file.
		if strings.Contains(err.Error(), "not a directory") {
			return FileInfo{}, errFileNotFound
		}

		// Return all errors here.
		return FileInfo{}, err
	}
	// If its a directory its not a regular file.
	if st.Mode().IsDir() {
		return FileInfo{}, errFileNotFound
	}
	return FileInfo{
		Volume:  volume,
		Name:    path,
		ModTime: st.ModTime(),
		Size:    st.Size(),
		Mode:    st.Mode(),
	}, nil
}

// deleteFile - delete file path if its empty.
func deleteFile(basePath, deletePath string) error {
	if basePath == deletePath {
		return nil
	}
	// Verify if the path exists.
	pathSt, err := os.Stat(preparePath(deletePath))
	if err != nil {
		if os.IsNotExist(err) {
			return errFileNotFound
		} else if os.IsPermission(err) {
			return errFileAccessDenied
		}
		return err
	}
	if pathSt.IsDir() && !isDirEmpty(deletePath) {
		// Verify if directory is empty.
		return nil
	}
	// Attempt to remove path.
	if err := os.Remove(preparePath(deletePath)); err != nil {
		return err
	}
	// Recursively go down the next path and delete again.
	if err := deleteFile(basePath, slashpath.Dir(deletePath)); err != nil {
		return err
	}
	return nil
}

// DeleteFile - delete a file at path.
func (s *posix) DeleteFile(volume, path string) (err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return errFaultyDisk
	}

	// Check disk availability.
	if _, err = getDiskInfo(s.diskPath); err != nil {
		return err
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}
	// Stat a volume entry.
	_, err = os.Stat(preparePath(volumeDir))
	if err != nil {
		if os.IsNotExist(err) {
			return errVolumeNotFound
		}
		return err
	}

	// Following code is needed so that we retain "/" suffix if any in
	// path argument.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	// Delete file and delete parent directory as well if its empty.
	return deleteFile(volumeDir, filePath)
}

// RenameFile - rename source path to destination path atomically.
func (s *posix) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return errFaultyDisk
	}

	// Validate if disk is free.
	if err = checkDiskFree(s.diskPath, s.minFreeDisk); err != nil {
		return err
	}

	srcVolumeDir, err := s.getVolDir(srcVolume)
	if err != nil {
		return err
	}
	dstVolumeDir, err := s.getVolDir(dstVolume)
	if err != nil {
		return err
	}
	// Stat a volume entry.
	_, err = os.Stat(preparePath(srcVolumeDir))
	if err != nil {
		if os.IsNotExist(err) {
			return errVolumeNotFound
		}
		return err
	}
	_, err = os.Stat(preparePath(dstVolumeDir))
	if err != nil {
		if os.IsNotExist(err) {
			return errVolumeNotFound
		}
	}

	srcIsDir := strings.HasSuffix(srcPath, slashSeparator)
	dstIsDir := strings.HasSuffix(dstPath, slashSeparator)
	// Either src and dst have to be directories or files, else return error.
	if !(srcIsDir && dstIsDir || !srcIsDir && !dstIsDir) {
		return errFileAccessDenied
	}
	srcFilePath := slashpath.Join(srcVolumeDir, srcPath)
	if err = checkPathLength(srcFilePath); err != nil {
		return err
	}
	dstFilePath := slashpath.Join(dstVolumeDir, dstPath)
	if err = checkPathLength(dstFilePath); err != nil {
		return err
	}
	if srcIsDir {
		// If source is a directory we expect the destination to be non-existent always.
		_, err = os.Stat(preparePath(dstFilePath))
		if err == nil {
			return errFileAccessDenied
		}
		if !os.IsNotExist(err) {
			return err
		}
		// Destination does not exist, hence proceed with the rename.
	}
	// Creates all the parent directories, with mode 0777 mkdir honors system umask.
	if err = mkdirAll(preparePath(slashpath.Dir(dstFilePath)), 0777); err != nil {
		// File path cannot be verified since one of the parents is a file.
		if strings.Contains(err.Error(), "not a directory") {
			return errFileAccessDenied
		} else if strings.Contains(err.Error(), "The system cannot find the path specified.") && runtime.GOOS == "windows" {
			// This is a special case should be handled only for
			// windows, because windows API does not return "not a
			// directory" error message. Handle this specifically here.
			return errFileAccessDenied
		}
		return err
	}
	// Finally attempt a rename.
	err = os.Rename(preparePath(srcFilePath), preparePath(dstFilePath))
	if err != nil {
		if os.IsNotExist(err) {
			return errFileNotFound
		}
		return err
	}
	return nil
}
