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

package main

import (
	"bytes"
	"io"
	"os"
	slashpath "path"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/minio/minio/pkg/disk"
)

const (
	fsMinSpacePercent = 5
)

// posix - implements StorageAPI interface.
type posix struct {
	diskPath    string
	minFreeDisk int64
}

// checkPathLength - returns error if given path name length more than 255
func checkPathLength(pathName string) error {
	// For MS Windows, the maximum path length is 255
	if runtime.GOOS == "windows" {
		if len(pathName) > 255 {
			return errFileNameTooLong
		}

		return nil
	}

	// For non-windows system, check each path segment length is > 255
	for len(pathName) > 0 && pathName != "." && pathName != "/" {
		dir, file := slashpath.Dir(pathName), slashpath.Base(pathName)

		if len(file) > 255 {
			return errFileNameTooLong
		}

		pathName = dir
	}

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
	fs := posix{
		diskPath:    diskPath,
		minFreeDisk: fsMinSpacePercent, // Minimum 5% disk should be free.
	}
	st, err := os.Stat(diskPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fs, errDiskNotFound
		}
		return fs, err
	}
	if !st.IsDir() {
		return fs, syscall.ENOTDIR
	}
	return fs, nil
}

// checkDiskFree verifies if disk path has sufficient minium free disk space.
func checkDiskFree(diskPath string, minFreeDisk int64) (err error) {
	if err = checkPathLength(diskPath); err != nil {
		return err
	}
	di, err := disk.GetInfo(diskPath)
	if err != nil {
		if os.IsNotExist(err) {
			return errDiskNotFound
		}
		return err
	}

	// Remove 5% from total space for cumulative disk
	// space used for journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= minFreeDisk {
		return errDiskFull
	}

	// Success.
	return nil
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
		fi, err = os.Stat(pathJoin(dirPath, entry))
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

// getVolDir - will convert incoming volume names to
// corresponding valid volume names on the backend in a platform
// compatible way for all operating systems. If volume is not found
// an error is generated.
func (s posix) getVolDir(volume string) (string, error) {
	if !isValidVolname(volume) {
		return "", errInvalidArgument
	}
	if err := checkPathLength(volume); err != nil {
		return "", err
	}
	volumeDir := pathJoin(s.diskPath, volume)
	return volumeDir, nil
}

// Make a volume entry.
func (s posix) MakeVol(volume string) (err error) {
	// Validate if disk is free.
	if err = checkDiskFree(s.diskPath, s.minFreeDisk); err != nil {
		return err
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}
	// Make a volume entry.
	err = os.Mkdir(volumeDir, 0700)
	if err != nil && os.IsExist(err) {
		return errVolumeExists
	}
	// Success
	return nil
}

// ListVols - list volumes.
func (s posix) ListVols() (volsInfo []VolInfo, err error) {
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

// StatVol - get volume info.
func (s posix) StatVol(volume string) (volInfo VolInfo, err error) {
	// Validate if disk is free.
	if err = checkDiskFree(s.diskPath, s.minFreeDisk); err != nil {
		return VolInfo{}, err
	}

	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return VolInfo{}, err
	}
	// Stat a volume entry.
	var st os.FileInfo
	st, err = os.Stat(volumeDir)
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
func (s posix) DeleteVol(volume string) error {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}
	err = os.Remove(volumeDir)
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
func (s posix) ListDir(volume, dirPath string) ([]string, error) {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}
	// Stat a volume entry.
	_, err = os.Stat(volumeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errVolumeNotFound
		}
		return nil, err
	}
	return readDir(pathJoin(volumeDir, dirPath))
}

// ReadFile reads exactly len(buf) bytes into buf. It returns the
// number of bytes copied. The error is EOF only if no bytes were
// read. On return, n == len(buf) if and only if err == nil. n == 0
// for io.EOF. Additionally ReadFile also starts reading from an
// offset.
func (s posix) ReadFile(volume string, path string, offset int64, buf []byte) (n int64, err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return 0, err
	}
	// Stat a volume entry.
	_, err = os.Stat(volumeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, errVolumeNotFound
		}
		return 0, err
	}

	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return 0, err
	}
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, errFileNotFound
		} else if os.IsPermission(err) {
			return 0, errFileAccessDenied
		} else if strings.Contains(err.Error(), "not a directory") {
			return 0, errFileNotFound
		}
		return 0, err
	}
	st, err := file.Stat()
	if err != nil {
		return 0, err
	}
	// Verify if its not a regular file, since subsequent Seek is undefined.
	if !st.Mode().IsRegular() {
		return 0, errFileNotFound
	}
	// Seek to requested offset.
	_, err = file.Seek(offset, os.SEEK_SET)
	if err != nil {
		return 0, err
	}

	// Close the reader.
	defer file.Close()

	// Read file.
	m, err := io.ReadFull(file, buf)

	// Error unexpected is valid, set this back to nil.
	if err == io.ErrUnexpectedEOF {
		err = nil
	}

	// Success.
	return int64(m), err
}

// AppendFile - append a byte array at path, if file doesn't exist at
// path this call explicitly creates it.
func (s posix) AppendFile(volume, path string, buf []byte) (n int64, err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return 0, err
	}
	// Stat a volume entry.
	_, err = os.Stat(volumeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, errVolumeNotFound
		}
		return 0, err
	}
	if err = checkDiskFree(s.diskPath, s.minFreeDisk); err != nil {
		return 0, err
	}
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return 0, err
	}
	// Verify if the file already exists and is not of regular type.
	var st os.FileInfo
	if st, err = os.Stat(filePath); err == nil {
		if st.IsDir() {
			return 0, errIsNotRegular
		}
	}
	// Create top level directories if they don't exist.
	if err = os.MkdirAll(filepath.Dir(filePath), 0700); err != nil {
		return 0, err
	}
	w, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		// File path cannot be verified since one of the parents is a file.
		if strings.Contains(err.Error(), "not a directory") {
			return 0, errFileAccessDenied
		}
		return 0, err
	}
	// Close upon return.
	defer w.Close()

	// Return io.Copy
	return io.Copy(w, bytes.NewReader(buf))
}

// StatFile - get file info.
func (s posix) StatFile(volume, path string) (file FileInfo, err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return FileInfo{}, err
	}
	// Stat a volume entry.
	_, err = os.Stat(volumeDir)
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
	st, err := os.Stat(filePath)
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
	pathSt, err := os.Stat(deletePath)
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
	if err := os.Remove(deletePath); err != nil {
		return err
	}
	// Recursively go down the next path and delete again.
	if err := deleteFile(basePath, slashpath.Dir(deletePath)); err != nil {
		return err
	}
	return nil
}

// DeleteFile - delete a file at path.
func (s posix) DeleteFile(volume, path string) error {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}
	// Stat a volume entry.
	_, err = os.Stat(volumeDir)
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
func (s posix) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) error {
	srcVolumeDir, err := s.getVolDir(srcVolume)
	if err != nil {
		return err
	}
	dstVolumeDir, err := s.getVolDir(dstVolume)
	if err != nil {
		return err
	}
	// Stat a volume entry.
	_, err = os.Stat(srcVolumeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return errVolumeNotFound
		}
		return err
	}
	_, err = os.Stat(dstVolumeDir)
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
	if srcIsDir {
		// If source is a directory we expect the destination to be non-existent always.
		_, err = os.Stat(slashpath.Join(dstVolumeDir, dstPath))
		if err == nil {
			return errFileAccessDenied
		}
		if !os.IsNotExist(err) {
			return err
		}
		// Destination does not exist, hence proceed with the rename.
	}
	if err = os.MkdirAll(slashpath.Dir(slashpath.Join(dstVolumeDir, dstPath)), 0755); err != nil {
		// File path cannot be verified since one of the parents is a file.
		if strings.Contains(err.Error(), "not a directory") {
			return errFileAccessDenied
		}
		return err
	}
	err = os.Rename(slashpath.Join(srcVolumeDir, srcPath), slashpath.Join(dstVolumeDir, dstPath))
	if err != nil {
		if os.IsNotExist(err) {
			return errFileNotFound
		}
		return err
	}
	return nil
}
