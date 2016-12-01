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
	"io"
	"io/ioutil"
	"os"
	slashpath "path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/disk"
)

const (
	fsMinFreeSpace    = 1 * humanize.GiByte // Min 1GiB free space.
	fsMinFreeInodes   = 10000               // Min 10000.
	maxAllowedIOError = 5
)

// posix - implements StorageAPI interface.
type posix struct {
	ioErrCount    int32 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	diskPath      string
	minFreeSpace  int64
	minFreeInodes int64
	pool          sync.Pool
}

// checkPathLength - returns error if given path name length more than 255
func checkPathLength(pathName string) error {
	// Apple OS X path length is limited to 1016
	if runtime.GOOS == "darwin" && len(pathName) > 1016 {
		return errFileNameTooLong
	}

	// Convert any '\' to '/'.
	pathName = filepath.ToSlash(pathName)

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
		errorIf(func() error {
			if !os.IsNotExist(err) {
				return err
			}
			return nil
		}(), "Unable to access directory.")
		return false
	}
	defer f.Close()
	// List one entry.
	_, err = f.Readdirnames(1)
	if err != io.EOF {
		errorIf(func() error {
			if !os.IsNotExist(err) {
				return err
			}
			return nil
		}(), "Unable to list directory.")
		return false
	}
	// Returns true if we have reached EOF, directory is indeed empty.
	return true
}

// Initialize a new storage disk.
func newPosix(path string) (StorageAPI, error) {
	if path == "" {
		return nil, errInvalidArgument
	}
	// Disallow relative paths, figure out absolute paths.
	diskPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	fs := &posix{
		diskPath:      diskPath,
		minFreeSpace:  fsMinFreeSpace,
		minFreeInodes: fsMinFreeInodes,
		// 1MiB buffer pool for posix internal operations.
		pool: sync.Pool{
			New: func() interface{} {
				b := make([]byte, readSizeV1)
				return &b
			},
		},
	}
	fi, err := os.Stat(preparePath(diskPath))
	if err == nil {
		if !fi.IsDir() {
			return nil, syscall.ENOTDIR
		}
	}
	if os.IsNotExist(err) {
		// Disk not found create it.
		err = os.MkdirAll(preparePath(diskPath), 0777)
		if err != nil {
			return nil, err
		}
	}
	if err = fs.checkDiskFree(); err != nil {
		return nil, err
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
func (s *posix) checkDiskFree() (err error) {
	// We don't validate disk space or inode utilization on windows.
	// Each windows calls to 'GetVolumeInformationW' takes around 3-5seconds.
	if runtime.GOOS == "windows" {
		return nil
	}

	var di disk.Info
	di, err = getDiskInfo(preparePath(s.diskPath))
	if err != nil {
		return err
	}

	// Remove 5% from free space for cumulative disk space used for journalling, inodes etc.
	availableDiskSpace := float64(di.Free) * 0.95
	if int64(availableDiskSpace) <= s.minFreeSpace {
		return errDiskFull
	}

	// Some filesystems do not implement a way to provide total inodes available, instead inodes
	// are allocated based on available disk space. For example CephFS, StoreNext CVFS, AzureFile driver.
	// Allow for the available disk to be separately validate and we will validate inodes only if
	// total inodes are provided by the underlying filesystem.
	if di.Files != 0 {
		availableFiles := int64(di.Ffree)
		if availableFiles <= s.minFreeInodes {
			return errDiskFull
		}
	}

	// Success.
	return nil
}

// Implements stringer compatible interface.
func (s *posix) String() string {
	return s.diskPath
}

// Init - this is a dummy call.
func (s *posix) Init() error {
	return nil
}

// Close - this is a dummy call.
func (s *posix) Close() error {
	return nil
}

// DiskInfo provides current information about disk space usage,
// total free inodes and underlying filesystem.
func (s *posix) DiskInfo() (info disk.Info, err error) {
	return getDiskInfo(preparePath(s.diskPath))
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

// checkDiskFound - validates if disk is available,
// returns errDiskNotFound if not found.
func (s *posix) checkDiskFound() (err error) {
	_, err = os.Stat(preparePath(s.diskPath))
	if err != nil {
		if os.IsNotExist(err) {
			return errDiskNotFound
		} else if isSysErrTooLong(err) {
			return errFileNameTooLong
		}
	}
	return err
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

	if err = s.checkDiskFound(); err != nil {
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

	if err = s.checkDiskFound(); err != nil {
		return nil, err
	}

	volsInfo, err = listVols(preparePath(s.diskPath))
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

	if err = s.checkDiskFound(); err != nil {
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

	if err = s.checkDiskFound(); err != nil {
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
		} else if isSysErrNotEmpty(err) {
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

	if err = s.checkDiskFound(); err != nil {
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

	if err = s.checkDiskFound(); err != nil {
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
	if err = checkPathLength(preparePath(filePath)); err != nil {
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
				if isSysErrHandleInvalid(pathErr.Err) {
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
// for io.EOF.
// If an EOF happens after reading some but not all the bytes,
// ReadFull returns ErrUnexpectedEOF.
// Additionally ReadFile also starts reading from an offset.
// ReadFile symantics are same as io.ReadFull
func (s *posix) ReadFile(volume string, path string, offset int64, buf []byte) (n int64, err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return 0, errFaultyDisk
	}

	if err = s.checkDiskFound(); err != nil {
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
	if err = checkPathLength(preparePath(filePath)); err != nil {
		return 0, err
	}

	// Open the file for reading.
	file, err := os.Open(preparePath(filePath))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, errFileNotFound
		} else if os.IsPermission(err) {
			return 0, errFileAccessDenied
		} else if isSysErrNotDir(err) {
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

func (s *posix) createFile(volume, path string) (f *os.File, err error) {
	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return nil, errFaultyDisk
	}

	if err = s.checkDiskFound(); err != nil {
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

	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(preparePath(filePath)); err != nil {
		return nil, err
	}

	// Verify if the file already exists and is not of regular type.
	var st os.FileInfo
	if st, err = os.Stat(preparePath(filePath)); err == nil {
		if !st.Mode().IsRegular() {
			return nil, errIsNotRegular
		}
	} else {
		// Create top level directories if they don't exist.
		// with mode 0777 mkdir honors system umask.
		if err = mkdirAll(preparePath(slashpath.Dir(filePath)), 0777); err != nil {
			// File path cannot be verified since one of the parents is a file.
			if isSysErrNotDir(err) {
				return nil, errFileAccessDenied
			} else if isSysErrPathNotFound(err) {
				// Add specific case for windows.
				return nil, errFileAccessDenied
			}
			return nil, err
		}
	}

	w, err := os.OpenFile(preparePath(filePath), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		// File path cannot be verified since one of the parents is a file.
		if isSysErrNotDir(err) {
			return nil, errFileAccessDenied
		}
		return nil, err
	}

	return w, nil
}

// PrepareFile - run prior actions before creating a new file for optimization purposes
// Currently we use fallocate when available to avoid disk fragmentation as much as possible
func (s *posix) PrepareFile(volume, path string, fileSize int64) (err error) {

	// It doesn't make sense to create a negative-sized file
	if fileSize <= 0 {
		return errInvalidArgument
	}

	defer func() {
		if err == syscall.EIO {
			atomic.AddInt32(&s.ioErrCount, 1)
		}
	}()

	if s.ioErrCount > maxAllowedIOError {
		return errFaultyDisk
	}

	// Validate if disk is indeed free.
	if err = s.checkDiskFree(); err != nil {
		return err
	}

	// Create file if not found
	w, err := s.createFile(volume, path)
	if err != nil {
		return err
	}

	// Close upon return.
	defer w.Close()

	// Allocate needed disk space to append data
	e := Fallocate(int(w.Fd()), 0, fileSize)

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

	// Create file if not found
	w, err := s.createFile(volume, path)
	if err != nil {
		return err
	}

	// Close upon return.
	defer w.Close()

	bufp := s.pool.Get().(*[]byte)

	// Reuse buffer.
	defer s.pool.Put(bufp)

	// Return io.Copy
	_, err = io.CopyBuffer(w, bytes.NewReader(buf), *bufp)
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

	if err = s.checkDiskFound(); err != nil {
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
	if err = checkPathLength(preparePath(filePath)); err != nil {
		return FileInfo{}, err
	}
	st, err := os.Stat(preparePath(filePath))
	if err != nil {
		// File is really not found.
		if os.IsNotExist(err) {
			return FileInfo{}, errFileNotFound
		}

		// File path cannot be verified since one of the parents is a file.
		if isSysErrNotDir(err) {
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
		if os.IsNotExist(err) {
			return errFileNotFound
		} else if os.IsPermission(err) {
			return errFileAccessDenied
		}
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

	if err = s.checkDiskFound(); err != nil {
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
	if err = checkPathLength(preparePath(filePath)); err != nil {
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

	if err = s.checkDiskFound(); err != nil {
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
	if err = checkPathLength(preparePath(srcFilePath)); err != nil {
		return err
	}
	dstFilePath := slashpath.Join(dstVolumeDir, dstPath)
	if err = checkPathLength(preparePath(dstFilePath)); err != nil {
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
		if isSysErrNotDir(err) {
			return errFileAccessDenied
		} else if isSysErrPathNotFound(err) {
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

	// Remove parent dir of the source file if empty
	if parentDir := slashpath.Dir(srcFilePath); isDirEmpty(parentDir) {
		deleteFile(srcVolumeDir, parentDir)
	}

	return nil
}
