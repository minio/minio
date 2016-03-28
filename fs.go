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
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/safe"
)

const (
	fsListLimit = 1000
)

// listParams - list object params used for list object map
type listParams struct {
	bucket    string
	recursive bool
	marker    string
	prefix    string
}

// fsStorage - implements StorageAPI interface.
type fsStorage struct {
	diskPath           string
	diskInfo           disk.Info
	minFreeDisk        int64
	listObjectMap      map[listParams][]*treeWalker
	listObjectMapMutex *sync.Mutex
}

// isDirEmpty - returns whether given directory is empty or not.
func isDirEmpty(dirname string) (status bool, err error) {
	f, err := os.Open(dirname)
	if err == nil {
		defer f.Close()
		if _, err = f.Readdirnames(1); err == io.EOF {
			status = true
			err = nil
		}
	}
	return status, err
}

// isDirExist - returns whether given directory exists or not.
func isDirExist(dirname string) (bool, error) {
	fi, e := os.Stat(dirname)
	if e != nil {
		if os.IsNotExist(e) {
			return false, nil
		}
		return false, e
	}
	return fi.IsDir(), nil
}

// Initialize a new storage disk.
func newFS(diskPath string) (StorageAPI, error) {
	if diskPath == "" {
		return nil, errInvalidArgument
	}
	st, e := os.Stat(diskPath)
	if e != nil {
		return nil, e
	}
	if !st.IsDir() {
		return nil, syscall.ENOTDIR
	}
	diskInfo, e := disk.GetInfo(diskPath)
	if e != nil {
		return nil, e
	}
	fs := fsStorage{
		diskPath:           diskPath,
		diskInfo:           diskInfo,
		minFreeDisk:        5, // Minimum 5% disk should be free.
		listObjectMap:      make(map[listParams][]*treeWalker),
		listObjectMapMutex: &sync.Mutex{},
	}
	return fs, nil
}

// checkDiskFree verifies if disk path has sufficient minium free disk space.
func checkDiskFree(diskPath string, minFreeDisk int64) (err error) {
	di, err := disk.GetInfo(diskPath)
	if err != nil {
		return err
	}

	// Remove 5% from total space for cumulative disk
	// space used for journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= minFreeDisk {
		return errDiskPathFull
	}

	// Success.
	return nil
}

// removeDuplicateVols - remove duplicate volumes.
func removeDuplicateVols(vols []VolInfo) []VolInfo {
	length := len(vols) - 1
	for i := 0; i < length; i++ {
		for j := i + 1; j <= length; j++ {
			if vols[i].Name == vols[j].Name {
				// Pick the latest volume, if there is a duplicate.
				if vols[i].Created.Sub(vols[j].Created) > 0 {
					vols[i] = vols[length]
				} else {
					vols[j] = vols[length]
				}
				vols = vols[0:length]
				length--
				j--
			}
		}
	}
	return vols
}

// gets all the unique directories from diskPath.
func getAllUniqueVols(dirPath string) ([]VolInfo, error) {
	volumeFn := func(dirent fsDirent) bool {
		// Return all directories.
		return dirent.IsDir() && isValidVolname(filepath.Clean(dirent.name))
	}
	namesOnly := true // Returned are only names.
	dirents, err := scandir(dirPath, volumeFn, namesOnly)
	if err != nil {
		return nil, err
	}
	var volsInfo []VolInfo
	for _, dirent := range dirents {
		fi, err := os.Stat(filepath.Join(dirPath, dirent.name))
		if err != nil {
			return nil, err
		}
		volsInfo = append(volsInfo, VolInfo{
			Name: fi.Name(),
			// As os.Stat() doesn't carry other than ModTime(), use
			// ModTime() as CreatedTime.
			Created: fi.ModTime(),
		})
	}
	volsInfo = removeDuplicateVols(volsInfo)
	return volsInfo, nil
}

// getVolumeDir - will convert incoming volume names to
// corresponding valid volume names on the backend in a platform
// compatible way for all operating systems. If volume is not found
// an error is generated.
func (s fsStorage) getVolumeDir(volume string) (string, error) {
	if !isValidVolname(volume) {
		return "", errInvalidArgument
	}
	volumeDir := filepath.Join(s.diskPath, volume)
	_, err := os.Stat(volumeDir)
	if err == nil {
		return volumeDir, nil
	}
	if os.IsNotExist(err) {
		var volsInfo []VolInfo
		volsInfo, err = getAllUniqueVols(s.diskPath)
		if err != nil {
			return volumeDir, errVolumeNotFound
		}
		for _, vol := range volsInfo {
			// Verify if lowercase version of the volume
			// is equal to the incoming volume, then use the proper name.
			if strings.ToLower(vol.Name) == volume {
				volumeDir = filepath.Join(s.diskPath, vol.Name)
				return volumeDir, nil
			}
		}
		return volumeDir, errVolumeNotFound
	} else if os.IsPermission(err) {
		return volumeDir, errVolumeAccessDenied
	}
	return volumeDir, err
}

// Make a volume entry.
func (s fsStorage) MakeVol(volume string) (err error) {
	volumeDir, err := s.getVolumeDir(volume)
	if err == nil {
		// Volume already exists, return error.
		return errVolumeExists
	}

	// Validate if disk is free.
	if e := checkDiskFree(s.diskPath, s.minFreeDisk); e != nil {
		return e
	}

	// If volume not found create it.
	if err == errVolumeNotFound {
		// Make a volume entry.
		return os.Mkdir(volumeDir, 0700)
	}

	// For all other errors return here.
	return err
}

// ListVols - list volumes.
func (s fsStorage) ListVols() (volsInfo []VolInfo, err error) {
	volsInfo, err = getAllUniqueVols(s.diskPath)
	if err != nil {
		return nil, err
	}
	for i, vol := range volsInfo {
		// Volname on case sensitive fs backends can come in as
		// capitalized, but object layer cannot consume it
		// directly. Convert it as we see fit.
		volName := strings.ToLower(vol.Name)
		volInfo := VolInfo{
			Name:    volName,
			Created: vol.Created,
		}
		volsInfo[i] = volInfo
	}
	return volsInfo, nil
}

// StatVol - get volume info.
func (s fsStorage) StatVol(volume string) (volInfo VolInfo, err error) {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolumeDir(volume)
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
func (s fsStorage) DeleteVol(volume string) error {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolumeDir(volume)
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

// Save the goroutine reference in the map
func (s *fsStorage) saveTreeWalk(params listParams, walker *treeWalker) {
	s.listObjectMapMutex.Lock()
	defer s.listObjectMapMutex.Unlock()

	walkers, _ := s.listObjectMap[params]
	walkers = append(walkers, walker)

	s.listObjectMap[params] = walkers
}

// Lookup the goroutine reference from map
func (s *fsStorage) lookupTreeWalk(params listParams) *treeWalker {
	s.listObjectMapMutex.Lock()
	defer s.listObjectMapMutex.Unlock()

	if walkChs, ok := s.listObjectMap[params]; ok {
		for i, walkCh := range walkChs {
			if !walkCh.timedOut {
				newWalkChs := walkChs[i+1:]
				if len(newWalkChs) > 0 {
					s.listObjectMap[params] = newWalkChs
				} else {
					delete(s.listObjectMap, params)
				}
				return walkCh
			}
		}
		// As all channels are timed out, delete the map entry
		delete(s.listObjectMap, params)
	}
	return nil
}

// List operation.
func (s fsStorage) ListFiles(volume, prefix, marker string, recursive bool, count int) ([]FileInfo, bool, error) {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolumeDir(volume)
	if err != nil {
		return nil, true, err
	}
	var fileInfos []FileInfo

	if marker != "" {
		// Verify if marker has prefix.
		if marker != "" && !strings.HasPrefix(marker, prefix) {
			return nil, true, errInvalidArgument
		}
	}

	// Return empty response for a valid request when count is 0.
	if count == 0 {
		return nil, true, nil
	}

	// Over flowing count - reset to fsListLimit.
	if count < 0 || count > fsListLimit {
		count = fsListLimit
	}

	// Verify if prefix exists.
	prefixDir := filepath.Dir(filepath.FromSlash(prefix))
	prefixRootDir := filepath.Join(volumeDir, prefixDir)
	var status bool
	if status, err = isDirExist(prefixRootDir); !status {
		if err == nil {
			// Prefix does not exist, not an error just respond empty list response.
			return nil, true, nil
		} else if strings.Contains(err.Error(), "not a directory") {
			// Prefix exists as a file.
			return nil, true, nil
		}
		// Rest errors should be treated as failure.
		return nil, true, err
	}

	// Maximum 1000 files returned in a single call.
	// Further calls will set right marker value to continue reading the rest of the files.
	// popTreeWalker returns nil if the call to ListFiles is done for the first time.
	// On further calls to ListFiles to retrive more files within the timeout period,
	// popTreeWalker returns the channel from which rest of the objects can be retrieved.
	walker := s.lookupTreeWalk(listParams{volume, recursive, marker, prefix})
	if walker == nil {
		walker = startTreeWalk(s.diskPath, volume, filepath.FromSlash(prefix), filepath.FromSlash(marker), recursive)
	}
	nextMarker := ""
	for i := 0; i < count; {
		walkResult, ok := <-walker.ch
		if !ok {
			// Closed channel.
			return fileInfos, true, nil
		}
		// For any walk error return right away.
		if walkResult.err != nil {
			return nil, true, walkResult.err
		}
		fileInfo := walkResult.fileInfo
		fileInfo.Name = filepath.ToSlash(fileInfo.Name)
		fileInfos = append(fileInfos, fileInfo)
		// We have listed everything return.
		if walkResult.end {
			return fileInfos, true, nil
		}
		nextMarker = fileInfo.Name
		i++
	}
	s.saveTreeWalk(listParams{volume, recursive, nextMarker, prefix}, walker)
	return fileInfos, false, nil
}

// ReadFile - read a file at a given offset.
func (s fsStorage) ReadFile(volume string, path string, offset int64) (readCloser io.ReadCloser, err error) {
	volumeDir, err := s.getVolumeDir(volume)
	if err != nil {
		return nil, err
	}

	filePath := filepath.Join(volumeDir, filepath.FromSlash(path))
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errFileNotFound
		} else if os.IsPermission(err) {
			return nil, errFileAccessDenied
		}
		return nil, err
	}
	st, err := file.Stat()
	if err != nil {
		return nil, err
	}
	// Verify if its not a regular file, since subsequent Seek is undefined.
	if !st.Mode().IsRegular() {
		return nil, errIsNotRegular
	}
	// Seek to requested offset.
	_, err = file.Seek(offset, os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// CreateFile - create a file at path.
func (s fsStorage) CreateFile(volume, path string) (writeCloser io.WriteCloser, err error) {
	volumeDir, err := s.getVolumeDir(volume)
	if err != nil {
		return nil, err
	}
	if err := checkDiskFree(s.diskPath, s.minFreeDisk); err != nil {
		return nil, err
	}
	filePath := filepath.Join(volumeDir, path)
	// Verify if the file already exists and is not of regular type.
	if st, err := os.Stat(filePath); err == nil {
		if st.IsDir() {
			return nil, errIsNotRegular
		}
	}
	return safe.CreateFileWithPrefix(filePath, "$tmpfile")
}

// StatFile - get file info.
func (s fsStorage) StatFile(volume, path string) (file FileInfo, err error) {
	volumeDir, err := s.getVolumeDir(volume)
	if err != nil {
		return FileInfo{}, err
	}

	filePath := filepath.Join(volumeDir, filepath.FromSlash(path))
	st, err := os.Stat(filePath)
	if err != nil {
		// File is really not found.
		if os.IsNotExist(err) {
			return FileInfo{}, errFileNotFound
		}
		// File path cannot be verified since one of the parents is a file.
		if strings.Contains(err.Error(), "not a directory") {
			return FileInfo{}, errIsNotRegular
		}
		// Return all errors here.
		return FileInfo{}, err
	}
	// If its a directory its not a regular file.
	if st.Mode().IsDir() {
		return FileInfo{}, errIsNotRegular
	}
	file = FileInfo{
		Volume:  volume,
		Name:    path,
		ModTime: st.ModTime(),
		Size:    st.Size(),
		Mode:    st.Mode(),
	}
	return file, nil
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
	if pathSt.IsDir() {
		// Verify if directory is empty.
		empty, err := isDirEmpty(deletePath)
		if err != nil {
			return err
		}
		if !empty {
			return nil
		}
	}
	// Attempt to remove path.
	if err := os.Remove(deletePath); err != nil {
		return err
	}
	// Recursively go down the next path and delete again.
	if err := deleteFile(basePath, filepath.Dir(deletePath)); err != nil {
		return err
	}
	return nil
}

// DeleteFile - delete a file at path.
func (s fsStorage) DeleteFile(volume, path string) error {
	volumeDir, err := s.getVolumeDir(volume)
	if err != nil {
		return err
	}

	// Following code is needed so that we retain "/" suffix if any in
	// path argument.
	filePath := filepath.Join(volumeDir, filepath.FromSlash(path))
	if strings.HasSuffix(filepath.FromSlash(path), string(os.PathSeparator)) {
		filePath = filePath + string(os.PathSeparator)
	}

	// Delete file and delete parent directory as well if its empty.
	return deleteFile(volumeDir, filePath)
}
