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
	"io/ioutil"
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
	rwLock             *sync.RWMutex
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
	fi, e := os.Lstat(dirname)
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
		rwLock:             &sync.RWMutex{},
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

// Make a volume entry.
func (s fsStorage) MakeVol(volume string) (err error) {
	if volume == "" {
		return errInvalidArgument
	}
	if err = checkDiskFree(s.diskPath, s.minFreeDisk); err != nil {
		return err
	}

	volumeDir := getVolumeDir(s.diskPath, volume)
	if _, err = os.Stat(volumeDir); err == nil {
		return errVolumeExists
	}

	// Make a volume entry.
	if err = os.Mkdir(volumeDir, 0700); err != nil {
		return err
	}

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

// ListVols - list volumes.
func (s fsStorage) ListVols() (volsInfo []VolInfo, err error) {
	files, err := ioutil.ReadDir(s.diskPath)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if !file.IsDir() {
			// If not directory, ignore all file types.
			continue
		}
		volInfo := VolInfo{
			Name:    file.Name(),
			Created: file.ModTime(),
		}
		volsInfo = append(volsInfo, volInfo)
	}
	// Remove duplicated volume entries.
	volsInfo = removeDuplicateVols(volsInfo)
	return volsInfo, nil
}

// getVolumeDir - will convert incoming volume names to
// corresponding valid volume names on the backend in a platform
// compatible way for all operating systems.
func getVolumeDir(diskPath, volume string) string {
	volumes, e := ioutil.ReadDir(diskPath)
	if e != nil {
		return volume
	}
	for _, vol := range volumes {
		// Verify if lowercase version of the volume
		// is equal to the incoming volume, then use the proper name.
		if strings.ToLower(vol.Name()) == volume {
			return filepath.Join(diskPath, vol.Name())
		}
	}
	return filepath.Join(diskPath, volume)
}

// StatVol - get volume info.
func (s fsStorage) StatVol(volume string) (volInfo VolInfo, err error) {
	if volume == "" {
		return VolInfo{}, errInvalidArgument
	}
	volumeDir := getVolumeDir(s.diskPath, volume)
	// Stat a volume entry.
	var st os.FileInfo
	st, err = os.Stat(volumeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return VolInfo{}, errVolumeNotFound
		}
		return VolInfo{}, err
	}
	return VolInfo{
		Name:    st.Name(),
		Created: st.ModTime(),
	}, nil
}

// DeleteVol - delete a volume.
func (s fsStorage) DeleteVol(volume string) error {
	if volume == "" {
		return errInvalidArgument
	}
	err := os.Remove(getVolumeDir(s.diskPath, volume))
	if err != nil && os.IsNotExist(err) {
		return errVolumeNotFound
	}
	return err
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

// List of special prefixes for files, includes old and new ones.
var specialPrefixes = []string{
	"$multipart",
	"$tmpobject",
	"$tmpfile",
	// Add new special prefixes if any used.
}

// List operation.
func (s fsStorage) ListFiles(volume, prefix, marker string, recursive bool, count int) ([]FileInfo, bool, error) {
	if volume == "" {
		return nil, true, errInvalidArgument
	}

	var fileInfos []FileInfo

	volumeDir := getVolumeDir(s.diskPath, volume)
	// Verify if volume directory exists
	if exists, err := isDirExist(volumeDir); !exists {
		if err == nil {
			return nil, true, errVolumeNotFound
		} else if os.IsNotExist(err) {
			return nil, true, errVolumeNotFound
		} else {
			return nil, true, err
		}
	}
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
	if status, err := isDirExist(prefixRootDir); !status {
		if err == nil {
			// Prefix does not exist, not an error just respond empty list response.
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
		// TODO: Find a proper place to skip these files.
		// Skip temporary files.
		for _, specialPrefix := range specialPrefixes {
			if strings.Contains(fileInfo.Name, specialPrefix) {
				if walkResult.end {
					return fileInfos, true, nil
				}
				continue
			}
		}
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
	if volume == "" || path == "" {
		return nil, errInvalidArgument
	}
	volumeDir := getVolumeDir(s.diskPath, volume)
	// Verify if volume directory exists
	var exists bool
	if exists, err = isDirExist(volumeDir); !exists {
		if err == nil {
			return nil, errVolumeNotFound
		} else if os.IsNotExist(err) {
			return nil, errVolumeNotFound
		} else {
			return nil, err
		}
	}
	filePath := filepath.Join(volumeDir, path)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errFileNotFound
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
	if volume == "" || path == "" {
		return nil, errInvalidArgument
	}
	if e := checkDiskFree(s.diskPath, s.minFreeDisk); e != nil {
		return nil, e
	}
	volumeDir := getVolumeDir(s.diskPath, volume)
	// Verify if volume directory exists
	if exists, err := isDirExist(volumeDir); !exists {
		if err == nil {
			return nil, errVolumeNotFound
		} else if os.IsNotExist(err) {
			return nil, errVolumeNotFound
		} else {
			return nil, err
		}
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
	if volume == "" || path == "" {
		return FileInfo{}, errInvalidArgument
	}
	volumeDir := getVolumeDir(s.diskPath, volume)
	// Verify if volume directory exists
	var exists bool
	if exists, err = isDirExist(volumeDir); !exists {
		if err == nil {
			return FileInfo{}, errVolumeNotFound
		} else if os.IsNotExist(err) {
			return FileInfo{}, errVolumeNotFound
		} else {
			return FileInfo{}, err
		}
	}

	filePath := filepath.Join(volumeDir, path)
	st, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return FileInfo{}, errFileNotFound
		}
		return FileInfo{}, err
	}
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
func deleteFile(basePath, deletePath, volume, path string) error {
	if basePath == deletePath {
		return nil
	}
	// Verify if the path exists.
	pathSt, e := os.Stat(deletePath)
	if e != nil {
		return e
	}
	if pathSt.IsDir() {
		// Verify if directory is empty.
		empty, e := isDirEmpty(deletePath)
		if e != nil {
			return e
		}
		if !empty {
			return nil
		}
	}
	// Attempt to remove path.
	if e := os.Remove(deletePath); e != nil {
		return e
	}
	// Recursively go down the next path and delete again.
	if e := deleteFile(basePath, filepath.Dir(deletePath), volume, path); e != nil {
		return e
	}
	return nil
}

// DeleteFile - delete a file at path.
func (s fsStorage) DeleteFile(volume, path string) error {
	if volume == "" || path == "" {
		return errInvalidArgument
	}

	volumeDir := getVolumeDir(s.diskPath, volume)

	// Following code is needed so that we retain "/" suffix if any in
	// path argument. Do not use filepath.Join() since it would strip
	// off any suffixes.
	filePath := s.diskPath + string(os.PathSeparator) + volume + string(os.PathSeparator) + path

	// Delete file and delete parent directory as well if its empty.
	return deleteFile(volumeDir, filePath, volume, path)
}
