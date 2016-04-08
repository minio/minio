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
	"fmt"
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

type fs struct {
	path               string
	info               disk.Info
	minFreeDisk        int64
	rwLock             *sync.RWMutex
	listObjectMap      map[listParams][]*treeWalker
	listObjectMapMutex *sync.Mutex
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
func newFS(path string) (StorageAPI, error) {
	if path == "" {
		return nil, errInvalidArgument
	}
	st, e := os.Stat(path)
	if e != nil {
		return nil, e
	}
	if !st.IsDir() {
		return nil, syscall.ENOTDIR
	}

	info, e := disk.GetInfo(path)
	if e != nil {
		return nil, e
	}
	filesystem := fs{
		path:               path,
		info:               info,
		minFreeDisk:        5, // Minimum 10% disk should be free.
		listObjectMap:      make(map[listParams][]*treeWalker),
		listObjectMapMutex: &sync.Mutex{},
		rwLock:             &sync.RWMutex{},
	}
	return filesystem, nil
}

// Make a volume entry.
func (s fs) MakeVol(volume string) error {
	di, e := disk.GetInfo(s.path)
	if e != nil {
		return e
	}

	// Remove 5% from total space for cumulative disk space used for journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= s.minFreeDisk {
		return errDiskPathFull
	}

	volumeDir := getVolumeDir(s.path, volume)
	if _, e := os.Stat(volumeDir); e == nil {
		return errVolumeExists
	}

	// Make a volume entry.
	if e := os.Mkdir(volumeDir, 0700); e != nil {
		return e
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
func (s fs) ListVols() ([]VolInfo, error) {
	files, e := ioutil.ReadDir(s.path)
	if e != nil {
		return nil, e
	}
	var volsInfo []VolInfo
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
func getVolumeDir(path, volume string) string {
	volumes, e := ioutil.ReadDir(path)
	if e != nil {
		return volume
	}
	for _, vol := range volumes {
		// Verify if lowercase version of the volume
		// is equal to the incoming volume, then use the proper name.
		if strings.ToLower(vol.Name()) == volume {
			return filepath.Join(path, vol.Name())
		}
	}
	return filepath.Join(path, volume)
}

// StatVol - get volume info.
func (s fs) StatVol(volume string) (VolInfo, error) {
	volumeDir := getVolumeDir(s.path, volume)
	// Stat a volume entry.
	st, e := os.Stat(volumeDir)
	if e != nil {
		return VolInfo{}, e
	}
	return VolInfo{st.Name(), st.ModTime()}, nil
}

func (s fs) DeleteVol(volume string) error {
	return os.Remove(getVolumeDir(s.path, volume))
}

// Save the goroutine reference in the map
func (s *fs) saveTreeWalk(params listParams, walker *treeWalker) {
	s.listObjectMapMutex.Lock()
	defer s.listObjectMapMutex.Unlock()

	walkers, _ := s.listObjectMap[params]
	walkers = append(walkers, walker)

	s.listObjectMap[params] = walkers
}

// Lookup the goroutine reference from map
func (s *fs) lookupTreeWalk(params listParams) *treeWalker {
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

// List operaion
func (s fs) ListFiles(volume, prefix, marker string, recursive bool, count int) ([]FileInfo, bool, error) {
	var fileInfos []FileInfo

	volumeDir := getVolumeDir(s.path, volume)
	// Verify if volume directory exists
	var exists bool
	var e error
	if exists, e = isDirExist(volumeDir); e != nil {
		return nil, false, e
	}
	if !exists {
		return nil, false, errVolumeExists
	}

	// Verify if marker has prefix.
	if marker != "" && !strings.HasPrefix(marker, prefix) {
		return nil, false, fmt.Errorf("Invalid combination of marker '%s' and prefix '%s'", marker, prefix)
	}

	// Return empty response for a valid request when count is 0.
	if count == 0 {
		return nil, false, nil
	}

	// Over flowing count - reset to fsListLimit.
	if count < 0 || count > fsListLimit {
		count = fsListLimit
	}

	// Maximum 1000 files returned in a single call.
	// Further calls will set right marker value to continue reading the rest of the files.
	// popTreeWalker returns nil if the call to ListFiles is done for the first time.
	// On further calls to ListFiles to retrive more files within the timeout period,
	// popTreeWalker returns the channel from which rest of the objects can be retrieved.
	walker := s.lookupTreeWalk(listParams{volume, recursive, marker, prefix})
	if walker == nil {
		walker = startTreeWalk(s.path, volume, filepath.FromSlash(prefix), filepath.FromSlash(marker), recursive)
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
			return nil, false, walkResult.err
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

func (s fs) ReadFile(volume string, path string, offset int64) (io.ReadCloser, error) {
	filePath := filepath.Join(getVolumeDir(s.path, volume), path)
	file, e := os.Open(filePath)
	if e != nil {
		return nil, e
	}
	_, e = file.Seek(offset, os.SEEK_SET)
	if e != nil {
		return nil, e
	}
	return file, nil
}

// CreateFile - create a file at path.
func (s fs) CreateFile(volume, path string) (writeCloser io.WriteCloser, err error) {
	filePath := filepath.Join(getVolumeDir(s.path, volume), path)
	return safe.CreateFileWithPrefix(filePath, "$tmpfile")
}

func (s fs) StatFile(volume, path string) (file FileInfo, err error) {
	filePath := filepath.Join(getVolumeDir(s.path, volume), path)
	st, e := os.Stat(filePath)
	if e != nil {
		return FileInfo{}, ObjectNotFound{Bucket: volume, Object: path}
	}
	if st.Mode().IsDir() {
		return FileInfo{}, ObjectNotFound{Bucket: volume, Object: path}
	}
	file = FileInfo{
		Volume:       volume,
		Name:         path,
		ModifiedTime: st.ModTime(),
		Size:         st.Size(),
		IsDir:        st.Mode().IsDir(),
	}
	if file.IsDir {
		file.Size = 0
	}
	return file, nil
}

func (s fs) DeleteFile(volume, path string) error {
	filePath := filepath.Join(getVolumeDir(s.path, volume), path)
	return os.Remove(filePath)
}
