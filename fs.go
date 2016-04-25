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

	"github.com/Sirupsen/logrus"
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
		log.Debug("Disk cannot be empty")
		return nil, errInvalidArgument
	}
	st, err := os.Stat(diskPath)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": diskPath,
		}).Debugf("Stat failed, with error %s.", err)
		return nil, err
	}
	if !st.IsDir() {
		log.WithFields(logrus.Fields{
			"diskPath": diskPath,
		}).Debugf("Disk %s.", syscall.ENOTDIR)
		return nil, syscall.ENOTDIR
	}
	fs := fsStorage{
		diskPath:           diskPath,
		minFreeDisk:        5, // Minimum 5% disk should be free.
		listObjectMap:      make(map[listParams][]*treeWalker),
		listObjectMapMutex: &sync.Mutex{},
	}
	log.WithFields(logrus.Fields{
		"diskPath":    diskPath,
		"minFreeDisk": 5,
	}).Debugf("Successfully configured FS storage API.")
	return fs, nil
}

// checkDiskFree verifies if disk path has sufficient minium free disk
// space.
func checkDiskFree(diskPath string, minFreeDisk int64) (err error) {
	di, err := disk.GetInfo(diskPath)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": diskPath,
		}).Debugf("Failed to get disk info, %s", err)
		return err
	}

	// Remove 5% from total space for cumulative disk
	// space used for journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= minFreeDisk {
		log.WithFields(logrus.Fields{
			"availableDiskSpace": int64(availableDiskSpace),
			"minFreeDiskSpace":   minFreeDisk,
		}).Debugf("Disk free space has reached its limit.")
		return errDiskFull
	}

	// Success.
	return nil
}

func removeDuplicateVols(volsInfo []VolInfo) []VolInfo {
	// Use map to record duplicates as we find them.
	result := []VolInfo{}

	m := make(map[string]VolInfo)
	for _, v := range volsInfo {
		if _, found := m[v.Name]; !found {
			m[v.Name] = v
		}
	}

	result = make([]VolInfo, 0, len(m))
	for _, v := range m {
		result = append(result, v)
	}

	// Return the new slice.
	return result
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
		log.WithFields(logrus.Fields{
			"dirPath":   dirPath,
			"namesOnly": true,
		}).Debugf("Scandir failed with error %s", err)
		return nil, err
	}
	var volsInfo []VolInfo
	for _, dirent := range dirents {
		fi, err := os.Stat(filepath.Join(dirPath, dirent.name))
		if err != nil {
			log.WithFields(logrus.Fields{
				"path": filepath.Join(dirPath, dirent.name),
			}).Debugf("Stat failed with error %s", err)
			return nil, err
		}
		volsInfo = append(volsInfo, VolInfo{
			Name: fi.Name(),
			// As os.Stat() doesn't carry other than ModTime(), use
			// ModTime() as CreatedTime.
			Created: fi.ModTime(),
		})
	}
	return removeDuplicateVols(volsInfo), nil
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
			// Verify if lowercase version of
			// the volume
			// is equal to the incoming volume, then use the proper
			// name.
			if strings.ToLower(vol.Name) == volume {
				volumeDir = filepath.Join(s.diskPath, vol.Name)
				return volumeDir, nil
			}
		}
		return volumeDir, errVolumeNotFound
	} else if os.IsPermission(err) {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
		}).Debugf("Stat failed with error %s", err)
		return volumeDir, errVolumeAccessDenied
	}
	log.WithFields(logrus.Fields{
		"diskPath": s.diskPath,
	}).Debugf("Stat failed with error %s", err)
	return volumeDir, err
}

// Make a volume entry.
func (s fsStorage) MakeVol(volume string) (err error) {
	// Validate if disk is free.
	if err = checkDiskFree(s.diskPath, s.minFreeDisk); err != nil {
		return err
	}

	volumeDir, err := s.getVolumeDir(volume)
	if err == nil {
		// Volume already exists, return error.
		return errVolumeExists
	}

	// If volume not found create it.
	if err == errVolumeNotFound {
		// Make a volume entry.
		return os.Mkdir(volumeDir, 0700)
	}

	log.WithFields(logrus.Fields{
		"diskPath": s.diskPath,
		"volume":   volume,
	}).Debugf("MakeVol failed with %s", err)

	// For all other errors return here.
	return err
}

// ListVols - list volumes.
func (s fsStorage) ListVols() (volsInfo []VolInfo, err error) {
	// Get disk info to be populated for VolInfo.
	var diskInfo disk.Info
	diskInfo, err = disk.GetInfo(s.diskPath)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
		}).Debugf("Failed to get disk info, %s", err)
		return nil, err
	}
	volsInfo, err = getAllUniqueVols(s.diskPath)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
		}).Debugf("getAllUniqueVols failed with %s", err)
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
			Total:   diskInfo.Total,
			Free:    diskInfo.Free,
			FSType:  diskInfo.FSType,
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
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"volume":   volume,
		}).Debugf("getVolumeDir failed with %s", err)
		return VolInfo{}, err
	}
	// Stat a volume entry.
	var st os.FileInfo
	st, err = os.Stat(volumeDir)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"volume":   volume,
		}).Debugf("Stat on the volume failed with %s", err)
		if os.IsNotExist(err) {
			return VolInfo{}, errVolumeNotFound
		}
		return VolInfo{}, err
	}
	// Get disk info, to be returned back along with volume info.
	var diskInfo disk.Info
	diskInfo, err = disk.GetInfo(s.diskPath)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"volume":   volume,
		}).Debugf("Failed to get disk info, %s", err)
		return VolInfo{}, err
	}
	// As os.Stat() doesn't carry other than ModTime(), use ModTime()
	// as CreatedTime.
	createdTime := st.ModTime()
	return VolInfo{
		Name:    volume,
		Created: createdTime,
		Free:    diskInfo.Free,
		Total:   diskInfo.Total,
		FSType:  diskInfo.FSType,
	}, nil
}

// DeleteVol - delete a volume.
func (s fsStorage) DeleteVol(volume string) error {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolumeDir(volume)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"volume":   volume,
		}).Debugf("getVolumeDir failed with %s", err)
		return err
	}
	err = os.Remove(volumeDir)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"volume":   volume,
		}).Debugf("Volume remove failed with %s", err)
		if os.IsNotExist(err) {
			return errVolumeNotFound
		} else if strings.Contains(err.Error(), "directory is not empty") {
			// On windows the string is slightly different, handle it here.
			return errVolumeNotEmpty
		} else if strings.Contains(err.Error(), "directory not empty") {
			// Hopefully for all other
			// operating systems, this is
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

	log.WithFields(logrus.Fields{
		"bucket":    params.bucket,
		"recursive": params.recursive,
		"marker":    params.marker,
		"prefix":    params.prefix,
	}).Debugf("saveTreeWalk has been invoked.")

	walkers, _ := s.listObjectMap[params]
	walkers = append(walkers, walker)

	s.listObjectMap[params] = walkers
	log.Debugf("Successfully saved in listObjectMap.")
}

// Lookup the goroutine reference from map
func (s *fsStorage) lookupTreeWalk(params listParams) *treeWalker {
	s.listObjectMapMutex.Lock()
	defer s.listObjectMapMutex.Unlock()

	log.WithFields(logrus.Fields{
		"bucket":    params.bucket,
		"recursive": params.recursive,
		"marker":    params.marker,
		"prefix":    params.prefix,
	}).Debugf("lookupTreeWalk has been invoked.")
	if walkChs, ok := s.listObjectMap[params]; ok {
		for i, walkCh := range walkChs {
			if !walkCh.timedOut {
				newWalkChs := walkChs[i+1:]
				if len(newWalkChs) > 0 {
					s.listObjectMap[params] = newWalkChs
				} else {
					delete(s.listObjectMap, params)
				}
				log.WithFields(logrus.Fields{
					"bucket":    params.bucket,
					"recursive": params.recursive,
					"marker":    params.marker,
					"prefix":    params.prefix,
				}).Debugf("Found the previous saved listsObjects params.")
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
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"volume":   volume,
		}).Debugf("getVolumeDir failed with %s", err)
		return nil, true, err
	}
	var fileInfos []FileInfo

	if marker != "" {
		// Verify if marker has prefix.
		if marker != "" && !strings.HasPrefix(marker, prefix) {
			log.WithFields(logrus.Fields{
				"diskPath": s.diskPath,
				"marker":   marker,
				"prefix":   prefix,
			}).Debugf("Marker doesn't have prefix in common.")
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
		log.WithFields(logrus.Fields{
			"volumeDir":     volumeDir,
			"prefixRootDir": prefixRootDir,
		}).Debugf("isDirExist returned an unhandled error %s", err)

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
	log.Debugf("Reading from the tree walk channel has begun.")
	for i := 0; i < count; {
		walkResult, ok := <-walker.ch
		if !ok {
			// Closed channel.
			return fileInfos, true, nil
		}
		// For any walk error return right away.
		if walkResult.err != nil {
			log.WithFields(logrus.Fields{
				"diskPath":  s.diskPath,
				"volume":    volume,
				"prefix":    prefix,
				"marker":    marker,
				"recursive": recursive,
			}).Debugf("Walk resulted in an error %s", walkResult.err)
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
	params := listParams{volume, recursive, nextMarker, prefix}
	log.WithFields(logrus.Fields{
		"bucket":    params.bucket,
		"recursive": params.recursive,
		"marker":    params.marker,
		"prefix":    params.prefix,
	}).Debugf("Save the tree walk into map for subsequent requests.")
	s.saveTreeWalk(params, walker)
	return fileInfos, false, nil
}

// ReadFile - read a file at a given offset.
func (s fsStorage) ReadFile(volume string, path string, offset int64) (readCloser io.ReadCloser, err error) {
	volumeDir, err := s.getVolumeDir(volume)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"volume":   volume,
		}).Debugf("getVolumeDir failed with %s", err)
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
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"filePath": filePath,
		}).Debugf("Opening a file failed with %s", err)
		return nil, err
	}
	st, err := file.Stat()
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"filePath": filePath,
		}).Debugf("Stat failed with %s", err)
		return nil, err
	}
	// Verify if its not a regular file, since subsequent Seek is undefined.
	if !st.Mode().IsRegular() {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"filePath": filePath,
		}).Debugf("Unexpected type %s", errIsNotRegular)
		return nil, errFileNotFound
	}
	// Seek to requested offset.
	_, err = file.Seek(offset, os.SEEK_SET)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"filePath": filePath,
			"offset":   offset,
		}).Debugf("Seek failed with %s", err)
		return nil, err
	}
	return file, nil
}

// CreateFile - create a file at path.
func (s fsStorage) CreateFile(volume, path string) (writeCloser io.WriteCloser, err error) {
	volumeDir, err := s.getVolumeDir(volume)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"volume":   volume,
		}).Debugf("getVolumeDir failed with %s", err)
		return nil, err
	}
	if err := checkDiskFree(s.diskPath, s.minFreeDisk); err != nil {
		return nil, err
	}
	filePath := filepath.Join(volumeDir, path)
	// Verify if the file already exists and is not of regular type.
	if st, err := os.Stat(filePath); err == nil {
		if st.IsDir() {
			log.WithFields(logrus.Fields{
				"diskPath": s.diskPath,
				"filePath": filePath,
			}).Debugf("Unexpected type %s", errIsNotRegular)
			return nil, errIsNotRegular
		}
	}
	return safe.CreateFileWithPrefix(filePath, "$tmpfile")
}

// StatFile - get file info.
func (s fsStorage) StatFile(volume, path string) (file FileInfo, err error) {
	volumeDir, err := s.getVolumeDir(volume)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"volume":   volume,
		}).Debugf("getVolumeDir failed with %s", err)
		return FileInfo{}, err
	}

	filePath := filepath.Join(volumeDir, filepath.FromSlash(path))
	st, err := os.Stat(filePath)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"filePath": filePath,
		}).Debugf("Stat failed with %s", err)

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
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"filePath": filePath,
		}).Debugf("File is %s.", errIsNotRegular)
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
		log.WithFields(logrus.Fields{
			"deletePath": deletePath,
		}).Debugf("Stat failed with %s", err)
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
			log.WithFields(logrus.Fields{
				"deletePath": deletePath,
			}).Debugf("isDirEmpty failed with %s", err)
			return err
		}
		if !empty {
			return nil
		}
	}
	// Attempt to remove path.
	if err := os.Remove(deletePath); err != nil {
		log.WithFields(logrus.Fields{
			"deletePath": deletePath,
		}).Debugf("Remove failed with %s", err)
		return err
	}
	// Recursively go down the next path and delete again.
	if err := deleteFile(basePath, filepath.Dir(deletePath)); err != nil {
		log.WithFields(logrus.Fields{
			"basePath":  basePath,
			"deleteDir": filepath.Dir(deletePath),
		}).Debugf("deleteFile failed with %s", err)
		return err
	}
	return nil
}

// DeleteFile - delete a file at path.
func (s fsStorage) DeleteFile(volume, path string) error {
	volumeDir, err := s.getVolumeDir(volume)
	if err != nil {
		log.WithFields(logrus.Fields{
			"diskPath": s.diskPath,
			"volume":   volume,
		}).Debugf("getVolumeDir failed with %s", err)
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
