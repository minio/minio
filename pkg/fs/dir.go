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

package fs

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	// listObjectsLimit - maximum list objects limit
	listObjectsLimit = 1000
)

// isDirEmpty - returns whether given directory is empty or not
func isDirEmpty(dirname string) (status bool, err error) {
	f, err := os.Open(dirname)
	if err == nil {
		defer f.Close()
		if _, err = f.Readdirnames(1); err == io.EOF {
			status = true
			err = nil
		}
	}

	return
}

// isDirExist - returns whether given directory is exist or not
func isDirExist(dirname string) (status bool, err error) {
	fi, err := os.Lstat(dirname)
	if err == nil {
		status = fi.IsDir()
	}

	return
}

// byName implements sort.Interface for sorting os.FileInfo list
type byName []os.FileInfo

func (f byName) Len() int      { return len(f) }
func (f byName) Swap(i, j int) { f[i], f[j] = f[j], f[i] }
func (f byName) Less(i, j int) bool {
	n1 := f[i].Name()
	if f[i].IsDir() {
		n1 = n1 + string(os.PathSeparator)
	}

	n2 := f[j].Name()
	if f[j].IsDir() {
		n2 = n2 + string(os.PathSeparator)
	}

	return n1 < n2
}

// ObjectInfo - object info
type ObjectInfo struct {
	Bucket       string
	Name         string
	ModifiedTime time.Time
	ContentType  string
	MD5Sum       string
	Size         int64
	IsDir        bool
	Err          error
}

// readDir - read 'scanDir' directory.  It returns list of ObjectInfo where
//           each object name is appended with 'namePrefix'
func readDir(scanDir, namePrefix string) (objInfos []ObjectInfo) {
	f, err := os.Open(scanDir)
	if err != nil {
		objInfos = append(objInfos, ObjectInfo{Err: err})
		return
	}

	fis, err := f.Readdir(-1)
	if err != nil {
		f.Close()
		objInfos = append(objInfos, ObjectInfo{Err: err})
		return
	}

	// Close the directory
	f.Close()

	// Sort files by Name.
	sort.Sort(byName(fis))

	// Populate []ObjectInfo from []FileInfo
	for _, fi := range fis {
		name := fi.Name()
		size := fi.Size()
		modTime := fi.ModTime()
		isDir := fi.Mode().IsDir()

		// Add prefix if name prefix exists.
		if namePrefix != "" {
			name = namePrefix + "/" + name
		}

		// For directories explicitly end with '/'.
		if isDir {
			name += "/"
			size = 0 // Size is set to '0' for directories explicitly.
		}

		if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
			// Handle symlink by doing an additional stat and follow the link.
			st, e := os.Stat(filepath.Join(scanDir, name))
			if e != nil {
				objInfos = append(objInfos, ObjectInfo{Err: err})
				return
			}
			size = st.Size()
			modTime = st.ModTime()
			isDir = st.Mode().IsDir()
			// For directories explicitly end with '/'.
			if isDir {
				name += "/"
				size = 0 // Size is set to '0' for directories explicitly.
			}
		}

		// Populate []ObjectInfo.
		objInfos = append(objInfos, ObjectInfo{
			Name:         name,
			ModifiedTime: modTime,
			MD5Sum:       "", // TODO
			Size:         size,
			IsDir:        isDir,
		})
	}

	return
}

// ObjectInfoChannel - object info channel
type ObjectInfoChannel struct {
	ch        <-chan ObjectInfo
	objInfo   *ObjectInfo
	closed    bool
	timeoutCh <-chan struct{}
	timedOut  bool
}

func (oic *ObjectInfoChannel) Read() (ObjectInfo, bool) {
	if oic.closed {
		return ObjectInfo{}, false
	}

	if oic.objInfo == nil {
		// first read
		if oi, ok := <-oic.ch; ok {
			oic.objInfo = &oi
		} else {
			oic.closed = true
			return ObjectInfo{}, false
		}
	}

	retObjInfo := *oic.objInfo
	status := true
	oic.objInfo = nil

	// read once more to know whether it was last read
	if oi, ok := <-oic.ch; ok {
		oic.objInfo = &oi
	} else {
		oic.closed = true
	}

	return retObjInfo, status
}

// IsClosed - return whether channel is closed or not
func (oic ObjectInfoChannel) IsClosed() bool {
	if oic.objInfo != nil {
		return false
	}

	return oic.closed
}

// IsTimedOut - return whether channel is closed due to timeout
func (oic ObjectInfoChannel) IsTimedOut() bool {
	if oic.timedOut {
		return true
	}

	select {
	case _, ok := <-oic.timeoutCh:
		if ok {
			oic.timedOut = true
			return true
		}
		return false
	default:
		return false
	}
}

// treeWalk - walk into 'scanDir' recursively when 'recursive' is true.
//            It uses 'bucketDir' to get name prefix for object name.
func treeWalk(scanDir, bucketDir string, recursive bool) ObjectInfoChannel {
	objectInfoCh := make(chan ObjectInfo, listObjectsLimit)
	timeoutCh := make(chan struct{}, 1)

	// goroutine - retrieves directory entries, makes ObjectInfo and sends into the channel
	go func() {
		defer close(objectInfoCh)
		defer close(timeoutCh)

		// send function - returns true if ObjectInfo is sent
		//                 within (time.Second * 15) else false on time-out
		send := func(oi ObjectInfo) bool {
			timer := time.After(time.Second * 15)
			select {
			case objectInfoCh <- oi:
				return true
			case <-timer:
				timeoutCh <- struct{}{}
				return false
			}
		}

		namePrefix := strings.Replace(filepath.ToSlash(scanDir), filepath.ToSlash(bucketDir), "", 1)
		if strings.HasPrefix(namePrefix, "/") {
			/* remove beginning "/" */
			namePrefix = namePrefix[1:]
		}

		for objInfos := readDir(scanDir, namePrefix); len(objInfos) > 0; {
			var objInfo ObjectInfo
			objInfo, objInfos = objInfos[0], objInfos[1:]
			if !send(objInfo) {
				return
			}

			if objInfo.IsDir && recursive {
				scanDir := filepath.Join(bucketDir, filepath.FromSlash(objInfo.Name))

				namePrefix = strings.Replace(filepath.ToSlash(scanDir), filepath.ToSlash(bucketDir), "", 1)
				if strings.HasPrefix(namePrefix, "/") {
					/* remove beginning "/" */
					namePrefix = namePrefix[1:]
				}

				objInfos = append(readDir(scanDir, namePrefix), objInfos...)
			}
		}
	}()

	return ObjectInfoChannel{ch: objectInfoCh, timeoutCh: timeoutCh}
}
