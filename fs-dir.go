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
	"sort"
	"strings"
	"time"
)

const (
	// listObjectsLimit - maximum list objects limit.
	listObjectsLimit = 1000
)

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

	return
}

// isDirExist - returns whether given directory is exist or not.
func isDirExist(dirname string) (status bool, err error) {
	fi, err := os.Lstat(dirname)
	if err == nil {
		status = fi.IsDir()
	}

	return
}

// byName implements sort.Interface for sorting os.FileInfo list.
type byName []os.FileInfo

func (f byName) Len() int {
	return len(f)
}

func (f byName) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

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

// Using sort.Search() internally to jump to the file entry containing the prefix.
func searchFileInfos(fileInfos []os.FileInfo, x string) int {
	processFunc := func(i int) bool {
		return fileInfos[i].Name() >= x
	}
	return sort.Search(len(fileInfos), processFunc)
}

// readDir - read 'scanDir' directory.  It returns list of ObjectInfo.
// Each object name is appended with 'namePrefix'.
func readDir(scanDir, namePrefix, queryPrefix string, isFirst bool) (objInfos []ObjectInfo) {
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
	// Close the directory.
	f.Close()
	// Sort files by Name.
	sort.Sort(byName(fis))

	var prefixIndex int
	// Searching for entries with objectName containing  prefix.
	// Binary search is used  for efficient search.
	if queryPrefix != "" && isFirst {
		prefixIndex = searchFileInfos(fis, queryPrefix)
		if prefixIndex == len(fis) {
			return
		}

		if !strings.HasPrefix(fis[prefixIndex].Name(), queryPrefix) {
			return
		}
		fis = fis[prefixIndex:]

	}

	// Populate []ObjectInfo from []FileInfo.
	for _, fi := range fis {
		name := fi.Name()
		if queryPrefix != "" && isFirst {
			// If control is here then there is a queryPrefix, and there are objects which satisfies the prefix.
			// Since the result is sorted, the object names which satisfies query prefix would be stored one after the other.
			// Push the ObjectInfo only if its contains the prefix.
			// This ensures that the channel containing object Info would only has objects with the given queryPrefix.
			if !strings.HasPrefix(name, queryPrefix) {
				return
			}
		}
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
			// size is set to '0' for directories explicitly.
			size = 0
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
				// size is set to '0' for directories explicitly.
				size = 0
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

// objectInfoChannel - object info channel.
type objectInfoChannel struct {
	ch        <-chan ObjectInfo
	objInfo   *ObjectInfo
	closed    bool
	timeoutCh <-chan struct{}
	timedOut  bool
}

func (oic *objectInfoChannel) Read() (ObjectInfo, bool) {
	if oic.closed {
		return ObjectInfo{}, false
	}

	if oic.objInfo == nil {
		// First read.
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

	// Read once more to know whether it was last read.
	if oi, ok := <-oic.ch; ok {
		oic.objInfo = &oi
	} else {
		oic.closed = true
	}

	return retObjInfo, status
}

// IsClosed - return whether channel is closed or not.
func (oic objectInfoChannel) IsClosed() bool {
	if oic.objInfo != nil {
		return false
	}

	return oic.closed
}

// IsTimedOut - return whether channel is closed due to timeout.
func (oic objectInfoChannel) IsTimedOut() bool {
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
// It uses 'bucketDir' to get name prefix for object name.
func treeWalk(scanDir, bucketDir string, recursive bool, queryPrefix string) objectInfoChannel {
	objectInfoCh := make(chan ObjectInfo, listObjectsLimit)
	timeoutCh := make(chan struct{}, 1)

	// goroutine - retrieves directory entries, makes ObjectInfo and sends into the channel.
	go func() {
		defer close(objectInfoCh)
		defer close(timeoutCh)

		// send function - returns true if ObjectInfo is sent.
		// Within (time.Second * 15) else false on time-out.
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
			// Remove forward slash ("/") from beginning.
			namePrefix = namePrefix[1:]
		}
		// The last argument (isFisrt), is set to `true` only during the first run of the function.
		// This makes sure that the sub-directories inside the prefixDir are recursed
		// without being asserted for prefix in the object name.
		isFirst := true
		for objInfos := readDir(scanDir, namePrefix, queryPrefix, isFirst); len(objInfos) > 0; {
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
				// The last argument is set to false in the further calls to readdir.
				isFirst = false
				objInfos = append(readDir(scanDir, namePrefix, queryPrefix, isFirst), objInfos...)
			}
		}
	}()

	return objectInfoChannel{ch: objectInfoCh, timeoutCh: timeoutCh}
}
