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
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// DirEntry - directory entry
type DirEntry struct {
	Name    string
	Size    int64
	Mode    os.FileMode
	ModTime time.Time
}

// IsDir - returns true if DirEntry is a directory
func (entry DirEntry) IsDir() bool {
	return entry.Mode.IsDir()
}

// IsSymlink - returns true if DirEntry is a symbolic link
func (entry DirEntry) IsSymlink() bool {
	return entry.Mode&os.ModeSymlink == os.ModeSymlink
}

// IsRegular - returns true if DirEntry is a regular file
func (entry DirEntry) IsRegular() bool {
	return entry.Mode.IsRegular()
}

// sort interface for DirEntry slice
type byEntryName []DirEntry

func (f byEntryName) Len() int           { return len(f) }
func (f byEntryName) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f byEntryName) Less(i, j int) bool { return f[i].Name < f[j].Name }

func filteredReaddir(dirname string, filter func(DirEntry) bool, appendPath bool) ([]DirEntry, error) {
	result := []DirEntry{}

	d, err := os.Open(dirname)
	if err != nil {
		return result, err
	}

	defer d.Close()

	for {
		fis, err := d.Readdir(1000)
		if err != nil {
			if err == io.EOF {
				break
			}

			return result, err
		}

		for _, fi := range fis {
			name := fi.Name()
			if appendPath {
				name = filepath.Join(dirname, name)
			}

			if fi.IsDir() {
				name += string(os.PathSeparator)
			}

			entry := DirEntry{Name: name, Size: fi.Size(), Mode: fi.Mode(), ModTime: fi.ModTime()}

			if filter == nil || filter(entry) {
				result = append(result, entry)
			}
		}
	}

	sort.Sort(byEntryName(result))

	return result, nil
}

func filteredReaddirnames(dirname string, filter func(string) bool) ([]string, error) {
	result := []string{}
	d, err := os.Open(dirname)
	if err != nil {
		return result, err
	}

	defer d.Close()

	for {
		names, err := d.Readdirnames(1000)
		if err != nil {
			if err == io.EOF {
				break
			}

			return result, err
		}

		for _, name := range names {
			if filter == nil || filter(name) {
				result = append(result, name)
			}
		}
	}

	sort.Strings(result)

	return result, nil
}

func scanMultipartDir(bucketDir, prefixPath, markerPath, uploadIDMarker string, recursive bool) multipartObjectInfoChannel {
	objectInfoCh := make(chan multipartObjectInfo, listObjectsLimit)
	timeoutCh := make(chan struct{}, 1)

	// TODO: check if bucketDir is absolute path
	scanDir := bucketDir
	dirDepth := bucketDir

	if prefixPath != "" {
		if !filepath.IsAbs(prefixPath) {
			tmpPrefixPath := filepath.Join(bucketDir, prefixPath)
			if strings.HasSuffix(prefixPath, string(os.PathSeparator)) {
				tmpPrefixPath += string(os.PathSeparator)
			}
			prefixPath = tmpPrefixPath
		}

		// TODO: check if prefixPath starts with bucketDir

		// Case #1: if prefixPath is /mnt/mys3/mybucket/2012/photos/paris, then
		//          dirDepth is /mnt/mys3/mybucket/2012/photos
		// Case #2: if prefixPath is /mnt/mys3/mybucket/2012/photos/, then
		//          dirDepth is /mnt/mys3/mybucket/2012/photos
		dirDepth = filepath.Dir(prefixPath)
		scanDir = dirDepth
	} else {
		prefixPath = bucketDir
	}

	if markerPath != "" {
		if !filepath.IsAbs(markerPath) {
			tmpMarkerPath := filepath.Join(bucketDir, markerPath)
			if strings.HasSuffix(markerPath, string(os.PathSeparator)) {
				tmpMarkerPath += string(os.PathSeparator)
			}

			markerPath = tmpMarkerPath
		}

		// TODO: check markerPath must be a file
		if uploadIDMarker != "" {
			markerPath = filepath.Join(markerPath, uploadIDMarker+uploadIDSuffix)
		}

		// TODO: check if markerPath starts with bucketDir
		// TODO: check if markerPath starts with prefixPath

		// Case #1: if markerPath is /mnt/mys3/mybucket/2012/photos/gophercon.png, then
		//          scanDir is /mnt/mys3/mybucket/2012/photos
		// Case #2: if markerPath is /mnt/mys3/mybucket/2012/photos/gophercon.png/1fbd117a-268a-4ed0-85c9-8cc3888cbf20.uploadid, then
		//          scanDir is /mnt/mys3/mybucket/2012/photos/gophercon.png
		// Case #3: if markerPath is /mnt/mys3/mybucket/2012/photos/, then
		//          scanDir is /mnt/mys3/mybucket/2012/photos

		scanDir = filepath.Dir(markerPath)
	} else {
		markerPath = bucketDir
	}

	// Have bucketDir ends with os.PathSeparator
	if !strings.HasSuffix(bucketDir, string(os.PathSeparator)) {
		bucketDir += string(os.PathSeparator)
	}

	// Remove os.PathSeparator if scanDir ends with
	if strings.HasSuffix(scanDir, string(os.PathSeparator)) {
		scanDir = filepath.Dir(scanDir)
	}

	// goroutine - retrieves directory entries, makes ObjectInfo and sends into the channel.
	go func() {
		defer close(objectInfoCh)
		defer close(timeoutCh)

		// send function - returns true if ObjectInfo is sent
		// within (time.Second * 15) else false on timeout.
		send := func(oi multipartObjectInfo) bool {
			timer := time.After(time.Second * 15)
			select {
			case objectInfoCh <- oi:
				return true
			case <-timer:
				timeoutCh <- struct{}{}
				return false
			}
		}

		for {
			entries, err := filteredReaddir(scanDir,
				func(entry DirEntry) bool {
					if entry.IsDir() || (entry.IsRegular() && strings.HasSuffix(entry.Name, uploadIDSuffix)) {
						return strings.HasPrefix(entry.Name, prefixPath) && entry.Name > markerPath
					}

					return false
				},
				true)
			if err != nil {
				send(multipartObjectInfo{Err: err})
				return
			}

			var entry DirEntry
			for len(entries) > 0 {
				entry, entries = entries[0], entries[1:]

				if entry.IsRegular() {
					// Handle uploadid file
					name := strings.Replace(filepath.Dir(entry.Name), bucketDir, "", 1)
					if name == "" {
						// This should not happen ie uploadid file should not be in bucket directory
						send(multipartObjectInfo{Err: errors.New("corrupted meta data")})
						return
					}

					uploadID := strings.Split(filepath.Base(entry.Name), uploadIDSuffix)[0]

					objInfo := multipartObjectInfo{
						Name:         name,
						UploadID:     uploadID,
						ModifiedTime: entry.ModTime,
					}

					if !send(objInfo) {
						return
					}

					continue
				}

				subentries, err := filteredReaddir(entry.Name,
					func(entry DirEntry) bool {
						return entry.IsDir() || (entry.IsRegular() && strings.HasSuffix(entry.Name, uploadIDSuffix))
					},
					true)
				if err != nil {
					send(multipartObjectInfo{Err: err})
					return
				}

				subDirFound := false
				uploadIDEntries := []DirEntry{}
				// If subentries has a directory, then current entry needs to be sent
				for _, subentry := range subentries {
					if subentry.IsDir() {
						subDirFound = true

						if recursive {
							break
						}
					}

					if !recursive && subentry.IsRegular() {
						uploadIDEntries = append(uploadIDEntries, subentry)
					}
				}

				// send directory only for non-recursive listing
				if !recursive && (subDirFound || len(subentries) == 0) {
					objInfo := multipartObjectInfo{
						Name:         strings.Replace(entry.Name, bucketDir, "", 1),
						ModifiedTime: entry.ModTime,
						IsDir:        true,
					}

					if !send(objInfo) {
						return
					}
				}

				if recursive {
					entries = append(subentries, entries...)
				} else {
					entries = append(uploadIDEntries, entries...)
				}
			}

			if !recursive {
				break
			}

			markerPath = scanDir + string(os.PathSeparator)

			if scanDir = filepath.Dir(scanDir); scanDir < dirDepth {
				break
			}
		}
	}()

	return multipartObjectInfoChannel{ch: objectInfoCh, timeoutCh: timeoutCh}
}

// multipartObjectInfo - Multipart object info
type multipartObjectInfo struct {
	Name         string
	UploadID     string
	ModifiedTime time.Time
	IsDir        bool
	Err          error
}

// multipartObjectInfoChannel - multipart object info channel
type multipartObjectInfoChannel struct {
	ch        <-chan multipartObjectInfo
	objInfo   *multipartObjectInfo
	closed    bool
	timeoutCh <-chan struct{}
	timedOut  bool
}

func (oic *multipartObjectInfoChannel) Read() (multipartObjectInfo, bool) {
	if oic.closed {
		return multipartObjectInfo{}, false
	}

	if oic.objInfo == nil {
		// First read.
		if oi, ok := <-oic.ch; ok {
			oic.objInfo = &oi
		} else {
			oic.closed = true
			return multipartObjectInfo{}, false
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
func (oic multipartObjectInfoChannel) IsClosed() bool {
	if oic.objInfo != nil {
		return false
	}
	return oic.closed
}

// IsTimedOut - return whether channel is closed due to timeout.
func (oic multipartObjectInfoChannel) IsTimedOut() bool {
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
