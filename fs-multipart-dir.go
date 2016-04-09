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
	"os"
	"path/filepath"
	"strings"
	"time"
)

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
			markerPath = filepath.Join(markerPath, uploadIDMarker+multipartUploadIDSuffix)
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
			// Filters scandir entries. This filter function is
			// specific for multipart listing.
			multipartFilterFn := func(dirent fsDirent) bool {
				// Verify if dirent is a directory a regular file
				// with match uploadID suffix.
				if dirent.IsDir() || (dirent.IsRegular() && strings.HasSuffix(dirent.name, multipartUploadIDSuffix)) {
					// Return if dirent matches prefix and
					// lexically higher than marker.
					return strings.HasPrefix(dirent.name, prefixPath) && dirent.name > markerPath
				}
				return false
			}
			dirents, err := scandir(scanDir, multipartFilterFn, false)
			if err != nil {
				send(multipartObjectInfo{Err: err})
				return
			}

			var dirent fsDirent
			for len(dirents) > 0 {
				dirent, dirents = dirents[0], dirents[1:]
				if dirent.IsRegular() {
					// Handle uploadid file
					name := strings.Replace(filepath.Dir(dirent.name), bucketDir, "", 1)
					if name == "" {
						// This should not happen ie uploadid file should not be in bucket directory
						send(multipartObjectInfo{Err: errors.New("Corrupted metadata")})
						return
					}

					uploadID := strings.Split(filepath.Base(dirent.name), multipartUploadIDSuffix)[0]

					// Solaris and older unixes have modTime to be
					// empty, fall back to os.Stat() to fill missing values.
					if dirent.modTime.IsZero() {
						if fi, e := os.Stat(dirent.name); e == nil {
							dirent.modTime = fi.ModTime()
						} else {
							send(multipartObjectInfo{Err: e})
							return
						}
					}

					objInfo := multipartObjectInfo{
						Name:         name,
						UploadID:     uploadID,
						ModifiedTime: dirent.modTime,
					}

					if !send(objInfo) {
						return
					}

					continue
				}

				multipartSubDirentFilterFn := func(dirent fsDirent) bool {
					return dirent.IsDir() || (dirent.IsRegular() && strings.HasSuffix(dirent.name, multipartUploadIDSuffix))
				}
				// Fetch sub dirents.
				subDirents, err := scandir(dirent.name, multipartSubDirentFilterFn, false)
				if err != nil {
					send(multipartObjectInfo{Err: err})
					return
				}

				subDirFound := false
				uploadIDDirents := []fsDirent{}
				// If subDirents has a directory, then current dirent needs to be sent
				for _, subdirent := range subDirents {
					if subdirent.IsDir() {
						subDirFound = true

						if recursive {
							break
						}
					}

					if !recursive && subdirent.IsRegular() {
						uploadIDDirents = append(uploadIDDirents, subdirent)
					}
				}

				// Send directory only for non-recursive listing
				if !recursive && (subDirFound || len(subDirents) == 0) {
					// Solaris and older unixes have modTime to be
					// empty, fall back to os.Stat() to fill missing values.
					if dirent.modTime.IsZero() {
						if fi, e := os.Stat(dirent.name); e == nil {
							dirent.modTime = fi.ModTime()
						} else {
							send(multipartObjectInfo{Err: e})
							return
						}
					}

					objInfo := multipartObjectInfo{
						Name:         strings.Replace(dirent.name, bucketDir, "", 1),
						ModifiedTime: dirent.modTime,
						IsDir:        true,
					}

					if !send(objInfo) {
						return
					}
				}

				if recursive {
					dirents = append(subDirents, dirents...)
				} else {
					dirents = append(uploadIDDirents, dirents...)
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

	// Return multipart info.
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
