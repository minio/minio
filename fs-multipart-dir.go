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

func scanMultipartDir(bucketDir, prefixPath, markerPath, uploadIDMarker string, recursive bool) <-chan multipartObjectInfo {
	objectInfoCh := make(chan multipartObjectInfo, listObjectsLimit)

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

		// send function - returns true if ObjectInfo is sent
		// within (time.Second * 15) else false on timeout.
		send := func(oi multipartObjectInfo) bool {
			timer := time.After(time.Second * 15)
			select {
			case objectInfoCh <- oi:
				return true
			case <-timer:
				return false
			}
		}

		// filter function - filters directory entries matching multipart uploadids, prefix and marker
		direntFilterFn := func(dirent fsDirent) bool {
			// check if dirent is a directory (or) dirent is a regular file and it's name ends with Upload ID suffix string
			if dirent.IsDir() || (dirent.IsRegular() && strings.HasSuffix(dirent.name, multipartUploadIDSuffix)) {
				// return if dirent's name starts with prefixPath and lexically higher than markerPath
				return strings.HasPrefix(dirent.name, prefixPath) && dirent.name > markerPath
			}
			return false
		}

		// filter function - filters directory entries matching multipart uploadids
		subDirentFilterFn := func(dirent fsDirent) bool {
			// check if dirent is a directory (or) dirent is a regular file and it's name ends with Upload ID suffix string
			return dirent.IsDir() || (dirent.IsRegular() && strings.HasSuffix(dirent.name, multipartUploadIDSuffix))
		}

		// lastObjInfo is used to save last object info which is sent at last with End=true
		var lastObjInfo *multipartObjectInfo

		sendError := func(err error) {
			if lastObjInfo != nil {
				if !send(*lastObjInfo) {
					// as we got error sending lastObjInfo, we can't send the error
					return
				}
			}

			send(multipartObjectInfo{Err: err, End: true})
			return
		}

		for {
			dirents, err := scandir(scanDir, direntFilterFn, false)
			if err != nil {
				sendError(err)
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
						sendError(errors.New("Corrupted metadata"))
						return
					}

					uploadID := strings.Split(filepath.Base(dirent.name), multipartUploadIDSuffix)[0]

					// Solaris and older unixes have modTime to be
					// empty, fallback to os.Stat() to fill missing values.
					if dirent.modTime.IsZero() {
						if fi, e := os.Stat(dirent.name); e == nil {
							dirent.modTime = fi.ModTime()
						} else {
							sendError(e)
							return
						}
					}

					objInfo := multipartObjectInfo{
						Name:         name,
						UploadID:     uploadID,
						ModifiedTime: dirent.modTime,
					}

					// as we got new object info, send last object info and keep new object info as last object info
					if lastObjInfo != nil {
						if !send(*lastObjInfo) {
							return
						}
					}
					lastObjInfo = &objInfo

					continue
				}

				// Fetch sub dirents.
				subDirents, err := scandir(dirent.name, subDirentFilterFn, false)
				if err != nil {
					sendError(err)
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
					// empty, fallback to os.Stat() to fill missing values.
					if dirent.modTime.IsZero() {
						if fi, e := os.Stat(dirent.name); e == nil {
							dirent.modTime = fi.ModTime()
						} else {
							sendError(e)
							return
						}
					}

					objInfo := multipartObjectInfo{
						Name:         strings.Replace(dirent.name, bucketDir, "", 1),
						ModifiedTime: dirent.modTime,
						IsDir:        true,
					}

					// as we got new object info, send last object info and keep new object info as last object info
					if lastObjInfo != nil {
						if !send(*lastObjInfo) {
							return
						}
					}
					lastObjInfo = &objInfo
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

		if lastObjInfo != nil {
			// we got last object
			lastObjInfo.End = true
			if !send(*lastObjInfo) {
				return
			}
		}
	}()

	return objectInfoCh
}

// multipartObjectInfo - Multipart object info
type multipartObjectInfo struct {
	Name         string
	UploadID     string
	ModifiedTime time.Time
	IsDir        bool
	Err          error
	End          bool
}
