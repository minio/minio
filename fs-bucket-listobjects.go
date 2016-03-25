/*
 * Minio Cloud Storage, (C) 2015-2016 Minio, Inc.
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
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio/pkg/probe"
)

const (
	// listObjectsLimit - maximum list objects limit.
	listObjectsLimit = 1000
)

// ObjectInfo - object info.
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

// ObjectInfoChannel - object info channel.
type ObjectInfoChannel struct {
	ch       <-chan ObjectInfo
	timedOut bool
}

func treeWalk(bucketDir, prefixDir, entryPrefixMatch, marker string, recursive bool, send func(ObjectInfo) bool) bool {
	markerPart := ""
	markerRest := ""

	if marker != "" {
		markerSplit := strings.SplitN(marker, string(os.PathSeparator), 2)
		markerPart = markerSplit[0]
		if len(markerSplit) == 2 {
			markerRest = markerSplit[1]
		}
	}

	dirents, err := readDirAll(path.Join(bucketDir, prefixDir), entryPrefixMatch)
	if err != nil {
		send(ObjectInfo{Err: err})
		return false
	}

	dirents = dirents[searchDirents(dirents, markerPart):]
	for i, dirent := range dirents {
		if i == 0 && markerPart == dirent.Name && !dirent.IsDir {
			continue
		}
		if dirent.IsDir && recursive {
			markerArg := ""
			if dirent.Name == markerPart {
				markerArg = markerRest
			}
			if !treeWalk(bucketDir, path.Join(prefixDir, dirent.Name), "", markerArg, recursive, send) {
				return false
			}
			continue
		}
		objectInfo := ObjectInfo{}
		objectInfo.Name = path.Join(prefixDir, dirent.Name)
		if dirent.ModifiedTime.IsZero() && dirent.Size == 0 {
			// ModifiedTime and Size are zero, Stat() and figure out
			// the actual values that need to be set.
			fi, err := os.Stat(path.Join(bucketDir, prefixDir, dirent.Name))
			if err != nil {
				send(ObjectInfo{Err: err})
				return false
			}
			objectInfo.ModifiedTime = fi.ModTime()
			objectInfo.Size = fi.Size()
			objectInfo.IsDir = fi.IsDir()
			if fi.IsDir() {
				objectInfo.Size = 0
				objectInfo.Name += "/"
			}
		} else {
			// If ModifiedTime or Size are set then we set them back
			// without attempting another Stat operation.
			objectInfo.ModifiedTime = dirent.ModifiedTime
			objectInfo.Size = dirent.Size
			objectInfo.IsDir = dirent.IsDir
		}
		if !send(objectInfo) {
			return false
		}
	}
	return true
}

func getObjectInfoChannel(fsPath, bucket, prefix, marker string, recursive bool) *ObjectInfoChannel {
	objectInfoCh := make(chan ObjectInfo, listObjectsLimit)
	objectInfoChannel := ObjectInfoChannel{ch: objectInfoCh}
	entryPrefixMatch := prefix
	prefixDir := ""
	lastIndex := strings.LastIndex(prefix, string(os.PathSeparator))
	if lastIndex != -1 {
		entryPrefixMatch = prefix[lastIndex+1:]
		prefixDir = prefix[:lastIndex]
	}

	go func() {
		defer close(objectInfoCh)
		send := func(oi ObjectInfo) bool {
			// Add the bucket.
			oi.Bucket = bucket
			timer := time.After(time.Second * 15)
			select {
			case objectInfoCh <- oi:
				return true
			case <-timer:
				objectInfoChannel.timedOut = true
				return false
			}
		}
		treeWalk(path.Join(fsPath, bucket), prefixDir, entryPrefixMatch, marker, recursive, send)
	}()
	return &objectInfoChannel
}

// isDirExist - returns whether given directory is exist or not.
func isDirExist(dirname string) (status bool, err error) {
	fi, err := os.Lstat(dirname)
	if err == nil {
		status = fi.IsDir()
	}
	return
}

// ListObjects - lists all objects for a given prefix, returns up to
// maxKeys number of objects per call.
func (fs Filesystem) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsResult, *probe.Error) {
	result := ListObjectsResult{}

	// Input validation.
	if !IsValidBucketName(bucket) {
		return result, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	bucket = getActualBucketname(fs.path, bucket) // Get the right bucket name.
	bucketDir := filepath.Join(fs.path, bucket)
	// Verify if bucket exists.
	if status, e := isDirExist(bucketDir); !status {
		if e == nil {
			// File exists, but its not a directory.
			return result, probe.NewError(BucketNotFound{Bucket: bucket})
		} else if os.IsNotExist(e) {
			// File does not exist.
			return result, probe.NewError(BucketNotFound{Bucket: bucket})
		} else {
			return result, probe.NewError(e)
		}
	}
	if !IsValidObjectPrefix(prefix) {
		return result, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: prefix})
	}

	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != "/" {
		return result, probe.NewError(fmt.Errorf("delimiter '%s' is not supported. Only '/' is supported.", delimiter))
	}

	// Marker is set unescape.
	if marker != "" {
		if markerUnescaped, err := url.QueryUnescape(marker); err == nil {
			marker = markerUnescaped
		} else {
			return result, probe.NewError(err)
		}

		if !strings.HasPrefix(marker, prefix) {
			return result, probe.NewError(fmt.Errorf("Invalid combination of marker '%s' and prefix '%s'", marker, prefix))
		}
	}

	// Return empty response for a valid request when maxKeys is 0.
	if maxKeys == 0 {
		return result, nil
	}

	// Over flowing maxkeys - reset to listObjectsLimit.
	if maxKeys < 0 || maxKeys > listObjectsLimit {
		maxKeys = listObjectsLimit
	}

	// Verify if prefix exists.
	prefixDir := filepath.Dir(filepath.FromSlash(prefix))
	rootDir := filepath.Join(bucketDir, prefixDir)
	_, e := isDirExist(rootDir)
	if e != nil {
		if os.IsNotExist(e) {
			// Prefix does not exist, not an error just respond empty
			// list response.
			return result, nil
		}
		// Rest errors should be treated as failure.
		return result, probe.NewError(e)
	}

	recursive := true
	if delimiter == "/" {
		recursive = false
	}

	// Maximum 1000 objects returned in a single to listObjects.
	// Further calls will set right marker value to continue reading the rest of the objectList.
	// popListObjectCh returns nil if the call to ListObject is done for the first time.
	// On further calls to ListObjects to retrive more objects within the timeout period,
	// popListObjectCh returns the channel from which rest of the objects can be retrieved.
	objectInfoCh := fs.popListObjectCh(listObjectParams{bucket, delimiter, marker, prefix})
	if objectInfoCh == nil {
		objectInfoCh = getObjectInfoChannel(fs.path, bucket, filepath.FromSlash(prefix), filepath.FromSlash(marker), recursive)
	}

	nextMarker := ""
	for i := 0; i < maxKeys; {
		objInfo, ok := <-objectInfoCh.ch
		if !ok {
			// Closed channel.
			return result, nil
		}
		if objInfo.Err != nil {
			return ListObjectsInfo{}, probe.NewError(objInfo.Err)
		}

		if strings.Contains(objInfo.Name, "$multiparts") || strings.Contains(objInfo.Name, "$tmpobject") {
			continue
		}

		if objInfo.IsDir {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
		} else {
			result.Objects = append(result.Objects, objInfo)
		}
		nextMarker = objInfo.Name
		i++
	}
	result.IsTruncated = true
	result.NextMarker = nextMarker
	fs.pushListObjectCh(ListObjectParams{bucket, delimiter, nextMarker, prefix}, *objectInfoCh)
	return result, nil
}
