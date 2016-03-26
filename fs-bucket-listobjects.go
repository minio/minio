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
	"path/filepath"
	"strings"

	"github.com/minio/minio/pkg/probe"
)

const (
	// listObjectsLimit - maximum list objects limit.
	listObjectsLimit = 1000
)

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
func (fs Filesystem) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, *probe.Error) {
	result := ListObjectsInfo{}

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
		return result, probe.NewError(fmt.Errorf("delimiter '%s' is not supported. Only '/' is supported", delimiter))
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
	// popTreeWalker returns nil if the call to ListObject is done for the first time.
	// On further calls to ListObjects to retrive more objects within the timeout period,
	// popTreeWalker returns the channel from which rest of the objects can be retrieved.
	walker := fs.popTreeWalker(listObjectParams{bucket, delimiter, marker, prefix})
	if walker == nil {
		walker = startTreeWalker(fs.path, bucket, filepath.FromSlash(prefix), filepath.FromSlash(marker), recursive)
	}

	nextMarker := ""
	for i := 0; i < maxKeys; {
		walkResult, ok := <-walker.ch
		if !ok {
			// Closed channel.
			return result, nil
		}
		if walkResult.err != nil {
			return ListObjectsInfo{}, probe.NewError(walkResult.err)
		}
		objInfo := walkResult.objectInfo
		objInfo.Name = filepath.ToSlash(objInfo.Name)
		if strings.Contains(objInfo.Name, "$multiparts") || strings.Contains(objInfo.Name, "$tmpobject") {
			continue
		}

		if objInfo.IsDir {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
		} else {
			result.Objects = append(result.Objects, objInfo)
		}

		if walkResult.end {
			return result, nil
		}
		nextMarker = objInfo.Name
		i++
	}
	result.IsTruncated = true
	result.NextMarker = nextMarker
	fs.pushTreeWalker(listObjectParams{bucket, delimiter, nextMarker, prefix}, walker)
	return result, nil
}
