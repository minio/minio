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

package fs

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/minio/minio/pkg/probe"
)

// ListObjects - lists all objects for a given prefix, returns up to
// maxKeys number of objects per call.
func (fs Filesystem) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsResult, *probe.Error) {
	result := ListObjectsResult{}
	var queryPrefix string

	// Input validation.
	if !IsValidBucketName(bucket) {
		return result, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	bucket = fs.denormalizeBucket(bucket)
	bucketDir := filepath.Join(fs.path, bucket)
	// Verify if bucket exists.
	if status, err := isDirExist(bucketDir); !status {
		if err == nil {
			// File exists, but its not a directory.
			return result, probe.NewError(BucketNotFound{Bucket: bucket})
		} else if os.IsNotExist(err) {
			// File does not exist.
			return result, probe.NewError(BucketNotFound{Bucket: bucket})
		} else {
			return result, probe.NewError(err)
		}
	}
	if !IsValidObjectPrefix(prefix) {
		return result, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: prefix})
	}

	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != "/" {
		return result, probe.NewError(fmt.Errorf("delimiter '%s' is not supported", delimiter))
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
	_, err := isDirExist(rootDir)
	if err != nil {
		if os.IsNotExist(err) {
			// Prefix does not exist, not an error just respond empty
			// list response.
			return result, nil
		}
		// Rest errors should be treated as failure.
		return result, probe.NewError(err)
	}

	recursive := true
	skipDir := true
	if delimiter == "/" {
		skipDir = false
		recursive = false
	}

	// Maximum 1000 objects returned in a single to listObjects.
	// Further calls will set right marker value to continue reading the rest of the objectList.
	// popListObjectCh returns nil if the call to ListObject is done for the first time.
	// On further calls to ListObjects to retrive more objects within the timeout period,
	// popListObjectCh returns the channel from which rest of the objects can be retrieved.
	objectInfoCh := fs.popListObjectCh(ListObjectParams{bucket, delimiter, marker, prefix})
	if objectInfoCh == nil {
		if prefix != "" {
			// queryPrefix variable is set to value of the prefix to be searched.
			// If prefix contains directory hierarchy queryPrefix is set to empty string,
			// this ensure that all objects inside the prefixDir is listed.
			// Otherwise the base name is extracted from path.Base and it'll be will be set to Querystring.
			// if prefix = /Asia/India/, queryPrefix will be set to empty string(""), so that all objects in prefixDir are listed.
			// if prefix = /Asia/India/summerpics , Querystring will be set to "summerpics",
			// so those all objects with the prefix "summerpics" inside the /Asia/India/ prefix folder gets listed.
			if prefix[len(prefix)-1:] == "/" {
				queryPrefix = ""
			} else {
				queryPrefix = path.Base(prefix)
			}
		}
		ch := treeWalk(rootDir, bucketDir, recursive, queryPrefix)
		objectInfoCh = &ch
	}

	nextMarker := ""
	for i := 0; i < maxKeys; {

		objInfo, ok := objectInfoCh.Read()
		if !ok {
			// Closed channel.
			return result, nil
		}

		if objInfo.Err != nil {
			return ListObjectsResult{}, probe.NewError(objInfo.Err)
		}

		if strings.Contains(objInfo.Name, "$multiparts") || strings.Contains(objInfo.Name, "$tmpobject") {
			continue
		}

		if objInfo.IsDir && skipDir {
			continue
		}

		// Add the bucket.
		objInfo.Bucket = bucket
		// In case its not the first call to ListObjects (before timeout),
		// The result is already inside the buffered channel.
		if objInfo.Name > marker {
			if objInfo.IsDir {
				result.Prefixes = append(result.Prefixes, objInfo.Name)
			} else {
				result.Objects = append(result.Objects, objInfo)
			}
			nextMarker = objInfo.Name
			i++
		}

	}

	if !objectInfoCh.IsClosed() {
		result.IsTruncated = true
		result.NextMarker = nextMarker
		fs.pushListObjectCh(ListObjectParams{bucket, delimiter, nextMarker, prefix}, *objectInfoCh)
	}

	return result, nil
}
