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
	"path/filepath"
	"strings"

	"github.com/minio/minio/pkg/probe"
)

// ListObjects - lists all objects for a given prefix, returns up to
// maxKeys number of objects per call.
func (fs Filesystem) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsResult, *probe.Error) {
	result := ListObjectsResult{}

	// Input validation.
	if !IsValidBucketName(bucket) {
		return result, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	bucket = fs.denormalizeBucket(bucket)

	if status, err := IsDirExist(filepath.Join(fs.path, bucket)); !status {
		if err == nil {
			return result, probe.NewError(BucketNotFound{Bucket: bucket})
		} else if os.IsNotExist(err) {
			return result, probe.NewError(BucketNotFound{Bucket: bucket})
		} else {
			return result, probe.NewError(err)
		}
	}

	if delimiter != "" && delimiter != "/" {
		return result, probe.NewError(fmt.Errorf("delimiter '%s' is not supported", delimiter))
	}

	if marker != "" {
		if markerUnescaped, err := url.QueryUnescape(marker); err == nil {
			marker = markerUnescaped
		} else {
			return result, probe.NewError(err)
		}

		if !strings.HasPrefix(marker, prefix) {
			return result, probe.NewError(fmt.Errorf("marker '%s' and prefix '%s' do not match", marker, prefix))
		}
	}

	if maxKeys <= 0 || maxKeys > listObjectsLimit {
		maxKeys = listObjectsLimit
	}

	bucketDir := filepath.Join(fs.path, bucket)

	recursive := true
	skipDir := true
	if delimiter == "/" {
		skipDir = false
		recursive = false
	}

	prefixDir := filepath.Dir(filepath.FromSlash(prefix))
	rootDir := filepath.Join(bucketDir, prefixDir)

	objectInfoCh := fs.popListObjectCh(ListObjectParams{bucket, delimiter, marker, prefix})
	if objectInfoCh == nil {
		ch := treeWalk(rootDir, bucketDir, recursive)
		objectInfoCh = &ch
	}

	nextMarker := ""
	for i := 0; i < maxKeys; {
		objInfo, ok := objectInfoCh.Read()
		if !ok {
			// closed channel
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

		if strings.HasPrefix(objInfo.Name, prefix) {
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
	}

	if !objectInfoCh.IsClosed() {
		result.IsTruncated = true
		result.NextMarker = nextMarker
		fs.pushListObjectCh(ListObjectParams{bucket, delimiter, nextMarker, prefix}, *objectInfoCh)
	}

	return result, nil
}
