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

package cmd

import (
	"path"
	"sort"
	"strings"
)

func listDirHealFactory(disks ...StorageAPI) listDirFunc {
	// Returns sorted merged entries from all the disks.
	listDir := func(bucket, prefixDir, prefixEntry string) (mergedentries []string, delayIsLeaf bool, err error) {
		for _, disk := range disks {
			var entries []string
			var newEntries []string
			entries, err = disk.ListDir(bucket, prefixDir)
			if err != nil {
				// Skip the disk of listDir returns error.
				continue
			}

			for i, entry := range entries {
				if strings.HasSuffix(entry, slashSeparator) {
					if _, err = disk.StatFile(bucket, path.Join(prefixDir, entry, xlMetaJSONFile)); err == nil {
						// If it is an object trim the trailing "/"
						entries[i] = strings.TrimSuffix(entry, slashSeparator)
					}
				}
			}

			if len(mergedentries) == 0 {
				// For the first successful disk.ListDir()
				mergedentries = entries
				sort.Strings(mergedentries)
				continue
			}

			// find elements in entries which are not in mergedentries
			for _, entry := range entries {
				idx := sort.SearchStrings(mergedentries, entry)
				if mergedentries[idx] == entry {
					continue
				}
				newEntries = append(newEntries, entry)
			}

			if len(newEntries) > 0 {
				// Merge the entries and sort it.
				mergedentries = append(mergedentries, newEntries...)
				sort.Strings(mergedentries)
			}
		}
		return mergedentries, false, nil
	}
	return listDir
}

// listObjectsHeal - wrapper function implemented over file tree walk.
func (xl xlObjects) listObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	// "heal" true for listObjectsHeal() and false for listObjects()
	heal := true
	walkResultCh, endWalkCh := xl.listPool.Release(listParams{bucket, recursive, marker, prefix, heal})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		listDir := listDirHealFactory(xl.storageDisks...)
		walkResultCh = startTreeWalk(bucket, prefix, marker, recursive, listDir, nil, endWalkCh)
	}

	var objInfos []ObjectInfo
	var eof bool
	var nextMarker string
	for i := 0; i < maxKeys; {
		walkResult, ok := <-walkResultCh
		if !ok {
			// Closed channel.
			eof = true
			break
		}
		// For any walk error return right away.
		if walkResult.err != nil {
			// File not found is a valid case.
			if walkResult.err == errFileNotFound {
				return ListObjectsInfo{}, nil
			}
			return ListObjectsInfo{}, toObjectErr(walkResult.err, bucket, prefix)
		}
		entry := walkResult.entry
		var objInfo ObjectInfo
		if strings.HasSuffix(entry, slashSeparator) {
			// Object name needs to be full path.
			objInfo.Bucket = bucket
			objInfo.Name = entry
			objInfo.IsDir = true
		} else {
			objInfo.Bucket = bucket
			objInfo.Name = entry
		}
		nextMarker = objInfo.Name
		objInfos = append(objInfos, objInfo)
		i++
		if walkResult.end == true {
			eof = true
			break
		}
	}

	params := listParams{bucket, recursive, nextMarker, prefix, heal}
	if !eof {
		xl.listPool.Set(params, walkResultCh, endWalkCh)
	}

	result := ListObjectsInfo{IsTruncated: !eof}
	for _, objInfo := range objInfos {
		result.NextMarker = objInfo.Name
		if objInfo.IsDir {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}

		// generates random string on setting MINIO_DEBUG=lock, else returns empty string.
		// used for instrumentation on locks.
		opsID := getOpsID()

		// Check if the current object needs healing
		nsMutex.RLock(bucket, objInfo.Name, opsID)
		partsMetadata, errs := readAllXLMetadata(xl.storageDisks, bucket, objInfo.Name)
		if xlShouldHeal(partsMetadata, errs) {
			result.Objects = append(result.Objects, ObjectInfo{
				Name:    objInfo.Name,
				ModTime: objInfo.ModTime,
				Size:    objInfo.Size,
				IsDir:   false,
			})
		}
		nsMutex.RUnlock(bucket, objInfo.Name, opsID)
	}
	return result, nil
}

// ListObjects - list all objects at prefix, delimited by '/'.
func (xl xlObjects) ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListObjectsInfo{}, traceError(BucketNameInvalid{Bucket: bucket})
	}
	// Verify if bucket exists.
	if !xl.isBucketExist(bucket) {
		return ListObjectsInfo{}, traceError(BucketNotFound{Bucket: bucket})
	}
	if !IsValidObjectPrefix(prefix) {
		return ListObjectsInfo{}, traceError(ObjectNameInvalid{Bucket: bucket, Object: prefix})
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return ListObjectsInfo{}, traceError(UnsupportedDelimiter{
			Delimiter: delimiter,
		})
	}
	// Verify if marker has prefix.
	if marker != "" {
		if !strings.HasPrefix(marker, prefix) {
			return ListObjectsInfo{}, traceError(InvalidMarkerPrefixCombination{
				Marker: marker,
				Prefix: prefix,
			})
		}
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return ListObjectsInfo{}, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter' along
	// with the prefix. On a flat namespace with 'prefix' as '/'
	// we don't have any entries, since all the keys are of form 'keyName/...'
	if delimiter == slashSeparator && prefix == slashSeparator {
		return ListObjectsInfo{}, nil
	}

	// Over flowing count - reset to maxObjectList.
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}

	// Initiate a list operation, if successful filter and return quickly.
	listObjInfo, err := xl.listObjectsHeal(bucket, prefix, marker, delimiter, maxKeys)
	if err == nil {
		// We got the entries successfully return.
		return listObjInfo, nil
	}

	// Return error at the end.
	return ListObjectsInfo{}, toObjectErr(err, bucket, prefix)
}
