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
	"sort"
	"strings"

	"github.com/docker/distribution/uuid"
)

func listDirHealFactory(isLeaf isLeafFunc, disks []StorageAPI) listDirFunc {
	// Returns sorted merged entries from all the disks.
	listDir := func(bucket, prefixDir, prefixEntry string) (mergedEntries []string, delayIsLeaf bool, err error) {
		for _, disk := range disks {
			if disk == nil {
				continue
			}
			var entries []string
			var newEntries []string
			entries, err = disk.ListDir(bucket, prefixDir)
			if err != nil {
				continue
			}
			// Listing needs to be sorted.
			sort.Strings(entries)

			// Filter entries that have the prefix prefixEntry.
			entries = filterMatchingPrefix(entries, prefixEntry)

			// isLeaf() check has to happen here so that trailing "/" for objects can be removed.
			for i, entry := range entries {
				if isLeaf(bucket, pathJoin(prefixDir, entry)) {
					entries[i] = strings.TrimSuffix(entry, slashSeparator)
				}
			}
			// Sort again after removing trailing "/" for objects as the previous sort
			// does not hold good anymore.
			sort.Strings(entries)
			if len(mergedEntries) == 0 {
				// For the first successful disk.ListDir()
				mergedEntries = entries
				sort.Strings(mergedEntries)
				continue
			}
			// find elements in entries which are not in mergedentries
			for _, entry := range entries {
				idx := sort.SearchStrings(mergedEntries, entry)
				if mergedEntries[idx] == entry {
					continue
				}
				newEntries = append(newEntries, entry)
			}
			if len(newEntries) > 0 {
				// Merge the entries and sort it.
				mergedEntries = append(mergedEntries, newEntries...)
				sort.Strings(mergedEntries)
			}
		}
		return mergedEntries, false, nil
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
		isLeaf := xl.isObject
		listDir := listDirHealFactory(isLeaf, xl.storageDisks)
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

		// Check if the current object needs healing
		objectLock := nsMutex.NewNSLock(bucket, objInfo.Name)
		objectLock.RLock()
		partsMetadata, errs := readAllXLMetadata(xl.storageDisks, bucket, objInfo.Name)
		if xlShouldHeal(partsMetadata, errs) {
			result.Objects = append(result.Objects, ObjectInfo{
				Name:    objInfo.Name,
				ModTime: objInfo.ModTime,
				Size:    objInfo.Size,
				IsDir:   false,
			})
		}
		objectLock.RUnlock()
	}
	return result, nil
}

func (xl xlObjects) listMultipartUploadsHeal(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	result := ListMultipartsInfo{
		IsTruncated: true,
		MaxUploads:  maxUploads,
		KeyMarker:   keyMarker,
		Prefix:      prefix,
		Delimiter:   delimiter,
	}

	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	// Not using path.Join() as it strips off the trailing '/'.
	multipartPrefixPath := pathJoin(mpartMetaPrefix, bucket, prefix)
	if prefix == "" {
		// Should have a trailing "/" if prefix is ""
		// For ex. multipartPrefixPath should be "multipart/bucket/" if prefix is ""
		multipartPrefixPath += slashSeparator
	}
	multipartMarkerPath := ""
	if keyMarker != "" {
		multipartMarkerPath = pathJoin(mpartMetaPrefix, bucket, keyMarker)
	}
	var uploads []uploadMetadata
	var err error
	var eof bool
	// List all upload ids for the keyMarker starting from
	// uploadIDMarker first.
	if uploadIDMarker != "" {
		// hold lock on keyMarker path
		keyMarkerLock := nsMutex.NewNSLock(minioMetaBucket,
			pathJoin(mpartMetaPrefix, bucket, keyMarker))
		keyMarkerLock.RLock()
		for _, disk := range xl.getLoadBalancedDisks() {
			if disk == nil {
				continue
			}
			uploads, _, err = listMultipartUploadIDs(bucket, keyMarker, uploadIDMarker, maxUploads, disk)
			if err == nil {
				break
			}
			if isErrIgnored(err, objMetadataOpIgnoredErrs) {
				continue
			}
			break
		}
		keyMarkerLock.RUnlock()
		if err != nil {
			return ListMultipartsInfo{}, err
		}
		maxUploads = maxUploads - len(uploads)
	}
	var walkerCh chan treeWalkResult
	var walkerDoneCh chan struct{}
	heal := false // true only for xl.ListObjectsHeal
	// Validate if we need to list further depending on maxUploads.
	if maxUploads > 0 {
		walkerCh, walkerDoneCh = xl.listPool.Release(listParams{minioMetaBucket, recursive, multipartMarkerPath, multipartPrefixPath, heal})
		if walkerCh == nil {
			walkerDoneCh = make(chan struct{})
			isLeaf := xl.isMultipartUpload
			listDir := listDirHealFactory(isLeaf, xl.getLoadBalancedDisks())
			walkerCh = startTreeWalk(minioMetaBucket, multipartPrefixPath, multipartMarkerPath, recursive, listDir, isLeaf, walkerDoneCh)
		}
		// Collect uploads until we have reached maxUploads count to 0.
		for maxUploads > 0 {
			walkResult, ok := <-walkerCh
			if !ok {
				// Closed channel.
				eof = true
				break
			}
			// For any walk error return right away.
			if walkResult.err != nil {
				// File not found or Disk not found is a valid case.
				if isErrIgnored(walkResult.err, xlTreeWalkIgnoredErrs) {
					continue
				}
				return ListMultipartsInfo{}, err
			}
			entry := strings.TrimPrefix(walkResult.entry, retainSlash(pathJoin(mpartMetaPrefix, bucket)))
			// For an entry looking like a directory, store and
			// continue the loop not need to fetch uploads.
			if strings.HasSuffix(walkResult.entry, slashSeparator) {
				uploads = append(uploads, uploadMetadata{
					Object: entry,
				})
				maxUploads--
				if maxUploads == 0 {
					eof = true
					break
				}
				continue
			}
			var newUploads []uploadMetadata
			var end bool
			uploadIDMarker = ""

			// For the new object entry we get all its
			// pending uploadIDs.
			entryLock := nsMutex.NewNSLock(minioMetaBucket,
				pathJoin(mpartMetaPrefix, bucket, entry))
			entryLock.RLock()
			var disk StorageAPI
			for _, disk = range xl.getLoadBalancedDisks() {
				if disk == nil {
					continue
				}
				newUploads, end, err = listMultipartUploadIDs(bucket, entry, uploadIDMarker, maxUploads, disk)
				if err == nil {
					break
				}
				if isErrIgnored(err, objMetadataOpIgnoredErrs) {
					continue
				}
				break
			}
			entryLock.RUnlock()
			if err != nil {
				if isErrIgnored(err, xlTreeWalkIgnoredErrs) {
					continue
				}
				return ListMultipartsInfo{}, err
			}
			uploads = append(uploads, newUploads...)
			maxUploads -= len(newUploads)
			if end && walkResult.end {
				eof = true
				break
			}
		}
	}
	// For all received uploads fill in the multiparts result.
	for _, upload := range uploads {
		var objectName string
		var uploadID string
		if strings.HasSuffix(upload.Object, slashSeparator) {
			// All directory entries are common prefixes.
			uploadID = "" // For common prefixes, upload ids are empty.
			objectName = upload.Object
			result.CommonPrefixes = append(result.CommonPrefixes, objectName)
		} else {
			uploadID = upload.UploadID
			objectName = upload.Object
			result.Uploads = append(result.Uploads, upload)
		}
		result.NextKeyMarker = objectName
		result.NextUploadIDMarker = uploadID
	}

	if !eof {
		// Save the go-routine state in the pool so that it can continue from where it left off on
		// the next request.
		xl.listPool.Set(listParams{bucket, recursive, result.NextKeyMarker, prefix, heal}, walkerCh, walkerDoneCh)
	}

	result.IsTruncated = !eof
	// Result is not truncated, reset the markers.
	if !result.IsTruncated {
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
	}
	return result, nil
}

func (xl xlObjects) ListMultipartUploadsHeal(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	result := ListMultipartsInfo{}

	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListMultipartsInfo{}, traceError(BucketNameInvalid{Bucket: bucket})
	}
	if !xl.isBucketExist(bucket) {
		return ListMultipartsInfo{}, traceError(BucketNotFound{Bucket: bucket})
	}
	if !IsValidObjectPrefix(prefix) {
		return ListMultipartsInfo{}, traceError(ObjectNameInvalid{Bucket: bucket, Object: prefix})
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return ListMultipartsInfo{}, traceError(UnsupportedDelimiter{
			Delimiter: delimiter,
		})
	}
	// Verify if marker has prefix.
	if keyMarker != "" && !strings.HasPrefix(keyMarker, prefix) {
		return ListMultipartsInfo{}, traceError(InvalidMarkerPrefixCombination{
			Marker: keyMarker,
			Prefix: prefix,
		})
	}
	if uploadIDMarker != "" {
		if strings.HasSuffix(keyMarker, slashSeparator) {
			return result, traceError(InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			})
		}
		id, err := uuid.Parse(uploadIDMarker)
		if err != nil {
			return result, traceError(err)
		}
		if id.IsZero() {
			return result, traceError(MalformedUploadID{
				UploadID: uploadIDMarker,
			})
		}
	}
	return xl.listMultipartUploadsHeal(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
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
