/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"path/filepath"
	"sort"
	"strings"
)

func listDirHealFactory(isLeaf isLeafFunc, disks ...StorageAPI) listDirFunc {
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

			// Filter entries that have the prefix prefixEntry.
			entries = filterMatchingPrefix(entries, prefixEntry)

			// isLeaf() check has to happen here so that
			// trailing "/" for objects can be removed.
			for i, entry := range entries {
				if isLeaf(bucket, pathJoin(prefixDir, entry)) {
					entries[i] = strings.TrimSuffix(entry, slashSeparator)
				}
			}

			// Find elements in entries which are not in mergedEntries
			for _, entry := range entries {
				idx := sort.SearchStrings(mergedEntries, entry)
				// if entry is already present in mergedEntries don't add.
				if idx < len(mergedEntries) && mergedEntries[idx] == entry {
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
func (xl xlObjects) listObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
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
		listDir := listDirHealFactory(isLeaf, xl.storageDisks...)
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
			return loi, toObjectErr(walkResult.err, bucket, prefix)
		}
		entry := walkResult.entry
		var objInfo ObjectInfo
		if hasSuffix(entry, slashSeparator) {
			// Object name needs to be full path.
			objInfo.Bucket = bucket
			objInfo.Name = entry
			objInfo.IsDir = true
		} else {
			var err error
			objInfo, err = xl.getObjectInfo(bucket, entry)
			if err != nil {
				// Ignore errFileNotFound
				if errorCause(err) == errFileNotFound {
					continue
				}
				return loi, toObjectErr(err, bucket, prefix)
			}
		}
		nextMarker = objInfo.Name
		objInfos = append(objInfos, objInfo)
		i++
		if walkResult.end {
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
		objectLock := globalNSMutex.NewNSLock(bucket, objInfo.Name)
		if err := objectLock.GetRLock(globalHealingTimeout); err != nil {
			return loi, err
		}
		partsMetadata, errs := readAllXLMetadata(xl.storageDisks, bucket, objInfo.Name)
		if xlShouldHeal(xl.storageDisks, partsMetadata, errs, bucket, objInfo.Name) {
			healStat := xlHealStat(xl, partsMetadata, errs)
			result.Objects = append(result.Objects, ObjectInfo{
				Name:           objInfo.Name,
				ModTime:        objInfo.ModTime,
				Size:           objInfo.Size,
				IsDir:          false,
				HealObjectInfo: &healStat,
			})
		}
		objectLock.RUnlock()
	}
	return result, nil
}

// ListObjects - list all objects at prefix, delimited by '/'.
func (xl xlObjects) ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	if err := checkListObjsArgs(bucket, prefix, marker, delimiter, xl); err != nil {
		return loi, err
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return loi, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter' along
	// with the prefix. On a flat namespace with 'prefix' as '/'
	// we don't have any entries, since all the keys are of form 'keyName/...'
	if delimiter == slashSeparator && prefix == slashSeparator {
		return loi, nil
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
	return loi, toObjectErr(err, bucket, prefix)
}

// ListUploadsHeal - lists ongoing multipart uploads that require
// healing in one or more disks.
func (xl xlObjects) ListUploadsHeal(bucket, prefix, marker, uploadIDMarker,
	delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter' along
	// with the prefix. On a flat namespace with 'prefix' as '/'
	// we don't have any entries, since all the keys are of form 'keyName/...'
	if delimiter == slashSeparator && prefix == slashSeparator {
		return lmi, nil
	}

	// Initiate a list operation.
	listMultipartInfo, err := xl.listMultipartUploadsHeal(bucket, prefix,
		marker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		return lmi, toObjectErr(err, bucket, prefix)
	}

	// We got the entries successfully return.
	return listMultipartInfo, nil
}

// Fetches list of multipart uploadIDs given bucket, keyMarker, uploadIDMarker.
func fetchMultipartUploadIDs(bucket, keyMarker, uploadIDMarker string,
	maxUploads int, disks []StorageAPI) (uploads []uploadMetadata, end bool,
	err error) {

	// Hold a read lock on keyMarker path.
	keyMarkerLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket,
		pathJoin(bucket, keyMarker))
	if err = keyMarkerLock.GetRLock(globalHealingTimeout); err != nil {
		return uploads, end, err
	}
	for _, disk := range disks {
		if disk == nil {
			continue
		}
		uploads, end, err = listMultipartUploadIDs(bucket, keyMarker,
			uploadIDMarker, maxUploads, disk)
		if err == nil ||
			!isErrIgnored(err, objMetadataOpIgnoredErrs...) {
			break
		}
	}
	keyMarkerLock.RUnlock()
	return uploads, end, err
}

// listMultipartUploadsHeal - Returns a list of incomplete multipart
// uploads that need to be healed.
func (xl xlObjects) listMultipartUploadsHeal(bucket, prefix, keyMarker,
	uploadIDMarker, delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {

	result := ListMultipartsInfo{
		IsTruncated: true,
		MaxUploads:  maxUploads,
		KeyMarker:   keyMarker,
		Prefix:      prefix,
		Delimiter:   delimiter,
	}

	recursive := delimiter != slashSeparator

	var uploads []uploadMetadata
	var err error
	// List all upload ids for the given keyMarker, starting from
	// uploadIDMarker.
	if uploadIDMarker != "" {
		uploads, _, err = fetchMultipartUploadIDs(bucket, keyMarker,
			uploadIDMarker, maxUploads, xl.getLoadBalancedDisks())
		if err != nil {
			return lmi, err
		}
		maxUploads = maxUploads - len(uploads)
	}

	// We can't use path.Join() as it strips off the trailing '/'.
	multipartPrefixPath := pathJoin(bucket, prefix)
	// multipartPrefixPath should have a trailing '/' when prefix = "".
	if prefix == "" {
		multipartPrefixPath += slashSeparator
	}

	multipartMarkerPath := ""
	if keyMarker != "" {
		multipartMarkerPath = pathJoin(bucket, keyMarker)
	}

	// `heal bool` is used to differentiate listing of incomplete
	// uploads (and parts) from a regular listing of incomplete
	// parts by client SDKs or mc-like commands, within a treewalk
	// pool.
	heal := true
	// The listing is truncated if we have maxUploads entries and
	// there are more entries to be listed.
	truncated := true
	var walkerCh chan treeWalkResult
	var walkerDoneCh chan struct{}
	// Check if we have room left to send more uploads.
	if maxUploads > 0 {
		uploadsLeft := maxUploads

		walkerCh, walkerDoneCh = xl.listPool.Release(listParams{
			bucket:    minioMetaMultipartBucket,
			recursive: recursive,
			marker:    multipartMarkerPath,
			prefix:    multipartPrefixPath,
			heal:      heal,
		})
		if walkerCh == nil {
			walkerDoneCh = make(chan struct{})
			isLeaf := xl.isMultipartUpload
			listDir := listDirFactory(isLeaf, xlTreeWalkIgnoredErrs,
				xl.getLoadBalancedDisks()...)
			walkerCh = startTreeWalk(minioMetaMultipartBucket,
				multipartPrefixPath, multipartMarkerPath,
				recursive, listDir, isLeaf, walkerDoneCh)
		}
		// Collect uploads until leftUploads limit is reached.
		for {
			walkResult, ok := <-walkerCh
			if !ok {
				truncated = false
				break
			}
			// For any error during tree walk, we should return right away.
			if walkResult.err != nil {
				return lmi, walkResult.err
			}

			entry := strings.TrimPrefix(walkResult.entry,
				retainSlash(bucket))
			// Skip entries that are not object directory.
			if hasSuffix(walkResult.entry, slashSeparator) {
				uploads = append(uploads, uploadMetadata{
					Object: entry,
				})
				uploadsLeft--
				if uploadsLeft == 0 {
					break
				}
				continue
			}

			// For an object entry we get all its pending
			// uploadIDs.
			var newUploads []uploadMetadata
			var end bool
			uploadIDMarker = ""
			newUploads, end, err = fetchMultipartUploadIDs(bucket, entry, uploadIDMarker,
				uploadsLeft, xl.getLoadBalancedDisks())
			if err != nil {
				return lmi, err
			}
			uploads = append(uploads, newUploads...)
			uploadsLeft -= len(newUploads)
			if end && walkResult.end {
				truncated = false
				break
			}
			if uploadsLeft == 0 {
				break
			}
		}

	}

	// For all received uploads fill in the multiparts result.
	for _, upload := range uploads {
		var objectName string
		var uploadID string
		if hasSuffix(upload.Object, slashSeparator) {
			// All directory entries are common
			// prefixes. For common prefixes, upload ids
			// are empty.
			uploadID = ""
			objectName = upload.Object
			result.CommonPrefixes = append(result.CommonPrefixes, objectName)
		} else {
			// Check if upload needs healing.
			uploadIDPath := filepath.Join(bucket, upload.Object, upload.UploadID)
			partsMetadata, errs := readAllXLMetadata(xl.storageDisks,
				minioMetaMultipartBucket, uploadIDPath)
			if xlShouldHeal(xl.storageDisks, partsMetadata, errs,
				minioMetaMultipartBucket, uploadIDPath) {

				healUploadInfo := xlHealStat(xl, partsMetadata, errs)
				upload.HealUploadInfo = &healUploadInfo
				result.Uploads = append(result.Uploads, upload)
			}
			uploadID = upload.UploadID
			objectName = upload.Object
		}

		result.NextKeyMarker = objectName
		result.NextUploadIDMarker = uploadID
	}

	if truncated {
		// Put back the tree walk go-routine into the pool for
		// subsequent use.
		xl.listPool.Set(listParams{
			bucket:    bucket,
			recursive: recursive,
			marker:    result.NextKeyMarker,
			prefix:    prefix,
			heal:      heal,
		}, walkerCh, walkerDoneCh)
	}

	result.IsTruncated = truncated
	// Result is not truncated, reset the markers.
	if !result.IsTruncated {
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
	}
	return result, nil
}
