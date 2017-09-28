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
	"encoding/hex"
	"fmt"
	"io"
	"os"
	pathutil "path"
	"strings"
	"time"

	"github.com/minio/minio/pkg/lock"
)

const (
	// Expiry duration after which the multipart uploads are deemed stale.
	fsMultipartExpiry = time.Hour * 24 * 14 // 2 weeks.
	// Cleanup interval when the stale multipart cleanup is initiated.
	fsMultipartCleanupInterval = time.Hour * 24 // 24 hrs.
)

// Returns if the prefix is a multipart upload.
func (fs fsObjects) isMultipartUpload(bucket, prefix string) bool {
	uploadsIDPath := pathJoin(fs.fsPath, bucket, prefix, uploadsJSONFile)
	_, err := fsStatFile(uploadsIDPath)
	if err != nil {
		if errorCause(err) == errFileNotFound {
			return false
		}
		errorIf(err, "Unable to access uploads.json "+uploadsIDPath)
		return false
	}
	return true
}

// Delete uploads.json file wrapper
func (fs fsObjects) deleteUploadsJSON(bucket, object, uploadID string) error {
	multipartBucketPath := pathJoin(fs.fsPath, minioMetaMultipartBucket)
	uploadPath := pathJoin(multipartBucketPath, bucket, object)
	uploadsMetaPath := pathJoin(uploadPath, uploadsJSONFile)

	tmpDir := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID)

	return fsRemoveMeta(multipartBucketPath, uploadsMetaPath, tmpDir)
}

// Removes the uploadID, called either by CompleteMultipart of AbortMultipart. If the resuling uploads
// slice is empty then we remove/purge the file.
func (fs fsObjects) removeUploadID(bucket, object, uploadID string, rwlk *lock.LockedFile) (bool, error) {
	uploadIDs := uploadsV1{}
	_, err := uploadIDs.ReadFrom(rwlk)
	if err != nil {
		return false, err
	}

	// Removes upload id from the uploads list.
	uploadIDs.RemoveUploadID(uploadID)

	// Check this is the last entry.
	if uploadIDs.IsEmpty() {
		// No more uploads left, so we delete `uploads.json` file.
		return true, fs.deleteUploadsJSON(bucket, object, uploadID)
	} // else not empty

	// Write update `uploads.json`.
	_, err = uploadIDs.WriteTo(rwlk)
	return false, err
}

// Adds a new uploadID if no previous `uploads.json` is
// found we initialize a new one.
func (fs fsObjects) addUploadID(bucket, object, uploadID string, initiated time.Time, rwlk *lock.LockedFile) error {
	uploadIDs := uploadsV1{}

	_, err := uploadIDs.ReadFrom(rwlk)
	// For all unexpected errors, we return.
	if err != nil && errorCause(err) != io.EOF {
		return err
	}

	// If we couldn't read anything, we assume a default
	// (empty) upload info.
	if errorCause(err) == io.EOF {
		uploadIDs = newUploadsV1("fs")
	}

	// Adds new upload id to the list.
	uploadIDs.AddUploadID(uploadID, initiated)

	// Write update `uploads.json`.
	_, err = uploadIDs.WriteTo(rwlk)
	return err
}

// listMultipartUploadIDs - list all the upload ids from a marker up to 'count'.
func (fs fsObjects) listMultipartUploadIDs(bucketName, objectName, uploadIDMarker string, count int) ([]uploadMetadata, bool, error) {
	var uploads []uploadMetadata

	// Hold the lock so that two parallel complete-multipart-uploads
	// do not leave a stale uploads.json behind.
	objectMPartPathLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket, pathJoin(bucketName, objectName))
	if err := objectMPartPathLock.GetRLock(globalListingTimeout); err != nil {
		return nil, false, traceError(err)
	}
	defer objectMPartPathLock.RUnlock()

	uploadsPath := pathJoin(bucketName, objectName, uploadsJSONFile)
	rlk, err := fs.rwPool.Open(pathJoin(fs.fsPath, minioMetaMultipartBucket, uploadsPath))
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return nil, true, nil
		}
		return nil, false, traceError(err)
	}
	defer fs.rwPool.Close(pathJoin(fs.fsPath, minioMetaMultipartBucket, uploadsPath))

	// Read `uploads.json`.
	uploadIDs := uploadsV1{}
	if _, err = uploadIDs.ReadFrom(rlk.LockedFile); err != nil {
		return nil, false, err
	}

	index := 0
	if uploadIDMarker != "" {
		for ; index < len(uploadIDs.Uploads); index++ {
			if uploadIDs.Uploads[index].UploadID == uploadIDMarker {
				// Skip the uploadID as it would already be listed in previous listing.
				index++
				break
			}
		}
	}

	for index < len(uploadIDs.Uploads) {
		uploads = append(uploads, uploadMetadata{
			Object:    objectName,
			UploadID:  uploadIDs.Uploads[index].UploadID,
			Initiated: uploadIDs.Uploads[index].Initiated,
		})
		count--
		index++
		if count == 0 {
			break
		}
	}

	end := (index == len(uploadIDs.Uploads))
	return uploads, end, nil
}

// listMultipartUploads - lists all multipart uploads.
func (fs fsObjects) listMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
	result := ListMultipartsInfo{}
	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	result.IsTruncated = true
	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = prefix
	result.Delimiter = delimiter

	// Not using path.Join() as it strips off the trailing '/'.
	multipartPrefixPath := pathJoin(bucket, prefix)
	if prefix == "" {
		// Should have a trailing "/" if prefix is ""
		// For ex. multipartPrefixPath should be "multipart/bucket/" if prefix is ""
		multipartPrefixPath += slashSeparator
	}
	multipartMarkerPath := ""
	if keyMarker != "" {
		multipartMarkerPath = pathJoin(bucket, keyMarker)
	}

	var uploads []uploadMetadata
	var err error
	var eof bool

	if uploadIDMarker != "" {
		uploads, _, err = fs.listMultipartUploadIDs(bucket, keyMarker, uploadIDMarker, maxUploads)
		if err != nil {
			return lmi, err
		}
		maxUploads = maxUploads - len(uploads)
	}

	var walkResultCh chan treeWalkResult
	var endWalkCh chan struct{}

	// true only for xl.ListObjectsHeal(), set to false.
	heal := false

	// Proceed to list only if we have more uploads to be listed.
	if maxUploads > 0 {
		listPrms := listParams{minioMetaMultipartBucket, recursive, multipartMarkerPath, multipartPrefixPath, heal}

		// Pop out any previously waiting marker.
		walkResultCh, endWalkCh = fs.listPool.Release(listPrms)
		if walkResultCh == nil {
			endWalkCh = make(chan struct{})
			isLeaf := fs.isMultipartUpload
			listDir := fs.listDirFactory(isLeaf)
			walkResultCh = startTreeWalk(minioMetaMultipartBucket, multipartPrefixPath,
				multipartMarkerPath, recursive, listDir, isLeaf, endWalkCh)
		}

		// List until maxUploads requested.
		for maxUploads > 0 {
			walkResult, ok := <-walkResultCh
			if !ok {
				// Closed channel.
				eof = true
				break
			}

			// For any walk error return right away.
			if walkResult.err != nil {
				// File not found or Disk not found is a valid case.
				if isErrIgnored(walkResult.err, fsTreeWalkIgnoredErrs...) {
					eof = true
					break
				}
				return lmi, walkResult.err
			}

			entry := strings.TrimPrefix(walkResult.entry, retainSlash(bucket))
			if hasSuffix(walkResult.entry, slashSeparator) {
				uploads = append(uploads, uploadMetadata{
					Object: entry,
				})
				maxUploads--
				if maxUploads == 0 {
					if walkResult.end {
						eof = true
						break
					}
				}
				continue
			}

			var tmpUploads []uploadMetadata
			var end bool
			uploadIDMarker = ""

			tmpUploads, end, err = fs.listMultipartUploadIDs(bucket, entry, uploadIDMarker, maxUploads)
			if err != nil {
				return lmi, err
			}

			uploads = append(uploads, tmpUploads...)
			maxUploads -= len(tmpUploads)
			if walkResult.end && end {
				eof = true
				break
			}
		}
	}

	// Loop through all the received uploads fill in the multiparts result.
	for _, upload := range uploads {
		var objectName string
		var uploadID string
		if hasSuffix(upload.Object, slashSeparator) {
			// All directory entries are common prefixes.
			uploadID = "" // Upload ids are empty for CommonPrefixes.
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
		fs.listPool.Set(listParams{bucket, recursive, result.NextKeyMarker, prefix, heal}, walkResultCh, endWalkCh)
	}

	result.IsTruncated = !eof
	if !result.IsTruncated {
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
	}

	// Success.
	return result, nil
}

// ListMultipartUploads - returns empty response always. The onus is
// on applications to remember uploadId of the multipart uploads that
// are in progress.
func (fs fsObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
	if err := checkListMultipartArgs(bucket, prefix, keyMarker, uploadIDMarker, delimiter, fs); err != nil {
		return lmi, err
	}

	if _, err := fs.statBucketDir(bucket); err != nil {
		return lmi, toObjectErr(err, bucket)
	}
	return ListMultipartsInfo{
		Prefix:         prefix,
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		Delimiter:      delimiter,
		MaxUploads:     maxUploads,
		IsTruncated:    false,
	}, nil
}

// listMultipartUploadsHelper - lists all the pending multipart uploads on a
// bucket. Additionally takes 'prefix, keyMarker, uploadIDmarker and a
// delimiter' which allows us to list uploads match a particular
// prefix or lexically starting from 'keyMarker' or delimiting the
// output to get a directory like listing.
//
// This function remains here to aid in listing uploads that require cleanup.
func (fs fsObjects) listMultipartUploadsHelper(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
	if err := checkListMultipartArgs(bucket, prefix, keyMarker, uploadIDMarker, delimiter, fs); err != nil {
		return lmi, err
	}

	if _, err := fs.statBucketDir(bucket); err != nil {
		return lmi, toObjectErr(err, bucket)
	}

	return fs.listMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// newMultipartUpload - wrapper for initializing a new multipart
// request, returns back a unique upload id.
//
// Internally this function creates 'uploads.json' associated for the
// incoming object at '.minio.sys/multipart/bucket/object/uploads.json' on
// all the disks. `uploads.json` carries metadata regarding on going
// multipart operation on the object.
func (fs fsObjects) newMultipartUpload(bucket string, object string, meta map[string]string) (uploadID string, err error) {
	// Initialize `fs.json` values.
	fsMeta := newFSMetaV1()

	// Save additional metadata.
	fsMeta.Meta = meta

	uploadID = mustGetUUID()
	initiated := UTCNow()

	// Add upload ID to uploads.json
	uploadsPath := pathJoin(bucket, object, uploadsJSONFile)
	rwlk, err := fs.rwPool.Create(pathJoin(fs.fsPath, minioMetaMultipartBucket, uploadsPath))
	if err != nil {
		return "", toObjectErr(traceError(err), bucket, object)
	}
	defer rwlk.Close()

	uploadIDPath := pathJoin(bucket, object, uploadID)
	fsMetaPath := pathJoin(fs.fsPath, minioMetaMultipartBucket, uploadIDPath, fsMetaJSONFile)
	metaFile, err := fs.rwPool.Create(fsMetaPath)
	if err != nil {
		return "", toObjectErr(traceError(err), bucket, object)
	}
	defer metaFile.Close()

	// Add a new upload id.
	if err = fs.addUploadID(bucket, object, uploadID, initiated, rwlk); err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Write all the set metadata.
	if _, err = fsMeta.WriteTo(metaFile); err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Return success.
	return uploadID, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a
// unique id. The unique id returned here is of UUID form, for each
// subsequent request each UUID is unique.
//
// Implements S3 compatible initiate multipart API.
func (fs fsObjects) NewMultipartUpload(bucket, object string, meta map[string]string) (string, error) {
	if err := checkNewMultipartArgs(bucket, object, fs); err != nil {
		return "", err
	}

	if _, err := fs.statBucketDir(bucket); err != nil {
		return "", toObjectErr(err, bucket)
	}

	// Hold the lock so that two parallel complete-multipart-uploads
	// do not leave a stale uploads.json behind.
	objectMPartPathLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket, pathJoin(bucket, object))
	if err := objectMPartPathLock.GetLock(globalOperationTimeout); err != nil {
		return "", err
	}
	defer objectMPartPathLock.Unlock()

	return fs.newMultipartUpload(bucket, object, meta)
}

// Returns if a new part can be appended to fsAppendDataFile.
func partToAppend(fsMeta fsMetaV1, fsAppendMeta fsMetaV1) (part objectPartInfo, appendNeeded bool) {
	if len(fsMeta.Parts) == 0 {
		return
	}

	// As fsAppendMeta.Parts will be sorted len(fsAppendMeta.Parts) will naturally be the next part number
	nextPartNum := len(fsAppendMeta.Parts) + 1
	nextPartIndex := fsMeta.ObjectPartIndex(nextPartNum)
	if nextPartIndex == -1 {
		return
	}

	return fsMeta.Parts[nextPartIndex], true
}

// CopyObjectPart - similar to PutObjectPart but reads data from an existing
// object. Internally incoming data is written to '.minio.sys/tmp' location
// and safely renamed to '.minio.sys/multipart' for reach parts.
func (fs fsObjects) CopyObjectPart(srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset int64, length int64) (pi PartInfo, e error) {
	if err := checkNewMultipartArgs(srcBucket, srcObject, fs); err != nil {
		return pi, err
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()

	go func() {
		if gerr := fs.GetObject(srcBucket, srcObject, startOffset, length, pipeWriter); gerr != nil {
			errorIf(gerr, "Unable to read %s/%s.", srcBucket, srcObject)
			pipeWriter.CloseWithError(gerr)
			return
		}
		pipeWriter.Close() // Close writer explicitly signalling we wrote all data.
	}()

	partInfo, err := fs.PutObjectPart(dstBucket, dstObject, uploadID, partID, NewHashReader(pipeReader, length, "", ""))
	if err != nil {
		return pi, toObjectErr(err, dstBucket, dstObject)
	}

	// Explicitly close the reader.
	pipeReader.Close()

	return partInfo, nil
}

// PutObjectPart - reads incoming data until EOF for the part file on
// an ongoing multipart transaction. Internally incoming data is
// written to '.minio.sys/tmp' location and safely renamed to
// '.minio.sys/multipart' for reach parts.
func (fs fsObjects) PutObjectPart(bucket, object, uploadID string, partID int, data *HashReader) (pi PartInfo, e error) {
	if err := checkPutObjectPartArgs(bucket, object, fs); err != nil {
		return pi, err
	}

	if _, err := fs.statBucketDir(bucket); err != nil {
		return pi, toObjectErr(err, bucket)
	}

	// Hold the lock so that two parallel complete-multipart-uploads
	// do not leave a stale uploads.json behind.
	objectMPartPathLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket, pathJoin(bucket, object))
	if err := objectMPartPathLock.GetLock(globalOperationTimeout); err != nil {
		return pi, err
	}
	defer objectMPartPathLock.Unlock()

	// Disallow any parallel abort or complete multipart operations.
	uploadsPath := pathJoin(fs.fsPath, minioMetaMultipartBucket, bucket, object, uploadsJSONFile)
	if _, err := fs.rwPool.Open(uploadsPath); err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return pi, traceError(InvalidUploadID{UploadID: uploadID})
		}
		return pi, toObjectErr(traceError(err), bucket, object)
	}
	defer fs.rwPool.Close(uploadsPath)

	uploadIDPath := pathJoin(bucket, object, uploadID)

	// Just check if the uploadID exists to avoid copy if it doesn't.
	fsMetaPath := pathJoin(fs.fsPath, minioMetaMultipartBucket, uploadIDPath, fsMetaJSONFile)
	rwlk, err := fs.rwPool.Write(fsMetaPath)
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return pi, traceError(InvalidUploadID{UploadID: uploadID})
		}
		return pi, toObjectErr(traceError(err), bucket, object)
	}
	defer rwlk.Close()

	fsMeta := fsMetaV1{}
	_, err = fsMeta.ReadFrom(rwlk)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, fsMetaPath)
	}

	partSuffix := fmt.Sprintf("object%d", partID)
	tmpPartPath := uploadID + "." + mustGetUUID() + "." + partSuffix

	bufSize := int64(readSizeV1)
	if size := data.Size(); size > 0 && bufSize > size {
		bufSize = size
	}
	buf := make([]byte, bufSize)

	fsPartPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, tmpPartPath)
	bytesWritten, cErr := fsCreateFile(fsPartPath, data, buf, data.Size())
	if cErr != nil {
		fsRemoveFile(fsPartPath)
		return pi, toObjectErr(cErr, minioMetaTmpBucket, tmpPartPath)
	}

	// Should return IncompleteBody{} error when reader has fewer
	// bytes than specified in request header.
	if bytesWritten < data.Size() {
		fsRemoveFile(fsPartPath)
		return pi, traceError(IncompleteBody{})
	}

	// Delete temporary part in case of failure. If
	// PutObjectPart succeeds then there would be nothing to
	// delete.
	defer fsRemoveFile(fsPartPath)

	if err = data.Verify(); err != nil {
		return pi, toObjectErr(err, minioMetaTmpBucket, tmpPartPath)
	}

	partPath := pathJoin(bucket, object, uploadID, partSuffix)
	// Lock the part so that another part upload with same part-number gets blocked
	// while the part is getting appended in the background.
	partLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket, partPath)
	if err = partLock.GetLock(globalOperationTimeout); err != nil {
		return pi, err
	}

	fsNSPartPath := pathJoin(fs.fsPath, minioMetaMultipartBucket, partPath)
	if err = fsRenameFile(fsPartPath, fsNSPartPath); err != nil {
		partLock.Unlock()
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Save the object part info in `fs.json`.
	md5Hex := hex.EncodeToString(data.MD5())
	fsMeta.AddObjectPart(partID, partSuffix, md5Hex, data.Size())
	if _, err = fsMeta.WriteTo(rwlk); err != nil {
		partLock.Unlock()
		return pi, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	partNamePath := pathJoin(fs.fsPath, minioMetaMultipartBucket, uploadIDPath, partSuffix)
	fi, err := fsStatFile(partNamePath)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partSuffix)
	}

	// Append the part in background.
	errCh := fs.append(bucket, object, uploadID, fsMeta)
	go func() {
		// Also receive the error so that the appendParts go-routine
		// does not block on send. But the error received is ignored
		// as fs.PutObjectPart() would have already returned success
		// to the client.
		<-errCh
		partLock.Unlock()
	}()

	return PartInfo{
		PartNumber:   partID,
		LastModified: fi.ModTime(),
		ETag:         md5Hex,
		Size:         fi.Size(),
	}, nil
}

// ListObjectParts - returns empty response always. Applications are
// expected to remember the uploaded part numbers and corresponding
// etags.
func (fs fsObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (lpi ListPartsInfo, e error) {
	if err := checkListPartsArgs(bucket, object, fs); err != nil {
		return lpi, err
	}

	// Check if bucket exists
	if _, err := fs.statBucketDir(bucket); err != nil {
		return lpi, toObjectErr(err, bucket)
	}

	// Check if uploadID exists. N B This check may race with a
	// concurrent abortMultipartUpload on the same uploadID. It is
	// OK to be eventually consistent w.r.t listing of objects,
	// uploads and parts.
	uploadIDPath := pathJoin(fs.fsPath, minioMetaMultipartBucket, bucket, object, uploadID)
	_, err := fsStatDir(uploadIDPath)
	if err != nil {
		return lpi, traceError(InvalidUploadID{UploadID: uploadID})
	}

	// Return empty list parts response
	return ListPartsInfo{
		Bucket:           bucket,
		Object:           object,
		UploadID:         uploadID,
		PartNumberMarker: 0,
		MaxParts:         maxParts,
	}, nil
}

// CompleteMultipartUpload - completes an ongoing multipart
// transaction after receiving all the parts indicated by the client.
// Returns an md5sum calculated by concatenating all the individual
// md5sums of all the parts.
//
// Implements S3 compatible Complete multipart API.
func (fs fsObjects) CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (oi ObjectInfo, e error) {
	if err := checkCompleteMultipartArgs(bucket, object, fs); err != nil {
		return oi, err
	}

	// Check if an object is present as one of the parent dir.
	if fs.parentDirIsObject(bucket, pathutil.Dir(object)) {
		return oi, toObjectErr(traceError(errFileAccessDenied), bucket, object)
	}

	if _, err := fs.statBucketDir(bucket); err != nil {
		return oi, toObjectErr(err, bucket)
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5, err := getCompleteMultipartMD5(parts)
	if err != nil {
		return oi, err
	}

	uploadIDPath := pathJoin(bucket, object, uploadID)
	var removeObjectDir bool

	// Hold the lock so that two parallel complete-multipart-uploads
	// do not leave a stale uploads.json behind.
	objectMPartPathLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket, pathJoin(bucket, object))
	if err = objectMPartPathLock.GetLock(globalOperationTimeout); err != nil {
		return oi, err
	}

	defer func() {
		if removeObjectDir {
			basePath := pathJoin(fs.fsPath, minioMetaMultipartBucket, bucket)
			derr := fsDeleteFile(basePath, pathJoin(basePath, object))
			errorIf(derr, "unable to remove %s in %s", pathJoin(basePath, object), basePath)
		}
		objectMPartPathLock.Unlock()
	}()

	fsMetaPathMultipart := pathJoin(fs.fsPath, minioMetaMultipartBucket, uploadIDPath, fsMetaJSONFile)
	rlk, err := fs.rwPool.Open(fsMetaPathMultipart)
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return oi, traceError(InvalidUploadID{UploadID: uploadID})
		}
		return oi, toObjectErr(traceError(err), bucket, object)
	}

	// Disallow any parallel abort or complete multipart operations.
	rwlk, err := fs.rwPool.Write(pathJoin(fs.fsPath, minioMetaMultipartBucket, bucket, object, uploadsJSONFile))
	if err != nil {
		fs.rwPool.Close(fsMetaPathMultipart)
		if err == errFileNotFound || err == errFileAccessDenied {
			return oi, traceError(InvalidUploadID{UploadID: uploadID})
		}
		return oi, toObjectErr(traceError(err), bucket, object)
	}
	defer rwlk.Close()

	fsMeta := fsMetaV1{}
	// Read saved fs metadata for ongoing multipart.
	_, err = fsMeta.ReadFrom(rlk.LockedFile)
	if err != nil {
		fs.rwPool.Close(fsMetaPathMultipart)
		return oi, toObjectErr(err, minioMetaMultipartBucket, fsMetaPathMultipart)
	}

	// Validate all parts and then commit to disk.
	for i, part := range parts {
		partIdx := fsMeta.ObjectPartIndex(part.PartNumber)
		if partIdx == -1 {
			fs.rwPool.Close(fsMetaPathMultipart)
			return oi, traceError(InvalidPart{})
		}

		if fsMeta.Parts[partIdx].ETag != part.ETag {
			fs.rwPool.Close(fsMetaPathMultipart)
			return oi, traceError(InvalidPart{})
		}

		// All parts except the last part has to be atleast 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(fsMeta.Parts[partIdx].Size) {
			fs.rwPool.Close(fsMetaPathMultipart)
			return oi, traceError(PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   fsMeta.Parts[partIdx].Size,
				PartETag:   part.ETag,
			})
		}
	}

	// Wait for any competing PutObject() operation on bucket/object, since same namespace
	// would be acquired for `fs.json`.
	fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fsMetaJSONFile)
	metaFile, err := fs.rwPool.Create(fsMetaPath)
	if err != nil {
		fs.rwPool.Close(fsMetaPathMultipart)
		return oi, toObjectErr(traceError(err), bucket, object)
	}
	defer metaFile.Close()

	fsNSObjPath := pathJoin(fs.fsPath, bucket, object)

	// This lock is held during rename of the appended tmp file to the actual
	// location so that any competing GetObject/PutObject/DeleteObject do not race.
	appendFallback := true // In case background-append did not append the required parts.

	if isPartsSame(fsMeta.Parts, parts) {
		err = fs.complete(bucket, object, uploadID, fsMeta)
		if err == nil {
			appendFallback = false
			fsTmpObjPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, uploadID)
			if err = fsRenameFile(fsTmpObjPath, fsNSObjPath); err != nil {
				fs.rwPool.Close(fsMetaPathMultipart)
				return oi, toObjectErr(err, minioMetaTmpBucket, uploadID)
			}
		}
	}

	if appendFallback {
		// background append could not do append all the required parts, hence we do it here.
		tempObj := uploadID + "-" + "part.1"

		fsTmpObjPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, tempObj)
		// Delete the temporary object in the case of a
		// failure. If PutObject succeeds, then there would be
		// nothing to delete.
		defer fsRemoveFile(fsTmpObjPath)

		// Allocate staging buffer.
		var buf = make([]byte, readSizeV1)

		for _, part := range parts {
			// Construct part suffix.
			partSuffix := fmt.Sprintf("object%d", part.PartNumber)
			multipartPartFile := pathJoin(fs.fsPath, minioMetaMultipartBucket, uploadIDPath, partSuffix)

			var reader io.ReadCloser
			var offset int64
			reader, _, err = fsOpenFile(multipartPartFile, offset)
			if err != nil {
				fs.rwPool.Close(fsMetaPathMultipart)
				if err == errFileNotFound {
					return oi, traceError(InvalidPart{})
				}
				return oi, toObjectErr(traceError(err), minioMetaMultipartBucket, partSuffix)
			}

			// No need to hold a lock, this is a unique file and will be only written
			// to one one process per uploadID per minio process.
			var wfile *os.File
			wfile, err = os.OpenFile((fsTmpObjPath), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
			if err != nil {
				reader.Close()
				fs.rwPool.Close(fsMetaPathMultipart)
				return oi, toObjectErr(traceError(err), bucket, object)
			}

			_, err = io.CopyBuffer(wfile, reader, buf)
			if err != nil {
				wfile.Close()
				reader.Close()
				fs.rwPool.Close(fsMetaPathMultipart)
				return oi, toObjectErr(traceError(err), bucket, object)
			}

			wfile.Close()
			reader.Close()
		}

		if err = fsRenameFile(fsTmpObjPath, fsNSObjPath); err != nil {
			fs.rwPool.Close(fsMetaPathMultipart)
			return oi, toObjectErr(err, minioMetaTmpBucket, uploadID)
		}
	}

	// No need to save part info, since we have concatenated all parts.
	fsMeta.Parts = nil

	// Save additional metadata.
	if len(fsMeta.Meta) == 0 {
		fsMeta.Meta = make(map[string]string)
	}
	fsMeta.Meta["etag"] = s3MD5

	// Write all the set metadata.
	if _, err = fsMeta.WriteTo(metaFile); err != nil {
		fs.rwPool.Close(fsMetaPathMultipart)
		return oi, toObjectErr(err, bucket, object)
	}

	// Close lock held on bucket/object/uploadid/fs.json,
	// this needs to be done for windows so that we can happily
	// delete the bucket/object/uploadid
	fs.rwPool.Close(fsMetaPathMultipart)

	// Cleanup all the parts if everything else has been safely committed.
	multipartObjectDir := pathJoin(fs.fsPath, minioMetaMultipartBucket, bucket, object)
	multipartUploadIDDir := pathJoin(multipartObjectDir, uploadID)
	if err = fsRemoveUploadIDPath(multipartObjectDir, multipartUploadIDDir); err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	// Remove entry from `uploads.json`.
	removeObjectDir, err = fs.removeUploadID(bucket, object, uploadID, rwlk)
	if err != nil {
		return oi, toObjectErr(err, minioMetaMultipartBucket, pathutil.Join(bucket, object))
	}

	fi, err := fsStatFile(fsNSObjPath)
	if err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	// Return object info.
	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// AbortMultipartUpload - aborts an ongoing multipart operation
// signified by the input uploadID. This is an atomic operation
// doesn't require clients to initiate multiple such requests.
//
// All parts are purged from all disks and reference to the uploadID
// would be removed from the system, rollback is not possible on this
// operation.
//
// Implements S3 compatible Abort multipart API, slight difference is
// that this is an atomic idempotent operation. Subsequent calls have
// no affect and further requests to the same uploadID would not be
// honored.
func (fs fsObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	if err := checkAbortMultipartArgs(bucket, object, fs); err != nil {
		return err
	}

	if _, err := fs.statBucketDir(bucket); err != nil {
		return toObjectErr(err, bucket)
	}

	uploadIDPath := pathJoin(bucket, object, uploadID)
	var removeObjectDir bool

	// Hold the lock so that two parallel complete-multipart-uploads
	// do not leave a stale uploads.json behind.
	objectMPartPathLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket,
		pathJoin(bucket, object))
	if err := objectMPartPathLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}

	defer func() {
		if removeObjectDir {
			basePath := pathJoin(fs.fsPath, minioMetaMultipartBucket, bucket)
			derr := fsDeleteFile(basePath, pathJoin(basePath, object))
			errorIf(derr, "unable to remove %s in %s", pathJoin(basePath, object), basePath)
		}
		objectMPartPathLock.Unlock()
	}()

	fsMetaPath := pathJoin(fs.fsPath, minioMetaMultipartBucket, uploadIDPath, fsMetaJSONFile)
	if _, err := fs.rwPool.Open(fsMetaPath); err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return traceError(InvalidUploadID{UploadID: uploadID})
		}
		return toObjectErr(traceError(err), bucket, object)
	}

	uploadsPath := pathJoin(bucket, object, uploadsJSONFile)
	rwlk, err := fs.rwPool.Write(pathJoin(fs.fsPath, minioMetaMultipartBucket, uploadsPath))
	if err != nil {
		fs.rwPool.Close(fsMetaPath)
		if err == errFileNotFound || err == errFileAccessDenied {
			return traceError(InvalidUploadID{UploadID: uploadID})
		}
		return toObjectErr(traceError(err), bucket, object)
	}
	defer rwlk.Close()

	// Signal appendParts routine to stop waiting for new parts to arrive.
	fs.abort(uploadID)

	// Close lock held on bucket/object/uploadid/fs.json,
	// this needs to be done for windows so that we can happily
	// delete the bucket/object/uploadid
	fs.rwPool.Close(fsMetaPath)

	// Cleanup all uploaded parts and abort the upload.
	multipartObjectDir := pathJoin(fs.fsPath, minioMetaMultipartBucket, bucket, object)
	multipartUploadIDDir := pathJoin(multipartObjectDir, uploadID)
	if err = fsRemoveUploadIDPath(multipartObjectDir, multipartUploadIDDir); err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Remove entry from `uploads.json`.
	removeObjectDir, err = fs.removeUploadID(bucket, object, uploadID, rwlk)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	return nil
}

// Removes multipart uploads if any older than `expiry` duration in a given bucket.
func (fs fsObjects) cleanupStaleMultipartUpload(bucket string, expiry time.Duration) (err error) {
	var lmi ListMultipartsInfo
	var st os.FileInfo
	for {
		// List multipart uploads in a bucket 1000 at a time
		prefix := ""
		lmi, err = fs.listMultipartUploadsHelper(bucket, prefix, lmi.KeyMarker, lmi.UploadIDMarker, "", 1000)
		if err != nil {
			errorIf(err, "Unable to list uploads")
			return err
		}

		// Remove uploads (and its parts) older than expiry duration.
		for _, upload := range lmi.Uploads {
			uploadIDPath := pathJoin(fs.fsPath, minioMetaMultipartBucket, bucket, upload.Object, upload.UploadID)
			if st, err = fsStatDir(uploadIDPath); err != nil {
				errorIf(err, "Failed to lookup uploads directory path %s", uploadIDPath)
				continue
			}
			if time.Since(st.ModTime()) > expiry {
				fs.AbortMultipartUpload(bucket, upload.Object, upload.UploadID)
			}
		}

		// No more incomplete uploads remain, break and return.
		if !lmi.IsTruncated {
			break
		}
	}

	return nil
}

// Removes multipart uploads if any older than `expiry` duration
// on all buckets for every `cleanupInterval`, this function is
// blocking and should be run in a go-routine.
func (fs fsObjects) cleanupStaleMultipartUploads(cleanupInterval, expiry time.Duration, doneCh chan struct{}) {
	ticker := time.NewTicker(cleanupInterval)
	for {
		select {
		case <-doneCh:
			// Stop the timer.
			ticker.Stop()
			return
		case <-ticker.C:
			bucketInfos, err := fs.ListBuckets()
			if err != nil {
				errorIf(err, "Unable to list buckets")
				continue
			}
			for _, bucketInfo := range bucketInfos {
				fs.cleanupStaleMultipartUpload(bucketInfo.Name, expiry)
			}
		}
	}
}
