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
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/mimedb"
)

// updateUploadJSON - add or remove upload ID info in all `uploads.json`.
func (xl xlObjects) updateUploadJSON(bucket, object, uploadID string, initiated time.Time, isRemove bool) error {
	uploadsPath := path.Join(bucket, object, uploadsJSONFile)
	tmpUploadsPath := mustGetUUID()

	// slice to store errors from disks
	errs := make([]error, len(xl.storageDisks))
	// slice to store if it is a delete operation on a disk
	isDelete := make([]bool, len(xl.storageDisks))

	wg := sync.WaitGroup{}
	for index, disk := range xl.storageDisks {
		if disk == nil {
			errs[index] = traceError(errDiskNotFound)
			continue
		}
		// Update `uploads.json` in a go routine.
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()

			// read and parse uploads.json on this disk
			uploadsJSON, err := readUploadsJSON(bucket, object, disk)
			if errorCause(err) == errFileNotFound {
				// If file is not found, we assume an
				// default (empty) upload info.
				uploadsJSON, err = newUploadsV1("xl"), nil
			}
			// If we have a read error, we store error and
			// exit.
			if err != nil {
				errs[index] = err
				return
			}

			if !isRemove {
				// Add the uploadID
				uploadsJSON.AddUploadID(uploadID, initiated)
			} else {
				// Remove the upload ID
				uploadsJSON.RemoveUploadID(uploadID)
				if len(uploadsJSON.Uploads) == 0 {
					isDelete[index] = true
				}
			}

			// For delete, rename to tmp, for the
			// possibility of recovery in case of quorum
			// failure.
			if !isDelete[index] {
				errs[index] = writeUploadJSON(&uploadsJSON, uploadsPath, tmpUploadsPath, disk)
			} else {
				wErr := disk.RenameFile(minioMetaMultipartBucket, uploadsPath, minioMetaTmpBucket, tmpUploadsPath)
				if wErr != nil {
					errs[index] = traceError(wErr)
				}

			}
		}(index, disk)
	}

	// Wait for all the writes to finish.
	wg.Wait()

	err := reduceWriteQuorumErrs(errs, objectOpIgnoredErrs, xl.writeQuorum)
	if errorCause(err) == errXLWriteQuorum {
		// No quorum. Perform cleanup on the minority of disks
		// on which the operation succeeded.

		// There are two cases:
		//
		// 1. uploads.json file was updated -> we delete the
		//    file that we successfully overwrote on the
		//    minority of disks, so that the failed quorum
		//    operation is not partially visible.
		//
		// 2. uploads.json was deleted -> in this case since
		//    the delete failed, we restore from tmp.
		for index, disk := range xl.storageDisks {
			if disk == nil || errs[index] != nil {
				continue
			}
			wg.Add(1)
			go func(index int, disk StorageAPI) {
				defer wg.Done()
				if !isDelete[index] {
					_ = disk.DeleteFile(
						minioMetaMultipartBucket,
						uploadsPath,
					)
				} else {
					_ = disk.RenameFile(
						minioMetaTmpBucket, tmpUploadsPath,
						minioMetaMultipartBucket, uploadsPath,
					)
				}
			}(index, disk)
		}
		wg.Wait()
		return err
	}

	// we do have quorum, so in case of delete upload.json file
	// operation, we purge from tmp.
	for index, disk := range xl.storageDisks {
		if disk == nil || !isDelete[index] {
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			// isDelete[index] = true at this point.
			_ = disk.DeleteFile(minioMetaTmpBucket, tmpUploadsPath)
		}(index, disk)
	}
	wg.Wait()
	return err
}

// addUploadID - add upload ID and its initiated time to 'uploads.json'.
func (xl xlObjects) addUploadID(bucket, object string, uploadID string, initiated time.Time) error {
	return xl.updateUploadJSON(bucket, object, uploadID, initiated, false)
}

// removeUploadID - remove upload ID in 'uploads.json'.
func (xl xlObjects) removeUploadID(bucket, object string, uploadID string) error {
	return xl.updateUploadJSON(bucket, object, uploadID, time.Time{}, true)
}

// Returns if the prefix is a multipart upload.
func (xl xlObjects) isMultipartUpload(bucket, prefix string) bool {
	for _, disk := range xl.getLoadBalancedDisks() {
		if disk == nil {
			continue
		}
		_, err := disk.StatFile(bucket, pathJoin(prefix, uploadsJSONFile))
		if err == nil {
			return true
		}
		// For any reason disk was deleted or goes offline, continue
		if isErrIgnored(err, objMetadataOpIgnoredErrs...) {
			continue
		}
		break
	}
	return false
}

// isUploadIDExists - verify if a given uploadID exists and is valid.
func (xl xlObjects) isUploadIDExists(bucket, object, uploadID string) bool {
	uploadIDPath := path.Join(bucket, object, uploadID)
	return xl.isObject(minioMetaMultipartBucket, uploadIDPath)
}

// Removes part given by partName belonging to a mulitpart upload from minioMetaBucket
func (xl xlObjects) removeObjectPart(bucket, object, uploadID, partName string) {
	curpartPath := path.Join(bucket, object, uploadID, partName)
	wg := sync.WaitGroup{}
	for i, disk := range xl.storageDisks {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			// Ignoring failure to remove parts that weren't present in CompleteMultipartUpload
			// requests. xl.json is the authoritative source of truth on which parts constitute
			// the object. The presence of parts that don't belong in the object doesn't affect correctness.
			_ = disk.DeleteFile(minioMetaMultipartBucket, curpartPath)
		}(i, disk)
	}
	wg.Wait()
}

// statPart - returns fileInfo structure for a successful stat on part file.
func (xl xlObjects) statPart(bucket, object, uploadID, partName string) (fileInfo FileInfo, err error) {
	var ignoredErrs []error
	partNamePath := path.Join(bucket, object, uploadID, partName)
	for _, disk := range xl.getLoadBalancedDisks() {
		if disk == nil {
			ignoredErrs = append(ignoredErrs, errDiskNotFound)
			continue
		}
		fileInfo, err = disk.StatFile(minioMetaMultipartBucket, partNamePath)
		if err == nil {
			return fileInfo, nil
		}
		// For any reason disk was deleted or goes offline we continue to next disk.
		if isErrIgnored(err, objMetadataOpIgnoredErrs...) {
			ignoredErrs = append(ignoredErrs, err)
			continue
		}
		// Error is not ignored, return right here.
		return FileInfo{}, traceError(err)
	}
	// If all errors were ignored, reduce to maximal occurrence
	// based on the read quorum.
	return FileInfo{}, reduceReadQuorumErrs(ignoredErrs, nil, xl.readQuorum)
}

// commitXLMetadata - commit `xl.json` from source prefix to destination prefix in the given slice of disks.
func commitXLMetadata(disks []StorageAPI, srcBucket, srcPrefix, dstBucket, dstPrefix string, quorum int) ([]StorageAPI, error) {
	var wg = &sync.WaitGroup{}
	var mErrs = make([]error, len(disks))

	srcJSONFile := path.Join(srcPrefix, xlMetaJSONFile)
	dstJSONFile := path.Join(dstPrefix, xlMetaJSONFile)

	// Rename `xl.json` to all disks in parallel.
	for index, disk := range disks {
		if disk == nil {
			mErrs[index] = traceError(errDiskNotFound)
			continue
		}
		wg.Add(1)
		// Rename `xl.json` in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			// Delete any dangling directories.
			defer disk.DeleteFile(srcBucket, srcPrefix)

			// Renames `xl.json` from source prefix to destination prefix.
			rErr := disk.RenameFile(srcBucket, srcJSONFile, dstBucket, dstJSONFile)
			if rErr != nil {
				mErrs[index] = traceError(rErr)
				return
			}
			mErrs[index] = nil
		}(index, disk)
	}
	// Wait for all the routines.
	wg.Wait()

	err := reduceWriteQuorumErrs(mErrs, objectOpIgnoredErrs, quorum)
	if errorCause(err) == errXLWriteQuorum {
		// Delete all `xl.json` successfully renamed.
		deleteAllXLMetadata(disks, dstBucket, dstPrefix, mErrs)
	}
	return evalDisks(disks, mErrs), err
}

// listMultipartUploads - lists all multipart uploads.
func (xl xlObjects) listMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
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
	// List all upload ids for the keyMarker starting from
	// uploadIDMarker first.
	if uploadIDMarker != "" {
		// hold lock on keyMarker path
		keyMarkerLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket,
			pathJoin(bucket, keyMarker))
		if err = keyMarkerLock.GetRLock(globalListingTimeout); err != nil {
			return lmi, err
		}
		for _, disk := range xl.getLoadBalancedDisks() {
			if disk == nil {
				continue
			}
			uploads, _, err = listMultipartUploadIDs(bucket, keyMarker, uploadIDMarker, maxUploads, disk)
			if err == nil {
				break
			}
			if isErrIgnored(err, objMetadataOpIgnoredErrs...) {
				continue
			}
			break
		}
		keyMarkerLock.RUnlock()
		if err != nil {
			return lmi, err
		}
		maxUploads = maxUploads - len(uploads)
	}
	var walkerCh chan treeWalkResult
	var walkerDoneCh chan struct{}
	heal := false // true only for xl.ListObjectsHeal
	// Validate if we need to list further depending on maxUploads.
	if maxUploads > 0 {
		walkerCh, walkerDoneCh = xl.listPool.Release(listParams{minioMetaMultipartBucket, recursive, multipartMarkerPath, multipartPrefixPath, heal})
		if walkerCh == nil {
			walkerDoneCh = make(chan struct{})
			isLeaf := xl.isMultipartUpload
			listDir := listDirFactory(isLeaf, xlTreeWalkIgnoredErrs, xl.getLoadBalancedDisks()...)
			walkerCh = startTreeWalk(minioMetaMultipartBucket, multipartPrefixPath, multipartMarkerPath, recursive, listDir, isLeaf, walkerDoneCh)
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
				return lmi, walkResult.err
			}
			entry := strings.TrimPrefix(walkResult.entry, retainSlash(bucket))
			// For an entry looking like a directory, store and
			// continue the loop not need to fetch uploads.
			if hasSuffix(walkResult.entry, slashSeparator) {
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
			entryLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket,
				pathJoin(bucket, entry))
			if err = entryLock.GetRLock(globalListingTimeout); err != nil {
				return lmi, err
			}
			var disk StorageAPI
			for _, disk = range xl.getLoadBalancedDisks() {
				if disk == nil {
					continue
				}
				newUploads, end, err = listMultipartUploadIDs(bucket, entry, uploadIDMarker, maxUploads, disk)
				if err == nil {
					break
				}
				if isErrIgnored(err, objMetadataOpIgnoredErrs...) {
					continue
				}
				break
			}
			entryLock.RUnlock()
			if err != nil {
				if isErrIgnored(err, xlTreeWalkIgnoredErrs...) {
					continue
				}
				return lmi, err
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
		if hasSuffix(upload.Object, slashSeparator) {
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

// ListMultipartUploads - lists all the pending multipart uploads on a
// bucket. Additionally takes 'prefix, keyMarker, uploadIDmarker and a
// delimiter' which allows us to list uploads match a particular
// prefix or lexically starting from 'keyMarker' or delimiting the
// output to get a directory like listing.
//
// Implements S3 compatible ListMultipartUploads API. The resulting
// ListMultipartsInfo structure is unmarshalled directly into XML and
// replied back to the client.
func (xl xlObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
	if err := checkListMultipartArgs(bucket, prefix, keyMarker, uploadIDMarker, delimiter, xl); err != nil {
		return lmi, err
	}

	return xl.listMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// newMultipartUpload - wrapper for initializing a new multipart
// request; returns a unique upload id.
//
// Internally this function creates 'uploads.json' associated for the
// incoming object at
// '.minio.sys/multipart/bucket/object/uploads.json' on all the
// disks. `uploads.json` carries metadata regarding on-going multipart
// operation(s) on the object.
func (xl xlObjects) newMultipartUpload(bucket string, object string, meta map[string]string) (string, error) {
	xlMeta := newXLMetaV1(object, xl.dataBlocks, xl.parityBlocks)
	// If not set default to "application/octet-stream"
	if meta["content-type"] == "" {
		contentType := "application/octet-stream"
		if objectExt := path.Ext(object); objectExt != "" {
			content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]
			if ok {
				contentType = content.ContentType
			}
		}
		meta["content-type"] = contentType
	}
	xlMeta.Stat.ModTime = UTCNow()
	xlMeta.Meta = meta

	// This lock needs to be held for any changes to the directory
	// contents of ".minio.sys/multipart/object/"
	objectMPartPathLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket,
		pathJoin(bucket, object))
	if err := objectMPartPathLock.GetLock(globalOperationTimeout); err != nil {
		return "", err
	}
	defer objectMPartPathLock.Unlock()

	uploadID := mustGetUUID()
	uploadIDPath := path.Join(bucket, object, uploadID)
	tempUploadIDPath := uploadID

	// Write updated `xl.json` to all disks.
	disks, err := writeSameXLMetadata(xl.storageDisks, minioMetaTmpBucket, tempUploadIDPath, xlMeta, xl.writeQuorum, xl.readQuorum)
	if err != nil {
		return "", toObjectErr(err, minioMetaTmpBucket, tempUploadIDPath)
	}
	// delete the tmp path later in case we fail to rename (ignore
	// returned errors) - this will be a no-op in case of a rename
	// success.
	defer xl.deleteObject(minioMetaTmpBucket, tempUploadIDPath)

	// Attempt to rename temp upload object to actual upload path object
	_, rErr := renameObject(disks, minioMetaTmpBucket, tempUploadIDPath, minioMetaMultipartBucket, uploadIDPath, xl.writeQuorum)
	if rErr != nil {
		return "", toObjectErr(rErr, minioMetaMultipartBucket, uploadIDPath)
	}

	initiated := UTCNow()
	// Create or update 'uploads.json'
	if err = xl.addUploadID(bucket, object, uploadID, initiated); err != nil {
		return "", err
	}
	// Return success.
	return uploadID, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a
// unique id. The unique id returned here is of UUID form, for each
// subsequent request each UUID is unique.
//
// Implements S3 compatible initiate multipart API.
func (xl xlObjects) NewMultipartUpload(bucket, object string, meta map[string]string) (string, error) {
	if err := checkNewMultipartArgs(bucket, object, xl); err != nil {
		return "", err
	}
	// No metadata is set, allocate a new one.
	if meta == nil {
		meta = make(map[string]string)
	}
	return xl.newMultipartUpload(bucket, object, meta)
}

// CopyObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to put object part operation but the source
// data is read from an existing object.
//
// Implements S3 compatible Upload Part Copy API.
func (xl xlObjects) CopyObjectPart(srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset int64, length int64) (pi PartInfo, e error) {
	if err := checkNewMultipartArgs(srcBucket, srcObject, xl); err != nil {
		return pi, err
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()

	go func() {
		if gerr := xl.GetObject(srcBucket, srcObject, startOffset, length, pipeWriter); gerr != nil {
			errorIf(gerr, "Unable to read %s of the object `%s/%s`.", srcBucket, srcObject)
			pipeWriter.CloseWithError(toObjectErr(gerr, srcBucket, srcObject))
			return
		}
		pipeWriter.Close() // Close writer explicitly signalling we wrote all data.
	}()

	hashReader, err := hash.NewReader(pipeReader, length, "", "")
	if err != nil {
		return pi, toObjectErr(err, dstBucket, dstObject)
	}

	partInfo, err := xl.PutObjectPart(dstBucket, dstObject, uploadID, partID, hashReader)
	if err != nil {
		return pi, toObjectErr(err, dstBucket, dstObject)
	}

	// Explicitly close the reader.
	pipeReader.Close()

	// Success.
	return partInfo, nil
}

// PutObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to single put operation but it is part
// of the multipart transaction.
//
// Implements S3 compatible Upload Part API.
func (xl xlObjects) PutObjectPart(bucket, object, uploadID string, partID int, data *hash.Reader) (pi PartInfo, e error) {
	if err := checkPutObjectPartArgs(bucket, object, xl); err != nil {
		return pi, err
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < 0 {
		return pi, toObjectErr(traceError(errInvalidArgument))
	}

	var partsMetadata []xlMetaV1
	var errs []error
	uploadIDPath := pathJoin(bucket, object, uploadID)

	// pre-check upload id lock.
	preUploadIDLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket, uploadIDPath)
	if err := preUploadIDLock.GetRLock(globalOperationTimeout); err != nil {
		return pi, err
	}

	// Validates if upload ID exists.
	if !xl.isUploadIDExists(bucket, object, uploadID) {
		preUploadIDLock.RUnlock()
		return pi, traceError(InvalidUploadID{UploadID: uploadID})
	}

	// Read metadata associated with the object from all disks.
	partsMetadata, errs = readAllXLMetadata(xl.storageDisks, minioMetaMultipartBucket,
		uploadIDPath)
	reducedErr := reduceWriteQuorumErrs(errs, objectOpIgnoredErrs, xl.writeQuorum)
	if errorCause(reducedErr) == errXLWriteQuorum {
		preUploadIDLock.RUnlock()
		return pi, toObjectErr(reducedErr, bucket, object)
	}
	preUploadIDLock.RUnlock()

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(xl.storageDisks, partsMetadata, errs)

	// Pick one from the first valid metadata.
	xlMeta, err := pickValidXLMeta(partsMetadata, modTime)
	if err != nil {
		return pi, err
	}

	onlineDisks = shuffleDisks(onlineDisks, xlMeta.Erasure.Distribution)

	// Need a unique name for the part being written in minioMetaBucket to
	// accommodate concurrent PutObjectPart requests

	partSuffix := fmt.Sprintf("part.%d", partID)
	tmpPart := mustGetUUID()
	tmpPartPath := path.Join(tmpPart, partSuffix)

	// Delete the temporary object part. If PutObjectPart succeeds there would be nothing to delete.
	defer xl.deleteObject(minioMetaTmpBucket, tmpPart)
	if data.Size() > 0 {
		if pErr := xl.prepareFile(minioMetaTmpBucket, tmpPartPath, data.Size(), onlineDisks, xlMeta.Erasure.BlockSize, xlMeta.Erasure.DataBlocks); err != nil {
			return pi, toObjectErr(pErr, bucket, object)

		}
	}

	storage, err := NewErasureStorage(onlineDisks, xlMeta.Erasure.DataBlocks, xlMeta.Erasure.ParityBlocks)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}
	buffer := make([]byte, xlMeta.Erasure.BlockSize, 2*xlMeta.Erasure.BlockSize) // alloc additional space for parity blocks created while erasure coding
	file, err := storage.CreateFile(data, minioMetaTmpBucket, tmpPartPath, buffer, DefaultBitrotAlgorithm, xl.writeQuorum)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if file.Size < data.Size() {
		return pi, traceError(IncompleteBody{})
	}

	// post-upload check (write) lock
	postUploadIDLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket, uploadIDPath)
	if err = postUploadIDLock.GetLock(globalOperationTimeout); err != nil {
		return pi, err
	}
	defer postUploadIDLock.Unlock()

	// Validate again if upload ID still exists.
	if !xl.isUploadIDExists(bucket, object, uploadID) {
		return pi, traceError(InvalidUploadID{UploadID: uploadID})
	}

	// Rename temporary part file to its final location.
	partPath := path.Join(uploadIDPath, partSuffix)
	onlineDisks, err = renamePart(onlineDisks, minioMetaTmpBucket, tmpPartPath, minioMetaMultipartBucket, partPath, xl.writeQuorum)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Read metadata again because it might be updated with parallel upload of another part.
	partsMetadata, errs = readAllXLMetadata(onlineDisks, minioMetaMultipartBucket, uploadIDPath)
	reducedErr = reduceWriteQuorumErrs(errs, objectOpIgnoredErrs, xl.writeQuorum)
	if errorCause(reducedErr) == errXLWriteQuorum {
		return pi, toObjectErr(reducedErr, bucket, object)
	}

	// Get current highest version based on re-read partsMetadata.
	onlineDisks, modTime = listOnlineDisks(onlineDisks, partsMetadata, errs)

	// Pick one from the first valid metadata.
	xlMeta, err = pickValidXLMeta(partsMetadata, modTime)
	if err != nil {
		return pi, err
	}

	// Once part is successfully committed, proceed with updating XL metadata.
	xlMeta.Stat.ModTime = UTCNow()

	md5hex := data.MD5HexString()

	// Add the current part.
	xlMeta.AddObjectPart(partID, partSuffix, md5hex, file.Size)

	for i, disk := range onlineDisks {
		if disk == OfflineDisk {
			continue
		}
		partsMetadata[i].Parts = xlMeta.Parts
		partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{partSuffix, file.Algorithm, file.Checksums[i]})
	}

	// Write all the checksum metadata.
	newUUID := mustGetUUID()
	tempXLMetaPath := newUUID

	// Writes a unique `xl.json` each disk carrying new checksum related information.
	if onlineDisks, err = writeUniqueXLMetadata(onlineDisks, minioMetaTmpBucket, tempXLMetaPath, partsMetadata, xl.writeQuorum); err != nil {
		return pi, toObjectErr(err, minioMetaTmpBucket, tempXLMetaPath)
	}

	if _, err = commitXLMetadata(onlineDisks, minioMetaTmpBucket, tempXLMetaPath, minioMetaMultipartBucket, uploadIDPath, xl.writeQuorum); err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	fi, err := xl.statPart(bucket, object, uploadID, partSuffix)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partSuffix)
	}

	// Return success.
	return PartInfo{
		PartNumber:   partID,
		LastModified: fi.ModTime,
		ETag:         md5hex,
		Size:         fi.Size,
	}, nil
}

// listObjectParts - wrapper reading `xl.json` for a given object and
// uploadID. Lists all the parts captured inside `xl.json` content.
func (xl xlObjects) listObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (lpi ListPartsInfo, e error) {
	result := ListPartsInfo{}

	uploadIDPath := path.Join(bucket, object, uploadID)

	xlParts, err := xl.readXLMetaParts(minioMetaMultipartBucket, uploadIDPath)
	if err != nil {
		return lpi, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	// Populate the result stub.
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts

	// For empty number of parts or maxParts as zero, return right here.
	if len(xlParts) == 0 || maxParts == 0 {
		return result, nil
	}

	// Limit output to maxPartsList.
	if maxParts > maxPartsList {
		maxParts = maxPartsList
	}

	// Only parts with higher part numbers will be listed.
	partIdx := objectPartIndex(xlParts, partNumberMarker)
	parts := xlParts
	if partIdx != -1 {
		parts = xlParts[partIdx+1:]
	}
	count := maxParts
	for _, part := range parts {
		var fi FileInfo
		fi, err = xl.statPart(bucket, object, uploadID, part.Name)
		if err != nil {
			return lpi, toObjectErr(err, minioMetaBucket, path.Join(uploadID, part.Name))
		}
		result.Parts = append(result.Parts, PartInfo{
			PartNumber:   part.Number,
			ETag:         part.ETag,
			LastModified: fi.ModTime,
			Size:         part.Size,
		})
		count--
		if count == 0 {
			break
		}
	}
	// If listed entries are more than maxParts, we set IsTruncated as true.
	if len(parts) > len(result.Parts) {
		result.IsTruncated = true
		// Make sure to fill next part number marker if IsTruncated is
		// true for subsequent listing.
		nextPartNumberMarker := result.Parts[len(result.Parts)-1].PartNumber
		result.NextPartNumberMarker = nextPartNumberMarker
	}
	return result, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is unmarshalled directly into XML and
// replied back to the client.
func (xl xlObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (lpi ListPartsInfo, e error) {
	if err := checkListPartsArgs(bucket, object, xl); err != nil {
		return lpi, err
	}

	// Hold lock so that there is no competing
	// abort-multipart-upload or complete-multipart-upload.
	uploadIDLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket,
		pathJoin(bucket, object, uploadID))
	if err := uploadIDLock.GetLock(globalListingTimeout); err != nil {
		return lpi, err
	}
	defer uploadIDLock.Unlock()

	if !xl.isUploadIDExists(bucket, object, uploadID) {
		return lpi, traceError(InvalidUploadID{UploadID: uploadID})
	}
	result, err := xl.listObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
	return result, err
}

// CompleteMultipartUpload - completes an ongoing multipart
// transaction after receiving all the parts indicated by the client.
// Returns an md5sum calculated by concatenating all the individual
// md5sums of all the parts.
//
// Implements S3 compatible Complete multipart API.
func (xl xlObjects) CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (oi ObjectInfo, e error) {
	if err := checkCompleteMultipartArgs(bucket, object, xl); err != nil {
		return oi, err
	}

	// Hold lock so that
	//
	// 1) no one aborts this multipart upload
	//
	// 2) no one does a parallel complete-multipart-upload on this
	// multipart upload
	uploadIDLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket,
		pathJoin(bucket, object, uploadID))
	if err := uploadIDLock.GetLock(globalOperationTimeout); err != nil {
		return oi, err
	}
	defer uploadIDLock.Unlock()

	if !xl.isUploadIDExists(bucket, object, uploadID) {
		return oi, traceError(InvalidUploadID{UploadID: uploadID})
	}

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
	if xl.parentDirIsObject(bucket, path.Dir(object)) {
		return oi, toObjectErr(traceError(errFileAccessDenied), bucket, object)
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5, err := getCompleteMultipartMD5(parts)
	if err != nil {
		return oi, err
	}

	uploadIDPath := pathJoin(bucket, object, uploadID)

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllXLMetadata(xl.storageDisks, minioMetaMultipartBucket, uploadIDPath)
	reducedErr := reduceWriteQuorumErrs(errs, objectOpIgnoredErrs, xl.writeQuorum)
	if errorCause(reducedErr) == errXLWriteQuorum {
		return oi, toObjectErr(reducedErr, bucket, object)
	}

	onlineDisks, modTime := listOnlineDisks(xl.storageDisks, partsMetadata, errs)

	// Calculate full object size.
	var objectSize int64

	// Pick one from the first valid metadata.
	xlMeta, err := pickValidXLMeta(partsMetadata, modTime)
	if err != nil {
		return oi, err
	}

	// Order online disks in accordance with distribution order.
	onlineDisks = shuffleDisks(onlineDisks, xlMeta.Erasure.Distribution)

	// Order parts metadata in accordance with distribution order.
	partsMetadata = shufflePartsMetadata(partsMetadata, xlMeta.Erasure.Distribution)

	// Save current xl meta for validation.
	var currentXLMeta = xlMeta

	// Allocate parts similar to incoming slice.
	xlMeta.Parts = make([]objectPartInfo, len(parts))

	// Validate each part and then commit to disk.
	for i, part := range parts {
		partIdx := objectPartIndex(currentXLMeta.Parts, part.PartNumber)
		// All parts should have same part number.
		if partIdx == -1 {
			return oi, traceError(InvalidPart{})
		}

		// All parts should have same ETag as previously generated.
		if currentXLMeta.Parts[partIdx].ETag != part.ETag {
			return oi, traceError(InvalidPart{})
		}

		// All parts except the last part has to be atleast 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(currentXLMeta.Parts[partIdx].Size) {
			return oi, traceError(PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   currentXLMeta.Parts[partIdx].Size,
				PartETag:   part.ETag,
			})
		}

		// Last part could have been uploaded as 0bytes, do not need
		// to save it in final `xl.json`.
		if (i == len(parts)-1) && currentXLMeta.Parts[partIdx].Size == 0 {
			xlMeta.Parts = xlMeta.Parts[:i] // Skip the part.
			continue
		}

		// Save for total object size.
		objectSize += currentXLMeta.Parts[partIdx].Size

		// Add incoming parts.
		xlMeta.Parts[i] = objectPartInfo{
			Number: part.PartNumber,
			ETag:   part.ETag,
			Size:   currentXLMeta.Parts[partIdx].Size,
			Name:   fmt.Sprintf("part.%d", part.PartNumber),
		}
	}

	// Save the final object size and modtime.
	xlMeta.Stat.Size = objectSize
	xlMeta.Stat.ModTime = UTCNow()

	// Save successfully calculated md5sum.
	xlMeta.Meta["etag"] = s3MD5

	uploadIDPath = path.Join(bucket, object, uploadID)
	tempUploadIDPath := uploadID

	// Update all xl metadata, make sure to not modify fields like
	// checksum which are different on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Stat = xlMeta.Stat
		partsMetadata[index].Meta = xlMeta.Meta
		partsMetadata[index].Parts = xlMeta.Parts
	}

	// Write unique `xl.json` for each disk.
	if onlineDisks, err = writeUniqueXLMetadata(onlineDisks, minioMetaTmpBucket, tempUploadIDPath, partsMetadata, xl.writeQuorum); err != nil {
		return oi, toObjectErr(err, minioMetaTmpBucket, tempUploadIDPath)
	}

	var rErr error
	onlineDisks, rErr = commitXLMetadata(onlineDisks, minioMetaTmpBucket, tempUploadIDPath, minioMetaMultipartBucket, uploadIDPath, xl.writeQuorum)
	if rErr != nil {
		return oi, toObjectErr(rErr, minioMetaMultipartBucket, uploadIDPath)
	}

	defer func() {
		if xl.objCacheEnabled {
			// A new complete multipart upload invalidates any
			// previously cached object in memory.
			xl.objCache.Delete(path.Join(bucket, object))

			// Prefetch the object from disk by triggering a fake GetObject call
			// Unlike a regular single PutObject,  multipart PutObject is comes in
			// stages and it is harder to cache.
			go xl.GetObject(bucket, object, 0, objectSize, ioutil.Discard)
		}
	}()

	if xl.isObject(bucket, object) {
		// Rename if an object already exists to temporary location.
		newUniqueID := mustGetUUID()

		// Delete success renamed object.
		defer xl.deleteObject(minioMetaTmpBucket, newUniqueID)

		// NOTE: Do not use online disks slice here.
		// The reason is that existing object should be purged
		// regardless of `xl.json` status and rolled back in case of errors.
		_, err = renameObject(xl.storageDisks, bucket, object, minioMetaTmpBucket, newUniqueID, xl.writeQuorum)
		if err != nil {
			return oi, toObjectErr(err, bucket, object)
		}
	}

	// Remove parts that weren't present in CompleteMultipartUpload request.
	for _, curpart := range currentXLMeta.Parts {
		if objectPartIndex(xlMeta.Parts, curpart.Number) == -1 {
			// Delete the missing part files. e.g,
			// Request 1: NewMultipart
			// Request 2: PutObjectPart 1
			// Request 3: PutObjectPart 2
			// Request 4: CompleteMultipartUpload --part 2
			// N.B. 1st part is not present. This part should be removed from the storage.
			xl.removeObjectPart(bucket, object, uploadID, curpart.Name)
		}
	}

	// Rename the multipart object to final location.
	if _, err = renameObject(onlineDisks, minioMetaMultipartBucket, uploadIDPath, bucket, object, xl.writeQuorum); err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	// Hold the lock so that two parallel
	// complete-multipart-uploads do not leave a stale
	// uploads.json behind.
	objectMPartPathLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket,
		pathJoin(bucket, object))
	if err = objectMPartPathLock.GetLock(globalOperationTimeout); err != nil {
		return oi, toObjectErr(err, bucket, object)
	}
	defer objectMPartPathLock.Unlock()

	// remove entry from uploads.json with quorum
	if err = xl.removeUploadID(bucket, object, uploadID); err != nil {
		return oi, toObjectErr(err, minioMetaMultipartBucket, path.Join(bucket, object))
	}

	objInfo := ObjectInfo{
		IsDir:           false,
		Bucket:          bucket,
		Name:            object,
		Size:            xlMeta.Stat.Size,
		ModTime:         xlMeta.Stat.ModTime,
		ETag:            xlMeta.Meta["etag"],
		ContentType:     xlMeta.Meta["content-type"],
		ContentEncoding: xlMeta.Meta["content-encoding"],
		UserDefined:     xlMeta.Meta,
	}

	// Success, return object info.
	return objInfo, nil
}

// Wrapper which removes all the uploaded parts.
func (xl xlObjects) cleanupUploadedParts(bucket, object, uploadID string) error {
	var errs = make([]error, len(xl.storageDisks))
	var wg = &sync.WaitGroup{}

	// Construct uploadIDPath.
	uploadIDPath := path.Join(bucket, object, uploadID)

	// Cleanup uploadID for all disks.
	for index, disk := range xl.storageDisks {
		if disk == nil {
			errs[index] = traceError(errDiskNotFound)
			continue
		}
		wg.Add(1)
		// Cleanup each uploadID in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := cleanupDir(disk, minioMetaMultipartBucket, uploadIDPath)
			if err != nil {
				errs[index] = err
			}
		}(index, disk)
	}

	// Wait for all the cleanups to finish.
	wg.Wait()

	return reduceWriteQuorumErrs(errs, objectOpIgnoredErrs, xl.writeQuorum)
}

// abortMultipartUpload - wrapper for purging an ongoing multipart
// transaction, deletes uploadID entry from `uploads.json` and purges
// the directory at '.minio.sys/multipart/bucket/object/uploadID' holding
// all the upload parts.
func (xl xlObjects) abortMultipartUpload(bucket, object, uploadID string) (err error) {
	// Cleanup all uploaded parts.
	if err = xl.cleanupUploadedParts(bucket, object, uploadID); err != nil {
		return toObjectErr(err, bucket, object)
	}

	// hold lock so we don't compete with a complete, or abort
	// multipart request.
	objectMPartPathLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket,
		pathJoin(bucket, object))
	if err = objectMPartPathLock.GetLock(globalOperationTimeout); err != nil {
		return toObjectErr(err, bucket, object)
	}
	defer objectMPartPathLock.Unlock()

	// remove entry from uploads.json with quorum
	if err = xl.removeUploadID(bucket, object, uploadID); err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Successfully purged.
	return nil
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
// no affect and further requests to the same uploadID would not be honored.
func (xl xlObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	if err := checkAbortMultipartArgs(bucket, object, xl); err != nil {
		return err
	}

	// Hold lock so that there is no competing
	// complete-multipart-upload or put-object-part.
	uploadIDLock := globalNSMutex.NewNSLock(minioMetaMultipartBucket,
		pathJoin(bucket, object, uploadID))
	if err := uploadIDLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer uploadIDLock.Unlock()

	if !xl.isUploadIDExists(bucket, object, uploadID) {
		return traceError(InvalidUploadID{UploadID: uploadID})
	}
	return xl.abortMultipartUpload(bucket, object, uploadID)
}
