/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/mimedb"
)

func (xl xlObjects) getUploadIDDir(bucket, object, uploadID string) string {
	return pathJoin(xl.getMultipartSHADir(bucket, object), uploadID)
}

// getUploadIDLockPath returns the name of the Lock in the form of
// bucket/object/uploadID. For locking, the path bucket/object/uploadID
// is locked instead of multipart-sha256-Dir/uploadID as it is more
// readable in the list-locks output which helps in debugging.
func (xl xlObjects) getUploadIDLockPath(bucket, object, uploadID string) string {
	return pathJoin(bucket, object, uploadID)
}

func (xl xlObjects) getMultipartSHADir(bucket, object string) string {
	return getSHA256Hash([]byte(pathJoin(bucket, object)))
}

// isUploadIDExists - verify if a given uploadID exists and is valid.
func (xl xlObjects) isUploadIDExists(ctx context.Context, bucket, object, uploadID string) bool {
	return xl.isObject(minioMetaMultipartBucket, xl.getUploadIDDir(bucket, object, uploadID))
}

// Removes part given by partName belonging to a mulitpart upload from minioMetaBucket
func (xl xlObjects) removeObjectPart(bucket, object, uploadID, partName string) {
	curpartPath := path.Join(bucket, object, uploadID, partName)
	wg := sync.WaitGroup{}
	for i, disk := range xl.getDisks() {
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
func (xl xlObjects) statPart(ctx context.Context, bucket, object, uploadID, partName string) (fileInfo FileInfo, err error) {
	var ignoredErrs []error
	partNamePath := path.Join(xl.getUploadIDDir(bucket, object, uploadID), partName)
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
		if IsErrIgnored(err, objMetadataOpIgnoredErrs...) {
			ignoredErrs = append(ignoredErrs, err)
			continue
		}
		// Error is not ignored, return right here.
		logger.LogIf(ctx, err)
		return FileInfo{}, err
	}
	// If all errors were ignored, reduce to maximal occurrence
	// based on the read quorum.
	readQuorum := len(xl.getDisks()) / 2
	return FileInfo{}, reduceReadQuorumErrs(ctx, ignoredErrs, nil, readQuorum)
}

// commitXLMetadata - commit `xl.json` from source prefix to destination prefix in the given slice of disks.
func commitXLMetadata(ctx context.Context, disks []StorageAPI, srcBucket, srcPrefix, dstBucket, dstPrefix string, quorum int) ([]StorageAPI, error) {
	var wg = &sync.WaitGroup{}
	var mErrs = make([]error, len(disks))

	srcJSONFile := path.Join(srcPrefix, xlMetaJSONFile)
	dstJSONFile := path.Join(dstPrefix, xlMetaJSONFile)

	// Rename `xl.json` to all disks in parallel.
	for index, disk := range disks {
		if disk == nil {
			mErrs[index] = errDiskNotFound
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
				logger.LogIf(ctx, rErr)
				mErrs[index] = rErr
				return
			}
			mErrs[index] = nil
		}(index, disk)
	}
	// Wait for all the routines.
	wg.Wait()

	err := reduceWriteQuorumErrs(ctx, mErrs, objectOpIgnoredErrs, quorum)
	if err == errXLWriteQuorum {
		// Delete all `xl.json` successfully renamed.
		deleteAllXLMetadata(ctx, disks, dstBucket, dstPrefix, mErrs)
	}
	return evalDisks(disks, mErrs), err
}

// ListMultipartUploads - lists all the pending multipart
// uploads for a particular object in a bucket.
//
// Implements minimal S3 compatible ListMultipartUploads API. We do
// not support prefix based listing, this is a deliberate attempt
// towards simplification of multipart APIs.
// The resulting ListMultipartsInfo structure is unmarshalled directly as XML.
func (xl xlObjects) ListMultipartUploads(ctx context.Context, bucket, object, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, e error) {
	if err := checkListMultipartArgs(ctx, bucket, object, keyMarker, uploadIDMarker, delimiter, xl); err != nil {
		return result, err
	}

	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = object
	result.Delimiter = delimiter

	for _, disk := range xl.getLoadBalancedDisks() {
		if disk == nil {
			continue
		}
		uploadIDs, err := disk.ListDir(minioMetaMultipartBucket, xl.getMultipartSHADir(bucket, object), -1)
		if err != nil {
			if err == errFileNotFound {
				return result, nil
			}
			logger.LogIf(ctx, err)
			return result, err
		}
		for i := range uploadIDs {
			uploadIDs[i] = strings.TrimSuffix(uploadIDs[i], slashSeparator)
		}
		sort.Strings(uploadIDs)
		for _, uploadID := range uploadIDs {
			if len(result.Uploads) == maxUploads {
				break
			}
			result.Uploads = append(result.Uploads, MultipartInfo{Object: object, UploadID: uploadID})
		}
		break
	}

	return result, nil
}

// newMultipartUpload - wrapper for initializing a new multipart
// request; returns a unique upload id.
//
// Internally this function creates 'uploads.json' associated for the
// incoming object at
// '.minio.sys/multipart/bucket/object/uploads.json' on all the
// disks. `uploads.json` carries metadata regarding on-going multipart
// operation(s) on the object.
func (xl xlObjects) newMultipartUpload(ctx context.Context, bucket string, object string, meta map[string]string) (string, error) {

	dataBlocks, parityBlocks := getRedundancyCount(meta[amzStorageClass], len(xl.getDisks()))

	xlMeta := newXLMetaV1(object, dataBlocks, parityBlocks)

	// we now know the number of blocks this object needs for data and parity.
	// establish the writeQuorum using this data
	writeQuorum := dataBlocks + 1

	if meta["content-type"] == "" {
		contentType := mimedb.TypeByExtension(path.Ext(object))
		meta["content-type"] = contentType
	}
	xlMeta.Stat.ModTime = UTCNow()
	xlMeta.Meta = meta

	uploadID := mustGetUUID()
	uploadIDPath := xl.getUploadIDDir(bucket, object, uploadID)
	tempUploadIDPath := uploadID

	// Write updated `xl.json` to all disks.
	disks, err := writeSameXLMetadata(ctx, xl.getDisks(), minioMetaTmpBucket, tempUploadIDPath, xlMeta, writeQuorum)
	if err != nil {
		return "", toObjectErr(err, minioMetaTmpBucket, tempUploadIDPath)
	}
	// delete the tmp path later in case we fail to rename (ignore
	// returned errors) - this will be a no-op in case of a rename
	// success.
	defer xl.deleteObject(ctx, minioMetaTmpBucket, tempUploadIDPath, writeQuorum, false)

	// Attempt to rename temp upload object to actual upload path object
	_, rErr := rename(ctx, disks, minioMetaTmpBucket, tempUploadIDPath, minioMetaMultipartBucket, uploadIDPath, true, writeQuorum, nil)
	if rErr != nil {
		return "", toObjectErr(rErr, minioMetaMultipartBucket, uploadIDPath)
	}

	// Return success.
	return uploadID, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a
// unique id. The unique id returned here is of UUID form, for each
// subsequent request each UUID is unique.
//
// Implements S3 compatible initiate multipart API.
func (xl xlObjects) NewMultipartUpload(ctx context.Context, bucket, object string, meta map[string]string, opts ObjectOptions) (string, error) {
	if err := checkNewMultipartArgs(ctx, bucket, object, xl); err != nil {
		return "", err
	}
	// No metadata is set, allocate a new one.
	if meta == nil {
		meta = make(map[string]string)
	}
	return xl.newMultipartUpload(ctx, bucket, object, meta)
}

// CopyObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to put object part operation but the source
// data is read from an existing object.
//
// Implements S3 compatible Upload Part Copy API.
func (xl xlObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (pi PartInfo, e error) {
	// Hold read locks on source object only if we are
	// going to read data from source object.
	objectSRLock := xl.nsMutex.NewNSLock(srcBucket, srcObject)
	if err := objectSRLock.GetRLock(globalObjectTimeout); err != nil {
		return pi, err
	}
	defer objectSRLock.RUnlock()

	if err := checkNewMultipartArgs(ctx, srcBucket, srcObject, xl); err != nil {
		return pi, err
	}

	partInfo, err := xl.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, NewPutObjReader(srcInfo.Reader, nil, nil), dstOpts)
	if err != nil {
		return pi, toObjectErr(err, dstBucket, dstObject)
	}

	// Success.
	return partInfo, nil
}

// PutObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to single put operation but it is part
// of the multipart transaction.
//
// Implements S3 compatible Upload Part API.
func (xl xlObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *PutObjReader, opts ObjectOptions) (pi PartInfo, e error) {
	data := r.Reader
	if err := checkPutObjectPartArgs(ctx, bucket, object, xl); err != nil {
		return pi, err
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument)
		return pi, toObjectErr(errInvalidArgument)
	}

	var partsMetadata []xlMetaV1
	var errs []error
	uploadIDPath := xl.getUploadIDDir(bucket, object, uploadID)
	uploadIDLockPath := xl.getUploadIDLockPath(bucket, object, uploadID)

	// pre-check upload id lock.
	preUploadIDLock := xl.nsMutex.NewNSLock(minioMetaMultipartBucket, uploadIDLockPath)
	if err := preUploadIDLock.GetRLock(globalOperationTimeout); err != nil {
		return pi, err
	}

	// Validates if upload ID exists.
	if !xl.isUploadIDExists(ctx, bucket, object, uploadID) {
		preUploadIDLock.RUnlock()
		return pi, InvalidUploadID{UploadID: uploadID}
	}

	// Read metadata associated with the object from all disks.
	partsMetadata, errs = readAllXLMetadata(ctx, xl.getDisks(), minioMetaMultipartBucket,
		uploadIDPath)

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(ctx, xl, partsMetadata, errs)
	if err != nil {
		preUploadIDLock.RUnlock()
		return pi, toObjectErr(err, bucket, object)
	}

	reducedErr := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errXLWriteQuorum {
		preUploadIDLock.RUnlock()
		return pi, toObjectErr(reducedErr, bucket, object)
	}
	preUploadIDLock.RUnlock()

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(xl.getDisks(), partsMetadata, errs)

	// Pick one from the first valid metadata.
	xlMeta, err := pickValidXLMeta(ctx, partsMetadata, modTime, writeQuorum)
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
	defer xl.deleteObject(ctx, minioMetaTmpBucket, tmpPart, writeQuorum, false)

	if data.Size() >= 0 {
		if pErr := xl.prepareFile(ctx, minioMetaTmpBucket, tmpPartPath, data.Size(),
			onlineDisks, xlMeta.Erasure.BlockSize, xlMeta.Erasure.DataBlocks, writeQuorum); pErr != nil {
			return pi, toObjectErr(pErr, bucket, object)

		}
	}

	erasure, err := NewErasure(ctx, xlMeta.Erasure.DataBlocks, xlMeta.Erasure.ParityBlocks, xlMeta.Erasure.BlockSize)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch size := data.Size(); {
	case size == 0:
		buffer = make([]byte, 1) // Allocate atleast a byte to reach EOF
	case size == -1 || size >= blockSizeV1:
		buffer = xl.bp.Get()
		defer xl.bp.Put(buffer)
	case size < blockSizeV1:
		// No need to allocate fully blockSizeV1 buffer if the incoming data is smaller.
		buffer = make([]byte, size, 2*size)
	}

	if len(buffer) > int(xlMeta.Erasure.BlockSize) {
		buffer = buffer[:xlMeta.Erasure.BlockSize]
	}

	writers := make([]*bitrotWriter, len(onlineDisks))
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}
		writers[i] = newBitrotWriter(disk, minioMetaTmpBucket, tmpPartPath, DefaultBitrotAlgorithm)
	}

	n, err := erasure.Encode(ctx, data, writers, buffer, erasure.dataBlocks+1)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if n < data.Size() {
		return pi, IncompleteBody{}
	}

	for i := range writers {
		if writers[i] == nil {
			onlineDisks[i] = nil
		}
	}

	// post-upload check (write) lock
	postUploadIDLock := xl.nsMutex.NewNSLock(minioMetaMultipartBucket, uploadIDLockPath)
	if err = postUploadIDLock.GetLock(globalOperationTimeout); err != nil {
		return pi, err
	}
	defer postUploadIDLock.Unlock()

	// Validate again if upload ID still exists.
	if !xl.isUploadIDExists(ctx, bucket, object, uploadID) {
		return pi, InvalidUploadID{UploadID: uploadID}
	}

	// Rename temporary part file to its final location.
	partPath := path.Join(uploadIDPath, partSuffix)
	onlineDisks, err = rename(ctx, onlineDisks, minioMetaTmpBucket, tmpPartPath, minioMetaMultipartBucket, partPath, false, writeQuorum, nil)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Read metadata again because it might be updated with parallel upload of another part.
	partsMetadata, errs = readAllXLMetadata(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath)
	reducedErr = reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errXLWriteQuorum {
		return pi, toObjectErr(reducedErr, bucket, object)
	}

	// Get current highest version based on re-read partsMetadata.
	onlineDisks, modTime = listOnlineDisks(onlineDisks, partsMetadata, errs)

	// Pick one from the first valid metadata.
	xlMeta, err = pickValidXLMeta(ctx, partsMetadata, modTime, writeQuorum)
	if err != nil {
		return pi, err
	}

	// Once part is successfully committed, proceed with updating XL metadata.
	xlMeta.Stat.ModTime = UTCNow()

	md5hex := r.MD5CurrentHexString()

	// Add the current part.
	xlMeta.AddObjectPart(partID, partSuffix, md5hex, n, data.ActualSize())

	for i, disk := range onlineDisks {
		if disk == OfflineDisk {
			continue
		}
		partsMetadata[i].Stat = xlMeta.Stat
		partsMetadata[i].Parts = xlMeta.Parts
		partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{partSuffix, DefaultBitrotAlgorithm, writers[i].Sum()})
	}

	// Write all the checksum metadata.
	newUUID := mustGetUUID()
	tempXLMetaPath := newUUID

	// Writes a unique `xl.json` each disk carrying new checksum related information.
	if onlineDisks, err = writeUniqueXLMetadata(ctx, onlineDisks, minioMetaTmpBucket, tempXLMetaPath, partsMetadata, writeQuorum); err != nil {
		return pi, toObjectErr(err, minioMetaTmpBucket, tempXLMetaPath)
	}

	if _, err = commitXLMetadata(ctx, onlineDisks, minioMetaTmpBucket, tempXLMetaPath, minioMetaMultipartBucket, uploadIDPath, writeQuorum); err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	fi, err := xl.statPart(ctx, bucket, object, uploadID, partSuffix)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partSuffix)
	}

	// Return success.
	return PartInfo{
		PartNumber:   partID,
		LastModified: fi.ModTime,
		ETag:         md5hex,
		Size:         fi.Size,
		ActualSize:   data.ActualSize(),
	}, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is marshaled directly into XML and
// replied back to the client.
func (xl xlObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int) (result ListPartsInfo, e error) {
	if err := checkListPartsArgs(ctx, bucket, object, xl); err != nil {
		return result, err
	}
	// Hold lock so that there is no competing
	// abort-multipart-upload or complete-multipart-upload.
	uploadIDLock := xl.nsMutex.NewNSLock(minioMetaMultipartBucket,
		xl.getUploadIDLockPath(bucket, object, uploadID))
	if err := uploadIDLock.GetLock(globalListingTimeout); err != nil {
		return result, err
	}
	defer uploadIDLock.Unlock()

	if !xl.isUploadIDExists(ctx, bucket, object, uploadID) {
		return result, InvalidUploadID{UploadID: uploadID}
	}

	uploadIDPath := xl.getUploadIDDir(bucket, object, uploadID)

	xlParts, xlMeta, err := xl.readXLMetaParts(ctx, minioMetaMultipartBucket, uploadIDPath)
	if err != nil {
		return result, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	// Populate the result stub.
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker
	result.UserDefined = xlMeta

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
		fi, err = xl.statPart(ctx, bucket, object, uploadID, part.Name)
		if err != nil {
			return result, toObjectErr(err, minioMetaBucket, path.Join(uploadID, part.Name))
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

// CompleteMultipartUpload - completes an ongoing multipart
// transaction after receiving all the parts indicated by the client.
// Returns an md5sum calculated by concatenating all the individual
// md5sums of all the parts.
//
// Implements S3 compatible Complete multipart API.
func (xl xlObjects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, parts []CompletePart, opts ObjectOptions) (oi ObjectInfo, e error) {
	if err := checkCompleteMultipartArgs(ctx, bucket, object, xl); err != nil {
		return oi, err
	}
	// Hold write lock on the object.
	destLock := xl.nsMutex.NewNSLock(bucket, object)
	if err := destLock.GetLock(globalObjectTimeout); err != nil {
		return oi, err
	}
	defer destLock.Unlock()

	uploadIDPath := xl.getUploadIDDir(bucket, object, uploadID)
	uploadIDLockPath := xl.getUploadIDLockPath(bucket, object, uploadID)

	// Hold lock so that
	//
	// 1) no one aborts this multipart upload
	//
	// 2) no one does a parallel complete-multipart-upload on this
	// multipart upload
	uploadIDLock := xl.nsMutex.NewNSLock(minioMetaMultipartBucket, uploadIDLockPath)
	if err := uploadIDLock.GetLock(globalOperationTimeout); err != nil {
		return oi, err
	}
	defer uploadIDLock.Unlock()

	if !xl.isUploadIDExists(ctx, bucket, object, uploadID) {
		return oi, InvalidUploadID{UploadID: uploadID}
	}

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
	if xl.parentDirIsObject(ctx, bucket, path.Dir(object)) {
		return oi, toObjectErr(errFileAccessDenied, bucket, object)
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5, err := getCompleteMultipartMD5(ctx, parts)
	if err != nil {
		return oi, err
	}

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllXLMetadata(ctx, xl.getDisks(), minioMetaMultipartBucket, uploadIDPath)

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(ctx, xl, partsMetadata, errs)
	if err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	reducedErr := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errXLWriteQuorum {
		return oi, toObjectErr(reducedErr, bucket, object)
	}

	onlineDisks, modTime := listOnlineDisks(xl.getDisks(), partsMetadata, errs)

	// Calculate full object size.
	var objectSize int64

	// Calculate consolidated actual size.
	var objectActualSize int64

	// Pick one from the first valid metadata.
	xlMeta, err := pickValidXLMeta(ctx, partsMetadata, modTime, writeQuorum)
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
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				GotETag:    part.ETag,
			}
			return oi, invp
		}

		if currentXLMeta.Parts[partIdx].ETag != part.ETag {
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				ExpETag:    currentXLMeta.Parts[partIdx].ETag,
				GotETag:    part.ETag,
			}
			return oi, invp
		}

		// All parts except the last part has to be atleast 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(currentXLMeta.Parts[partIdx].ActualSize) {
			return oi, PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   currentXLMeta.Parts[partIdx].ActualSize,
				PartETag:   part.ETag,
			}
		}

		// Save for total object size.
		objectSize += currentXLMeta.Parts[partIdx].Size

		// Save the consolidated actual size.
		objectActualSize += currentXLMeta.Parts[partIdx].ActualSize

		// Add incoming parts.
		xlMeta.Parts[i] = objectPartInfo{
			Number:     part.PartNumber,
			ETag:       part.ETag,
			Size:       currentXLMeta.Parts[partIdx].Size,
			Name:       fmt.Sprintf("part.%d", part.PartNumber),
			ActualSize: currentXLMeta.Parts[partIdx].ActualSize,
		}
	}

	// Save the final object size and modtime.
	xlMeta.Stat.Size = objectSize
	xlMeta.Stat.ModTime = UTCNow()

	// Save successfully calculated md5sum.
	xlMeta.Meta["etag"] = s3MD5

	// Save the consolidated actual size.
	xlMeta.Meta[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)

	tempUploadIDPath := uploadID

	// Update all xl metadata, make sure to not modify fields like
	// checksum which are different on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Stat = xlMeta.Stat
		partsMetadata[index].Meta = xlMeta.Meta
		partsMetadata[index].Parts = xlMeta.Parts
	}

	// Write unique `xl.json` for each disk.
	if onlineDisks, err = writeUniqueXLMetadata(ctx, onlineDisks, minioMetaTmpBucket, tempUploadIDPath, partsMetadata, writeQuorum); err != nil {
		return oi, toObjectErr(err, minioMetaTmpBucket, tempUploadIDPath)
	}

	var rErr error
	onlineDisks, rErr = commitXLMetadata(ctx, onlineDisks, minioMetaTmpBucket, tempUploadIDPath, minioMetaMultipartBucket, uploadIDPath, writeQuorum)
	if rErr != nil {
		return oi, toObjectErr(rErr, minioMetaMultipartBucket, uploadIDPath)
	}

	if xl.isObject(bucket, object) {
		// Deny if WORM is enabled
		if globalWORMEnabled {
			return ObjectInfo{}, ObjectAlreadyExists{Bucket: bucket, Object: object}
		}

		// Rename if an object already exists to temporary location.
		newUniqueID := mustGetUUID()

		// Delete success renamed object.
		defer xl.deleteObject(ctx, minioMetaTmpBucket, newUniqueID, writeQuorum, false)

		// NOTE: Do not use online disks slice here: the reason is that existing object should be purged
		// regardless of `xl.json` status and rolled back in case of errors. Also allow renaming of the
		// existing object if it is not present in quorum disks so users can overwrite stale objects.
		_, err = rename(ctx, xl.getDisks(), bucket, object, minioMetaTmpBucket, newUniqueID, true, writeQuorum, []error{errFileNotFound})
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
	if _, err = rename(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath, bucket, object, true, writeQuorum, nil); err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	// Success, return object info.
	return xlMeta.ToObjectInfo(bucket, object), nil
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
func (xl xlObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	if err := checkAbortMultipartArgs(ctx, bucket, object, xl); err != nil {
		return err
	}
	// Construct uploadIDPath.
	uploadIDPath := xl.getUploadIDDir(bucket, object, uploadID)
	uploadIDLockPath := xl.getUploadIDLockPath(bucket, object, uploadID)
	// Hold lock so that there is no competing
	// complete-multipart-upload or put-object-part.
	uploadIDLock := xl.nsMutex.NewNSLock(minioMetaMultipartBucket, uploadIDLockPath)
	if err := uploadIDLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer uploadIDLock.Unlock()

	if !xl.isUploadIDExists(ctx, bucket, object, uploadID) {
		return InvalidUploadID{UploadID: uploadID}
	}

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllXLMetadata(ctx, xl.getDisks(), minioMetaMultipartBucket, uploadIDPath)

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(ctx, xl, partsMetadata, errs)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Cleanup all uploaded parts.
	if err = xl.deleteObject(ctx, minioMetaMultipartBucket, uploadIDPath, writeQuorum, false); err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Successfully purged.
	return nil
}

// Clean-up the old multipart uploads. Should be run in a Go routine.
func (xl xlObjects) cleanupStaleMultipartUploads(ctx context.Context, cleanupInterval, expiry time.Duration, doneCh chan struct{}) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-doneCh:
			return
		case <-ticker.C:
			var disk StorageAPI
			for _, d := range xl.getLoadBalancedDisks() {
				if d != nil {
					disk = d
					break
				}
			}
			if disk == nil {
				continue
			}
			xl.cleanupStaleMultipartUploadsOnDisk(ctx, disk, expiry)
		}
	}
}

// Remove the old multipart uploads on the given disk.
func (xl xlObjects) cleanupStaleMultipartUploadsOnDisk(ctx context.Context, disk StorageAPI, expiry time.Duration) {
	now := time.Now()
	shaDirs, err := disk.ListDir(minioMetaMultipartBucket, "", -1)
	if err != nil {
		return
	}
	for _, shaDir := range shaDirs {
		uploadIDDirs, err := disk.ListDir(minioMetaMultipartBucket, shaDir, -1)
		if err != nil {
			continue
		}
		for _, uploadIDDir := range uploadIDDirs {
			uploadIDPath := pathJoin(shaDir, uploadIDDir)
			fi, err := disk.StatFile(minioMetaMultipartBucket, pathJoin(uploadIDPath, xlMetaJSONFile))
			if err != nil {
				continue
			}
			if now.Sub(fi.ModTime) > expiry {
				xl.deleteObject(ctx, minioMetaMultipartBucket, uploadIDPath, len(xl.getDisks())/2+1, false)
			}
		}
	}
}
