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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	pathutil "path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/cmd/logger"
	mioutil "github.com/minio/minio/pkg/ioutil"
)

// Returns EXPORT/.minio.sys/multipart/SHA256/UPLOADID
func (fs *FSObjects) getUploadIDDir(bucket, object, uploadID string) string {
	return pathJoin(fs.fsPath, minioMetaMultipartBucket, getSHA256Hash([]byte(pathJoin(bucket, object))), uploadID)
}

// Returns EXPORT/.minio.sys/multipart/SHA256
func (fs *FSObjects) getMultipartSHADir(bucket, object string) string {
	return pathJoin(fs.fsPath, minioMetaMultipartBucket, getSHA256Hash([]byte(pathJoin(bucket, object))))
}

// Returns partNumber.etag
func (fs *FSObjects) encodePartFile(partNumber int, etag string, actualSize int64) string {
	return fmt.Sprintf("%.5d.%s.%d", partNumber, etag, actualSize)
}

// Returns partNumber and etag
func (fs *FSObjects) decodePartFile(name string) (partNumber int, etag string, actualSize int64, err error) {
	result := strings.Split(name, ".")
	if len(result) != 3 {
		return 0, "", 0, errUnexpected
	}
	partNumber, err = strconv.Atoi(result[0])
	if err != nil {
		return 0, "", 0, errUnexpected
	}
	actualSize, err = strconv.ParseInt(result[2], 10, 64)
	if err != nil {
		return 0, "", 0, errUnexpected
	}
	return partNumber, result[1], actualSize, nil
}

// Appends parts to an appendFile sequentially.
func (fs *FSObjects) backgroundAppend(ctx context.Context, bucket, object, uploadID string) {
	fs.appendFileMapMu.Lock()
	logger.GetReqInfo(ctx).AppendTags("uploadID", uploadID)
	file := fs.appendFileMap[uploadID]
	if file == nil {
		file = &fsAppendFile{
			filePath: pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, fmt.Sprintf("%s.%s", uploadID, mustGetUUID())),
		}
		fs.appendFileMap[uploadID] = file
	}
	fs.appendFileMapMu.Unlock()

	file.Lock()
	defer file.Unlock()

	// Since we append sequentially nextPartNumber will always be len(file.parts)+1
	nextPartNumber := len(file.parts) + 1
	uploadIDDir := fs.getUploadIDDir(bucket, object, uploadID)

	entries, err := readDir(uploadIDDir)
	if err != nil {
		logger.GetReqInfo(ctx).AppendTags("uploadIDDir", uploadIDDir)
		logger.LogIf(ctx, err)
		return
	}
	sort.Strings(entries)

	for _, entry := range entries {
		if entry == fs.metaJSONFile {
			continue
		}
		partNumber, etag, actualSize, err := fs.decodePartFile(entry)
		if err != nil {
			logger.GetReqInfo(ctx).AppendTags("entry", entry)
			logger.LogIf(ctx, err)
			return
		}
		if partNumber < nextPartNumber {
			// Part already appended.
			continue
		}
		if partNumber > nextPartNumber {
			// Required part number is not yet uploaded.
			return
		}

		partPath := pathJoin(uploadIDDir, entry)
		err = mioutil.AppendFile(file.filePath, partPath)
		if err != nil {
			reqInfo := logger.GetReqInfo(ctx).AppendTags("partPath", partPath)
			reqInfo.AppendTags("filepath", file.filePath)
			logger.LogIf(ctx, err)
			return
		}

		file.parts = append(file.parts, PartInfo{PartNumber: partNumber, ETag: etag, ActualSize: actualSize})
		nextPartNumber++
	}
}

// ListMultipartUploads - lists all the uploadIDs for the specified object.
// We do not support prefix based listing.
func (fs *FSObjects) ListMultipartUploads(ctx context.Context, bucket, object, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, e error) {
	if err := checkListMultipartArgs(ctx, bucket, object, keyMarker, uploadIDMarker, delimiter, fs); err != nil {
		return result, toObjectErr(err)
	}

	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return result, toObjectErr(err, bucket)
	}

	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = object
	result.Delimiter = delimiter
	result.NextKeyMarker = object
	result.UploadIDMarker = uploadIDMarker

	uploadIDs, err := readDir(fs.getMultipartSHADir(bucket, object))
	if err != nil {
		if err == errFileNotFound {
			result.IsTruncated = false
			return result, nil
		}
		logger.LogIf(ctx, err)
		return result, toObjectErr(err)
	}

	// S3 spec says uploaIDs should be sorted based on initiated time. ModTime of fs.json
	// is the creation time of the uploadID, hence we will use that.
	var uploads []MultipartInfo
	for _, uploadID := range uploadIDs {
		metaFilePath := pathJoin(fs.getMultipartSHADir(bucket, object), uploadID, fs.metaJSONFile)
		fi, err := fsStatFile(ctx, metaFilePath)
		if err != nil {
			return result, toObjectErr(err, bucket, object)
		}
		uploads = append(uploads, MultipartInfo{
			Object:    object,
			UploadID:  strings.TrimSuffix(uploadID, slashSeparator),
			Initiated: fi.ModTime(),
		})
	}
	sort.Slice(uploads, func(i int, j int) bool {
		return uploads[i].Initiated.Before(uploads[j].Initiated)
	})

	uploadIndex := 0
	if uploadIDMarker != "" {
		for uploadIndex < len(uploads) {
			if uploads[uploadIndex].UploadID != uploadIDMarker {
				uploadIndex++
				continue
			}
			if uploads[uploadIndex].UploadID == uploadIDMarker {
				uploadIndex++
				break
			}
			uploadIndex++
		}
	}
	for uploadIndex < len(uploads) {
		result.Uploads = append(result.Uploads, uploads[uploadIndex])
		result.NextUploadIDMarker = uploads[uploadIndex].UploadID
		uploadIndex++
		if len(result.Uploads) == maxUploads {
			break
		}
	}

	result.IsTruncated = uploadIndex < len(uploads)

	if !result.IsTruncated {
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
	}

	return result, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a
// unique id. The unique id returned here is of UUID form, for each
// subsequent request each UUID is unique.
//
// Implements S3 compatible initiate multipart API.
func (fs *FSObjects) NewMultipartUpload(ctx context.Context, bucket, object string, meta map[string]string, opts ObjectOptions) (string, error) {
	if err := checkNewMultipartArgs(ctx, bucket, object, fs); err != nil {
		return "", toObjectErr(err, bucket)
	}

	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return "", toObjectErr(err, bucket)
	}

	uploadID := mustGetUUID()
	uploadIDDir := fs.getUploadIDDir(bucket, object, uploadID)

	err := mkdirAll(uploadIDDir, 0755)
	if err != nil {
		logger.LogIf(ctx, err)
		return "", err
	}

	// Initialize fs.json values.
	fsMeta := newFSMetaV1()
	fsMeta.Meta = meta

	fsMetaBytes, err := json.Marshal(fsMeta)
	if err != nil {
		logger.LogIf(ctx, err)
		return "", err
	}

	if err = ioutil.WriteFile(pathJoin(uploadIDDir, fs.metaJSONFile), fsMetaBytes, 0644); err != nil {
		logger.LogIf(ctx, err)
		return "", err
	}

	return uploadID, nil
}

// CopyObjectPart - similar to PutObjectPart but reads data from an existing
// object. Internally incoming data is written to '.minio.sys/tmp' location
// and safely renamed to '.minio.sys/multipart' for reach parts.
func (fs *FSObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int,
	startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (pi PartInfo, e error) {

	if err := checkNewMultipartArgs(ctx, srcBucket, srcObject, fs); err != nil {
		return pi, toObjectErr(err)
	}

	partInfo, err := fs.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
	if err != nil {
		return pi, toObjectErr(err, dstBucket, dstObject)
	}

	return partInfo, nil
}

// PutObjectPart - reads incoming data until EOF for the part file on
// an ongoing multipart transaction. Internally incoming data is
// written to '.minio.sys/tmp' location and safely renamed to
// '.minio.sys/multipart' for reach parts.
func (fs *FSObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *PutObjReader, opts ObjectOptions) (pi PartInfo, e error) {
	data := r.Reader
	if err := checkPutObjectPartArgs(ctx, bucket, object, fs); err != nil {
		return pi, toObjectErr(err, bucket)
	}

	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return pi, toObjectErr(err, bucket)
	}

	// Validate input data size and it can never be less than -1.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument)
		return pi, toObjectErr(errInvalidArgument)
	}

	uploadIDDir := fs.getUploadIDDir(bucket, object, uploadID)

	// Just check if the uploadID exists to avoid copy if it doesn't.
	_, err := fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return pi, InvalidUploadID{UploadID: uploadID}
		}
		return pi, toObjectErr(err, bucket, object)
	}

	bufSize := int64(readSizeV1)
	if size := data.Size(); size > 0 && bufSize > size {
		bufSize = size
	}
	buf := make([]byte, bufSize)

	tmpPartPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, uploadID+"."+mustGetUUID()+"."+strconv.Itoa(partID))
	bytesWritten, err := fsCreateFile(ctx, tmpPartPath, data, buf, data.Size())
	if err != nil {
		fsRemoveFile(ctx, tmpPartPath)
		return pi, toObjectErr(err, minioMetaTmpBucket, tmpPartPath)
	}

	// Should return IncompleteBody{} error when reader has fewer
	// bytes than specified in request header.
	if bytesWritten < data.Size() {
		fsRemoveFile(ctx, tmpPartPath)
		return pi, IncompleteBody{}
	}

	// Delete temporary part in case of failure. If
	// PutObjectPart succeeds then there would be nothing to
	// delete in which case we just ignore the error.
	defer fsRemoveFile(ctx, tmpPartPath)
	etag := r.MD5CurrentHexString()

	if etag == "" {
		etag = GenETag()
	}

	partPath := pathJoin(uploadIDDir, fs.encodePartFile(partID, etag, data.ActualSize()))

	if err = fsRenameFile(ctx, tmpPartPath, partPath); err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	go fs.backgroundAppend(ctx, bucket, object, uploadID)

	fi, err := fsStatFile(ctx, partPath)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}
	return PartInfo{
		PartNumber:   partID,
		LastModified: fi.ModTime(),
		ETag:         etag,
		Size:         fi.Size(),
		ActualSize:   data.ActualSize(),
	}, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is unmarshalled directly into XML and
// replied back to the client.
func (fs *FSObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int) (result ListPartsInfo, e error) {
	if err := checkListPartsArgs(ctx, bucket, object, fs); err != nil {
		return result, toObjectErr(err)
	}
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker

	// Check if bucket exists
	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return result, toObjectErr(err, bucket)
	}

	uploadIDDir := fs.getUploadIDDir(bucket, object, uploadID)
	if _, err := fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile)); err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return result, InvalidUploadID{UploadID: uploadID}
		}
		return result, toObjectErr(err, bucket, object)
	}

	entries, err := readDir(uploadIDDir)
	if err != nil {
		logger.LogIf(ctx, err)
		return result, toObjectErr(err, bucket)
	}

	partsMap := make(map[int]string)
	for _, entry := range entries {
		if entry == fs.metaJSONFile {
			continue
		}
		partNumber, etag1, _, derr := fs.decodePartFile(entry)
		if derr != nil {
			logger.LogIf(ctx, derr)
			return result, toObjectErr(derr)
		}
		etag2, ok := partsMap[partNumber]
		if !ok {
			partsMap[partNumber] = etag1
			continue
		}
		stat1, serr := fsStatFile(ctx, pathJoin(uploadIDDir, getPartFile(entries, partNumber, etag1)))
		if serr != nil {
			return result, toObjectErr(serr)
		}
		stat2, serr := fsStatFile(ctx, pathJoin(uploadIDDir, getPartFile(entries, partNumber, etag2)))
		if serr != nil {
			return result, toObjectErr(serr)
		}
		if stat1.ModTime().After(stat2.ModTime()) {
			partsMap[partNumber] = etag1
		}
	}

	var parts []PartInfo
	var actualSize int64
	for partNumber, etag := range partsMap {
		partFile := getPartFile(entries, partNumber, etag)
		if partFile == "" {
			return result, InvalidPart{}
		}
		// Read the actualSize from the pathFileName.
		subParts := strings.Split(partFile, ".")
		actualSize, err = strconv.ParseInt(subParts[len(subParts)-1], 10, 64)
		if err != nil {
			return result, InvalidPart{}
		}
		parts = append(parts, PartInfo{PartNumber: partNumber, ETag: etag, ActualSize: actualSize})
	}
	sort.Slice(parts, func(i int, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})
	i := 0
	if partNumberMarker != 0 {
		// If the marker was set, skip the entries till the marker.
		for _, part := range parts {
			i++
			if part.PartNumber == partNumberMarker {
				break
			}
		}
	}

	partsCount := 0
	for partsCount < maxParts && i < len(parts) {
		result.Parts = append(result.Parts, parts[i])
		i++
		partsCount++
	}
	if i < len(parts) {
		result.IsTruncated = true
		if partsCount != 0 {
			result.NextPartNumberMarker = result.Parts[partsCount-1].PartNumber
		}
	}
	for i, part := range result.Parts {
		var stat os.FileInfo
		stat, err = fsStatFile(ctx, pathJoin(uploadIDDir, fs.encodePartFile(part.PartNumber, part.ETag, part.ActualSize)))
		if err != nil {
			return result, toObjectErr(err)
		}
		result.Parts[i].LastModified = stat.ModTime()
		result.Parts[i].Size = part.ActualSize
	}

	fsMetaBytes, err := ioutil.ReadFile(pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		logger.LogIf(ctx, err)
		return result, err
	}

	result.UserDefined = parseFSMetaMap(fsMetaBytes)
	return result, nil
}

// CompleteMultipartUpload - completes an ongoing multipart
// transaction after receiving all the parts indicated by the client.
// Returns an md5sum calculated by concatenating all the individual
// md5sums of all the parts.
//
// Implements S3 compatible Complete multipart API.
func (fs *FSObjects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, parts []CompletePart, opts ObjectOptions) (oi ObjectInfo, e error) {

	var actualSize int64

	if err := checkCompleteMultipartArgs(ctx, bucket, object, fs); err != nil {
		return oi, toObjectErr(err)
	}

	// Check if an object is present as one of the parent dir.
	if fs.parentDirIsObject(ctx, bucket, pathutil.Dir(object)) {
		return oi, toObjectErr(errFileAccessDenied, bucket, object)
	}

	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return oi, toObjectErr(err, bucket)
	}

	uploadIDDir := fs.getUploadIDDir(bucket, object, uploadID)
	// Just check if the uploadID exists to avoid copy if it doesn't.
	_, err := fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return oi, InvalidUploadID{UploadID: uploadID}
		}
		return oi, toObjectErr(err, bucket, object)
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5, err := getCompleteMultipartMD5(ctx, parts)
	if err != nil {
		return oi, err
	}

	partSize := int64(-1) // Used later to ensure that all parts sizes are same.

	fsMeta := fsMetaV1{}

	// Allocate parts similar to incoming slice.
	fsMeta.Parts = make([]objectPartInfo, len(parts))

	entries, err := readDir(uploadIDDir)
	if err != nil {
		logger.GetReqInfo(ctx).AppendTags("uploadIDDir", uploadIDDir)
		logger.LogIf(ctx, err)
		return oi, err
	}

	// Save consolidated actual size.
	var objectActualSize int64
	// Validate all parts and then commit to disk.
	for i, part := range parts {
		partFile := getPartFile(entries, part.PartNumber, part.ETag)
		if partFile == "" {
			return oi, InvalidPart{
				PartNumber: part.PartNumber,
				GotETag:    part.ETag,
			}
		}

		// Read the actualSize from the pathFileName.
		subParts := strings.Split(partFile, ".")
		actualSize, err = strconv.ParseInt(subParts[len(subParts)-1], 10, 64)
		if err != nil {
			return oi, InvalidPart{
				PartNumber: part.PartNumber,
				GotETag:    part.ETag,
			}
		}

		partPath := pathJoin(uploadIDDir, partFile)

		var fi os.FileInfo
		fi, err = fsStatFile(ctx, partPath)
		if err != nil {
			if err == errFileNotFound || err == errFileAccessDenied {
				return oi, InvalidPart{}
			}
			return oi, err
		}
		if partSize == -1 {
			partSize = actualSize
		}

		fsMeta.Parts[i] = objectPartInfo{
			Number:     part.PartNumber,
			ETag:       part.ETag,
			Size:       fi.Size(),
			ActualSize: actualSize,
		}

		// Consolidate the actual size.
		objectActualSize += actualSize

		if i == len(parts)-1 {
			break
		}

		// All parts except the last part has to be atleast 5MB.
		if !isMinAllowedPartSize(actualSize) {
			return oi, PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   actualSize,
				PartETag:   part.ETag,
			}
		}
	}

	appendFallback := true // In case background-append did not append the required parts.
	appendFilePath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, fmt.Sprintf("%s.%s", uploadID, mustGetUUID()))

	// Most of the times appendFile would already be fully appended by now. We call fs.backgroundAppend()
	// to take care of the following corner case:
	// 1. The last PutObjectPart triggers go-routine fs.backgroundAppend, this go-routine has not started yet.
	// 2. Now CompleteMultipartUpload gets called which sees that lastPart is not appended and starts appending
	//    from the beginning
	fs.backgroundAppend(ctx, bucket, object, uploadID)

	fs.appendFileMapMu.Lock()
	file := fs.appendFileMap[uploadID]
	delete(fs.appendFileMap, uploadID)
	fs.appendFileMapMu.Unlock()

	if file != nil {
		file.Lock()
		defer file.Unlock()
		// Verify that appendFile has all the parts.
		if len(file.parts) == len(parts) {
			for i := range parts {
				if parts[i].ETag != file.parts[i].ETag {
					break
				}
				if parts[i].PartNumber != file.parts[i].PartNumber {
					break
				}
				if i == len(parts)-1 {
					appendFilePath = file.filePath
					appendFallback = false
				}
			}
		}
	}

	if appendFallback {
		if file != nil {
			fsRemoveFile(ctx, file.filePath)
		}
		for _, part := range parts {
			partPath := getPartFile(entries, part.PartNumber, part.ETag)
			if err = mioutil.AppendFile(appendFilePath, pathJoin(uploadIDDir, partPath)); err != nil {
				logger.LogIf(ctx, err)
				return oi, toObjectErr(err)
			}
		}
	}

	// Hold write lock on the object.
	destLock := fs.nsMutex.NewNSLock(bucket, object)
	if err = destLock.GetLock(globalObjectTimeout); err != nil {
		return oi, err
	}
	defer destLock.Unlock()
	fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fs.metaJSONFile)
	metaFile, err := fs.rwPool.Create(fsMetaPath)
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}
	defer metaFile.Close()

	// Read saved fs metadata for ongoing multipart.
	fsMetaBuf, err := ioutil.ReadFile(pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}
	err = json.Unmarshal(fsMetaBuf, &fsMeta)
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}
	// Save additional metadata.
	if len(fsMeta.Meta) == 0 {
		fsMeta.Meta = make(map[string]string)
	}
	fsMeta.Meta["etag"] = s3MD5
	// Save consolidated actual size.
	fsMeta.Meta[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)
	if _, err = fsMeta.WriteTo(metaFile); err != nil {
		logger.LogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		if _, err = fsStatFile(ctx, pathJoin(fs.fsPath, bucket, object)); err == nil {
			return ObjectInfo{}, ObjectAlreadyExists{Bucket: bucket, Object: object}
		}
	}

	err = fsRenameFile(ctx, appendFilePath, pathJoin(fs.fsPath, bucket, object))
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}
	fsRemoveAll(ctx, uploadIDDir)
	// It is safe to ignore any directory not empty error (in case there were multiple uploadIDs on the same object)
	fsRemoveDir(ctx, fs.getMultipartSHADir(bucket, object))
	fi, err := fsStatFile(ctx, pathJoin(fs.fsPath, bucket, object))
	if err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

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
func (fs *FSObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	if err := checkAbortMultipartArgs(ctx, bucket, object, fs); err != nil {
		return err
	}

	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return toObjectErr(err, bucket)
	}

	fs.appendFileMapMu.Lock()
	// Remove file in tmp folder
	file := fs.appendFileMap[uploadID]
	if file != nil {
		fsRemoveFile(ctx, file.filePath)
	}
	delete(fs.appendFileMap, uploadID)
	fs.appendFileMapMu.Unlock()

	uploadIDDir := fs.getUploadIDDir(bucket, object, uploadID)
	// Just check if the uploadID exists to avoid copy if it doesn't.
	_, err := fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return InvalidUploadID{UploadID: uploadID}
		}
		return toObjectErr(err, bucket, object)
	}

	// Ignore the error returned as Windows fails to remove directory if a file in it
	// is Open()ed by the backgroundAppend()
	fsRemoveAll(ctx, uploadIDDir)

	// It is safe to ignore any directory not empty error (in case there were multiple uploadIDs on the same object)
	fsRemoveDir(ctx, fs.getMultipartSHADir(bucket, object))

	return nil
}

// Removes multipart uploads if any older than `expiry` duration
// on all buckets for every `cleanupInterval`, this function is
// blocking and should be run in a go-routine.
func (fs *FSObjects) cleanupStaleMultipartUploads(ctx context.Context, cleanupInterval, expiry time.Duration, doneCh chan struct{}) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-doneCh:
			return
		case <-ticker.C:
			now := time.Now()
			entries, err := readDir(pathJoin(fs.fsPath, minioMetaMultipartBucket))
			if err != nil {
				continue
			}
			for _, entry := range entries {
				uploadIDs, err := readDir(pathJoin(fs.fsPath, minioMetaMultipartBucket, entry))
				if err != nil {
					continue
				}
				for _, uploadID := range uploadIDs {
					fi, err := fsStatDir(ctx, pathJoin(fs.fsPath, minioMetaMultipartBucket, entry, uploadID))
					if err != nil {
						continue
					}
					if now.Sub(fi.ModTime()) > expiry {
						fsRemoveAll(ctx, pathJoin(fs.fsPath, minioMetaMultipartBucket, entry, uploadID))
						// It is safe to ignore any directory not empty error (in case there were multiple uploadIDs on the same object)
						fsRemoveDir(ctx, pathJoin(fs.fsPath, minioMetaMultipartBucket, entry))

					}
				}
			}
		}
	}
}
