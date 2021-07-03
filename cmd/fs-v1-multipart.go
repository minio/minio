// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	pathutil "path"
	"sort"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/trie"
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
			// Skip part files whose name don't match expected format. These could be backend filesystem specific files.
			continue
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
		err = xioutil.AppendFile(file.filePath, partPath, globalFSOSync)
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

	// S3 spec says uploadIDs should be sorted based on initiated time. ModTime of fs.json
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
			UploadID:  strings.TrimSuffix(uploadID, SlashSeparator),
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
func (fs *FSObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (string, error) {
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
	fsMeta.Meta = opts.UserDefined

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

	if srcOpts.VersionID != "" && srcOpts.VersionID != nullVersionID {
		return pi, VersionNotFound{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	}

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
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return pi, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	data := r.Reader
	if err := checkPutObjectPartArgs(ctx, bucket, object, fs); err != nil {
		return pi, toObjectErr(err, bucket)
	}

	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return pi, toObjectErr(err, bucket)
	}

	// Validate input data size and it can never be less than -1.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument, logger.Application)
		return pi, toObjectErr(errInvalidArgument)
	}

	uploadIDDir := fs.getUploadIDDir(bucket, object, uploadID)

	// Just check if the uploadID exists to avoid copy if it doesn't.
	_, err := fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return pi, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return pi, toObjectErr(err, bucket, object)
	}

	tmpPartPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, uploadID+"."+mustGetUUID()+"."+strconv.Itoa(partID))
	bytesWritten, err := fsCreateFile(ctx, tmpPartPath, data, data.Size())

	// Delete temporary part in case of failure. If
	// PutObjectPart succeeds then there would be nothing to
	// delete in which case we just ignore the error.
	defer fsRemoveFile(ctx, tmpPartPath)

	if err != nil {
		return pi, toObjectErr(err, minioMetaTmpBucket, tmpPartPath)
	}

	// Should return IncompleteBody{} error when reader has fewer
	// bytes than specified in request header.
	if bytesWritten < data.Size() {
		return pi, IncompleteBody{Bucket: bucket, Object: object}
	}

	etag := r.MD5CurrentHexString()

	if etag == "" {
		etag = GenETag()
	}

	partPath := pathJoin(uploadIDDir, fs.encodePartFile(partID, etag, data.ActualSize()))

	// Make sure not to create parent directories if they don't exist - the upload might have been aborted.
	if err = fsSimpleRenameFile(ctx, tmpPartPath, partPath); err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return pi, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
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

// GetMultipartInfo returns multipart metadata uploaded during newMultipartUpload, used
// by callers to verify object states
// - encrypted
// - compressed
func (fs *FSObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	minfo := MultipartInfo{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}

	if err := checkListPartsArgs(ctx, bucket, object, fs); err != nil {
		return minfo, toObjectErr(err)
	}

	// Check if bucket exists
	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return minfo, toObjectErr(err, bucket)
	}

	uploadIDDir := fs.getUploadIDDir(bucket, object, uploadID)
	if _, err := fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile)); err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return minfo, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return minfo, toObjectErr(err, bucket, object)
	}

	fsMetaBytes, err := xioutil.ReadFile(pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		logger.LogIf(ctx, err)
		return minfo, toObjectErr(err, bucket, object)
	}

	var fsMeta fsMetaV1
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(fsMetaBytes, &fsMeta); err != nil {
		return minfo, toObjectErr(err, bucket, object)
	}

	minfo.UserDefined = fsMeta.Meta
	return minfo, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is unmarshalled directly into XML and
// replied back to the client.
func (fs *FSObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts ObjectOptions) (result ListPartsInfo, e error) {
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
			return result, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return result, toObjectErr(err, bucket, object)
	}

	entries, err := readDir(uploadIDDir)
	if err != nil {
		logger.LogIf(ctx, err)
		return result, toObjectErr(err, bucket)
	}

	partsMap := make(map[int]PartInfo)
	for _, entry := range entries {
		if entry == fs.metaJSONFile {
			continue
		}

		partNumber, currentEtag, actualSize, derr := fs.decodePartFile(entry)
		if derr != nil {
			// Skip part files whose name don't match expected format. These could be backend filesystem specific files.
			continue
		}

		entryStat, err := fsStatFile(ctx, pathJoin(uploadIDDir, entry))
		if err != nil {
			continue
		}

		currentMeta := PartInfo{
			PartNumber:   partNumber,
			ETag:         currentEtag,
			ActualSize:   actualSize,
			Size:         entryStat.Size(),
			LastModified: entryStat.ModTime(),
		}

		cachedMeta, ok := partsMap[partNumber]
		if !ok {
			partsMap[partNumber] = currentMeta
			continue
		}

		if currentMeta.LastModified.After(cachedMeta.LastModified) {
			partsMap[partNumber] = currentMeta
		}
	}

	var parts []PartInfo
	for _, partInfo := range partsMap {
		parts = append(parts, partInfo)
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

	rc, _, err := fsOpenFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile), 0)
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return result, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return result, toObjectErr(err, bucket, object)
	}
	defer rc.Close()

	fsMetaBytes, err := ioutil.ReadAll(rc)
	if err != nil {
		return result, toObjectErr(err, bucket, object)
	}

	var fsMeta fsMetaV1
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(fsMetaBytes, &fsMeta); err != nil {
		return result, err
	}

	result.UserDefined = fsMeta.Meta
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
		return oi, toObjectErr(errFileParentIsFile, bucket, object)
	}

	if _, err := fs.statBucketDir(ctx, bucket); err != nil {
		return oi, toObjectErr(err, bucket)
	}
	defer NSUpdated(bucket, object)

	uploadIDDir := fs.getUploadIDDir(bucket, object, uploadID)
	// Just check if the uploadID exists to avoid copy if it doesn't.
	_, err := fsStatFile(ctx, pathJoin(uploadIDDir, fs.metaJSONFile))
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return oi, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return oi, toObjectErr(err, bucket, object)
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5 := getCompleteMultipartMD5(parts)

	// ensure that part ETag is canonicalized to strip off extraneous quotes
	for i := range parts {
		parts[i].ETag = canonicalizeETag(parts[i].ETag)
	}

	fsMeta := fsMetaV1{}

	// Allocate parts similar to incoming slice.
	fsMeta.Parts = make([]ObjectPartInfo, len(parts))

	entries, err := readDir(uploadIDDir)
	if err != nil {
		logger.GetReqInfo(ctx).AppendTags("uploadIDDir", uploadIDDir)
		logger.LogIf(ctx, err)
		return oi, err
	}

	// Create entries trie structure for prefix match
	entriesTrie := trie.NewTrie()
	for _, entry := range entries {
		entriesTrie.Insert(entry)
	}

	// Save consolidated actual size.
	var objectActualSize int64
	// Validate all parts and then commit to disk.
	for i, part := range parts {
		partFile := getPartFile(entriesTrie, part.PartNumber, part.ETag)
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

		fsMeta.Parts[i] = ObjectPartInfo{
			Number:     part.PartNumber,
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
			partFile := getPartFile(entriesTrie, part.PartNumber, part.ETag)
			if partFile == "" {
				logger.LogIf(ctx, fmt.Errorf("%.5d.%s missing will not proceed",
					part.PartNumber, part.ETag))
				return oi, InvalidPart{
					PartNumber: part.PartNumber,
					GotETag:    part.ETag,
				}
			}
			if err = xioutil.AppendFile(appendFilePath, pathJoin(uploadIDDir, partFile), globalFSOSync); err != nil {
				logger.LogIf(ctx, err)
				return oi, toObjectErr(err)
			}
		}
	}

	// Hold write lock on the object.
	destLock := fs.NewNSLock(bucket, object)
	lkctx, err := destLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return oi, err
	}
	ctx = lkctx.Context()
	defer destLock.Unlock(lkctx.Cancel)

	bucketMetaDir := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix)
	fsMetaPath := pathJoin(bucketMetaDir, bucket, object, fs.metaJSONFile)
	metaFile, err := fs.rwPool.Write(fsMetaPath)
	var freshFile bool
	if err != nil {
		if !errors.Is(err, errFileNotFound) {
			logger.LogIf(ctx, err)
			return oi, toObjectErr(err, bucket, object)
		}
		metaFile, err = fs.rwPool.Create(fsMetaPath)
		if err != nil {
			logger.LogIf(ctx, err)
			return oi, toObjectErr(err, bucket, object)
		}
		freshFile = true
	}
	defer metaFile.Close()
	defer func() {
		// Remove meta file when CompleteMultipart encounters
		// any error and it is a fresh file.
		//
		// We should preserve the `fs.json` of any
		// existing object
		if e != nil && freshFile {
			tmpDir := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID)
			fsRemoveMeta(ctx, bucketMetaDir, fsMetaPath, tmpDir)
		}
	}()

	// Read saved fs metadata for ongoing multipart.
	fsMetaBuf, err := xioutil.ReadFile(pathJoin(uploadIDDir, fs.metaJSONFile))
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
	if fsMeta.Meta == nil {
		fsMeta.Meta = make(map[string]string)
	}
	fsMeta.Meta["etag"] = s3MD5
	// Save consolidated actual size.
	fsMeta.Meta[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)
	if _, err = fsMeta.WriteTo(metaFile); err != nil {
		logger.LogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}

	err = fsRenameFile(ctx, appendFilePath, pathJoin(fs.fsPath, bucket, object))
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}

	// Purge multipart folders
	{
		fsTmpObjPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, mustGetUUID())
		defer fsRemoveAll(ctx, fsTmpObjPath) // remove multipart temporary files in background.

		fsSimpleRenameFile(ctx, uploadIDDir, fsTmpObjPath)

		// It is safe to ignore any directory not empty error (in case there were multiple uploadIDs on the same object)
		fsRemoveDir(ctx, fs.getMultipartSHADir(bucket, object))
	}

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
func (fs *FSObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) error {
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
			return InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return toObjectErr(err, bucket, object)
	}

	// Purge multipart folders
	{
		fsTmpObjPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, mustGetUUID())
		defer fsRemoveAll(ctx, fsTmpObjPath) // remove multipart temporary files in background.

		fsSimpleRenameFile(ctx, uploadIDDir, fsTmpObjPath)

		// It is safe to ignore any directory not empty error (in case there were multiple uploadIDs on the same object)
		fsRemoveDir(ctx, fs.getMultipartSHADir(bucket, object))
	}

	return nil
}

// Removes multipart uploads if any older than `expiry` duration
// on all buckets for every `cleanupInterval`, this function is
// blocking and should be run in a go-routine.
func (fs *FSObjects) cleanupStaleUploads(ctx context.Context, cleanupInterval, expiry time.Duration) {
	timer := time.NewTimer(cleanupInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// Reset for the next interval
			timer.Reset(cleanupInterval)

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

				// Remove the trailing slash separator
				for i := range uploadIDs {
					uploadIDs[i] = strings.TrimSuffix(uploadIDs[i], SlashSeparator)
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

						// Remove uploadID from the append file map and its corresponding temporary file
						fs.appendFileMapMu.Lock()
						bgAppend, ok := fs.appendFileMap[uploadID]
						if ok {
							_ = fsRemoveFile(ctx, bgAppend.filePath)
							delete(fs.appendFileMap, uploadID)
						}
						fs.appendFileMapMu.Unlock()
					}
				}
			}
		}
	}
}
