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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/skyrings/skyring-common/tools/uuid"
)

// listMultipartUploads - lists all multipart uploads.
func (fs fsObjects) listMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
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
	if uploadIDMarker != "" {
		// generates random string on setting MINIO_DEBUG=lock, else returns empty string.
		// used for instrumentation on locks.
		opsID := getOpsID()
		nsMutex.RLock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, keyMarker), opsID)
		uploads, _, err = listMultipartUploadIDs(bucket, keyMarker, uploadIDMarker, maxUploads, fs.storage)
		nsMutex.RUnlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, keyMarker), opsID)
		if err != nil {
			return ListMultipartsInfo{}, err
		}
		maxUploads = maxUploads - len(uploads)
	}
	var walkResultCh chan treeWalkResult
	var endWalkCh chan struct{}
	heal := false // true only for xl.ListObjectsHeal()
	if maxUploads > 0 {
		walkResultCh, endWalkCh = fs.listPool.Release(listParams{minioMetaBucket, recursive, multipartMarkerPath, multipartPrefixPath, heal})
		if walkResultCh == nil {
			endWalkCh = make(chan struct{})
			isLeaf := fs.isMultipartUpload
			listDir := listDirFactory(isLeaf, fsTreeWalkIgnoredErrs, fs.storage)
			walkResultCh = startTreeWalk(minioMetaBucket, multipartPrefixPath, multipartMarkerPath, recursive, listDir, isLeaf, endWalkCh)
		}
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
				if isErrIgnored(walkResult.err, fsTreeWalkIgnoredErrs) {
					eof = true
					break
				}
				return ListMultipartsInfo{}, walkResult.err
			}
			entry := strings.TrimPrefix(walkResult.entry, retainSlash(pathJoin(mpartMetaPrefix, bucket)))
			if strings.HasSuffix(walkResult.entry, slashSeparator) {
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

			// generates random string on setting MINIO_DEBUG=lock, else returns empty string.
			// used for instrumentation on locks.
			opsID := getOpsID()

			nsMutex.RLock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, entry), opsID)
			tmpUploads, end, err = listMultipartUploadIDs(bucket, entry, uploadIDMarker, maxUploads, fs.storage)
			nsMutex.RUnlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, entry), opsID)
			if err != nil {
				return ListMultipartsInfo{}, err
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
		if strings.HasSuffix(upload.Object, slashSeparator) {
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
func (fs fsObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	// Validate input arguments.
	if !IsValidBucketName(bucket) {
		return ListMultipartsInfo{}, traceError(BucketNameInvalid{Bucket: bucket})
	}
	if !fs.isBucketExist(bucket) {
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
			return ListMultipartsInfo{}, traceError(InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			})
		}
		id, err := uuid.Parse(uploadIDMarker)
		if err != nil {
			return ListMultipartsInfo{}, traceError(err)
		}
		if id.IsZero() {
			return ListMultipartsInfo{}, traceError(MalformedUploadID{
				UploadID: uploadIDMarker,
			})
		}
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

	// Save additional metadata only if extended headers such as "X-Amz-Meta-" are set.
	if hasExtendedHeader(meta) {
		fsMeta.Meta = meta
	}

	// generates random string on setting MINIO_DEBUG=lock, else returns empty string.
	// used for instrumentation on locks.
	opsID := getOpsID()

	// This lock needs to be held for any changes to the directory contents of ".minio.sys/multipart/object/"
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object), opsID)
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object), opsID)

	uploadID = getUUID()
	initiated := time.Now().UTC()
	// Create 'uploads.json'
	if err = fs.writeUploadJSON(bucket, object, uploadID, initiated); err != nil {
		return "", err
	}
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	if err = writeFSMetadata(fs.storage, minioMetaBucket, path.Join(uploadIDPath, fsMetaJSONFile), fsMeta); err != nil {
		return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
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
	// Verify if bucket name is valid.
	if !IsValidBucketName(bucket) {
		return "", traceError(BucketNameInvalid{Bucket: bucket})
	}
	// Verify whether the bucket exists.
	if !fs.isBucketExist(bucket) {
		return "", traceError(BucketNotFound{Bucket: bucket})
	}
	// Verify if object name is valid.
	if !IsValidObjectName(object) {
		return "", traceError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
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

// Returns metadata path for the file holding info about the parts that
// have been appended to the "append-file"
func getFSAppendMetaPath(uploadID string) string {
	return path.Join(tmpMetaPrefix, uploadID+".json")
}

// Returns path for the append-file.
func getFSAppendDataPath(uploadID string) string {
	return path.Join(tmpMetaPrefix, uploadID+".data")
}

// Append parts to fsAppendDataFile.
func appendParts(disk StorageAPI, bucket, object, uploadID, opsID string) {
	cleanupAppendPaths := func() {
		// In case of any error, cleanup the append data and json files
		// from the tmp so that we do not have any inconsistent append
		// data/json files.
		disk.DeleteFile(bucket, getFSAppendDataPath(uploadID))
		disk.DeleteFile(bucket, getFSAppendMetaPath(uploadID))
	}
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	// fs-append.json path
	fsAppendMetaPath := getFSAppendMetaPath(uploadID)
	// fs.json path
	fsMetaPath := path.Join(mpartMetaPrefix, bucket, object, uploadID, fsMetaJSONFile)

	// Lock the uploadID so that no one modifies fs.json
	nsMutex.RLock(minioMetaBucket, uploadIDPath, opsID)
	fsMeta, err := readFSMetadata(disk, minioMetaBucket, fsMetaPath)
	nsMutex.RUnlock(minioMetaBucket, uploadIDPath, opsID)
	if err != nil {
		return
	}

	// Lock fs-append.json so that there is no parallel append to the file.
	nsMutex.Lock(minioMetaBucket, fsAppendMetaPath, opsID)
	defer nsMutex.Unlock(minioMetaBucket, fsAppendMetaPath, opsID)

	fsAppendMeta, err := readFSMetadata(disk, minioMetaBucket, fsAppendMetaPath)
	if err != nil {
		if errorCause(err) != errFileNotFound {
			cleanupAppendPaths()
			return
		}
		fsAppendMeta = fsMeta
		fsAppendMeta.Parts = nil
	}

	// Check if a part needs to be appended to
	part, appendNeeded := partToAppend(fsMeta, fsAppendMeta)
	if !appendNeeded {
		return
	}
	// Hold write lock on the part so that there is no parallel upload on the part.
	partPath := pathJoin(mpartMetaPrefix, bucket, object, uploadID, strconv.Itoa(part.Number))
	nsMutex.Lock(minioMetaBucket, partPath, opsID)
	defer nsMutex.Unlock(minioMetaBucket, partPath, opsID)

	// Proceed to append "part"
	fsAppendDataPath := getFSAppendDataPath(uploadID)
	// Path to the part that needs to be appended.
	partPath = path.Join(mpartMetaPrefix, bucket, object, uploadID, part.Name)
	offset := int64(0)
	totalLeft := part.Size
	buf := make([]byte, readSizeV1)
	for totalLeft > 0 {
		curLeft := int64(readSizeV1)
		if totalLeft < readSizeV1 {
			curLeft = totalLeft
		}
		var n int64
		n, err = disk.ReadFile(minioMetaBucket, partPath, offset, buf[:curLeft])
		if n > 0 {
			if err = disk.AppendFile(minioMetaBucket, fsAppendDataPath, buf[:n]); err != nil {
				cleanupAppendPaths()
				return
			}
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			cleanupAppendPaths()
			return
		}
		offset += n
		totalLeft -= n
	}
	fsAppendMeta.AddObjectPart(part.Number, part.Name, part.ETag, part.Size)
	// Overwrite previous fs-append.json
	if err = writeFSMetadata(disk, minioMetaBucket, fsAppendMetaPath, fsAppendMeta); err != nil {
		cleanupAppendPaths()
		return
	}
	// If there are more parts that need to be appended to fsAppendDataFile
	_, appendNeeded = partToAppend(fsMeta, fsAppendMeta)
	if appendNeeded {
		go appendParts(disk, bucket, object, uploadID, opsID)
	}
}

// PutObjectPart - reads incoming data until EOF for the part file on
// an ongoing multipart transaction. Internally incoming data is
// written to '.minio.sys/tmp' location and safely renamed to
// '.minio.sys/multipart' for reach parts.
func (fs fsObjects) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", traceError(BucketNameInvalid{Bucket: bucket})
	}
	// Verify whether the bucket exists.
	if !fs.isBucketExist(bucket) {
		return "", traceError(BucketNotFound{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", traceError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)

	// generates random string on setting MINIO_DEBUG=lock, else returns empty string.
	// used for instrumentation on locks.
	opsID := getOpsID()

	nsMutex.RLock(minioMetaBucket, uploadIDPath, opsID)
	// Just check if the uploadID exists to avoid copy if it doesn't.
	uploadIDExists := fs.isUploadIDExists(bucket, object, uploadID)
	nsMutex.RUnlock(minioMetaBucket, uploadIDPath, opsID)
	if !uploadIDExists {
		return "", traceError(InvalidUploadID{UploadID: uploadID})
	}

	partSuffix := fmt.Sprintf("object%d", partID)
	tmpPartPath := path.Join(tmpMetaPrefix, uploadID+"."+getUUID()+"."+partSuffix)

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Limit the reader to its provided size if specified.
	var limitDataReader io.Reader
	if size > 0 {
		// This is done so that we can avoid erroneous clients sending more data than the set content size.
		limitDataReader = io.LimitReader(data, size)
	} else {
		// else we read till EOF.
		limitDataReader = data
	}

	teeReader := io.TeeReader(limitDataReader, md5Writer)
	bufSize := int64(readSizeV1)
	if size > 0 && bufSize > size {
		bufSize = size
	}
	buf := make([]byte, int(bufSize))
	bytesWritten, cErr := fsCreateFile(fs.storage, teeReader, buf, minioMetaBucket, tmpPartPath)
	if cErr != nil {
		fs.storage.DeleteFile(minioMetaBucket, tmpPartPath)
		return "", toObjectErr(cErr, minioMetaBucket, tmpPartPath)
	}
	// Should return IncompleteBody{} error when reader has fewer
	// bytes than specified in request header.
	if bytesWritten < size {
		fs.storage.DeleteFile(minioMetaBucket, tmpPartPath)
		return "", traceError(IncompleteBody{})
	}

	// Validate if payload is valid.
	if isSignVerify(data) {
		if err := data.(*signVerifyReader).Verify(); err != nil {
			// Incoming payload wrong, delete the temporary object.
			fs.storage.DeleteFile(minioMetaBucket, tmpPartPath)
			// Error return.
			return "", toObjectErr(traceError(err), bucket, object)
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			// MD5 mismatch, delete the temporary object.
			fs.storage.DeleteFile(minioMetaBucket, tmpPartPath)
			// Returns md5 mismatch.
			return "", traceError(BadDigest{md5Hex, newMD5Hex})
		}
	}

	// generates random string on setting MINIO_DEBUG=lock, else returns empty string.
	// used for instrumentation on locks.
	opsID = getOpsID()

	// Hold write lock as we are updating fs.json
	nsMutex.Lock(minioMetaBucket, uploadIDPath, opsID)
	defer nsMutex.Unlock(minioMetaBucket, uploadIDPath, opsID)

	// Just check if the uploadID exists to avoid copy if it doesn't.
	if !fs.isUploadIDExists(bucket, object, uploadID) {
		return "", traceError(InvalidUploadID{UploadID: uploadID})
	}

	fsMetaPath := pathJoin(uploadIDPath, fsMetaJSONFile)
	fsMeta, err := readFSMetadata(fs.storage, minioMetaBucket, fsMetaPath)
	if err != nil {
		return "", toObjectErr(err, minioMetaBucket, fsMetaPath)
	}
	fsMeta.AddObjectPart(partID, partSuffix, newMD5Hex, size)

	partPath := path.Join(mpartMetaPrefix, bucket, object, uploadID, partSuffix)
	err = fs.storage.RenameFile(minioMetaBucket, tmpPartPath, minioMetaBucket, partPath)
	if err != nil {
		if dErr := fs.storage.DeleteFile(minioMetaBucket, tmpPartPath); dErr != nil {
			return "", toObjectErr(traceError(dErr), minioMetaBucket, tmpPartPath)
		}
		return "", toObjectErr(traceError(err), minioMetaBucket, partPath)
	}
	uploadIDPath = path.Join(mpartMetaPrefix, bucket, object, uploadID)
	if err = writeFSMetadata(fs.storage, minioMetaBucket, path.Join(uploadIDPath, fsMetaJSONFile), fsMeta); err != nil {
		return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
	}
	go appendParts(fs.storage, bucket, object, uploadID, opsID)
	return newMD5Hex, nil
}

// listObjectParts - wrapper scanning through
// '.minio.sys/multipart/bucket/object/UPLOADID'. Lists all the parts
// saved inside '.minio.sys/multipart/bucket/object/UPLOADID'.
func (fs fsObjects) listObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, error) {
	result := ListPartsInfo{}

	fsMetaPath := path.Join(mpartMetaPrefix, bucket, object, uploadID, fsMetaJSONFile)
	fsMeta, err := readFSMetadata(fs.storage, minioMetaBucket, fsMetaPath)
	if err != nil {
		return ListPartsInfo{}, toObjectErr(err, minioMetaBucket, fsMetaPath)
	}
	// Only parts with higher part numbers will be listed.
	partIdx := fsMeta.ObjectPartIndex(partNumberMarker)
	parts := fsMeta.Parts
	if partIdx != -1 {
		parts = fsMeta.Parts[partIdx+1:]
	}
	count := maxParts
	for _, part := range parts {
		var fi FileInfo
		partNamePath := path.Join(mpartMetaPrefix, bucket, object, uploadID, part.Name)
		fi, err = fs.storage.StatFile(minioMetaBucket, partNamePath)
		if err != nil {
			return ListPartsInfo{}, toObjectErr(traceError(err), minioMetaBucket, partNamePath)
		}
		result.Parts = append(result.Parts, partInfo{
			PartNumber:   part.Number,
			ETag:         part.ETag,
			LastModified: fi.ModTime,
			Size:         fi.Size,
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
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	return result, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is unmarshalled directly into XML and
// replied back to the client.
func (fs fsObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListPartsInfo{}, traceError(BucketNameInvalid{Bucket: bucket})
	}
	// Verify whether the bucket exists.
	if !fs.isBucketExist(bucket) {
		return ListPartsInfo{}, traceError(BucketNotFound{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return ListPartsInfo{}, traceError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// generates random string on setting MINIO_DEBUG=lock, else returns empty string.
	// used for instrumentation on locks.
	opsID := getOpsID()

	// Hold lock so that there is no competing abort-multipart-upload or complete-multipart-upload.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID), opsID)
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID), opsID)

	if !fs.isUploadIDExists(bucket, object, uploadID) {
		return ListPartsInfo{}, traceError(InvalidUploadID{UploadID: uploadID})
	}
	return fs.listObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
}

// CompleteMultipartUpload - completes an ongoing multipart
// transaction after receiving all the parts indicated by the client.
// Returns an md5sum calculated by concatenating all the individual
// md5sums of all the parts.
//
// Implements S3 compatible Complete multipart API.
func (fs fsObjects) CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", traceError(BucketNameInvalid{Bucket: bucket})
	}
	// Verify whether the bucket exists.
	if !fs.isBucketExist(bucket) {
		return "", traceError(BucketNotFound{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", traceError(ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		})
	}

	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	// generates random string on setting MINIO_DEBUG=lock, else returns empty string.
	// used for instrumentation on locks.
	opsID := getOpsID()

	// Hold lock so that
	// 1) no one aborts this multipart upload
	// 2) no one does a parallel complete-multipart-upload on this
	// multipart upload
	nsMutex.Lock(minioMetaBucket, uploadIDPath, opsID)
	defer nsMutex.Unlock(minioMetaBucket, uploadIDPath, opsID)

	if !fs.isUploadIDExists(bucket, object, uploadID) {
		return "", traceError(InvalidUploadID{UploadID: uploadID})
	}

	// fs-append.json path
	fsAppendMetaPath := getFSAppendMetaPath(uploadID)
	// Lock fs-append.json so that no parallel appendParts() is being done.
	nsMutex.Lock(minioMetaBucket, fsAppendMetaPath, opsID)
	defer nsMutex.Unlock(minioMetaBucket, fsAppendMetaPath, opsID)

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5, err := completeMultipartMD5(parts...)
	if err != nil {
		return "", err
	}

	// Read saved fs metadata for ongoing multipart.
	fsMetaPath := pathJoin(uploadIDPath, fsMetaJSONFile)
	fsMeta, err := readFSMetadata(fs.storage, minioMetaBucket, fsMetaPath)
	if err != nil {
		return "", toObjectErr(err, minioMetaBucket, fsMetaPath)
	}

	fsAppendMeta, err := readFSMetadata(fs.storage, minioMetaBucket, fsAppendMetaPath)
	if err == nil && isPartsSame(fsAppendMeta.Parts, parts) {
		fsAppendDataPath := getFSAppendDataPath(uploadID)
		if err = fs.storage.RenameFile(minioMetaBucket, fsAppendDataPath, bucket, object); err != nil {
			return "", toObjectErr(traceError(err), minioMetaBucket, fsAppendDataPath)
		}
		// Remove the append-file metadata file in tmp location as we no longer need it.
		fs.storage.DeleteFile(minioMetaBucket, fsAppendMetaPath)
	} else {
		tempObj := path.Join(tmpMetaPrefix, uploadID+"-"+"part.1")

		// Allocate staging buffer.
		var buf = make([]byte, readSizeV1)

		// Loop through all parts, validate them and then commit to disk.
		for i, part := range parts {
			partIdx := fsMeta.ObjectPartIndex(part.PartNumber)
			if partIdx == -1 {
				return "", traceError(InvalidPart{})
			}
			if fsMeta.Parts[partIdx].ETag != part.ETag {
				return "", traceError(BadDigest{})
			}
			// All parts except the last part has to be atleast 5MB.
			if (i < len(parts)-1) && !isMinAllowedPartSize(fsMeta.Parts[partIdx].Size) {
				return "", traceError(PartTooSmall{
					PartNumber: part.PartNumber,
					PartSize:   fsMeta.Parts[partIdx].Size,
					PartETag:   part.ETag,
				})
			}
			// Construct part suffix.
			partSuffix := fmt.Sprintf("object%d", part.PartNumber)
			multipartPartFile := path.Join(mpartMetaPrefix, bucket, object, uploadID, partSuffix)
			offset := int64(0)
			totalLeft := fsMeta.Parts[partIdx].Size
			for totalLeft > 0 {
				curLeft := int64(readSizeV1)
				if totalLeft < readSizeV1 {
					curLeft = totalLeft
				}
				var n int64
				n, err = fs.storage.ReadFile(minioMetaBucket, multipartPartFile, offset, buf[:curLeft])
				if n > 0 {
					if err = fs.storage.AppendFile(minioMetaBucket, tempObj, buf[:n]); err != nil {
						return "", toObjectErr(traceError(err), minioMetaBucket, tempObj)
					}
				}
				if err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						break
					}
					if err == errFileNotFound {
						return "", traceError(InvalidPart{})
					}
					return "", toObjectErr(traceError(err), minioMetaBucket, multipartPartFile)
				}
				offset += n
				totalLeft -= n
			}
		}

		// Rename the file back to original location, if not delete the temporary object.
		err = fs.storage.RenameFile(minioMetaBucket, tempObj, bucket, object)
		if err != nil {
			if dErr := fs.storage.DeleteFile(minioMetaBucket, tempObj); dErr != nil {
				return "", toObjectErr(traceError(dErr), minioMetaBucket, tempObj)
			}
			return "", toObjectErr(traceError(err), bucket, object)
		}
	}

	// No need to save part info, since we have concatenated all parts.
	fsMeta.Parts = nil

	// Save additional metadata only if extended headers such as "X-Amz-Meta-" are set.
	if hasExtendedHeader(fsMeta.Meta) {
		if len(fsMeta.Meta) == 0 {
			fsMeta.Meta = make(map[string]string)
		}
		fsMeta.Meta["md5Sum"] = s3MD5

		fsMetaPath := path.Join(bucketMetaPrefix, bucket, object, fsMetaJSONFile)
		// Write the metadata to a temp file and rename it to the actual location.
		if err = writeFSMetadata(fs.storage, minioMetaBucket, fsMetaPath, fsMeta); err != nil {
			return "", toObjectErr(err, bucket, object)
		}
	}

	// Cleanup all the parts if everything else has been safely committed.
	if err = cleanupUploadedParts(bucket, object, uploadID, fs.storage); err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// generates random string on setting MINIO_DEBUG=lock, else returns empty string.
	// used for instrumentation on locks.
	opsID = getOpsID()

	// Hold the lock so that two parallel complete-multipart-uploads do not
	// leave a stale uploads.json behind.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object), opsID)
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object), opsID)

	// Validate if there are other incomplete upload-id's present for
	// the object, if yes do not attempt to delete 'uploads.json'.
	uploadsJSON, err := readUploadsJSON(bucket, object, fs.storage)
	if err != nil {
		return "", toObjectErr(err, minioMetaBucket, object)
	}
	// If we have successfully read `uploads.json`, then we proceed to
	// purge or update `uploads.json`.
	uploadIDIdx := uploadsJSON.Index(uploadID)
	if uploadIDIdx != -1 {
		uploadsJSON.Uploads = append(uploadsJSON.Uploads[:uploadIDIdx], uploadsJSON.Uploads[uploadIDIdx+1:]...)
	}
	if len(uploadsJSON.Uploads) > 0 {
		if err = fs.updateUploadsJSON(bucket, object, uploadsJSON); err != nil {
			return "", toObjectErr(err, minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object))
		}
		// Return success.
		return s3MD5, nil
	}

	if err = fs.storage.DeleteFile(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)); err != nil {
		return "", toObjectErr(traceError(err), minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object))
	}

	// Return md5sum.
	return s3MD5, nil
}

// abortMultipartUpload - wrapper for purging an ongoing multipart
// transaction, deletes uploadID entry from `uploads.json` and purges
// the directory at '.minio.sys/multipart/bucket/object/uploadID' holding
// all the upload parts.
func (fs fsObjects) abortMultipartUpload(bucket, object, uploadID string) error {
	// Cleanup all uploaded parts.
	if err := cleanupUploadedParts(bucket, object, uploadID, fs.storage); err != nil {
		return err
	}

	// Validate if there are other incomplete upload-id's present for
	// the object, if yes do not attempt to delete 'uploads.json'.
	uploadsJSON, err := readUploadsJSON(bucket, object, fs.storage)
	if err == nil {
		uploadIDIdx := uploadsJSON.Index(uploadID)
		if uploadIDIdx != -1 {
			uploadsJSON.Uploads = append(uploadsJSON.Uploads[:uploadIDIdx], uploadsJSON.Uploads[uploadIDIdx+1:]...)
		}
		// There are pending uploads for the same object, preserve
		// them update 'uploads.json' in-place.
		if len(uploadsJSON.Uploads) > 0 {
			err = fs.updateUploadsJSON(bucket, object, uploadsJSON)
			if err != nil {
				return toObjectErr(err, bucket, object)
			}
			return nil
		}
	} // No more pending uploads for the object, we purge the entire
	// entry at '.minio.sys/multipart/bucket/object'.
	if err = fs.storage.DeleteFile(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)); err != nil {
		return toObjectErr(traceError(err), minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object))
	}
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
// no affect and further requests to the same uploadID would not be
// honored.
func (fs fsObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return traceError(BucketNameInvalid{Bucket: bucket})
	}
	if !fs.isBucketExist(bucket) {
		return traceError(BucketNotFound{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return traceError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// generates random string on setting MINIO_DEBUG=lock, else returns empty string.
	// used for instrumentation on locks.
	opsID := getOpsID()

	// Hold lock so that there is no competing complete-multipart-upload or put-object-part.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID), opsID)
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID), opsID)

	if !fs.isUploadIDExists(bucket, object, uploadID) {
		return traceError(InvalidUploadID{UploadID: uploadID})
	}

	fsAppendMetaPath := getFSAppendMetaPath(uploadID)
	// Lock fs-append.json so that no parallel appendParts() is being done.
	nsMutex.Lock(minioMetaBucket, fsAppendMetaPath, opsID)
	defer nsMutex.Unlock(minioMetaBucket, fsAppendMetaPath, opsID)

	err := fs.abortMultipartUpload(bucket, object, uploadID)
	return err
}
