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

package main

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
		nsMutex.RLock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, keyMarker))
		uploads, _, err = listMultipartUploadIDs(bucket, keyMarker, uploadIDMarker, maxUploads, fs.storage)
		nsMutex.RUnlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, keyMarker))
		if err != nil {
			return ListMultipartsInfo{}, err
		}
		maxUploads = maxUploads - len(uploads)
	}
	if maxUploads > 0 {
		walker := fs.lookupTreeWalk(listParams{minioMetaBucket, recursive, multipartMarkerPath, multipartPrefixPath})
		if walker == nil {
			walker = fs.startTreeWalk(minioMetaBucket, multipartPrefixPath, multipartMarkerPath, recursive, func(bucket, object string) bool {
				return fs.isMultipartUpload(bucket, object)
			})
		}
		for maxUploads > 0 {
			walkResult, ok := <-walker.ch
			if !ok {
				// Closed channel.
				eof = true
				break
			}
			// For any walk error return right away.
			if walkResult.err != nil {
				// File not found or Disk not found is a valid case.
				if walkResult.err == errFileNotFound || walkResult.err == errDiskNotFound || walkResult.err == errFaultyDisk {
					eof = true
					break
				}
				return ListMultipartsInfo{}, err
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
			nsMutex.RLock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, entry))
			tmpUploads, end, err = listMultipartUploadIDs(bucket, entry, uploadIDMarker, maxUploads, fs.storage)
			nsMutex.RUnlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, entry))
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
		return ListMultipartsInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	if !fs.isBucketExist(bucket) {
		return ListMultipartsInfo{}, BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectPrefix(prefix) {
		return ListMultipartsInfo{}, ObjectNameInvalid{Bucket: bucket, Object: prefix}
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return ListMultipartsInfo{}, UnsupportedDelimiter{
			Delimiter: delimiter,
		}
	}
	// Verify if marker has prefix.
	if keyMarker != "" && !strings.HasPrefix(keyMarker, prefix) {
		return ListMultipartsInfo{}, InvalidMarkerPrefixCombination{
			Marker: keyMarker,
			Prefix: prefix,
		}
	}
	if uploadIDMarker != "" {
		if strings.HasSuffix(keyMarker, slashSeparator) {
			return ListMultipartsInfo{}, InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			}
		}
		id, err := uuid.Parse(uploadIDMarker)
		if err != nil {
			return ListMultipartsInfo{}, err
		}
		if id.IsZero() {
			return ListMultipartsInfo{}, MalformedUploadID{
				UploadID: uploadIDMarker,
			}
		}
	}
	return fs.listMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// newMultipartUpload - wrapper for initializing a new multipart
// request, returns back a unique upload id.
//
// Internally this function creates 'uploads.json' associated for the
// incoming object at '.minio/multipart/bucket/object/uploads.json' on
// all the disks. `uploads.json` carries metadata regarding on going
// multipart operation on the object.
func (fs fsObjects) newMultipartUpload(bucket string, object string, meta map[string]string) (uploadID string, err error) {
	// No metadata is set, allocate a new one.
	if meta == nil {
		meta = make(map[string]string)
	}

	// Initialize `fs.json` values.
	fsMeta := newFSMetaV1()

	// This lock needs to be held for any changes to the directory contents of ".minio/multipart/object/"
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object))

	uploadID = getUUID()
	initiated := time.Now().UTC()
	// Create 'uploads.json'
	if err = writeUploadJSON(bucket, object, uploadID, initiated, fs.storage); err != nil {
		return "", err
	}
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	tempUploadIDPath := path.Join(tmpMetaPrefix, uploadID)
	if err = fs.writeFSMetadata(minioMetaBucket, tempUploadIDPath, fsMeta); err != nil {
		return "", toObjectErr(err, minioMetaBucket, tempUploadIDPath)
	}
	err = fs.storage.RenameFile(minioMetaBucket, path.Join(tempUploadIDPath, fsMetaJSONFile), minioMetaBucket, path.Join(uploadIDPath, fsMetaJSONFile))
	if err != nil {
		if dErr := fs.storage.DeleteFile(minioMetaBucket, path.Join(tempUploadIDPath, fsMetaJSONFile)); dErr != nil {
			return "", toObjectErr(dErr, minioMetaBucket, tempUploadIDPath)
		}
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
	meta = make(map[string]string) // Reset the meta value, we are not going to save headers for fs.
	// Verify if bucket name is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify whether the bucket exists.
	if !fs.isBucketExist(bucket) {
		return "", BucketNotFound{Bucket: bucket}
	}
	// Verify if object name is valid.
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	return fs.newMultipartUpload(bucket, object, meta)
}

// PutObjectPart - reads incoming data until EOF for the part file on
// an ongoing multipart transaction. Internally incoming data is
// written to '.minio/tmp' location and safely renamed to
// '.minio/multipart' for reach parts.
func (fs fsObjects) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify whether the bucket exists.
	if !fs.isBucketExist(bucket) {
		return "", BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)

	nsMutex.RLock(minioMetaBucket, uploadIDPath)
	// Just check if the uploadID exists to avoid copy if it doesn't.
	uploadIDExists := fs.isUploadIDExists(bucket, object, uploadID)
	nsMutex.RUnlock(minioMetaBucket, uploadIDPath)
	if !uploadIDExists {
		return "", InvalidUploadID{UploadID: uploadID}
	}

	// Hold write lock on the part so that there is no parallel upload on the part.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID, strconv.Itoa(partID)))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID, strconv.Itoa(partID)))

	partSuffix := fmt.Sprintf("object%d", partID)
	tmpPartPath := path.Join(tmpMetaPrefix, uploadID, partSuffix)

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Allocate 32KiB buffer for staging buffer.
	var buf = make([]byte, 128*1024)
	for {
		n, err := io.ReadFull(data, buf)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return "", toObjectErr(err, bucket, object)
		}
		// Update md5 writer.
		md5Writer.Write(buf[:n])
		if err = fs.storage.AppendFile(minioMetaBucket, tmpPartPath, buf[:n]); err != nil {
			return "", toObjectErr(err, bucket, object)
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}

	// Hold write lock as we are updating fs.json
	nsMutex.Lock(minioMetaBucket, uploadIDPath)
	defer nsMutex.Unlock(minioMetaBucket, uploadIDPath)

	// Just check if the uploadID exists to avoid copy if it doesn't.
	if !fs.isUploadIDExists(bucket, object, uploadID) {
		return "", InvalidUploadID{UploadID: uploadID}
	}

	fsMeta, err := readFSMetadata(fs.storage, minioMetaBucket, uploadIDPath)
	if err != nil {
		return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
	}
	fsMeta.AddObjectPart(partID, partSuffix, newMD5Hex, size)

	partPath := path.Join(mpartMetaPrefix, bucket, object, uploadID, partSuffix)
	err = fs.storage.RenameFile(minioMetaBucket, tmpPartPath, minioMetaBucket, partPath)
	if err != nil {
		if dErr := fs.storage.DeleteFile(minioMetaBucket, tmpPartPath); dErr != nil {
			return "", toObjectErr(dErr, minioMetaBucket, tmpPartPath)
		}
		return "", toObjectErr(err, minioMetaBucket, partPath)
	}
	uploadIDPath = path.Join(mpartMetaPrefix, bucket, object, uploadID)
	tempUploadIDPath := path.Join(tmpMetaPrefix, uploadID)
	if err = fs.writeFSMetadata(minioMetaBucket, tempUploadIDPath, fsMeta); err != nil {
		return "", toObjectErr(err, minioMetaBucket, tempUploadIDPath)
	}
	err = fs.storage.RenameFile(minioMetaBucket, path.Join(tempUploadIDPath, fsMetaJSONFile), minioMetaBucket, path.Join(uploadIDPath, fsMetaJSONFile))
	if err != nil {
		if dErr := fs.storage.DeleteFile(minioMetaBucket, path.Join(tempUploadIDPath, fsMetaJSONFile)); dErr != nil {
			return "", toObjectErr(dErr, minioMetaBucket, tempUploadIDPath)
		}
		return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
	}
	return newMD5Hex, nil
}

// listObjectParts - wrapper scanning through
// '.minio/multipart/bucket/object/UPLOADID'. Lists all the parts
// saved inside '.minio/multipart/bucket/object/UPLOADID'.
func (fs fsObjects) listObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, error) {
	result := ListPartsInfo{}

	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	fsMeta, err := readFSMetadata(fs.storage, minioMetaBucket, uploadIDPath)
	if err != nil {
		return ListPartsInfo{}, toObjectErr(err, minioMetaBucket, uploadIDPath)
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
			return ListPartsInfo{}, toObjectErr(err, minioMetaBucket, partNamePath)
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
		return ListPartsInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	// Verify whether the bucket exists.
	if !fs.isBucketExist(bucket) {
		return ListPartsInfo{}, BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ListPartsInfo{}, ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	// Hold lock so that there is no competing abort-multipart-upload or complete-multipart-upload.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))

	if !fs.isUploadIDExists(bucket, object, uploadID) {
		return ListPartsInfo{}, InvalidUploadID{UploadID: uploadID}
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
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify whether the bucket exists.
	if !fs.isBucketExist(bucket) {
		return "", BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}

	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	// Hold lock so that
	// 1) no one aborts this multipart upload
	// 2) no one does a parallel complete-multipart-upload on this
	// multipart upload
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))

	if !fs.isUploadIDExists(bucket, object, uploadID) {
		return "", InvalidUploadID{UploadID: uploadID}
	}

	// Read saved fs metadata for ongoing multipart.
	fsMeta, err := readFSMetadata(fs.storage, minioMetaBucket, uploadIDPath)
	if err != nil {
		return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5, err := completeMultipartMD5(parts...)
	if err != nil {
		return "", err
	}

	tempObj := path.Join(tmpMetaPrefix, uploadID, "object1")

	// Allocate 32KiB buffer for staging buffer.
	var buf = make([]byte, 128*1024)

	// Loop through all parts, validate them and then commit to disk.
	for i, part := range parts {
		partIdx := fsMeta.ObjectPartIndex(part.PartNumber)
		if partIdx == -1 {
			return "", InvalidPart{}
		}
		if fsMeta.Parts[partIdx].ETag != part.ETag {
			return "", BadDigest{}
		}
		// All parts except the last part has to be atleast 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(fsMeta.Parts[partIdx].Size) {
			return "", PartTooSmall{}
		}
		// Construct part suffix.
		partSuffix := fmt.Sprintf("object%d", part.PartNumber)
		multipartPartFile := path.Join(mpartMetaPrefix, bucket, object, uploadID, partSuffix)
		offset := int64(0)
		totalLeft := fsMeta.Parts[partIdx].Size
		for totalLeft > 0 {
			var n int64
			n, err = fs.storage.ReadFile(minioMetaBucket, multipartPartFile, offset, buf)
			if n > 0 {
				if err = fs.storage.AppendFile(minioMetaBucket, tempObj, buf[:n]); err != nil {
					return "", toObjectErr(err, minioMetaBucket, tempObj)
				}
			}
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				if err == errFileNotFound {
					return "", InvalidPart{}
				}
				return "", toObjectErr(err, minioMetaBucket, multipartPartFile)
			}
			offset += n
			totalLeft -= n
		}
	}

	// Rename the file back to original location, if not delete the temporary object.
	err = fs.storage.RenameFile(minioMetaBucket, tempObj, bucket, object)
	if err != nil {
		if dErr := fs.storage.DeleteFile(minioMetaBucket, tempObj); dErr != nil {
			return "", toObjectErr(dErr, minioMetaBucket, tempObj)
		}
		return "", toObjectErr(err, bucket, object)
	}

	// Cleanup all the parts if everything else has been safely committed.
	if err = cleanupUploadedParts(bucket, object, uploadID, fs.storage); err != nil {
		return "", err
	}

	// Hold the lock so that two parallel complete-multipart-uploads do not
	// leave a stale uploads.json behind.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object))

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
		if err = updateUploadsJSON(bucket, object, uploadsJSON, fs.storage); err != nil {
			return "", toObjectErr(err, minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object))
		}
		// Return success.
		return s3MD5, nil
	}

	if err = fs.storage.DeleteFile(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)); err != nil {
		return "", toObjectErr(err, minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object))
	}

	// Return md5sum.
	return s3MD5, nil
}

// abortMultipartUpload - wrapper for purging an ongoing multipart
// transaction, deletes uploadID entry from `uploads.json` and purges
// the directory at '.minio/multipart/bucket/object/uploadID' holding
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
			err = updateUploadsJSON(bucket, object, uploadsJSON, fs.storage)
			if err != nil {
				return toObjectErr(err, bucket, object)
			}
			return nil
		}
	} // No more pending uploads for the object, we purge the entire
	// entry at '.minio/multipart/bucket/object'.
	if err = fs.storage.DeleteFile(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)); err != nil {
		return toObjectErr(err, minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object))
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
		return BucketNameInvalid{Bucket: bucket}
	}
	if !fs.isBucketExist(bucket) {
		return BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	// Hold lock so that there is no competing complete-multipart-upload or put-object-part.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))

	if !fs.isUploadIDExists(bucket, object, uploadID) {
		return InvalidUploadID{UploadID: uploadID}
	}

	err := fs.abortMultipartUpload(bucket, object, uploadID)
	return err
}
