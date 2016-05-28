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

// Checks whether bucket exists.
func (fs fsObjects) isBucketExist(bucket string) bool {
	// Check whether bucket exists.
	_, err := fs.storage.StatVol(bucket)
	if err != nil {
		if err == errVolumeNotFound {
			return false
		}
		errorIf(err, "Stat failed on bucket "+bucket+".")
		return false
	}
	return true
}

// newMultipartUploadCommon - initialize a new multipart, is a common function for both object layers.
func (fs fsObjects) newMultipartUploadCommon(bucket string, object string, meta map[string]string) (uploadID string, err error) {
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
	tempUploadIDPath := path.Join(tmpMetaPrefix, bucket, object, uploadID)
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

// Returns if the prefix is a multipart upload.
func (fs fsObjects) isMultipartUpload(bucket, prefix string) bool {
	_, err := fs.storage.StatFile(bucket, pathJoin(prefix, uploadsJSONFile))
	return err == nil
}

// listUploadsInfo - list all uploads info.
func (fs fsObjects) listUploadsInfo(prefixPath string) (uploads []uploadInfo, err error) {
	splitPrefixes := strings.SplitN(prefixPath, "/", 3)
	uploadIDs, err := readUploadsJSON(splitPrefixes[1], splitPrefixes[2], fs.storage)
	if err != nil {
		if err == errFileNotFound {
			return []uploadInfo{}, nil
		}
		return nil, err
	}
	uploads = uploadIDs.Uploads
	return uploads, nil
}

// listMultipartUploadsCommon - lists all multipart uploads, common function for both object layers.
func (fs fsObjects) listMultipartUploadsCommon(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	result := ListMultipartsInfo{}
	// Verify if bucket is valid.
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
			return result, InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			}
		}
		id, err := uuid.Parse(uploadIDMarker)
		if err != nil {
			return result, err
		}
		if id.IsZero() {
			return result, MalformedUploadID{
				UploadID: uploadIDMarker,
			}
		}
	}

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
		uploads, _, err = listMultipartUploadIDs(bucket, keyMarker, uploadIDMarker, maxUploads, fs.storage)
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
				if walkResult.err == errFileNotFound || walkResult.err == errDiskNotFound {
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
			tmpUploads, end, err = listMultipartUploadIDs(bucket, entry, uploadIDMarker, maxUploads, fs.storage)
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

// ListMultipartUploads - list multipart uploads.
func (fs fsObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	return fs.listMultipartUploadsCommon(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

// NewMultipartUpload - initialize a new multipart upload, returns a unique id.
func (fs fsObjects) NewMultipartUpload(bucket, object string, meta map[string]string) (string, error) {
	meta = make(map[string]string) // Reset the meta value, we are not going to save headers for fs.
	return fs.newMultipartUploadCommon(bucket, object, meta)
}

// putObjectPartCommon - put object part.
func (fs fsObjects) putObjectPartCommon(bucket string, object string, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, error) {
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
	if !fs.isUploadIDExists(bucket, object, uploadID) {
		return "", InvalidUploadID{UploadID: uploadID}
	}
	// Hold read lock on the uploadID so that no one aborts it.
	nsMutex.RLock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	defer nsMutex.RUnlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))

	// Hold write lock on the part so that there is no parallel upload on the part.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID, strconv.Itoa(partID)))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID, strconv.Itoa(partID)))

	partSuffix := fmt.Sprintf("object%d", partID)
	tmpPartPath := path.Join(tmpMetaPrefix, bucket, object, uploadID, partSuffix)

	// Initialize md5 writer.
	md5Writer := md5.New()

	var buf = make([]byte, blockSize)
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
		m, err := fs.storage.AppendFile(minioMetaBucket, tmpPartPath, buf[:n])
		if err != nil {
			return "", toObjectErr(err, bucket, object)
		}
		if m != int64(len(buf[:n])) {
			return "", toObjectErr(errUnexpected, bucket, object)
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}

	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	fsMeta, err := fs.readFSMetadata(minioMetaBucket, uploadIDPath)
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
	tempUploadIDPath := path.Join(tmpMetaPrefix, bucket, object, uploadID)
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

// PutObjectPart - writes the multipart upload chunks.
func (fs fsObjects) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, error) {
	return fs.putObjectPartCommon(bucket, object, uploadID, partID, size, data, md5Hex)
}

func (fs fsObjects) listObjectPartsCommon(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, error) {
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
	if !fs.isUploadIDExists(bucket, object, uploadID) {
		return ListPartsInfo{}, InvalidUploadID{UploadID: uploadID}
	}
	// Hold lock so that there is no competing abort-multipart-upload or complete-multipart-upload.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	result := ListPartsInfo{}

	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	fsMeta, err := fs.readFSMetadata(minioMetaBucket, uploadIDPath)
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

func (fs fsObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, error) {
	return fs.listObjectPartsCommon(bucket, object, uploadID, partNumberMarker, maxParts)
}

// isUploadIDExists - verify if a given uploadID exists and is valid.
func (fs fsObjects) isUploadIDExists(bucket, object, uploadID string) bool {
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	_, err := fs.storage.StatFile(minioMetaBucket, path.Join(uploadIDPath, fsMetaJSONFile))
	if err != nil {
		if err == errFileNotFound {
			return false
		}
		errorIf(err, "Unable to access upload id"+uploadIDPath)
		return false
	}
	return true
}

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
	if !fs.isUploadIDExists(bucket, object, uploadID) {
		return "", InvalidUploadID{UploadID: uploadID}
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5, err := completeMultipartMD5(parts...)
	if err != nil {
		return "", err
	}

	tempObj := path.Join(tmpMetaPrefix, bucket, object, uploadID, "object1")
	var buffer = make([]byte, blockSize)

	// Loop through all parts, validate them and then commit to disk.
	for i, part := range parts {
		// Construct part suffix.
		partSuffix := fmt.Sprintf("object%d", part.PartNumber)
		multipartPartFile := path.Join(mpartMetaPrefix, bucket, object, uploadID, partSuffix)
		var fi FileInfo
		fi, err = fs.storage.StatFile(minioMetaBucket, multipartPartFile)
		if err != nil {
			if err == errFileNotFound {
				return "", InvalidPart{}
			}
			return "", toObjectErr(err, minioMetaBucket, multipartPartFile)
		}
		// All parts except the last part has to be atleast 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(fi.Size) {
			return "", PartTooSmall{}
		}
		offset := int64(0)
		totalLeft := fi.Size
		for totalLeft > 0 {
			var n int64
			n, err = fs.storage.ReadFile(minioMetaBucket, multipartPartFile, offset, buffer)
			if err != nil {
				if err == errFileNotFound {
					return "", InvalidPart{}
				}
				return "", toObjectErr(err, minioMetaBucket, multipartPartFile)
			}
			n, err = fs.storage.AppendFile(minioMetaBucket, tempObj, buffer[:n])
			if err != nil {
				return "", toObjectErr(err, minioMetaBucket, tempObj)
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

	// Return md5sum.
	return s3MD5, nil
}

// abortMultipartUploadCommon - aborts a multipart upload, common
// function used by both object layers.
func (fs fsObjects) abortMultipartUploadCommon(bucket, object, uploadID string) error {
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
	if !fs.isUploadIDExists(bucket, object, uploadID) {
		return InvalidUploadID{UploadID: uploadID}
	}

	// Hold lock so that there is no competing complete-multipart-upload or put-object-part.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))

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
		if len(uploadsJSON.Uploads) > 0 {
			err = updateUploadsJSON(bucket, object, uploadsJSON, fs.storage)
			if err != nil {
				return toObjectErr(err, bucket, object)
			}
			return nil
		}
	}
	if err = fs.storage.DeleteFile(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)); err != nil {
		return toObjectErr(err, minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object))
	}
	return nil
}

// AbortMultipartUpload - aborts a multipart upload.
func (fs fsObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	return fs.abortMultipartUploadCommon(bucket, object, uploadID)
}
