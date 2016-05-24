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
	"io/ioutil"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/pkg/mimedb"
)

// ListMultipartUploads - list multipart uploads.
func (xl xlObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	return xl.listMultipartUploadsCommon(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
}

/// Common multipart object layer functions.

// newMultipartUploadCommon - initialize a new multipart, is a common function for both object layers.
func (xl xlObjects) newMultipartUploadCommon(bucket string, object string, meta map[string]string) (uploadID string, err error) {
	// Verify if bucket name is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify whether the bucket exists.
	if !xl.isBucketExist(bucket) {
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

	xlMeta := xlMetaV1{}
	// If not set default to "application/octet-stream"
	if meta["content-type"] == "" {
		contentType := "application/octet-stream"
		if objectExt := filepath.Ext(object); objectExt != "" {
			content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]
			if ok {
				contentType = content.ContentType
			}
		}
		meta["content-type"] = contentType
	}
	xlMeta.Meta = meta

	// This lock needs to be held for any changes to the directory contents of ".minio/multipart/object/"
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object))

	uploadID = getUUID()
	initiated := time.Now().UTC()
	// Create 'uploads.json'
	if err = writeUploadJSON(bucket, object, uploadID, initiated, xl.storageDisks...); err != nil {
		return "", err
	}
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	tempUploadIDPath := path.Join(tmpMetaPrefix, bucket, object, uploadID)
	if err = xl.writeXLMetadata(minioMetaBucket, tempUploadIDPath, xlMeta); err != nil {
		return "", toObjectErr(err, minioMetaBucket, tempUploadIDPath)
	}
	if err = xl.renameXLMetadata(minioMetaBucket, tempUploadIDPath, minioMetaBucket, uploadIDPath); err != nil {
		if dErr := xl.deleteXLMetadata(minioMetaBucket, tempUploadIDPath); dErr != nil {
			return "", toObjectErr(dErr, minioMetaBucket, tempUploadIDPath)
		}
		return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
	}
	// Return success.
	return uploadID, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a unique id.
func (xl xlObjects) NewMultipartUpload(bucket, object string, meta map[string]string) (string, error) {
	return xl.newMultipartUploadCommon(bucket, object, meta)
}

// putObjectPartCommon - put object part.
func (xl xlObjects) putObjectPartCommon(bucket string, object string, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify whether the bucket exists.
	if !xl.isBucketExist(bucket) {
		return "", BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if !xl.isUploadIDExists(bucket, object, uploadID) {
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
	fileWriter, err := xl.erasureDisk.CreateFile(minioMetaBucket, tmpPartPath)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Instantiate a new multi writer.
	multiWriter := io.MultiWriter(md5Writer, fileWriter)

	// Instantiate checksum hashers and create a multiwriter.
	if size > 0 {
		if _, err = io.CopyN(multiWriter, data, size); err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", toObjectErr(clErr, bucket, object)
			}
			return "", toObjectErr(err, bucket, object)
		}
		// Reader shouldn't have more data what mentioned in size argument.
		// reading one more byte from the reader to validate it.
		// expected to fail, success validates existence of more data in the reader.
		if _, err = io.CopyN(ioutil.Discard, data, 1); err == nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", toObjectErr(clErr, bucket, object)
			}
			return "", UnExpectedDataSize{Size: int(size)}
		}
	} else {
		var n int64
		if n, err = io.Copy(multiWriter, data); err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", toObjectErr(clErr, bucket, object)
			}
			return "", toObjectErr(err, bucket, object)
		}
		size = n
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", toObjectErr(clErr, bucket, object)
			}
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}
	err = fileWriter.Close()
	if err != nil {
		if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
			return "", toObjectErr(clErr, bucket, object)
		}
		return "", err
	}

	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	xlMeta, err := readXLMetadata(xl.getRandomDisk(), minioMetaBucket, uploadIDPath)
	if err != nil {
		return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
	}
	xlMeta.AddObjectPart(partSuffix, newMD5Hex, size)

	partPath := path.Join(mpartMetaPrefix, bucket, object, uploadID, partSuffix)
	err = xl.renameObject(minioMetaBucket, tmpPartPath, minioMetaBucket, partPath)
	if err != nil {
		if dErr := xl.deleteObject(minioMetaBucket, tmpPartPath); dErr != nil {
			return "", toObjectErr(dErr, minioMetaBucket, tmpPartPath)
		}
		return "", toObjectErr(err, minioMetaBucket, partPath)
	}
	if err = xl.writeXLMetadata(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadID), xlMeta); err != nil {
		return "", toObjectErr(err, minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadID))
	}
	return newMD5Hex, nil
}

// PutObjectPart - writes the multipart upload chunks.
func (xl xlObjects) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, error) {
	return xl.putObjectPartCommon(bucket, object, uploadID, partID, size, data, md5Hex)
}

// ListObjectParts - list object parts, common function across both object layers.
func (xl xlObjects) listObjectPartsCommon(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListPartsInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	// Verify whether the bucket exists.
	if !xl.isBucketExist(bucket) {
		return ListPartsInfo{}, BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ListPartsInfo{}, ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if !xl.isUploadIDExists(bucket, object, uploadID) {
		return ListPartsInfo{}, InvalidUploadID{UploadID: uploadID}
	}
	// Hold lock so that there is no competing abort-multipart-upload or complete-multipart-upload.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	result := ListPartsInfo{}

	disk := xl.getRandomDisk() // Pick a random disk and read `xl.json` from there.
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	xlMeta, err := readXLMetadata(disk, minioMetaBucket, uploadIDPath)
	if err != nil {
		return ListPartsInfo{}, toObjectErr(err, minioMetaBucket, uploadIDPath)
	}
	// Only parts with higher part numbers will be listed.
	parts := xlMeta.Parts[partNumberMarker:]
	count := maxParts
	for i, part := range parts {
		var fi FileInfo
		partNamePath := path.Join(mpartMetaPrefix, bucket, object, uploadID, part.Name)
		fi, err = disk.StatFile(minioMetaBucket, partNamePath)
		if err != nil {
			return ListPartsInfo{}, toObjectErr(err, minioMetaBucket, partNamePath)
		}
		partNum := i + partNumberMarker + 1
		result.Parts = append(result.Parts, partInfo{
			PartNumber:   partNum,
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

// ListObjectParts - list object parts.
func (xl xlObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, error) {
	return xl.listObjectPartsCommon(bucket, object, uploadID, partNumberMarker, maxParts)
}

func (xl xlObjects) CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify whether the bucket exists.
	if !xl.isBucketExist(bucket) {
		return "", BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}
	if !xl.isUploadIDExists(bucket, object, uploadID) {
		return "", InvalidUploadID{UploadID: uploadID}
	}
	// Hold lock so that
	// 1) no one aborts this multipart upload
	// 2) no one does a parallel complete-multipart-upload on this multipart upload
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5, err := completeMultipartMD5(parts...)
	if err != nil {
		return "", err
	}

	uploadIDPath := pathJoin(mpartMetaPrefix, bucket, object, uploadID)
	xlMeta, err := readXLMetadata(xl.getRandomDisk(), minioMetaBucket, uploadIDPath)
	if err != nil {
		return "", err
	}

	var objectSize int64
	// Loop through all parts, validate them and then commit to disk.
	for i, part := range parts {
		// Construct part suffix.
		partSuffix := fmt.Sprintf("object%d", part.PartNumber)
		if xlMeta.SearchObjectPart(partSuffix, part.ETag) == -1 {
			return "", InvalidPart{}
		}
		// All parts except the last part has to be atleast 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(xlMeta.Parts[i].Size) {
			return "", PartTooSmall{}
		}
		objectSize += xlMeta.Parts[i].Size
	}

	// Check if an object is present as one of the parent dir.
	if xl.parentDirIsObject(bucket, path.Dir(object)) {
		return "", toObjectErr(errFileAccessDenied, bucket, object)
	}

	// Save the final object size and modtime.
	xlMeta.Stat.Size = objectSize
	xlMeta.Stat.ModTime = time.Now().UTC()

	// Save successfully calculated md5sum.
	xlMeta.Meta["md5Sum"] = s3MD5
	if err = xl.writeXLMetadata(minioMetaBucket, uploadIDPath, xlMeta); err != nil {
		return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
	}

	// Hold write lock on the destination before rename
	nsMutex.Lock(bucket, object)
	defer nsMutex.Unlock(bucket, object)

	// Delete if an object already exists.
	// FIXME: rename it to tmp file and delete only after
	// the newly uploaded file is renamed from tmp location to
	// the original location. Verify if the object is a multipart object.
	err = xl.deleteObject(bucket, object)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	if err = xl.renameObject(minioMetaBucket, uploadIDPath, bucket, object); err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Hold the lock so that two parallel complete-multipart-uploads do no
	// leave a stale uploads.json behind.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object))

	// Validate if there are other incomplete upload-id's present for
	// the object, if yes do not attempt to delete 'uploads.json'.
	uploadIDs, err := getUploadIDs(bucket, object, xl.storageDisks...)
	if err == nil {
		uploadIDIdx := uploadIDs.SearchUploadID(uploadID)
		if uploadIDIdx != -1 {
			uploadIDs.Uploads = append(uploadIDs.Uploads[:uploadIDIdx], uploadIDs.Uploads[uploadIDIdx+1:]...)
		}
		if len(uploadIDs.Uploads) > 0 {
			if err = updateUploadJSON(bucket, object, uploadIDs, xl.storageDisks...); err != nil {
				return "", err
			}
			return s3MD5, nil
		}
	}

	err = xl.deleteObject(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object))
	if err != nil {
		return "", toObjectErr(err, minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object))
	}

	// Return md5sum.
	return s3MD5, nil
}

// abortMultipartUploadCommon - aborts a multipart upload, common
// function used by both object layers.
func (xl xlObjects) abortMultipartUploadCommon(bucket, object, uploadID string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if !xl.isBucketExist(bucket) {
		return BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if !xl.isUploadIDExists(bucket, object, uploadID) {
		return InvalidUploadID{UploadID: uploadID}
	}

	// Hold lock so that there is no competing complete-multipart-upload or put-object-part.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))

	// Cleanup all uploaded parts.
	if err := cleanupUploadedParts(bucket, object, uploadID, xl.storageDisks...); err != nil {
		return err
	}

	// Validate if there are other incomplete upload-id's present for
	// the object, if yes do not attempt to delete 'uploads.json'.
	uploadIDs, err := getUploadIDs(bucket, object, xl.storageDisks...)
	if err == nil {
		uploadIDIdx := uploadIDs.SearchUploadID(uploadID)
		if uploadIDIdx != -1 {
			uploadIDs.Uploads = append(uploadIDs.Uploads[:uploadIDIdx], uploadIDs.Uploads[uploadIDIdx+1:]...)
		}
		if len(uploadIDs.Uploads) > 0 {
			return nil
		}
	}
	if err = xl.deleteObject(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object)); err != nil {
		return toObjectErr(err, minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object))
	}
	return nil
}

// AbortMultipartUpload - aborts a multipart upload.
func (xl xlObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	return xl.abortMultipartUploadCommon(bucket, object, uploadID)
}
