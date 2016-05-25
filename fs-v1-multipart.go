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

	fsMeta := fsMetaV1{}
	fsMeta.Format = "fs"
	fsMeta.Version = "1"

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

func isMultipartObject(storage StorageAPI, bucket, prefix string) bool {
	_, err := storage.StatFile(bucket, path.Join(prefix, fsMetaJSONFile))
	if err != nil {
		if err == errFileNotFound {
			return false
		}
		errorIf(err, "Unable to access "+path.Join(prefix, fsMetaJSONFile))
		return false
	}
	return true
}

// listUploadsInfo - list all uploads info.
func (fs fsObjects) listUploadsInfo(prefixPath string) (uploads []uploadInfo, err error) {
	splitPrefixes := strings.SplitN(prefixPath, "/", 3)
	uploadIDs, err := getUploadIDs(splitPrefixes[1], splitPrefixes[2], fs.storage)
	if err != nil {
		if err == errFileNotFound {
			return []uploadInfo{}, nil
		}
		return nil, err
	}
	uploads = uploadIDs.Uploads
	return uploads, nil
}

// listMetaBucketMultipart - list all objects at a given prefix inside minioMetaBucket.
func (fs fsObjects) listMetaBucketMultipart(prefixPath string, markerPath string, recursive bool, maxKeys int) (fileInfos []FileInfo, eof bool, err error) {
	walker := fs.lookupTreeWalk(listParams{minioMetaBucket, recursive, markerPath, prefixPath})
	if walker == nil {
		walker = fs.startTreeWalk(minioMetaBucket, prefixPath, markerPath, recursive)
	}

	// newMaxKeys tracks the size of entries which are going to be
	// returned back.
	var newMaxKeys int

	// Following loop gathers and filters out special files inside
	// minio meta volume.
	for {
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
				return nil, true, nil
			}
			return nil, false, toObjectErr(walkResult.err, minioMetaBucket, prefixPath)
		}
		fileInfo := walkResult.fileInfo
		var uploads []uploadInfo
		if fileInfo.Mode.IsDir() {
			// List all the entries if fi.Name is a leaf directory, if
			// fi.Name is not a leaf directory then the resulting
			// entries are empty.
			uploads, err = fs.listUploadsInfo(fileInfo.Name)
			if err != nil {
				return nil, false, err
			}
		}
		if len(uploads) > 0 {
			for _, upload := range uploads {
				fileInfos = append(fileInfos, FileInfo{
					Name:    path.Join(fileInfo.Name, upload.UploadID),
					ModTime: upload.Initiated,
				})
				newMaxKeys++
				// If we have reached the maxKeys, it means we have listed
				// everything that was requested.
				if newMaxKeys == maxKeys {
					break
				}
			}
		} else {
			// We reach here for a non-recursive case non-leaf entry
			// OR recursive case with fi.Name.
			if !fileInfo.Mode.IsDir() { // Do not skip non-recursive case directory entries.
				// Validate if 'fi.Name' is incomplete multipart.
				if !strings.HasSuffix(fileInfo.Name, fsMetaJSONFile) {
					continue
				}
				fileInfo.Name = path.Dir(fileInfo.Name)
			}
			fileInfos = append(fileInfos, fileInfo)
			newMaxKeys++
			// If we have reached the maxKeys, it means we have listed
			// everything that was requested.
			if newMaxKeys == maxKeys {
				break
			}
		}
	}

	if !eof && len(fileInfos) != 0 {
		// EOF has not reached, hence save the walker channel to the map so that the walker go routine
		// can continue from where it left off for the next list request.
		lastFileInfo := fileInfos[len(fileInfos)-1]
		markerPath = lastFileInfo.Name
		fs.saveTreeWalk(listParams{minioMetaBucket, recursive, markerPath, prefixPath}, walker)
	}

	// Return entries here.
	return fileInfos, eof, nil
}

// FIXME: Currently the code sorts based on keyName/upload-id which is
// not correct based on the S3 specs. According to s3 specs we are
// supposed to only lexically sort keyNames and then for keyNames with
// multiple upload ids should be sorted based on the initiated time.
// Currently this case is not handled.

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

	// Not using path.Join() as it strips off the trailing '/'.
	multipartPrefixPath := pathJoin(mpartMetaPrefix, pathJoin(bucket, prefix))
	if prefix == "" {
		// Should have a trailing "/" if prefix is ""
		// For ex. multipartPrefixPath should be "multipart/bucket/" if prefix is ""
		multipartPrefixPath += slashSeparator
	}
	multipartMarkerPath := ""
	if keyMarker != "" {
		keyMarkerPath := pathJoin(pathJoin(bucket, keyMarker), uploadIDMarker)
		multipartMarkerPath = pathJoin(mpartMetaPrefix, keyMarkerPath)
	}

	// List all the multipart files at prefixPath, starting with marker keyMarkerPath.
	fileInfos, eof, err := fs.listMetaBucketMultipart(multipartPrefixPath, multipartMarkerPath, recursive, maxUploads)
	if err != nil {
		return ListMultipartsInfo{}, err
	}

	// Loop through all the received files fill in the multiparts result.
	for _, fileInfo := range fileInfos {
		var objectName string
		var uploadID string
		if fileInfo.Mode.IsDir() {
			// All directory entries are common prefixes.
			uploadID = "" // Upload ids are empty for CommonPrefixes.
			objectName = strings.TrimPrefix(fileInfo.Name, retainSlash(pathJoin(mpartMetaPrefix, bucket)))
			result.CommonPrefixes = append(result.CommonPrefixes, objectName)
		} else {
			uploadID = path.Base(fileInfo.Name)
			objectName = strings.TrimPrefix(path.Dir(fileInfo.Name), retainSlash(pathJoin(mpartMetaPrefix, bucket)))
			result.Uploads = append(result.Uploads, uploadMetadata{
				Object:    objectName,
				UploadID:  uploadID,
				Initiated: fileInfo.ModTime,
			})
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
	fileWriter, err := fs.storage.CreateFile(minioMetaBucket, tmpPartPath)
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
	if err = fs.writeFSMetadata(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadID), fsMeta); err != nil {
		return "", toObjectErr(err, minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadID))
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
	partIdx := fsMeta.SearchObjectPart(partNumberMarker)
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
	fileWriter, err := fs.storage.CreateFile(minioMetaBucket, tempObj)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

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
			return "", err
		}
		// All parts except the last part has to be atleast 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(fi.Size) {
			return "", PartTooSmall{}
		}
		var fileReader io.ReadCloser
		fileReader, err = fs.storage.ReadFile(minioMetaBucket, multipartPartFile, 0)
		if err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", clErr
			}
			if err == errFileNotFound {
				return "", InvalidPart{}
			}
			return "", err
		}
		_, err = io.Copy(fileWriter, fileReader)
		if err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", clErr
			}
			return "", err
		}
		err = fileReader.Close()
		if err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", clErr
			}
			return "", err
		}
	}

	err = fileWriter.Close()
	if err != nil {
		if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
			return "", clErr
		}
		return "", err
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
	uploadIDs, err := getUploadIDs(bucket, object, fs.storage)
	if err == nil {
		uploadIDIdx := uploadIDs.SearchUploadID(uploadID)
		if uploadIDIdx != -1 {
			uploadIDs.Uploads = append(uploadIDs.Uploads[:uploadIDIdx], uploadIDs.Uploads[uploadIDIdx+1:]...)
		}
		if len(uploadIDs.Uploads) > 0 {
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
