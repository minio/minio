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
	"sort"
	"strconv"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/skyrings/skyring-common/tools/uuid"
)

/// Common multipart object layer functions.

// newMultipartUploadCommon - initialize a new multipart, is a common
// function for both object layers.
func newMultipartUploadCommon(storage StorageAPI, bucket string, object string) (uploadID string, err error) {
	// Verify if bucket name is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object name is valid.
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	// Verify whether the bucket exists.
	if isExist, err := isBucketExist(storage, bucket); err != nil {
		return "", err
	} else if !isExist {
		return "", BucketNotFound{Bucket: bucket}
	}

	// Loops through until successfully generates a new unique upload id.
	for {
		uuid, err := uuid.New()
		if err != nil {
			return "", err
		}
		uploadID := uuid.String()
		uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
		tempUploadIDPath := path.Join(tmpMetaPrefix, bucket, object, uploadID)
		if _, err = storage.StatFile(minioMetaBucket, uploadIDPath); err != nil {
			if err != errFileNotFound {
				return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
			}
			// uploadIDPath doesn't exist, so create empty file to reserve the name
			var w io.WriteCloser
			if w, err = storage.CreateFile(minioMetaBucket, tempUploadIDPath); err != nil {
				return "", toObjectErr(err, minioMetaBucket, tempUploadIDPath)
			}
			// Close the writer.
			if err = w.Close(); err != nil {
				return "", toObjectErr(err, minioMetaBucket, tempUploadIDPath)
			}
			err = storage.RenameFile(minioMetaBucket, tempUploadIDPath, minioMetaBucket, uploadIDPath)
			if err != nil {
				return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
			}
			return uploadID, nil
		}
		// uploadIDPath already exists.
		// loop again to try with different uuid generated.
	}
}

// putObjectPartCommon - put object part.
func putObjectPartCommon(storage StorageAPI, bucket string, object string, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	// Verify whether the bucket exists.
	if isExist, err := isBucketExist(storage, bucket); err != nil {
		return "", err
	} else if !isExist {
		return "", BucketNotFound{Bucket: bucket}
	}

	if status, err := isUploadIDExists(storage, bucket, object, uploadID); err != nil {
		return "", err
	} else if !status {
		return "", InvalidUploadID{UploadID: uploadID}
	}

	partSuffix := fmt.Sprintf("%s.%d", uploadID, partID)
	partSuffixPath := path.Join(tmpMetaPrefix, bucket, object, partSuffix)
	fileWriter, err := storage.CreateFile(minioMetaBucket, partSuffixPath)
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
			safeCloseAndRemove(fileWriter)
			return "", toObjectErr(err, bucket, object)
		}
		// Reader shouldn't have more data what mentioned in size argument.
		// reading one more byte from the reader to validate it.
		// expected to fail, success validates existence of more data in the reader.
		if _, err = io.CopyN(ioutil.Discard, data, 1); err == nil {
			safeCloseAndRemove(fileWriter)
			return "", UnExpectedDataSize{Size: int(size)}
		}
	} else {
		if _, err = io.Copy(multiWriter, data); err != nil {
			safeCloseAndRemove(fileWriter)
			return "", toObjectErr(err, bucket, object)
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			safeCloseAndRemove(fileWriter)
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}
	err = fileWriter.Close()
	if err != nil {
		return "", err
	}

	partSuffixMD5 := fmt.Sprintf("%s.%.5d.%s", uploadID, partID, newMD5Hex)
	partSuffixMD5Path := path.Join(mpartMetaPrefix, bucket, object, partSuffixMD5)
	err = storage.RenameFile(minioMetaBucket, partSuffixPath, minioMetaBucket, partSuffixMD5Path)
	if err != nil {
		return "", err
	}
	return newMD5Hex, nil
}

// Cleanup all temp entries inside tmpMetaPrefix directory, upon server initialization.
func cleanupAllTmpEntries(storage StorageAPI) error {
	return cleanupUploadedParts(storage, tmpMetaPrefix, "", "", "")
}

// Wrapper to which removes all the uploaded parts after a successful
// complete multipart upload.
func cleanupUploadedParts(storage StorageAPI, prefix, bucket, object, uploadID string) error {
	multipartDir := path.Join(prefix, bucket, object)
	entries, err := storage.ListDir(minioMetaBucket, multipartDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry, uploadID) {
			if err = storage.DeleteFile(minioMetaBucket, path.Join(multipartDir, entry)); err != nil {
				return err
			}
		}
	}
	return nil
}

// abortMultipartUploadCommon - aborts a multipart upload, common
// function used by both object layers.
func abortMultipartUploadCommon(storage StorageAPI, bucket, object, uploadID string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if status, err := isUploadIDExists(storage, bucket, object, uploadID); err != nil {
		return err
	} else if !status {
		return InvalidUploadID{UploadID: uploadID}
	}
	return cleanupUploadedParts(storage, mpartMetaPrefix, bucket, object, uploadID)
}

// listLeafEntries - lists all entries if a given prefixPath is a leaf
// directory, returns error if any - returns empty list if prefixPath
// is not a leaf directory.
func listLeafEntries(storage StorageAPI, prefixPath string) (entries []string, err error) {
	entries, err = storage.ListDir(minioMetaBucket, prefixPath)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry, slashSeparator) {
			return nil, nil
		}
	}
	return entries, nil
}

// listMetaBucketMultipartFiles - list all files at a given prefix inside minioMetaBucket.
func listMetaBucketMultipartFiles(storage StorageAPI, prefixPath string, markerPath string, recursive bool, maxKeys int) (allFileInfos []FileInfo, eof bool, err error) {
	return nil, true, nil
	// // newMaxKeys tracks the size of entries which are going to be
	// // returned back.
	// var newMaxKeys int

	// // Following loop gathers and filters out special files inside
	// // minio meta volume.
	// for {
	// 	var fileInfos []FileInfo
	// 	// List files up to maxKeys-newMaxKeys, since we are skipping
	// 	// entries for special files.
	// 	fileInfos, eof, err = storage.ListFiles(minioMetaBucket, prefixPath, markerPath, recursive, maxKeys-newMaxKeys)
	// 	if err != nil {
	// 		log.WithFields(logrus.Fields{
	// 			"prefixPath": prefixPath,
	// 			"markerPath": markerPath,
	// 			"recursive":  recursive,
	// 			"maxKeys":    maxKeys,
	// 		}).Errorf("%s", err)
	// 		return nil, true, err
	// 	}
	// 	// Loop through and validate individual file.
	// 	for _, fi := range fileInfos {
	// 		var entries []FileInfo
	// 		if fi.Mode.IsDir() {
	// 			// List all the entries if fi.Name is a leaf directory, if
	// 			// fi.Name is not a leaf directory then the resulting
	// 			// entries are empty.
	// 			entries, err = listLeafEntries(storage, fi.Name)
	// 			if err != nil {
	// 				log.WithFields(logrus.Fields{
	// 					"prefixPath": fi.Name,
	// 				}).Errorf("%s", err)
	// 				return nil, false, err
	// 			}
	// 		}
	// 		// Set markerPath for next batch of listing.
	// 		markerPath = fi.Name
	// 		if len(entries) > 0 {
	// 			// We reach here for non-recursive case and a leaf entry.
	// 			for _, entry := range entries {
	// 				allFileInfos = append(allFileInfos, entry)
	// 				newMaxKeys++
	// 				// If we have reached the maxKeys, it means we have listed
	// 				// everything that was requested. Return right here.
	// 				if newMaxKeys == maxKeys {
	// 					// Return values:
	// 					// allFileInfos : "maxKeys" number of entries.
	// 					// eof : eof returned by fs.storage.ListFiles()
	// 					// error : nil
	// 					return
	// 				}
	// 			}
	// 		} else {
	// 			// We reach here for a non-recursive case non-leaf entry
	// 			// OR recursive case with fi.Name matching pattern bucket/object/uploadID[.partNum.md5sum]
	// 			if !fi.Mode.IsDir() { // Do not skip non-recursive case directory entries.
	// 				// Skip files matching pattern bucket/object/uploadID.partNum.md5sum
	// 				// and retain files matching pattern bucket/object/uploadID
	// 				specialFile := path.Base(fi.Name)
	// 				if strings.Contains(specialFile, ".") {
	// 					// Contains partnumber and md5sum info, skip this.
	// 					continue
	// 				}
	// 			}
	// 			allFileInfos = append(allFileInfos, fi)
	// 			newMaxKeys++
	// 			// If we have reached the maxKeys, it means we have listed
	// 			// everything that was requested. Return right here.
	// 			if newMaxKeys == maxKeys {
	// 				// Return values:
	// 				// allFileInfos : "maxKeys" number of entries.
	// 				// eof : eof returned by fs.storage.ListFiles()
	// 				// error : nil
	// 				return
	// 			}
	// 		}
	// 	}
	// 	// If we have reached eof then we break out.
	// 	if eof {
	// 		break
	// 	}
	// }

	// // Return entries here.
	// return allFileInfos, eof, nil
}

// listMultipartUploadsCommon - lists all multipart uploads, common
// function for both object layers.
func listMultipartUploadsCommon(storage StorageAPI, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	result := ListMultipartsInfo{}
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListMultipartsInfo{}, BucketNameInvalid{Bucket: bucket}
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
	// Also bucket should always be followed by '/' even if prefix is empty.
	multipartPrefixPath := pathJoin(mpartMetaPrefix, pathJoin(bucket, prefix))
	multipartMarkerPath := ""
	if keyMarker != "" {
		keyMarkerPath := pathJoin(pathJoin(bucket, keyMarker), uploadIDMarker)
		multipartMarkerPath = pathJoin(mpartMetaPrefix, keyMarkerPath)
	}

	// List all the multipart files at prefixPath, starting with marker keyMarkerPath.
	fileInfos, eof, err := listMetaBucketMultipartFiles(storage, multipartPrefixPath, multipartMarkerPath, recursive, maxUploads)
	if err != nil {
		log.WithFields(logrus.Fields{
			"prefixPath": multipartPrefixPath,
			"markerPath": multipartMarkerPath,
			"recursive":  recursive,
			"maxUploads": maxUploads,
		}).Errorf("listMetaBucketMultipartFiles failed with %s", err)
		return ListMultipartsInfo{}, err
	}

	// Loop through all the received files fill in the multiparts result.
	for _, fi := range fileInfos {
		var objectName string
		var uploadID string
		if fi.Mode.IsDir() {
			// All directory entries are common prefixes.
			uploadID = "" // Upload ids are empty for CommonPrefixes.
			objectName = strings.TrimPrefix(fi.Name, retainSlash(path.Join(mpartMetaPrefix, bucket)))
			result.CommonPrefixes = append(result.CommonPrefixes, objectName)
		} else {
			uploadID = path.Base(fi.Name)
			objectName = strings.TrimPrefix(path.Dir(fi.Name), retainSlash(path.Join(mpartMetaPrefix, bucket)))
			result.Uploads = append(result.Uploads, uploadMetadata{
				Object:    objectName,
				UploadID:  uploadID,
				Initiated: fi.ModTime,
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

// ListObjectParts - list object parts, common function across both
// object layers.
func listObjectPartsCommon(storage StorageAPI, bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListPartsInfo{}, (BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return ListPartsInfo{}, (ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	if status, err := isUploadIDExists(storage, bucket, object, uploadID); err != nil {
		return ListPartsInfo{}, err
	} else if !status {
		return ListPartsInfo{}, (InvalidUploadID{UploadID: uploadID})
	}
	result := ListPartsInfo{}
	entries, err := storage.ListDir(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object))
	if err != nil {
		return result, err
	}
	sort.Strings(entries)
	var newEntries []string
	for _, entry := range entries {
		if !strings.Contains(entry, ".") {
			continue
		}
		if !strings.HasPrefix(entry, uploadID) {
			continue
		}
		newEntries = append(newEntries, entry)
	}
	idx := sort.SearchStrings(newEntries, fmt.Sprintf("%s.%.5d.", uploadID, partNumberMarker+1))
	newEntries = newEntries[idx:]
	count := maxParts
	for _, entry := range newEntries {
		fi, err := storage.StatFile(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, entry))
		splitEntry := strings.Split(entry, ".")
		partNum, err := strconv.Atoi(splitEntry[1])
		if err != nil {
			return ListPartsInfo{}, err
		}
		result.Parts = append(result.Parts, partInfo{
			PartNumber:   partNum,
			LastModified: fi.ModTime,
			ETag:         splitEntry[2],
			Size:         fi.Size,
		})
		count--
		if count == 0 {
			break
		}
	}
	if len(newEntries) > len(result.Parts) {
		result.IsTruncated = true
	}
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	return result, nil
}

// isUploadIDExists - verify if a given uploadID exists and is valid.
func isUploadIDExists(storage StorageAPI, bucket, object, uploadID string) (bool, error) {
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	st, err := storage.StatFile(minioMetaBucket, uploadIDPath)
	if err != nil {
		// Upload id does not exist.
		if err == errFileNotFound {
			return false, nil
		}
		return false, err
	}
	// Upload id exists and is a regular file.
	return st.Mode.IsRegular(), nil
}
