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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/skyrings/skyring-common/tools/uuid"
)

const (
	incompleteFile  = "00000.incomplete"
	uploadsJSONFile = "uploads.json"
)

// createUploadsJSON - create uploads.json placeholder file.
func createUploadsJSON(storage StorageAPI, bucket, object, uploadID string) error {
	// Place holder uploads.json
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	uploadsJSONSuffix := fmt.Sprintf("%s.%s", uploadID, uploadsJSONFile)
	tmpUploadsPath := path.Join(tmpMetaPrefix, bucket, object, uploadsJSONSuffix)
	w, err := storage.CreateFile(minioMetaBucket, uploadsPath)
	if err != nil {
		return err
	}
	if err = w.Close(); err != nil {
		if clErr := safeCloseAndRemove(w); clErr != nil {
			return clErr
		}
		return err
	}
	_, err = storage.StatFile(minioMetaBucket, uploadsPath)
	if err != nil {
		if err == errFileNotFound {
			err = storage.RenameFile(minioMetaBucket, tmpUploadsPath, minioMetaBucket, uploadsPath)
			if err == nil {
				return nil
			}
		}
		if derr := storage.DeleteFile(minioMetaBucket, tmpUploadsPath); derr != nil {
			return derr
		}
		return err
	}
	return nil
}

/// Common multipart object layer functions.

// newMultipartUploadCommon - initialize a new multipart, is a common
// function for both object layers.
func newMultipartUploadCommon(storage StorageAPI, bucket string, object string, meta map[string]string) (uploadID string, err error) {
	// Verify if bucket name is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify whether the bucket exists.
	if !isBucketExist(storage, bucket) {
		return "", BucketNotFound{Bucket: bucket}
	}
	// Verify if object name is valid.
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	// This lock needs to be held for any changes to the directory contents of ".minio/multipart/object/"
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object))
	// Loops through until successfully generates a new unique upload id.
	for {
		uuid, err := uuid.New()
		if err != nil {
			return "", err
		}
		uploadID := uuid.String()
		// Create placeholder file 'uploads.json'
		err = createUploadsJSON(storage, bucket, object, uploadID)
		if err != nil {
			return "", err
		}
		uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID, incompleteFile)
		incompleteSuffix := fmt.Sprintf("%s.%s", uploadID, incompleteFile)
		tempUploadIDPath := path.Join(tmpMetaPrefix, bucket, object, incompleteSuffix)
		if _, err = storage.StatFile(minioMetaBucket, uploadIDPath); err != nil {
			if err != errFileNotFound {
				return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
			}
			// uploadIDPath doesn't exist, so create empty file to reserve the name
			var w io.WriteCloser
			if w, err = storage.CreateFile(minioMetaBucket, tempUploadIDPath); err != nil {
				return "", toObjectErr(err, minioMetaBucket, tempUploadIDPath)
			}

			// Encode the uploaded metadata into incomplete file.
			encoder := json.NewEncoder(w)
			err = encoder.Encode(&meta)
			if err != nil {
				if clErr := safeCloseAndRemove(w); clErr != nil {
					return "", toObjectErr(clErr, minioMetaBucket, tempUploadIDPath)
				}
				return "", toObjectErr(err, minioMetaBucket, tempUploadIDPath)
			}

			// Close the writer.
			if err = w.Close(); err != nil {
				if clErr := safeCloseAndRemove(w); clErr != nil {
					return "", toObjectErr(clErr, minioMetaBucket, tempUploadIDPath)
				}
				return "", toObjectErr(err, minioMetaBucket, tempUploadIDPath)
			}

			// Rename the file to the actual location from temporary path.
			err = storage.RenameFile(minioMetaBucket, tempUploadIDPath, minioMetaBucket, uploadIDPath)
			if err != nil {
				if derr := storage.DeleteFile(minioMetaBucket, tempUploadIDPath); derr != nil {
					return "", toObjectErr(derr, minioMetaBucket, tempUploadIDPath)
				}
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
	// Verify whether the bucket exists.
	if !isBucketExist(storage, bucket) {
		return "", BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if !isUploadIDExists(storage, bucket, object, uploadID) {
		return "", InvalidUploadID{UploadID: uploadID}
	}
	// Hold read lock on the uploadID so that no one aborts it.
	nsMutex.RLock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	defer nsMutex.RUnlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))

	// Hold write lock on the part so that there is no parallel upload on the part.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID, strconv.Itoa(partID)))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID, strconv.Itoa(partID)))

	partSuffix := fmt.Sprintf("%s.%.5d", uploadID, partID)
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
		if _, err = io.Copy(multiWriter, data); err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", toObjectErr(clErr, bucket, object)
			}
			return "", toObjectErr(err, bucket, object)
		}
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

	partSuffixMD5 := fmt.Sprintf("%.5d.%s", partID, newMD5Hex)
	partSuffixMD5Path := path.Join(mpartMetaPrefix, bucket, object, uploadID, partSuffixMD5)
	if _, err = storage.StatFile(minioMetaBucket, partSuffixMD5Path); err == nil {
		// Part already uploaded as md5sum matches with the previous part.
		// Just delete the temporary file.
		if err = storage.DeleteFile(minioMetaBucket, partSuffixPath); err != nil {
			return "", toObjectErr(err, minioMetaBucket, partSuffixPath)
		}
		return newMD5Hex, nil
	}
	err = storage.RenameFile(minioMetaBucket, partSuffixPath, minioMetaBucket, partSuffixMD5Path)
	if err != nil {
		if derr := storage.DeleteFile(minioMetaBucket, partSuffixPath); derr != nil {
			return "", toObjectErr(derr, minioMetaBucket, partSuffixPath)
		}
		return "", toObjectErr(err, minioMetaBucket, partSuffixMD5Path)
	}
	return newMD5Hex, nil
}

// Wrapper to which removes all the uploaded parts after a successful
// complete multipart upload.
func cleanupUploadedParts(storage StorageAPI, bucket, object, uploadID string) error {
	return cleanupDir(storage, minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadID))
}

// abortMultipartUploadCommon - aborts a multipart upload, common
// function used by both object layers.
func abortMultipartUploadCommon(storage StorageAPI, bucket, object, uploadID string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if !isBucketExist(storage, bucket) {
		return BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if !isUploadIDExists(storage, bucket, object, uploadID) {
		return InvalidUploadID{UploadID: uploadID}
	}

	// Hold lock so that there is no competing complete-multipart-upload or put-object-part.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))

	if err := cleanupUploadedParts(storage, bucket, object, uploadID); err != nil {
		return err
	}

	// Validate if there are other incomplete upload-id's present for
	// the object, if yes do not attempt to delete 'uploads.json'.
	if entries, err := storage.ListDir(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object)); err == nil {
		if len(entries) > 1 {
			return nil
		}
	}

	uploadsJSONPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	if err := storage.DeleteFile(minioMetaBucket, uploadsJSONPath); err != nil {
		return err
	}

	return nil
}

// isIncompleteMultipart - is object incomplete multipart.
func isIncompleteMultipart(storage StorageAPI, objectPath string) (bool, error) {
	_, err := storage.StatFile(minioMetaBucket, path.Join(objectPath, uploadsJSONFile))
	if err != nil {
		if err == errFileNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// listLeafEntries - lists all entries if a given prefixPath is a leaf
// directory, returns error if any - returns empty list if prefixPath
// is not a leaf directory.
func listLeafEntries(storage StorageAPI, prefixPath string) (entries []string, err error) {
	var ok bool
	if ok, err = isIncompleteMultipart(storage, prefixPath); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	entries, err = storage.ListDir(minioMetaBucket, prefixPath)
	if err != nil {
		return nil, err
	}
	var newEntries []string
	for _, entry := range entries {
		if strings.HasSuffix(entry, slashSeparator) {
			newEntries = append(newEntries, entry)
		}
	}
	return newEntries, nil
}

// listMetaBucketMultipartFiles - list all files at a given prefix inside minioMetaBucket.
func listMetaBucketMultipartFiles(layer ObjectLayer, prefixPath string, markerPath string, recursive bool, maxKeys int) (fileInfos []FileInfo, eof bool, err error) {
	var storage StorageAPI
	switch l := layer.(type) {
	case fsObjects:
		storage = l.storage
	case xlObjects:
		storage = l.storage
	}

	walker := lookupTreeWalk(layer, listParams{minioMetaBucket, recursive, markerPath, prefixPath})
	if walker == nil {
		walker = startTreeWalk(layer, minioMetaBucket, prefixPath, markerPath, recursive)
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
		fi := walkResult.fileInfo
		var entries []string
		if fi.Mode.IsDir() {
			// List all the entries if fi.Name is a leaf directory, if
			// fi.Name is not a leaf directory then the resulting
			// entries are empty.
			entries, err = listLeafEntries(storage, fi.Name)
			if err != nil {
				return nil, false, err
			}
		}
		if len(entries) > 0 {
			// We reach here for non-recursive case and a leaf entry.
			sort.Strings(entries)
			for _, entry := range entries {
				var fileInfo FileInfo
				incompleteUploadFile := path.Join(fi.Name, entry, incompleteFile)
				fileInfo, err = storage.StatFile(minioMetaBucket, incompleteUploadFile)
				if err != nil {
					return nil, false, err
				}
				fileInfo.Name = path.Join(fi.Name, entry)
				fileInfos = append(fileInfos, fileInfo)
				newMaxKeys++
				// If we have reached the maxKeys, it means we have listed
				// everything that was requested. Return right here.
				if newMaxKeys == maxKeys {
					return
				}
			}
		} else {
			// We reach here for a non-recursive case non-leaf entry
			// OR recursive case with fi.Name.
			if !fi.Mode.IsDir() { // Do not skip non-recursive case directory entries.
				// Validate if 'fi.Name' is incomplete multipart.
				if !strings.HasSuffix(fi.Name, incompleteFile) {
					continue
				}
				fi.Name = path.Dir(fi.Name)
			}
			fileInfos = append(fileInfos, fi)
			newMaxKeys++
			// If we have reached the maxKeys, it means we have listed
			// everything that was requested. Return right here.
			if newMaxKeys == maxKeys {
				return
			}
		}
	}

	// Return entries here.
	return fileInfos, eof, nil
}

// FIXME: Currently the code sorts based on keyName/upload-id which is
// in correct based on the S3 specs. According to s3 specs we are
// supposed to only lexically sort keyNames and then for keyNames with
// multiple upload ids should be sorted based on the initiated time.
// Currently this case is not handled.

// listMultipartUploadsCommon - lists all multipart uploads, common
// function for both object layers.
func listMultipartUploadsCommon(layer ObjectLayer, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	var storage StorageAPI
	switch l := layer.(type) {
	case xlObjects:
		storage = l.storage
	case fsObjects:
		storage = l.storage
	}
	result := ListMultipartsInfo{}
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListMultipartsInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	if !isBucketExist(storage, bucket) {
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
	fileInfos, eof, err := listMetaBucketMultipartFiles(layer, multipartPrefixPath, multipartMarkerPath, recursive, maxUploads)
	if err != nil {
		return ListMultipartsInfo{}, err
	}

	// Loop through all the received files fill in the multiparts result.
	for _, fi := range fileInfos {
		var objectName string
		var uploadID string
		if fi.Mode.IsDir() {
			// All directory entries are common prefixes.
			uploadID = "" // Upload ids are empty for CommonPrefixes.
			objectName = strings.TrimPrefix(fi.Name, retainSlash(pathJoin(mpartMetaPrefix, bucket)))
			result.CommonPrefixes = append(result.CommonPrefixes, objectName)
		} else {
			uploadID = path.Base(fi.Name)
			objectName = strings.TrimPrefix(path.Dir(fi.Name), retainSlash(pathJoin(mpartMetaPrefix, bucket)))
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

// ListObjectParts - list object parts, common function across both object layers.
func listObjectPartsCommon(storage StorageAPI, bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListPartsInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	// Verify whether the bucket exists.
	if !isBucketExist(storage, bucket) {
		return ListPartsInfo{}, BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ListPartsInfo{}, ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if !isUploadIDExists(storage, bucket, object, uploadID) {
		return ListPartsInfo{}, InvalidUploadID{UploadID: uploadID}
	}
	// Hold lock so that there is no competing abort-multipart-upload or complete-multipart-upload.
	nsMutex.Lock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	defer nsMutex.Unlock(minioMetaBucket, pathJoin(mpartMetaPrefix, bucket, object, uploadID))
	result := ListPartsInfo{}
	entries, err := storage.ListDir(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadID))
	if err != nil {
		return result, err
	}
	sort.Strings(entries)
	var newEntries []string
	for _, entry := range entries {
		newEntries = append(newEntries, path.Base(entry))
	}
	idx := sort.SearchStrings(newEntries, fmt.Sprintf("%.5d.", partNumberMarker+1))
	newEntries = newEntries[idx:]
	count := maxParts
	for _, entry := range newEntries {
		fi, err := storage.StatFile(minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadID, entry))
		splitEntry := strings.SplitN(entry, ".", 2)
		partStr := splitEntry[0]
		etagStr := splitEntry[1]
		partNum, err := strconv.Atoi(partStr)
		if err != nil {
			return ListPartsInfo{}, err
		}
		result.Parts = append(result.Parts, partInfo{
			PartNumber:   partNum,
			LastModified: fi.ModTime,
			ETag:         etagStr,
			Size:         fi.Size,
		})
		count--
		if count == 0 {
			break
		}
	}
	// If listed entries are more than maxParts, we set IsTruncated as true.
	if len(newEntries) > len(result.Parts) {
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

// isUploadIDExists - verify if a given uploadID exists and is valid.
func isUploadIDExists(storage StorageAPI, bucket, object, uploadID string) bool {
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID, incompleteFile)
	st, err := storage.StatFile(minioMetaBucket, uploadIDPath)
	if err != nil {
		if err == errFileNotFound {
			return false
		}
		errorIf(err, "Stat failed on "+minioMetaBucket+"/"+uploadIDPath+".")
		return false
	}
	return st.Mode.IsRegular()
}
