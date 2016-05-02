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

	"github.com/Sirupsen/logrus"
	"github.com/skyrings/skyring-common/tools/uuid"
)

// listLeafEntries - lists all entries if a given prefixPath is a leaf
// directory, returns error if any - returns empty list if prefixPath
// is not a leaf directory.
func (fs fsObjects) listLeafEntries(prefixPath string) (entries []FileInfo, e error) {
	var markerPath string
	for {
		fileInfos, eof, err := fs.storage.ListFiles(minioMetaBucket, prefixPath, markerPath, false, 1000)
		if err != nil {
			log.WithFields(logrus.Fields{
				"prefixPath": prefixPath,
				"markerPath": markerPath,
			}).Errorf("%s", err)
			return nil, err
		}
		for _, fileInfo := range fileInfos {
			// Set marker for next batch of ListFiles.
			markerPath = fileInfo.Name
			if fileInfo.Mode.IsDir() {
				// If a directory is found, doesn't return anything.
				return nil, nil
			}
			fileName := path.Base(fileInfo.Name)
			if !strings.Contains(fileName, ".") {
				// Skip the entry if it is of the pattern bucket/object/uploadID.partNum.md5sum
				// and retain entries of the pattern bucket/object/uploadID
				entries = append(entries, fileInfo)
			}
		}
		if eof {
			break
		}
	}
	return entries, nil
}

// listMetaBucketFiles - list all files at a given prefix inside minioMetaBucket.
func (fs fsObjects) listMetaBucketFiles(prefixPath string, markerPath string, recursive bool, maxKeys int) (allFileInfos []FileInfo, eof bool, err error) {
	// newMaxKeys tracks the size of entries which are going to be
	// returned back.
	var newMaxKeys int

	// Following loop gathers and filters out special files inside
	// minio meta volume.
	for {
		var fileInfos []FileInfo
		// List files up to maxKeys-newMaxKeys, since we are skipping entries for special files.
		fileInfos, eof, err = fs.storage.ListFiles(minioMetaBucket, prefixPath, markerPath, recursive, maxKeys-newMaxKeys)
		if err != nil {
			log.WithFields(logrus.Fields{
				"prefixPath": prefixPath,
				"markerPath": markerPath,
				"recursive":  recursive,
				"maxKeys":    maxKeys,
			}).Errorf("%s", err)
			return nil, true, err
		}
		// Loop through and validate individual file.
		for _, fi := range fileInfos {
			var entries []FileInfo
			if fi.Mode.IsDir() {
				// List all the entries if fi.Name is a leaf directory, if
				// fi.Name is not a leaf directory then the resulting
				// entries are empty.
				entries, err = fs.listLeafEntries(fi.Name)
				if err != nil {
					log.WithFields(logrus.Fields{
						"prefixPath": fi.Name,
					}).Errorf("%s", err)
					return nil, false, err
				}
			}
			// Set markerPath for next batch of listing.
			markerPath = fi.Name
			if len(entries) > 0 {
				// We reach here for non-recursive case and a leaf entry.
				for _, entry := range entries {
					allFileInfos = append(allFileInfos, entry)
					newMaxKeys++
					// If we have reached the maxKeys, it means we have listed
					// everything that was requested. Return right here.
					if newMaxKeys == maxKeys {
						// Return values:
						// allFileInfos : "maxKeys" number of entries.
						// eof : eof returned by fs.storage.ListFiles()
						// error : nil
						return
					}
				}
			} else {
				// We reach here for a non-recursive case non-leaf entry
				// OR recursive case with fi.Name matching pattern bucket/object/uploadID[.partNum.md5sum]
				if !fi.Mode.IsDir() { // Do not skip non-recursive case directory entries.
					// Skip files matching pattern bucket/object/uploadID.partNum.md5sum
					// and retain files matching pattern bucket/object/uploadID
					specialFile := path.Base(fi.Name)
					if strings.Contains(specialFile, ".") {
						// Contains partnumber and md5sum info, skip this.
						continue
					}
				}
			}
			allFileInfos = append(allFileInfos, fi)
			newMaxKeys++
			// If we have reached the maxKeys, it means we have listed
			// everything that was requested. Return right here.
			if newMaxKeys == maxKeys {
				// Return values:
				// allFileInfos : "maxKeys" number of entries.
				// eof : eof returned by fs.storage.ListFiles()
				// error : nil
				return
			}
		}
		// If we have reached eof then we break out.
		if eof {
			break
		}
	}

	// Return entries here.
	return allFileInfos, eof, nil
}

// ListMultipartUploads - list multipart uploads.
func (fs fsObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	result := ListMultipartsInfo{}
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListMultipartsInfo{}, (BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectPrefix(prefix) {
		return ListMultipartsInfo{}, (ObjectNameInvalid{Bucket: bucket, Object: prefix})
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return ListMultipartsInfo{}, (UnsupportedDelimiter{
			Delimiter: delimiter,
		})
	}
	// Verify if marker has prefix.
	if keyMarker != "" && !strings.HasPrefix(keyMarker, prefix) {
		return ListMultipartsInfo{}, (InvalidMarkerPrefixCombination{
			Marker: keyMarker,
			Prefix: prefix,
		})
	}
	if uploadIDMarker != "" {
		if strings.HasSuffix(keyMarker, slashSeparator) {
			return result, (InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			})
		}
		id, err := uuid.Parse(uploadIDMarker)
		if err != nil {
			return result, err
		}
		if id.IsZero() {
			return result, (MalformedUploadID{
				UploadID: uploadIDMarker,
			})
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
	prefixPath := pathJoin(bucket, prefix)
	keyMarkerPath := ""
	if keyMarker != "" {
		keyMarkerPath = pathJoin(pathJoin(bucket, keyMarker), uploadIDMarker)
	}
	// List all the multipart files at prefixPath, starting with marker keyMarkerPath.
	fileInfos, eof, err := fs.listMetaBucketFiles(prefixPath, keyMarkerPath, recursive, maxUploads)
	if err != nil {
		log.WithFields(logrus.Fields{
			"prefixPath": prefixPath,
			"markerPath": keyMarkerPath,
			"recursive":  recursive,
			"maxUploads": maxUploads,
		}).Errorf("listMetaBucketFiles failed with %s", err)
		return ListMultipartsInfo{}, err
	}

	// Loop through all the received files fill in the multiparts result.
	for _, fi := range fileInfos {
		var objectName string
		var uploadID string
		if fi.Mode.IsDir() {
			// All directory entries are common prefixes.
			uploadID = "" // Upload ids are empty for CommonPrefixes.
			objectName = strings.TrimPrefix(fi.Name, retainSlash(bucket))
			result.CommonPrefixes = append(result.CommonPrefixes, objectName)
		} else {
			uploadID = path.Base(fi.Name)
			objectName = strings.TrimPrefix(path.Dir(fi.Name), retainSlash(bucket))
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

func (fs fsObjects) NewMultipartUpload(bucket, object string) (string, error) {
	// Verify if bucket name is valid.
	if !IsValidBucketName(bucket) {
		return "", (BucketNameInvalid{Bucket: bucket})
	}
	// Verify if object name is valid.
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	// Verify whether the bucket exists.
	if isExist, err := isBucketExist(fs.storage, bucket); err != nil {
		return "", err
	} else if !isExist {
		return "", BucketNotFound{Bucket: bucket}
	}

	if _, err := fs.storage.StatVol(minioMetaBucket); err != nil {
		if err == errVolumeNotFound {
			err = fs.storage.MakeVol(minioMetaBucket)
			if err != nil {
				return "", toObjectErr(err)
			}
		}
	}
	for {
		uuid, err := uuid.New()
		if err != nil {
			return "", err
		}
		uploadID := uuid.String()
		uploadIDPath := path.Join(bucket, object, uploadID)
		if _, err = fs.storage.StatFile(minioMetaBucket, uploadIDPath); err != nil {
			if err != errFileNotFound {
				return "", (toObjectErr(err, minioMetaBucket, uploadIDPath))
			}
			// uploadIDPath doesn't exist, so create empty file to reserve the name
			var w io.WriteCloser
			if w, err = fs.storage.CreateFile(minioMetaBucket, uploadIDPath); err == nil {
				// Close the writer.
				if err = w.Close(); err != nil {
					return "", err
				}
			} else {
				return "", toObjectErr(err, minioMetaBucket, uploadIDPath)
			}
			return uploadID, nil
		}
		// uploadIDPath already exists.
		// loop again to try with different uuid generated.
	}
}

// PutObjectPart - writes the multipart upload chunks.
func (fs fsObjects) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	// Verify whether the bucket exists.
	if isExist, err := isBucketExist(fs.storage, bucket); err != nil {
		return "", err
	} else if !isExist {
		return "", BucketNotFound{Bucket: bucket}
	}

	if status, err := isUploadIDExists(fs.storage, bucket, object, uploadID); err != nil {
		return "", err
	} else if !status {
		return "", InvalidUploadID{UploadID: uploadID}
	}

	partSuffix := fmt.Sprintf("%s.%d", uploadID, partID)
	partSuffixPath := path.Join(bucket, object, partSuffix)
	fileWriter, err := fs.storage.CreateFile(minioMetaBucket, partSuffixPath)
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
			return "", (toObjectErr(err))
		}
		// Reader shouldn't have more data what mentioned in size argument.
		// reading one more byte from the reader to validate it.
		// expected to fail, success validates existence of more data in the reader.
		if _, err = io.CopyN(ioutil.Discard, data, 1); err == nil {
			safeCloseAndRemove(fileWriter)
			return "", (UnExpectedDataSize{Size: int(size)})
		}
	} else {
		if _, err = io.Copy(multiWriter, data); err != nil {
			safeCloseAndRemove(fileWriter)
			return "", (toObjectErr(err))
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			safeCloseAndRemove(fileWriter)
			return "", (BadDigest{md5Hex, newMD5Hex})
		}
	}
	err = fileWriter.Close()
	if err != nil {
		return "", err
	}
	partSuffixMD5 := fmt.Sprintf("%s.%d.%s", uploadID, partID, newMD5Hex)
	partSuffixMD5Path := path.Join(bucket, object, partSuffixMD5)
	err = fs.storage.RenameFile(minioMetaBucket, partSuffixPath, minioMetaBucket, partSuffixMD5Path)
	if err != nil {
		return "", err
	}
	return newMD5Hex, nil
}

func (fs fsObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListPartsInfo{}, (BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return ListPartsInfo{}, (ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	if status, err := isUploadIDExists(fs.storage, bucket, object, uploadID); err != nil {
		return ListPartsInfo{}, err
	} else if !status {
		return ListPartsInfo{}, (InvalidUploadID{UploadID: uploadID})
	}
	result := ListPartsInfo{}
	var markerPath string
	nextPartNumberMarker := 0
	uploadIDPath := path.Join(bucket, object, uploadID)
	// Figure out the marker for the next subsequent calls, if the
	// partNumberMarker is already set.
	if partNumberMarker > 0 {
		partNumberMarkerPath := uploadIDPath + "." + strconv.Itoa(partNumberMarker) + "."
		fileInfos, _, err := fs.storage.ListFiles(minioMetaBucket, partNumberMarkerPath, "", false, 1)
		if err != nil {
			return result, toObjectErr(err, minioMetaBucket, partNumberMarkerPath)
		}
		if len(fileInfos) == 0 {
			return result, (InvalidPart{})
		}
		markerPath = fileInfos[0].Name
	}
	uploadIDPrefix := uploadIDPath + "."
	fileInfos, eof, err := fs.storage.ListFiles(minioMetaBucket, uploadIDPrefix, markerPath, false, maxParts)
	if err != nil {
		return result, InvalidPart{}
	}
	for _, fileInfo := range fileInfos {
		fileName := path.Base(fileInfo.Name)
		splitResult := strings.Split(fileName, ".")
		partNum, err := strconv.Atoi(splitResult[1])
		if err != nil {
			return result, err
		}
		md5sum := splitResult[2]
		result.Parts = append(result.Parts, partInfo{
			PartNumber:   partNum,
			LastModified: fileInfo.ModTime,
			ETag:         md5sum,
			Size:         fileInfo.Size,
		})
		nextPartNumberMarker = partNum
	}
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.PartNumberMarker = partNumberMarker
	result.NextPartNumberMarker = nextPartNumberMarker
	result.MaxParts = maxParts
	result.IsTruncated = !eof
	return result, nil
}

func (fs fsObjects) CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", (BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", (ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		})
	}
	if status, err := isUploadIDExists(fs.storage, bucket, object, uploadID); err != nil {
		return "", err
	} else if !status {
		return "", (InvalidUploadID{UploadID: uploadID})
	}

	fileWriter, err := fs.storage.CreateFile(bucket, object)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	var md5Sums []string
	for _, part := range parts {
		// Construct part suffix.
		partSuffix := fmt.Sprintf("%s.%d.%s", uploadID, part.PartNumber, part.ETag)
		var fileReader io.ReadCloser
		fileReader, err = fs.storage.ReadFile(minioMetaBucket, path.Join(bucket, object, partSuffix), 0)
		if err != nil {
			if err == errFileNotFound {
				return "", (InvalidPart{})
			}
			return "", err
		}
		_, err = io.Copy(fileWriter, fileReader)
		if err != nil {
			return "", err
		}
		err = fileReader.Close()
		if err != nil {
			return "", err
		}
		md5Sums = append(md5Sums, part.ETag)
	}

	err = fileWriter.Close()
	if err != nil {
		return "", err
	}

	// Save the s3 md5.
	s3MD5, err := makeS3MD5(md5Sums...)
	if err != nil {
		return "", err
	}

	// Cleanup all the parts.
	fs.removeMultipartUpload(bucket, object, uploadID)

	// Return md5sum.
	return s3MD5, nil
}

func (fs fsObjects) removeMultipartUpload(bucket, object, uploadID string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return (BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return (ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	marker := ""
	for {
		uploadIDPath := path.Join(bucket, object, uploadID)
		fileInfos, eof, err := fs.storage.ListFiles(minioMetaBucket, uploadIDPath, marker, false, 1000)
		if err != nil {
			if err == errFileNotFound {
				return (InvalidUploadID{UploadID: uploadID})
			}
			return toObjectErr(err)
		}
		for _, fileInfo := range fileInfos {
			fs.storage.DeleteFile(minioMetaBucket, fileInfo.Name)
			marker = fileInfo.Name
		}
		if eof {
			break
		}
	}
	return nil
}

func (fs fsObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return (BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return (ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	if status, err := isUploadIDExists(fs.storage, bucket, object, uploadID); err != nil {
		return err
	} else if !status {
		return (InvalidUploadID{UploadID: uploadID})
	}
	return fs.removeMultipartUpload(bucket, object, uploadID)
}
