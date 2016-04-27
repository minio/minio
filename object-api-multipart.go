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
	"github.com/minio/minio/pkg/probe"
	"github.com/skyrings/skyring-common/tools/uuid"
)

const (
	// Minio meta volume.
	minioMetaVolume = ".minio"
)

// checks whether bucket exists.
func (o objectAPI) isBucketExist(bucketName string) (bool, error) {
	// Check whether bucket exists.
	if _, e := o.storage.StatVol(bucketName); e != nil {
		if e == errVolumeNotFound {
			return false, nil
		}
		return false, e
	}
	return true, nil
}

// listLeafEntries - lists all entries if a given prefixPath is a leaf
// directory, returns error if any - returns empty list if prefixPath
// is not a leaf directory.
func (o objectAPI) listLeafEntries(prefixPath string) (entries []FileInfo, e error) {
	var markerPath string
	for {
		fileInfos, eof, e := o.storage.ListFiles(minioMetaVolume, prefixPath, markerPath, false, 1000)
		if e != nil {
			log.WithFields(logrus.Fields{
				"prefixPath": prefixPath,
				"markerPath": markerPath,
			}).Errorf("%s", e)
			return nil, e
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

// listMetaVolumeFiles - list all files at a given prefix inside minioMetaVolume.
func (o objectAPI) listMetaVolumeFiles(prefixPath string, markerPath string, recursive bool, maxKeys int) (allFileInfos []FileInfo, eof bool, e error) {
	// newMaxKeys tracks the size of entries which are going to be
	// returned back.
	var newMaxKeys int

	// Following loop gathers and filters out special files inside
	// minio meta volume.
	for {
		var fileInfos []FileInfo
		// List files up to maxKeys-newMaxKeys, since we are skipping entries for special files.
		fileInfos, eof, e = o.storage.ListFiles(minioMetaVolume, prefixPath, markerPath, recursive, maxKeys-newMaxKeys)
		if e != nil {
			log.WithFields(logrus.Fields{
				"prefixPath": prefixPath,
				"markerPath": markerPath,
				"recursive":  recursive,
				"maxKeys":    maxKeys,
			}).Errorf("%s", e)
			return nil, true, e
		}
		// Loop through and validate individual file.
		for _, fi := range fileInfos {
			var entries []FileInfo
			if fi.Mode.IsDir() {
				// List all the entries if fi.Name is a leaf directory, if
				// fi.Name is not a leaf directory them the resulting
				// entries are empty.
				entries, e = o.listLeafEntries(fi.Name)
				if e != nil {
					log.WithFields(logrus.Fields{
						"prefixPath": fi.Name,
					}).Errorf("%s", e)
					return nil, false, e
				}
			}
			// Set markerPath for next batch of listing.
			markerPath = fi.Name
			if len(entries) > 0 {
				for _, entry := range entries {
					// Skip the entries for erasure parts if any.
					if strings.Contains(path.Base(entry.Name), ".") {
						continue
					}
					allFileInfos = append(allFileInfos, entry)
				}
			} else {
				// Skip special files.
				specialFile := path.Base(fi.Name)
				if strings.Contains(specialFile, ".") {
					// Contains partnumber and md5sum info, skip this.
					continue
				}
				allFileInfos = append(allFileInfos, fi)
			}
			newMaxKeys++
			// If we have reached the maxKeys, it means we have listed
			// everything that was requested. Return right here.
			if newMaxKeys == maxKeys {
				// Returns all the entries until maxKeys entries.
				//
				// eof is deliberately set as false since most of the
				// time if newMaxKeys == maxKeys, there are most
				// probably more than 1000 multipart sessions in
				// progress.
				//
				// Setting this here allows us to set proper Markers
				// so that the subsequent call returns the next set of
				// entries.
				eof = false
				return allFileInfos, eof, nil
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
func (o objectAPI) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, *probe.Error) {
	result := ListMultipartsInfo{}
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListMultipartsInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectPrefix(prefix) {
		return ListMultipartsInfo{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: prefix})
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return ListMultipartsInfo{}, probe.NewError(UnsupportedDelimiter{
			Delimiter: delimiter,
		})
	}
	// Verify if marker has prefix.
	if keyMarker != "" && !strings.HasPrefix(keyMarker, prefix) {
		return ListMultipartsInfo{}, probe.NewError(InvalidMarkerPrefixCombination{
			Marker: keyMarker,
			Prefix: prefix,
		})
	}
	if uploadIDMarker != "" {
		if strings.HasSuffix(keyMarker, slashSeparator) {
			return result, probe.NewError(InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			})
		}
		id, e := uuid.Parse(uploadIDMarker)
		if e != nil {
			return result, probe.NewError(e)
		}
		if id.IsZero() {
			return result, probe.NewError(MalformedUploadID{
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
		keyMarkerPath = path.Join(bucket, keyMarker, uploadIDMarker)
	}
	// List all the multipart files at prefixPath, starting with
	// marker keyMarkerPath.
	fileInfos, eof, e := o.listMetaVolumeFiles(prefixPath, keyMarkerPath, recursive, maxUploads)
	if e != nil {
		log.WithFields(logrus.Fields{
			"prefixPath": prefixPath,
			"markerPath": keyMarkerPath,
			"recursive":  recursive,
			"maxUploads": maxUploads,
		}).Errorf("listMetaVolumeFiles failed with %s", e)
		return ListMultipartsInfo{}, probe.NewError(e)
	}
	for _, fi := range fileInfos {
		var objectName string
		var uploadID string
		if fi.Mode.IsDir() {
			objectName = strings.TrimPrefix(fi.Name, retainSlash(bucket))
			// For a directory entry
			result.CommonPrefixes = append(result.CommonPrefixes, objectName)
			continue
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

func (o objectAPI) NewMultipartUpload(bucket, object string) (string, *probe.Error) {
	// Verify if bucket name is valid.
	if !IsValidBucketName(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	// Verify if object name is valid.
	if !IsValidObjectName(object) {
		return "", probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	// Verify whether the bucket exists.
	isExist, err := o.isBucketExist(bucket)
	if err != nil {
		return "", probe.NewError(err)
	}
	if !isExist {
		return "", probe.NewError(BucketNotFound{Bucket: bucket})
	}

	if _, e := o.storage.StatVol(minioMetaVolume); e != nil {
		if e == errVolumeNotFound {
			e = o.storage.MakeVol(minioMetaVolume)
			if e != nil {
				if e == errDiskFull {
					return "", probe.NewError(StorageFull{})
				}
				return "", probe.NewError(e)
			}
		}
	}
	for {
		uuid, e := uuid.New()
		if e != nil {
			return "", probe.NewError(e)
		}
		uploadID := uuid.String()
		uploadIDPath := path.Join(bucket, object, uploadID)
		if _, e = o.storage.StatFile(minioMetaVolume, uploadIDPath); e != nil {
			if e != errFileNotFound {
				return "", probe.NewError(toObjectErr(e, minioMetaVolume, uploadIDPath))
			}
			// uploadIDPath doesn't exist, so create empty file to reserve the name
			var w io.WriteCloser
			if w, e = o.storage.CreateFile(minioMetaVolume, uploadIDPath); e == nil {
				// Close the writer.
				if e = w.Close(); e != nil {
					return "", probe.NewError(e)
				}
			} else {
				return "", probe.NewError(toObjectErr(e, minioMetaVolume, uploadIDPath))
			}
			return uploadID, nil
		}
		// uploadIDPath already exists.
		// loop again to try with different uuid generated.
	}
}

// isUploadIDExists - verify if a given uploadID exists and is valid.
func (o objectAPI) isUploadIDExists(bucket, object, uploadID string) (bool, error) {
	uploadIDPath := path.Join(bucket, object, uploadID)
	st, e := o.storage.StatFile(minioMetaVolume, uploadIDPath)
	if e != nil {
		// Upload id does not exist.
		if e == errFileNotFound {
			return false, nil
		}
		return false, e
	}
	// Upload id exists and is a regular file.
	return st.Mode.IsRegular(), nil
}

// PutObjectPart - writes the multipart upload chunks.
func (o objectAPI) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, *probe.Error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	// Verify whether the bucket exists.
	isExist, err := o.isBucketExist(bucket)
	if err != nil {
		return "", probe.NewError(err)
	}
	if !isExist {
		return "", probe.NewError(BucketNotFound{Bucket: bucket})
	}

	if status, e := o.isUploadIDExists(bucket, object, uploadID); e != nil {
		return "", probe.NewError(e)
	} else if !status {
		return "", probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	partSuffix := fmt.Sprintf("%s.%d.%s", uploadID, partID, md5Hex)
	fileWriter, e := o.storage.CreateFile(minioMetaVolume, path.Join(bucket, object, partSuffix))
	if e != nil {
		return "", probe.NewError(toObjectErr(e, bucket, object))
	}

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Instantiate a new multi writer.
	multiWriter := io.MultiWriter(md5Writer, fileWriter)

	// Instantiate checksum hashers and create a multiwriter.
	if size > 0 {
		if _, e = io.CopyN(multiWriter, data, size); e != nil {
			safeCloseAndRemove(fileWriter)
			return "", probe.NewError(toObjectErr(e))
		}
		// Reader shouldn't have more data what mentioned in size argument.
		// reading one more byte from the reader to validate it.
		// expected to fail, success validates existence of more data in the reader.
		if _, e = io.CopyN(ioutil.Discard, data, 1); e == nil {
			safeCloseAndRemove(fileWriter)
			return "", probe.NewError(UnExpectedDataSize{Size: int(size)})
		}
	} else {
		if _, e = io.Copy(multiWriter, data); e != nil {
			safeCloseAndRemove(fileWriter)
			return "", probe.NewError(toObjectErr(e))
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			safeCloseAndRemove(fileWriter)
			return "", probe.NewError(BadDigest{md5Hex, newMD5Hex})
		}
	}
	e = fileWriter.Close()
	if e != nil {
		return "", probe.NewError(e)
	}
	return newMD5Hex, nil
}

func (o objectAPI) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, *probe.Error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListPartsInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return ListPartsInfo{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	if status, e := o.isUploadIDExists(bucket, object, uploadID); e != nil {
		return ListPartsInfo{}, probe.NewError(e)
	} else if !status {
		return ListPartsInfo{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	result := ListPartsInfo{}
	var markerPath string
	nextPartNumberMarker := 0
	uploadIDPath := path.Join(bucket, object, uploadID)
	// Figure out the marker for the next subsequent calls, if the
	// partNumberMarker is already set.
	if partNumberMarker > 0 {
		partNumberMarkerPath := uploadIDPath + "." + strconv.Itoa(partNumberMarker) + "."
		fileInfos, _, e := o.storage.ListFiles(minioMetaVolume, partNumberMarkerPath, "", false, 1)
		if e != nil {
			return result, probe.NewError(toObjectErr(e, minioMetaVolume, partNumberMarkerPath))
		}
		if len(fileInfos) == 0 {
			return result, probe.NewError(InvalidPart{})
		}
		markerPath = fileInfos[0].Name
	}
	uploadIDPrefix := uploadIDPath + "."
	fileInfos, eof, e := o.storage.ListFiles(minioMetaVolume, uploadIDPrefix, markerPath, false, maxParts)
	if e != nil {
		return result, probe.NewError(InvalidPart{})
	}
	for _, fileInfo := range fileInfos {
		fileName := path.Base(fileInfo.Name)
		splitResult := strings.Split(fileName, ".")
		partNum, e := strconv.Atoi(splitResult[1])
		if e != nil {
			return result, probe.NewError(e)
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

// Create an s3 compatible MD5sum for complete multipart transaction.
func makeS3MD5(md5Strs ...string) (string, *probe.Error) {
	var finalMD5Bytes []byte
	for _, md5Str := range md5Strs {
		md5Bytes, e := hex.DecodeString(md5Str)
		if e != nil {
			return "", probe.NewError(e)
		}
		finalMD5Bytes = append(finalMD5Bytes, md5Bytes...)
	}
	md5Hasher := md5.New()
	md5Hasher.Write(finalMD5Bytes)
	s3MD5 := fmt.Sprintf("%s-%d", hex.EncodeToString(md5Hasher.Sum(nil)), len(md5Strs))
	return s3MD5, nil
}

func (o objectAPI) CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (string, *probe.Error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", probe.NewError(ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		})
	}
	if status, e := o.isUploadIDExists(bucket, object, uploadID); e != nil {
		return "", probe.NewError(e)
	} else if !status {
		return "", probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	fileWriter, e := o.storage.CreateFile(bucket, object)
	if e != nil {
		return "", probe.NewError(toObjectErr(e, bucket, object))
	}

	var md5Sums []string
	for _, part := range parts {
		// Construct part suffix.
		partSuffix := fmt.Sprintf("%s.%d.%s", uploadID, part.PartNumber, part.ETag)
		var fileReader io.ReadCloser
		fileReader, e = o.storage.ReadFile(minioMetaVolume, path.Join(bucket, object, partSuffix), 0)
		if e != nil {
			if e == errFileNotFound {
				return "", probe.NewError(InvalidPart{})
			}
			return "", probe.NewError(e)
		}
		_, e = io.Copy(fileWriter, fileReader)
		if e != nil {
			return "", probe.NewError(e)
		}
		e = fileReader.Close()
		if e != nil {
			return "", probe.NewError(e)
		}
		md5Sums = append(md5Sums, part.ETag)
	}

	e = fileWriter.Close()
	if e != nil {
		return "", probe.NewError(e)
	}

	// Save the s3 md5.
	s3MD5, err := makeS3MD5(md5Sums...)
	if err != nil {
		return "", err.Trace(md5Sums...)
	}

	// Cleanup all the parts.
	o.removeMultipartUpload(bucket, object, uploadID)

	// Return md5sum.
	return s3MD5, nil
}

func (o objectAPI) removeMultipartUpload(bucket, object, uploadID string) *probe.Error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	marker := ""
	for {
		uploadIDPath := path.Join(bucket, object, uploadID)
		fileInfos, eof, e := o.storage.ListFiles(minioMetaVolume, uploadIDPath, marker, false, 1000)
		if e != nil {

			return probe.NewError(InvalidUploadID{UploadID: uploadID})
		}
		for _, fileInfo := range fileInfos {
			o.storage.DeleteFile(minioMetaVolume, fileInfo.Name)
			marker = fileInfo.Name
		}
		if eof {
			break
		}
	}
	return nil
}

func (o objectAPI) AbortMultipartUpload(bucket, object, uploadID string) *probe.Error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	if status, e := o.isUploadIDExists(bucket, object, uploadID); e != nil {
		return probe.NewError(e)
	} else if !status {
		return probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	err := o.removeMultipartUpload(bucket, object, uploadID)
	if err != nil {
		return err.Trace(bucket, object, uploadID)
	}
	return nil
}
