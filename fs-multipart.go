/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/safe"
	"github.com/skyrings/skyring-common/tools/uuid"
)

const (
	minioMetaDir            = ".minio"
	multipartUploadIDSuffix = ".uploadid"
)

// Removes files and its parent directories up to a given level.
func removeFileTree(fileName string, level string) error {
	if e := os.Remove(fileName); e != nil {
		return e
	}

	for fileDir := filepath.Dir(fileName); fileDir > level; fileDir = filepath.Dir(fileDir) {
		if status, e := isDirEmpty(fileDir); e != nil {
			return e
		} else if !status {
			break
		}
		if e := os.Remove(fileDir); e != nil {
			return e
		}
	}

	return nil
}

// Takes an input stream and safely writes to disk, additionally
// verifies checksum.
func safeWriteFile(fileName string, data io.Reader, size int64, md5sum string) error {
	safeFile, e := safe.CreateFileWithSuffix(fileName, "-")
	if e != nil {
		return e
	}

	md5Hasher := md5.New()
	multiWriter := io.MultiWriter(md5Hasher, safeFile)
	if size > 0 {
		if _, e = io.CopyN(multiWriter, data, size); e != nil {
			// Closes the file safely and removes it in a single atomic operation.
			safeFile.CloseAndRemove()
			return e
		}
	} else {
		if _, e = io.Copy(multiWriter, data); e != nil {
			// Closes the file safely and removes it in a single atomic operation.
			safeFile.CloseAndRemove()
			return e
		}
	}

	dataMd5sum := hex.EncodeToString(md5Hasher.Sum(nil))
	if md5sum != "" && !isMD5SumEqual(md5sum, dataMd5sum) {
		// Closes the file safely and removes it in a single atomic operation.
		safeFile.CloseAndRemove()
		return BadDigest{ExpectedMD5: md5sum, CalculatedMD5: dataMd5sum}
	}

	// Safely close the file and atomically renames it the actual filePath.
	safeFile.Close()

	// Safely wrote the file.
	return nil
}

func isFileExist(filename string) (bool, error) {
	fi, e := os.Lstat(filename)
	if e != nil {
		if os.IsNotExist(e) {
			return false, nil
		}

		return false, e
	}

	return fi.Mode().IsRegular(), nil
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

func (fs Filesystem) newUploadID(bucket, object string) (string, error) {
	metaObjectDir := filepath.Join(fs.diskPath, minioMetaDir, bucket, object)

	// create metaObjectDir if not exist
	if status, e := isDirExist(metaObjectDir); e != nil {
		return "", e
	} else if !status {
		if e := os.MkdirAll(metaObjectDir, 0755); e != nil {
			return "", e
		}
	}

	for {
		uuid, e := uuid.New()
		if e != nil {
			return "", e
		}

		uploadID := uuid.String()
		uploadIDFile := filepath.Join(metaObjectDir, uploadID+multipartUploadIDSuffix)
		if _, e := os.Lstat(uploadIDFile); e != nil {
			if !os.IsNotExist(e) {
				return "", e
			}

			// uploadIDFile doesn't exist, so create empty file to reserve the name
			if e := ioutil.WriteFile(uploadIDFile, []byte{}, 0644); e != nil {
				return "", e
			}

			return uploadID, nil
		}
		// uploadIDFile already exists.
		// loop again to try with different uuid generated.
	}
}

func (fs Filesystem) isUploadIDExist(bucket, object, uploadID string) (bool, error) {
	return isFileExist(filepath.Join(fs.diskPath, minioMetaDir, bucket, object, uploadID+multipartUploadIDSuffix))
}

func (fs Filesystem) cleanupUploadID(bucket, object, uploadID string) error {
	metaObjectDir := filepath.Join(fs.diskPath, minioMetaDir, bucket, object)
	uploadIDPrefix := uploadID + "."

	dirents, e := scandir(metaObjectDir,
		func(dirent fsDirent) bool {
			return dirent.IsRegular() && strings.HasPrefix(dirent.name, uploadIDPrefix)
		},
		true)

	if e != nil {
		return e
	}

	for _, dirent := range dirents {
		if e := os.Remove(filepath.Join(metaObjectDir, dirent.name)); e != nil {
			return e
		}
	}

	if status, e := isDirEmpty(metaObjectDir); e != nil {
		return e
	} else if status {
		if e := removeFileTree(metaObjectDir, filepath.Join(fs.diskPath, minioMetaDir, bucket)); e != nil {
			return e
		}
	}

	return nil
}

func (fs Filesystem) checkMultipartArgs(bucket, object string) (string, error) {
	bucket, e := fs.checkBucketArg(bucket)
	if e != nil {
		return "", e
	}

	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{Object: object}
	}

	return bucket, nil
}

// NewMultipartUpload - initiate a new multipart session
func (fs Filesystem) NewMultipartUpload(bucket, object string) (string, *probe.Error) {
	if bucketDirName, e := fs.checkMultipartArgs(bucket, object); e == nil {
		bucket = bucketDirName
	} else {
		return "", probe.NewError(e)
	}

	if e := checkDiskFree(fs.diskPath, fs.minFreeDisk); e != nil {
		return "", probe.NewError(e)
	}

	uploadID, e := fs.newUploadID(bucket, object)
	if e != nil {
		return "", probe.NewError(e)
	}

	return uploadID, nil
}

// PutObjectPart - create a part in a multipart session
func (fs Filesystem) PutObjectPart(bucket, object, uploadID string, partNumber int, size int64, data io.Reader, md5Hex string) (string, *probe.Error) {
	if bucketDirName, e := fs.checkMultipartArgs(bucket, object); e == nil {
		bucket = bucketDirName
	} else {
		return "", probe.NewError(e)
	}

	if status, e := fs.isUploadIDExist(bucket, object, uploadID); e != nil {
		//return "", probe.NewError(InternalError{Err: err})
		return "", probe.NewError(e)
	} else if !status {
		return "", probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	// Part id cannot be negative.
	if partNumber <= 0 {
		return "", probe.NewError(errors.New("invalid part id, cannot be zero or less than zero"))
	}

	if partNumber > 10000 {
		return "", probe.NewError(errors.New("invalid part id, should be not more than 10000"))
	}

	if e := checkDiskFree(fs.diskPath, fs.minFreeDisk); e != nil {
		return "", probe.NewError(e)
	}

	partSuffix := fmt.Sprintf("%s.%d.%s", uploadID, partNumber, md5Hex)
	partFilePath := filepath.Join(fs.diskPath, minioMetaDir, bucket, object, partSuffix)
	if e := safeWriteFile(partFilePath, data, size, md5Hex); e != nil {
		return "", probe.NewError(e)
	}
	return md5Hex, nil
}

// AbortMultipartUpload - abort an incomplete multipart session
func (fs Filesystem) AbortMultipartUpload(bucket, object, uploadID string) *probe.Error {
	if bucketDirName, e := fs.checkMultipartArgs(bucket, object); e == nil {
		bucket = bucketDirName
	} else {
		return probe.NewError(e)
	}

	if status, e := fs.isUploadIDExist(bucket, object, uploadID); e != nil {
		//return probe.NewError(InternalError{Err: err})
		return probe.NewError(e)
	} else if !status {
		return probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	if e := fs.cleanupUploadID(bucket, object, uploadID); e != nil {
		return probe.NewError(e)
	}

	return nil
}

// CompleteMultipartUpload - complete a multipart upload and persist the data
func (fs Filesystem) CompleteMultipartUpload(bucket, object, uploadID string, parts []completePart) (ObjectInfo, *probe.Error) {
	if bucketDirName, e := fs.checkMultipartArgs(bucket, object); e == nil {
		bucket = bucketDirName
	} else {
		return ObjectInfo{}, probe.NewError(e)
	}

	if status, e := fs.isUploadIDExist(bucket, object, uploadID); e != nil {
		//return probe.NewError(InternalError{Err: err})
		return ObjectInfo{}, probe.NewError(e)
	} else if !status {
		return ObjectInfo{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	if e := checkDiskFree(fs.diskPath, fs.minFreeDisk); e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}

	metaObjectDir := filepath.Join(fs.diskPath, minioMetaDir, bucket, object)

	var md5Sums []string
	for _, part := range parts {
		partNumber := part.PartNumber
		md5sum := strings.Trim(part.ETag, "\"")
		partFile := filepath.Join(metaObjectDir, uploadID+"."+strconv.Itoa(partNumber)+"."+md5sum)
		if status, err := isFileExist(partFile); err != nil {
			return ObjectInfo{}, probe.NewError(err)
		} else if !status {
			return ObjectInfo{}, probe.NewError(InvalidPart{})
		}
		md5Sums = append(md5Sums, md5sum)
	}

	// Save the s3 md5.
	s3MD5, err := makeS3MD5(md5Sums...)
	if err != nil {
		return ObjectInfo{}, err.Trace(md5Sums...)
	}

	completeObjectFile := filepath.Join(metaObjectDir, uploadID+".complete.")
	safeFile, e := safe.CreateFileWithSuffix(completeObjectFile, "-")
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}
	for _, part := range parts {
		partNumber := part.PartNumber
		// Trim off the odd double quotes from ETag in the beginning and end.
		md5sum := strings.TrimPrefix(part.ETag, "\"")
		md5sum = strings.TrimSuffix(md5sum, "\"")
		partFileStr := filepath.Join(metaObjectDir, fmt.Sprintf("%s.%d.%s", uploadID, partNumber, md5sum))
		var partFile *os.File
		partFile, e = os.Open(partFileStr)
		if e != nil {
			// Remove the complete file safely.
			safeFile.CloseAndRemove()
			return ObjectInfo{}, probe.NewError(e)
		} else if _, e = io.Copy(safeFile, partFile); e != nil {
			// Remove the complete file safely.
			safeFile.CloseAndRemove()
			return ObjectInfo{}, probe.NewError(e)
		}
		partFile.Close() // Close part file after successful copy.
	}
	// All parts concatenated, safely close the temp file.
	safeFile.Close()

	// Stat to gather fresh stat info.
	objSt, e := os.Stat(completeObjectFile)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}

	bucketPath := filepath.Join(fs.diskPath, bucket)
	objectPath := filepath.Join(bucketPath, object)
	if e = os.MkdirAll(filepath.Dir(objectPath), 0755); e != nil {
		os.Remove(completeObjectFile)
		return ObjectInfo{}, probe.NewError(e)
	}
	if e = os.Rename(completeObjectFile, objectPath); e != nil {
		os.Remove(completeObjectFile)
		return ObjectInfo{}, probe.NewError(e)
	}

	fs.cleanupUploadID(bucket, object, uploadID) // TODO: handle and log the error

	contentType := "application/octet-stream"
	if objectExt := filepath.Ext(objectPath); objectExt != "" {
		if content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]; ok {
			contentType = content.ContentType
		}
	}

	newObject := ObjectInfo{
		Bucket:       bucket,
		Name:         object,
		ModifiedTime: objSt.ModTime(),
		Size:         objSt.Size(),
		ContentType:  contentType,
		MD5Sum:       s3MD5,
	}

	return newObject, nil
}

func (fs *Filesystem) saveListMultipartObjectCh(params listMultipartObjectParams, ch multipartObjectInfoChannel) {
	fs.listMultipartObjectMapMutex.Lock()
	defer fs.listMultipartObjectMapMutex.Unlock()

	channels := []multipartObjectInfoChannel{ch}
	if _, ok := fs.listMultipartObjectMap[params]; ok {
		channels = append(fs.listMultipartObjectMap[params], ch)
	}

	fs.listMultipartObjectMap[params] = channels
}

func (fs *Filesystem) lookupListMultipartObjectCh(params listMultipartObjectParams) *multipartObjectInfoChannel {
	fs.listMultipartObjectMapMutex.Lock()
	defer fs.listMultipartObjectMapMutex.Unlock()

	if channels, ok := fs.listMultipartObjectMap[params]; ok {
		for i, channel := range channels {
			if !channel.IsTimedOut() {
				chs := channels[i+1:]
				if len(chs) > 0 {
					fs.listMultipartObjectMap[params] = chs
				} else {
					delete(fs.listMultipartObjectMap, params)
				}

				return &channel
			}
		}

		// As all channels are timed out, delete the map entry
		delete(fs.listMultipartObjectMap, params)
	}
	return nil
}

// ListMultipartUploads - list incomplete multipart sessions for a given BucketMultipartResourcesMetadata
func (fs Filesystem) ListMultipartUploads(bucket, objectPrefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, *probe.Error) {
	result := ListMultipartsInfo{}

	bucket, e := fs.checkBucketArg(bucket)
	if e != nil {
		return result, probe.NewError(e)
	}

	if !IsValidObjectPrefix(objectPrefix) {
		return result, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: objectPrefix})
	}

	prefixPath := filepath.FromSlash(objectPrefix)

	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != "/" {
		return result, probe.NewError(fmt.Errorf("delimiter '%s' is not supported", delimiter))
	}

	if keyMarker != "" && !strings.HasPrefix(keyMarker, objectPrefix) {
		return result, probe.NewError(fmt.Errorf("Invalid combination of marker '%s' and prefix '%s'", keyMarker, objectPrefix))
	}

	markerPath := filepath.FromSlash(keyMarker)
	if uploadIDMarker != "" {
		if strings.HasSuffix(markerPath, string(os.PathSeparator)) {
			return result, probe.NewError(fmt.Errorf("Invalid combination of uploadID marker '%s' and marker '%s'", uploadIDMarker, keyMarker))
		}
		id, e := uuid.Parse(uploadIDMarker)
		if e != nil {
			return result, probe.NewError(e)
		}
		if id.IsZero() {
			return result, probe.NewError(fmt.Errorf("Invalid upload ID marker %s", uploadIDMarker))
		}
	}

	// Return empty response if maxUploads is zero
	if maxUploads == 0 {
		return result, nil
	}

	// set listObjectsLimit to maxUploads for out-of-range limit
	if maxUploads < 0 || maxUploads > listObjectsLimit {
		maxUploads = listObjectsLimit
	}

	recursive := true
	if delimiter == "/" {
		recursive = false
	}

	metaBucketDir := filepath.Join(fs.diskPath, minioMetaDir, bucket)
	// Lookup of if listMultipartObjectChannel is available for given
	// parameters, else create a new one.
	multipartObjectInfoCh := fs.lookupListMultipartObjectCh(listMultipartObjectParams{
		bucket:         bucket,
		delimiter:      delimiter,
		keyMarker:      markerPath,
		prefix:         prefixPath,
		uploadIDMarker: uploadIDMarker,
	})
	if multipartObjectInfoCh == nil {
		ch := scanMultipartDir(metaBucketDir, objectPrefix, keyMarker, uploadIDMarker, recursive)
		multipartObjectInfoCh = &ch
	}

	nextKeyMarker := ""
	nextUploadIDMarker := ""
	for i := 0; i < maxUploads; {
		multipartObjInfo, ok := multipartObjectInfoCh.Read()
		if !ok {
			// Closed channel.
			return result, nil
		}

		if multipartObjInfo.Err != nil {
			if os.IsNotExist(multipartObjInfo.Err) {
				return ListMultipartsInfo{}, nil
			}
			return ListMultipartsInfo{}, probe.NewError(multipartObjInfo.Err)
		}

		if strings.Contains(multipartObjInfo.Name, "$multiparts") ||
			strings.Contains(multipartObjInfo.Name, "$tmpobject") {
			continue
		}

		// Directories are listed only if recursive is false
		if multipartObjInfo.IsDir {
			result.CommonPrefixes = append(result.CommonPrefixes, multipartObjInfo.Name)
		} else {
			result.Uploads = append(result.Uploads, uploadMetadata{
				Object:    multipartObjInfo.Name,
				UploadID:  multipartObjInfo.UploadID,
				Initiated: multipartObjInfo.ModifiedTime,
			})
		}
		nextKeyMarker = multipartObjInfo.Name
		nextUploadIDMarker = multipartObjInfo.UploadID
		i++
	}

	if !multipartObjectInfoCh.IsClosed() {
		result.IsTruncated = true
		result.NextKeyMarker = nextKeyMarker
		result.NextUploadIDMarker = nextUploadIDMarker
		fs.saveListMultipartObjectCh(listMultipartObjectParams{
			bucket:         bucket,
			delimiter:      delimiter,
			keyMarker:      nextKeyMarker,
			prefix:         objectPrefix,
			uploadIDMarker: nextUploadIDMarker,
		}, *multipartObjectInfoCh)
	}

	return result, nil
}

// ListObjectParts - list parts from incomplete multipart session for a given ObjectResourcesMetadata
func (fs Filesystem) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, *probe.Error) {
	if bucketDirName, err := fs.checkMultipartArgs(bucket, object); err == nil {
		bucket = bucketDirName
	} else {
		return ListPartsInfo{}, probe.NewError(err)
	}

	if status, err := fs.isUploadIDExist(bucket, object, uploadID); err != nil {
		//return probe.NewError(InternalError{Err: err})
		return ListPartsInfo{}, probe.NewError(err)
	} else if !status {
		return ListPartsInfo{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	// return empty ListPartsInfo
	if maxParts == 0 {
		return ListPartsInfo{}, nil
	}

	if maxParts < 0 || maxParts > 1000 {
		maxParts = 1000
	}

	metaObjectDir := filepath.Join(fs.diskPath, minioMetaDir, bucket, object)
	uploadIDPrefix := uploadID + "."

	dirents, e := scandir(metaObjectDir,
		func(dirent fsDirent) bool {
			// Part file is a regular file and has to be started with 'UPLOADID.'
			if !(dirent.IsRegular() && strings.HasPrefix(dirent.name, uploadIDPrefix)) {
				return false
			}

			// Valid part file has to be 'UPLOADID.PARTNUMBER.MD5SUM'
			tokens := strings.Split(dirent.name, ".")
			if len(tokens) != 3 {
				return false
			}

			if partNumber, err := strconv.Atoi(tokens[1]); err == nil {
				if partNumber >= 1 && partNumber <= 10000 && partNumber > partNumberMarker {
					return true
				}
			}

			return false
		},
		true)
	if e != nil {
		return ListPartsInfo{}, probe.NewError(e)
	}

	isTruncated := false
	nextPartNumberMarker := 0

	parts := []partInfo{}
	for i := range dirents {
		if i == maxParts {
			isTruncated = true
			break
		}

		// In some OS modTime is empty and use os.Stat() to fill missing values
		if dirents[i].modTime.IsZero() {
			if fi, e := os.Stat(filepath.Join(metaObjectDir, dirents[i].name)); e == nil {
				dirents[i].modTime = fi.ModTime()
				dirents[i].size = fi.Size()
			} else {
				return ListPartsInfo{}, probe.NewError(e)
			}
		}

		tokens := strings.Split(dirents[i].name, ".")
		partNumber, _ := strconv.Atoi(tokens[1])
		md5sum := tokens[2]
		parts = append(parts, partInfo{
			PartNumber:   partNumber,
			LastModified: dirents[i].modTime,
			ETag:         md5sum,
			Size:         dirents[i].size,
		})
	}

	if isTruncated {
		nextPartNumberMarker = 0
	}

	return ListPartsInfo{
		Bucket:               bucket,
		Object:               object,
		UploadID:             uploadID,
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             maxParts,
		IsTruncated:          isTruncated,
		Parts:                parts,
	}, nil
}
