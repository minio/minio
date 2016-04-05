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
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/probe"
	"github.com/skyrings/skyring-common/tools/uuid"
)

const configDir = ".minio"
const uploadIDSuffix = ".uploadid"

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

func safeWrite(fileName string, data io.Reader, size int64, md5sum string) error {
	tempFile, e := ioutil.TempFile(filepath.Dir(fileName), filepath.Base(fileName)+"-")
	if e != nil {
		return e
	}

	md5Hasher := md5.New()
	multiWriter := io.MultiWriter(md5Hasher, tempFile)
	if _, e := io.CopyN(multiWriter, data, size); e != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return e
	}
	tempFile.Close()

	dataMd5sum := hex.EncodeToString(md5Hasher.Sum(nil))
	if md5sum != "" && !isMD5SumEqual(md5sum, dataMd5sum) {
		os.Remove(tempFile.Name())
		return BadDigest{ExpectedMD5: md5sum, CalculatedMD5: dataMd5sum}
	}

	if e := os.Rename(tempFile.Name(), fileName); e != nil {
		os.Remove(tempFile.Name())
		return e
	}

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
	metaObjectDir := filepath.Join(fs.path, configDir, bucket, object)

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
		uploadIDFile := filepath.Join(metaObjectDir, uploadID+uploadIDSuffix)
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
	return isFileExist(filepath.Join(fs.path, configDir, bucket, object, uploadID+uploadIDSuffix))
}

func (fs Filesystem) cleanupUploadID(bucket, object, uploadID string) error {
	metaObjectDir := filepath.Join(fs.path, configDir, bucket, object)
	uploadIDPrefix := uploadID + "."

	names, e := filteredReaddirnames(metaObjectDir,
		func(name string) bool {
			return strings.HasPrefix(name, uploadIDPrefix)
		},
	)

	if e != nil {
		return e
	}

	for _, name := range names {
		if e := os.Remove(filepath.Join(metaObjectDir, name)); e != nil {
			//return InternalError{Err: err}
			return e
		}
	}

	if status, e := isDirEmpty(metaObjectDir); e != nil {
		// TODO: add log than returning error
		//return InternalError{Err: err}
		return e
	} else if status {
		if e := removeFileTree(metaObjectDir, filepath.Join(fs.path, configDir, bucket)); e != nil {
			// TODO: add log than returning error
			//return InternalError{Err: err}
			return e
		}
	}

	return nil
}

func (fs Filesystem) checkBucketArg(bucket string) (string, error) {
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}

	bucket = getActualBucketname(fs.path, bucket)
	if status, e := isDirExist(filepath.Join(fs.path, bucket)); e != nil {
		//return "", InternalError{Err: err}
		return "", e
	} else if !status {
		return "", BucketNotFound{Bucket: bucket}
	}

	return bucket, nil
}

func (fs Filesystem) checkDiskFree() error {
	di, e := disk.GetInfo(fs.path)
	if e != nil {
		return e
	}

	// Remove 5% from total space for cumulative disk space used for journalling, inodes etc.
	availableDiskSpace := (float64(di.Free) / (float64(di.Total) - (0.05 * float64(di.Total)))) * 100
	if int64(availableDiskSpace) <= fs.minFreeDisk {
		return RootPathFull{Path: fs.path}
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

	if e := fs.checkDiskFree(); e != nil {
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

	if e := fs.checkDiskFree(); e != nil {
		return "", probe.NewError(e)
	}

	partFile := filepath.Join(fs.path, configDir, bucket, object, uploadID+"."+strconv.Itoa(partNumber)+"."+md5Hex)
	if e := safeWrite(partFile, data, size, md5Hex); e != nil {
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

	if e := fs.checkDiskFree(); e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}

	metaObjectDir := filepath.Join(fs.path, configDir, bucket, object)

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

	tempFile, e := ioutil.TempFile(metaObjectDir, uploadID+".complete.")
	if e != nil {
		//return ObjectInfo{}, probe.NewError(InternalError{Err: err})
		return ObjectInfo{}, probe.NewError(e)
	}

	for _, part := range parts {
		partNumber := part.PartNumber
		md5sum := strings.Trim(part.ETag, "\"")
		partFile := filepath.Join(metaObjectDir, uploadID+"."+strconv.Itoa(partNumber)+"."+md5sum)
		var f *os.File
		f, e = os.Open(partFile)
		if e != nil {
			tempFile.Close()
			os.Remove(tempFile.Name())
			//return ObjectInfo{}, probe.NewError(InternalError{Err: err})
			return ObjectInfo{}, probe.NewError(e)
		} else if _, e = io.Copy(tempFile, f); e != nil {
			tempFile.Close()
			os.Remove(tempFile.Name())
			//return ObjectInfo{}, probe.NewError(InternalError{Err: err})
			return ObjectInfo{}, probe.NewError(e)
		}
		f.Close()
	}
	tempFile.Close()
	// fi is used later
	fi, e := os.Stat(tempFile.Name())
	if e != nil {
		os.Remove(tempFile.Name())
		return ObjectInfo{}, probe.NewError(e)
	}

	bucketPath := filepath.Join(fs.path, bucket)
	objectPath := filepath.Join(bucketPath, object)
	if e = os.MkdirAll(filepath.Dir(objectPath), 0755); e != nil {
		os.Remove(tempFile.Name())
		//return ObjectInfo{}, probe.NewError(InternalError{Err: err})
		return ObjectInfo{}, probe.NewError(e)
	}
	if e = os.Rename(tempFile.Name(), objectPath); e != nil {
		os.Remove(tempFile.Name())
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
		ModifiedTime: fi.ModTime(),
		Size:         fi.Size(),
		ContentType:  contentType,
		MD5Sum:       s3MD5,
	}

	return newObject, nil
}

// ListMultipartUploads - list incomplete multipart sessions for a given BucketMultipartResourcesMetadata
func (fs Filesystem) ListMultipartUploads(bucket, objectPrefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, *probe.Error) {
	result := ListMultipartsInfo{}

	if bucketDirName, err := fs.checkBucketArg(bucket); err == nil {
		bucket = bucketDirName
	} else {
		return result, probe.NewError(err)
	}

	if !IsValidObjectPrefix(objectPrefix) {
		return result, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: objectPrefix})
	}

	prefixPath := filepath.FromSlash(objectPrefix)

	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != "/" {
		return result, probe.NewError(fmt.Errorf("delimiter '%s' is not supported", delimiter))
	}

	// Unescape keyMarker string
	if tmpKeyMarker, err := url.QueryUnescape(keyMarker); err == nil {
		keyMarker = tmpKeyMarker
	} else {
		return result, probe.NewError(err)
	}

	if keyMarker != "" && !strings.HasPrefix(keyMarker, objectPrefix) {
		return result, probe.NewError(fmt.Errorf("Invalid combination of marker '%s' and prefix '%s'", keyMarker, objectPrefix))
	}

	markerPath := filepath.FromSlash(keyMarker)

	if uploadIDMarker != "" {
		if strings.HasSuffix(markerPath, string(os.PathSeparator)) {
			return result, probe.NewError(fmt.Errorf("Invalid combination of uploadID marker '%s' and marker '%s'", uploadIDMarker, keyMarker))
		}

		id, err := uuid.Parse(uploadIDMarker)

		if err != nil {
			return result, probe.NewError(err)
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
	skipDir := true
	if delimiter == "/" {
		skipDir = false
		recursive = false
	}

	bucketDir := filepath.Join(fs.path, bucket)
	// If listMultipartObjectChannel is available for given parameters, then use it, else create new one
	objectInfoCh := fs.popListMultipartObjectCh(listMultipartObjectParams{bucket, delimiter, markerPath, prefixPath, uploadIDMarker})
	if objectInfoCh == nil {
		ch := scanMultipartDir(bucketDir, objectPrefix, keyMarker, uploadIDMarker, recursive)
		objectInfoCh = &ch
	}

	nextKeyMarker := ""
	nextUploadIDMarker := ""
	for i := 0; i < maxUploads; {
		objInfo, ok := objectInfoCh.Read()
		if !ok {
			// Closed channel.
			return result, nil
		}

		if objInfo.Err != nil {
			return ListMultipartsInfo{}, probe.NewError(objInfo.Err)
		}

		if strings.Contains(objInfo.Name, "$multiparts") || strings.Contains(objInfo.Name, "$tmpobject") {
			continue
		}

		if objInfo.IsDir && skipDir {
			continue
		}

		if objInfo.IsDir {
			result.CommonPrefixes = append(result.CommonPrefixes, objInfo.Name)
		} else {
			result.Uploads = append(result.Uploads, uploadMetadata{Object: objInfo.Name, UploadID: objInfo.UploadID, Initiated: objInfo.ModifiedTime})
		}
		nextKeyMarker = objInfo.Name
		nextUploadIDMarker = objInfo.UploadID
		i++
	}

	if !objectInfoCh.IsClosed() {
		result.IsTruncated = true
		result.NextKeyMarker = nextKeyMarker
		result.NextUploadIDMarker = nextUploadIDMarker
		fs.pushListMultipartObjectCh(listMultipartObjectParams{bucket, delimiter, nextKeyMarker, objectPrefix, nextUploadIDMarker}, *objectInfoCh)
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

	metaObjectDir := filepath.Join(fs.path, configDir, bucket, object)
	entries, err := filteredReaddir(metaObjectDir,
		func(entry DirEntry) bool {
			if tokens := strings.Split(entry.Name, "."); len(tokens) == 3 {
				if tokens[0] == uploadID {
					if partNumber, err := strconv.Atoi(tokens[1]); err == nil {
						if partNumber >= 1 && partNumber <= 10000 && partNumber > partNumberMarker {
							return true
						}
					}
				}
			}

			return false
		},
		false,
	)

	if err != nil {
		return ListPartsInfo{}, probe.NewError(err)
	}

	isTruncated := false
	if maxParts <= 0 || maxParts > 1000 {
		maxParts = 1000
	}
	nextPartNumberMarker := 0

	parts := []partInfo{}
	for i := range entries {
		if i == maxParts {
			isTruncated = true
			break
		}

		tokens := strings.Split(entries[i].Name, ".")
		partNumber, _ := strconv.Atoi(tokens[1])
		md5sum := tokens[2]
		parts = append(parts, partInfo{
			PartNumber:   partNumber,
			LastModified: entries[i].ModTime,
			ETag:         md5sum,
			Size:         entries[i].Size,
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
