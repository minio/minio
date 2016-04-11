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
	"path/filepath"
	"strconv"
	"strings"

	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/safe"
	"github.com/skyrings/skyring-common/tools/uuid"
)

const (
	minioMetaVolume    = ".minio"
	slashPathSeparator = "/"
)

// checkLeafDirectory - verifies if a given path is leaf directory if
// yes returns all the files inside it.
func (o objectAPI) checkLeafDirectory(prefixPath string) (isLeaf bool, fis []FileInfo) {
	var allFileInfos []FileInfo
	for {
		fileInfos, eof, e := o.storage.ListFiles(minioMetaVolume, prefixPath, "", false, 1000)
		if e != nil {
			break
		}
		allFileInfos = append(allFileInfos, fileInfos...)
		if eof {
			break
		}
	}
	for _, fileInfo := range allFileInfos {
		if fileInfo.Mode.IsDir() {
			isLeaf = false
			return isLeaf, nil
		}
		fileName := path.Base(fileInfo.Name)
		if !strings.Contains(fileName, ".") {
			fis = append(fis, fileInfo)
		}
	}
	isLeaf = true
	return isLeaf, fis
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
	if delimiter != "" && delimiter != slashPathSeparator {
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
		if strings.HasSuffix(keyMarker, slashPathSeparator) {
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
	if delimiter == slashPathSeparator {
		recursive = false
	}

	prefixPath := path.Join(bucket, prefix) + slashPathSeparator
	fileInfos, eof, e := o.storage.ListFiles(minioMetaVolume, prefixPath, keyMarker+uploadIDMarker, recursive, maxUploads)
	if e != nil {
		return ListMultipartsInfo{}, probe.NewError(e)
	}

	result.IsTruncated = !eof
	for _, fileInfo := range fileInfos {
		if fileInfo.Mode.IsDir() {
			isLeaf, fis := o.checkLeafDirectory(fileInfo.Name)
			if isLeaf {
				fileName := strings.Replace(fileInfo.Name, bucket+slashPathSeparator, "", 1)
				fileName = path.Clean(fileName)
				for _, newFileInfo := range fis {
					newFileName := path.Base(newFileInfo.Name)
					result.Uploads = append(result.Uploads, uploadMetadata{
						Object:    fileName,
						UploadID:  newFileName,
						Initiated: newFileInfo.ModTime,
					})
				}
			} else {
				dirName := strings.Replace(fileInfo.Name, bucket+slashPathSeparator, "", 1)
				result.CommonPrefixes = append(result.CommonPrefixes, dirName+slashPathSeparator)
			}
		} else {
			fileName := path.Base(fileInfo.Name)
			fileDir := strings.Replace(path.Dir(fileInfo.Name), bucket+slashPathSeparator, "", 1)
			if !strings.Contains(fileName, ".") {
				result.Uploads = append(result.Uploads, uploadMetadata{
					Object:    fileDir,
					UploadID:  fileName,
					Initiated: fileInfo.ModTime,
				})
			}
		}
	}
	return result, nil
}

func (o objectAPI) NewMultipartUpload(bucket, object string) (string, *probe.Error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	if _, e := o.storage.StatVol(minioMetaVolume); e != nil {
		if e == errVolumeNotFound {
			e = o.storage.MakeVol(minioMetaVolume)
			if e != nil {
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
		uploadIDFile := filepath.Join(bucket, object, uploadID)
		if _, e = o.storage.StatFile(minioMetaVolume, uploadIDFile); e != nil {
			if e != errFileNotFound {
				return "", probe.NewError(e)
			}
			// uploadIDFile doesn't exist, so create empty file to reserve the name
			var w io.WriteCloser
			if w, e = o.storage.CreateFile(minioMetaVolume, uploadIDFile); e == nil {
				if e = w.Close(); e != nil {
					return "", probe.NewError(e)
				}
			} else {
				return "", probe.NewError(e)
			}
			return uploadID, nil
		}
		// uploadIDFile already exists.
		// loop again to try with different uuid generated.
	}
}

func (o objectAPI) isUploadIDExist(bucket, object, uploadID string) (bool, error) {
	st, e := o.storage.StatFile(minioMetaVolume, filepath.Join(bucket, object, uploadID))
	if e != nil {
		if e == errFileNotFound {
			return false, nil
		}
		return false, e
	}
	return st.Mode.IsRegular(), nil
}

func (o objectAPI) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, *probe.Error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	if status, e := o.isUploadIDExist(bucket, object, uploadID); e != nil {
		return "", probe.NewError(e)
	} else if !status {
		return "", probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	partSuffix := fmt.Sprintf("%s.%d.%s", uploadID, partID, md5Hex)
	fileWriter, e := o.storage.CreateFile(minioMetaVolume, filepath.Join(bucket, object, partSuffix))
	if e != nil {
		if e == errVolumeNotFound {
			return "", probe.NewError(BucketNotFound{
				Bucket: bucket,
			})
		} else if e == errIsNotRegular {
			return "", probe.NewError(ObjectExistsAsPrefix{
				Bucket: bucket,
				Prefix: object,
			})
		}
		return "", probe.NewError(e)
	}

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Instantiate a new multi writer.
	multiWriter := io.MultiWriter(md5Writer, fileWriter)

	// Instantiate checksum hashers and create a multiwriter.
	if size > 0 {
		if _, e = io.CopyN(multiWriter, data, size); e != nil {
			fileWriter.(*safe.File).CloseAndRemove()
			return "", probe.NewError(e)
		}
	} else {
		if _, e = io.Copy(multiWriter, data); e != nil {
			fileWriter.(*safe.File).CloseAndRemove()
			return "", probe.NewError(e)
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			fileWriter.(*safe.File).CloseAndRemove()
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
	if status, e := o.isUploadIDExist(bucket, object, uploadID); e != nil {
		return ListPartsInfo{}, probe.NewError(e)
	} else if !status {
		return ListPartsInfo{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	result := ListPartsInfo{}
	marker := ""
	nextPartNumberMarker := 0
	if partNumberMarker > 0 {
		fileInfos, _, e := o.storage.ListFiles(minioMetaVolume, filepath.Join(bucket, object, uploadID)+"."+strconv.Itoa(partNumberMarker)+".", "", false, 1)
		if e != nil {
			return result, probe.NewError(e)
		}
		if len(fileInfos) == 0 {
			return result, probe.NewError(InvalidPart{})
		}
		marker = fileInfos[0].Name
	}
	fileInfos, eof, e := o.storage.ListFiles(minioMetaVolume, filepath.Join(bucket, object, uploadID)+".", marker, false, maxParts)
	if e != nil {
		return result, probe.NewError(InvalidPart{})
	}
	for _, fileInfo := range fileInfos {
		fileName := filepath.Base(fileInfo.Name)
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

func (o objectAPI) CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (ObjectInfo, *probe.Error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return ObjectInfo{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	if status, e := o.isUploadIDExist(bucket, object, uploadID); e != nil {
		return ObjectInfo{}, probe.NewError(e)
	} else if !status {
		return ObjectInfo{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	fileWriter, e := o.storage.CreateFile(bucket, object)
	if e != nil {
		return ObjectInfo{}, nil
	}
	for _, part := range parts {
		partSuffix := fmt.Sprintf("%s.%d.%s", uploadID, part.PartNumber, part.ETag)
		var fileReader io.ReadCloser
		fileReader, e = o.storage.ReadFile(minioMetaVolume, filepath.Join(bucket, object, partSuffix), 0)
		if e != nil {
			return ObjectInfo{}, probe.NewError(e)
		}
		_, e = io.Copy(fileWriter, fileReader)
		if e != nil {
			return ObjectInfo{}, probe.NewError(e)
		}
		e = fileReader.Close()
		if e != nil {
			return ObjectInfo{}, probe.NewError(e)
		}
	}
	e = fileWriter.Close()
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}
	fi, e := o.storage.StatFile(bucket, object)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}
	o.removeMultipartUpload(bucket, object, uploadID)
	return ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ModTime: fi.ModTime,
		Size:    fi.Size,
		IsDir:   false,
	}, nil
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
		fileInfos, eof, e := o.storage.ListFiles(minioMetaVolume, filepath.Join(bucket, object, uploadID), marker, false, 1000)
		if e != nil {
			return probe.NewError(ObjectNotFound{Bucket: bucket, Object: object})
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
	if status, e := o.isUploadIDExist(bucket, object, uploadID); e != nil {
		return probe.NewError(e)
	} else if !status {
		return probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	e := o.removeMultipartUpload(bucket, object, uploadID)
	if e != nil {
		return e.Trace()
	}
	return nil
}
