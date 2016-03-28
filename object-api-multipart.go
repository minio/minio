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

	"github.com/minio/minio/pkg/probe"
	"github.com/skyrings/skyring-common/tools/uuid"
)

const (
	minioMetaVolume = ".minio"
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
	if _, e := o.storage.StatVol(minioMetaVolume); e != nil {
		if e == errVolumeNotFound {
			e = o.storage.MakeVol(minioMetaVolume)
			if e != nil {
				return ListMultipartsInfo{}, probe.NewError(e)
			}
		}
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
	result.IsTruncated = true
	result.MaxUploads = maxUploads
	newMaxUploads := 0
	// not using path.Join() as it strips off the trailing '/'.
	// Also bucket should always be followed by '/' even if prefix is empty.
	prefixPath := pathJoin(bucket, prefix)
	if recursive {
		keyMarkerPath := ""
		if keyMarker != "" {
			keyMarkerPath = path.Join(bucket, keyMarker, uploadIDMarker)
		}
	outerLoop:
		for {
			fileInfos, eof, e := o.storage.ListFiles(minioMetaVolume, prefixPath, keyMarkerPath, recursive, maxUploads-newMaxUploads)
			if e != nil {
				return ListMultipartsInfo{}, probe.NewError(e)
			}
			for _, fi := range fileInfos {
				keyMarkerPath = fi.Name
				// fi.Name will look like bucket/object/uploadID, extract object and uploadID.
				uploadID := path.Base(fi.Name)
				objectName := strings.TrimPrefix(path.Dir(fi.Name), bucket+slashPathSeparator)
				if strings.Contains(uploadID, ".") {
					// contains partnumber and md5sum info, skip this.
					continue
				}
				result.Uploads = append(result.Uploads, uploadMetadata{
					Object:    objectName,
					UploadID:  uploadID,
					Initiated: fi.ModTime,
				})
				result.NextKeyMarker = objectName
				result.NextUploadIDMarker = uploadID
				newMaxUploads++
				if newMaxUploads == maxUploads {
					if eof {
						result.IsTruncated = false
					}
					break outerLoop
				}
			}
			if eof {
				result.IsTruncated = false
				break
			}
		}
		if !result.IsTruncated {
			result.NextKeyMarker = ""
			result.NextUploadIDMarker = ""
		}
		return result, nil
	}

	var fileInfos []FileInfo
	// read all the "fileInfos" in the prefix
	for {
		marker := ""
		fis, eof, e := o.storage.ListFiles(minioMetaVolume, prefixPath, marker, recursive, 1000)
		if e != nil {
			return ListMultipartsInfo{}, probe.NewError(e)
		}
		for _, fi := range fis {
			marker = fi.Name
			if fi.Mode.IsDir() {
				fileInfos = append(fileInfos, fi)
			}
		}
		if eof {
			break
		}
	}
	// Create "uploads" slice from "fileInfos" slice.
	var uploads []uploadMetadata
	for _, fi := range fileInfos {
		leaf, entries := o.checkLeafDirectory(fi.Name)
		objectName := strings.TrimPrefix(fi.Name, bucket+slashPathSeparator)
		if leaf {
			for _, entry := range entries {
				if strings.Contains(entry.Name, ".") {
					continue
				}
				uploads = append(uploads, uploadMetadata{
					Object:    strings.TrimSuffix(objectName, slashPathSeparator),
					UploadID:  path.Base(entry.Name),
					Initiated: entry.ModTime,
				})
			}
			continue
		}
		uploads = append(uploads, uploadMetadata{
			Object: objectName,
		})
	}
	index := 0
	for i, upload := range uploads {
		index = i
		if upload.Object > keyMarker {
			break
		}
		if uploads[index].Object == keyMarker && upload.UploadID > uploadIDMarker {
			break
		}
	}
	for ; index < len(uploads); index++ {
		if (len(result.Uploads) + len(result.CommonPrefixes)) == maxUploads {
			break
		}
		result.NextKeyMarker = uploads[index].Object
		if strings.HasSuffix(uploads[index].Object, slashPathSeparator) {
			// for a directory entry
			result.CommonPrefixes = append(result.CommonPrefixes, uploads[index].Object)
			continue
		}
		result.NextUploadIDMarker = uploads[index].UploadID
		result.Uploads = append(result.Uploads, uploads[index])
	}
	if index == len(uploads) {
		result.IsTruncated = false
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
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
		uploadIDPath := path.Join(bucket, object, uploadID)
		if _, e = o.storage.StatFile(minioMetaVolume, uploadIDPath); e != nil {
			if e != errFileNotFound {
				return "", probe.NewError(e)
			}
			// uploadIDPath doesn't exist, so create empty file to reserve the name
			var w io.WriteCloser
			if w, e = o.storage.CreateFile(minioMetaVolume, uploadIDPath); e == nil {
				if e = w.Close(); e != nil {
					return "", probe.NewError(e)
				}
			} else {
				return "", probe.NewError(e)
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

func (o objectAPI) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (string, *probe.Error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return "", probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	if status, e := o.isUploadIDExists(bucket, object, uploadID); e != nil {
		return "", probe.NewError(e)
	} else if !status {
		return "", probe.NewError(InvalidUploadID{UploadID: uploadID})
	}

	partSuffix := fmt.Sprintf("%s.%d.%s", uploadID, partID, md5Hex)
	fileWriter, e := o.storage.CreateFile(minioMetaVolume, path.Join(bucket, object, partSuffix))
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
			safeCloseAndRemove(fileWriter)
			return "", probe.NewError(e)
		}
	} else {
		if _, e = io.Copy(multiWriter, data); e != nil {
			safeCloseAndRemove(fileWriter)
			return "", probe.NewError(e)
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
	// Create minio meta volume, if it doesn't exist yet.
	if _, e := o.storage.StatVol(minioMetaVolume); e != nil {
		if e == errVolumeNotFound {
			e = o.storage.MakeVol(minioMetaVolume)
			if e != nil {
				return ListPartsInfo{}, probe.NewError(e)
			}
		}
	}
	if status, e := o.isUploadIDExists(bucket, object, uploadID); e != nil {
		return ListPartsInfo{}, probe.NewError(e)
	} else if !status {
		return ListPartsInfo{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	result := ListPartsInfo{}
	marker := ""
	nextPartNumberMarker := 0
	uploadIDPath := path.Join(bucket, object, uploadID)
	// Figure out the marker for the next subsequent calls, if the
	// partNumberMarker is already set.
	if partNumberMarker > 0 {
		fileInfos, _, e := o.storage.ListFiles(minioMetaVolume, uploadIDPath+"."+strconv.Itoa(partNumberMarker)+".", "", false, 1)
		if e != nil {
			return result, probe.NewError(e)
		}
		if len(fileInfos) == 0 {
			return result, probe.NewError(InvalidPart{})
		}
		marker = fileInfos[0].Name
	}
	fileInfos, eof, e := o.storage.ListFiles(minioMetaVolume, uploadIDPath+".", marker, false, maxParts)
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
	if _, e := o.storage.StatVol(minioMetaVolume); e != nil {
		return nil
	}

	marker := ""
	for {
		uploadIDPath := path.Join(bucket, object, uploadID)
		fileInfos, eof, e := o.storage.ListFiles(minioMetaVolume, uploadIDPath, marker, false, 1000)
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
