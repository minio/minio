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

package cmd

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/mimedb"
)

// fsObjects - Implements fs object layer.
type fsObjects struct {
	storage      StorageAPI
	physicalDisk string

	// List pool management.
	listPool *treeWalkPool
}

// creates format.json, the FS format info in minioMetaBucket.
func initFormatFS(storageDisk StorageAPI) error {
	return writeFSFormatData(storageDisk, newFSFormatV1())
}

// loads format.json from minioMetaBucket if it exists.
func loadFormatFS(storageDisk StorageAPI) (format formatConfigV1, err error) {
	// Reads entire `format.json`.
	buf, err := storageDisk.ReadAll(minioMetaBucket, fsFormatJSONFile)
	if err != nil {
		return formatConfigV1{}, err
	}

	// Unmarshal format config.
	if err = json.Unmarshal(buf, &format); err != nil {
		return formatConfigV1{}, err
	}

	// Return structured `format.json`.
	return format, nil
}

// newFSObjects - initialize new fs object layer.
func newFSObjects(disk string) (ObjectLayer, error) {
	storage, err := newStorageAPI(disk)
	if err != nil && err != errDiskNotFound {
		return nil, err
	}

	// Attempt to create `.minio`.
	err = storage.MakeVol(minioMetaBucket)
	if err != nil {
		switch err {
		// Ignore the errors.
		case errVolumeExists, errDiskNotFound, errFaultyDisk:
		default:
			return nil, toObjectErr(err, minioMetaBucket)
		}
	}

	// Runs house keeping code, like creating minioMetaBucket, cleaning up tmp files etc.
	if err = fsHouseKeeping(storage); err != nil {
		return nil, err
	}

	// loading format.json from minioMetaBucket.
	// Note: The format.json content is ignored, reserved for future use.
	format, err := loadFormatFS(storage)
	if err != nil {
		if err == errFileNotFound {
			// format.json doesn't exist, create it inside minioMetaBucket.
			err = initFormatFS(storage)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else if !isFSFormat(format) {
		return nil, errFSDiskFormat
	}

	// Initialize fs objects.
	fs := fsObjects{
		storage:      storage,
		physicalDisk: disk,
		listPool:     newTreeWalkPool(globalLookupTimeout),
	}

	// Return successfully initialized object layer.
	return fs, nil
}

// Should be called when process shuts down.
func (fs fsObjects) Shutdown() error {
	// List if there are any multipart entries.
	_, err := fs.storage.ListDir(minioMetaBucket, mpartMetaPrefix)
	if err != errFileNotFound {
		// Multipart directory is not empty hence do not remove '.minio.sys' volume.
		return nil
	}
	// List if there are any bucket configuration entries.
	_, err = fs.storage.ListDir(minioMetaBucket, bucketConfigPrefix)
	if err != errFileNotFound {
		// Bucket config directory is not empty hence do not remove '.minio.sys' volume.
		return nil
	}
	// Cleanup everything else.
	prefix := ""
	if err = cleanupDir(fs.storage, minioMetaBucket, prefix); err != nil {
		errorIf(err, "Unable to cleanup minio meta bucket")
		return err
	}
	if err = fs.storage.DeleteVol(minioMetaBucket); err != nil {
		if err != errVolumeNotEmpty {
			errorIf(err, "Unable to delete minio meta bucket %s", minioMetaBucket)
			return err
		}
	}
	// Successful.
	return nil
}

// StorageInfo - returns underlying storage statistics.
func (fs fsObjects) StorageInfo() StorageInfo {
	info, err := disk.GetInfo(fs.physicalDisk)
	fatalIf(err, "Unable to get disk info "+fs.physicalDisk)
	return StorageInfo{
		Total: info.Total,
		Free:  info.Free,
	}
}

/// Bucket operations

// MakeBucket - make a bucket.
func (fs fsObjects) MakeBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if err := fs.storage.MakeVol(bucket); err != nil {
		return toObjectErr(err, bucket)
	}
	return nil
}

// GetBucketInfo - get bucket info.
func (fs fsObjects) GetBucketInfo(bucket string) (BucketInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	vi, err := fs.storage.StatVol(bucket)
	if err != nil {
		return BucketInfo{}, toObjectErr(err, bucket)
	}
	return BucketInfo{
		Name:    bucket,
		Created: vi.Created,
	}, nil
}

// ListBuckets - list buckets.
func (fs fsObjects) ListBuckets() ([]BucketInfo, error) {
	var bucketInfos []BucketInfo
	vols, err := fs.storage.ListVols()
	if err != nil {
		return nil, toObjectErr(err)
	}
	for _, vol := range vols {
		// StorageAPI can send volume names which are incompatible
		// with buckets, handle it and skip them.
		if !IsValidBucketName(vol.Name) {
			continue
		}
		// Ignore the volume special bucket.
		if vol.Name == minioMetaBucket {
			continue
		}
		bucketInfos = append(bucketInfos, BucketInfo{
			Name:    vol.Name,
			Created: vol.Created,
		})
	}
	sort.Sort(byBucketName(bucketInfos))
	return bucketInfos, nil
}

// DeleteBucket - delete a bucket.
func (fs fsObjects) DeleteBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	// Attempt to delete regular bucket.
	if err := fs.storage.DeleteVol(bucket); err != nil {
		return toObjectErr(err, bucket)
	}
	// Cleanup all the previously incomplete multiparts.
	if err := cleanupDir(fs.storage, path.Join(minioMetaBucket, mpartMetaPrefix), bucket); err != nil && err != errVolumeNotFound {
		return toObjectErr(err, bucket)
	}
	return nil
}

/// Object Operations

// GetObject - get an object.
func (fs fsObjects) GetObject(bucket, object string, offset int64, length int64, writer io.Writer) (err error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	// Offset and length cannot be negative.
	if offset < 0 || length < 0 {
		return toObjectErr(errUnexpected, bucket, object)
	}
	// Writer cannot be nil.
	if writer == nil {
		return toObjectErr(errUnexpected, bucket, object)
	}

	// Stat the file to get file size.
	fi, err := fs.storage.StatFile(bucket, object)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Reply back invalid range if the input offset and length fall out of range.
	if offset > fi.Size || length > fi.Size {
		return InvalidRange{offset, length, fi.Size}
	}
	// Reply if we have inputs with offset and length falling out of file size range.
	if offset+length > fi.Size {
		return InvalidRange{offset, length, fi.Size}
	}

	var totalLeft = length
	bufSize := int64(readSizeV1)
	if length > 0 && bufSize > length {
		bufSize = length
	}
	// Allocate a staging buffer.
	buf := make([]byte, int(bufSize))
	for {
		// Figure out the right size for the buffer.
		curLeft := bufSize
		if totalLeft < bufSize {
			curLeft = totalLeft
		}
		// Reads the file at offset.
		nr, er := fs.storage.ReadFile(bucket, object, offset, buf[:curLeft])
		if nr > 0 {
			// Write to response writer.
			nw, ew := writer.Write(buf[0:nr])
			if nw > 0 {
				// Decrement whats left to write.
				totalLeft -= int64(nw)

				// Progress the offset
				offset += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != int64(nw) {
				err = io.ErrShortWrite
				break
			}
		}
		if er == io.EOF || er == io.ErrUnexpectedEOF {
			break
		}
		if er != nil {
			err = er
			break
		}
		if totalLeft == 0 {
			break
		}
	}
	// Returns any error.
	return toObjectErr(err, bucket, object)
}

// GetObjectInfo - get object info.
func (fs fsObjects) GetObjectInfo(bucket, object string) (ObjectInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, (BucketNameInvalid{Bucket: bucket})
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return ObjectInfo{}, (ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	fi, err := fs.storage.StatFile(bucket, object)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	fsMeta, err := readFSMetadata(fs.storage, minioMetaBucket, path.Join(bucketMetaPrefix, bucket, object, fsMetaJSONFile))
	if err != nil && err != errFileNotFound {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if len(fsMeta.Meta) == 0 {
		fsMeta.Meta = make(map[string]string)
	}

	// Guess content-type from the extension if possible.
	if fsMeta.Meta["content-type"] == "" {
		if objectExt := path.Ext(object); objectExt != "" {
			if content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]; ok {
				fsMeta.Meta["content-type"] = content.ContentType
			}
		}
	}

	// Guess content-type from the extension if possible.
	return ObjectInfo{
		Bucket:          bucket,
		Name:            object,
		ModTime:         fi.ModTime,
		Size:            fi.Size,
		IsDir:           fi.Mode.IsDir(),
		MD5Sum:          fsMeta.Meta["md5Sum"],
		ContentType:     fsMeta.Meta["content-type"],
		ContentEncoding: fsMeta.Meta["content-encoding"],
		UserDefined:     fsMeta.Meta,
	}, nil
}

// PutObject - create an object.
func (fs fsObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}
	// No metadata is set, allocate a new one.
	if metadata == nil {
		metadata = make(map[string]string)
	}

	uniqueID := getUUID()

	// Uploaded object will first be written to the temporary location which will eventually
	// be renamed to the actual location. It is first written to the temporary location
	// so that cleaning it up will be easy if the server goes down.
	tempObj := path.Join(tmpMetaPrefix, uniqueID)

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Limit the reader to its provided size if specified.
	var limitDataReader io.Reader
	if size > 0 {
		// This is done so that we can avoid erroneous clients sending more data than the set content size.
		limitDataReader = io.LimitReader(data, size)
	} else {
		// else we read till EOF.
		limitDataReader = data
	}

	if size == 0 {
		// For size 0 we write a 0byte file.
		err := fs.storage.AppendFile(minioMetaBucket, tempObj, []byte(""))
		if err != nil {
			return "", toObjectErr(err, bucket, object)
		}
	} else {
		// Allocate a buffer to Read() from request body
		bufSize := int64(readSizeV1)
		if size > 0 && bufSize > size {
			bufSize = size
		}
		buf := make([]byte, int(bufSize))
		teeReader := io.TeeReader(limitDataReader, md5Writer)
		bytesWritten, err := fsCreateFile(fs.storage, teeReader, buf, minioMetaBucket, tempObj)
		if err != nil {
			fs.storage.DeleteFile(minioMetaBucket, tempObj)
			return "", toObjectErr(err, bucket, object)
		}

		// Should return IncompleteBody{} error when reader has fewer
		// bytes than specified in request header.
		if bytesWritten < size {
			fs.storage.DeleteFile(minioMetaBucket, tempObj)
			return "", IncompleteBody{}
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	// Update the md5sum if not set with the newly calculated one.
	if len(metadata["md5Sum"]) == 0 {
		metadata["md5Sum"] = newMD5Hex
	}

	// Validate if payload is valid.
	if isSignVerify(data) {
		if vErr := data.(*signVerifyReader).Verify(); vErr != nil {
			// Incoming payload wrong, delete the temporary object.
			fs.storage.DeleteFile(minioMetaBucket, tempObj)
			// Error return.
			return "", toObjectErr(vErr, bucket, object)
		}
	}

	// md5Hex representation.
	md5Hex := metadata["md5Sum"]
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			// MD5 mismatch, delete the temporary object.
			fs.storage.DeleteFile(minioMetaBucket, tempObj)
			// Returns md5 mismatch.
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}

	// Entire object was written to the temp location, now it's safe to rename it to the actual location.
	err := fs.storage.RenameFile(minioMetaBucket, tempObj, bucket, object)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Save additional metadata only if extended headers such as "X-Amz-Meta-" are set.
	if hasExtendedHeader(metadata) {
		// Initialize `fs.json` values.
		fsMeta := newFSMetaV1()
		fsMeta.Meta = metadata

		fsMetaPath := path.Join(bucketMetaPrefix, bucket, object, fsMetaJSONFile)
		if err = writeFSMetadata(fs.storage, minioMetaBucket, fsMetaPath, fsMeta); err != nil {
			return "", toObjectErr(err, bucket, object)
		}
	}

	// Return md5sum, successfully wrote object.
	return newMD5Hex, nil
}

// DeleteObject - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (fs fsObjects) DeleteObject(bucket, object string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	err := fs.storage.DeleteFile(minioMetaBucket, path.Join(bucketMetaPrefix, bucket, object, fsMetaJSONFile))
	if err != nil && err != errFileNotFound {
		return toObjectErr(err, bucket, object)
	}
	if err = fs.storage.DeleteFile(bucket, object); err != nil {
		return toObjectErr(err, bucket, object)
	}
	return nil
}

// Checks whether bucket exists.
func isBucketExist(storage StorageAPI, bucketName string) bool {
	// Check whether bucket exists.
	_, err := storage.StatVol(bucketName)
	if err != nil {
		if err == errVolumeNotFound {
			return false
		}
		errorIf(err, "Stat failed on bucket "+bucketName+".")
		return false
	}
	return true
}

// ListObjects - list all objects at prefix upto maxKeys., optionally delimited by '/'. Maintains the list pool
// state for future re-entrant list requests.
func (fs fsObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	// Convert entry to FileInfo
	entryToFileInfo := func(entry string) (fileInfo FileInfo, err error) {
		if strings.HasSuffix(entry, slashSeparator) {
			// Object name needs to be full path.
			fileInfo.Name = entry
			fileInfo.Mode = os.ModeDir
			return
		}
		if fileInfo, err = fs.storage.StatFile(bucket, entry); err != nil {
			return
		}
		fsMeta, mErr := readFSMetadata(fs.storage, minioMetaBucket, path.Join(bucketMetaPrefix, bucket, entry, fsMetaJSONFile))
		if mErr != nil && mErr != errFileNotFound {
			return FileInfo{}, mErr
		}
		if len(fsMeta.Meta) == 0 {
			fsMeta.Meta = make(map[string]string)
		}
		// Object name needs to be full path.
		fileInfo.Name = entry
		fileInfo.MD5Sum = fsMeta.Meta["md5Sum"]
		return
	}

	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListObjectsInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	// Verify if bucket exists.
	if !isBucketExist(fs.storage, bucket) {
		return ListObjectsInfo{}, BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectPrefix(prefix) {
		return ListObjectsInfo{}, ObjectNameInvalid{Bucket: bucket, Object: prefix}
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return ListObjectsInfo{}, UnsupportedDelimiter{
			Delimiter: delimiter,
		}
	}
	// Verify if marker has prefix.
	if marker != "" {
		if !strings.HasPrefix(marker, prefix) {
			return ListObjectsInfo{}, InvalidMarkerPrefixCombination{
				Marker: marker,
				Prefix: prefix,
			}
		}
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return ListObjectsInfo{}, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if delimiter == slashSeparator && prefix == slashSeparator {
		return ListObjectsInfo{}, nil
	}

	// Over flowing count - reset to maxObjectList.
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	heal := false // true only for xl.ListObjectsHeal()
	walkResultCh, endWalkCh := fs.listPool.Release(listParams{bucket, recursive, marker, prefix, heal})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		isLeaf := func(bucket, object string) bool {
			// bucket argument is unused as we don't need to StatFile
			// to figure if it's a file, just need to check that the
			// object string does not end with "/".
			return !strings.HasSuffix(object, slashSeparator)
		}
		listDir := listDirFactory(isLeaf, fs.storage)
		walkResultCh = startTreeWalk(bucket, prefix, marker, recursive, listDir, isLeaf, endWalkCh)
	}
	var fileInfos []FileInfo
	var eof bool
	var nextMarker string
	for i := 0; i < maxKeys; {
		walkResult, ok := <-walkResultCh
		if !ok {
			// Closed channel.
			eof = true
			break
		}
		// For any walk error return right away.
		if walkResult.err != nil {
			// File not found is a valid case.
			if walkResult.err == errFileNotFound {
				return ListObjectsInfo{}, nil
			}
			return ListObjectsInfo{}, toObjectErr(walkResult.err, bucket, prefix)
		}
		fileInfo, err := entryToFileInfo(walkResult.entry)
		if err != nil {
			return ListObjectsInfo{}, nil
		}
		nextMarker = fileInfo.Name
		fileInfos = append(fileInfos, fileInfo)
		if walkResult.end {
			eof = true
			break
		}
		i++
	}
	params := listParams{bucket, recursive, nextMarker, prefix, heal}
	if !eof {
		fs.listPool.Set(params, walkResultCh, endWalkCh)
	}

	result := ListObjectsInfo{IsTruncated: !eof}
	for _, fileInfo := range fileInfos {
		result.NextMarker = fileInfo.Name
		if fileInfo.Mode.IsDir() {
			result.Prefixes = append(result.Prefixes, fileInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, ObjectInfo{
			Name:    fileInfo.Name,
			ModTime: fileInfo.ModTime,
			Size:    fileInfo.Size,
			MD5Sum:  fileInfo.MD5Sum,
			IsDir:   false,
		})
	}
	return result, nil
}

// HealObject - no-op for fs. Valid only for XL.
func (fs fsObjects) HealObject(bucket, object string) error {
	return NotImplemented{}
}

// HealListObjects - list objects for healing. Valid only for XL
func (fs fsObjects) ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return ListObjectsInfo{}, NotImplemented{}
}
