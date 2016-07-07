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
	"io"
	"os"
	"path"
	"path/filepath"
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

// Should be called when process shuts down.
func shutdownFS(storage StorageAPI) {
	_, err := storage.ListDir(minioMetaBucket, mpartMetaPrefix)
	if err != errFileNotFound {
		// Multipart directory is not empty hence do not remove .minio volume.
		os.Exit(0)
	}
	prefix := ""
	if err := cleanupDir(storage, minioMetaBucket, prefix); err != nil {
		os.Exit(0)
		return
	}
	storage.DeleteVol(minioMetaBucket)
	os.Exit(0)
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

	// Register the callback that should be called when the process shuts down.
	registerShutdown(func() {
		shutdownFS(storage)
	})

	// Return successfully initialized object layer.
	return fsObjects{
		storage:      storage,
		physicalDisk: disk,
		listPool:     newTreeWalkPool(globalLookupTimeout),
	}, nil
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
	if err := fs.storage.DeleteVol(bucket); err != nil {
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
	var totalLeft = length
	bufSize := int64(readSizeV1)
	if length > 0 && bufSize > length {
		bufSize = length
	}
	// Allocate a staging buffer.
	buf := make([]byte, int(bufSize))
	for totalLeft > 0 {
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

	// Guess content-type from the extension if possible.
	contentType := ""
	if objectExt := filepath.Ext(object); objectExt != "" {
		if content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]; ok {
			contentType = content.ContentType
		}
	}

	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     fi.ModTime,
		Size:        fi.Size,
		IsDir:       fi.Mode.IsDir(),
		ContentType: contentType,
		MD5Sum:      "", // Read from metadata.
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
		// Allocate a buffer to Read() the object upload stream.
		bufSize := int64(readSizeV1)
		if size > 0 && bufSize > size {
			bufSize = size
		}
		buf := make([]byte, int(bufSize))

		// Read the buffer till io.EOF and append the read data to the temporary file.
		for {
			n, rErr := limitDataReader.Read(buf)
			if rErr != nil && rErr != io.EOF {
				return "", toObjectErr(rErr, bucket, object)
			}
			if n > 0 {
				// Update md5 writer.
				md5Writer.Write(buf[0:n])
				wErr := fs.storage.AppendFile(minioMetaBucket, tempObj, buf[0:n])
				if wErr != nil {
					return "", toObjectErr(wErr, bucket, object)
				}
			}
			if rErr == io.EOF {
				break
			}
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	// md5Hex representation.
	var md5Hex string
	if len(metadata) != 0 {
		md5Hex = metadata["md5Sum"]
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

	// Return md5sum, successfully wrote object.
	return newMD5Hex, nil
}

func (fs fsObjects) DeleteObject(bucket, object string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if err := fs.storage.DeleteFile(bucket, object); err != nil {
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

func (fs fsObjects) listObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
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
		// Object name needs to be full path.
		fileInfo.Name = entry
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

	walkResultCh, endWalkCh := fs.listPool.Release(listParams{bucket, recursive, marker, prefix})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		listDir := listDirFactory(func(bucket, object string) bool {
			return !strings.HasSuffix(object, slashSeparator)
		}, fs.storage)
		walkResultCh = startTreeWalk(bucket, prefix, marker, recursive, listDir, endWalkCh)
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
	params := listParams{bucket, recursive, nextMarker, prefix}
	if !eof {
		fs.listPool.Set(params, walkResultCh, endWalkCh)
	}

	result := ListObjectsInfo{IsTruncated: !eof}
	for _, fileInfo := range fileInfos {
		// With delimiter set we fill in NextMarker and Prefixes.
		if delimiter == slashSeparator {
			result.NextMarker = fileInfo.Name
			if fileInfo.Mode.IsDir() {
				result.Prefixes = append(result.Prefixes, fileInfo.Name)
				continue
			}
		}
		result.Objects = append(result.Objects, ObjectInfo{
			Name:    fileInfo.Name,
			ModTime: fileInfo.ModTime,
			Size:    fileInfo.Size,
			IsDir:   false,
		})
	}
	return result, nil
}

// ListObjects - list all objects.
func (fs fsObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return fs.listObjects(bucket, prefix, marker, delimiter, maxKeys)
}
