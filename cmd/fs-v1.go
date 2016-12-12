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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"path"
	"sort"
	"strings"

	"github.com/minio/minio/pkg/mimedb"
)

// fsObjects - Implements fs object layer.
type fsObjects struct {
	storage StorageAPI

	// List pool management.
	listPool *treeWalkPool

	// To manage the appendRoutine go0routines
	bgAppend *backgroundAppend
}

// list of all errors that can be ignored in tree walk operation in FS
var fsTreeWalkIgnoredErrs = []error{
	errFileNotFound,
	errVolumeNotFound,
}

// newFSObjects - initialize new fs object layer.
func newFSObjects(storage StorageAPI) (ObjectLayer, error) {
	if storage == nil {
		return nil, errInvalidArgument
	}

	// Load format and validate.
	_, err := loadFormatFS(storage)
	if err != nil {
		return nil, fmt.Errorf("Unable to recognize backend format, %s", err)
	}

	// Initialize meta volume, if volume already exists ignores it.
	if err = initMetaVolume([]StorageAPI{storage}); err != nil {
		return nil, fmt.Errorf("Unable to initialize '.minio.sys' meta volume, %s", err)
	}

	// Initialize fs objects.
	fs := fsObjects{
		storage:  storage,
		listPool: newTreeWalkPool(globalLookupTimeout),
		bgAppend: &backgroundAppend{
			infoMap: make(map[string]bgAppendPartsInfo),
		},
	}

	// Return successfully initialized object layer.
	return fs, nil
}

// Should be called when process shuts down.
func (fs fsObjects) Shutdown() error {
	// List if there are any multipart entries.
	prefix := ""
	entries, err := fs.storage.ListDir(minioMetaMultipartBucket, prefix)
	if err != nil {
		// A non nil err means that an unexpected error occurred
		return toObjectErr(traceError(err))
	}
	if len(entries) > 0 {
		// Should not remove .minio.sys if there are any multipart
		// uploads were found.
		return nil
	}
	if err = fs.storage.DeleteVol(minioMetaMultipartBucket); err != nil {
		return toObjectErr(traceError(err))
	}
	// List if there are any bucket configuration entries.
	_, err = fs.storage.ListDir(minioMetaBucket, bucketConfigPrefix)
	if err != errFileNotFound {
		// A nil err means that bucket config directory is not empty hence do not remove '.minio.sys' volume.
		// A non nil err means that an unexpected error occurred
		return toObjectErr(traceError(err))
	}
	// Cleanup and delete tmp bucket.
	if err = cleanupDir(fs.storage, minioMetaTmpBucket, prefix); err != nil {
		return err
	}
	if err = fs.storage.DeleteVol(minioMetaTmpBucket); err != nil {
		return toObjectErr(traceError(err))
	}

	// Remove format.json and delete .minio.sys bucket
	if err = fs.storage.DeleteFile(minioMetaBucket, fsFormatJSONFile); err != nil {
		return toObjectErr(traceError(err))
	}
	if err = fs.storage.DeleteVol(minioMetaBucket); err != nil {
		if err != errVolumeNotEmpty {
			return toObjectErr(traceError(err))
		}
	}
	// Successful.
	return nil
}

// StorageInfo - returns underlying storage statistics.
func (fs fsObjects) StorageInfo() StorageInfo {
	info, err := fs.storage.DiskInfo()
	errorIf(err, "Unable to get disk info %#v", fs.storage)
	storageInfo := StorageInfo{
		Total: info.Total,
		Free:  info.Free,
	}
	storageInfo.Backend.Type = FS
	return storageInfo
}

/// Bucket operations

// MakeBucket - make a bucket.
func (fs fsObjects) MakeBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return traceError(BucketNameInvalid{Bucket: bucket})
	}
	if err := fs.storage.MakeVol(bucket); err != nil {
		return toObjectErr(traceError(err), bucket)
	}
	return nil
}

// GetBucketInfo - get bucket info.
func (fs fsObjects) GetBucketInfo(bucket string) (BucketInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketInfo{}, traceError(BucketNameInvalid{Bucket: bucket})
	}
	vi, err := fs.storage.StatVol(bucket)
	if err != nil {
		return BucketInfo{}, toObjectErr(traceError(err), bucket)
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
		return nil, toObjectErr(traceError(err))
	}
	var invalidBucketNames []string
	for _, vol := range vols {
		// StorageAPI can send volume names which are incompatible
		// with buckets, handle it and skip them.
		if !IsValidBucketName(vol.Name) {
			invalidBucketNames = append(invalidBucketNames, vol.Name)
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
	// Print a user friendly message if we indeed skipped certain directories which are
	// incompatible with S3's bucket name restrictions.
	if len(invalidBucketNames) > 0 {
		errorIf(errors.New("One or more invalid bucket names found"), "Skipping %s", invalidBucketNames)
	}
	sort.Sort(byBucketName(bucketInfos))
	return bucketInfos, nil
}

// DeleteBucket - delete a bucket.
func (fs fsObjects) DeleteBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return traceError(BucketNameInvalid{Bucket: bucket})
	}
	// Attempt to delete regular bucket.
	if err := fs.storage.DeleteVol(bucket); err != nil {
		return toObjectErr(traceError(err), bucket)
	}
	// Cleanup all the previously incomplete multiparts.
	if err := cleanupDir(fs.storage, minioMetaMultipartBucket, bucket); err != nil && errorCause(err) != errVolumeNotFound {
		return toObjectErr(err, bucket)
	}
	return nil
}

/// Object Operations

// GetObject - get an object.
func (fs fsObjects) GetObject(bucket, object string, offset int64, length int64, writer io.Writer) (err error) {
	if err = checkGetObjArgs(bucket, object); err != nil {
		return err
	}
	// Offset and length cannot be negative.
	if offset < 0 || length < 0 {
		return toObjectErr(traceError(errUnexpected), bucket, object)
	}
	// Writer cannot be nil.
	if writer == nil {
		return toObjectErr(traceError(errUnexpected), bucket, object)
	}

	// Stat the file to get file size.
	fi, err := fs.storage.StatFile(bucket, object)
	if err != nil {
		return toObjectErr(traceError(err), bucket, object)
	}

	// Reply back invalid range if the input offset and length fall out of range.
	if offset > fi.Size || length > fi.Size {
		return traceError(InvalidRange{offset, length, fi.Size})
	}
	// Reply if we have inputs with offset and length falling out of file size range.
	if offset+length > fi.Size {
		return traceError(InvalidRange{offset, length, fi.Size})
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
				err = traceError(ew)
				break
			}
			if nr != int64(nw) {
				err = traceError(io.ErrShortWrite)
				break
			}
		}
		if er == io.EOF || er == io.ErrUnexpectedEOF {
			break
		}
		if er != nil {
			err = traceError(er)
			break
		}
		if totalLeft == 0 {
			break
		}
	}
	// Returns any error.
	return toObjectErr(err, bucket, object)
}

// getObjectInfo - get object info.
func (fs fsObjects) getObjectInfo(bucket, object string) (ObjectInfo, error) {
	fi, err := fs.storage.StatFile(bucket, object)
	if err != nil {
		return ObjectInfo{}, toObjectErr(traceError(err), bucket, object)
	}
	fsMeta, err := readFSMetadata(fs.storage, minioMetaBucket, path.Join(bucketMetaPrefix, bucket, object, fsMetaJSONFile))
	// Ignore error if the metadata file is not found, other errors must be returned.
	if err != nil && errorCause(err) != errFileNotFound {
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

	objInfo := ObjectInfo{
		Bucket:          bucket,
		Name:            object,
		ModTime:         fi.ModTime,
		Size:            fi.Size,
		IsDir:           fi.Mode.IsDir(),
		MD5Sum:          fsMeta.Meta["md5Sum"],
		ContentType:     fsMeta.Meta["content-type"],
		ContentEncoding: fsMeta.Meta["content-encoding"],
	}

	// md5Sum has already been extracted into objInfo.MD5Sum.  We
	// need to remove it from fsMeta.Meta to avoid it from appearing as
	// part of response headers. e.g, X-Minio-* or X-Amz-*.
	delete(fsMeta.Meta, "md5Sum")
	objInfo.UserDefined = fsMeta.Meta

	return objInfo, nil
}

// GetObjectInfo - get object info.
func (fs fsObjects) GetObjectInfo(bucket, object string) (ObjectInfo, error) {
	if err := checkGetObjArgs(bucket, object); err != nil {
		return ObjectInfo{}, err
	}
	return fs.getObjectInfo(bucket, object)
}

// PutObject - create an object.
func (fs fsObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error) {
	if err = checkPutObjectArgs(bucket, object, fs); err != nil {
		return ObjectInfo{}, err
	}
	// No metadata is set, allocate a new one.
	if metadata == nil {
		metadata = make(map[string]string)
	}

	uniqueID := mustGetUUID()

	// Uploaded object will first be written to the temporary location which will eventually
	// be renamed to the actual location. It is first written to the temporary location
	// so that cleaning it up will be easy if the server goes down.
	tempObj := uniqueID

	// Initialize md5 writer.
	md5Writer := md5.New()

	hashWriters := []io.Writer{md5Writer}

	var sha256Writer hash.Hash
	if sha256sum != "" {
		sha256Writer = sha256.New()
		hashWriters = append(hashWriters, sha256Writer)
	}
	multiWriter := io.MultiWriter(hashWriters...)

	// Limit the reader to its provided size if specified.
	var limitDataReader io.Reader
	if size > 0 {
		// This is done so that we can avoid erroneous clients sending more data than the set content size.
		limitDataReader = io.LimitReader(data, size)
	} else {
		// else we read till EOF.
		limitDataReader = data
	}

	// Prepare file to avoid disk fragmentation
	if size > 0 {
		err = fs.storage.PrepareFile(minioMetaTmpBucket, tempObj, size)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	// Allocate a buffer to Read() from request body
	bufSize := int64(readSizeV1)
	if size > 0 && bufSize > size {
		bufSize = size
	}

	buf := make([]byte, int(bufSize))
	teeReader := io.TeeReader(limitDataReader, multiWriter)
	var bytesWritten int64
	bytesWritten, err = fsCreateFile(fs.storage, teeReader, buf, minioMetaTmpBucket, tempObj)
	if err != nil {
		fs.storage.DeleteFile(minioMetaTmpBucket, tempObj)
		errorIf(err, "Failed to create object %s/%s", bucket, object)
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Should return IncompleteBody{} error when reader has fewer
	// bytes than specified in request header.
	if bytesWritten < size {
		fs.storage.DeleteFile(minioMetaTmpBucket, tempObj)
		return ObjectInfo{}, traceError(IncompleteBody{})
	}

	// Delete the temporary object in the case of a
	// failure. If PutObject succeeds, then there would be
	// nothing to delete.
	defer fs.storage.DeleteFile(minioMetaTmpBucket, tempObj)

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	// Update the md5sum if not set with the newly calculated one.
	if len(metadata["md5Sum"]) == 0 {
		metadata["md5Sum"] = newMD5Hex
	}

	// md5Hex representation.
	md5Hex := metadata["md5Sum"]
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			// Returns md5 mismatch.
			return ObjectInfo{}, traceError(BadDigest{md5Hex, newMD5Hex})
		}
	}

	if sha256sum != "" {
		newSHA256sum := hex.EncodeToString(sha256Writer.Sum(nil))
		if newSHA256sum != sha256sum {
			return ObjectInfo{}, traceError(SHA256Mismatch{})
		}
	}

	// Entire object was written to the temp location, now it's safe to rename it to the actual location.
	err = fs.storage.RenameFile(minioMetaTmpBucket, tempObj, bucket, object)
	if err != nil {
		return ObjectInfo{}, toObjectErr(traceError(err), bucket, object)
	}

	if bucket != minioMetaBucket {
		// Save objects' metadata in `fs.json`.
		// Skip creating fs.json if bucket is .minio.sys as the object would have been created
		// by minio's S3 layer (ex. policy.json)
		fsMeta := newFSMetaV1()
		fsMeta.Meta = metadata

		fsMetaPath := path.Join(bucketMetaPrefix, bucket, object, fsMetaJSONFile)
		if err = writeFSMetadata(fs.storage, minioMetaBucket, fsMetaPath, fsMeta); err != nil {
			return ObjectInfo{}, toObjectErr(traceError(err), bucket, object)
		}
	}

	return fs.getObjectInfo(bucket, object)
}

// DeleteObject - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (fs fsObjects) DeleteObject(bucket, object string) error {
	if err := checkDelObjArgs(bucket, object); err != nil {
		return err
	}

	if bucket != minioMetaBucket {
		// We don't store fs.json for minio-S3-layer created files like policy.json,
		// hence we don't try to delete fs.json for such files.
		err := fs.storage.DeleteFile(minioMetaBucket, path.Join(bucketMetaPrefix, bucket, object, fsMetaJSONFile))
		if err != nil && err != errFileNotFound {
			return toObjectErr(traceError(err), bucket, object)
		}
	}
	if err := fs.storage.DeleteFile(bucket, object); err != nil {
		return toObjectErr(traceError(err), bucket, object)
	}
	return nil
}

// ListObjects - list all objects at prefix upto maxKeys., optionally delimited by '/'. Maintains the list pool
// state for future re-entrant list requests.
func (fs fsObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	// Convert entry to ObjectInfo
	entryToObjectInfo := func(entry string) (objInfo ObjectInfo, err error) {
		if strings.HasSuffix(entry, slashSeparator) {
			// Object name needs to be full path.
			objInfo.Name = entry
			objInfo.IsDir = true
			return
		}
		if objInfo, err = fs.getObjectInfo(bucket, entry); err != nil {
			return ObjectInfo{}, err
		}
		return
	}

	if err := checkListObjsArgs(bucket, prefix, marker, delimiter, fs); err != nil {
		return ListObjectsInfo{}, err
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
		listDir := listDirFactory(isLeaf, fsTreeWalkIgnoredErrs, fs.storage)
		walkResultCh = startTreeWalk(bucket, prefix, marker, recursive, listDir, isLeaf, endWalkCh)
	}
	var objInfos []ObjectInfo
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
			if errorCause(walkResult.err) == errFileNotFound {
				return ListObjectsInfo{}, nil
			}
			return ListObjectsInfo{}, toObjectErr(walkResult.err, bucket, prefix)
		}
		objInfo, err := entryToObjectInfo(walkResult.entry)
		if err != nil {
			return ListObjectsInfo{}, nil
		}
		nextMarker = objInfo.Name
		objInfos = append(objInfos, objInfo)
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
	for _, objInfo := range objInfos {
		result.NextMarker = objInfo.Name
		if objInfo.IsDir {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}
	return result, nil
}

// HealObject - no-op for fs. Valid only for XL.
func (fs fsObjects) HealObject(bucket, object string) error {
	return traceError(NotImplemented{})
}

// HealBucket - no-op for fs, Valid only for XL.
func (fs fsObjects) HealBucket(bucket string) error {
	return traceError(NotImplemented{})
}

// ListObjectsHeal - list all objects to be healed. Valid only for XL
func (fs fsObjects) ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return ListObjectsInfo{}, traceError(NotImplemented{})
}
