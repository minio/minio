/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/lock"
	"github.com/minio/minio/pkg/madmin"
)

// fsObjects - Implements fs object layer.
type fsObjects struct {
	// Path to be exported over S3 API.
	fsPath string

	// Unique value to be used for all
	// temporary transactions.
	fsUUID string

	// This value shouldn't be touched, once initialized.
	fsFormatRlk *lock.RLockedFile // Is a read lock on `format.json`.

	// FS rw pool.
	rwPool *fsIOPool

	// ListObjects pool management.
	listPool *treeWalkPool

	// To manage the appendRoutine go0routines
	bgAppend *backgroundAppend

	// name space mutex for object layer
	nsMutex *nsLockMap
}

// Initializes meta volume on all the fs path.
func initMetaVolumeFS(fsPath, fsUUID string) error {
	// This happens for the first time, but keep this here since this
	// is the only place where it can be made less expensive
	// optimizing all other calls. Create minio meta volume,
	// if it doesn't exist yet.
	metaBucketPath := pathJoin(fsPath, minioMetaBucket)
	if err := os.MkdirAll(metaBucketPath, 0777); err != nil {
		return err
	}

	metaTmpPath := pathJoin(fsPath, minioMetaTmpBucket, fsUUID)
	if err := os.MkdirAll(metaTmpPath, 0777); err != nil {
		return err
	}

	metaMultipartPath := pathJoin(fsPath, minioMetaMultipartBucket)
	return os.MkdirAll(metaMultipartPath, 0777)

}

// newFSObjectLayer - initialize new fs object layer.
func newFSObjectLayer(fsPath string) (ObjectLayer, error) {
	if fsPath == "" {
		return nil, errInvalidArgument
	}

	var err error
	// Disallow relative paths, figure out absolute paths.
	fsPath, err = filepath.Abs(fsPath)
	if err != nil {
		return nil, err
	}

	fi, err := os.Stat((fsPath))
	if err == nil {
		if !fi.IsDir() {
			return nil, syscall.ENOTDIR
		}
	}
	if os.IsNotExist(err) {
		// Disk not found create it.
		err = os.MkdirAll(fsPath, 0777)
		if err != nil {
			return nil, err
		}
	}

	di, err := getDiskInfo((fsPath))
	if err != nil {
		return nil, err
	}

	// Check if disk has minimum required total space.
	if err = checkDiskMinTotal(di); err != nil {
		return nil, err
	}

	// Assign a new UUID for FS minio mode. Each server instance
	// gets its own UUID for temporary file transaction.
	fsUUID := mustGetUUID()

	// Initialize meta volume, if volume already exists ignores it.
	if err = initMetaVolumeFS(fsPath, fsUUID); err != nil {
		return nil, fmt.Errorf("Unable to initialize '.minio.sys' meta volume, %s", err)
	}

	// Initialize `format.json`, this function also returns.
	rlk, err := initFormatFS(fsPath)
	if err != nil {
		return nil, err
	}

	// Initialize fs objects.
	fs := &fsObjects{
		fsPath: fsPath,
		fsUUID: fsUUID,
		rwPool: &fsIOPool{
			readersMap: make(map[string]*lock.RLockedFile),
		},
		listPool: newTreeWalkPool(globalLookupTimeout),
		bgAppend: &backgroundAppend{
			infoMap: make(map[string]bgAppendPartsInfo),
		},
		nsMutex: newNSLock(false),
	}

	// Once the filesystem has initialized hold the read lock for
	// the life time of the server. This is done to ensure that under
	// shared backend mode for FS, remote servers do not migrate
	// or cause changes on backend format.
	fs.fsFormatRlk = rlk

	// Initialize and load bucket policies.
	if err = initBucketPolicies(fs); err != nil {
		return nil, fmt.Errorf("Unable to load all bucket policies. %s", err)
	}

	// Initialize a new event notifier.
	if err = initEventNotifier(fs); err != nil {
		return nil, fmt.Errorf("Unable to initialize event notification. %s", err)
	}

	// Start background process to cleanup old multipart objects in `.minio.sys`.
	go cleanupStaleMultipartUploads(multipartCleanupInterval, multipartExpiry, fs, fs.listMultipartUploadsCleanup, globalServiceDoneCh)

	// Return successfully initialized object layer.
	return fs, nil
}

// Should be called when process shuts down.
func (fs fsObjects) Shutdown() error {
	fs.fsFormatRlk.Close()

	// Cleanup and delete tmp uuid.
	return fsRemoveAll(pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID))
}

// StorageInfo - returns underlying storage statistics.
func (fs fsObjects) StorageInfo() StorageInfo {
	info, err := getDiskInfo((fs.fsPath))
	errorIf(err, "Unable to get disk info %#v", fs.fsPath)
	storageInfo := StorageInfo{
		Total: info.Total,
		Free:  info.Free,
	}
	storageInfo.Backend.Type = FS
	return storageInfo
}

// Locking operations

// List namespace locks held in object layer
func (fs fsObjects) ListLocks(bucket, prefix string, duration time.Duration) ([]VolumeLockInfo, error) {
	return []VolumeLockInfo{}, NotImplemented{}
}

// Clear namespace locks held in object layer
func (fs fsObjects) ClearLocks([]VolumeLockInfo) error {
	return NotImplemented{}
}

/// Bucket operations

// getBucketDir - will convert incoming bucket names to
// corresponding valid bucket names on the backend in a platform
// compatible way for all operating systems.
func (fs fsObjects) getBucketDir(bucket string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", errors.Trace(BucketNameInvalid{Bucket: bucket})
	}

	bucketDir := pathJoin(fs.fsPath, bucket)
	return bucketDir, nil
}

func (fs fsObjects) statBucketDir(bucket string) (os.FileInfo, error) {
	bucketDir, err := fs.getBucketDir(bucket)
	if err != nil {
		return nil, err
	}
	st, err := fsStatVolume(bucketDir)
	if err != nil {
		return nil, err
	}
	return st, nil
}

// MakeBucket - create a new bucket, returns if it
// already exists.
func (fs fsObjects) MakeBucketWithLocation(bucket, location string) error {
	bucketLock := fs.nsMutex.NewNSLock(bucket, "")
	if err := bucketLock.GetLock(globalObjectTimeout); err != nil {
		return err
	}
	defer bucketLock.Unlock()
	bucketDir, err := fs.getBucketDir(bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}

	if err = fsMkdir(bucketDir); err != nil {
		return toObjectErr(err, bucket)
	}

	return nil
}

// GetBucketInfo - fetch bucket metadata info.
func (fs fsObjects) GetBucketInfo(bucket string) (bi BucketInfo, e error) {
	st, err := fs.statBucketDir(bucket)
	if err != nil {
		return bi, toObjectErr(err, bucket)
	}

	// As os.Stat() doesn't carry other than ModTime(), use ModTime() as CreatedTime.
	createdTime := st.ModTime()
	return BucketInfo{
		Name:    bucket,
		Created: createdTime,
	}, nil
}

// ListBuckets - list all s3 compatible buckets (directories) at fsPath.
func (fs fsObjects) ListBuckets() ([]BucketInfo, error) {
	if err := checkPathLength(fs.fsPath); err != nil {
		return nil, errors.Trace(err)
	}
	var bucketInfos []BucketInfo
	entries, err := readDir((fs.fsPath))
	if err != nil {
		return nil, toObjectErr(errors.Trace(errDiskNotFound))
	}

	for _, entry := range entries {
		// Ignore all reserved bucket names and invalid bucket names.
		if isReservedOrInvalidBucket(entry) {
			continue
		}
		var fi os.FileInfo
		fi, err = fsStatVolume(pathJoin(fs.fsPath, entry))
		// There seems like no practical reason to check for errors
		// at this point, if there are indeed errors we can simply
		// just ignore such buckets and list only those which
		// return proper Stat information instead.
		if err != nil {
			// Ignore any errors returned here.
			continue
		}
		bucketInfos = append(bucketInfos, BucketInfo{
			Name: fi.Name(),
			// As os.Stat() doesnt carry CreatedTime, use ModTime() as CreatedTime.
			Created: fi.ModTime(),
		})
	}

	// Sort bucket infos by bucket name.
	sort.Sort(byBucketName(bucketInfos))

	// Succes.
	return bucketInfos, nil
}

// DeleteBucket - delete a bucket and all the metadata associated
// with the bucket including pending multipart, object metadata.
func (fs fsObjects) DeleteBucket(bucket string) error {
	bucketDir, err := fs.getBucketDir(bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}

	// Attempt to delete regular bucket.
	if err = fsRemoveDir(bucketDir); err != nil {
		return toObjectErr(err, bucket)
	}

	// Cleanup all the previously incomplete multiparts.
	minioMetaMultipartBucketDir := pathJoin(fs.fsPath, minioMetaMultipartBucket, bucket)
	if err = fsRemoveAll(minioMetaMultipartBucketDir); err != nil {
		return toObjectErr(err, bucket)
	}

	// Cleanup all the bucket metadata.
	minioMetadataBucketDir := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket)
	if err = fsRemoveAll(minioMetadataBucketDir); err != nil {
		return toObjectErr(err, bucket)
	}

	return nil
}

/// Object Operations

// CopyObject - copy object source object to destination object.
// if source object and destination object are same we only
// update metadata.
func (fs fsObjects) CopyObject(srcBucket, srcObject, dstBucket, dstObject string, metadata map[string]string, srcEtag string) (oi ObjectInfo, e error) {
	cpSrcDstSame := srcBucket == dstBucket && srcObject == dstObject
	// Hold write lock on destination since in both cases
	// - if source and destination are same
	// - if source and destination are different
	// it is the sole mutating state.
	objectDWLock := fs.nsMutex.NewNSLock(dstBucket, dstObject)
	if err := objectDWLock.GetLock(globalObjectTimeout); err != nil {
		return oi, err
	}
	defer objectDWLock.Unlock()
	// if source and destination are different, we have to hold
	// additional read lock as well to protect against writes on
	// source.
	if !cpSrcDstSame {
		// Hold read locks on source object only if we are
		// going to read data from source object.
		objectSRLock := fs.nsMutex.NewNSLock(srcBucket, srcObject)
		if err := objectSRLock.GetRLock(globalObjectTimeout); err != nil {
			return oi, err
		}
		defer objectSRLock.RUnlock()
	}
	if _, err := fs.statBucketDir(srcBucket); err != nil {
		return oi, toObjectErr(err, srcBucket)
	}

	// Stat the file to get file size.
	fi, err := fsStatFile(pathJoin(fs.fsPath, srcBucket, srcObject))
	if err != nil {
		return oi, toObjectErr(err, srcBucket, srcObject)
	}
	if srcEtag != "" {
		etag, perr := fs.getObjectETag(srcBucket, srcObject)
		if perr != nil {
			return oi, toObjectErr(perr, srcBucket, srcObject)
		}
		if etag != srcEtag {
			return oi, toObjectErr(errors.Trace(InvalidETag{}), srcBucket, srcObject)
		}
	}

	// Check if this request is only metadata update.
	cpMetadataOnly := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))
	if cpMetadataOnly {
		fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, srcBucket, srcObject, fsMetaJSONFile)
		var wlk *lock.LockedFile
		wlk, err = fs.rwPool.Write(fsMetaPath)
		if err != nil {
			return oi, toObjectErr(errors.Trace(err), srcBucket, srcObject)
		}
		// This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()

		// Save objects' metadata in `fs.json`.
		fsMeta := newFSMetaV1()
		fsMeta.Meta = metadata
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		// Return the new object info.
		return fsMeta.ToObjectInfo(srcBucket, srcObject, fi), nil
	}

	// Length of the file to read.
	length := fi.Size()

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()

	go func() {
		var startOffset int64 // Read the whole file.
		if gerr := fs.getObject(srcBucket, srcObject, startOffset, length, pipeWriter, ""); gerr != nil {
			errorIf(gerr, "Unable to read %s/%s.", srcBucket, srcObject)
			pipeWriter.CloseWithError(gerr)
			return
		}
		pipeWriter.Close() // Close writer explicitly signalling we wrote all data.
	}()

	hashReader, err := hash.NewReader(pipeReader, length, "", "")
	if err != nil {
		return oi, toObjectErr(err, dstBucket, dstObject)
	}

	objInfo, err := fs.putObject(dstBucket, dstObject, hashReader, metadata)
	if err != nil {
		return oi, toObjectErr(err, dstBucket, dstObject)
	}

	// Explicitly close the reader.
	pipeReader.Close()

	return objInfo, nil
}

// GetObject - reads an object from the disk.
// Supports additional parameters like offset and length
// which are synonymous with HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (fs fsObjects) GetObject(bucket, object string, offset int64, length int64, writer io.Writer, etag string) (err error) {
	if err = checkBucketAndObjectNamesFS(bucket, object); err != nil {
		return err
	}

	// Lock the object before reading.
	objectLock := fs.nsMutex.NewNSLock(bucket, object)
	if err := objectLock.GetRLock(globalObjectTimeout); err != nil {
		return err
	}
	defer objectLock.RUnlock()
	return fs.getObject(bucket, object, offset, length, writer, etag)
}

// getObject - wrapper for GetObject
func (fs fsObjects) getObject(bucket, object string, offset int64, length int64, writer io.Writer, etag string) (err error) {
	if _, err = fs.statBucketDir(bucket); err != nil {
		return toObjectErr(err, bucket)
	}

	// Offset cannot be negative.
	if offset < 0 {
		return toObjectErr(errors.Trace(errUnexpected), bucket, object)
	}

	// Writer cannot be nil.
	if writer == nil {
		return toObjectErr(errors.Trace(errUnexpected), bucket, object)
	}

	// If its a directory request, we return an empty body.
	if hasSuffix(object, slashSeparator) {
		_, err = writer.Write([]byte(""))
		return toObjectErr(errors.Trace(err), bucket, object)
	}

	if bucket != minioMetaBucket {
		fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fsMetaJSONFile)
		_, err = fs.rwPool.Open(fsMetaPath)
		if err != nil && err != errFileNotFound {
			return toObjectErr(errors.Trace(err), bucket, object)
		}
		defer fs.rwPool.Close(fsMetaPath)
	}

	if etag != "" {
		objEtag, perr := fs.getObjectETag(bucket, object)
		if perr != nil {
			return toObjectErr(errors.Trace(perr), bucket, object)
		}
		if objEtag != etag {
			return toObjectErr(errors.Trace(InvalidETag{}), bucket, object)
		}
	}
	// Read the object, doesn't exist returns an s3 compatible error.
	fsObjPath := pathJoin(fs.fsPath, bucket, object)
	reader, size, err := fsOpenFile(fsObjPath, offset)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	defer reader.Close()

	bufSize := int64(readSizeV1)
	if length > 0 && bufSize > length {
		bufSize = length
	}

	// For negative length we read everything.
	if length < 0 {
		length = size - offset
	}

	// Reply back invalid range if the input offset and length fall out of range.
	if offset > size || offset+length > size {
		return errors.Trace(InvalidRange{offset, length, size})
	}

	// Allocate a staging buffer.
	buf := make([]byte, int(bufSize))

	_, err = io.CopyBuffer(writer, io.LimitReader(reader, length), buf)

	return toObjectErr(errors.Trace(err), bucket, object)
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (fs fsObjects) getObjectInfo(bucket, object string) (oi ObjectInfo, e error) {
	fsMeta := fsMetaV1{}
	fi, err := fsStatDir(pathJoin(fs.fsPath, bucket, object))
	if err != nil && errors.Cause(err) != errFileAccessDenied {
		return oi, toObjectErr(err, bucket, object)
	}
	if fi != nil {
		// If file found and request was with object ending with "/", consider it
		// as directory and return object info
		if hasSuffix(object, slashSeparator) {
			return fsMeta.ToObjectInfo(bucket, object, fi), nil
		}
		return oi, toObjectErr(errFileNotFound, bucket, object)
	}

	fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, object, fsMetaJSONFile)

	// Read `fs.json` to perhaps contend with
	// parallel Put() operations.
	rlk, err := fs.rwPool.Open(fsMetaPath)
	if err == nil {
		// Read from fs metadata only if it exists.
		defer fs.rwPool.Close(fsMetaPath)
		if _, rerr := fsMeta.ReadFrom(rlk.LockedFile); rerr != nil {
			// `fs.json` can be empty due to previously failed
			// PutObject() transaction, if we arrive at such
			// a situation we just ignore and continue.
			if errors.Cause(rerr) != io.EOF {
				return oi, toObjectErr(rerr, bucket, object)
			}
		}
	}

	// Ignore if `fs.json` is not available, this is true for pre-existing data.
	if err != nil && err != errFileNotFound {
		return oi, toObjectErr(errors.Trace(err), bucket, object)
	}

	// Stat the file to get file size.
	fi, err = fsStatFile(pathJoin(fs.fsPath, bucket, object))
	if err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// Checks bucket and object name validity, returns nil if both are valid.
func checkBucketAndObjectNamesFS(bucket, object string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return errors.Trace(BucketNameInvalid{Bucket: bucket})
	}
	// Verify if object is valid.
	if len(object) == 0 {
		return errors.Trace(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	if !IsValidObjectPrefix(object) {
		return errors.Trace(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	return nil
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (fs fsObjects) GetObjectInfo(bucket, object string) (oi ObjectInfo, e error) {
	// Lock the object before reading.
	objectLock := fs.nsMutex.NewNSLock(bucket, object)
	if err := objectLock.GetRLock(globalObjectTimeout); err != nil {
		return oi, err
	}
	defer objectLock.RUnlock()

	if err := checkBucketAndObjectNamesFS(bucket, object); err != nil {
		return oi, err
	}

	if _, err := fs.statBucketDir(bucket); err != nil {
		return oi, toObjectErr(err, bucket)
	}

	return fs.getObjectInfo(bucket, object)
}

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (fs fsObjects) parentDirIsObject(bucket, parent string) bool {
	var isParentDirObject func(string) bool
	isParentDirObject = func(p string) bool {
		if p == "." || p == "/" {
			return false
		}
		if _, err := fsStatFile(pathJoin(fs.fsPath, bucket, p)); err == nil {
			// If there is already a file at prefix "p", return true.
			return true
		}

		// Check if there is a file as one of the parent paths.
		return isParentDirObject(path.Dir(p))
	}
	return isParentDirObject(parent)
}

// PutObject - creates an object upon reading from the input stream
// until EOF, writes data directly to configured filesystem path.
// Additionally writes `fs.json` which carries the necessary metadata
// for future object operations.
func (fs fsObjects) PutObject(bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, retErr error) {
	// Lock the object.
	objectLock := fs.nsMutex.NewNSLock(bucket, object)
	if err := objectLock.GetLock(globalObjectTimeout); err != nil {
		return objInfo, err
	}
	defer objectLock.Unlock()
	return fs.putObject(bucket, object, data, metadata)
}

// putObject - wrapper for PutObject
func (fs fsObjects) putObject(bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, retErr error) {
	// No metadata is set, allocate a new one.
	if metadata == nil {
		metadata = make(map[string]string)
	}
	var err error

	// Validate if bucket name is valid and exists.
	if _, err = fs.statBucketDir(bucket); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket)
	}

	fsMeta := newFSMetaV1()
	fsMeta.Meta = metadata

	// This is a special case with size as '0' and object ends
	// with a slash separator, we treat it like a valid operation
	// and return success.
	if isObjectDir(object, data.Size()) {
		// Check if an object is present as one of the parent dir.
		if fs.parentDirIsObject(bucket, path.Dir(object)) {
			return ObjectInfo{}, toObjectErr(errors.Trace(errFileAccessDenied), bucket, object)
		}
		if err = mkdirAll(pathJoin(fs.fsPath, bucket, object), 0777); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		var fi os.FileInfo
		if fi, err = fsStatDir(pathJoin(fs.fsPath, bucket, object)); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	if err = checkPutObjectArgs(bucket, object, fs); err != nil {
		return ObjectInfo{}, err
	}

	// Check if an object is present as one of the parent dir.
	if fs.parentDirIsObject(bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(errors.Trace(errFileAccessDenied), bucket, object)
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < 0 {
		return ObjectInfo{}, errors.Trace(errInvalidArgument)
	}

	var wlk *lock.LockedFile
	if bucket != minioMetaBucket {
		bucketMetaDir := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix)
		fsMetaPath := pathJoin(bucketMetaDir, bucket, object, fsMetaJSONFile)
		wlk, err = fs.rwPool.Create(fsMetaPath)
		if err != nil {
			return ObjectInfo{}, toObjectErr(errors.Trace(err), bucket, object)
		}
		// This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()
		defer func() {
			// Remove meta file when PutObject encounters any error
			if retErr != nil {
				tmpDir := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID)
				fsRemoveMeta(bucketMetaDir, fsMetaPath, tmpDir)
			}
		}()
	}

	// Uploaded object will first be written to the temporary location which will eventually
	// be renamed to the actual location. It is first written to the temporary location
	// so that cleaning it up will be easy if the server goes down.
	tempObj := mustGetUUID()

	// Allocate a buffer to Read() from request body
	bufSize := int64(readSizeV1)
	if size := data.Size(); size > 0 && bufSize > size {
		bufSize = size
	}

	buf := make([]byte, int(bufSize))
	fsTmpObjPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, tempObj)
	bytesWritten, err := fsCreateFile(fsTmpObjPath, data, buf, data.Size())
	if err != nil {
		fsRemoveFile(fsTmpObjPath)
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	metadata["etag"] = hex.EncodeToString(data.MD5Current())

	// Should return IncompleteBody{} error when reader has fewer
	// bytes than specified in request header.
	if bytesWritten < data.Size() {
		fsRemoveFile(fsTmpObjPath)
		return ObjectInfo{}, errors.Trace(IncompleteBody{})
	}

	// Delete the temporary object in the case of a
	// failure. If PutObject succeeds, then there would be
	// nothing to delete.
	defer fsRemoveFile(fsTmpObjPath)

	// Entire object was written to the temp location, now it's safe to rename it to the actual location.
	fsNSObjPath := pathJoin(fs.fsPath, bucket, object)
	if err = fsRenameFile(fsTmpObjPath, fsNSObjPath); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		// Write FS metadata after a successful namespace operation.
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	// Stat the file to fetch timestamp, size.
	fi, err := fsStatFile(pathJoin(fs.fsPath, bucket, object))
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Success.
	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// DeleteObject - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (fs fsObjects) DeleteObject(bucket, object string) error {
	// Acquire a write lock before deleting the object.
	objectLock := fs.nsMutex.NewNSLock(bucket, object)
	if err := objectLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objectLock.Unlock()

	if err := checkBucketAndObjectNamesFS(bucket, object); err != nil {
		return err
	}

	if _, err := fs.statBucketDir(bucket); err != nil {
		return toObjectErr(err, bucket)
	}

	minioMetaBucketDir := pathJoin(fs.fsPath, minioMetaBucket)
	fsMetaPath := pathJoin(minioMetaBucketDir, bucketMetaPrefix, bucket, object, fsMetaJSONFile)
	if bucket != minioMetaBucket {
		rwlk, lerr := fs.rwPool.Write(fsMetaPath)
		if lerr == nil {
			// This close will allow for fs locks to be synchronized on `fs.json`.
			defer rwlk.Close()
		}
		if lerr != nil && lerr != errFileNotFound {
			return toObjectErr(errors.Trace(lerr), bucket, object)
		}
	}

	// Delete the object.
	if err := fsDeleteFile(pathJoin(fs.fsPath, bucket), pathJoin(fs.fsPath, bucket, object)); err != nil {
		return toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		// Delete the metadata object.
		err := fsDeleteFile(minioMetaBucketDir, fsMetaPath)
		if err != nil && errors.Cause(err) != errFileNotFound {
			return toObjectErr(err, bucket, object)
		}
	}
	return nil
}

// list of all errors that can be ignored in tree walk operation in FS
var fsTreeWalkIgnoredErrs = append(baseIgnoredErrs, []error{
	errFileNotFound,
	errVolumeNotFound,
}...)

// Returns function "listDir" of the type listDirFunc.
// isLeaf - is used by listDir function to check if an entry
// is a leaf or non-leaf entry.
func (fs fsObjects) listDirFactory(isLeaf isLeafFunc) listDirFunc {
	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (entries []string, delayIsLeaf bool, err error) {
		entries, err = readDir(pathJoin(fs.fsPath, bucket, prefixDir))
		if err != nil {
			return nil, false, err
		}
		entries, delayIsLeaf = filterListEntries(bucket, prefixDir, entries, prefixEntry, isLeaf)
		return entries, delayIsLeaf, nil
	}

	// Return list factory instance.
	return listDir
}

// getObjectETag is a helper function, which returns only the md5sum
// of the file on the disk.
func (fs fsObjects) getObjectETag(bucket, entry string) (string, error) {
	fsMetaPath := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix, bucket, entry, fsMetaJSONFile)

	// Read `fs.json` to perhaps contend with
	// parallel Put() operations.
	rlk, err := fs.rwPool.Open(fsMetaPath)
	// Ignore if `fs.json` is not available, this is true for pre-existing data.
	if err != nil && err != errFileNotFound {
		return "", toObjectErr(errors.Trace(err), bucket, entry)
	}

	// If file is not found, we don't need to proceed forward.
	if err == errFileNotFound {
		return "", nil
	}

	// Read from fs metadata only if it exists.
	defer fs.rwPool.Close(fsMetaPath)

	// Fetch the size of the underlying file.
	fi, err := rlk.LockedFile.Stat()
	if err != nil {
		return "", toObjectErr(errors.Trace(err), bucket, entry)
	}

	// `fs.json` can be empty due to previously failed
	// PutObject() transaction, if we arrive at such
	// a situation we just ignore and continue.
	if fi.Size() == 0 {
		return "", nil
	}

	// Wrap the locked file in a ReadAt() backend section reader to
	// make sure the underlying offsets don't move.
	fsMetaBuf, err := ioutil.ReadAll(io.NewSectionReader(rlk.LockedFile, 0, fi.Size()))
	if err != nil {
		return "", errors.Trace(err)
	}

	// Check if FS metadata is valid, if not return error.
	if !isFSMetaValid(parseFSVersion(fsMetaBuf), parseFSFormat(fsMetaBuf)) {
		return "", toObjectErr(errors.Trace(errCorruptedFormat), bucket, entry)
	}

	return extractETag(parseFSMetaMap(fsMetaBuf)), nil
}

// ListObjects - list all objects at prefix upto maxKeys., optionally delimited by '/'. Maintains the list pool
// state for future re-entrant list requests.
func (fs fsObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	if err := checkListObjsArgs(bucket, prefix, marker, delimiter, fs); err != nil {
		return loi, err
	}

	if _, err := fs.statBucketDir(bucket); err != nil {
		return loi, err
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return loi, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if delimiter == slashSeparator && prefix == slashSeparator {
		return loi, nil
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

	// Convert entry to ObjectInfo
	entryToObjectInfo := func(entry string) (objInfo ObjectInfo, err error) {
		// Protect the entry from concurrent deletes, or renames.
		objectLock := fs.nsMutex.NewNSLock(bucket, entry)
		if err = objectLock.GetRLock(globalListingTimeout); err != nil {
			return ObjectInfo{}, err
		}

		if hasSuffix(entry, slashSeparator) {
			var fi os.FileInfo
			fi, err = fsStatDir(pathJoin(fs.fsPath, bucket, entry))
			objectLock.RUnlock()
			if err != nil {
				return objInfo, err
			}
			// Success.
			return ObjectInfo{
				// Object name needs to be full path.
				Name:    entry,
				Bucket:  bucket,
				Size:    fi.Size(),
				ModTime: fi.ModTime(),
				IsDir:   fi.IsDir(),
			}, nil
		}

		var etag string
		etag, err = fs.getObjectETag(bucket, entry)
		objectLock.RUnlock()
		if err != nil {
			return ObjectInfo{}, err
		}

		// Stat the file to get file size.
		var fi os.FileInfo
		fi, err = fsStatFile(pathJoin(fs.fsPath, bucket, entry))
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, entry)
		}

		// Success.
		return ObjectInfo{
			Name:    entry,
			Bucket:  bucket,
			Size:    fi.Size(),
			ModTime: fi.ModTime(),
			IsDir:   fi.IsDir(),
			ETag:    etag,
		}, nil
	}

	heal := false // true only for xl.ListObjectsHeal()
	walkResultCh, endWalkCh := fs.listPool.Release(listParams{bucket, recursive, marker, prefix, heal})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		isLeaf := func(bucket, object string) bool {
			// bucket argument is unused as we don't need to StatFile
			// to figure if it's a file, just need to check that the
			// object string does not end with "/".
			return !hasSuffix(object, slashSeparator)
		}
		listDir := fs.listDirFactory(isLeaf)
		walkResultCh = startTreeWalk(bucket, prefix, marker, recursive, listDir, isLeaf, endWalkCh)
	}

	var objInfos []ObjectInfo
	var eof bool
	var nextMarker string

	// List until maxKeys requested.
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
			if errors.Cause(walkResult.err) == errFileNotFound {
				return loi, nil
			}
			return loi, toObjectErr(walkResult.err, bucket, prefix)
		}
		objInfo, err := entryToObjectInfo(walkResult.entry)
		if err != nil {
			errorIf(err, "Unable to fetch object info for %s", walkResult.entry)
			return loi, nil
		}
		nextMarker = objInfo.Name
		objInfos = append(objInfos, objInfo)
		if walkResult.end {
			eof = true
			break
		}
		i++
	}

	// Save list routine for the next marker if we haven't reached EOF.
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

	// Success.
	return result, nil
}

// HealObject - no-op for fs. Valid only for XL.
func (fs fsObjects) HealObject(bucket, object string, dryRun bool) (
	res madmin.HealResultItem, err error) {

	return res, errors.Trace(NotImplemented{})
}

// HealBucket - no-op for fs, Valid only for XL.
func (fs fsObjects) HealBucket(bucket string, dryRun bool) ([]madmin.HealResultItem,
	error) {

	return nil, errors.Trace(NotImplemented{})
}

// ListObjectsHeal - list all objects to be healed. Valid only for XL
func (fs fsObjects) ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	return loi, errors.Trace(NotImplemented{})
}

// ListBucketsHeal - list all buckets to be healed. Valid only for XL
func (fs fsObjects) ListBucketsHeal() ([]BucketInfo, error) {
	return []BucketInfo{}, errors.Trace(NotImplemented{})
}
