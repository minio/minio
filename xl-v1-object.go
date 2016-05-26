package main

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/pkg/mimedb"
)

// nullReadCloser - returns 0 bytes and io.EOF upon first read attempt.
type nullReadCloser struct{}

func (n nullReadCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (n nullReadCloser) Close() error             { return nil }

/// Object Operations

// GetObject - get an object.
func (xl xlObjects) GetObject(bucket, object string, startOffset int64) (io.ReadCloser, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return nil, BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return nil, ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	// Lock the object before reading.
	nsMutex.RLock(bucket, object)
	defer nsMutex.RUnlock(bucket, object)

	// Read metadata associated with the object.
	xlMeta, err := xl.readXLMetadata(bucket, object)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}
	// List all online disks.
	onlineDisks, _, err := xl.listOnlineDisks(bucket, object)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}
	// For zero byte files, return a null reader.
	if xlMeta.Stat.Size == 0 {
		return nullReadCloser{}, nil
	}
	erasure := newErasure(onlineDisks) // Initialize a new erasure with online disks

	// Get part index offset.
	partIndex, partOffset, err := xlMeta.objectToPartOffset(startOffset)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}

	fileReader, fileWriter := io.Pipe()

	// Hold a read lock once more which can be released after the following go-routine ends.
	// We hold RLock once more because the current function would return before the go routine below
	// executes and hence releasing the read lock (because of defer'ed nsMutex.RUnlock() call).
	nsMutex.RLock(bucket, object)
	go func() {
		defer nsMutex.RUnlock(bucket, object)
		for ; partIndex < len(xlMeta.Parts); partIndex++ {
			part := xlMeta.Parts[partIndex]
			r, err := erasure.ReadFile(bucket, pathJoin(object, part.Name), partOffset, part.Size)
			if err != nil {
				fileWriter.CloseWithError(toObjectErr(err, bucket, object))
				return
			}
			// Reset part offset to 0 to read rest of the parts from
			// the beginning.
			partOffset = 0
			if _, err = io.Copy(fileWriter, r); err != nil {
				switch reader := r.(type) {
				case *io.PipeReader:
					reader.CloseWithError(err)
				case io.ReadCloser:
					reader.Close()
				}
				fileWriter.CloseWithError(toObjectErr(err, bucket, object))
				return
			}
			// Close the readerCloser that reads multiparts of an object.
			// Not closing leaks underlying file descriptors.
			r.Close()
		}
		fileWriter.Close()
	}()
	return fileReader, nil
}

// GetObjectInfo - get object info.
func (xl xlObjects) GetObjectInfo(bucket, object string) (ObjectInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return ObjectInfo{}, ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	nsMutex.RLock(bucket, object)
	defer nsMutex.RUnlock(bucket, object)
	info, err := xl.getObjectInfo(bucket, object)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	return info, nil
}

// getObjectInfo - get object info.
func (xl xlObjects) getObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	var xlMeta xlMetaV1
	xlMeta, err = xl.readXLMetadata(bucket, object)
	if err != nil {
		// Return error.
		return ObjectInfo{}, err
	}
	objInfo = ObjectInfo{}
	objInfo.IsDir = false
	objInfo.Bucket = bucket
	objInfo.Name = object
	objInfo.Size = xlMeta.Stat.Size
	objInfo.ModTime = xlMeta.Stat.ModTime
	objInfo.MD5Sum = xlMeta.Meta["md5Sum"]
	objInfo.ContentType = xlMeta.Meta["content-type"]
	objInfo.ContentEncoding = xlMeta.Meta["content-encoding"]
	return objInfo, nil
}

// renameObject - renaming all source objects to destination object across all disks.
func (xl xlObjects) renameObject(srcBucket, srcObject, dstBucket, dstObject string) error {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var errs = make([]error, len(xl.storageDisks))

	// Rename file on all underlying storage disks.
	for index, disk := range xl.storageDisks {
		// Append "/" as srcObject and dstObject are either leaf-dirs or non-leaf-dris.
		// If srcObject is an object instead of prefix we just rename the leaf-dir and
		// not rename the part and metadata files separately.
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := disk.RenameFile(srcBucket, retainSlash(srcObject), dstBucket, retainSlash(dstObject))
			if err != nil {
				errs[index] = err
			}
			errs[index] = nil
		}(index, disk)
	}

	// Wait for all RenameFile to finish.
	wg.Wait()

	// Gather err count.
	var errCount = 0
	for _, err := range errs {
		if err == nil {
			continue
		}
		errCount++
	}
	// We can safely allow RenameFile errors up to len(xl.storageDisks) - xl.writeQuorum
	// otherwise return failure. Cleanup successful renames.
	if errCount > len(xl.storageDisks)-xl.writeQuorum {
		// Special condition if readQuorum exists, then return success.
		if errCount <= len(xl.storageDisks)-xl.readQuorum {
			return nil
		}
		xl.deleteObject(srcBucket, srcObject)
		return errWriteQuorum
	}
	return nil
}

// PutObject - create an object.
func (xl xlObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify bucket exists.
	if !xl.isBucketExist(bucket) {
		return "", BucketNotFound{Bucket: bucket}
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
	nsMutex.Lock(bucket, object)
	defer nsMutex.Unlock(bucket, object)

	tempErasureObj := path.Join(tmpMetaPrefix, bucket, object, "object1")
	tempObj := path.Join(tmpMetaPrefix, bucket, object)

	// List all online disks.
	onlineDisks, higherVersion, err := xl.listOnlineDisks(bucket, object)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}
	// Increment version only if we have online disks less than configured storage disks.
	if diskCount(onlineDisks) < len(xl.storageDisks) {
		higherVersion++
	}
	erasure := newErasure(onlineDisks) // Initialize a new erasure with online disks
	fileWriter, err := erasure.CreateFile(minioMetaBucket, tempErasureObj)
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
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", toObjectErr(clErr, bucket, object)
			}
			return "", toObjectErr(err, bucket, object)
		}
	} else {
		if _, err = io.Copy(multiWriter, data); err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", toObjectErr(clErr, bucket, object)
			}
			return "", toObjectErr(err, bucket, object)
		}
	}

	// Save additional erasureMetadata.
	modTime := time.Now().UTC()

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	// Update the md5sum if not set with the newly calculated one.
	if len(metadata["md5Sum"]) == 0 {
		metadata["md5Sum"] = newMD5Hex
	}
	// If not set default to "application/octet-stream"
	if metadata["content-type"] == "" {
		contentType := "application/octet-stream"
		if objectExt := filepath.Ext(object); objectExt != "" {
			content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]
			if ok {
				contentType = content.ContentType
			}
		}
		metadata["content-type"] = contentType
	}

	// md5Hex representation.
	md5Hex := metadata["md5Sum"]
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			if err = safeCloseAndRemove(fileWriter); err != nil {
				return "", toObjectErr(err, bucket, object)
			}
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}

	err = fileWriter.Close()
	if err != nil {
		if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
			return "", toObjectErr(clErr, bucket, object)
		}
		return "", toObjectErr(err, bucket, object)
	}

	// Check if an object is present as one of the parent dir.
	if xl.parentDirIsObject(bucket, path.Dir(object)) {
		return "", toObjectErr(errFileAccessDenied, bucket, object)
	}

	// Delete if an object already exists.
	err = xl.deleteObject(bucket, object)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	err = xl.renameObject(minioMetaBucket, tempObj, bucket, object)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	xlMeta := newXLMetaV1(xl.dataBlocks, xl.parityBlocks)
	xlMeta.Meta = metadata
	xlMeta.Stat.Size = size
	xlMeta.Stat.ModTime = modTime
	xlMeta.Stat.Version = higherVersion
	xlMeta.AddObjectPart(1, "object1", newMD5Hex, xlMeta.Stat.Size)
	if err = xl.writeXLMetadata(bucket, object, xlMeta); err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Return md5sum, successfully wrote object.
	return newMD5Hex, nil
}

// deleteObject - deletes a regular object.
func (xl xlObjects) deleteObject(bucket, object string) error {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(xl.storageDisks))

	for index, disk := range xl.storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := cleanupDir(disk, bucket, object)
			if err != nil {
				dErrs[index] = err
				return
			}
			dErrs[index] = nil
		}(index, disk)
	}

	// Wait for all routines to finish.
	wg.Wait()

	var fileNotFoundCnt, deleteFileErr int
	// Count for specific errors.
	for _, err := range dErrs {
		if err == nil {
			continue
		}
		// If file not found, count them.
		if err == errFileNotFound {
			fileNotFoundCnt++
			continue
		}

		// Update error counter separately.
		deleteFileErr++
	}

	// Return err if all disks report file not found.
	if fileNotFoundCnt == len(xl.storageDisks) {
		return errFileNotFound
	} else if deleteFileErr > len(xl.storageDisks)-xl.writeQuorum {
		// Return errWriteQuorum if errors were more than
		// allowed write quorum.
		return errWriteQuorum
	}

	return nil
}

// DeleteObject - delete the object.
func (xl xlObjects) DeleteObject(bucket, object string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	nsMutex.Lock(bucket, object)
	defer nsMutex.Unlock(bucket, object)
	xl.deleteObject(bucket, object)
	return nil
}
