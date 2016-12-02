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
	"hash"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/pkg/bpool"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/objcache"
)

// list all errors which can be ignored in object operations.
var objectOpIgnoredErrs = []error{
	errDiskNotFound,
	errDiskAccessDenied,
	errFaultyDisk,
	errFaultyRemoteDisk,
}

/// Object Operations

// GetObject - reads an object erasured coded across multiple
// disks. Supports additional parameters like offset and length
// which is synonymous with HTTP Range requests.
//
// startOffset indicates the location at which the client requested
// object to be read at. length indicates the total length of the
// object requested by client.
func (xl xlObjects) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) error {
	if err := checkGetObjArgs(bucket, object); err != nil {
		return err
	}
	// Start offset and length cannot be negative.
	if startOffset < 0 || length < 0 {
		return traceError(errUnexpected)
	}
	// Writer cannot be nil.
	if writer == nil {
		return traceError(errUnexpected)
	}

	// Lock the object before reading.
	objectLock := nsMutex.NewNSLock(bucket, object)
	objectLock.RLock()
	defer objectLock.RUnlock()

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllXLMetadata(xl.storageDisks, bucket, object)
	// Do we have read quorum?
	if !isDiskQuorum(errs, xl.readQuorum) {
		return traceError(InsufficientReadQuorum{}, errs...)
	}

	if reducedErr := reduceReadQuorumErrs(errs, objectOpIgnoredErrs, xl.readQuorum); reducedErr != nil {
		return toObjectErr(reducedErr, bucket, object)
	}

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(xl.storageDisks, metaArr, errs)

	// Pick latest valid metadata.
	xlMeta, err := pickValidXLMeta(metaArr, modTime)
	if err != nil {
		return err
	}

	// Reorder online disks based on erasure distribution order.
	onlineDisks = getOrderedDisks(xlMeta.Erasure.Distribution, onlineDisks)

	// Reorder parts metadata based on erasure distribution order.
	metaArr = getOrderedPartsMetadata(xlMeta.Erasure.Distribution, metaArr)

	// Reply back invalid range if the input offset and length fall out of range.
	if startOffset > xlMeta.Stat.Size || length > xlMeta.Stat.Size {
		return traceError(InvalidRange{startOffset, length, xlMeta.Stat.Size})
	}

	// Reply if we have inputs with offset and length.
	if startOffset+length > xlMeta.Stat.Size {
		return traceError(InvalidRange{startOffset, length, xlMeta.Stat.Size})
	}

	// Get start part index and offset.
	partIndex, partOffset, err := xlMeta.ObjectToPartOffset(startOffset)
	if err != nil {
		return traceError(InvalidRange{startOffset, length, xlMeta.Stat.Size})
	}

	// Get last part index to read given length.
	lastPartIndex, _, err := xlMeta.ObjectToPartOffset(startOffset + length - 1)
	if err != nil {
		return traceError(InvalidRange{startOffset, length, xlMeta.Stat.Size})
	}

	// Save the writer.
	mw := writer

	// Object cache enabled block.
	if xlMeta.Stat.Size > 0 && xl.objCacheEnabled {
		// Validate if we have previous cache.
		var cachedBuffer io.ReadSeeker
		cachedBuffer, err = xl.objCache.Open(path.Join(bucket, object), modTime)
		if err == nil { // Cache hit.
			// Advance the buffer to offset as if it was read.
			if _, err = cachedBuffer.Seek(startOffset, 0); err != nil { // Seek to the offset.
				return traceError(err)
			}
			// Write the requested length.
			if _, err = io.CopyN(writer, cachedBuffer, length); err != nil {
				return traceError(err)
			}
			return nil
		} // Cache miss.
		// For unknown error, return and error out.
		if err != objcache.ErrKeyNotFoundInCache {
			return traceError(err)
		} // Cache has not been found, fill the cache.

		// Cache is only set if whole object is being read.
		if startOffset == 0 && length == xlMeta.Stat.Size {
			// Proceed to set the cache.
			var newBuffer io.WriteCloser
			// Create a new entry in memory of length.
			newBuffer, err = xl.objCache.Create(path.Join(bucket, object), length)
			if err == nil {
				// Create a multi writer to write to both memory and client response.
				mw = io.MultiWriter(newBuffer, writer)
				defer newBuffer.Close()
			}
			// Ignore error if cache is full, proceed to write the object.
			if err != nil && err != objcache.ErrCacheFull {
				// For any other error return here.
				return toObjectErr(traceError(err), bucket, object)
			}
		}
	}

	totalBytesRead := int64(0)

	chunkSize := getChunkSize(xlMeta.Erasure.BlockSize, xlMeta.Erasure.DataBlocks)
	pool := bpool.NewBytePool(chunkSize, len(onlineDisks))

	// Read from all parts.
	for ; partIndex <= lastPartIndex; partIndex++ {
		if length == totalBytesRead {
			break
		}
		// Save the current part name and size.
		partName := xlMeta.Parts[partIndex].Name
		partSize := xlMeta.Parts[partIndex].Size

		readSize := partSize - partOffset
		// readSize should be adjusted so that we don't write more data than what was requested.
		if readSize > (length - totalBytesRead) {
			readSize = length - totalBytesRead
		}

		// Get the checksums of the current part.
		checkSums := make([]string, len(onlineDisks))
		var ckSumAlgo string
		for index, disk := range onlineDisks {
			// Disk is not found skip the checksum.
			if disk == nil {
				checkSums[index] = ""
				continue
			}
			ckSumInfo := metaArr[index].Erasure.GetCheckSumInfo(partName)
			checkSums[index] = ckSumInfo.Hash
			// Set checksum algo only once, while it is possible to have
			// different algos per block because of our `xl.json`.
			// It is not a requirement, set this only once for all the disks.
			if ckSumAlgo != "" {
				ckSumAlgo = ckSumInfo.Algorithm
			}
		}

		// Start erasure decoding and writing to the client.
		n, err := erasureReadFile(mw, onlineDisks, bucket, pathJoin(object, partName), partOffset, readSize, partSize, xlMeta.Erasure.BlockSize, xlMeta.Erasure.DataBlocks, xlMeta.Erasure.ParityBlocks, checkSums, ckSumAlgo, pool)
		if err != nil {
			errorIf(err, "Unable to read %s of the object `%s/%s`.", partName, bucket, object)
			return toObjectErr(err, bucket, object)
		}

		// Track total bytes read from disk and written to the client.
		totalBytesRead += n

		// partOffset will be valid only for the first part, hence reset it to 0 for
		// the remaining parts.
		partOffset = 0
	} // End of read all parts loop.

	// Return success.
	return nil
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (xl xlObjects) GetObjectInfo(bucket, object string) (ObjectInfo, error) {
	if err := checkGetObjArgs(bucket, object); err != nil {
		return ObjectInfo{}, err
	}

	objectLock := nsMutex.NewNSLock(bucket, object)
	objectLock.RLock()
	defer objectLock.RUnlock()

	info, err := xl.getObjectInfo(bucket, object)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	return info, nil
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (xl xlObjects) getObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	// returns xl meta map and stat info.
	xlStat, xlMetaMap, err := xl.readXLMetaStat(bucket, object)
	if err != nil {
		// Return error.
		return ObjectInfo{}, err
	}

	objInfo = ObjectInfo{
		IsDir:           false,
		Bucket:          bucket,
		Name:            object,
		Size:            xlStat.Size,
		ModTime:         xlStat.ModTime,
		MD5Sum:          xlMetaMap["md5Sum"],
		ContentType:     xlMetaMap["content-type"],
		ContentEncoding: xlMetaMap["content-encoding"],
	}

	// md5Sum has already been extracted into objInfo.MD5Sum.  We
	// need to remove it from xlMetaMap to avoid it from appearing as
	// part of response headers. e.g, X-Minio-* or X-Amz-*.

	delete(xlMetaMap, "md5Sum")
	objInfo.UserDefined = xlMetaMap
	return objInfo, nil
}

func undoRename(disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, isPart bool, errs []error) {
	var wg = &sync.WaitGroup{}
	// Undo rename object on disks where RenameFile succeeded.

	// If srcEntry/dstEntry are objects then add a trailing slash to copy
	// over all the parts inside the object directory
	if !isPart {
		srcEntry = retainSlash(srcEntry)
		dstEntry = retainSlash(dstEntry)
	}
	for index, disk := range disks {
		if disk == nil {
			continue
		}
		// Undo rename object in parallel.
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			if errs[index] != nil {
				return
			}
			_ = disk.RenameFile(dstBucket, dstEntry, srcBucket, srcEntry)
		}(index, disk)
	}
	wg.Wait()
}

// rename - common function that renamePart and renameObject use to rename
// the respective underlying storage layer representations.
func rename(disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, isPart bool, quorum int) error {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var errs = make([]error, len(disks))

	if !isPart {
		dstEntry = retainSlash(dstEntry)
		srcEntry = retainSlash(srcEntry)
	}

	// Rename file on all underlying storage disks.
	for index, disk := range disks {
		if disk == nil {
			errs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := disk.RenameFile(srcBucket, srcEntry, dstBucket, dstEntry)
			if err != nil && err != errFileNotFound {
				errs[index] = traceError(err)
			}
		}(index, disk)
	}

	// Wait for all renames to finish.
	wg.Wait()

	// We can safely allow RenameFile errors up to len(xl.storageDisks) - xl.writeQuorum
	// otherwise return failure. Cleanup successful renames.
	if !isDiskQuorum(errs, quorum) {
		// Undo all the partial rename operations.
		undoRename(disks, srcBucket, srcEntry, dstBucket, dstEntry, isPart, errs)
		return traceError(errXLWriteQuorum)
	}
	return reduceWriteQuorumErrs(errs, objectOpIgnoredErrs, quorum)
}

// renamePart - renames a part of the source object to the destination
// across all disks in parallel. Additionally if we have errors and do
// not have a readQuorum partially renamed files are renamed back to
// its proper location.
func renamePart(disks []StorageAPI, srcBucket, srcPart, dstBucket, dstPart string, quorum int) error {
	isPart := true
	return rename(disks, srcBucket, srcPart, dstBucket, dstPart, isPart, quorum)
}

// renameObject - renames all source objects to destination object
// across all disks in parallel. Additionally if we have errors and do
// not have a readQuorum partially renamed files are renamed back to
// its proper location.
func renameObject(disks []StorageAPI, srcBucket, srcObject, dstBucket, dstObject string, quorum int) error {
	isPart := false
	return rename(disks, srcBucket, srcObject, dstBucket, dstObject, isPart, quorum)
}

// PutObject - creates an object upon reading from the input stream
// until EOF, erasure codes the data across all disk and additionally
// writes `xl.json` which carries the necessary metadata for future
// object operations.
func (xl xlObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error) {
	if err = checkPutObjectArgs(bucket, object, xl); err != nil {
		return ObjectInfo{}, err
	}
	// No metadata is set, allocate a new one.
	if metadata == nil {
		metadata = make(map[string]string)
	}

	uniqueID := mustGetUUID()
	tempErasureObj := path.Join(uniqueID, "part.1")
	tempObj := uniqueID

	// Initialize md5 writer.
	md5Writer := md5.New()

	writers := []io.Writer{md5Writer}

	var sha256Writer hash.Hash
	if sha256sum != "" {
		sha256Writer = sha256.New()
		writers = append(writers, sha256Writer)
	}

	// Proceed to set the cache.
	var newBuffer io.WriteCloser

	// If caching is enabled, proceed to set the cache.
	if size > 0 && xl.objCacheEnabled {
		// PutObject invalidates any previously cached object in memory.
		xl.objCache.Delete(path.Join(bucket, object))

		// Create a new entry in memory of size.
		newBuffer, err = xl.objCache.Create(path.Join(bucket, object), size)
		if err == nil {
			// Create a multi writer to write to both memory and client response.
			writers = append(writers, newBuffer)
		}
		// Ignore error if cache is full, proceed to write the object.
		if err != nil && err != objcache.ErrCacheFull {
			// For any other error return here.
			return ObjectInfo{}, toObjectErr(traceError(err), bucket, object)
		}
	}

	mw := io.MultiWriter(writers...)

	// Limit the reader to its provided size if specified.
	var limitDataReader io.Reader
	if size > 0 {
		// This is done so that we can avoid erroneous clients sending
		// more data than the set content size.
		limitDataReader = io.LimitReader(data, size)
	} else {
		// else we read till EOF.
		limitDataReader = data
	}

	// Tee reader combines incoming data stream and md5, data read from input stream is written to md5.
	teeReader := io.TeeReader(limitDataReader, mw)

	// Initialize xl meta.
	xlMeta := newXLMetaV1(object, xl.dataBlocks, xl.parityBlocks)

	onlineDisks := getOrderedDisks(xlMeta.Erasure.Distribution, xl.storageDisks)

	// Delete temporary object in the event of failure. If
	// PutObject succeeded there would be no temporary object to
	// delete.
	defer xl.deleteObject(minioMetaTmpBucket, tempObj)

	if size > 0 {
		for _, disk := range onlineDisks {
			if disk != nil {
				actualSize := xl.sizeOnDisk(size, xlMeta.Erasure.BlockSize, xlMeta.Erasure.DataBlocks)
				disk.PrepareFile(minioMetaTmpBucket, tempErasureObj, actualSize)
			}
		}
	}

	// Erasure code data and write across all disks.
	sizeWritten, checkSums, err := erasureCreateFile(onlineDisks, minioMetaTmpBucket, tempErasureObj, teeReader, xlMeta.Erasure.BlockSize, xlMeta.Erasure.DataBlocks, xlMeta.Erasure.ParityBlocks, bitRotAlgo, xl.writeQuorum)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, minioMetaTmpBucket, tempErasureObj)
	}
	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if sizeWritten < size {
		return ObjectInfo{}, traceError(IncompleteBody{})
	}

	// For size == -1, perhaps client is sending in chunked encoding
	// set the size as size that was actually written.
	if size == -1 {
		size = sizeWritten
	}

	// Save additional erasureMetadata.
	modTime := time.Now().UTC()

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	// Update the md5sum if not set with the newly calculated one.
	if len(metadata["md5Sum"]) == 0 {
		metadata["md5Sum"] = newMD5Hex
	}

	// Guess content-type from the extension if possible.
	if metadata["content-type"] == "" {
		if objectExt := path.Ext(object); objectExt != "" {
			if content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]; ok {
				metadata["content-type"] = content.ContentType
			}
		}
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

	// Lock the object.
	objectLock := nsMutex.NewNSLock(bucket, object)
	objectLock.Lock()
	defer objectLock.Unlock()

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
	if xl.parentDirIsObject(bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(traceError(errFileAccessDenied), bucket, object)
	}

	// Rename if an object already exists to temporary location.
	newUniqueID := mustGetUUID()
	if xl.isObject(bucket, object) {
		// Delete the temporary copy of the object that existed before this PutObject request.
		defer xl.deleteObject(minioMetaTmpBucket, newUniqueID)

		// NOTE: Do not use online disks slice here.
		// The reason is that existing object should be purged
		// regardless of `xl.json` status and rolled back in case of errors.
		err = renameObject(xl.storageDisks, bucket, object, minioMetaTmpBucket, newUniqueID, xl.writeQuorum)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	// Fill all the necessary metadata.
	xlMeta.Meta = metadata
	xlMeta.Stat.Size = size
	xlMeta.Stat.ModTime = modTime

	// Add the final part.
	xlMeta.AddObjectPart(1, "part.1", newMD5Hex, xlMeta.Stat.Size)

	partsMetadata := make([]xlMetaV1, len(xl.storageDisks))
	// Update `xl.json` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index] = xlMeta
		partsMetadata[index].Erasure.AddCheckSumInfo(checkSumInfo{
			Name:      "part.1",
			Hash:      checkSums[index],
			Algorithm: bitRotAlgo,
		})
	}

	// Write unique `xl.json` for each disk.
	if err = writeUniqueXLMetadata(onlineDisks, minioMetaTmpBucket, tempObj, partsMetadata, xl.writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Rename the successfully written temporary object to final location.
	err = renameObject(onlineDisks, minioMetaTmpBucket, tempObj, bucket, object, xl.writeQuorum)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Once we have successfully renamed the object, Close the buffer which would
	// save the object on cache.
	if size > 0 && xl.objCacheEnabled && newBuffer != nil {
		newBuffer.Close()
	}

	objInfo = ObjectInfo{
		IsDir:           false,
		Bucket:          bucket,
		Name:            object,
		Size:            xlMeta.Stat.Size,
		ModTime:         xlMeta.Stat.ModTime,
		MD5Sum:          xlMeta.Meta["md5Sum"],
		ContentType:     xlMeta.Meta["content-type"],
		ContentEncoding: xlMeta.Meta["content-encoding"],
		UserDefined:     xlMeta.Meta,
	}
	return objInfo, nil
}

// deleteObject - wrapper for delete object, deletes an object from
// all the disks in parallel, including `xl.json` associated with the
// object.
func (xl xlObjects) deleteObject(bucket, object string) error {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(xl.storageDisks))

	for index, disk := range xl.storageDisks {
		if disk == nil {
			dErrs[index] = traceError(errDiskNotFound)
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := cleanupDir(disk, bucket, object)
			if err != nil && errorCause(err) != errVolumeNotFound {
				dErrs[index] = err
			}
		}(index, disk)
	}

	// Wait for all routines to finish.
	wg.Wait()

	// Do we have write quorum?
	if !isDiskQuorum(dErrs, xl.writeQuorum) {
		// Return errXLWriteQuorum if errors were more than allowed write quorum.
		return traceError(errXLWriteQuorum)
	}

	return nil
}

// DeleteObject - deletes an object, this call doesn't necessary reply
// any error as it is not necessary for the handler to reply back a
// response to the client request.
func (xl xlObjects) DeleteObject(bucket, object string) (err error) {
	if err = checkDelObjArgs(bucket, object); err != nil {
		return err
	}

	objectLock := nsMutex.NewNSLock(bucket, object)
	objectLock.Lock()
	defer objectLock.Unlock()

	// Validate object exists.
	if !xl.isObject(bucket, object) {
		return traceError(ObjectNotFound{bucket, object})
	} // else proceed to delete the object.

	// Delete the object on all disks.
	err = xl.deleteObject(bucket, object)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Delete from the cache.
	xl.objCache.Delete(pathJoin(bucket, object))

	// Success.
	return nil
}
