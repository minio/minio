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
	"io"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/objcache"
)

// list all errors which can be ignored in object operations.
var objectOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied)

// prepareFile hints the bottom layer to optimize the creation of a new object
func (xl xlObjects) prepareFile(bucket, object string, size int64, onlineDisks []StorageAPI, blockSize int64, dataBlocks int) error {
	pErrs := make([]error, len(onlineDisks))
	// Calculate the real size of the part in one disk.
	actualSize := xl.sizeOnDisk(size, blockSize, dataBlocks)
	// Prepare object creation in a all disks
	for index, disk := range onlineDisks {
		if disk != nil {
			if err := disk.PrepareFile(bucket, object, actualSize); err != nil {
				// Save error to reduce it later
				pErrs[index] = err
				// Ignore later access to disk which generated the error
				onlineDisks[index] = nil
			}
		}
	}
	return reduceWriteQuorumErrs(pErrs, objectOpIgnoredErrs, xl.writeQuorum)
}

/// Object Operations

// CopyObject - copy object source object to destination object.
// if source object and destination object are same we only
// update metadata.
func (xl xlObjects) CopyObject(srcBucket, srcObject, dstBucket, dstObject string, metadata map[string]string) (oi ObjectInfo, e error) {
	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllXLMetadata(xl.storageDisks, srcBucket, srcObject)
	if reducedErr := reduceReadQuorumErrs(errs, objectOpIgnoredErrs, xl.readQuorum); reducedErr != nil {
		return oi, toObjectErr(reducedErr, srcBucket, srcObject)
	}

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(xl.storageDisks, metaArr, errs)

	// Pick latest valid metadata.
	xlMeta, err := pickValidXLMeta(metaArr, modTime)
	if err != nil {
		return oi, toObjectErr(err, srcBucket, srcObject)
	}

	// Reorder online disks based on erasure distribution order.
	onlineDisks = shuffleDisks(onlineDisks, xlMeta.Erasure.Distribution)

	// Length of the file to read.
	length := xlMeta.Stat.Size

	// Check if this request is only metadata update.
	cpMetadataOnly := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))
	if cpMetadataOnly {
		xlMeta.Meta = metadata
		partsMetadata := make([]xlMetaV1, len(xl.storageDisks))
		// Update `xl.json` content on each disks.
		for index := range partsMetadata {
			partsMetadata[index] = xlMeta
		}

		tempObj := mustGetUUID()

		// Write unique `xl.json` for each disk.
		if onlineDisks, err = writeUniqueXLMetadata(onlineDisks, minioMetaTmpBucket, tempObj, partsMetadata, xl.writeQuorum); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}
		// Rename atomically `xl.json` from tmp location to destination for each disk.
		if _, err = renameXLMetadata(onlineDisks, minioMetaTmpBucket, tempObj, srcBucket, srcObject, xl.writeQuorum); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}
		return xlMeta.ToObjectInfo(srcBucket, srcObject), nil
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()

	go func() {
		var startOffset int64 // Read the whole file.
		if gerr := xl.GetObject(srcBucket, srcObject, startOffset, length, pipeWriter); gerr != nil {
			errorIf(gerr, "Unable to read %s of the object `%s/%s`.", srcBucket, srcObject)
			pipeWriter.CloseWithError(toObjectErr(gerr, srcBucket, srcObject))
			return
		}
		pipeWriter.Close() // Close writer explicitly signalling we wrote all data.
	}()

	objInfo, err := xl.PutObject(dstBucket, dstObject, NewHashReader(pipeReader, length, metadata["etag"], ""), metadata)
	if err != nil {
		return oi, toObjectErr(err, dstBucket, dstObject)
	}

	// Explicitly close the reader.
	pipeReader.Close()

	return objInfo, nil
}

// GetObject - reads an object erasured coded across multiple
// disks. Supports additional parameters like offset and length
// which are synonymous with HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (xl xlObjects) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) error {
	if err := checkGetObjArgs(bucket, object); err != nil {
		return err
	}

	// Start offset cannot be negative.
	if startOffset < 0 {
		return traceError(errUnexpected)
	}

	// Writer cannot be nil.
	if writer == nil {
		return traceError(errUnexpected)
	}

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllXLMetadata(xl.storageDisks, bucket, object)
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
	onlineDisks = shuffleDisks(onlineDisks, xlMeta.Erasure.Distribution)

	// Reorder parts metadata based on erasure distribution order.
	metaArr = shufflePartsMetadata(metaArr, xlMeta.Erasure.Distribution)

	// For negative length read everything.
	if length < 0 {
		length = xlMeta.Stat.Size - startOffset
	}

	// Reply back invalid range if the input offset and length fall out of range.
	if startOffset > xlMeta.Stat.Size || startOffset+length > xlMeta.Stat.Size {
		return traceError(InvalidRange{startOffset, length, xlMeta.Stat.Size})
	}

	// Get start part index and offset.
	partIndex, partOffset, err := xlMeta.ObjectToPartOffset(startOffset)
	if err != nil {
		return traceError(InvalidRange{startOffset, length, xlMeta.Stat.Size})
	}

	// Calculate endOffset according to length
	endOffset := startOffset
	if length > 0 {
		endOffset += length - 1
	}

	// Get last part index to read given length.
	lastPartIndex, _, err := xlMeta.ObjectToPartOffset(endOffset)
	if err != nil {
		return traceError(InvalidRange{startOffset, length, xlMeta.Stat.Size})
	}

	// Save the writer.
	mw := writer

	// Object cache enabled block.
	if xlMeta.Stat.Size > 0 && xl.objCacheEnabled {
		// Validate if we have previous cache.
		var cachedBuffer io.ReaderAt
		cachedBuffer, err = xl.objCache.Open(path.Join(bucket, object), modTime)
		if err == nil { // Cache hit
			// Create a new section reader, starting at an offset with length.
			reader := io.NewSectionReader(cachedBuffer, startOffset, length)

			// Copy the data out.
			if _, err = io.Copy(writer, reader); err != nil {
				return traceError(err)
			}

			// Success.
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

	var totalBytesRead int64
	storage, err := NewErasureStorage(onlineDisks, xlMeta.Erasure.DataBlocks, xlMeta.Erasure.ParityBlocks)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	checksums := make([][]byte, len(storage.disks))
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
		var algorithm BitrotAlgorithm
		for index, disk := range storage.disks {
			if disk == OfflineDisk {
				continue
			}
			checksumInfo := metaArr[index].Erasure.GetChecksumInfo(partName)
			algorithm = checksumInfo.Algorithm
			checksums[index] = checksumInfo.Hash
		}

		file, err := storage.ReadFile(mw, bucket, pathJoin(object, partName), partOffset, readSize, partSize, checksums, algorithm, xlMeta.Erasure.BlockSize)
		if err != nil {
			errorIf(err, "Unable to read %s of the object `%s/%s`.", partName, bucket, object)
			return toObjectErr(err, bucket, object)
		}

		// Track total bytes read from disk and written to the client.
		totalBytesRead += file.Size

		// partOffset will be valid only for the first part, hence reset it to 0 for
		// the remaining parts.
		partOffset = 0
	} // End of read all parts loop.

	// Return success.
	return nil
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (xl xlObjects) GetObjectInfo(bucket, object string) (oi ObjectInfo, e error) {
	if err := checkGetObjArgs(bucket, object); err != nil {
		return oi, err
	}

	info, err := xl.getObjectInfo(bucket, object)
	if err != nil {
		return oi, toObjectErr(err, bucket, object)
	}
	return info, nil
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (xl xlObjects) getObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	// Extracts xlStat and xlMetaMap.
	xlStat, xlMetaMap, err := xl.readXLMetaStat(bucket, object)
	if err != nil {
		return ObjectInfo{}, err
	}

	objInfo = ObjectInfo{
		IsDir:           false,
		Bucket:          bucket,
		Name:            object,
		Size:            xlStat.Size,
		ModTime:         xlStat.ModTime,
		ContentType:     xlMetaMap["content-type"],
		ContentEncoding: xlMetaMap["content-encoding"],
	}

	// Extract etag.
	objInfo.ETag = extractETag(xlMetaMap)

	// etag/md5Sum has already been extracted. We need to
	// remove to avoid it from appearing as part of
	// response headers. e.g, X-Minio-* or X-Amz-*.
	objInfo.UserDefined = cleanMetaETag(xlMetaMap)

	// Success.
	return objInfo, nil
}

func undoRename(disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, isDir bool, errs []error) {
	var wg = &sync.WaitGroup{}
	// Undo rename object on disks where RenameFile succeeded.

	// If srcEntry/dstEntry are objects then add a trailing slash to copy
	// over all the parts inside the object directory
	if isDir {
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
func rename(disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, isDir bool, quorum int) ([]StorageAPI, error) {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var errs = make([]error, len(disks))

	if isDir {
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
	err := reduceWriteQuorumErrs(errs, objectOpIgnoredErrs, quorum)
	if errorCause(err) == errXLWriteQuorum {
		// Undo all the partial rename operations.
		undoRename(disks, srcBucket, srcEntry, dstBucket, dstEntry, isDir, errs)
	}
	return evalDisks(disks, errs), err
}

// renamePart - renames a part of the source object to the destination
// across all disks in parallel. Additionally if we have errors and do
// not have a readQuorum partially renamed files are renamed back to
// its proper location.
func renamePart(disks []StorageAPI, srcBucket, srcPart, dstBucket, dstPart string, quorum int) ([]StorageAPI, error) {
	isDir := false
	return rename(disks, srcBucket, srcPart, dstBucket, dstPart, isDir, quorum)
}

// renameObject - renames all source objects to destination object
// across all disks in parallel. Additionally if we have errors and do
// not have a readQuorum partially renamed files are renamed back to
// its proper location.
func renameObject(disks []StorageAPI, srcBucket, srcObject, dstBucket, dstObject string, quorum int) ([]StorageAPI, error) {
	isDir := true
	return rename(disks, srcBucket, srcObject, dstBucket, dstObject, isDir, quorum)
}

// PutObject - creates an object upon reading from the input stream
// until EOF, erasure codes the data across all disk and additionally
// writes `xl.json` which carries the necessary metadata for future
// object operations.
func (xl xlObjects) PutObject(bucket string, object string, data *HashReader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	// This is a special case with size as '0' and object ends with
	// a slash separator, we treat it like a valid operation and
	// return success.
	if isObjectDir(object, data.Size()) {
		// Check if an object is present as one of the parent dir.
		// -- FIXME. (needs a new kind of lock).
		// -- FIXME (this also causes performance issue when disks are down).
		if xl.parentDirIsObject(bucket, path.Dir(object)) {
			return ObjectInfo{}, toObjectErr(traceError(errFileAccessDenied), bucket, object)
		}
		return dirObjectInfo(bucket, object, data.Size(), metadata), nil
	}

	// Validate put object input args.
	if err = checkPutObjectArgs(bucket, object, xl); err != nil {
		return ObjectInfo{}, err
	}

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
	// -- FIXME (this also causes performance issue when disks are down).
	if xl.parentDirIsObject(bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(traceError(errFileAccessDenied), bucket, object)
	}

	// No metadata is set, allocate a new one.
	if metadata == nil {
		metadata = make(map[string]string)
	}

	uniqueID := mustGetUUID()
	tempObj := uniqueID

	// Limit the reader to its provided size if specified.
	var reader io.Reader = data

	// Proceed to set the cache.
	var newBuffer io.WriteCloser

	// If caching is enabled, proceed to set the cache.
	if data.Size() > 0 && xl.objCacheEnabled {
		// PutObject invalidates any previously cached object in memory.
		xl.objCache.Delete(path.Join(bucket, object))

		// Create a new entry in memory of size.
		newBuffer, err = xl.objCache.Create(path.Join(bucket, object), data.Size())
		// Ignore error if cache is full, proceed to write the object.
		if err != nil && err != objcache.ErrCacheFull {
			// For any other error return here.
			return ObjectInfo{}, toObjectErr(traceError(err), bucket, object)
		}
		reader = io.TeeReader(data, newBuffer)
	}

	// Initialize parts metadata
	partsMetadata := make([]xlMetaV1, len(xl.storageDisks))

	xlMeta := newXLMetaV1(object, xl.dataBlocks, xl.parityBlocks)

	// Initialize xl meta.
	for index := range partsMetadata {
		partsMetadata[index] = xlMeta
	}

	// Order disks according to erasure distribution
	onlineDisks := shuffleDisks(xl.storageDisks, partsMetadata[0].Erasure.Distribution)

	// Delete temporary object in the event of failure.
	// If PutObject succeeded there would be no temporary
	// object to delete.
	defer xl.deleteObject(minioMetaTmpBucket, tempObj)

	// Total size of the written object
	var sizeWritten int64

	storage, err := NewErasureStorage(onlineDisks, xlMeta.Erasure.DataBlocks, xlMeta.Erasure.ParityBlocks)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	buffer := make([]byte, partsMetadata[0].Erasure.BlockSize, 2*partsMetadata[0].Erasure.BlockSize) // alloc additional space for parity blocks created while erasure coding
	// Read data and split into parts - similar to multipart mechanism
	for partIdx := 1; ; partIdx++ {
		// Compute part name
		partName := "part." + strconv.Itoa(partIdx)
		// Compute the path of current part
		tempErasureObj := pathJoin(uniqueID, partName)

		// Calculate the size of the current part, if size is unknown, curPartSize wil be unknown too.
		// allowEmptyPart will always be true if this is the first part and false otherwise.
		var curPartSize int64
		curPartSize, err = getPartSizeFromIdx(data.Size(), globalPutPartSize, partIdx)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}

		// Hint the filesystem to pre-allocate one continuous large block.
		// This is only an optimization.
		if curPartSize > 0 {
			pErr := xl.prepareFile(minioMetaTmpBucket, tempErasureObj, curPartSize, storage.disks, xlMeta.Erasure.BlockSize, xlMeta.Erasure.DataBlocks)
			if pErr != nil {
				return ObjectInfo{}, toObjectErr(pErr, bucket, object)
			}
		}

		file, erasureErr := storage.CreateFile(io.LimitReader(reader, globalPutPartSize), minioMetaTmpBucket, tempErasureObj, buffer, DefaultBitrotAlgorithm, xl.writeQuorum)
		if erasureErr != nil {
			return ObjectInfo{}, toObjectErr(erasureErr, minioMetaTmpBucket, tempErasureObj)
		}

		// Should return IncompleteBody{} error when reader has fewer bytes
		// than specified in request header.
		if file.Size < curPartSize {
			return ObjectInfo{}, traceError(IncompleteBody{})
		}

		// Update the total written size
		sizeWritten += file.Size

		// allowEmpty creating empty earsure file only when this is the first part. This flag is useful
		// when size == -1 because in this case, we are not able to predict how many parts we will have.
		allowEmpty := partIdx == 1
		if file.Size > 0 || allowEmpty {
			for i := range partsMetadata {
				partsMetadata[i].AddObjectPart(partIdx, partName, "", file.Size)
				partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{partName, file.Algorithm, file.Checksums[i]})
			}
		}

		// If we didn't write anything or we know that the next part doesn't have any
		// data to write, we should quit this loop immediately
		if file.Size == 0 {
			break
		}

		// Check part size for the next index.
		var partSize int64
		partSize, err = getPartSizeFromIdx(data.Size(), globalPutPartSize, partIdx+1)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		if partSize == 0 {
			break
		}
	}

	if size := data.Size(); size > 0 && sizeWritten < data.Size() {
		return ObjectInfo{}, traceError(IncompleteBody{})
	}

	// Save additional erasureMetadata.
	modTime := UTCNow()

	if err = data.Verify(); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	metadata["etag"] = hex.EncodeToString(data.MD5())

	// Guess content-type from the extension if possible.
	if metadata["content-type"] == "" {
		if objectExt := path.Ext(object); objectExt != "" {
			if content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]; ok {
				metadata["content-type"] = content.ContentType
			}
		}
	}

	if xl.isObject(bucket, object) {
		// Rename if an object already exists to temporary location.
		newUniqueID := mustGetUUID()

		// Delete successfully renamed object.
		defer xl.deleteObject(minioMetaTmpBucket, newUniqueID)

		// NOTE: Do not use online disks slice here.
		// The reason is that existing object should be purged
		// regardless of `xl.json` status and rolled back in case of errors.
		_, err = renameObject(xl.storageDisks, bucket, object, minioMetaTmpBucket, newUniqueID, xl.writeQuorum)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	// Fill all the necessary metadata.
	// Update `xl.json` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Meta = metadata
		partsMetadata[index].Stat.Size = sizeWritten
		partsMetadata[index].Stat.ModTime = modTime
	}

	// Write unique `xl.json` for each disk.
	if onlineDisks, err = writeUniqueXLMetadata(onlineDisks, minioMetaTmpBucket, tempObj, partsMetadata, xl.writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Rename the successfully written temporary object to final location.
	if _, err = renameObject(onlineDisks, minioMetaTmpBucket, tempObj, bucket, object, xl.writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Once we have successfully renamed the object, Close the buffer which would
	// save the object on cache.
	if sizeWritten > 0 && xl.objCacheEnabled && newBuffer != nil {
		newBuffer.Close()
	}

	// Object info is the same in all disks, so we can pick the first meta
	// of the first disk
	xlMeta = partsMetadata[0]

	objInfo = ObjectInfo{
		IsDir:           false,
		Bucket:          bucket,
		Name:            object,
		Size:            xlMeta.Stat.Size,
		ModTime:         xlMeta.Stat.ModTime,
		ETag:            xlMeta.Meta["etag"],
		ContentType:     xlMeta.Meta["content-type"],
		ContentEncoding: xlMeta.Meta["content-encoding"],
		UserDefined:     xlMeta.Meta,
	}

	// Success, return object info.
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

	return reduceWriteQuorumErrs(dErrs, objectOpIgnoredErrs, xl.writeQuorum)
}

// DeleteObject - deletes an object, this call doesn't necessary reply
// any error as it is not necessary for the handler to reply back a
// response to the client request.
func (xl xlObjects) DeleteObject(bucket, object string) (err error) {
	if err = checkDelObjArgs(bucket, object); err != nil {
		return err
	}

	// Validate object exists.
	if !xl.isObject(bucket, object) {
		return traceError(ObjectNotFound{bucket, object})
	} // else proceed to delete the object.

	// Delete the object on all disks.
	err = xl.deleteObject(bucket, object)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	if xl.objCacheEnabled {
		// Delete from the cache.
		xl.objCache.Delete(pathJoin(bucket, object))
	}

	// Success.
	return nil
}
