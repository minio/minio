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

	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/mimedb"
)

// list all errors which can be ignored in object operations.
var objectOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied)

// putObjectDir hints the bottom layer to create a new directory.
func (xl xlObjects) putObjectDir(bucket, object string, writeQuorum int) error {
	var wg = &sync.WaitGroup{}

	errs := make([]error, len(xl.storageDisks))
	// Prepare object creation in all disks
	for index, disk := range xl.storageDisks {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			if err := disk.MakeVol(pathJoin(bucket, object)); err != nil && err != errVolumeExists {
				errs[index] = err
			}
		}(index, disk)
	}
	wg.Wait()

	return reduceWriteQuorumErrs(errs, objectOpIgnoredErrs, writeQuorum)
}

// prepareFile hints the bottom layer to optimize the creation of a new object
func (xl xlObjects) prepareFile(bucket, object string, size int64, onlineDisks []StorageAPI, blockSize int64, dataBlocks, writeQuorum int) error {
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
	return reduceWriteQuorumErrs(pErrs, objectOpIgnoredErrs, writeQuorum)
}

/// Object Operations

// CopyObject - copy object source object to destination object.
// if source object and destination object are same we only
// update metadata.
func (xl xlObjects) CopyObject(srcBucket, srcObject, dstBucket, dstObject string, metadata map[string]string, srcEtag string) (oi ObjectInfo, e error) {
	cpSrcDstSame := srcBucket == dstBucket && srcObject == dstObject
	// Hold write lock on destination since in both cases
	// - if source and destination are same
	// - if source and destination are different
	// it is the sole mutating state.
	objectDWLock := xl.nsMutex.NewNSLock(dstBucket, dstObject)
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
		objectSRLock := xl.nsMutex.NewNSLock(srcBucket, srcObject)
		if err := objectSRLock.GetRLock(globalObjectTimeout); err != nil {
			return oi, err
		}
		defer objectSRLock.RUnlock()
	}

	if srcEtag != "" {
		objInfo, perr := xl.getObjectInfo(srcBucket, srcObject)
		if perr != nil {
			return oi, toObjectErr(perr, srcBucket, srcObject)
		}
		if objInfo.ETag != srcEtag {
			return oi, toObjectErr(errors.Trace(InvalidETag{}), srcBucket, srcObject)
		}
	}

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllXLMetadata(xl.storageDisks, srcBucket, srcObject)

	// get Quorum for this object
	readQuorum, writeQuorum, err := objectQuorumFromMeta(xl, metaArr, errs)
	if err != nil {
		return oi, toObjectErr(err, srcBucket, srcObject)
	}

	if reducedErr := reduceReadQuorumErrs(errs, objectOpIgnoredErrs, readQuorum); reducedErr != nil {
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
		if onlineDisks, err = writeUniqueXLMetadata(onlineDisks, minioMetaTmpBucket, tempObj, partsMetadata, writeQuorum); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}
		// Rename atomically `xl.json` from tmp location to destination for each disk.
		if _, err = renameXLMetadata(onlineDisks, minioMetaTmpBucket, tempObj, srcBucket, srcObject, writeQuorum); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}
		return xlMeta.ToObjectInfo(srcBucket, srcObject), nil
	}

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()

	go func() {
		var startOffset int64 // Read the whole file.
		if gerr := xl.getObject(srcBucket, srcObject, startOffset, length, pipeWriter, ""); gerr != nil {
			errorIf(gerr, "Unable to read %s of the object `%s/%s`.", srcBucket, srcObject)
			pipeWriter.CloseWithError(toObjectErr(gerr, srcBucket, srcObject))
			return
		}
		pipeWriter.Close() // Close writer explicitly signalling we wrote all data.
	}()

	hashReader, err := hash.NewReader(pipeReader, length, "", "")
	if err != nil {
		return oi, toObjectErr(errors.Trace(err), dstBucket, dstObject)
	}

	objInfo, err := xl.putObject(dstBucket, dstObject, hashReader, metadata)
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
func (xl xlObjects) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) error {
	// Lock the object before reading.
	objectLock := xl.nsMutex.NewNSLock(bucket, object)
	if err := objectLock.GetRLock(globalObjectTimeout); err != nil {
		return err
	}
	defer objectLock.RUnlock()
	return xl.getObject(bucket, object, startOffset, length, writer, etag)
}

// getObject wrapper for xl GetObject
func (xl xlObjects) getObject(bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) error {

	if err := checkGetObjArgs(bucket, object); err != nil {
		return err
	}

	// Start offset cannot be negative.
	if startOffset < 0 {
		return errors.Trace(errUnexpected)
	}

	// Writer cannot be nil.
	if writer == nil {
		return errors.Trace(errUnexpected)
	}

	// If its a directory request, we return an empty body.
	if hasSuffix(object, slashSeparator) {
		_, err := writer.Write([]byte(""))
		return toObjectErr(errors.Trace(err), bucket, object)
	}

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllXLMetadata(xl.storageDisks, bucket, object)

	// get Quorum for this object
	readQuorum, _, err := objectQuorumFromMeta(xl, metaArr, errs)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	if reducedErr := reduceReadQuorumErrs(errs, objectOpIgnoredErrs, readQuorum); reducedErr != nil {
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
		return errors.Trace(InvalidRange{startOffset, length, xlMeta.Stat.Size})
	}

	// Get start part index and offset.
	partIndex, partOffset, err := xlMeta.ObjectToPartOffset(startOffset)
	if err != nil {
		return errors.Trace(InvalidRange{startOffset, length, xlMeta.Stat.Size})
	}

	// Calculate endOffset according to length
	endOffset := startOffset
	if length > 0 {
		endOffset += length - 1
	}

	// Get last part index to read given length.
	lastPartIndex, _, err := xlMeta.ObjectToPartOffset(endOffset)
	if err != nil {
		return errors.Trace(InvalidRange{startOffset, length, xlMeta.Stat.Size})
	}

	var totalBytesRead int64
	storage, err := NewErasureStorage(onlineDisks, xlMeta.Erasure.DataBlocks, xlMeta.Erasure.ParityBlocks, xlMeta.Erasure.BlockSize)
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

		file, err := storage.ReadFile(writer, bucket, pathJoin(object, partName), partOffset, readSize, partSize, checksums, algorithm, xlMeta.Erasure.BlockSize)
		if err != nil {
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

// getObjectInfoDir - This getObjectInfo is specific to object directory lookup.
func (xl xlObjects) getObjectInfoDir(bucket, object string) (oi ObjectInfo, err error) {
	var wg = &sync.WaitGroup{}

	errs := make([]error, len(xl.storageDisks))
	// Prepare object creation in a all disks
	for index, disk := range xl.storageDisks {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			if _, err := disk.StatVol(pathJoin(bucket, object)); err != nil {
				// Since we are re-purposing StatVol, an object which
				// is a directory if it doesn't exist should be
				// returned as errFileNotFound instead, convert
				// the error right here accordingly.
				if err == errVolumeNotFound {
					err = errFileNotFound
				} else if err == errVolumeAccessDenied {
					err = errFileAccessDenied
				}

				// Save error to reduce it later
				errs[index] = err
			}
		}(index, disk)
	}

	wg.Wait()

	readQuorum := len(xl.storageDisks) / 2
	return dirObjectInfo(bucket, object, 0, map[string]string{}), reduceReadQuorumErrs(errs, objectOpIgnoredErrs, readQuorum)
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (xl xlObjects) GetObjectInfo(bucket, object string) (oi ObjectInfo, e error) {
	// Lock the object before reading.
	objectLock := xl.nsMutex.NewNSLock(bucket, object)
	if err := objectLock.GetRLock(globalObjectTimeout); err != nil {
		return oi, err
	}
	defer objectLock.RUnlock()

	if err := checkGetObjArgs(bucket, object); err != nil {
		return oi, err
	}

	if hasSuffix(object, slashSeparator) {
		if oi, e = xl.getObjectInfoDir(bucket, object); e != nil {
			return oi, toObjectErr(e, bucket, object)
		}
		return oi, nil
	}

	info, err := xl.getObjectInfo(bucket, object)
	if err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	return info, nil
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (xl xlObjects) getObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllXLMetadata(xl.storageDisks, bucket, object)

	// get Quorum for this object
	readQuorum, _, err := objectQuorumFromMeta(xl, metaArr, errs)
	if err != nil {
		return objInfo, toObjectErr(err, bucket, object)
	}

	if reducedErr := reduceReadQuorumErrs(errs, objectOpIgnoredErrs, readQuorum); reducedErr != nil {
		return objInfo, toObjectErr(reducedErr, bucket, object)
	}

	// List all the file commit ids from parts metadata.
	modTimes := listObjectModtimes(metaArr, errs)

	// Reduce list of UUIDs to a single common value.
	modTime, _ := commonTime(modTimes)

	// Pick latest valid metadata.
	xlMeta, err := pickValidXLMeta(metaArr, modTime)
	if err != nil {
		return objInfo, err
	}

	objInfo = ObjectInfo{
		IsDir:           false,
		Bucket:          bucket,
		Name:            object,
		Size:            xlMeta.Stat.Size,
		ModTime:         xlMeta.Stat.ModTime,
		ContentType:     xlMeta.Meta["content-type"],
		ContentEncoding: xlMeta.Meta["content-encoding"],
	}

	// Extract etag.
	objInfo.ETag = extractETag(xlMeta.Meta)

	// etag/md5Sum has already been extracted. We need to
	// remove to avoid it from appearing as part of
	// response headers. e.g, X-Minio-* or X-Amz-*.
	objInfo.UserDefined = cleanMetadata(xlMeta.Meta)

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
func rename(disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, isDir bool, writeQuorum int) ([]StorageAPI, error) {
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
				errs[index] = errors.Trace(err)
			}
		}(index, disk)
	}

	// Wait for all renames to finish.
	wg.Wait()

	// We can safely allow RenameFile errors up to len(xl.storageDisks) - writeQuorum
	// otherwise return failure. Cleanup successful renames.
	err := reduceWriteQuorumErrs(errs, objectOpIgnoredErrs, writeQuorum)
	if errors.Cause(err) == errXLWriteQuorum {
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
func (xl xlObjects) PutObject(bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	// Validate put object input args.
	if err = checkPutObjectArgs(bucket, object, xl, data.Size()); err != nil {
		return ObjectInfo{}, err
	}
	// Lock the object.
	objectLock := xl.nsMutex.NewNSLock(bucket, object)
	if err := objectLock.GetLock(globalObjectTimeout); err != nil {
		return objInfo, err
	}
	defer objectLock.Unlock()
	return xl.putObject(bucket, object, data, metadata)
}

// putObject wrapper for xl PutObject
func (xl xlObjects) putObject(bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	uniqueID := mustGetUUID()
	tempObj := uniqueID

	// No metadata is set, allocate a new one.
	if metadata == nil {
		metadata = make(map[string]string)
	}

	// Get parity and data drive count based on storage class metadata
	dataDrives, parityDrives := getRedundancyCount(metadata[amzStorageClass], len(xl.storageDisks))

	// we now know the number of blocks this object needs for data and parity.
	// writeQuorum is dataBlocks + 1
	writeQuorum := dataDrives + 1

	// Delete temporary object in the event of failure.
	// If PutObject succeeded there would be no temporary
	// object to delete.
	defer xl.deleteObject(minioMetaTmpBucket, tempObj)

	// This is a special case with size as '0' and object ends with
	// a slash separator, we treat it like a valid operation and
	// return success.
	if isObjectDir(object, data.Size()) {
		// Check if an object is present as one of the parent dir.
		// -- FIXME. (needs a new kind of lock).
		// -- FIXME (this also causes performance issue when disks are down).
		if xl.parentDirIsObject(bucket, path.Dir(object)) {
			return ObjectInfo{}, toObjectErr(errors.Trace(errFileAccessDenied), bucket, object)
		}

		if err = xl.putObjectDir(minioMetaTmpBucket, tempObj, writeQuorum); err != nil {
			return ObjectInfo{}, toObjectErr(errors.Trace(err), bucket, object)
		}

		// Rename the successfully written temporary object to final location.
		if _, err = renameObject(xl.storageDisks, minioMetaTmpBucket, tempObj, bucket, object, writeQuorum); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}

		return dirObjectInfo(bucket, object, data.Size(), metadata), nil
	}

	// Validate put object input args.
	if err = checkPutObjectArgs(bucket, object, xl, data.Size()); err != nil {
		return ObjectInfo{}, err
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < 0 {
		return ObjectInfo{}, toObjectErr(errors.Trace(errInvalidArgument))
	}

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
	// -- FIXME (this also causes performance issue when disks are down).
	if xl.parentDirIsObject(bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(errors.Trace(errFileAccessDenied), bucket, object)
	}

	// Limit the reader to its provided size if specified.
	var reader io.Reader = data

	// Initialize parts metadata
	partsMetadata := make([]xlMetaV1, len(xl.storageDisks))

	xlMeta := newXLMetaV1(object, dataDrives, parityDrives)

	// Initialize xl meta.
	for index := range partsMetadata {
		partsMetadata[index] = xlMeta
	}

	// Order disks according to erasure distribution
	onlineDisks := shuffleDisks(xl.storageDisks, partsMetadata[0].Erasure.Distribution)

	// Total size of the written object
	var sizeWritten int64

	storage, err := NewErasureStorage(onlineDisks, xlMeta.Erasure.DataBlocks, xlMeta.Erasure.ParityBlocks, xlMeta.Erasure.BlockSize)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Alloc additional space for parity blocks created while erasure coding
	buffer := make([]byte, xlMeta.Erasure.BlockSize, 2*xlMeta.Erasure.BlockSize)

	// Read data and split into parts - similar to multipart mechanism
	for partIdx := 1; ; partIdx++ {
		// Compute part name
		partName := "part." + strconv.Itoa(partIdx)
		// Compute the path of current part
		tempErasureObj := pathJoin(uniqueID, partName)

		// Calculate the size of the current part.
		var curPartSize int64
		curPartSize, err = calculatePartSizeFromIdx(data.Size(), globalPutPartSize, partIdx)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}

		// Hint the filesystem to pre-allocate one continuous large block.
		// This is only an optimization.
		var curPartReader io.Reader
		if curPartSize > 0 {
			pErr := xl.prepareFile(minioMetaTmpBucket, tempErasureObj, curPartSize, storage.disks, xlMeta.Erasure.BlockSize, xlMeta.Erasure.DataBlocks, writeQuorum)
			if pErr != nil {
				return ObjectInfo{}, toObjectErr(pErr, bucket, object)
			}
		}

		if curPartSize < data.Size() {
			curPartReader = io.LimitReader(reader, curPartSize)
		} else {
			curPartReader = reader
		}

		file, erasureErr := storage.CreateFile(curPartReader, minioMetaTmpBucket,
			tempErasureObj, buffer, DefaultBitrotAlgorithm, writeQuorum)
		if erasureErr != nil {
			return ObjectInfo{}, toObjectErr(erasureErr, minioMetaTmpBucket, tempErasureObj)
		}

		// Should return IncompleteBody{} error when reader has fewer bytes
		// than specified in request header.
		if file.Size < curPartSize {
			return ObjectInfo{}, errors.Trace(IncompleteBody{})
		}

		// Update the total written size
		sizeWritten += file.Size

		for i := range partsMetadata {
			partsMetadata[i].AddObjectPart(partIdx, partName, "", file.Size)
			partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{partName, file.Algorithm, file.Checksums[i]})
		}

		// We wrote everything, break out.
		if sizeWritten == data.Size() {
			break
		}
	}

	// Save additional erasureMetadata.
	modTime := UTCNow()
	metadata["etag"] = hex.EncodeToString(data.MD5Current())

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
		_, err = renameObject(xl.storageDisks, bucket, object, minioMetaTmpBucket, newUniqueID, writeQuorum)
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
	if onlineDisks, err = writeUniqueXLMetadata(onlineDisks, minioMetaTmpBucket, tempObj, partsMetadata, writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Rename the successfully written temporary object to final location.
	if _, err = renameObject(onlineDisks, minioMetaTmpBucket, tempObj, bucket, object, writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
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

	var writeQuorum int
	var err error
	// If its a directory request, no need to read metadata.
	if !hasSuffix(object, slashSeparator) {
		// Read metadata associated with the object from all disks.
		metaArr, errs := readAllXLMetadata(xl.storageDisks, bucket, object)

		// get Quorum for this object
		_, writeQuorum, err = objectQuorumFromMeta(xl, metaArr, errs)
		if err != nil {
			return err
		}
	} else {
		writeQuorum = len(xl.storageDisks)/2 + 1
	}

	// Initialize list of errors.
	var dErrs = make([]error, len(xl.storageDisks))

	for index, disk := range xl.storageDisks {
		if disk == nil {
			dErrs[index] = errors.Trace(errDiskNotFound)
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := cleanupDir(disk, bucket, object)
			if err != nil && errors.Cause(err) != errVolumeNotFound {
				dErrs[index] = err
			}
		}(index, disk)
	}

	// Wait for all routines to finish.
	wg.Wait()

	return reduceWriteQuorumErrs(dErrs, objectOpIgnoredErrs, writeQuorum)
}

// DeleteObject - deletes an object, this call doesn't necessary reply
// any error as it is not necessary for the handler to reply back a
// response to the client request.
func (xl xlObjects) DeleteObject(bucket, object string) (err error) {
	// Acquire a write lock before deleting the object.
	objectLock := xl.nsMutex.NewNSLock(bucket, object)
	if perr := objectLock.GetLock(globalOperationTimeout); perr != nil {
		return perr
	}
	defer objectLock.Unlock()

	if err = checkDelObjArgs(bucket, object); err != nil {
		return err
	}

	if hasSuffix(object, slashSeparator) {
		// Delete the object on all disks.
		if err = xl.deleteObject(bucket, object); err != nil {
			return toObjectErr(err, bucket, object)
		}
	}

	// Validate object exists.
	if !xl.isObject(bucket, object) {
		return errors.Trace(ObjectNotFound{bucket, object})
	} // else proceed to delete the object.

	// Delete the object on all disks.
	if err = xl.deleteObject(bucket, object); err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Success.
	return nil
}

// ListObjectsV2 lists all blobs in bucket filtered by prefix
func (xl xlObjects) ListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	loi, err := xl.ListObjects(bucket, prefix, continuationToken, delimiter, maxKeys)
	if err != nil {
		return result, err
	}

	listObjectsV2Info := ListObjectsV2Info{
		IsTruncated:           loi.IsTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: loi.NextMarker,
		Objects:               loi.Objects,
		Prefixes:              loi.Prefixes,
	}
	return listObjectsV2Info, err
}
