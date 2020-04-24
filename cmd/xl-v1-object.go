/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"sync"

	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/object/tagging"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// list all errors which can be ignored in object operations.
var objectOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied)

// putObjectDir hints the bottom layer to create a new directory.
func (xl xlObjects) putObjectDir(ctx context.Context, bucket, object string, writeQuorum int) error {
	storageDisks := xl.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))

	// Prepare object creation in all disks
	for index := range storageDisks {
		if storageDisks[index] == nil {
			continue
		}
		index := index
		g.Go(func() error {
			err := storageDisks[index].MakeVol(pathJoin(bucket, object))
			if err != nil && err != errVolumeExists {
				return err
			}
			return nil
		}, index)
	}

	return reduceWriteQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, writeQuorum)
}

/// Object Operations

// CopyObject - copy object source object to destination object.
// if source object and destination object are same we only
// update metadata.
func (xl xlObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (oi ObjectInfo, e error) {
	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))

	// Check if this request is only metadata update.
	if cpSrcDstSame {
		// Read metadata associated with the object from all disks.
		storageDisks := xl.getDisks()

		metaArr, errs := readAllXLMetadata(ctx, storageDisks, srcBucket, srcObject)

		// get Quorum for this object
		readQuorum, writeQuorum, err := objectQuorumFromMeta(ctx, xl, metaArr, errs)
		if err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		if reducedErr := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum); reducedErr != nil {
			return oi, toObjectErr(reducedErr, srcBucket, srcObject)
		}

		// List all online disks.
		_, modTime := listOnlineDisks(storageDisks, metaArr, errs)

		// Pick latest valid metadata.
		xlMeta, err := pickValidXLMeta(ctx, metaArr, modTime, readQuorum)
		if err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		// Update `xl.json` content on each disks.
		for index := range metaArr {
			metaArr[index].Meta = srcInfo.UserDefined
			metaArr[index].Meta["etag"] = srcInfo.ETag
		}

		var onlineDisks []StorageAPI

		tempObj := mustGetUUID()

		// Cleanup in case of xl.json writing failure
		defer xl.deleteObject(ctx, minioMetaTmpBucket, tempObj, writeQuorum, false)

		// Write unique `xl.json` for each disk.
		if onlineDisks, err = writeUniqueXLMetadata(ctx, storageDisks, minioMetaTmpBucket, tempObj, metaArr, writeQuorum); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		// Rename atomically `xl.json` from tmp location to destination for each disk.
		if _, err = renameXLMetadata(ctx, onlineDisks, minioMetaTmpBucket, tempObj, srcBucket, srcObject, writeQuorum); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		return xlMeta.ToObjectInfo(srcBucket, srcObject), nil
	}

	putOpts := ObjectOptions{ServerSideEncryption: dstOpts.ServerSideEncryption, UserDefined: srcInfo.UserDefined}
	return xl.PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, putOpts)
}

// GetObjectNInfo - returns object info and an object
// Read(Closer). When err != nil, the returned reader is always nil.
func (xl xlObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}

	// Handler directory request by returning a reader that
	// returns no bytes.
	if HasSuffix(object, SlashSeparator) {
		var objInfo ObjectInfo
		if objInfo, err = xl.getObjectInfoDir(ctx, bucket, object); err != nil {
			return nil, toObjectErr(err, bucket, object)
		}
		return NewGetObjectReaderFromReader(bytes.NewBuffer(nil), objInfo, opts)
	}

	var objInfo ObjectInfo
	objInfo, err = xl.getObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}

	fn, off, length, nErr := NewGetObjectReader(rs, objInfo, opts)
	if nErr != nil {
		return nil, nErr
	}

	pr, pw := io.Pipe()
	go func() {
		err := xl.getObject(ctx, bucket, object, off, length, pw, "", opts)
		pw.CloseWithError(err)
	}()

	// Cleanup function to cause the go routine above to exit, in
	// case of incomplete read.
	pipeCloser := func() { pr.Close() }

	return fn(pr, h, opts.CheckCopyPrecondFn, pipeCloser)
}

// GetObject - reads an object erasured coded across multiple
// disks. Supports additional parameters like offset and length
// which are synonymous with HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (xl xlObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) error {
	return xl.getObject(ctx, bucket, object, startOffset, length, writer, etag, opts)
}

// getObject wrapper for xl GetObject
func (xl xlObjects) getObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) error {

	if err := checkGetObjArgs(ctx, bucket, object); err != nil {
		return err
	}

	// Start offset cannot be negative.
	if startOffset < 0 {
		logger.LogIf(ctx, errUnexpected, logger.Application)
		return errUnexpected
	}

	// Writer cannot be nil.
	if writer == nil {
		logger.LogIf(ctx, errUnexpected)
		return errUnexpected
	}

	// If its a directory request, we return an empty body.
	if HasSuffix(object, SlashSeparator) {
		_, err := writer.Write([]byte(""))
		logger.LogIf(ctx, err)
		return toObjectErr(err, bucket, object)
	}

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllXLMetadata(ctx, xl.getDisks(), bucket, object)

	// get Quorum for this object
	readQuorum, _, err := objectQuorumFromMeta(ctx, xl, metaArr, errs)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	if reducedErr := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum); reducedErr != nil {
		return toObjectErr(reducedErr, bucket, object)
	}

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(xl.getDisks(), metaArr, errs)

	// Pick latest valid metadata.
	xlMeta, err := pickValidXLMeta(ctx, metaArr, modTime, readQuorum)
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
		logger.LogIf(ctx, InvalidRange{startOffset, length, xlMeta.Stat.Size}, logger.Application)
		return InvalidRange{startOffset, length, xlMeta.Stat.Size}
	}

	// Get start part index and offset.
	partIndex, partOffset, err := xlMeta.ObjectToPartOffset(ctx, startOffset)
	if err != nil {
		return InvalidRange{startOffset, length, xlMeta.Stat.Size}
	}

	// Calculate endOffset according to length
	endOffset := startOffset
	if length > 0 {
		endOffset += length - 1
	}

	// Get last part index to read given length.
	lastPartIndex, _, err := xlMeta.ObjectToPartOffset(ctx, endOffset)
	if err != nil {
		return InvalidRange{startOffset, length, xlMeta.Stat.Size}
	}

	var totalBytesRead int64
	erasure, err := NewErasure(ctx, xlMeta.Erasure.DataBlocks, xlMeta.Erasure.ParityBlocks, xlMeta.Erasure.BlockSize)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	for ; partIndex <= lastPartIndex; partIndex++ {
		if length == totalBytesRead {
			break
		}

		partNumber := xlMeta.Parts[partIndex].Number

		// Save the current part name and size.
		partSize := xlMeta.Parts[partIndex].Size

		partLength := partSize - partOffset
		// partLength should be adjusted so that we don't write more data than what was requested.
		if partLength > (length - totalBytesRead) {
			partLength = length - totalBytesRead
		}

		tillOffset := erasure.ShardFileTillOffset(partOffset, partLength, partSize)
		// Get the checksums of the current part.
		readers := make([]io.ReaderAt, len(onlineDisks))
		for index, disk := range onlineDisks {
			if disk == OfflineDisk {
				continue
			}
			checksumInfo := metaArr[index].Erasure.GetChecksumInfo(partNumber)
			partPath := pathJoin(object, fmt.Sprintf("part.%d", partNumber))
			readers[index] = newBitrotReader(disk, bucket, partPath, tillOffset,
				checksumInfo.Algorithm, checksumInfo.Hash, erasure.ShardSize())
		}
		err := erasure.Decode(ctx, writer, readers, partOffset, partLength, partSize)
		// Note: we should not be defer'ing the following closeBitrotReaders() call as we are inside a for loop i.e if we use defer, we would accumulate a lot of open files by the time
		// we return from this function.
		closeBitrotReaders(readers)
		if err != nil {
			if decodeHealErr, ok := err.(*errDecodeHealRequired); ok {
				go deepHealObject(pathJoin(bucket, object))
				err = decodeHealErr.err
			}
			if err != nil {
				return toObjectErr(err, bucket, object)
			}
		}
		for i, r := range readers {
			if r == nil {
				onlineDisks[i] = OfflineDisk
			}
		}
		// Track total bytes read from disk and written to the client.
		totalBytesRead += partLength

		// partOffset will be valid only for the first part, hence reset it to 0 for
		// the remaining parts.
		partOffset = 0
	} // End of read all parts loop.

	// Return success.
	return nil
}

// getObjectInfoDir - This getObjectInfo is specific to object directory lookup.
func (xl xlObjects) getObjectInfoDir(ctx context.Context, bucket, object string) (ObjectInfo, error) {
	storageDisks := xl.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))

	// Prepare object creation in a all disks
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		index := index
		g.Go(func() error {
			// Check if 'prefix' is an object on this 'disk'.
			entries, err := storageDisks[index].ListDir(bucket, object, 1, "")
			if err != nil {
				return err
			}
			if len(entries) > 0 {
				// Not a directory if not empty.
				return errFileNotFound
			}
			return nil
		}, index)
	}

	readQuorum := getReadQuorum(len(storageDisks))
	err := reduceReadQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, readQuorum)
	return dirObjectInfo(bucket, object, 0, map[string]string{}), err
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (xl xlObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (oi ObjectInfo, e error) {
	if err := checkGetObjArgs(ctx, bucket, object); err != nil {
		return oi, err
	}

	if HasSuffix(object, SlashSeparator) {
		info, err := xl.getObjectInfoDir(ctx, bucket, object)
		if err != nil {
			return oi, toObjectErr(err, bucket, object)
		}
		return info, nil
	}

	info, err := xl.getObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	return info, nil
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (xl xlObjects) getObjectInfo(ctx context.Context, bucket, object string, opt ObjectOptions) (objInfo ObjectInfo, err error) {
	disks := xl.getDisks()

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllXLMetadata(ctx, disks, bucket, object)

	readQuorum, _, err := objectQuorumFromMeta(ctx, xl, metaArr, errs)
	if err != nil {
		return objInfo, err
	}

	// List all the file commit ids from parts metadata.
	modTimes := listObjectModtimes(metaArr, errs)

	// Reduce list of UUIDs to a single common value.
	modTime, _ := commonTime(modTimes)

	// Pick latest valid metadata.
	xlMeta, err := pickValidXLMeta(ctx, metaArr, modTime, readQuorum)
	if err != nil {
		return objInfo, err
	}

	return xlMeta.ToObjectInfo(bucket, object), nil
}

func undoRename(disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, isDir bool, errs []error) {
	// Undo rename object on disks where RenameFile succeeded.

	// If srcEntry/dstEntry are objects then add a trailing slash to copy
	// over all the parts inside the object directory
	if isDir {
		srcEntry = retainSlash(srcEntry)
		dstEntry = retainSlash(dstEntry)
	}
	g := errgroup.WithNErrs(len(disks))
	for index, disk := range disks {
		if disk == nil {
			continue
		}
		index := index
		g.Go(func() error {
			if errs[index] == nil {
				_ = disks[index].RenameFile(dstBucket, dstEntry, srcBucket, srcEntry)
			}
			return nil
		}, index)
	}
	g.Wait()
}

// rename - common function that renamePart and renameObject use to rename
// the respective underlying storage layer representations.
func rename(ctx context.Context, disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, isDir bool, writeQuorum int, ignoredErr []error) ([]StorageAPI, error) {

	if isDir {
		dstEntry = retainSlash(dstEntry)
		srcEntry = retainSlash(srcEntry)
	}

	g := errgroup.WithNErrs(len(disks))

	// Rename file on all underlying storage disks.
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			if err := disks[index].RenameFile(srcBucket, srcEntry, dstBucket, dstEntry); err != nil {
				if !IsErrIgnored(err, ignoredErr...) {
					return err
				}
			}
			return nil
		}, index)
	}

	// Wait for all renames to finish.
	errs := g.Wait()

	// We can safely allow RenameFile errors up to len(xl.getDisks()) - writeQuorum
	// otherwise return failure. Cleanup successful renames.
	err := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if err == errXLWriteQuorum {
		// Undo all the partial rename operations.
		undoRename(disks, srcBucket, srcEntry, dstBucket, dstEntry, isDir, errs)
	}
	return evalDisks(disks, errs), err
}

// PutObject - creates an object upon reading from the input stream
// until EOF, erasure codes the data across all disk and additionally
// writes `xl.json` which carries the necessary metadata for future
// object operations.
func (xl xlObjects) PutObject(ctx context.Context, bucket string, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	// Validate put object input args.
	if err = checkPutObjectArgs(ctx, bucket, object, xl, data.Size()); err != nil {
		return ObjectInfo{}, err
	}

	return xl.putObject(ctx, bucket, object, data, opts)
}

// putObject wrapper for xl PutObject
func (xl xlObjects) putObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	data := r.Reader

	uniqueID := mustGetUUID()
	tempObj := uniqueID
	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	storageDisks := xl.getDisks()

	// Get parity and data drive count based on storage class metadata
	parityDrives := globalStorageClass.GetParityForSC(opts.UserDefined[xhttp.AmzStorageClass])
	if parityDrives == 0 {
		parityDrives = getDefaultParityBlocks(len(storageDisks))
	}
	dataDrives := len(storageDisks) - parityDrives

	// we now know the number of blocks this object needs for data and parity.
	// writeQuorum is dataBlocks + 1
	writeQuorum := dataDrives + 1

	// Delete temporary object in the event of failure.
	// If PutObject succeeded there would be no temporary
	// object to delete.
	defer xl.deleteObject(ctx, minioMetaTmpBucket, tempObj, writeQuorum, false)

	// This is a special case with size as '0' and object ends with
	// a slash separator, we treat it like a valid operation and
	// return success.
	if isObjectDir(object, data.Size()) {
		// Check if an object is present as one of the parent dir.
		// -- FIXME. (needs a new kind of lock).
		// -- FIXME (this also causes performance issue when disks are down).
		if xl.parentDirIsObject(ctx, bucket, path.Dir(object)) {
			return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
		}

		if err = xl.putObjectDir(ctx, minioMetaTmpBucket, tempObj, writeQuorum); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}

		// Rename the successfully written temporary object to final location. Ignore errFileAccessDenied
		// error because it means that the target object dir exists and we want to be close to S3 specification.
		if _, err = rename(ctx, storageDisks, minioMetaTmpBucket, tempObj, bucket, object, true, writeQuorum, []error{errFileAccessDenied}); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}

		return dirObjectInfo(bucket, object, data.Size(), opts.UserDefined), nil
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument, logger.Application)
		return ObjectInfo{}, toObjectErr(errInvalidArgument)
	}

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
	// -- FIXME (this also causes performance issue when disks are down).
	if xl.parentDirIsObject(ctx, bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
	}

	// Initialize parts metadata
	partsMetadata := make([]xlMetaV1, len(xl.getDisks()))

	xlMeta := newXLMetaV1(object, dataDrives, parityDrives)

	// Initialize xl meta.
	for index := range partsMetadata {
		partsMetadata[index] = xlMeta
	}

	// Order disks according to erasure distribution
	onlineDisks := shuffleDisks(storageDisks, xlMeta.Erasure.Distribution)

	erasure, err := NewErasure(ctx, xlMeta.Erasure.DataBlocks, xlMeta.Erasure.ParityBlocks, xlMeta.Erasure.BlockSize)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch size := data.Size(); {
	case size == 0:
		buffer = make([]byte, 1) // Allocate atleast a byte to reach EOF
	case size == -1 || size >= blockSizeV1:
		buffer = xl.bp.Get()
		defer xl.bp.Put(buffer)
	case size < blockSizeV1:
		// No need to allocate fully blockSizeV1 buffer if the incoming data is smaller.
		buffer = make([]byte, size, 2*size+int64(erasure.parityBlocks+erasure.dataBlocks-1))
	}

	if len(buffer) > int(xlMeta.Erasure.BlockSize) {
		buffer = buffer[:xlMeta.Erasure.BlockSize]
	}

	partName := "part.1"
	tempErasureObj := pathJoin(uniqueID, partName)

	writers := make([]io.Writer, len(onlineDisks))
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}
		writers[i] = newBitrotWriter(disk, minioMetaTmpBucket, tempErasureObj, erasure.ShardFileSize(data.Size()), DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	n, erasureErr := erasure.Encode(ctx, data, writers, buffer, erasure.dataBlocks+1)
	closeBitrotWriters(writers)
	if erasureErr != nil {
		return ObjectInfo{}, toObjectErr(erasureErr, minioMetaTmpBucket, tempErasureObj)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if n < data.Size() {
		logger.LogIf(ctx, IncompleteBody{}, logger.Application)
		return ObjectInfo{}, IncompleteBody{}
	}

	for i, w := range writers {
		if w == nil {
			onlineDisks[i] = nil
			continue
		}
		partsMetadata[i].AddObjectPart(1, "", n, data.ActualSize())
		partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{
			PartNumber: 1,
			Algorithm:  DefaultBitrotAlgorithm,
			Hash:       bitrotWriterSum(w),
		})
	}

	// Save additional erasureMetadata.
	modTime := UTCNow()

	opts.UserDefined["etag"] = r.MD5CurrentHexString()

	// Guess content-type from the extension if possible.
	if opts.UserDefined["content-type"] == "" {
		opts.UserDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
	}

	if xl.isObject(bucket, object) {
		// Deny if WORM is enabled
		if isWORMEnabled(bucket) {
			if _, err := xl.getObjectInfo(ctx, bucket, object, ObjectOptions{}); err == nil {
				return ObjectInfo{}, ObjectAlreadyExists{Bucket: bucket, Object: object}
			}
		}

		// Rename if an object already exists to temporary location.
		newUniqueID := mustGetUUID()

		// Delete successfully renamed object.
		defer xl.deleteObject(ctx, minioMetaTmpBucket, newUniqueID, writeQuorum, false)

		// NOTE: Do not use online disks slice here: the reason is that existing object should be purged
		// regardless of `xl.json` status and rolled back in case of errors. Also allow renaming the
		// existing object if it is not present in quorum disks so users can overwrite stale objects.
		_, err = rename(ctx, storageDisks, bucket, object, minioMetaTmpBucket, newUniqueID, true, writeQuorum, []error{errFileNotFound})
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	// Fill all the necessary metadata.
	// Update `xl.json` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Meta = opts.UserDefined
		partsMetadata[index].Stat.Size = n
		partsMetadata[index].Stat.ModTime = modTime
	}

	// Write unique `xl.json` for each disk.
	if onlineDisks, err = writeUniqueXLMetadata(ctx, onlineDisks, minioMetaTmpBucket, tempObj, partsMetadata, writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Rename the successfully written temporary object to final location.
	if onlineDisks, err = rename(ctx, onlineDisks, minioMetaTmpBucket, tempObj, bucket, object, true, writeQuorum, nil); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Whether a disk was initially or becomes offline
	// during this upload, send it to the MRF list.
	for i := 0; i < len(onlineDisks); i++ {
		if onlineDisks[i] == nil || storageDisks[i] == nil {
			xl.addPartialUpload(bucket, object)
			break
		}
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

	return objInfo, nil
}

// deleteObject - wrapper for delete object, deletes an object from
// all the disks in parallel, including `xl.json` associated with the
// object.
func (xl xlObjects) deleteObject(ctx context.Context, bucket, object string, writeQuorum int, isDir bool) error {
	var disks []StorageAPI
	var err error

	tmpObj := mustGetUUID()
	if bucket == minioMetaTmpBucket {
		tmpObj = object
		disks = xl.getDisks()
	} else {
		// Rename the current object while requiring write quorum, but also consider
		// that a non found object in a given disk as a success since it already
		// confirms that the object doesn't have a part in that disk (already removed)
		if isDir {
			disks, err = rename(ctx, xl.getDisks(), bucket, object, minioMetaTmpBucket, tmpObj, true, writeQuorum,
				[]error{errFileNotFound, errFileAccessDenied})
		} else {
			disks, err = rename(ctx, xl.getDisks(), bucket, object, minioMetaTmpBucket, tmpObj, true, writeQuorum,
				[]error{errFileNotFound})
		}
		if err != nil {
			return toObjectErr(err, bucket, object)
		}
	}

	g := errgroup.WithNErrs(len(disks))

	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			var err error
			if isDir {
				// DeleteFile() simply tries to remove a directory
				// and will succeed only if that directory is empty.
				err = disks[index].DeleteFile(minioMetaTmpBucket, tmpObj)
			} else {
				err = cleanupDir(ctx, disks[index], minioMetaTmpBucket, tmpObj)
			}
			if err != nil && err != errVolumeNotFound {
				return err
			}
			return nil
		}, index)
	}

	// return errors if any during deletion
	return reduceWriteQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, writeQuorum)
}

// deleteObject - wrapper for delete object, deletes an object from
// all the disks in parallel, including `xl.json` associated with the
// object.
func (xl xlObjects) doDeleteObjects(ctx context.Context, bucket string, objects []string, errs []error, writeQuorums []int, isDirs []bool) ([]error, error) {
	var tmpObjs = make([]string, len(objects))
	if bucket == minioMetaTmpBucket {
		copy(tmpObjs, objects)
	} else {
		for idx := range objects {
			if errs[idx] != nil {
				continue
			}
			tmpObjs[idx] = mustGetUUID()
			var err error
			// Rename the current object while requiring
			// write quorum, but also consider that a non
			// found object in a given disk as a success
			// since it already confirms that the object
			// doesn't have a part in that disk (already removed)
			if isDirs[idx] {
				_, err = rename(ctx, xl.getDisks(), bucket, objects[idx],
					minioMetaTmpBucket, tmpObjs[idx], true, writeQuorums[idx],
					[]error{errFileNotFound, errFileAccessDenied})
			} else {
				_, err = rename(ctx, xl.getDisks(), bucket, objects[idx],
					minioMetaTmpBucket, tmpObjs[idx], true, writeQuorums[idx],
					[]error{errFileNotFound})
			}
			if err != nil {
				errs[idx] = err
			}
		}
	}

	disks := xl.getDisks()

	// Initialize list of errors.
	var opErrs = make([]error, len(disks))
	var delObjErrs = make([][]error, len(disks))
	var wg = sync.WaitGroup{}

	// Remove objects in bulk for each disk
	for i, d := range disks {
		if d == nil {
			opErrs[i] = errDiskNotFound
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			delObjErrs[index], opErrs[index] = disk.DeletePrefixes(minioMetaTmpBucket, tmpObjs)
			if opErrs[index] == errVolumeNotFound || opErrs[index] == errFileNotFound {
				opErrs[index] = nil
			}
		}(i, d)
	}

	wg.Wait()

	// Return errors if any during deletion
	if err := reduceWriteQuorumErrs(ctx, opErrs, objectOpIgnoredErrs, len(disks)/2+1); err != nil {
		return nil, err
	}

	// Reduce errors for each object
	for objIndex := range objects {
		if errs[objIndex] != nil {
			continue
		}
		listErrs := make([]error, len(disks))
		// Iterate over disks to fetch the error
		// of deleting of the current object
		for i := range delObjErrs {
			// delObjErrs[i] is not nil when disks[i] is also not nil
			if delObjErrs[i] != nil {
				if delObjErrs[i][objIndex] != errFileNotFound {
					listErrs[i] = delObjErrs[i][objIndex]
				}
			}
		}
		errs[objIndex] = reduceWriteQuorumErrs(ctx, listErrs, objectOpIgnoredErrs, writeQuorums[objIndex])
	}

	return errs, nil
}

func (xl xlObjects) deleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	errs := make([]error, len(objects))
	writeQuorums := make([]int, len(objects))
	isObjectDirs := make([]bool, len(objects))

	for i, object := range objects {
		errs[i] = checkDelObjArgs(ctx, bucket, object)
	}

	for i, object := range objects {
		isObjectDirs[i] = HasSuffix(object, SlashSeparator)
	}

	storageDisks := xl.getDisks()

	for i, object := range objects {
		if isObjectDirs[i] {
			_, err := xl.getObjectInfoDir(ctx, bucket, object)
			if err == errXLReadQuorum {
				if isObjectDirDangling(statAllDirs(ctx, storageDisks, bucket, object)) {
					// If object is indeed dangling, purge it.
					errs[i] = nil
				}
			}
			if err != nil {
				errs[i] = toObjectErr(err, bucket, object)
				continue
			}
		}
	}

	for i := range objects {
		if errs[i] != nil {
			continue
		}
		// Assume (N/2 + 1) quorums for all objects
		// this is a theoretical assumption such that
		// for delete's we do not need to honor storage
		// class for objects which have reduced quorum
		// storage class only needs to be honored for
		// Read() requests alone which we already do.
		writeQuorums[i] = getWriteQuorum(len(storageDisks))
	}

	return xl.doDeleteObjects(ctx, bucket, objects, errs, writeQuorums, isObjectDirs)
}

// DeleteObjects deletes objects in bulk, this function will still automatically split objects list
// into smaller bulks if some object names are found to be duplicated in the delete list, splitting
// into smaller bulks will avoid holding twice the write lock of the duplicated object names.
func (xl xlObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {

	var (
		i, start, end int
		// Deletion result for all objects
		deleteErrs []error
		// Object names store will be used to check for object name duplication
		objectNamesStore = make(map[string]interface{})
	)

	for {
		if i >= len(objects) {
			break
		}

		object := objects[i]

		_, duplicationFound := objectNamesStore[object]
		if duplicationFound {
			end = i - 1
		} else {
			objectNamesStore[object] = true
			end = i
		}

		if duplicationFound || i == len(objects)-1 {
			errs, err := xl.deleteObjects(ctx, bucket, objects[start:end+1])
			if err != nil {
				return nil, err
			}
			deleteErrs = append(deleteErrs, errs...)
			objectNamesStore = make(map[string]interface{})
		}

		if duplicationFound {
			// Avoid to increase the index if object
			// name is found to be duplicated.
			start = i
		} else {
			i++
		}
	}

	return deleteErrs, nil
}

// DeleteObject - deletes an object, this call doesn't necessary reply
// any error as it is not necessary for the handler to reply back a
// response to the client request.
func (xl xlObjects) DeleteObject(ctx context.Context, bucket, object string) (err error) {
	if err = checkDelObjArgs(ctx, bucket, object); err != nil {
		return err
	}

	var writeQuorum int
	var isObjectDir = HasSuffix(object, SlashSeparator)

	storageDisks := xl.getDisks()

	if isObjectDir {
		_, err = xl.getObjectInfoDir(ctx, bucket, object)
		if err == errXLReadQuorum {
			if isObjectDirDangling(statAllDirs(ctx, storageDisks, bucket, object)) {
				// If object is indeed dangling, purge it.
				err = nil
			}
		}
		if err != nil {
			return toObjectErr(err, bucket, object)
		}
	}

	if isObjectDir {
		writeQuorum = getWriteQuorum(len(storageDisks))
	} else {
		// Read metadata associated with the object from all disks.
		partsMetadata, errs := readAllXLMetadata(ctx, storageDisks, bucket, object)
		// get Quorum for this object
		_, writeQuorum, err = objectQuorumFromMeta(ctx, xl, partsMetadata, errs)
		if err != nil {
			return toObjectErr(err, bucket, object)
		}
	}

	// Delete the object on all disks.
	if err = xl.deleteObject(ctx, bucket, object, writeQuorum, isObjectDir); err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Success.
	return nil
}

// ListObjectsV2 lists all blobs in bucket filtered by prefix
func (xl xlObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	loi, err := xl.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
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

// Send the successful but partial upload, however ignore
// if the channel is blocked by other items.
func (xl xlObjects) addPartialUpload(bucket, key string) {
	select {
	case xl.mrfUploadCh <- partialUpload{bucket: bucket, object: key}:
	default:
	}
}

// PutObjectTag - replace or add tags to an existing object
func (xl xlObjects) PutObjectTag(ctx context.Context, bucket, object string, tags string) error {
	disks := xl.getDisks()

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllXLMetadata(ctx, disks, bucket, object)

	_, writeQuorum, err := objectQuorumFromMeta(ctx, xl, metaArr, errs)
	if err != nil {
		return err
	}

	for i, xlMeta := range metaArr {
		// clean xlMeta.Meta of tag key, before updating the new tags
		delete(xlMeta.Meta, xhttp.AmzObjectTagging)
		// Don't update for empty tags
		if tags != "" {
			xlMeta.Meta[xhttp.AmzObjectTagging] = tags
		}
		metaArr[i].Meta = xlMeta.Meta
	}

	tempObj := mustGetUUID()

	// Write unique `xl.json` for each disk.
	if disks, err = writeUniqueXLMetadata(ctx, disks, minioMetaTmpBucket, tempObj, metaArr, writeQuorum); err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Atomically rename `xl.json` from tmp location to destination for each disk.
	if _, err = renameXLMetadata(ctx, disks, minioMetaTmpBucket, tempObj, bucket, object, writeQuorum); err != nil {
		return toObjectErr(err, bucket, object)
	}

	return nil
}

// DeleteObjectTag - delete object tags from an existing object
func (xl xlObjects) DeleteObjectTag(ctx context.Context, bucket, object string) error {
	return xl.PutObjectTag(ctx, bucket, object, "")
}

// GetObjectTag - get object tags from an existing object
func (xl xlObjects) GetObjectTag(ctx context.Context, bucket, object string) (tagging.Tagging, error) {
	// GetObjectInfo will return tag value as well
	oi, err := xl.GetObjectInfo(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		return tagging.Tagging{}, err
	}

	tags, err := tagging.FromString(oi.UserTags)
	if err != nil {
		return tagging.Tagging{}, err
	}
	return tags, nil
}
