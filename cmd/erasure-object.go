/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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

	"github.com/minio/minio-go/v6/pkg/tags"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// list all errors which can be ignored in object operations.
var objectOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied)

// putObjectDir hints the bottom layer to create a new directory.
func (er erasureObjects) putObjectDir(ctx context.Context, bucket, object string, writeQuorum int) error {
	storageDisks := er.getDisks()

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
func (er erasureObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (oi ObjectInfo, e error) {
	// This call shouldn't be used for anything other than metadata updates.
	if !srcInfo.metadataOnly {
		return oi, NotImplemented{}
	}

	defer ObjectPathUpdated(path.Join(dstBucket, dstObject))

	// Read metadata associated with the object from all disks.
	storageDisks := er.getDisks()

	metaArr, errs := readAllFileInfo(ctx, storageDisks, srcBucket, srcObject, srcOpts.VersionID)

	// get Quorum for this object
	readQuorum, writeQuorum, err := objectQuorumFromMeta(ctx, er, metaArr, errs)
	if err != nil {
		return oi, toObjectErr(err, srcBucket, srcObject)
	}

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(storageDisks, metaArr, errs)

	// Pick latest valid metadata.
	fi, err := pickValidFileInfo(ctx, metaArr, modTime, readQuorum)
	if err != nil {
		return oi, toObjectErr(err, srcBucket, srcObject)
	}

	if fi.Deleted {
		if srcOpts.VersionID == "" {
			return oi, toObjectErr(errFileNotFound, srcBucket, srcObject)
		}
		return fi.ToObjectInfo(srcBucket, srcObject), toObjectErr(errMethodNotAllowed, srcBucket, srcObject)
	}

	// Update `xl.meta` content on each disks.
	for index := range metaArr {
		metaArr[index].Metadata = srcInfo.UserDefined
		metaArr[index].Metadata["etag"] = srcInfo.ETag
	}

	tempObj := mustGetUUID()

	// Cleanup in case of xl.meta writing failure
	defer er.deleteObject(ctx, minioMetaTmpBucket, tempObj, writeQuorum)

	// Write unique `xl.meta` for each disk.
	if onlineDisks, err = writeUniqueFileInfo(ctx, onlineDisks, minioMetaTmpBucket, tempObj, metaArr, writeQuorum); err != nil {
		return oi, toObjectErr(err, srcBucket, srcObject)
	}

	// Rename atomically `xl.meta` from tmp location to destination for each disk.
	if _, err = renameFileInfo(ctx, onlineDisks, minioMetaTmpBucket, tempObj, srcBucket, srcObject, writeQuorum); err != nil {
		return oi, toObjectErr(err, srcBucket, srcObject)
	}

	return fi.ToObjectInfo(srcBucket, srcObject), nil
}

// GetObjectNInfo - returns object info and an object
// Read(Closer). When err != nil, the returned reader is always nil.
func (er erasureObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}

	// Handler directory request by returning a reader that
	// returns no bytes.
	if HasSuffix(object, SlashSeparator) {
		var objInfo ObjectInfo
		if objInfo, err = er.getObjectInfoDir(ctx, bucket, object); err != nil {
			return nil, toObjectErr(err, bucket, object)
		}
		return NewGetObjectReaderFromReader(bytes.NewBuffer(nil), objInfo, opts)
	}

	fi, metaArr, onlineDisks, err := er.getObjectFileInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}

	fn, off, length, nErr := NewGetObjectReader(rs, fi.ToObjectInfo(bucket, object), opts)
	if nErr != nil {
		return nil, nErr
	}

	pr, pw := io.Pipe()
	go func() {
		err := er.getObjectWithFileInfo(ctx, bucket, object, off, length, pw, "", opts, fi, metaArr, onlineDisks)
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
func (er erasureObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) error {
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

	return er.getObject(ctx, bucket, object, startOffset, length, writer, etag, opts)
}

func (er erasureObjects) getObjectWithFileInfo(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions, fi FileInfo, metaArr []FileInfo, onlineDisks []StorageAPI) error {
	// Reorder online disks based on erasure distribution order.
	onlineDisks = shuffleDisks(onlineDisks, fi.Erasure.Distribution)

	// Reorder parts metadata based on erasure distribution order.
	metaArr = shufflePartsMetadata(metaArr, fi.Erasure.Distribution)

	// For negative length read everything.
	if length < 0 {
		length = fi.Size - startOffset
	}

	// Reply back invalid range if the input offset and length fall out of range.
	if startOffset > fi.Size || startOffset+length > fi.Size {
		logger.LogIf(ctx, InvalidRange{startOffset, length, fi.Size}, logger.Application)
		return InvalidRange{startOffset, length, fi.Size}
	}

	// Get start part index and offset.
	partIndex, partOffset, err := fi.ObjectToPartOffset(ctx, startOffset)
	if err != nil {
		return InvalidRange{startOffset, length, fi.Size}
	}

	// Calculate endOffset according to length
	endOffset := startOffset
	if length > 0 {
		endOffset += length - 1
	}

	// Get last part index to read given length.
	lastPartIndex, _, err := fi.ObjectToPartOffset(ctx, endOffset)
	if err != nil {
		return InvalidRange{startOffset, length, fi.Size}
	}

	var totalBytesRead int64
	erasure, err := NewErasure(ctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	var healOnce sync.Once
	for ; partIndex <= lastPartIndex; partIndex++ {
		if length == totalBytesRead {
			break
		}

		partNumber := fi.Parts[partIndex].Number

		// Save the current part name and size.
		partSize := fi.Parts[partIndex].Size

		partLength := partSize - partOffset
		// partLength should be adjusted so that we don't write more data than what was requested.
		if partLength > (length - totalBytesRead) {
			partLength = length - totalBytesRead
		}

		tillOffset := erasure.ShardFileOffset(partOffset, partLength, partSize)
		// Get the checksums of the current part.
		readers := make([]io.ReaderAt, len(onlineDisks))
		prefer := make([]bool, len(onlineDisks))
		for index, disk := range onlineDisks {
			if disk == OfflineDisk {
				continue
			}
			checksumInfo := metaArr[index].Erasure.GetChecksumInfo(partNumber)
			partPath := pathJoin(object, metaArr[index].DataDir, fmt.Sprintf("part.%d", partNumber))
			readers[index] = newBitrotReader(disk, bucket, partPath, tillOffset,
				checksumInfo.Algorithm, checksumInfo.Hash, erasure.ShardSize())

			// Prefer local disks
			prefer[index] = disk.Hostname() == ""
		}
		err = erasure.Decode(ctx, writer, readers, partOffset, partLength, partSize, prefer)
		// Note: we should not be defer'ing the following closeBitrotReaders() call as
		// we are inside a for loop i.e if we use defer, we would accumulate a lot of open files by the time
		// we return from this function.
		closeBitrotReaders(readers)
		if err != nil {
			if decodeHealErr, ok := err.(*errDecodeHealRequired); ok {
				healOnce.Do(func() {
					go deepHealObject(bucket, object, fi.VersionID)
				})
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

// getObject wrapper for erasure GetObject
func (er erasureObjects) getObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) error {
	fi, metaArr, onlineDisks, err := er.getObjectFileInfo(ctx, bucket, object, opts)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	return er.getObjectWithFileInfo(ctx, bucket, object, startOffset, length, writer, etag, opts, fi, metaArr, onlineDisks)
}

// getObjectInfoDir - This getObjectInfo is specific to object directory lookup.
func (er erasureObjects) getObjectInfoDir(ctx context.Context, bucket, object string) (ObjectInfo, error) {
	storageDisks := er.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))

	// Prepare object creation in a all disks
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		index := index
		g.Go(func() error {
			// Check if 'prefix' is an object on this 'disk'.
			entries, err := storageDisks[index].ListDir(bucket, object, 1)
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
func (er erasureObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (info ObjectInfo, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return info, err
	}

	if HasSuffix(object, SlashSeparator) {
		info, err = er.getObjectInfoDir(ctx, bucket, object)
		if err != nil {
			return info, toObjectErr(err, bucket, object)
		}
		return info, nil
	}

	info, err = er.getObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return info, toObjectErr(err, bucket, object)
	}

	return info, nil
}

func (er erasureObjects) getObjectFileInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (fi FileInfo, metaArr []FileInfo, onlineDisks []StorageAPI, err error) {
	disks := er.getDisks()

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllFileInfo(ctx, disks, bucket, object, opts.VersionID)

	readQuorum, _, err := objectQuorumFromMeta(ctx, er, metaArr, errs)
	if err != nil {
		return fi, nil, nil, err
	}

	if reducedErr := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum); reducedErr != nil {
		return fi, nil, nil, toObjectErr(reducedErr, bucket, object)
	}

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(disks, metaArr, errs)

	// Pick latest valid metadata.
	fi, err = pickValidFileInfo(ctx, metaArr, modTime, readQuorum)
	if err != nil {
		return fi, nil, nil, err
	}

	return fi, metaArr, onlineDisks, nil
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (er erasureObjects) getObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	fi, _, _, err := er.getObjectFileInfo(ctx, bucket, object, opts)
	if err != nil {
		return objInfo, err
	}

	if fi.Deleted {
		objInfo = fi.ToObjectInfo(bucket, object)
		if opts.VersionID == "" {
			return objInfo, toObjectErr(errFileNotFound, bucket, object)
		}
		// Make sure to return object info to provide extra information.
		return objInfo, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	return fi.ToObjectInfo(bucket, object), nil
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

// Similar to rename but renames data from srcEntry to dstEntry at dataDir
func renameData(ctx context.Context, disks []StorageAPI, srcBucket, srcEntry, dataDir, dstBucket, dstEntry string, writeQuorum int, ignoredErr []error) ([]StorageAPI, error) {
	dataDir = retainSlash(dataDir)
	g := errgroup.WithNErrs(len(disks))

	// Rename file on all underlying storage disks.
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			if err := disks[index].RenameData(srcBucket, srcEntry, dataDir, dstBucket, dstEntry); err != nil {
				if !IsErrIgnored(err, ignoredErr...) {
					return err
				}
			}
			return nil
		}, index)
	}

	// Wait for all renames to finish.
	errs := g.Wait()

	// We can safely allow RenameFile errors up to len(er.getDisks()) - writeQuorum
	// otherwise return failure. Cleanup successful renames.
	err := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if err == errErasureWriteQuorum {
		ug := errgroup.WithNErrs(len(disks))
		for index, disk := range disks {
			if disk == nil {
				continue
			}
			index := index
			ug.Go(func() error {
				// Undo all the partial rename operations.
				if errs[index] == nil {
					_ = disks[index].RenameData(dstBucket, dstEntry, dataDir, srcBucket, srcEntry)
				}
				return nil
			}, index)
		}
		ug.Wait()
	}
	return evalDisks(disks, errs), err
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

	// We can safely allow RenameFile errors up to len(er.getDisks()) - writeQuorum
	// otherwise return failure. Cleanup successful renames.
	err := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if err == errErasureWriteQuorum {
		// Undo all the partial rename operations.
		undoRename(disks, srcBucket, srcEntry, dstBucket, dstEntry, isDir, errs)
	}
	return evalDisks(disks, errs), err
}

// PutObject - creates an object upon reading from the input stream
// until EOF, erasure codes the data across all disk and additionally
// writes `xl.meta` which carries the necessary metadata for future
// object operations.
func (er erasureObjects) PutObject(ctx context.Context, bucket string, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	// Validate put object input args.
	if err = checkPutObjectArgs(ctx, bucket, object, er, data.Size()); err != nil {
		return ObjectInfo{}, err
	}

	return er.putObject(ctx, bucket, object, data, opts)
}

// putObject wrapper for erasureObjects PutObject
func (er erasureObjects) putObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	defer ObjectPathUpdated(path.Join(bucket, object))

	data := r.Reader

	uniqueID := mustGetUUID()
	tempObj := uniqueID
	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	storageDisks := er.getDisks()

	// Get parity and data drive count based on storage class metadata
	parityDrives := globalStorageClass.GetParityForSC(opts.UserDefined[xhttp.AmzStorageClass])
	if parityDrives == 0 {
		parityDrives = getDefaultParityBlocks(len(storageDisks))
	}
	dataDrives := len(storageDisks) - parityDrives

	// we now know the number of blocks this object needs for data and parity.
	// writeQuorum is dataBlocks + 1
	writeQuorum := dataDrives
	if dataDrives == parityDrives {
		writeQuorum = dataDrives + 1
	}

	// Delete temporary object in the event of failure.
	// If PutObject succeeded there would be no temporary
	// object to delete.
	defer er.deleteObject(ctx, minioMetaTmpBucket, tempObj, writeQuorum)

	// This is a special case with size as '0' and object ends with
	// a slash separator, we treat it like a valid operation and
	// return success.
	if isObjectDir(object, data.Size()) {
		// Check if an object is present as one of the parent dir.
		// -- FIXME. (needs a new kind of lock).
		// -- FIXME (this also causes performance issue when disks are down).
		if er.parentDirIsObject(ctx, bucket, path.Dir(object)) {
			return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
		}

		if err = er.putObjectDir(ctx, bucket, object, writeQuorum); err != nil {
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
	if er.parentDirIsObject(ctx, bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
	}

	// Initialize parts metadata
	partsMetadata := make([]FileInfo, len(er.getDisks()))

	fi := newFileInfo(object, dataDrives, parityDrives)

	if opts.Versioned {
		fi.VersionID = opts.VersionID
		if fi.VersionID == "" {
			fi.VersionID = mustGetUUID()
		}
	}
	fi.DataDir = mustGetUUID()

	// Initialize erasure metadata.
	for index := range partsMetadata {
		partsMetadata[index] = fi
	}

	// Order disks according to erasure distribution
	onlineDisks := shuffleDisks(storageDisks, fi.Erasure.Distribution)

	erasure, err := NewErasure(ctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch size := data.Size(); {
	case size == 0:
		buffer = make([]byte, 1) // Allocate atleast a byte to reach EOF
	case size == -1 || size >= fi.Erasure.BlockSize:
		buffer = er.bp.Get()
		defer er.bp.Put(buffer)
	case size < fi.Erasure.BlockSize:
		// No need to allocate fully blockSizeV1 buffer if the incoming data is smaller.
		buffer = make([]byte, size, 2*size+int64(fi.Erasure.ParityBlocks+fi.Erasure.DataBlocks-1))
	}

	if len(buffer) > int(fi.Erasure.BlockSize) {
		buffer = buffer[:fi.Erasure.BlockSize]
	}

	partName := "part.1"
	tempErasureObj := pathJoin(uniqueID, fi.DataDir, partName)

	writers := make([]io.Writer, len(onlineDisks))
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}
		writers[i] = newBitrotWriter(disk, minioMetaTmpBucket, tempErasureObj, erasure.ShardFileSize(data.Size()), DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	n, erasureErr := erasure.Encode(ctx, data, writers, buffer, fi.Erasure.DataBlocks+1)
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

	// Fill all the necessary metadata.
	// Update `xl.meta` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Metadata = opts.UserDefined
		partsMetadata[index].Size = n
		partsMetadata[index].ModTime = modTime
	}

	// Write unique `xl.meta` for each disk.
	if onlineDisks, err = writeUniqueFileInfo(ctx, onlineDisks, minioMetaTmpBucket, tempObj, partsMetadata, writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Rename the successfully written temporary object to final location.
	if onlineDisks, err = renameData(ctx, onlineDisks, minioMetaTmpBucket, tempObj, fi.DataDir, bucket, object, writeQuorum, nil); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Whether a disk was initially or becomes offline
	// during this upload, send it to the MRF list.
	for i := 0; i < len(onlineDisks); i++ {
		if onlineDisks[i] == nil || storageDisks[i] == nil {
			er.addPartialUpload(bucket, object)
			break
		}
	}

	for i := 0; i < len(onlineDisks); i++ {
		if onlineDisks[i] == nil {
			continue
		}
		// Object info is the same in all disks, so we can pick
		// the first meta from online disk
		fi = partsMetadata[i]
		break
	}

	return fi.ToObjectInfo(bucket, object), nil
}

func (er erasureObjects) deleteObjectVersion(ctx context.Context, bucket, object string, writeQuorum int, fi FileInfo) error {
	disks := er.getDisks()

	g := errgroup.WithNErrs(len(disks))

	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			err := disks[index].DeleteVersion(bucket, object, fi)
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
// all the disks in parallel, including `xl.meta` associated with the
// object.
func (er erasureObjects) deleteObject(ctx context.Context, bucket, object string, writeQuorum int) error {
	var disks []StorageAPI
	var err error
	defer ObjectPathUpdated(path.Join(bucket, object))

	tmpObj := mustGetUUID()
	if bucket == minioMetaTmpBucket {
		tmpObj = object
		disks = er.getDisks()
	} else {
		// Rename the current object while requiring write quorum, but also consider
		// that a non found object in a given disk as a success since it already
		// confirms that the object doesn't have a part in that disk (already removed)
		disks, err = rename(ctx, er.getDisks(), bucket, object, minioMetaTmpBucket, tmpObj, true, writeQuorum,
			[]error{errFileNotFound})
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
			err := cleanupDir(ctx, disks[index], minioMetaTmpBucket, tmpObj)
			if err != nil && err != errVolumeNotFound {
				return err
			}
			return nil
		}, index)
	}

	// return errors if any during deletion
	return reduceWriteQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, writeQuorum)
}

// DeleteObjects deletes objects/versions in bulk, this function will still automatically split objects list
// into smaller bulks if some object names are found to be duplicated in the delete list, splitting
// into smaller bulks will avoid holding twice the write lock of the duplicated object names.
func (er erasureObjects) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]DeletedObject, len(objects))
	writeQuorums := make([]int, len(objects))

	for i, object := range objects {
		errs[i] = checkDelObjArgs(ctx, bucket, object.ObjectName)
	}

	storageDisks := er.getDisks()

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

	versions := make([]FileInfo, len(objects))
	for i := range objects {
		if objects[i].VersionID == "" {
			if opts.Versioned && !HasSuffix(objects[i].ObjectName, SlashSeparator) {
				versions[i] = FileInfo{
					Name:      objects[i].ObjectName,
					VersionID: mustGetUUID(),
					ModTime:   UTCNow(),
					Deleted:   true, // delete marker
				}
				continue
			}
		}
		versions[i] = FileInfo{
			Name:      objects[i].ObjectName,
			VersionID: objects[i].VersionID,
		}
	}

	// Initialize list of errors.
	var opErrs = make([]error, len(storageDisks))
	var delObjErrs = make([][]error, len(storageDisks))

	var wg sync.WaitGroup
	// Remove versions in bulk for each disk
	for index, disk := range storageDisks {
		if disk == nil {
			opErrs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			delObjErrs[index] = disk.DeleteVersions(bucket, versions)
		}(index, disk)
	}

	wg.Wait()

	// Reduce errors for each object
	for objIndex := range objects {
		if errs[objIndex] != nil {
			continue
		}
		diskErrs := make([]error, len(storageDisks))
		// Iterate over disks to fetch the error
		// of deleting of the current object
		for i := range delObjErrs {
			// delObjErrs[i] is not nil when disks[i] is also not nil
			if delObjErrs[i] != nil {
				if delObjErrs[i][objIndex] != errFileNotFound {
					diskErrs[i] = delObjErrs[i][objIndex]
				}
			}
		}
		errs[objIndex] = reduceWriteQuorumErrs(ctx, diskErrs, objectOpIgnoredErrs, writeQuorums[objIndex])
		if errs[objIndex] == nil {
			if versions[objIndex].Deleted {
				dobjects[objIndex] = DeletedObject{
					DeleteMarker:          versions[objIndex].Deleted,
					DeleteMarkerVersionID: versions[objIndex].VersionID,
					ObjectName:            versions[objIndex].Name,
				}
			} else {
				dobjects[objIndex] = DeletedObject{
					ObjectName: versions[objIndex].Name,
					VersionID:  versions[objIndex].VersionID,
				}
			}
		}
	}

	return dobjects, errs
}

// DeleteObject - deletes an object, this call doesn't necessary reply
// any error as it is not necessary for the handler to reply back a
// response to the client request.
func (er erasureObjects) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if err = checkDelObjArgs(ctx, bucket, object); err != nil {
		return objInfo, err
	}

	storageDisks := er.getDisks()
	writeQuorum := len(storageDisks)/2 + 1

	if opts.VersionID == "" {
		if opts.Versioned && !HasSuffix(object, SlashSeparator) {
			fi := FileInfo{
				Name:      object,
				VersionID: mustGetUUID(),
				Deleted:   true,
				ModTime:   UTCNow(),
			}
			// Add delete marker, since we don't have any version specified explicitly.
			if err = er.deleteObjectVersion(ctx, bucket, object, writeQuorum, fi); err != nil {
				return objInfo, toObjectErr(err, bucket, object)
			}
			return fi.ToObjectInfo(bucket, object), nil
		}
	}

	// Delete the object version on all disks.
	if err = er.deleteObjectVersion(ctx, bucket, object, writeQuorum, FileInfo{
		Name:      object,
		VersionID: opts.VersionID,
	}); err != nil {
		return objInfo, toObjectErr(err, bucket, object)
	}

	return ObjectInfo{Bucket: bucket, Name: object, VersionID: opts.VersionID}, nil
}

// Send the successful but partial upload, however ignore
// if the channel is blocked by other items.
func (er erasureObjects) addPartialUpload(bucket, key string) {
	select {
	case er.mrfUploadCh <- partialUpload{bucket: bucket, object: key}:
	default:
	}
}

// PutObjectTags - replace or add tags to an existing object
func (er erasureObjects) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) error {
	disks := er.getDisks()

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllFileInfo(ctx, disks, bucket, object, opts.VersionID)

	readQuorum, writeQuorum, err := objectQuorumFromMeta(ctx, er, metaArr, errs)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	// List all online disks.
	_, modTime := listOnlineDisks(disks, metaArr, errs)

	// Pick latest valid metadata.
	fi, err := pickValidFileInfo(ctx, metaArr, modTime, readQuorum)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	if fi.Deleted {
		if opts.VersionID == "" {
			return toObjectErr(errFileNotFound, bucket, object)
		}
		return toObjectErr(errMethodNotAllowed, bucket, object)
	}

	for i, fi := range metaArr {
		if errs[i] != nil {
			// Avoid disks where loading metadata fail
			continue
		}

		// clean fi.Meta of tag key, before updating the new tags
		delete(fi.Metadata, xhttp.AmzObjectTagging)
		// Don't update for empty tags
		if tags != "" {
			fi.Metadata[xhttp.AmzObjectTagging] = tags
		}
		metaArr[i].Metadata = fi.Metadata
	}

	tempObj := mustGetUUID()

	// Write unique `xl.meta` for each disk.
	if disks, err = writeUniqueFileInfo(ctx, disks, minioMetaTmpBucket, tempObj, metaArr, writeQuorum); err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Atomically rename metadata from tmp location to destination for each disk.
	if _, err = renameFileInfo(ctx, disks, minioMetaTmpBucket, tempObj, bucket, object, writeQuorum); err != nil {
		return toObjectErr(err, bucket, object)
	}

	return nil
}

// DeleteObjectTags - delete object tags from an existing object
func (er erasureObjects) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return er.PutObjectTags(ctx, bucket, object, "", opts)
}

// GetObjectTags - get object tags from an existing object
func (er erasureObjects) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	// GetObjectInfo will return tag value as well
	oi, err := er.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	return tags.ParseObjectTags(oi.UserTags)
}
