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
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7/pkg/tags"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/lifecycle"
	"github.com/minio/minio/pkg/bucket/replication"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// list all errors which can be ignored in object operations.
var objectOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errUnformattedDisk)

/// Object Operations

func countOnlineDisks(onlineDisks []StorageAPI) (online int) {
	for _, onlineDisk := range onlineDisks {
		if onlineDisk != nil && onlineDisk.IsOnline() {
			online++
		}
	}
	return online
}

// CopyObject - copy object source object to destination object.
// if source object and destination object are same we only
// update metadata.
func (er erasureObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (oi ObjectInfo, err error) {
	// This call shouldn't be used for anything other than metadata updates or adding self referential versions.
	if !srcInfo.metadataOnly {
		return oi, NotImplemented{}
	}

	defer ObjectPathUpdated(pathJoin(dstBucket, dstObject))

	lk := er.NewNSLock(dstBucket, dstObject)
	ctx, err = lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return oi, err
	}
	defer lk.Unlock()

	// Read metadata associated with the object from all disks.
	storageDisks := er.getDisks()
	metaArr, errs := readAllFileInfo(ctx, storageDisks, srcBucket, srcObject, srcOpts.VersionID, true)

	// get Quorum for this object
	readQuorum, writeQuorum, err := objectQuorumFromMeta(ctx, metaArr, errs, er.defaultParityCount)
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

	onlineDisks, metaArr = shuffleDisksAndPartsMetadataByIndex(onlineDisks, metaArr, fi)
	versionID := srcInfo.VersionID
	if srcInfo.versionOnly {
		versionID = dstOpts.VersionID
		// preserve destination versionId if specified.
		if versionID == "" {
			versionID = mustGetUUID()
		}
		modTime = UTCNow()
	}
	fi.VersionID = versionID // set any new versionID we might have created
	fi.ModTime = modTime     // set modTime for the new versionID
	if !dstOpts.MTime.IsZero() {
		modTime = dstOpts.MTime
		fi.ModTime = dstOpts.MTime
	}
	fi.Metadata = srcInfo.UserDefined
	srcInfo.UserDefined["etag"] = srcInfo.ETag

	// Update `xl.meta` content on each disks.
	for index := range metaArr {
		if metaArr[index].IsValid() {
			metaArr[index].ModTime = modTime
			metaArr[index].VersionID = versionID
			metaArr[index].Metadata = srcInfo.UserDefined
		}
	}

	tempObj := mustGetUUID()

	var online int
	// Cleanup in case of xl.meta writing failure
	defer func() {
		if online != len(onlineDisks) {
			er.deleteObject(context.Background(), minioMetaTmpBucket, tempObj, writeQuorum)
		}
	}()

	// Write unique `xl.meta` for each disk.
	if onlineDisks, err = writeUniqueFileInfo(ctx, onlineDisks, minioMetaTmpBucket, tempObj, metaArr, writeQuorum); err != nil {
		return oi, toObjectErr(err, srcBucket, srcObject)
	}

	// Rename atomically `xl.meta` from tmp location to destination for each disk.
	if _, err = renameFileInfo(ctx, onlineDisks, minioMetaTmpBucket, tempObj, srcBucket, srcObject, writeQuorum); err != nil {
		return oi, toObjectErr(err, srcBucket, srcObject)
	}

	online = countOnlineDisks(onlineDisks)

	return fi.ToObjectInfo(srcBucket, srcObject), nil
}

// GetObjectNInfo - returns object info and an object
// Read(Closer). When err != nil, the returned reader is always nil.
func (er erasureObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	var unlockOnDefer bool
	var nsUnlocker = func() {}
	defer func() {
		if unlockOnDefer {
			nsUnlocker()
		}
	}()

	// Acquire lock
	if lockType != noLock {
		lock := er.NewNSLock(bucket, object)
		switch lockType {
		case writeLock:
			ctx, err = lock.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			nsUnlocker = lock.Unlock
		case readLock:
			ctx, err = lock.GetRLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			nsUnlocker = lock.RUnlock
		}
		unlockOnDefer = true
	}

	fi, metaArr, onlineDisks, err := er.getObjectFileInfo(ctx, bucket, object, opts, true)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}

	objInfo := fi.ToObjectInfo(bucket, object)
	if objInfo.DeleteMarker {
		if opts.VersionID == "" {
			return &GetObjectReader{
				ObjInfo: objInfo,
			}, toObjectErr(errFileNotFound, bucket, object)
		}
		// Make sure to return object info to provide extra information.
		return &GetObjectReader{
			ObjInfo: objInfo,
		}, toObjectErr(errMethodNotAllowed, bucket, object)
	}
	if objInfo.TransitionStatus == lifecycle.TransitionComplete {
		// If transitioned, stream from transition tier unless object is restored locally or restore date is past.
		restoreHdr, ok := caseInsensitiveMap(objInfo.UserDefined).Lookup(xhttp.AmzRestore)
		if !ok || !strings.HasPrefix(restoreHdr, "ongoing-request=false") || (!objInfo.RestoreExpires.IsZero() && time.Now().After(objInfo.RestoreExpires)) {
			return getTransitionedObjectReader(ctx, bucket, object, rs, h, objInfo, opts)
		}
	}
	unlockOnDefer = false
	fn, off, length, nErr := NewGetObjectReader(rs, objInfo, opts, nsUnlocker)
	if nErr != nil {
		return nil, nErr
	}
	pr, pw := io.Pipe()
	go func() {
		err := er.getObjectWithFileInfo(ctx, bucket, object, off, length, pw, fi, metaArr, onlineDisks)
		pw.CloseWithError(err)
	}()

	// Cleanup function to cause the go routine above to exit, in
	// case of incomplete read.
	pipeCloser := func() { pr.Close() }

	return fn(pr, h, opts.CheckPrecondFn, pipeCloser)
}

// GetObject - reads an object erasured coded across multiple
// disks. Supports additional parameters like offset and length
// which are synonymous with HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (er erasureObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts ObjectOptions) (err error) {
	// Lock the object before reading.
	lk := er.NewNSLock(bucket, object)
	ctx, err = lk.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return err
	}
	defer lk.RUnlock()

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

	return er.getObject(ctx, bucket, object, startOffset, length, writer, opts)
}

func (er erasureObjects) getObjectWithFileInfo(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, fi FileInfo, metaArr []FileInfo, onlineDisks []StorageAPI) error {
	// Reorder online disks based on erasure distribution order.
	// Reorder parts metadata based on erasure distribution order.
	onlineDisks, metaArr = shuffleDisksAndPartsMetadataByIndex(onlineDisks, metaArr, fi)

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
			if !metaArr[index].IsValid() {
				continue
			}
			checksumInfo := metaArr[index].Erasure.GetChecksumInfo(partNumber)
			partPath := pathJoin(object, metaArr[index].DataDir, fmt.Sprintf("part.%d", partNumber))
			data := metaArr[index].Data
			readers[index] = newBitrotReader(disk, data, bucket, partPath, tillOffset,
				checksumInfo.Algorithm, checksumInfo.Hash, erasure.ShardSize())

			// Prefer local disks
			prefer[index] = disk.Hostname() == ""
		}

		written, err := erasure.Decode(ctx, writer, readers, partOffset, partLength, partSize, prefer)
		// Note: we should not be defer'ing the following closeBitrotReaders() call as
		// we are inside a for loop i.e if we use defer, we would accumulate a lot of open files by the time
		// we return from this function.
		closeBitrotReaders(readers)
		if err != nil {
			// If we have successfully written all the content that was asked
			// by the client, but we still see an error - this would mean
			// that we have some parts or data blocks missing or corrupted
			// - attempt a heal to successfully heal them for future calls.
			if written == partLength {
				var scan madmin.HealScanMode
				if errors.Is(err, errFileNotFound) {
					scan = madmin.HealNormalScan
				} else if errors.Is(err, errFileCorrupt) {
					scan = madmin.HealDeepScan
				}
				if scan != madmin.HealUnknownScan {
					healOnce.Do(func() {
						if _, healing := er.getOnlineDisksWithHealing(); !healing {
							go healObject(bucket, object, fi.VersionID, scan)
						}
					})
				}
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
func (er erasureObjects) getObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, opts ObjectOptions) error {
	fi, metaArr, onlineDisks, err := er.getObjectFileInfo(ctx, bucket, object, opts, true)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	if fi.Deleted {
		if opts.VersionID == "" {
			return toObjectErr(errFileNotFound, bucket, object)
		}
		// Make sure to return object info to provide extra information.
		return toObjectErr(errMethodNotAllowed, bucket, object)
	}

	return er.getObjectWithFileInfo(ctx, bucket, object, startOffset, length, writer, fi, metaArr, onlineDisks)
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (er erasureObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (info ObjectInfo, err error) {
	if !opts.NoLock {
		// Lock the object before reading.
		lk := er.NewNSLock(bucket, object)
		ctx, err = lk.GetRLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		defer lk.RUnlock()
	}

	return er.getObjectInfo(ctx, bucket, object, opts)
}

func (er erasureObjects) getObjectFileInfo(ctx context.Context, bucket, object string, opts ObjectOptions, readData bool) (fi FileInfo, metaArr []FileInfo, onlineDisks []StorageAPI, err error) {
	disks := er.getDisks()

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllFileInfo(ctx, disks, bucket, object, opts.VersionID, readData)

	readQuorum, _, err := objectQuorumFromMeta(ctx, metaArr, errs, er.defaultParityCount)
	if err != nil {
		return fi, nil, nil, err
	}

	if reducedErr := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum); reducedErr != nil {
		if reducedErr == errErasureReadQuorum && bucket != minioMetaBucket {
			if _, ok := isObjectDangling(metaArr, errs, nil); ok {
				reducedErr = errFileNotFound
				if opts.VersionID != "" {
					reducedErr = errFileVersionNotFound
				}
				// Remove the dangling object only when:
				//  - This is a non versioned bucket
				//  - This is a versioned bucket and the version ID is passed, the reason
				//    is that we cannot fetch the ID of the latest version when we don't trust xl.meta
				if !opts.Versioned || opts.VersionID != "" {
					er.deleteObjectVersion(ctx, bucket, object, 1, FileInfo{
						Name:      object,
						VersionID: opts.VersionID,
					}, false)
				}
			}
		}
		return fi, nil, nil, toObjectErr(reducedErr, bucket, object)
	}

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(disks, metaArr, errs)

	// Pick latest valid metadata.
	fi, err = pickValidFileInfo(ctx, metaArr, modTime, readQuorum)
	if err != nil {
		return fi, nil, nil, err
	}

	var missingBlocks int
	for i, err := range errs {
		if err != nil && errors.Is(err, errFileNotFound) {
			missingBlocks++
			continue
		}
		if metaArr[i].IsValid() && metaArr[i].ModTime.Equal(fi.ModTime) {
			continue
		}
		missingBlocks++
	}

	// if missing metadata can be reconstructed, attempt to reconstruct.
	if missingBlocks > 0 && missingBlocks < readQuorum {
		if _, healing := er.getOnlineDisksWithHealing(); !healing {
			go healObject(bucket, object, fi.VersionID, madmin.HealNormalScan)
		}
	}

	return fi, metaArr, onlineDisks, nil
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (er erasureObjects) getObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	fi, _, _, err := er.getObjectFileInfo(ctx, bucket, object, opts, false)
	if err != nil {
		return objInfo, toObjectErr(err, bucket, object)

	}
	objInfo = fi.ToObjectInfo(bucket, object)
	if !fi.VersionPurgeStatus.Empty() {
		// Make sure to return object info to provide extra information.
		return objInfo, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	if fi.Deleted {
		if opts.VersionID == "" || opts.DeleteMarker {
			return objInfo, toObjectErr(errFileNotFound, bucket, object)
		}
		// Make sure to return object info to provide extra information.
		return objInfo, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	return objInfo, nil
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
				_ = disks[index].RenameFile(context.TODO(), dstBucket, dstEntry, srcBucket, srcEntry)
			}
			return nil
		}, index)
	}
	g.Wait()
}

// Similar to rename but renames data from srcEntry to dstEntry at dataDir
func renameData(ctx context.Context, disks []StorageAPI, srcBucket, srcEntry, dataDir, dstBucket, dstEntry string, writeQuorum int, ignoredErr []error) ([]StorageAPI, error) {
	dataDir = retainSlash(dataDir)
	defer ObjectPathUpdated(pathJoin(srcBucket, srcEntry))
	defer ObjectPathUpdated(pathJoin(dstBucket, dstEntry))

	g := errgroup.WithNErrs(len(disks))

	// Rename file on all underlying storage disks.
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			if err := disks[index].RenameData(ctx, srcBucket, srcEntry, dataDir, dstBucket, dstEntry); err != nil {
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
					_ = disks[index].RenameData(context.Background(), dstBucket, dstEntry, dataDir, srcBucket, srcEntry)
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
	defer ObjectPathUpdated(pathJoin(srcBucket, srcEntry))
	defer ObjectPathUpdated(pathJoin(dstBucket, dstEntry))

	g := errgroup.WithNErrs(len(disks))

	// Rename file on all underlying storage disks.
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			if err := disks[index].RenameFile(ctx, srcBucket, srcEntry, dstBucket, dstEntry); err != nil {
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
	return er.putObject(ctx, bucket, object, data, opts)
}

// putObject wrapper for erasureObjects PutObject
func (er erasureObjects) putObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	defer func() {
		ObjectPathUpdated(pathJoin(bucket, object))
	}()

	data := r.Reader

	uniqueID := mustGetUUID()
	tempObj := uniqueID
	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	storageDisks := er.getDisks()

	parityDrives := len(storageDisks) / 2
	if !opts.MaxParity {
		// Get parity and data drive count based on storage class metadata
		parityDrives = globalStorageClass.GetParityForSC(opts.UserDefined[xhttp.AmzStorageClass])
		if parityDrives <= 0 {
			parityDrives = er.defaultParityCount
		}
	}
	dataDrives := len(storageDisks) - parityDrives

	// we now know the number of blocks this object needs for data and parity.
	// writeQuorum is dataBlocks + 1
	writeQuorum := dataDrives
	if dataDrives == parityDrives {
		writeQuorum++
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument, logger.Application)
		return ObjectInfo{}, toObjectErr(errInvalidArgument)
	}

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
	// -- FIXME (this also causes performance issue when disks are down).
	if opts.ParentIsObject != nil && opts.ParentIsObject(ctx, bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
	}

	// Initialize parts metadata
	partsMetadata := make([]FileInfo, len(storageDisks))

	fi := newFileInfo(pathJoin(bucket, object), dataDrives, parityDrives)

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
	var onlineDisks []StorageAPI
	onlineDisks, partsMetadata = shuffleDisksAndPartsMetadata(storageDisks, partsMetadata, fi.Erasure.Distribution)

	erasure, err := NewErasure(ctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch size := data.Size(); {
	case size == 0:
		buffer = make([]byte, 1) // Allocate atleast a byte to reach EOF
	case size == -1:
		if size := data.ActualSize(); size > 0 && size < fi.Erasure.BlockSize {
			buffer = make([]byte, data.ActualSize()+256, data.ActualSize()*2+512)
		} else {
			buffer = er.bp.Get()
			defer er.bp.Put(buffer)
		}
	case size >= fi.Erasure.BlockSize:
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

	// Delete temporary object in the event of failure.
	// If PutObject succeeded there would be no temporary
	// object to delete.
	var online int
	defer func() {
		if online != len(onlineDisks) {
			er.deleteObject(context.Background(), minioMetaTmpBucket, tempObj, writeQuorum)
		}
	}()

	shardFileSize := erasure.ShardFileSize(data.Size())
	writers := make([]io.Writer, len(onlineDisks))
	var inlineBuffers []*bytes.Buffer
	if shardFileSize >= 0 {
		if !opts.Versioned && shardFileSize < smallFileThreshold {
			inlineBuffers = make([]*bytes.Buffer, len(onlineDisks))
		} else if shardFileSize < smallFileThreshold/8 {
			inlineBuffers = make([]*bytes.Buffer, len(onlineDisks))
		}
	}
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}

		if len(inlineBuffers) > 0 {
			inlineBuffers[i] = bytes.NewBuffer(make([]byte, 0, shardFileSize))
			writers[i] = newStreamingBitrotWriterBuffer(inlineBuffers[i], DefaultBitrotAlgorithm, erasure.ShardSize())
			continue
		}
		writers[i] = newBitrotWriter(disk, minioMetaTmpBucket, tempErasureObj,
			shardFileSize, DefaultBitrotAlgorithm, erasure.ShardSize(), false)
	}

	n, erasureErr := erasure.Encode(ctx, data, writers, buffer, writeQuorum)
	closeBitrotWriters(writers)
	if erasureErr != nil {
		return ObjectInfo{}, toObjectErr(erasureErr, minioMetaTmpBucket, tempErasureObj)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if n < data.Size() {
		return ObjectInfo{}, IncompleteBody{Bucket: bucket, Object: object}
	}

	if !opts.NoLock {
		var err error
		lk := er.NewNSLock(bucket, object)
		ctx, err = lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		defer lk.Unlock()
	}

	for i, w := range writers {
		if w == nil {
			onlineDisks[i] = nil
			continue
		}
		if len(inlineBuffers) > 0 && inlineBuffers[i] != nil {
			partsMetadata[i].Data = inlineBuffers[i].Bytes()
		}
		partsMetadata[i].AddObjectPart(1, "", n, data.ActualSize())
		partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{
			PartNumber: 1,
			Algorithm:  DefaultBitrotAlgorithm,
			Hash:       bitrotWriterSum(w),
		})
	}
	if opts.UserDefined["etag"] == "" {
		opts.UserDefined["etag"] = r.MD5CurrentHexString()
	}

	// Guess content-type from the extension if possible.
	if opts.UserDefined["content-type"] == "" {
		opts.UserDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
	}

	modTime := opts.MTime
	if opts.MTime.IsZero() {
		modTime = UTCNow()
	}

	// Fill all the necessary metadata.
	// Update `xl.meta` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Metadata = opts.UserDefined
		partsMetadata[index].Size = n
		partsMetadata[index].ModTime = modTime
		if len(inlineBuffers) > 0 && inlineBuffers[index] != nil {
			partsMetadata[index].Data = inlineBuffers[index].Bytes()
		}
	}

	// Write unique `xl.meta` for each disk.
	if onlineDisks, err = writeUniqueFileInfo(ctx, onlineDisks, minioMetaTmpBucket, tempObj, partsMetadata, writeQuorum); err != nil {
		logger.LogIf(ctx, err)
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Rename the successfully written temporary object to final location.
	if onlineDisks, err = renameData(ctx, onlineDisks, minioMetaTmpBucket, tempObj, fi.DataDir, bucket, object, writeQuorum, nil); err != nil {
		logger.LogIf(ctx, err)
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Whether a disk was initially or becomes offline
	// during this upload, send it to the MRF list.
	for i := 0; i < len(onlineDisks); i++ {
		if onlineDisks[i] != nil && onlineDisks[i].IsOnline() {
			continue
		}
		er.addPartial(bucket, object, fi.VersionID)
		break
	}

	for i := 0; i < len(onlineDisks); i++ {
		if onlineDisks[i] != nil && onlineDisks[i].IsOnline() {
			// Object info is the same in all disks, so we can pick
			// the first meta from online disk
			fi = partsMetadata[i]
			break
		}
	}
	online = countOnlineDisks(onlineDisks)

	return fi.ToObjectInfo(bucket, object), nil
}

func (er erasureObjects) deleteObjectVersion(ctx context.Context, bucket, object string, writeQuorum int, fi FileInfo, forceDelMarker bool) error {
	defer ObjectPathUpdated(pathJoin(bucket, object))
	disks := er.getDisks()
	g := errgroup.WithNErrs(len(disks))
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			return disks[index].DeleteVersion(ctx, bucket, object, fi, forceDelMarker)
		}, index)
	}
	// return errors if any during deletion
	return reduceWriteQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, writeQuorum)
}

// deleteEmptyDir knows only how to remove an empty directory (not the empty object with a
// trailing slash), this is called for the healing code to remove such directories.
func (er erasureObjects) deleteEmptyDir(ctx context.Context, bucket, object string) error {
	defer ObjectPathUpdated(pathJoin(bucket, object))

	if bucket == minioMetaTmpBucket {
		return nil
	}

	disks := er.getDisks()
	g := errgroup.WithNErrs(len(disks))
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			return disks[index].Delete(ctx, bucket, object, false)
		}, index)
	}

	// return errors if any during deletion
	return reduceWriteQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, len(disks)/2+1)
}

// deleteObject - wrapper for delete object, deletes an object from
// all the disks in parallel, including `xl.meta` associated with the
// object.
func (er erasureObjects) deleteObject(ctx context.Context, bucket, object string, writeQuorum int) error {
	var err error
	defer ObjectPathUpdated(pathJoin(bucket, object))

	disks := er.getDisks()
	tmpObj := mustGetUUID()
	if bucket == minioMetaTmpBucket {
		tmpObj = object
	} else {
		// Rename the current object while requiring write quorum, but also consider
		// that a non found object in a given disk as a success since it already
		// confirms that the object doesn't have a part in that disk (already removed)
		disks, err = rename(ctx, disks, bucket, object, minioMetaTmpBucket, tmpObj, true, writeQuorum,
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
			return disks[index].Delete(ctx, minioMetaTmpBucket, tmpObj, true)
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

	storageDisks := er.getDisks()

	for i := range objects {
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
			modTime := opts.MTime
			if opts.MTime.IsZero() {
				modTime = UTCNow()
			}
			uuid := opts.VersionID
			if uuid == "" {
				uuid = mustGetUUID()
			}
			if opts.Versioned || opts.VersionSuspended {
				versions[i] = FileInfo{
					Name:                          objects[i].ObjectName,
					ModTime:                       modTime,
					Deleted:                       true, // delete marker
					DeleteMarkerReplicationStatus: objects[i].DeleteMarkerReplicationStatus,
					VersionPurgeStatus:            objects[i].VersionPurgeStatus,
				}
				if opts.Versioned {
					versions[i].VersionID = uuid
				}
				continue
			}
		}
		versions[i] = FileInfo{
			Name:                          objects[i].ObjectName,
			VersionID:                     objects[i].VersionID,
			DeleteMarkerReplicationStatus: objects[i].DeleteMarkerReplicationStatus,
			VersionPurgeStatus:            objects[i].VersionPurgeStatus,
		}
	}

	// Initialize list of errors.
	var delObjErrs = make([][]error, len(storageDisks))

	var wg sync.WaitGroup
	// Remove versions in bulk for each disk
	for index, disk := range storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			if disk == nil {
				delObjErrs[index] = make([]error, len(versions))
				for i := range versions {
					delObjErrs[index][i] = errDiskNotFound
				}
				return
			}
			delObjErrs[index] = disk.DeleteVersions(ctx, bucket, versions)
		}(index, disk)
	}

	wg.Wait()

	// Reduce errors for each object
	for objIndex := range objects {
		diskErrs := make([]error, len(storageDisks))
		// Iterate over disks to fetch the error
		// of deleting of the current object
		for i := range delObjErrs {
			// delObjErrs[i] is not nil when disks[i] is also not nil
			if delObjErrs[i] != nil {
				diskErrs[i] = delObjErrs[i][objIndex]
			}
		}
		err := reduceWriteQuorumErrs(ctx, diskErrs, objectOpIgnoredErrs, writeQuorums[objIndex])
		if objects[objIndex].VersionID != "" {
			errs[objIndex] = toObjectErr(err, bucket, objects[objIndex].ObjectName, objects[objIndex].VersionID)
		} else {
			errs[objIndex] = toObjectErr(err, bucket, objects[objIndex].ObjectName)
		}

		if errs[objIndex] == nil {
			ObjectPathUpdated(pathJoin(bucket, objects[objIndex].ObjectName))
		}

		if versions[objIndex].Deleted {
			dobjects[objIndex] = DeletedObject{
				DeleteMarker:                  versions[objIndex].Deleted,
				DeleteMarkerVersionID:         versions[objIndex].VersionID,
				DeleteMarkerMTime:             DeleteMarkerMTime{versions[objIndex].ModTime},
				DeleteMarkerReplicationStatus: versions[objIndex].DeleteMarkerReplicationStatus,
				ObjectName:                    versions[objIndex].Name,
				VersionPurgeStatus:            versions[objIndex].VersionPurgeStatus,
			}
		} else {
			dobjects[objIndex] = DeletedObject{
				ObjectName:                    versions[objIndex].Name,
				VersionID:                     versions[objIndex].VersionID,
				VersionPurgeStatus:            versions[objIndex].VersionPurgeStatus,
				DeleteMarkerReplicationStatus: versions[objIndex].DeleteMarkerReplicationStatus,
			}
		}
	}

	// Check failed deletes across multiple objects
	for _, version := range versions {
		// Check if there is any offline disk and add it to the MRF list
		for _, disk := range storageDisks {
			if disk != nil && disk.IsOnline() {
				// Skip attempted heal on online disks.
				continue
			}

			// all other direct versionId references we should
			// ensure no dangling file is left over.
			er.addPartial(bucket, version.Name, version.VersionID)
			break
		}
	}

	return dobjects, errs
}

// DeleteObject - deletes an object, this call doesn't necessary reply
// any error as it is not necessary for the handler to reply back a
// response to the client request.
func (er erasureObjects) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	versionFound := true
	objInfo = ObjectInfo{VersionID: opts.VersionID} // version id needed in Delete API response.
	goi, gerr := er.GetObjectInfo(ctx, bucket, object, opts)
	if gerr != nil && goi.Name == "" {
		switch gerr.(type) {
		case InsufficientReadQuorum:
			return objInfo, InsufficientWriteQuorum{}
		}
		// For delete marker replication, versionID being replicated will not exist on disk
		if opts.DeleteMarker {
			versionFound = false
		} else {
			return objInfo, gerr
		}
	}
	// Acquire a write lock before deleting the object.
	lk := er.NewNSLock(bucket, object)
	ctx, err = lk.GetLock(ctx, globalDeleteOperationTimeout)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer lk.Unlock()

	storageDisks := er.getDisks()
	writeQuorum := len(storageDisks)/2 + 1
	var markDelete bool
	// Determine whether to mark object deleted for replication
	if goi.VersionID != "" {
		markDelete = true
	}

	// Default deleteMarker to true if object is under versioning
	deleteMarker := opts.Versioned

	if opts.VersionID != "" {
		// case where replica version needs to be deleted on target cluster
		if versionFound && opts.DeleteMarkerReplicationStatus == replication.Replica.String() {
			markDelete = false
		}
		if opts.VersionPurgeStatus.Empty() && opts.DeleteMarkerReplicationStatus == "" {
			markDelete = false
		}
		if opts.VersionPurgeStatus == Complete {
			markDelete = false
		}
		// determine if the version represents an object delete
		// deleteMarker = true
		if versionFound && !goi.DeleteMarker { // implies a versioned delete of object
			deleteMarker = false
		}
	}

	modTime := opts.MTime
	if opts.MTime.IsZero() {
		modTime = UTCNow()
	}

	if markDelete {
		if opts.Versioned || opts.VersionSuspended {
			fi := FileInfo{
				Name:                          object,
				Deleted:                       deleteMarker,
				MarkDeleted:                   markDelete,
				ModTime:                       modTime,
				DeleteMarkerReplicationStatus: opts.DeleteMarkerReplicationStatus,
				VersionPurgeStatus:            opts.VersionPurgeStatus,
				TransitionStatus:              opts.Transition.Status,
				ExpireRestored:                opts.Transition.ExpireRestored,
			}
			if opts.Versioned {
				fi.VersionID = mustGetUUID()
				if opts.VersionID != "" {
					fi.VersionID = opts.VersionID
				}
			}
			// versioning suspended means we add `null`
			// version as delete marker
			// Add delete marker, since we don't have any version specified explicitly.
			// Or if a particular version id needs to be replicated.
			if err = er.deleteObjectVersion(ctx, bucket, object, writeQuorum, fi, opts.DeleteMarker); err != nil {
				return objInfo, toObjectErr(err, bucket, object)
			}
			return fi.ToObjectInfo(bucket, object), nil
		}
	}

	// Delete the object version on all disks.
	if err = er.deleteObjectVersion(ctx, bucket, object, writeQuorum, FileInfo{
		Name:                          object,
		VersionID:                     opts.VersionID,
		MarkDeleted:                   markDelete,
		Deleted:                       deleteMarker,
		ModTime:                       modTime,
		DeleteMarkerReplicationStatus: opts.DeleteMarkerReplicationStatus,
		VersionPurgeStatus:            opts.VersionPurgeStatus,
		TransitionStatus:              opts.Transition.Status,
		ExpireRestored:                opts.Transition.ExpireRestored,
	}, opts.DeleteMarker); err != nil {
		return objInfo, toObjectErr(err, bucket, object)
	}

	for _, disk := range storageDisks {
		if disk != nil && disk.IsOnline() {
			continue
		}
		er.addPartial(bucket, object, opts.VersionID)
		break
	}

	return ObjectInfo{
		Bucket:             bucket,
		Name:               object,
		VersionID:          opts.VersionID,
		VersionPurgeStatus: opts.VersionPurgeStatus,
		ReplicationStatus:  replication.StatusType(opts.DeleteMarkerReplicationStatus),
	}, nil
}

// Send the successful but partial upload/delete, however ignore
// if the channel is blocked by other items.
func (er erasureObjects) addPartial(bucket, object, versionID string) {
	select {
	case er.mrfOpCh <- partialOperation{bucket: bucket, object: object, versionID: versionID}:
	default:
	}
}

func (er erasureObjects) PutObjectMetadata(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	var err error
	// Lock the object before updating tags.
	lk := er.NewNSLock(bucket, object)
	ctx, err = lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer lk.Unlock()

	disks := er.getDisks()

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllFileInfo(ctx, disks, bucket, object, opts.VersionID, false)

	readQuorum, _, err := objectQuorumFromMeta(ctx, metaArr, errs, er.defaultParityCount)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// List all online disks.
	_, modTime := listOnlineDisks(disks, metaArr, errs)

	// Pick latest valid metadata.
	fi, err := pickValidFileInfo(ctx, metaArr, modTime, readQuorum)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	if fi.Deleted {
		if opts.VersionID == "" {
			return ObjectInfo{}, toObjectErr(errFileNotFound, bucket, object)
		}
		return ObjectInfo{}, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	for k, v := range opts.UserDefined {
		fi.Metadata[k] = v
	}
	fi.ModTime = opts.MTime
	fi.VersionID = opts.VersionID

	if err = er.updateObjectMeta(ctx, bucket, object, fi); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	objInfo := fi.ToObjectInfo(bucket, object)
	return objInfo, nil

}

// PutObjectTags - replace or add tags to an existing object
func (er erasureObjects) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) (ObjectInfo, error) {
	var err error
	// Lock the object before updating tags.
	lk := er.NewNSLock(bucket, object)
	ctx, err = lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer lk.Unlock()

	disks := er.getDisks()

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllFileInfo(ctx, disks, bucket, object, opts.VersionID, false)

	readQuorum, _, err := objectQuorumFromMeta(ctx, metaArr, errs, er.defaultParityCount)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// List all online disks.
	_, modTime := listOnlineDisks(disks, metaArr, errs)

	// Pick latest valid metadata.
	fi, err := pickValidFileInfo(ctx, metaArr, modTime, readQuorum)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	if fi.Deleted {
		if opts.VersionID == "" {
			return ObjectInfo{}, toObjectErr(errFileNotFound, bucket, object)
		}
		return ObjectInfo{}, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	fi.Metadata[xhttp.AmzObjectTagging] = tags
	for k, v := range opts.UserDefined {
		fi.Metadata[k] = v
	}

	if err = er.updateObjectMeta(ctx, bucket, object, fi); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	return fi.ToObjectInfo(bucket, object), nil
}

// updateObjectMeta will update the metadata of a file.
func (er erasureObjects) updateObjectMeta(ctx context.Context, bucket, object string, fi FileInfo) error {
	if len(fi.Metadata) == 0 {
		return nil
	}

	disks := er.getDisks()

	g := errgroup.WithNErrs(len(disks))

	// Start writing `xl.meta` to all disks in parallel.
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			return disks[index].UpdateMetadata(ctx, bucket, object, fi)
		}, index)
	}

	// Wait for all the routines.
	mErrs := g.Wait()

	return reduceWriteQuorumErrs(ctx, mErrs, objectOpIgnoredErrs, getWriteQuorum(len(disks)))
}

// DeleteObjectTags - delete object tags from an existing object
func (er erasureObjects) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
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

// TransitionObject - transition object content to target tier.
func (er erasureObjects) TransitionObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	tgtClient, err := globalTierConfigMgr.getDriver(opts.Transition.Tier)
	if err != nil {
		return err
	}
	// Acquire a write lock before deleting the object.
	lk := er.NewNSLock(bucket, object)
	ctx, err = lk.GetLock(ctx, globalDeleteOperationTimeout)
	if err != nil {
		return err
	}
	defer lk.Unlock()

	fi, metaArr, onlineDisks, err := er.getObjectFileInfo(ctx, bucket, object, opts, false)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	if fi.Deleted {
		if opts.VersionID == "" {
			return toObjectErr(errFileNotFound, bucket, object)
		}
		// Make sure to return object info to provide extra information.
		return toObjectErr(errMethodNotAllowed, bucket, object)
	}
	// verify that the object queued for transition is identical to that on disk.
	if !opts.MTime.Equal(fi.ModTime) || !strings.EqualFold(opts.Transition.ETag, extractETag(fi.Metadata)) {
		return toObjectErr(errFileNotFound, bucket, object)
	}
	pr, pw := io.Pipe()
	go func() {
		err := er.getObjectWithFileInfo(ctx, bucket, object, 0, fi.Size, pw, fi, metaArr, onlineDisks)
		pw.CloseWithError(err)
	}()

	// if object already transitioned, return
	if fi.TransitionStatus == lifecycle.TransitionComplete {
		pr.Close()
		return nil
	}

	destObj, err := genTransitionObjName()
	if err != nil {
		pr.Close()
		return err
	}

	if err = tgtClient.Put(ctx, destObj, pr, fi.Size); err != nil {
		pr.Close()
		logger.LogIf(ctx, fmt.Errorf("Unable to transition %s/%s(%s) to %s tier: %w", bucket, object, opts.VersionID, opts.Transition.Tier, err))
		return err
	}
	pr.Close()
	fi.TransitionStatus = lifecycle.TransitionComplete
	fi.TransitionedObjName = destObj
	fi.TransitionTier = opts.Transition.Tier
	eventName := event.ObjectTransitionComplete

	storageDisks := er.getDisks()
	writeQuorum := len(storageDisks)/2 + 1
	if err = er.deleteObjectVersion(ctx, bucket, object, writeQuorum, fi, false); err != nil {
		eventName = event.ObjectTransitionFailed
	}
	for _, disk := range storageDisks {
		if disk != nil && disk.IsOnline() {
			continue
		}
		er.addPartial(bucket, object, opts.VersionID)
		break
	}
	// Notify object deleted event.
	sendEvent(eventArgs{
		EventName:  eventName,
		BucketName: bucket,
		Object: ObjectInfo{
			Name:      object,
			VersionID: opts.VersionID,
		},
		Host: "Internal: [ILM-Transition]",
	})
	return err
}

// RestoreTransitionedObject - restore transitioned object content locally on this cluster.
// This is similar to PostObjectRestore from AWS GLACIER
// storage class. When PostObjectRestore API is called, a temporary copy of the object
// is restored locally to the bucket on source cluster until the restore expiry date.
// The copy that was transitioned continues to reside in the transitioned tier.
func (er erasureObjects) RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	// Acquire a write lock before restoring the object.
	lk := er.NewNSLock(bucket, object)
	ctx, err := lk.GetLock(ctx, globalDeleteOperationTimeout)
	if err != nil {
		return err
	}
	defer lk.Unlock()
	oi, err := er.getObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return err
	}
	var rs *HTTPRangeSpec
	gr, err := getTransitionedObjectReader(ctx, bucket, object, rs, http.Header{}, oi, ObjectOptions{
		VersionID: oi.VersionID})
	if err != nil {
		return err
	}
	defer gr.Close()
	hashReader, err := hash.NewReader(gr, oi.Size, "", "", oi.Size)
	if err != nil {
		return err
	}
	pReader := NewPutObjReader(hashReader)
	ropts := putRestoreOpts(bucket, object, opts.Transition.RestoreRequest, oi)
	ropts.UserDefined[xhttp.AmzRestore] = fmt.Sprintf("ongoing-request=%t, expiry-date=%s", false, opts.Transition.RestoreExpiry.Format(http.TimeFormat))
	ropts.NoLock = true
	if _, err := er.PutObject(ctx, bucket, object, pReader, ropts); err != nil {
		return err
	}
	return nil
}
