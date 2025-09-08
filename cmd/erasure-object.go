// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"path"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/readahead"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/grid"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/mimedb"
	"github.com/minio/pkg/v3/sync/errgroup"
	"github.com/minio/sio"
)

// list all errors which can be ignored in object operations.
var objectOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errUnformattedDisk, errDiskOngoingReq)

// Object Operations

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
	if !dstOpts.NoAuditLog {
		auditObjectErasureSet(ctx, "CopyObject", dstObject, &er)
	}

	// This call shouldn't be used for anything other than metadata updates or adding self referential versions.
	if !srcInfo.metadataOnly {
		return oi, NotImplemented{}
	}

	if !dstOpts.NoLock {
		lk := er.NewNSLock(dstBucket, dstObject)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return oi, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}
	// Read metadata associated with the object from all disks.
	storageDisks := er.getDisks()

	var metaArr []FileInfo
	var errs []error

	// Read metadata associated with the object from all disks.
	if srcOpts.VersionID != "" {
		metaArr, errs = readAllFileInfo(ctx, storageDisks, "", srcBucket, srcObject, srcOpts.VersionID, true, false)
	} else {
		metaArr, errs = readAllXL(ctx, storageDisks, srcBucket, srcObject, true, false)
	}

	readQuorum, writeQuorum, err := objectQuorumFromMeta(ctx, metaArr, errs, er.defaultParityCount)
	if err != nil {
		if shouldCheckForDangling(err, errs, srcBucket) {
			_, derr := er.deleteIfDangling(context.Background(), srcBucket, srcObject, metaArr, errs, nil, srcOpts)
			if derr == nil {
				if srcOpts.VersionID != "" {
					err = errFileVersionNotFound
				} else {
					err = errFileNotFound
				}
			}
		}
		return ObjectInfo{}, toObjectErr(err, srcBucket, srcObject)
	}

	// List all online disks.
	onlineDisks, modTime, etag := listOnlineDisks(storageDisks, metaArr, errs, readQuorum)

	// Pick latest valid metadata.
	fi, err := pickValidFileInfo(ctx, metaArr, modTime, etag, readQuorum)
	if err != nil {
		return oi, toObjectErr(err, srcBucket, srcObject)
	}
	if fi.Deleted {
		if srcOpts.VersionID == "" {
			return oi, toObjectErr(errFileNotFound, srcBucket, srcObject)
		}
		return fi.ToObjectInfo(srcBucket, srcObject, srcOpts.Versioned || srcOpts.VersionSuspended), toObjectErr(errMethodNotAllowed, srcBucket, srcObject)
	}

	filterOnlineDisksInplace(fi, metaArr, onlineDisks)

	versionID := srcInfo.VersionID
	if srcInfo.versionOnly {
		versionID = dstOpts.VersionID
		// preserve destination versionId if specified.
		if versionID == "" {
			versionID = mustGetUUID()
			fi.IsLatest = true // we are creating a new version so this is latest.
		}
	}

	modTime = UTCNow() // We only preserve modTime if dstOpts.MTime is true.
	// in all other cases mtime is latest.

	fi.VersionID = versionID // set any new versionID we might have created
	fi.ModTime = modTime     // set modTime for the new versionID
	if !dstOpts.MTime.IsZero() {
		modTime = dstOpts.MTime
		fi.ModTime = dstOpts.MTime
	}
	// check inline before overwriting metadata.
	inlineData := fi.InlineData()

	fi.Metadata = srcInfo.UserDefined
	srcInfo.UserDefined["etag"] = srcInfo.ETag

	freeVersionID := fi.TierFreeVersionID()
	freeVersionMarker := fi.TierFreeVersion()

	// Update `xl.meta` content on each disks.
	for index := range metaArr {
		if metaArr[index].IsValid() {
			metaArr[index].ModTime = modTime
			metaArr[index].VersionID = versionID
			if !metaArr[index].InlineData() {
				// If the data is not inlined, we may end up incorrectly
				// inlining the data here, that leads to an inconsistent
				// situation where some objects are were not inlined
				// were now inlined, make sure to `nil` the Data such
				// that xl.meta is written as expected.
				metaArr[index].Data = nil
			}
			metaArr[index].Metadata = srcInfo.UserDefined
			// Preserve existing values
			if inlineData {
				metaArr[index].SetInlineData()
			}
			if freeVersionID != "" {
				metaArr[index].SetTierFreeVersionID(freeVersionID)
			}
			if freeVersionMarker {
				metaArr[index].SetTierFreeVersion()
			}
		}
	}

	// Write unique `xl.meta` for each disk.
	if _, err = writeUniqueFileInfo(ctx, onlineDisks, "", srcBucket, srcObject, metaArr, writeQuorum); err != nil {
		return oi, toObjectErr(err, srcBucket, srcObject)
	}

	return fi.ToObjectInfo(srcBucket, srcObject, srcOpts.Versioned || srcOpts.VersionSuspended), nil
}

// GetObjectNInfo - returns object info and an object
// Read(Closer). When err != nil, the returned reader is always nil.
func (er erasureObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "GetObject", object, &er)
	}

	var unlockOnDefer bool
	nsUnlocker := func() {}
	defer func() {
		if unlockOnDefer {
			nsUnlocker()
		}
	}()

	// Acquire lock
	if !opts.NoLock {
		lock := er.NewNSLock(bucket, object)
		lkctx, err := lock.GetRLock(ctx, globalOperationTimeout)
		if err != nil {
			return nil, err
		}
		ctx = lkctx.Context()

		// Release lock when the metadata is verified, and reader
		// is ready to be read.
		//
		// This is possible to be lock free because
		// - xl.meta for inlined objects has already read the data
		//   into memory, any mutation on xl.meta subsequently is
		//   inconsequential to the overall read operation.
		// - xl.meta metadata is still verified for quorum under lock()
		//   however writing the response doesn't need to serialize
		//   concurrent writers
		unlockOnDefer = true
		nsUnlocker = func() { lock.RUnlock(lkctx) }
	}

	fi, metaArr, onlineDisks, err := er.getObjectFileInfo(ctx, bucket, object, opts, true)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}

	objInfo := fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended)
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

	// Set NoDecryption for SSE-C objects and if replication request
	if crypto.SSEC.IsEncrypted(objInfo.UserDefined) && opts.ReplicationRequest {
		opts.NoDecryption = true
	}

	if objInfo.Size == 0 {
		if _, _, err := rs.GetOffsetLength(objInfo.Size); err != nil {
			// Make sure to return object info to provide extra information.
			return &GetObjectReader{
				ObjInfo: objInfo,
			}, err
		}

		// Zero byte objects don't even need to further initialize pipes etc.
		return NewGetObjectReaderFromReader(bytes.NewReader(nil), objInfo, opts)
	}

	if objInfo.IsRemote() {
		gr, err := getTransitionedObjectReader(ctx, bucket, object, rs, h, objInfo, opts)
		if err != nil {
			return nil, err
		}
		unlockOnDefer = false
		return gr.WithCleanupFuncs(nsUnlocker), nil
	}

	fn, off, length, err := NewGetObjectReader(rs, objInfo, opts, h)
	if err != nil {
		return nil, err
	}

	if unlockOnDefer {
		unlockOnDefer = fi.InlineData() || len(fi.Data) > 0
	}

	pr, pw := xioutil.WaitPipe()
	go func() {
		pw.CloseWithError(er.getObjectWithFileInfo(ctx, bucket, object, off, length, pw, fi, metaArr, onlineDisks))
	}()

	// Cleanup function to cause the go routine above to exit, in
	// case of incomplete read.
	pipeCloser := func() {
		pr.CloseWithError(nil)
	}

	if !unlockOnDefer {
		return fn(pr, h, pipeCloser, nsUnlocker)
	}

	return fn(pr, h, pipeCloser)
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
			if !metaArr[index].Erasure.Equal(fi.Erasure) {
				continue
			}
			checksumInfo := metaArr[index].Erasure.GetChecksumInfo(partNumber)
			partPath := pathJoin(object, metaArr[index].DataDir, fmt.Sprintf("part.%d", partNumber))
			readers[index] = newBitrotReader(disk, metaArr[index].Data, bucket, partPath, tillOffset,
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
				if errors.Is(err, errFileNotFound) || errors.Is(err, errFileCorrupt) {
					healOnce.Do(func() {
						globalMRFState.addPartialOp(PartialOperation{
							Bucket:     bucket,
							Object:     object,
							VersionID:  fi.VersionID,
							Queued:     time.Now(),
							SetIndex:   er.setIndex,
							PoolIndex:  er.poolIndex,
							BitrotScan: errors.Is(err, errFileCorrupt),
						})
					})
					// Healing is triggered and we have written
					// successfully the content to client for
					// the specific part, we should `nil` this error
					// and proceed forward, instead of throwing errors.
					err = nil
				}
			}
			if err != nil {
				return toObjectErr(err, bucket, object)
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

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (er erasureObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (info ObjectInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "GetObjectInfo", object, &er)
	}

	if !opts.NoLock {
		// Lock the object before reading.
		lk := er.NewNSLock(bucket, object)
		lkctx, err := lk.GetRLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.RUnlock(lkctx)
	}

	return er.getObjectInfo(ctx, bucket, object, opts)
}

func auditDanglingObjectDeletion(ctx context.Context, bucket, object, versionID string, tags map[string]string) {
	if len(logger.AuditTargets()) == 0 {
		return
	}

	opts := AuditLogOptions{
		Event:     "DeleteDanglingObject",
		Bucket:    bucket,
		Object:    object,
		VersionID: versionID,
		Tags:      tags,
	}

	auditLogInternal(ctx, opts)
}

func joinErrs(errs []error) string {
	var s string
	for i := range s {
		if s != "" {
			s += ","
		}
		if errs[i] == nil {
			s += "<nil>"
		} else {
			s += errs[i].Error()
		}
	}
	return s
}

func (er erasureObjects) deleteIfDangling(ctx context.Context, bucket, object string, metaArr []FileInfo, errs []error, dataErrsByPart map[int][]int, opts ObjectOptions) (FileInfo, error) {
	m, ok := isObjectDangling(metaArr, errs, dataErrsByPart)
	if !ok {
		// We only come here if we cannot figure out if the object
		// can be deleted safely, in such a scenario return ReadQuorum error.
		return FileInfo{}, errErasureReadQuorum
	}
	tags := make(map[string]string, 16)
	tags["set"] = strconv.Itoa(er.setIndex)
	tags["pool"] = strconv.Itoa(er.poolIndex)
	tags["merrs"] = joinErrs(errs)
	tags["derrs"] = fmt.Sprintf("%v", dataErrsByPart)
	if m.IsValid() {
		tags["sz"] = strconv.FormatInt(m.Size, 10)
		tags["mt"] = m.ModTime.Format(iso8601Format)
		tags["d:p"] = fmt.Sprintf("%d:%d", m.Erasure.DataBlocks, m.Erasure.ParityBlocks)
	} else {
		tags["invalid"] = "1"
		tags["d:p"] = fmt.Sprintf("%d:%d", er.setDriveCount-er.defaultParityCount, er.defaultParityCount)
	}

	// count the number of offline disks
	offline := 0
	for i := range len(errs) {
		var found bool
		switch {
		case errors.Is(errs[i], errDiskNotFound):
			found = true
		default:
			for p := range dataErrsByPart {
				if dataErrsByPart[p][i] == checkPartDiskNotFound {
					found = true
					break
				}
			}
		}
		if found {
			offline++
		}
	}
	if offline > 0 {
		tags["offline"] = strconv.Itoa(offline)
	}

	_, file, line, cok := runtime.Caller(1)
	if cok {
		tags["caller"] = fmt.Sprintf("%s:%d", file, line)
	}

	defer auditDanglingObjectDeletion(ctx, bucket, object, m.VersionID, tags)

	fi := FileInfo{
		VersionID: m.VersionID,
	}
	if opts.VersionID != "" {
		fi.VersionID = opts.VersionID
	}
	fi.SetTierFreeVersionID(mustGetUUID())
	disks := er.getDisks()
	g := errgroup.WithNErrs(len(disks))
	for index := range disks {
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			return disks[index].DeleteVersion(ctx, bucket, object, fi, false, DeleteOptions{})
		}, index)
	}

	for index, err := range g.Wait() {
		var errStr string
		if err != nil {
			errStr = err.Error()
		} else {
			errStr = "<nil>"
		}
		tags[fmt.Sprintf("ddisk-%d", index)] = errStr
	}

	return m, nil
}

func fileInfoFromRaw(ri RawFileInfo, bucket, object string, readData, inclFreeVers bool) (FileInfo, error) {
	return getFileInfo(ri.Buf, bucket, object, "", fileInfoOpts{
		Data:             readData,
		InclFreeVersions: inclFreeVers,
	})
}

func readAllRawFileInfo(ctx context.Context, disks []StorageAPI, bucket, object string, readData bool) ([]RawFileInfo, []error) {
	rawFileInfos := make([]RawFileInfo, len(disks))
	g := errgroup.WithNErrs(len(disks))
	for index := range disks {
		g.Go(func() (err error) {
			if disks[index] == nil {
				return errDiskNotFound
			}
			rf, err := disks[index].ReadXL(ctx, bucket, object, readData)
			if err != nil {
				return err
			}
			rawFileInfos[index] = rf
			return nil
		}, index)
	}

	return rawFileInfos, g.Wait()
}

func pickLatestQuorumFilesInfo(ctx context.Context, rawFileInfos []RawFileInfo, errs []error, bucket, object string, readData, inclFreeVers bool) ([]FileInfo, []error) {
	metadataArray := make([]*xlMetaV2, len(rawFileInfos))
	metaFileInfos := make([]FileInfo, len(rawFileInfos))
	metadataShallowVersions := make([][]xlMetaV2ShallowVersion, len(rawFileInfos))
	var v2bufs [][]byte
	if !readData {
		v2bufs = make([][]byte, len(rawFileInfos))
	}

	// Read `xl.meta` in parallel across disks.
	for index := range rawFileInfos {
		rf := rawFileInfos[index]
		if rf.Buf == nil {
			continue
		}
		if !readData {
			// Save the buffer so we can reuse it.
			v2bufs[index] = rf.Buf
		}

		var xl xlMetaV2
		if err := xl.LoadOrConvert(rf.Buf); err != nil {
			errs[index] = err
			continue
		}
		metadataArray[index] = &xl
		metaFileInfos[index] = FileInfo{}
	}

	for index := range metadataArray {
		if metadataArray[index] != nil {
			metadataShallowVersions[index] = metadataArray[index].versions
		}
	}

	readQuorum := (len(rawFileInfos) + 1) / 2
	meta := &xlMetaV2{versions: mergeXLV2Versions(readQuorum, false, 1, metadataShallowVersions...)}
	lfi, err := meta.ToFileInfo(bucket, object, "", inclFreeVers, true)
	if err != nil {
		for i := range errs {
			if errs[i] == nil {
				errs[i] = err
			}
		}
		return metaFileInfos, errs
	}
	if !lfi.IsValid() {
		for i := range errs {
			if errs[i] == nil {
				errs[i] = errFileCorrupt
			}
		}
		return metaFileInfos, errs
	}

	versionID := lfi.VersionID
	if versionID == "" {
		versionID = nullVersionID
	}

	for index := range metadataArray {
		if metadataArray[index] == nil {
			continue
		}

		// make sure to preserve this for diskmtime based healing bugfix.
		metaFileInfos[index], errs[index] = metadataArray[index].ToFileInfo(bucket, object, versionID, inclFreeVers, true)
		if errs[index] != nil {
			continue
		}

		if readData {
			metaFileInfos[index].Data = metadataArray[index].data.find(versionID)
		}
	}
	if !readData {
		for i := range v2bufs {
			metaDataPoolPut(v2bufs[i])
		}
	}

	// Return all the metadata.
	return metaFileInfos, errs
}

// Checking if an object is dangling costs some IOPS; hence implementing this function
// which decides which condition it is useful to check if an object is dangling
//
//	  errs: errors from reading xl.meta in all disks
//	   err: reduced errs
//	bucket: the object name in question
func shouldCheckForDangling(err error, errs []error, bucket string) bool {
	// Avoid data in .minio.sys for now
	if bucket == minioMetaBucket {
		return false
	}
	switch {
	// Check if we have a read quorum issue
	case errors.Is(err, errErasureReadQuorum):
		return true
	// Check if the object is non-existent on most disks but not all of them
	case (errors.Is(err, errFileNotFound) || errors.Is(err, errFileVersionNotFound)) && (countErrs(errs, nil) > 0):
		return true
	}
	return false
}

func readAllXL(ctx context.Context, disks []StorageAPI, bucket, object string, readData, inclFreeVers bool) ([]FileInfo, []error) {
	rawFileInfos, errs := readAllRawFileInfo(ctx, disks, bucket, object, readData)
	return pickLatestQuorumFilesInfo(ctx, rawFileInfos, errs, bucket, object, readData, inclFreeVers)
}

func (er erasureObjects) getObjectFileInfo(ctx context.Context, bucket, object string, opts ObjectOptions, readData bool) (FileInfo, []FileInfo, []StorageAPI, error) {
	rawArr := make([]RawFileInfo, er.setDriveCount)
	metaArr := make([]FileInfo, er.setDriveCount)
	errs := make([]error, er.setDriveCount)
	for i := range errs {
		errs[i] = errDiskOngoingReq
	}

	done := make(chan bool, er.setDriveCount)
	disks := er.getDisks()

	ropts := ReadOptions{
		ReadData:         readData,
		InclFreeVersions: opts.InclFreeVersions,
		Healing:          false,
	}

	mrfCheck := make(chan FileInfo)
	defer xioutil.SafeClose(mrfCheck)

	var rw sync.Mutex

	// Ask for all disks first;
	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		wg := sync.WaitGroup{}
		for i, disk := range disks {
			if disk == nil {
				done <- false
				continue
			}
			if !disk.IsOnline() {
				done <- false
				continue
			}
			wg.Add(1)
			go func(i int, disk StorageAPI) {
				defer wg.Done()

				var (
					fi  FileInfo
					rfi RawFileInfo
					err error
				)

				if opts.VersionID != "" {
					// Read a specific version ID
					fi, err = disk.ReadVersion(ctx, "", bucket, object, opts.VersionID, ropts)
				} else {
					// Read the latest version
					rfi, err = disk.ReadXL(ctx, bucket, object, readData)
					if err == nil {
						fi, err = fileInfoFromRaw(rfi, bucket, object, readData, opts.InclFreeVersions)
					}
				}

				rw.Lock()
				rawArr[i] = rfi
				metaArr[i], errs[i] = fi, err
				rw.Unlock()

				done <- err == nil
			}(i, disk)
		}

		wg.Wait()
		xioutil.SafeClose(done)

		fi, ok := <-mrfCheck
		if !ok {
			return
		}

		if fi.Deleted {
			return
		}

		// if one of the disk is offline, return right here no need
		// to attempt a heal on the object.
		if countErrs(errs, errDiskNotFound) > 0 {
			return
		}

		var missingBlocks int
		for i := range errs {
			if IsErr(errs[i],
				errFileNotFound,
				errFileVersionNotFound,
				errFileCorrupt,
			) {
				missingBlocks++
			}
		}

		// if missing metadata can be reconstructed, attempt to reconstruct.
		// additionally do not heal delete markers inline, let them be
		// healed upon regular heal process.
		if missingBlocks > 0 && missingBlocks < fi.Erasure.DataBlocks {
			globalMRFState.addPartialOp(PartialOperation{
				Bucket:    fi.Volume,
				Object:    fi.Name,
				VersionID: fi.VersionID,
				Queued:    time.Now(),
				SetIndex:  er.setIndex,
				PoolIndex: er.poolIndex,
			})
		}
	}()

	validResp := 0
	totalResp := 0

	// minDisks value is only to reduce the number of calls
	// to the disks; this value is not accurate because we do
	// not know the storage class of the object yet
	minDisks := 0
	if p := globalStorageClass.GetParityForSC(""); p > -1 {
		minDisks = er.setDriveCount - p
	} else {
		minDisks = er.setDriveCount - er.defaultParityCount
	}

	if minDisks == er.setDriveCount/2 {
		// when data and parity are same we must atleast
		// wait for response from 1 extra drive to avoid
		// split-brain.
		minDisks++
	}

	calcQuorum := func(metaArr []FileInfo, errs []error) (FileInfo, []FileInfo, []StorageAPI, time.Time, string, error) {
		readQuorum, _, err := objectQuorumFromMeta(ctx, metaArr, errs, er.defaultParityCount)
		if err != nil {
			return FileInfo{}, nil, nil, time.Time{}, "", err
		}
		if err := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum); err != nil {
			return FileInfo{}, nil, nil, time.Time{}, "", err
		}
		onlineDisks, modTime, etag := listOnlineDisks(disks, metaArr, errs, readQuorum)
		fi, err := pickValidFileInfo(ctx, metaArr, modTime, etag, readQuorum)
		if err != nil {
			return FileInfo{}, nil, nil, time.Time{}, "", err
		}

		onlineMeta := make([]FileInfo, len(metaArr))
		for i, disk := range onlineDisks {
			if disk != nil {
				onlineMeta[i] = metaArr[i]
			}
		}

		return fi, onlineMeta, onlineDisks, modTime, etag, nil
	}

	var (
		modTime     time.Time
		etag        string
		fi          FileInfo
		onlineMeta  []FileInfo
		onlineDisks []StorageAPI
		err         error
	)

	for success := range done {
		totalResp++
		if success {
			validResp++
		}

		if totalResp >= minDisks && opts.FastGetObjInfo {
			rw.Lock()
			ok := countErrs(errs, errFileNotFound) >= minDisks || countErrs(errs, errFileVersionNotFound) >= minDisks
			rw.Unlock()
			if ok {
				err = errFileNotFound
				if opts.VersionID != "" {
					err = errFileVersionNotFound
				}
				break
			}
		}

		if totalResp < er.setDriveCount {
			if !opts.FastGetObjInfo {
				continue
			}
			if validResp < minDisks {
				continue
			}
		}

		rw.Lock()
		// when its a versioned bucket and empty versionID - at totalResp == setDriveCount
		// we must use rawFileInfo to resolve versions to figure out the latest version.
		if opts.VersionID == "" && totalResp == er.setDriveCount {
			fi, onlineMeta, onlineDisks, modTime, etag, err = calcQuorum(pickLatestQuorumFilesInfo(ctx,
				rawArr, errs, bucket, object, readData, opts.InclFreeVersions))
		} else {
			fi, onlineMeta, onlineDisks, modTime, etag, err = calcQuorum(metaArr, errs)
		}
		rw.Unlock()
		if err == nil && (fi.InlineData() || len(fi.Data) > 0) {
			break
		}
	}

	if err != nil {
		// We can only look for dangling if we received all the responses, if we did
		// not we simply ignore it, since we can't tell for sure if its dangling object.
		if totalResp == er.setDriveCount && shouldCheckForDangling(err, errs, bucket) {
			_, derr := er.deleteIfDangling(context.Background(), bucket, object, metaArr, errs, nil, opts)
			if derr == nil {
				if opts.VersionID != "" {
					err = errFileVersionNotFound
				} else {
					err = errFileNotFound
				}
			}
		}
		// when we have insufficient read quorum and inconsistent metadata return
		// file not found, since we can't possibly have a way to recover this object
		// anyway.
		if v, ok := err.(InsufficientReadQuorum); ok && v.Type == RQInconsistentMeta {
			if opts.VersionID != "" {
				err = errFileVersionNotFound
			} else {
				err = errFileNotFound
			}
		}
		return fi, nil, nil, toObjectErr(err, bucket, object)
	}

	if !fi.Deleted && len(fi.Erasure.Distribution) != len(onlineDisks) {
		err := fmt.Errorf("unexpected file distribution (%v) from online disks (%v), looks like backend disks have been manually modified refusing to heal %s/%s(%s)",
			fi.Erasure.Distribution, onlineDisks, bucket, object, opts.VersionID)
		storageLogOnceIf(ctx, err, "get-object-file-info-manually-modified")
		return fi, nil, nil, toObjectErr(err, bucket, object, opts.VersionID)
	}

	filterOnlineDisksInplace(fi, onlineMeta, onlineDisks)
	for i := range onlineMeta {
		// verify metadata is valid, it has similar erasure info
		// as well as common modtime, if modtime is not possible
		// verify if it has common "etag" at least.
		if onlineMeta[i].IsValid() && onlineMeta[i].Erasure.Equal(fi.Erasure) {
			ok := onlineMeta[i].ModTime.Equal(modTime)
			if modTime.IsZero() || modTime.Equal(timeSentinel) {
				ok = etag != "" && etag == fi.Metadata["etag"]
			}
			if ok {
				continue
			}
		} // in all other cases metadata is corrupt, do not read from it.

		onlineMeta[i] = FileInfo{}
		onlineDisks[i] = nil
	}

	select {
	case mrfCheck <- fi.ShallowCopy():
	case <-ctx.Done():
		return fi, onlineMeta, onlineDisks, toObjectErr(ctx.Err(), bucket, object)
	}

	return fi, onlineMeta, onlineDisks, nil
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (er erasureObjects) getObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	fi, _, _, err := er.getObjectFileInfo(ctx, bucket, object, opts, false)
	if err != nil {
		return objInfo, toObjectErr(err, bucket, object)
	}
	objInfo = fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended)
	if fi.Deleted {
		if opts.VersionID == "" || opts.DeleteMarker {
			return objInfo, toObjectErr(errFileNotFound, bucket, object)
		}
		// Make sure to return object info to provide extra information.
		return objInfo, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	return objInfo, nil
}

// getObjectInfoAndQuorum - wrapper for reading object metadata and constructs ObjectInfo, additionally returns write quorum for the object.
func (er erasureObjects) getObjectInfoAndQuorum(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, wquorum int, err error) {
	fi, _, _, err := er.getObjectFileInfo(ctx, bucket, object, opts, false)
	if err != nil {
		return objInfo, er.defaultWQuorum(), toObjectErr(err, bucket, object)
	}

	wquorum = fi.WriteQuorum(er.defaultWQuorum())

	objInfo = fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended)
	if !fi.VersionPurgeStatus().Empty() && opts.VersionID != "" {
		// Make sure to return object info to provide extra information.
		return objInfo, wquorum, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	if fi.Deleted {
		if opts.VersionID == "" || opts.DeleteMarker {
			return objInfo, wquorum, toObjectErr(errFileNotFound, bucket, object)
		}
		// Make sure to return object info to provide extra information.
		return objInfo, wquorum, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	return objInfo, wquorum, nil
}

// Similar to rename but renames data from srcEntry to dstEntry at dataDir
func renameData(ctx context.Context, disks []StorageAPI, srcBucket, srcEntry string, metadata []FileInfo, dstBucket, dstEntry string, writeQuorum int) ([]StorageAPI, []byte, string, error) {
	g := errgroup.WithNErrs(len(disks))

	fvID := mustGetUUID()
	for index := range disks {
		metadata[index].SetTierFreeVersionID(fvID)
	}

	diskVersions := make([][]byte, len(disks))
	dataDirs := make([]string, len(disks))
	// Rename file on all underlying storage disks.
	for index := range disks {
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}

			// Pick one FileInfo for a disk at index.
			fi := metadata[index]
			// Assign index when index is initialized
			if fi.Erasure.Index == 0 {
				fi.Erasure.Index = index + 1
			}

			if !fi.IsValid() {
				return errFileCorrupt
			}
			resp, err := disks[index].RenameData(ctx, srcBucket, srcEntry, fi, dstBucket, dstEntry, RenameOptions{})
			if err != nil {
				return err
			}
			diskVersions[index] = resp.Sign
			dataDirs[index] = resp.OldDataDir
			return nil
		}, index)
	}

	// Wait for all renames to finish.
	errs := g.Wait()

	err := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if err != nil {
		dg := errgroup.WithNErrs(len(disks))
		for index, nerr := range errs {
			if nerr != nil {
				continue
			}
			index := index
			// When we are going to return error, attempt to delete success
			// on some of the drives, if we cannot we do not have to notify
			// caller this dangling object will be now scheduled to be removed
			// via active healing.
			dg.Go(func() error {
				return disks[index].DeleteVersion(context.Background(), dstBucket, dstEntry, metadata[index], false, DeleteOptions{
					UndoWrite:  true,
					OldDataDir: dataDirs[index],
				})
			}, index)
		}
		dg.Wait()
	}
	var dataDir string
	var versions []byte
	if err == nil {
		versions = reduceCommonVersions(diskVersions, writeQuorum)
		for index, dversions := range diskVersions {
			if errs[index] != nil {
				continue
			}
			if !bytes.Equal(dversions, versions) {
				if len(dversions) > len(versions) {
					versions = dversions
				}
				break
			}
		}
		dataDir = reduceCommonDataDir(dataDirs, writeQuorum)
	}

	// We can safely allow RenameData errors up to len(er.getDisks()) - writeQuorum
	// otherwise return failure.
	return evalDisks(disks, errs), versions, dataDir, err
}

func (er erasureObjects) putMetacacheObject(ctx context.Context, key string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	data := r.Reader

	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	storageDisks := er.getDisks()
	// Get parity and data drive count based on storage class metadata
	parityDrives := globalStorageClass.GetParityForSC(opts.UserDefined[xhttp.AmzStorageClass])
	if parityDrives < 0 {
		parityDrives = er.defaultParityCount
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
		bugLogIf(ctx, errInvalidArgument, logger.ErrorKind)
		return ObjectInfo{}, toObjectErr(errInvalidArgument)
	}

	// Initialize parts metadata
	partsMetadata := make([]FileInfo, len(storageDisks))

	fi := newFileInfo(pathJoin(minioMetaBucket, key), dataDrives, parityDrives)
	fi.DataDir = mustGetUUID()

	// Initialize erasure metadata.
	for index := range partsMetadata {
		partsMetadata[index] = fi
	}

	// Order disks according to erasure distribution
	var onlineDisks []StorageAPI
	onlineDisks, partsMetadata = shuffleDisksAndPartsMetadata(storageDisks, partsMetadata, fi)

	erasure, err := NewErasure(ctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, minioMetaBucket, key)
	}

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch size := data.Size(); {
	case size == 0:
		buffer = make([]byte, 1) // Allocate at least a byte to reach EOF
	case size >= fi.Erasure.BlockSize:
		buffer = globalBytePoolCap.Load().Get()
		defer globalBytePoolCap.Load().Put(buffer)
	case size < fi.Erasure.BlockSize:
		// No need to allocate fully blockSizeV1 buffer if the incoming data is smaller.
		buffer = make([]byte, size, 2*size+int64(fi.Erasure.ParityBlocks+fi.Erasure.DataBlocks-1))
	}

	if len(buffer) > int(fi.Erasure.BlockSize) {
		buffer = buffer[:fi.Erasure.BlockSize]
	}

	writers := make([]io.Writer, len(onlineDisks))
	inlineBuffers := make([]*bytes.Buffer, len(onlineDisks))
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}
		if disk.IsOnline() {
			buf := grid.GetByteBufferCap(int(erasure.ShardFileSize(data.Size())) + 64)
			inlineBuffers[i] = bytes.NewBuffer(buf[:0])
			defer grid.PutByteBuffer(buf)
			writers[i] = newStreamingBitrotWriterBuffer(inlineBuffers[i], DefaultBitrotAlgorithm, erasure.ShardSize())
		}
	}

	n, erasureErr := erasure.Encode(ctx, data, writers, buffer, writeQuorum)
	closeErrs := closeBitrotWriters(writers)
	if erasureErr != nil {
		return ObjectInfo{}, toObjectErr(erasureErr, minioMetaBucket, key)
	}

	if closeErr := reduceWriteQuorumErrs(ctx, closeErrs, objectOpIgnoredErrs, writeQuorum); closeErr != nil {
		return ObjectInfo{}, toObjectErr(closeErr, minioMetaBucket, key)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if n < data.Size() {
		return ObjectInfo{}, IncompleteBody{Bucket: minioMetaBucket, Object: key}
	}
	var index []byte
	if opts.IndexCB != nil {
		index = opts.IndexCB()
	}

	modTime := UTCNow()

	for i, w := range writers {
		if w == nil {
			// Make sure to avoid writing to disks which we couldn't complete in erasure.Encode()
			onlineDisks[i] = nil
			continue
		}
		partsMetadata[i].Data = inlineBuffers[i].Bytes()
		partsMetadata[i].AddObjectPart(1, "", n, data.ActualSize(), modTime, index, nil)
	}

	// Fill all the necessary metadata.
	// Update `xl.meta` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Size = n
		partsMetadata[index].Fresh = true
		partsMetadata[index].ModTime = modTime
		partsMetadata[index].Metadata = opts.UserDefined
	}

	// Set an additional header when data is inlined.
	for index := range partsMetadata {
		partsMetadata[index].SetInlineData()
	}

	for i := range len(onlineDisks) {
		if onlineDisks[i] != nil && onlineDisks[i].IsOnline() {
			// Object info is the same in all disks, so we can pick
			// the first meta from online disk
			fi = partsMetadata[i]
			break
		}
	}

	if _, err = writeUniqueFileInfo(ctx, onlineDisks, "", minioMetaBucket, key, partsMetadata, writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, minioMetaBucket, key)
	}

	return fi.ToObjectInfo(minioMetaBucket, key, opts.Versioned || opts.VersionSuspended), nil
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
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "PutObject", object, &er)
	}

	data := r.Reader

	if opts.CheckPrecondFn != nil {
		if !opts.NoLock {
			ns := er.NewNSLock(bucket, object)
			lkctx, err := ns.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return ObjectInfo{}, err
			}
			ctx = lkctx.Context()
			defer ns.Unlock(lkctx)
			opts.NoLock = true
		}

		obj, err := er.getObjectInfo(ctx, bucket, object, opts)
		if err == nil && opts.CheckPrecondFn(obj) {
			return objInfo, PreConditionFailed{}
		}
		if err != nil && !isErrVersionNotFound(err) && !isErrObjectNotFound(err) && !isErrReadQuorum(err) {
			return objInfo, err
		}

		// if object doesn't exist return error for If-Match conditional requests
		// If-None-Match should be allowed to proceed for non-existent objects
		if err != nil && opts.HasIfMatch && (isErrObjectNotFound(err) || isErrVersionNotFound(err)) {
			return objInfo, err
		}
	}

	// Validate input data size and it can never be less than -1.
	if data.Size() < -1 {
		bugLogIf(ctx, errInvalidArgument, logger.ErrorKind)
		return ObjectInfo{}, toObjectErr(errInvalidArgument)
	}

	userDefined := cloneMSS(opts.UserDefined)

	storageDisks := er.getDisks()

	// Get parity and data drive count based on storage class metadata
	parityDrives := globalStorageClass.GetParityForSC(userDefined[xhttp.AmzStorageClass])
	if parityDrives < 0 {
		parityDrives = er.defaultParityCount
	}
	if opts.MaxParity {
		parityDrives = len(storageDisks) / 2
	}
	if !opts.MaxParity && globalStorageClass.AvailabilityOptimized() {
		// If we have offline disks upgrade the number of erasure codes for this object.
		parityOrig := parityDrives

		var offlineDrives int
		for _, disk := range storageDisks {
			if disk == nil || !disk.IsOnline() {
				parityDrives++
				offlineDrives++
				continue
			}
		}

		if offlineDrives >= (len(storageDisks)+1)/2 {
			// if offline drives are more than 50% of the drives
			// we have no quorum, we shouldn't proceed just
			// fail at that point.
			return ObjectInfo{}, toObjectErr(errErasureWriteQuorum, bucket, object)
		}

		if parityDrives >= len(storageDisks)/2 {
			parityDrives = len(storageDisks) / 2
		}

		if parityOrig != parityDrives {
			userDefined[minIOErasureUpgraded] = strconv.Itoa(parityOrig) + "->" + strconv.Itoa(parityDrives)
		}
	}
	dataDrives := len(storageDisks) - parityDrives

	// we now know the number of blocks this object needs for data and parity.
	// writeQuorum is dataBlocks + 1
	writeQuorum := dataDrives
	if dataDrives == parityDrives {
		writeQuorum++
	}

	// Initialize parts metadata
	partsMetadata := make([]FileInfo, len(storageDisks))

	fi := newFileInfo(pathJoin(bucket, object), dataDrives, parityDrives)
	fi.VersionID = opts.VersionID
	if opts.Versioned && fi.VersionID == "" {
		fi.VersionID = mustGetUUID()
	}

	fi.DataDir = mustGetUUID()
	if ckSum := userDefined[ReplicationSsecChecksumHeader]; ckSum != "" {
		if v, err := base64.StdEncoding.DecodeString(ckSum); err == nil {
			fi.Checksum = v
		}
		delete(userDefined, ReplicationSsecChecksumHeader)
	}
	uniqueID := mustGetUUID()
	tempObj := uniqueID

	// Initialize erasure metadata.
	for index := range partsMetadata {
		partsMetadata[index] = fi
	}

	// Order disks according to erasure distribution
	var onlineDisks []StorageAPI
	onlineDisks, partsMetadata = shuffleDisksAndPartsMetadata(storageDisks, partsMetadata, fi)

	erasure, err := NewErasure(ctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch size := data.Size(); {
	case size == 0:
		buffer = make([]byte, 1) // Allocate at least a byte to reach EOF
	case size >= fi.Erasure.BlockSize || size == -1:
		buffer = globalBytePoolCap.Load().Get()
		defer globalBytePoolCap.Load().Put(buffer)
	case size < fi.Erasure.BlockSize:
		// No need to allocate fully blockSizeV1 buffer if the incoming data is smaller.
		buffer = make([]byte, size, 2*size+int64(fi.Erasure.ParityBlocks+fi.Erasure.DataBlocks-1))
	}

	if len(buffer) > int(fi.Erasure.BlockSize) {
		buffer = buffer[:fi.Erasure.BlockSize]
	}

	partName := "part.1"
	tempErasureObj := pathJoin(uniqueID, fi.DataDir, partName)

	defer er.deleteAll(context.Background(), minioMetaTmpBucket, tempObj)

	var inlineBuffers []*bytes.Buffer
	if globalStorageClass.ShouldInline(erasure.ShardFileSize(data.ActualSize()), opts.Versioned) {
		inlineBuffers = make([]*bytes.Buffer, len(onlineDisks))
	}

	shardFileSize := erasure.ShardFileSize(data.Size())
	writers := make([]io.Writer, len(onlineDisks))
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}

		if !disk.IsOnline() {
			continue
		}

		if len(inlineBuffers) > 0 {
			buf := grid.GetByteBufferCap(int(shardFileSize) + 64)
			inlineBuffers[i] = bytes.NewBuffer(buf[:0])
			defer grid.PutByteBuffer(buf)
			writers[i] = newStreamingBitrotWriterBuffer(inlineBuffers[i], DefaultBitrotAlgorithm, erasure.ShardSize())
			continue
		}

		writers[i] = newBitrotWriter(disk, bucket, minioMetaTmpBucket, tempErasureObj, shardFileSize, DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	toEncode := io.Reader(data)
	if data.Size() >= bigFileThreshold {
		// We use 2 buffers, so we always have a full buffer of input.
		pool := globalBytePoolCap.Load()
		bufA := pool.Get()
		bufB := pool.Get()
		defer pool.Put(bufA)
		defer pool.Put(bufB)
		ra, err := readahead.NewReaderBuffer(data, [][]byte{bufA[:fi.Erasure.BlockSize], bufB[:fi.Erasure.BlockSize]})
		if err == nil {
			toEncode = ra
			defer ra.Close()
		}
		bugLogIf(ctx, err)
	}
	n, erasureErr := erasure.Encode(ctx, toEncode, writers, buffer, writeQuorum)
	closeErrs := closeBitrotWriters(writers)
	if erasureErr != nil {
		return ObjectInfo{}, toObjectErr(erasureErr, bucket, object)
	}

	if closeErr := reduceWriteQuorumErrs(ctx, closeErrs, objectOpIgnoredErrs, writeQuorum); closeErr != nil {
		return ObjectInfo{}, toObjectErr(closeErr, bucket, object)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if n < data.Size() {
		return ObjectInfo{}, IncompleteBody{Bucket: bucket, Object: object}
	}

	var compIndex []byte
	if opts.IndexCB != nil {
		compIndex = opts.IndexCB()
	}

	modTime := opts.MTime
	if opts.MTime.IsZero() {
		modTime = UTCNow()
	}

	kind, encrypted := crypto.IsEncrypted(userDefined)
	actualSize := data.ActualSize()
	if actualSize < 0 {
		compressed := fi.IsCompressed()
		switch {
		case compressed:
			// ... nothing changes for compressed stream.
			// if actualSize is -1 we have no known way to
			// determine what is the actualSize.
		case encrypted:
			decSize, err := sio.DecryptedSize(uint64(n))
			if err == nil {
				actualSize = int64(decSize)
			}
		default:
			actualSize = n
		}
	}
	// If ServerSideChecksum is wanted for this object, it takes precedence
	// over opts.WantChecksum.
	if opts.WantServerSideChecksumType.IsSet() {
		serverSideChecksum := r.RawServerSideChecksumResult()
		if serverSideChecksum != nil {
			fi.Checksum = serverSideChecksum.AppendTo(nil, nil)
			if opts.EncryptFn != nil {
				fi.Checksum = opts.EncryptFn("object-checksum", fi.Checksum)
			}
		}
	} else if fi.Checksum == nil && opts.WantChecksum != nil {
		// Trailing headers checksums should now be filled.
		fi.Checksum = opts.WantChecksum.AppendTo(nil, nil)
		if opts.EncryptFn != nil {
			fi.Checksum = opts.EncryptFn("object-checksum", fi.Checksum)
		}
	}
	for i, w := range writers {
		if w == nil {
			onlineDisks[i] = nil
			continue
		}
		if len(inlineBuffers) > 0 && inlineBuffers[i] != nil {
			partsMetadata[i].Data = inlineBuffers[i].Bytes()
		} else {
			partsMetadata[i].Data = nil
		}
		// No need to add checksum to part. We already have it on the object.
		partsMetadata[i].AddObjectPart(1, "", n, actualSize, modTime, compIndex, nil)
		partsMetadata[i].Versioned = opts.Versioned || opts.VersionSuspended
		partsMetadata[i].Checksum = fi.Checksum
	}

	userDefined["etag"] = r.MD5CurrentHexString()
	if opts.PreserveETag != "" {
		if !opts.ReplicationRequest {
			userDefined["etag"] = opts.PreserveETag
		} else if kind != crypto.S3 {
			// if we have a replication request
			// and SSE-S3 is specified do not preserve
			// the incoming etag.
			userDefined["etag"] = opts.PreserveETag
		}
	}

	// Guess content-type from the extension if possible.
	if userDefined["content-type"] == "" {
		userDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
	}

	// if storageClass is standard no need to save it as part of metadata.
	if userDefined[xhttp.AmzStorageClass] == storageclass.STANDARD {
		delete(userDefined, xhttp.AmzStorageClass)
	}

	// Fill all the necessary metadata.
	// Update `xl.meta` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Metadata = userDefined
		partsMetadata[index].Size = n
		partsMetadata[index].ModTime = modTime
		if len(inlineBuffers) > 0 {
			partsMetadata[index].SetInlineData()
		}
		if opts.DataMovement {
			partsMetadata[index].SetDataMov()
		}
	}

	if !opts.NoLock {
		lk := er.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

	// Rename the successfully written temporary object to final location.
	onlineDisks, versions, oldDataDir, err := renameData(ctx, onlineDisks, minioMetaTmpBucket, tempObj, partsMetadata, bucket, object, writeQuorum)
	if err != nil {
		if errors.Is(err, errFileNotFound) {
			// An in-quorum errFileNotFound means that client stream
			// prematurely closed and we do not find any xl.meta or
			// part.1's - in such a scenario we must return as if client
			// disconnected. This means that erasure.Encode() CreateFile()
			// did not do anything.
			return ObjectInfo{}, IncompleteBody{Bucket: bucket, Object: object}
		}
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if err = er.commitRenameDataDir(ctx, bucket, object, oldDataDir, onlineDisks, writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	for i := range len(onlineDisks) {
		if onlineDisks[i] != nil && onlineDisks[i].IsOnline() {
			// Object info is the same in all disks, so we can pick
			// the first meta from online disk
			fi = partsMetadata[i]
			break
		}
	}

	// For speedtest objects do not attempt to heal them.
	if !opts.Speedtest {
		// When there is versions disparity we are healing
		// the content implicitly for all versions, we can
		// avoid triggering another MRF heal for offline drives.
		if len(versions) == 0 {
			// Whether a disk was initially or becomes offline
			// during this upload, send it to the MRF list.
			for i := range len(onlineDisks) {
				if onlineDisks[i] != nil && onlineDisks[i].IsOnline() {
					continue
				}

				er.addPartial(bucket, object, fi.VersionID)
				break
			}
		} else {
			globalMRFState.addPartialOp(PartialOperation{
				Bucket:    bucket,
				Object:    object,
				Queued:    time.Now(),
				Versions:  versions,
				SetIndex:  er.setIndex,
				PoolIndex: er.poolIndex,
			})
		}
	}

	fi.ReplicationState = opts.PutReplicationState()

	// we are adding a new version to this object under the namespace lock, so this is the latest version.
	fi.IsLatest = true

	return fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended), nil
}

func (er erasureObjects) deleteObjectVersion(ctx context.Context, bucket, object string, fi FileInfo, forceDelMarker bool) error {
	disks := er.getDisks()
	// Assume (N/2 + 1) quorum for Delete()
	// this is a theoretical assumption such that
	// for delete's we do not need to honor storage
	// class for objects that have reduced quorum
	// due to storage class - this only needs to be honored
	// for Read() requests alone that we already do.
	writeQuorum := len(disks)/2 + 1

	g := errgroup.WithNErrs(len(disks))
	for index := range disks {
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			return disks[index].DeleteVersion(ctx, bucket, object, fi, forceDelMarker, DeleteOptions{})
		}, index)
	}
	// return errors if any during deletion
	return reduceWriteQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, writeQuorum)
}

// DeleteObjects deletes objects/versions in bulk, this function will still automatically split objects list
// into smaller bulks if some object names are found to be duplicated in the delete list, splitting
// into smaller bulks will avoid holding twice the write lock of the duplicated object names.
func (er erasureObjects) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	if !opts.NoAuditLog {
		for _, obj := range objects {
			auditObjectErasureSet(ctx, "DeleteObjects", obj.ObjectName, &er)
		}
	}

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
		writeQuorums[i] = len(storageDisks)/2 + 1
	}

	versionsMap := make(map[string]FileInfoVersions, len(objects))
	for i := range objects {
		// Construct the FileInfo data that needs to be preserved on the disk.
		vr := FileInfo{
			Name:             objects[i].ObjectName,
			VersionID:        objects[i].VersionID,
			ReplicationState: objects[i].ReplicationState(),
			// save the index to set correct error at this index.
			Idx: i,
		}
		vr.SetTierFreeVersionID(mustGetUUID())
		// VersionID is not set means delete is not specific about
		// any version, look for if the bucket is versioned or not.
		if objects[i].VersionID == "" {
			// MinIO extension to bucket version configuration
			suspended := opts.VersionSuspended
			versioned := opts.Versioned
			if opts.PrefixEnabledFn != nil {
				versioned = opts.PrefixEnabledFn(objects[i].ObjectName)
			}
			if versioned || suspended {
				// Bucket is versioned and no version was explicitly
				// mentioned for deletes, create a delete marker instead.
				vr.ModTime = UTCNow()
				vr.Deleted = true
				// Versioning suspended means that we add a `null` version
				// delete marker, if not add a new version for this delete
				// marker.
				if versioned {
					vr.VersionID = mustGetUUID()
				}
			}
		}
		// De-dup same object name to collect multiple versions for same object.
		v, ok := versionsMap[objects[i].ObjectName]
		if ok {
			v.Versions = append(v.Versions, vr)
		} else {
			v = FileInfoVersions{
				Name:     vr.Name,
				Versions: []FileInfo{vr},
			}
		}
		if vr.Deleted {
			dobjects[i] = DeletedObject{
				DeleteMarker:          vr.Deleted,
				DeleteMarkerVersionID: vr.VersionID,
				DeleteMarkerMTime:     DeleteMarkerMTime{vr.ModTime},
				ObjectName:            vr.Name,
				ReplicationState:      vr.ReplicationState,
			}
		} else {
			dobjects[i] = DeletedObject{
				ObjectName:       vr.Name,
				VersionID:        vr.VersionID,
				ReplicationState: vr.ReplicationState,
			}
		}
		versionsMap[objects[i].ObjectName] = v
	}

	dedupVersions := make([]FileInfoVersions, 0, len(versionsMap))
	for _, fivs := range versionsMap {
		// Removal of existing versions and adding a delete marker in the same
		// request is supported. At the same time, we cannot allow adding
		// two delete markers on top of any object. To avoid this situation,
		// we will sort deletions to execute existing deletion first,
		// then add only one delete marker if requested
		sort.SliceStable(fivs.Versions, func(i, j int) bool {
			return !fivs.Versions[i].Deleted
		})
		if idx := slices.IndexFunc(fivs.Versions, func(fi FileInfo) bool {
			return fi.Deleted
		}); idx > -1 {
			fivs.Versions = fivs.Versions[:idx+1]
		}
		dedupVersions = append(dedupVersions, fivs)
	}

	// Initialize list of errors.
	delObjErrs := make([][]error, len(storageDisks))

	var wg sync.WaitGroup
	// Remove versions in bulk for each disk
	for index, disk := range storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			delObjErrs[index] = make([]error, len(objects))
			if disk == nil {
				for i := range objects {
					delObjErrs[index][i] = errDiskNotFound
				}
				return
			}
			errs := disk.DeleteVersions(ctx, bucket, dedupVersions, DeleteOptions{})
			for i, err := range errs {
				if err == nil {
					continue
				}
				for _, v := range dedupVersions[i].Versions {
					delObjErrs[index][v.Idx] = err
				}
			}
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
		if err == nil {
			dobjects[objIndex].found = true
		} else if isErrVersionNotFound(err) || isErrObjectNotFound(err) {
			if !dobjects[objIndex].DeleteMarker {
				err = nil
			}
		}
		if objects[objIndex].VersionID != "" {
			errs[objIndex] = toObjectErr(err, bucket, objects[objIndex].ObjectName, objects[objIndex].VersionID)
		} else {
			errs[objIndex] = toObjectErr(err, bucket, objects[objIndex].ObjectName)
		}
	}

	// Check failed deletes across multiple objects
	for i, dobj := range dobjects {
		// This object errored, we should attempt a heal just in case.
		if errs[i] != nil && !isErrVersionNotFound(errs[i]) && !isErrObjectNotFound(errs[i]) {
			// all other direct versionId references we should
			// ensure no dangling file is left over.
			er.addPartial(bucket, dobj.ObjectName, dobj.VersionID)
			continue
		}

		// Check if there is any offline disk and add it to the MRF list
		for _, disk := range storageDisks {
			if disk != nil && disk.IsOnline() {
				// Skip attempted heal on online disks.
				continue
			}

			// all other direct versionId references we should
			// ensure no dangling file is left over.
			er.addPartial(bucket, dobj.ObjectName, dobj.VersionID)
			break
		}
	}

	return dobjects, errs
}

func (er erasureObjects) commitRenameDataDir(ctx context.Context, bucket, object, dataDir string, onlineDisks []StorageAPI, writeQuorum int) error {
	if dataDir == "" {
		return nil
	}
	g := errgroup.WithNErrs(len(onlineDisks))
	for index := range onlineDisks {
		g.Go(func() error {
			if onlineDisks[index] == nil {
				return nil
			}
			return onlineDisks[index].Delete(ctx, bucket, pathJoin(object, dataDir), DeleteOptions{
				Recursive: true,
			})
		}, index)
	}

	return reduceWriteQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, writeQuorum)
}

func (er erasureObjects) deletePrefix(ctx context.Context, bucket, prefix string) error {
	disks := er.getDisks()
	// Assume (N/2 + 1) quorum for Delete()
	// this is a theoretical assumption such that
	// for delete's we do not need to honor storage
	// class for objects that have reduced quorum
	// due to storage class - this only needs to be honored
	// for Read() requests alone that we already do.
	writeQuorum := len(disks)/2 + 1

	g := errgroup.WithNErrs(len(disks))
	for index := range disks {
		g.Go(func() error {
			if disks[index] == nil {
				return nil
			}
			return disks[index].Delete(ctx, bucket, prefix, DeleteOptions{
				Recursive: true,
				Immediate: true,
			})
		}, index)
	}

	// return errors if any during deletion
	return reduceWriteQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, writeQuorum)
}

// DeleteObject - deletes an object, this call doesn't necessary reply
// any error as it is not necessary for the handler to reply back a
// response to the client request.
func (er erasureObjects) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "DeleteObject", object, &er)
	}

	var lc *lifecycle.Lifecycle
	var rcfg lock.Retention
	var replcfg *replication.Config
	if opts.Expiration.Expire {
		// Check if the current bucket has a configured lifecycle policy
		lc, err = globalLifecycleSys.Get(bucket)
		if err != nil && !errors.Is(err, BucketLifecycleNotFound{Bucket: bucket}) {
			return objInfo, err
		}
		rcfg, err = globalBucketObjectLockSys.Get(bucket)
		if err != nil {
			return objInfo, err
		}
		replcfg, err = getReplicationConfig(ctx, bucket)
		if err != nil {
			return objInfo, err
		}
	}

	// expiration attempted on a bucket with no lifecycle
	// rules shall be rejected.
	if lc == nil && opts.Expiration.Expire {
		if opts.VersionID != "" {
			return objInfo, VersionNotFound{
				Bucket:    bucket,
				Object:    object,
				VersionID: opts.VersionID,
			}
		}
		return objInfo, ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}

	if opts.DeletePrefix {
		if opts.Expiration.Expire {
			// Expire all versions expiration must still verify the state() on disk
			// via a getObjectInfo() call as follows, any read quorum issues we
			// must not proceed further for safety reasons. attempt a MRF heal
			// while we see such quorum errors.
			goi, _, gerr := er.getObjectInfoAndQuorum(ctx, bucket, object, opts)
			if gerr != nil && goi.Name == "" {
				if _, ok := gerr.(InsufficientReadQuorum); ok {
					// Add an MRF heal for next time.
					er.addPartial(bucket, object, opts.VersionID)

					return objInfo, InsufficientWriteQuorum{}
				}
				return objInfo, gerr
			}

			// Add protection and re-verify the ILM rules for qualification
			// based on the latest objectInfo and see if the object still
			// qualifies for deletion.
			if gerr == nil {
				var isErr bool
				evt := evalActionFromLifecycle(ctx, *lc, rcfg, replcfg, goi)
				switch evt.Action {
				case lifecycle.DeleteAllVersionsAction, lifecycle.DelMarkerDeleteAllVersionsAction:
					// opts.DeletePrefix is used only in the above lifecycle Expiration actions.
				default:
					// object has been modified since lifecycle action was previously evaluated
					isErr = true
				}
				if isErr {
					if goi.VersionID != "" {
						return goi, VersionNotFound{
							Bucket:    bucket,
							Object:    object,
							VersionID: goi.VersionID,
						}
					}
					return goi, ObjectNotFound{
						Bucket: bucket,
						Object: object,
					}
				}
			}
		} // Delete marker and any latest that qualifies shall be expired permanently.

		return ObjectInfo{}, toObjectErr(er.deletePrefix(ctx, bucket, object), bucket, object)
	}

	storageDisks := er.getDisks()
	versionFound := true
	objInfo = ObjectInfo{VersionID: opts.VersionID} // version id needed in Delete API response.
	goi, _, gerr := er.getObjectInfoAndQuorum(ctx, bucket, object, opts)
	tryDel := false
	if gerr != nil && goi.Name == "" {
		if _, ok := gerr.(InsufficientReadQuorum); ok {
			if opts.Versioned || opts.VersionSuspended || countOnlineDisks(storageDisks) < len(storageDisks)/2+1 {
				// Add an MRF heal for next time.
				er.addPartial(bucket, object, opts.VersionID)
				return objInfo, InsufficientWriteQuorum{}
			}
			tryDel = true // only for unversioned objects if there is write quorum
		}
		// For delete marker replication, versionID being replicated will not exist on disk
		if opts.DeleteMarker {
			versionFound = false
		} else if !tryDel {
			return objInfo, gerr
		}
	}

	if opts.EvalMetadataFn != nil {
		dsc, err := opts.EvalMetadataFn(&goi, gerr)
		if err != nil {
			return ObjectInfo{}, err
		}
		if dsc.ReplicateAny() {
			opts.SetDeleteReplicationState(dsc, opts.VersionID)
			goi.replicationDecision = opts.DeleteReplication.ReplicateDecisionStr
		}
	}

	if opts.EvalRetentionBypassFn != nil {
		if err := opts.EvalRetentionBypassFn(goi, gerr); err != nil {
			return ObjectInfo{}, err
		}
	}

	if opts.Expiration.Expire {
		if gerr == nil {
			evt := evalActionFromLifecycle(ctx, *lc, rcfg, replcfg, goi)
			var isErr bool
			switch evt.Action {
			case lifecycle.NoneAction:
				isErr = true
			case lifecycle.TransitionAction, lifecycle.TransitionVersionAction:
				isErr = true
			}
			if isErr {
				if goi.VersionID != "" {
					return goi, VersionNotFound{
						Bucket:    bucket,
						Object:    object,
						VersionID: goi.VersionID,
					}
				}
				return goi, ObjectNotFound{
					Bucket: bucket,
					Object: object,
				}
			}
		}
	}

	//  Determine whether to mark object deleted for replication
	markDelete := goi.VersionID != ""

	// Default deleteMarker to true if object is under versioning
	deleteMarker := opts.Versioned

	if opts.VersionID != "" {
		// case where replica version needs to be deleted on target cluster
		if versionFound && opts.DeleteMarkerReplicationStatus() == replication.Replica {
			markDelete = false
		}
		if opts.VersionPurgeStatus().Empty() && opts.DeleteMarkerReplicationStatus().Empty() {
			markDelete = false
		}
		if opts.VersionPurgeStatus() == replication.VersionPurgeComplete {
			markDelete = false
		}
		// now, since VersionPurgeStatus() is already set, we can let the
		// lower layers decide this. This fixes a regression that was introduced
		// in PR #14555 where !VersionPurgeStatus.Empty() is automatically
		// considered as Delete marker true to avoid listing such objects by
		// regular ListObjects() calls. However for delete replication this
		// ends up being a problem because "upon" a successful delete this
		// ends up creating a new delete marker that is spurious and unnecessary.
		//
		// Regression introduced by #14555 was reintroduced in #15564
		if versionFound {
			if !goi.VersionPurgeStatus.Empty() {
				deleteMarker = false
			} else if !goi.DeleteMarker { // implies a versioned delete of object
				deleteMarker = false
			}
		}
	}

	modTime := opts.MTime
	if opts.MTime.IsZero() {
		modTime = UTCNow()
	}
	fvID := mustGetUUID()

	defer func() {
		// attempt a heal before returning if there are offline disks
		// for both del marker and permanent delete situations.
		for _, disk := range storageDisks {
			if disk != nil && disk.IsOnline() {
				continue
			}
			er.addPartial(bucket, object, opts.VersionID)
			break
		}
	}()

	if markDelete && (opts.Versioned || opts.VersionSuspended) {
		if !deleteMarker {
			// versioning suspended means we add `null` version as
			// delete marker, if its not decided already.
			deleteMarker = opts.VersionSuspended && opts.VersionID == ""
		}
		fi := FileInfo{
			Name:             object,
			Deleted:          deleteMarker,
			MarkDeleted:      markDelete,
			ModTime:          modTime,
			ReplicationState: opts.DeleteReplication,
			TransitionStatus: opts.Transition.Status,
			ExpireRestored:   opts.Transition.ExpireRestored,
		}
		fi.SetTierFreeVersionID(fvID)
		if opts.SkipFreeVersion {
			fi.SetSkipTierFreeVersion()
		}
		if opts.VersionID != "" {
			fi.VersionID = opts.VersionID
		} else if opts.Versioned {
			fi.VersionID = mustGetUUID()
		}
		// versioning suspended means we add `null` version as
		// delete marker. Add delete marker, since we don't have
		// any version specified explicitly. Or if a particular
		// version id needs to be replicated.
		if err = er.deleteObjectVersion(ctx, bucket, object, fi, opts.DeleteMarker); err != nil {
			return objInfo, toObjectErr(err, bucket, object)
		}
		oi := fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended)
		oi.replicationDecision = goi.replicationDecision
		return oi, nil
	}

	// Delete the object version on all disks.
	dfi := FileInfo{
		Name:             object,
		VersionID:        opts.VersionID,
		MarkDeleted:      markDelete,
		Deleted:          deleteMarker,
		ModTime:          modTime,
		ReplicationState: opts.DeleteReplication,
		TransitionStatus: opts.Transition.Status,
		ExpireRestored:   opts.Transition.ExpireRestored,
	}
	dfi.SetTierFreeVersionID(fvID)
	if opts.SkipFreeVersion {
		dfi.SetSkipTierFreeVersion()
	}
	if err = er.deleteObjectVersion(ctx, bucket, object, dfi, opts.DeleteMarker); err != nil {
		return objInfo, toObjectErr(err, bucket, object)
	}

	return dfi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended), nil
}

// Send the successful but partial upload/delete, however ignore
// if the channel is blocked by other items.
func (er erasureObjects) addPartial(bucket, object, versionID string) {
	globalMRFState.addPartialOp(PartialOperation{
		Bucket:    bucket,
		Object:    object,
		VersionID: versionID,
		Queued:    time.Now(),
	})
}

func (er erasureObjects) PutObjectMetadata(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	if !opts.NoLock {
		// Lock the object before updating metadata.
		lk := er.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

	disks := er.getDisks()

	var metaArr []FileInfo
	var errs []error

	// Read metadata associated with the object from all disks.
	if opts.VersionID != "" {
		metaArr, errs = readAllFileInfo(ctx, disks, "", bucket, object, opts.VersionID, false, false)
	} else {
		metaArr, errs = readAllXL(ctx, disks, bucket, object, false, false)
	}

	readQuorum, _, err := objectQuorumFromMeta(ctx, metaArr, errs, er.defaultParityCount)
	if err != nil {
		if shouldCheckForDangling(err, errs, bucket) {
			_, derr := er.deleteIfDangling(context.Background(), bucket, object, metaArr, errs, nil, opts)
			if derr == nil {
				if opts.VersionID != "" {
					err = errFileVersionNotFound
				} else {
					err = errFileNotFound
				}
			}
		}
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// List all online disks.
	onlineDisks, modTime, etag := listOnlineDisks(disks, metaArr, errs, readQuorum)

	// Pick latest valid metadata.
	fi, err := pickValidFileInfo(ctx, metaArr, modTime, etag, readQuorum)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if fi.Deleted {
		return ObjectInfo{}, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	filterOnlineDisksInplace(fi, metaArr, onlineDisks)

	// if version-id is not specified retention is supposed to be set on the latest object.
	if opts.VersionID == "" {
		opts.VersionID = fi.VersionID
	}

	objInfo := fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended)
	if opts.EvalMetadataFn != nil {
		if _, err := opts.EvalMetadataFn(&objInfo, err); err != nil {
			return ObjectInfo{}, err
		}
	}
	maps.Copy(fi.Metadata, objInfo.UserDefined)
	fi.ModTime = opts.MTime
	fi.VersionID = opts.VersionID

	if err = er.updateObjectMeta(ctx, bucket, object, fi, onlineDisks); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	return fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended), nil
}

// PutObjectTags - replace or add tags to an existing object
func (er erasureObjects) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) (ObjectInfo, error) {
	if !opts.NoLock {
		// Lock the object before updating tags.
		lk := er.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

	disks := er.getDisks()

	var metaArr []FileInfo
	var errs []error

	// Read metadata associated with the object from all disks.
	if opts.VersionID != "" {
		metaArr, errs = readAllFileInfo(ctx, disks, "", bucket, object, opts.VersionID, false, false)
	} else {
		metaArr, errs = readAllXL(ctx, disks, bucket, object, false, false)
	}

	readQuorum, _, err := objectQuorumFromMeta(ctx, metaArr, errs, er.defaultParityCount)
	if err != nil {
		if shouldCheckForDangling(err, errs, bucket) {
			_, derr := er.deleteIfDangling(context.Background(), bucket, object, metaArr, errs, nil, opts)
			if derr == nil {
				if opts.VersionID != "" {
					err = errFileVersionNotFound
				} else {
					err = errFileNotFound
				}
			}
		}
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// List all online disks.
	onlineDisks, modTime, etag := listOnlineDisks(disks, metaArr, errs, readQuorum)

	// Pick latest valid metadata.
	fi, err := pickValidFileInfo(ctx, metaArr, modTime, etag, readQuorum)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	if fi.Deleted {
		if opts.VersionID == "" {
			return ObjectInfo{}, toObjectErr(errFileNotFound, bucket, object)
		}
		return ObjectInfo{}, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	filterOnlineDisksInplace(fi, metaArr, onlineDisks)

	fi.Metadata[xhttp.AmzObjectTagging] = tags
	fi.ReplicationState = opts.PutReplicationState()
	maps.Copy(fi.Metadata, opts.UserDefined)

	if err = er.updateObjectMeta(ctx, bucket, object, fi, onlineDisks); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	return fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended), nil
}

func (er erasureObjects) updateObjectMetaWithOpts(ctx context.Context, bucket, object string, fi FileInfo, onlineDisks []StorageAPI, opts UpdateMetadataOpts) error {
	if len(fi.Metadata) == 0 {
		return nil
	}

	g := errgroup.WithNErrs(len(onlineDisks))

	// Start writing `xl.meta` to all disks in parallel.
	for index := range onlineDisks {
		g.Go(func() error {
			if onlineDisks[index] == nil {
				return errDiskNotFound
			}
			return onlineDisks[index].UpdateMetadata(ctx, bucket, object, fi, opts)
		}, index)
	}

	// Wait for all the routines.
	mErrs := g.Wait()

	return reduceWriteQuorumErrs(ctx, mErrs, objectOpIgnoredErrs, fi.WriteQuorum(er.defaultWQuorum()))
}

// updateObjectMeta will update the metadata of a file.
func (er erasureObjects) updateObjectMeta(ctx context.Context, bucket, object string, fi FileInfo, onlineDisks []StorageAPI) error {
	return er.updateObjectMetaWithOpts(ctx, bucket, object, fi, onlineDisks, UpdateMetadataOpts{})
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
	tgtClient, err := globalTierConfigMgr.getDriver(ctx, opts.Transition.Tier)
	if err != nil {
		return err
	}

	if !opts.NoLock {
		// Acquire write lock before starting to transition the object.
		lk := er.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalDeleteOperationTimeout)
		if err != nil {
			return err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx)
	}

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
	// verify that the object queued for transition is identical to that on disk.
	if !opts.MTime.Equal(fi.ModTime) || !strings.EqualFold(opts.Transition.ETag, extractETag(fi.Metadata)) {
		return toObjectErr(errFileNotFound, bucket, object)
	}
	// if object already transitioned, return
	if fi.TransitionStatus == lifecycle.TransitionComplete {
		return nil
	}

	if fi.XLV1 {
		if _, err = er.HealObject(ctx, bucket, object, "", madmin.HealOpts{NoLock: true}); err != nil {
			return err
		}
		// Fetch FileInfo again. HealObject migrates object the latest
		// format. Among other things this changes fi.DataDir and
		// possibly fi.Data (if data is inlined).
		fi, metaArr, onlineDisks, err = er.getObjectFileInfo(ctx, bucket, object, opts, true)
		if err != nil {
			return toObjectErr(err, bucket, object)
		}
	}
	traceFn := globalLifecycleSys.trace(fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended))

	destObj, err := genTransitionObjName(bucket)
	if err != nil {
		traceFn(ILMTransition, nil, err)
		return err
	}

	pr, pw := xioutil.WaitPipe()
	go func() {
		err := er.getObjectWithFileInfo(ctx, bucket, object, 0, fi.Size, pw, fi, metaArr, onlineDisks)
		pw.CloseWithError(err)
	}()

	var rv remoteVersionID
	rv, err = tgtClient.PutWithMeta(ctx, destObj, pr, fi.Size, map[string]string{
		"name": object, // preserve the original name of the object on the remote tier object metadata.
		// this is just for future reverse lookup() purposes (applies only for new objects)
		// does not apply retro-actively on already transitioned objects.
	})
	pr.CloseWithError(err)
	if err != nil {
		traceFn(ILMTransition, nil, err)
		return err
	}
	fi.TransitionStatus = lifecycle.TransitionComplete
	fi.TransitionedObjName = destObj
	fi.TransitionTier = opts.Transition.Tier
	fi.TransitionVersionID = string(rv)
	eventName := event.ObjectTransitionComplete

	storageDisks := er.getDisks()

	if err = er.deleteObjectVersion(ctx, bucket, object, fi, false); err != nil {
		eventName = event.ObjectTransitionFailed
	}

	for _, disk := range storageDisks {
		if disk != nil && disk.IsOnline() {
			continue
		}
		er.addPartial(bucket, object, opts.VersionID)
		break
	}

	objInfo := fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended)
	sendEvent(eventArgs{
		EventName:  eventName,
		BucketName: bucket,
		Object:     objInfo,
		UserAgent:  "Internal: [ILM-Transition]",
		Host:       globalLocalNodeName,
	})
	tags := opts.LifecycleAuditEvent.Tags()
	auditLogLifecycle(ctx, objInfo, ILMTransition, tags, traceFn)
	return err
}

// RestoreTransitionedObject - restore transitioned object content locally on this cluster.
// This is similar to PostObjectRestore from AWS GLACIER
// storage class. When PostObjectRestore API is called, a temporary copy of the object
// is restored locally to the bucket on source cluster until the restore expiry date.
// The copy that was transitioned continues to reside in the transitioned tier.
func (er erasureObjects) RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return er.restoreTransitionedObject(ctx, bucket, object, opts)
}

// update restore status header in the metadata
func (er erasureObjects) updateRestoreMetadata(ctx context.Context, bucket, object string, objInfo ObjectInfo, opts ObjectOptions) error {
	oi := objInfo.Clone()
	oi.metadataOnly = true // Perform only metadata updates.

	// allow retry in the case of failure to restore
	delete(oi.UserDefined, xhttp.AmzRestore)

	if _, err := er.CopyObject(ctx, bucket, object, bucket, object, oi, ObjectOptions{
		VersionID: oi.VersionID,
	}, ObjectOptions{
		VersionID: oi.VersionID,
	}); err != nil {
		storageLogIf(ctx, fmt.Errorf("Unable to update transition restore metadata for %s/%s(%s): %s", bucket, object, oi.VersionID, err))
		return err
	}
	return nil
}

// restoreTransitionedObject for multipart object chunks the file stream from remote tier into the same number of parts
// as in the xl.meta for this version and rehydrates the part.n into the fi.DataDir for this version as in the xl.meta
func (er erasureObjects) restoreTransitionedObject(ctx context.Context, bucket string, object string, opts ObjectOptions) error {
	setRestoreHeaderFn := func(oi ObjectInfo, rerr error) error {
		if rerr == nil {
			return nil // nothing to do; restore object was successful
		}
		er.updateRestoreMetadata(ctx, bucket, object, oi, opts)
		return rerr
	}
	var oi ObjectInfo
	// get the file info on disk for transitioned object
	actualfi, _, _, err := er.getObjectFileInfo(ctx, bucket, object, opts, false)
	if err != nil {
		return setRestoreHeaderFn(oi, toObjectErr(err, bucket, object))
	}

	oi = actualfi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended)
	ropts := putRestoreOpts(bucket, object, opts.Transition.RestoreRequest, oi)
	if len(oi.Parts) == 1 {
		var rs *HTTPRangeSpec
		gr, err := getTransitionedObjectReader(ctx, bucket, object, rs, http.Header{}, oi, opts)
		if err != nil {
			return setRestoreHeaderFn(oi, toObjectErr(err, bucket, object))
		}
		defer gr.Close()
		hashReader, err := hash.NewReader(ctx, gr, gr.ObjInfo.Size, "", "", gr.ObjInfo.Size)
		if err != nil {
			return setRestoreHeaderFn(oi, toObjectErr(err, bucket, object))
		}
		pReader := NewPutObjReader(hashReader)
		_, err = er.PutObject(ctx, bucket, object, pReader, ropts)
		return setRestoreHeaderFn(oi, toObjectErr(err, bucket, object))
	}

	res, err := er.NewMultipartUpload(ctx, bucket, object, ropts)
	if err != nil {
		return setRestoreHeaderFn(oi, err)
	}

	var uploadedParts []CompletePart
	var rs *HTTPRangeSpec
	// get reader from the warm backend - note that even in the case of encrypted objects, this stream is still encrypted.
	gr, err := getTransitionedObjectReader(ctx, bucket, object, rs, http.Header{}, oi, opts)
	if err != nil {
		return setRestoreHeaderFn(oi, err)
	}
	defer gr.Close()

	// rehydrate the parts back on disk as per the original xl.meta prior to transition
	for _, partInfo := range oi.Parts {
		hr, err := hash.NewReader(ctx, io.LimitReader(gr, partInfo.Size), partInfo.Size, "", "", partInfo.Size)
		if err != nil {
			return setRestoreHeaderFn(oi, err)
		}
		pInfo, err := er.PutObjectPart(ctx, bucket, object, res.UploadID, partInfo.Number, NewPutObjReader(hr), ObjectOptions{})
		if err != nil {
			return setRestoreHeaderFn(oi, err)
		}
		if pInfo.Size != partInfo.Size {
			return setRestoreHeaderFn(oi, InvalidObjectState{Bucket: bucket, Object: object})
		}
		uploadedParts = append(uploadedParts, CompletePart{
			PartNumber: pInfo.PartNumber,
			ETag:       pInfo.ETag,
		})
	}
	_, err = er.CompleteMultipartUpload(ctx, bucket, object, res.UploadID, uploadedParts, ObjectOptions{
		MTime: oi.ModTime,
	})
	return setRestoreHeaderFn(oi, err)
}

// DecomTieredObject - moves tiered object to another pool during decommissioning.
func (er erasureObjects) DecomTieredObject(ctx context.Context, bucket, object string, fi FileInfo, opts ObjectOptions) error {
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}
	// overlay Erasure info for this set of disks
	storageDisks := er.getDisks()
	// Get parity and data drive count based on storage class metadata
	parityDrives := globalStorageClass.GetParityForSC(opts.UserDefined[xhttp.AmzStorageClass])
	if parityDrives < 0 {
		parityDrives = er.defaultParityCount
	}
	dataDrives := len(storageDisks) - parityDrives

	// we now know the number of blocks this object needs for data and parity.
	// writeQuorum is dataBlocks + 1
	writeQuorum := dataDrives
	if dataDrives == parityDrives {
		writeQuorum++
	}

	// Initialize parts metadata
	partsMetadata := make([]FileInfo, len(storageDisks))

	fi2 := newFileInfo(pathJoin(bucket, object), dataDrives, parityDrives)
	fi.Erasure = fi2.Erasure
	// Initialize erasure metadata.
	for index := range partsMetadata {
		partsMetadata[index] = fi
		partsMetadata[index].Erasure.Index = index + 1
	}

	// Order disks according to erasure distribution
	var onlineDisks []StorageAPI
	onlineDisks, partsMetadata = shuffleDisksAndPartsMetadata(storageDisks, partsMetadata, fi)

	if _, err := writeUniqueFileInfo(ctx, onlineDisks, "", bucket, object, partsMetadata, writeQuorum); err != nil {
		return toObjectErr(err, bucket, object)
	}

	return nil
}
