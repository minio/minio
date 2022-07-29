// Copyright (c) 2015-2022 MinIO, Inc.
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
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/klauspost/readahead"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/bpool"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
	"github.com/minio/pkg/mimedb"
)

// erasureSingle - Implements single drive XL layer
type erasureSingle struct {
	GatewayUnsupported

	disk StorageAPI

	endpoint Endpoint

	// Locker mutex map.
	nsMutex *nsLockMap

	// Byte pools used for temporary i/o buffers.
	bp *bpool.BytePoolCap

	deletedCleanupSleeper *dynamicSleeper

	// Shut down async operations
	shutdown context.CancelFunc

	format *formatErasureV3
}

// Initialize new set of erasure coded sets.
func newErasureSingle(ctx context.Context, storageDisk StorageAPI, format *formatErasureV3) (ObjectLayer, error) {
	// Number of buffers, max 2GB
	n := (2 * humanize.GiByte) / (blockSizeV2 * 2)

	// Initialize byte pool once for all sets, bpool size is set to
	// setCount * setDriveCount with each memory upto blockSizeV2.
	bp := bpool.NewBytePoolCap(n, blockSizeV2, blockSizeV2*2)

	// Initialize the erasure sets instance.
	s := &erasureSingle{
		disk:                  storageDisk,
		endpoint:              storageDisk.Endpoint(),
		format:                format,
		nsMutex:               newNSLock(false),
		bp:                    bp,
		deletedCleanupSleeper: newDynamicSleeper(10, 2*time.Second, false),
	}

	// start cleanup stale uploads go-routine.
	go s.cleanupStaleUploads(ctx)

	// start cleanup of deleted objects.
	go s.cleanupDeletedObjects(ctx)

	ctx, s.shutdown = context.WithCancel(ctx)
	go intDataUpdateTracker.start(ctx, s.endpoint.Path)

	return s, nil
}

// List all buckets from one of the set, we are not doing merge
// sort here just for simplification. As per design it is assumed
// that all buckets are present on all sets.
func (es *erasureSingle) ListBuckets(ctx context.Context, opts BucketOptions) (buckets []BucketInfo, err error) {
	var listBuckets []BucketInfo
	healBuckets := map[string]VolInfo{}
	// lists all unique buckets across drives.
	if err := listAllBuckets(ctx, []StorageAPI{es.disk}, healBuckets, 0); err != nil {
		return nil, err
	}
	// include deleted buckets in listBuckets output
	deletedBuckets := map[string]VolInfo{}

	if opts.Deleted {
		// lists all deleted buckets across drives.
		if err := listDeletedBuckets(ctx, []StorageAPI{es.disk}, deletedBuckets, 0); err != nil {
			return nil, err
		}
	}
	for _, v := range healBuckets {
		bi := BucketInfo{
			Name:    v.Name,
			Created: v.Created,
		}
		if vi, ok := deletedBuckets[v.Name]; ok {
			bi.Deleted = vi.Created
		}
		listBuckets = append(listBuckets, bi)
	}

	for _, v := range deletedBuckets {
		if _, ok := healBuckets[v.Name]; !ok {
			listBuckets = append(listBuckets, BucketInfo{
				Name:    v.Name,
				Deleted: v.Created,
			})
		}
	}

	sort.Slice(listBuckets, func(i, j int) bool {
		return listBuckets[i].Name < listBuckets[j].Name
	})

	for i := range listBuckets {
		meta, err := globalBucketMetadataSys.Get(listBuckets[i].Name)
		if err == nil {
			listBuckets[i].Created = meta.Created
		}
	}

	return listBuckets, nil
}

func (es *erasureSingle) cleanupStaleUploads(ctx context.Context) {
	timer := time.NewTimer(globalAPIConfig.getStaleUploadsCleanupInterval())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			es.cleanupStaleUploadsOnDisk(ctx, es.disk, globalAPIConfig.getStaleUploadsExpiry())

			// Reset for the next interval
			timer.Reset(globalAPIConfig.getStaleUploadsCleanupInterval())
		}
	}
}

// cleanup ".trash/" folder every 5m minutes with sufficient sleep cycles, between each
// deletes a dynamic sleeper is used with a factor of 10 ratio with max delay between
// deletes to be 2 seconds.
func (es *erasureSingle) cleanupDeletedObjects(ctx context.Context) {
	timer := time.NewTimer(globalAPIConfig.getDeleteCleanupInterval())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			es.cleanupDeletedObjectsInner(ctx)
			// Reset for the next interval
			timer.Reset(globalAPIConfig.getDeleteCleanupInterval())
		}
	}
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (es *erasureSingle) NewNSLock(bucket string, objects ...string) RWLocker {
	return es.nsMutex.NewNSLock(nil, bucket, objects...)
}

// Shutdown function for object storage interface.
func (es *erasureSingle) Shutdown(ctx context.Context) error {
	defer es.shutdown()

	// Add any object layer shutdown activities here.
	closeStorageDisks(es.disk)
	return nil
}

func (es *erasureSingle) SetDriveCounts() []int {
	return []int{1}
}

func (es *erasureSingle) BackendInfo() (b madmin.BackendInfo) {
	b.Type = madmin.Erasure

	scParity := 0
	rrSCParity := 0

	// Data blocks can vary per pool, but parity is same.
	for _, setDriveCount := range es.SetDriveCounts() {
		b.StandardSCData = append(b.StandardSCData, setDriveCount-scParity)
		b.RRSCData = append(b.RRSCData, setDriveCount-rrSCParity)
	}

	b.StandardSCParity = scParity
	b.RRSCParity = rrSCParity
	return
}

// StorageInfo - returns underlying storage statistics.
func (es *erasureSingle) StorageInfo(ctx context.Context) (StorageInfo, []error) {
	disks := []StorageAPI{es.disk}
	endpoints := []Endpoint{es.endpoint}

	storageInfo, errs := getStorageInfo(disks, endpoints)
	storageInfo.Backend = es.BackendInfo()
	return storageInfo, errs
}

// LocalStorageInfo - returns underlying local storage statistics.
func (es *erasureSingle) LocalStorageInfo(ctx context.Context) (StorageInfo, []error) {
	disks := []StorageAPI{es.disk}
	endpoints := []Endpoint{es.endpoint}

	var localDisks []StorageAPI
	var localEndpoints []Endpoint

	for i, endpoint := range endpoints {
		if endpoint.IsLocal {
			localDisks = append(localDisks, disks[i])
			localEndpoints = append(localEndpoints, endpoint)
		}
	}

	return getStorageInfo(localDisks, localEndpoints)
}

// Clean-up previously deleted objects. from .minio.sys/tmp/.trash/
func (es *erasureSingle) cleanupDeletedObjectsInner(ctx context.Context) {
	diskPath := es.disk.Endpoint().Path
	readDirFn(pathJoin(diskPath, minioMetaTmpDeletedBucket), func(ddir string, typ os.FileMode) error {
		wait := es.deletedCleanupSleeper.Timer(ctx)
		removeAll(pathJoin(diskPath, minioMetaTmpDeletedBucket, ddir))
		wait()
		return nil
	})
}

func (es *erasureSingle) renameAll(ctx context.Context, bucket, prefix string) {
	if es.disk != nil {
		es.disk.RenameFile(ctx, bucket, prefix, minioMetaTmpDeletedBucket, mustGetUUID())
	}
}

type renameAllStorager interface {
	renameAll(ctx context.Context, bucket, prefix string)
}

// Bucket operations
// MakeBucket - make a bucket.
func (es *erasureSingle) MakeBucketWithLocation(ctx context.Context, bucket string, opts MakeBucketOptions) error {
	defer NSUpdated(bucket, slashSeparator)

	// Lock the bucket name before creating.
	lk := es.NewNSLock(minioMetaTmpBucket, bucket+".lck")
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	// Verify if bucket is valid.
	if !isMinioMetaBucketName(bucket) {
		if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
			return BucketNameInvalid{Bucket: bucket}
		}
	}

	if err := es.disk.MakeVol(ctx, bucket); err != nil {
		if opts.ForceCreate && errors.Is(err, errVolumeExists) {
			// No need to return error when force create was
			// requested.
			return nil
		}
		if !errors.Is(err, errVolumeExists) {
			logger.LogIf(ctx, err)
		}
		return toObjectErr(err, bucket)
	}

	// If it doesn't exist we get a new, so ignore errors
	meta := newBucketMetadata(bucket)
	meta.SetCreatedAt(opts.CreatedAt)
	if opts.LockEnabled {
		meta.VersioningConfigXML = enabledBucketVersioningConfig
		meta.ObjectLockConfigXML = enabledBucketObjectLockConfig
	}

	if opts.VersioningEnabled {
		meta.VersioningConfigXML = enabledBucketVersioningConfig
	}

	if err := meta.Save(context.Background(), es); err != nil {
		return toObjectErr(err, bucket)
	}

	globalBucketMetadataSys.Set(bucket, meta)

	return nil
}

// GetBucketInfo - returns BucketInfo for a bucket.
func (es *erasureSingle) GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (bi BucketInfo, e error) {
	volInfo, err := es.disk.StatVol(ctx, bucket)
	if err != nil {
		if opts.Deleted {
			if dvi, derr := es.disk.StatVol(ctx, pathJoin(minioMetaBucket, bucketMetaPrefix, deletedBucketsPrefix, bucket)); derr == nil {
				return BucketInfo{Name: bucket, Deleted: dvi.Created}, nil
			}
		}
		return bi, toObjectErr(err, bucket)
	}
	return BucketInfo{Name: volInfo.Name, Created: volInfo.Created}, nil
}

// DeleteBucket - deletes a bucket.
func (es *erasureSingle) DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	// Collect if all disks report volume not found.
	defer NSUpdated(bucket, slashSeparator)

	err := es.disk.DeleteVol(ctx, bucket, opts.Force)
	// Purge the entire bucket metadata entirely.
	deleteBucketMetadata(ctx, es, bucket)
	globalBucketMetadataSys.Remove(bucket)

	if err == nil || errors.Is(err, errVolumeNotFound) {
		if opts.SRDeleteOp == MarkDelete {
			es.markDelete(ctx, minioMetaBucket, pathJoin(bucketMetaPrefix, deletedBucketsPrefix, bucket))
		}
	}
	return toObjectErr(err, bucket)
}

// markDelete creates a vol entry in .minio.sys/buckets/.deleted until site replication
// syncs the delete to peers
func (es *erasureSingle) markDelete(ctx context.Context, bucket, prefix string) error {
	err := es.disk.MakeVol(ctx, pathJoin(bucket, prefix))
	if err != nil && errors.Is(err, errVolumeExists) {
		return nil
	}
	return toObjectErr(err, bucket)
}

// purgeDelete deletes vol entry in .minio.sys/buckets/.deleted after site replication
// syncs the delete to peers OR on a new MakeBucket call.
func (es *erasureSingle) purgeDelete(ctx context.Context, bucket, prefix string) error {
	err := es.disk.DeleteVol(ctx, pathJoin(bucket, prefix), true)
	return toObjectErr(err, bucket)
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (es *erasureSingle) IsNotificationSupported() bool {
	return true
}

// IsListenSupported returns whether listen bucket notification is applicable for this layer.
func (es *erasureSingle) IsListenSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (es *erasureSingle) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (es *erasureSingle) IsCompressionSupported() bool {
	return true
}

// IsTaggingSupported indicates whethes *erasureSingle implements tagging support.
func (es *erasureSingle) IsTaggingSupported() bool {
	return true
}

// Object Operations

// CopyObject - copy object source object to destination object.
// if source object and destination object are same we only
// update metadata.
func (es *erasureSingle) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (oi ObjectInfo, err error) {
	defer NSUpdated(dstBucket, dstObject)

	srcObject = encodeDirObject(srcObject)
	dstObject = encodeDirObject(dstObject)

	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))

	if !dstOpts.NoLock {
		ns := es.NewNSLock(dstBucket, dstObject)
		lkctx, err := ns.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer ns.Unlock(lkctx.Cancel)
		dstOpts.NoLock = true
	}

	if cpSrcDstSame && srcInfo.metadataOnly {
		// Read metadata associated with the object from all disks.
		storageDisks := []StorageAPI{es.disk}

		var metaArr []FileInfo
		var errs []error

		// Read metadata associated with the object from all disks.
		if srcOpts.VersionID != "" {
			metaArr, errs = readAllFileInfo(ctx, storageDisks, srcBucket, srcObject, srcOpts.VersionID, true)
		} else {
			metaArr, errs = readAllXL(ctx, storageDisks, srcBucket, srcObject, true)
		}

		readQuorum, writeQuorum, err := objectQuorumFromMeta(ctx, metaArr, errs, 0)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, srcBucket, srcObject)
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
			modTime = UTCNow()
		}

		// If the data is not inlined, we may end up incorrectly
		// inlining the data here, that leads to an inconsistent
		// situation where some objects are were not inlined
		// were now inlined, make sure to `nil` the Data such
		// that xl.meta is written as expected.
		if !fi.InlineData() {
			fi.Data = nil
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
				if !metaArr[index].InlineData() {
					// If the data is not inlined, we may end up incorrectly
					// inlining the data here, that leads to an inconsistent
					// situation where some objects are were not inlined
					// were now inlined, make sure to `nil` the Data such
					// that xl.meta is written as expected.
					metaArr[index].Data = nil
				}
			}
		}

		// Write unique `xl.meta` for each disk.
		if _, err = writeUniqueFileInfo(ctx, onlineDisks, srcBucket, srcObject, metaArr, writeQuorum); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		return fi.ToObjectInfo(srcBucket, srcObject, srcOpts.Versioned || srcOpts.VersionSuspended), nil
	}

	putOpts := ObjectOptions{
		ServerSideEncryption: dstOpts.ServerSideEncryption,
		UserDefined:          srcInfo.UserDefined,
		Versioned:            dstOpts.Versioned,
		VersionID:            dstOpts.VersionID,
		MTime:                dstOpts.MTime,
		NoLock:               true,
	}

	return es.PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, putOpts)
}

// GetObjectNInfo - returns object info and an object
// Read(Closer). When err != nil, the returned reader is always nil.
func (es *erasureSingle) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}

	object = encodeDirObject(object)

	var unlockOnDefer bool
	nsUnlocker := func() {}
	defer func() {
		if unlockOnDefer {
			nsUnlocker()
		}
	}()

	// Acquire lock
	if lockType != noLock {
		lock := es.NewNSLock(bucket, object)
		switch lockType {
		case writeLock:
			lkctx, err := lock.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			nsUnlocker = func() { lock.Unlock(lkctx.Cancel) }
		case readLock:
			lkctx, err := lock.GetRLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			nsUnlocker = func() { lock.RUnlock(lkctx.Cancel) }
		}
		unlockOnDefer = true
	}

	fi, metaArr, onlineDisks, err := es.getObjectFileInfo(ctx, bucket, object, opts, true)
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
	if objInfo.IsRemote() {
		gr, err := getTransitionedObjectReader(ctx, bucket, object, rs, h, objInfo, opts)
		if err != nil {
			return nil, err
		}
		unlockOnDefer = false
		return gr.WithCleanupFuncs(nsUnlocker), nil
	}

	fn, off, length, err := NewGetObjectReader(rs, objInfo, opts)
	if err != nil {
		return nil, err
	}
	unlockOnDefer = false

	pr, pw := xioutil.WaitPipe()
	go func() {
		pw.CloseWithError(es.getObjectWithFileInfo(ctx, bucket, object, off, length, pw, fi, metaArr, onlineDisks))
	}()

	// Cleanup function to cause the go routine above to exit, in
	// case of incomplete read.
	pipeCloser := func() {
		pr.CloseWithError(nil)
	}

	return fn(pr, h, pipeCloser, nsUnlocker)
}

func (es *erasureSingle) getObjectWithFileInfo(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, fi FileInfo, metaArr []FileInfo, onlineDisks []StorageAPI) error {
	// Reorder online disks based on erasure distribution ordes.
	// Reorder parts metadata based on erasure distribution ordes.
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

	// once we have obtained a common FileInfo i.e latest, we should stick
	// to single dataDir to read the content to avoid reading from some other
	// dataDir that has stale FileInfo{} to ensure that we fail appropriately
	// during reads and expect the same dataDir everywhere.
	dataDir := fi.DataDir
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
			partPath := pathJoin(object, dataDir, fmt.Sprintf("part.%d", partNumber))
			readers[index] = newBitrotReader(disk, metaArr[index].Data, bucket, partPath, tillOffset,
				checksumInfo.Algorithm, checksumInfo.Hash, erasure.ShardSize())

			// Prefer local disks
			prefer[index] = disk.Hostname() == ""
		}

		_, err = erasure.Decode(ctx, writer, readers, partOffset, partLength, partSize, prefer)
		// Note: we should not be defer'ing the following closeBitrotReaders() call as
		// we are inside a for loop i.e if we use defer, we would accumulate a lot of open files by the time
		// we return from this function.
		closeBitrotReaders(readers)
		if err != nil {
			return toObjectErr(err, bucket, object)
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

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (es *erasureSingle) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (info ObjectInfo, err error) {
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return info, err
	}

	object = encodeDirObject(object)
	if !opts.NoLock {
		// Lock the object before reading.
		lk := es.NewNSLock(bucket, object)
		lkctx, err := lk.GetRLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.RUnlock(lkctx.Cancel)
	}

	return es.getObjectInfo(ctx, bucket, object, opts)
}

func (es *erasureSingle) getObjectFileInfo(ctx context.Context, bucket, object string, opts ObjectOptions, readData bool) (fi FileInfo, metaArr []FileInfo, onlineDisks []StorageAPI, err error) {
	disks := []StorageAPI{es.disk}

	var errs []error

	// Read metadata associated with the object from all disks.
	metaArr, errs = readAllFileInfo(ctx, disks, bucket, object, opts.VersionID, readData)
	readQuorum, _, err := objectQuorumFromMeta(ctx, metaArr, errs, 0)
	if err != nil {
		return fi, nil, nil, toObjectErr(err, bucket, object)
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

	filterOnlineDisksInplace(fi, metaArr, onlineDisks)
	return fi, metaArr, onlineDisks, nil
}

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (es *erasureSingle) getObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	fi, _, _, err := es.getObjectFileInfo(ctx, bucket, object, opts, false)
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

// getObjectInfoAndQuroum - wrapper for reading object metadata and constructs ObjectInfo, additionally returns write quorum for the object.
func (es *erasureSingle) getObjectInfoAndQuorum(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, wquorum int, err error) {
	fi, _, _, err := es.getObjectFileInfo(ctx, bucket, object, opts, false)
	if err != nil {
		return objInfo, 1, toObjectErr(err, bucket, object)
	}

	wquorum = fi.Erasure.DataBlocks
	if fi.Erasure.DataBlocks == fi.Erasure.ParityBlocks {
		wquorum++
	}

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

func (es *erasureSingle) putMetacacheObject(ctx context.Context, key string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	data := r.Reader

	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	storageDisks := []StorageAPI{es.disk}
	// Get parity and data drive count based on storage class metadata
	parityDrives := 0
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
		buffer = make([]byte, 1) // Allocate atleast a byte to reach EOF
	case size >= fi.Erasure.BlockSize:
		buffer = es.bp.Get()
		defer es.bp.Put(buffer)
	case size < fi.Erasure.BlockSize:
		// No need to allocate fully blockSizeV1 buffer if the incoming data is smaller.
		buffer = make([]byte, size, 2*size+int64(fi.Erasure.ParityBlocks+fi.Erasure.DataBlocks-1))
	}

	if len(buffer) > int(fi.Erasure.BlockSize) {
		buffer = buffer[:fi.Erasure.BlockSize]
	}

	shardFileSize := erasure.ShardFileSize(data.Size())
	writers := make([]io.Writer, len(onlineDisks))
	inlineBuffers := make([]*bytes.Buffer, len(onlineDisks))
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}
		if disk.IsOnline() {
			inlineBuffers[i] = bytes.NewBuffer(make([]byte, 0, shardFileSize))
			writers[i] = newStreamingBitrotWriterBuffer(inlineBuffers[i], DefaultBitrotAlgorithm, erasure.ShardSize())
		}
	}

	n, erasureErr := erasure.Encode(ctx, data, writers, buffer, writeQuorum)
	closeBitrotWriters(writers)
	if erasureErr != nil {
		return ObjectInfo{}, toObjectErr(erasureErr, minioMetaBucket, key)
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
		partsMetadata[i].AddObjectPart(1, "", n, data.ActualSize(), modTime, index)
		partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{
			PartNumber: 1,
			Algorithm:  DefaultBitrotAlgorithm,
			Hash:       bitrotWriterSum(w),
		})
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

	for i := 0; i < len(onlineDisks); i++ {
		if onlineDisks[i] != nil && onlineDisks[i].IsOnline() {
			// Object info is the same in all disks, so we can pick
			// the first meta from online disk
			fi = partsMetadata[i]
			break
		}
	}

	if _, err = writeUniqueFileInfo(ctx, onlineDisks, minioMetaBucket, key, partsMetadata, writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, minioMetaBucket, key)
	}

	return fi.ToObjectInfo(minioMetaBucket, key, opts.Versioned || opts.VersionSuspended), nil
}

// PutObject - creates an object upon reading from the input stream
// until EOF, erasure codes the data across all disk and additionally
// writes `xl.meta` which carries the necessary metadata for future
// object operations.
func (es *erasureSingle) PutObject(ctx context.Context, bucket string, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	// Validate put object input args.
	if err := checkPutObjectArgs(ctx, bucket, object, es); err != nil {
		return ObjectInfo{}, err
	}

	object = encodeDirObject(object)

	if !isMinioMetaBucketName(bucket) && !hasSpaceFor(getDiskInfos(ctx, es.disk), data.Size()) {
		return ObjectInfo{}, toObjectErr(errDiskFull)
	}

	return es.putObject(ctx, bucket, object, data, opts)
}

// putObject wrapper for erasureObjects PutObject
func (es *erasureSingle) putObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	data := r.Reader

	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	storageDisks := []StorageAPI{es.disk}
	parityDrives := 0
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

	// Initialize parts metadata
	partsMetadata := make([]FileInfo, len(storageDisks))

	fi := newFileInfo(pathJoin(bucket, object), dataDrives, parityDrives)
	fi.VersionID = opts.VersionID
	if opts.Versioned && fi.VersionID == "" {
		fi.VersionID = mustGetUUID()
	}

	fi.DataDir = mustGetUUID()
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
		buffer = make([]byte, 1) // Allocate atleast a byte to reach EOF
	case size == -1:
		if size := data.ActualSize(); size > 0 && size < fi.Erasure.BlockSize {
			buffer = make([]byte, data.ActualSize()+256, data.ActualSize()*2+512)
		} else {
			buffer = es.bp.Get()
			defer es.bp.Put(buffer)
		}
	case size >= fi.Erasure.BlockSize:
		buffer = es.bp.Get()
		defer es.bp.Put(buffer)
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
			es.disk.RenameFile(context.Background(), minioMetaTmpBucket, tempObj, minioMetaTmpDeletedBucket, mustGetUUID())
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
	} else {
		// If compressed, use actual size to determine.
		if sz := erasure.ShardFileSize(data.ActualSize()); sz > 0 {
			if !opts.Versioned && sz < smallFileThreshold {
				inlineBuffers = make([]*bytes.Buffer, len(onlineDisks))
			} else if sz < smallFileThreshold/8 {
				inlineBuffers = make([]*bytes.Buffer, len(onlineDisks))
			}
		}
	}
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}

		if !disk.IsOnline() {
			continue
		}

		if len(inlineBuffers) > 0 {
			sz := shardFileSize
			if sz < 0 {
				sz = data.ActualSize()
			}
			inlineBuffers[i] = bytes.NewBuffer(make([]byte, 0, sz))
			writers[i] = newStreamingBitrotWriterBuffer(inlineBuffers[i], DefaultBitrotAlgorithm, erasure.ShardSize())
			continue
		}

		writers[i] = newBitrotWriter(disk, minioMetaTmpBucket, tempErasureObj, shardFileSize, DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	toEncode := io.Reader(data)
	if data.Size() > bigFileThreshold {
		// We use 2 buffers, so we always have a full buffer of input.
		bufA := es.bp.Get()
		bufB := es.bp.Get()
		defer es.bp.Put(bufA)
		defer es.bp.Put(bufB)
		ra, err := readahead.NewReaderBuffer(data, [][]byte{bufA[:fi.Erasure.BlockSize], bufB[:fi.Erasure.BlockSize]})
		if err == nil {
			toEncode = ra
			defer ra.Close()
		}
		logger.LogIf(ctx, err)
	}
	n, erasureErr := erasure.Encode(ctx, toEncode, writers, buffer, writeQuorum)
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
		lk := es.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx.Cancel)
	}

	var index []byte
	if opts.IndexCB != nil {
		index = opts.IndexCB()
	}

	modTime := opts.MTime
	if opts.MTime.IsZero() {
		modTime = UTCNow()
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
		partsMetadata[i].AddObjectPart(1, "", n, data.ActualSize(), modTime, index)
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

	// Fill all the necessary metadata.
	// Update `xl.meta` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Metadata = opts.UserDefined
		partsMetadata[index].Size = n
		partsMetadata[index].ModTime = modTime
	}

	if len(inlineBuffers) > 0 {
		// Set an additional header when data is inlined.
		for index := range partsMetadata {
			partsMetadata[index].SetInlineData()
		}
	}

	// Rename the successfully written temporary object to final location.
	if onlineDisks, err = renameData(ctx, onlineDisks, minioMetaTmpBucket, tempObj, partsMetadata, bucket, object, writeQuorum); err != nil {
		if errors.Is(err, errFileNotFound) {
			return ObjectInfo{}, toObjectErr(errErasureWriteQuorum, bucket, object)
		}
		logger.LogIf(ctx, err)
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	for i := 0; i < len(onlineDisks); i++ {
		if onlineDisks[i] != nil && onlineDisks[i].IsOnline() {
			// Object info is the same in all disks, so we can pick
			// the first meta from online disk
			fi = partsMetadata[i]
			break
		}
	}

	fi.ReplicationState = opts.PutReplicationState()
	online = countOnlineDisks(onlineDisks)

	// we are adding a new version to this object under the namespace lock, so this is the latest version.
	fi.IsLatest = true

	return fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended), nil
}

func (es *erasureSingle) deleteObjectVersion(ctx context.Context, bucket, object string, writeQuorum int, fi FileInfo, forceDelMarker bool) error {
	return es.disk.DeleteVersion(ctx, bucket, object, fi, forceDelMarker)
}

// DeleteObjects deletes objects/versions in bulk, this function will still automatically split objects list
// into smaller bulks if some object names are found to be duplicated in the delete list, splitting
// into smaller bulks will avoid holding twice the write lock of the duplicated object names.
func (es *erasureSingle) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]DeletedObject, len(objects))
	objSets := set.NewStringSet()
	for i := range errs {
		objects[i].ObjectName = encodeDirObject(objects[i].ObjectName)

		errs[i] = checkDelObjArgs(ctx, bucket, objects[i].ObjectName)
		objSets.Add(objects[i].ObjectName)
	}

	// Acquire a bulk write lock across 'objects'
	multiDeleteLock := es.NewNSLock(bucket, objSets.ToSlice()...)
	lkctx, err := multiDeleteLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		for i := range errs {
			errs[i] = err
		}
		return dobjects, errs
	}
	ctx = lkctx.Context()
	defer multiDeleteLock.Unlock(lkctx.Cancel)

	writeQuorums := make([]int, len(objects))
	storageDisks := []StorageAPI{es.disk}

	for i := range objects {
		// Single drive write quorum is '1'
		writeQuorums[i] = 1
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
	for _, version := range versionsMap {
		dedupVersions = append(dedupVersions, version)
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
			errs := disk.DeleteVersions(ctx, bucket, dedupVersions)
			for i, err := range errs {
				if err == nil {
					continue
				}
				for _, v := range dedupVersions[i].Versions {
					if err == errFileNotFound || err == errFileVersionNotFound {
						if !dobjects[v.Idx].DeleteMarker {
							// Not delete marker, if not found, ok.
							continue
						}
					}
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
		if objects[objIndex].VersionID != "" {
			errs[objIndex] = toObjectErr(err, bucket, objects[objIndex].ObjectName, objects[objIndex].VersionID)
		} else {
			errs[objIndex] = toObjectErr(err, bucket, objects[objIndex].ObjectName)
		}

		defer NSUpdated(bucket, objects[objIndex].ObjectName)
	}

	return dobjects, errs
}

func (es *erasureSingle) deletePrefix(ctx context.Context, bucket, prefix string) error {
	dirPrefix := encodeDirObject(prefix)
	defer es.disk.Delete(ctx, bucket, dirPrefix, DeleteOptions{
		Recursive: true,
		Force:     true,
	})
	return es.disk.Delete(ctx, bucket, prefix, DeleteOptions{
		Recursive: true,
		Force:     true,
	})
}

// DeleteObject - deletes an object, this call doesn't necessary reply
// any error as it is not necessary for the handler to reply back a
// response to the client request.
func (es *erasureSingle) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if err = checkDelObjArgs(ctx, bucket, object); err != nil {
		return objInfo, err
	}

	if opts.DeletePrefix {
		return ObjectInfo{}, toObjectErr(es.deletePrefix(ctx, bucket, object), bucket, object)
	}

	object = encodeDirObject(object)
	var lc *lifecycle.Lifecycle
	var rcfg lock.Retention
	if opts.Expiration.Expire {
		// Check if the current bucket has a configured lifecycle policy
		lc, _ = globalLifecycleSys.Get(bucket)
		rcfg, _ = globalBucketObjectLockSys.Get(bucket)
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

	// Acquire a write lock before deleting the object.
	lk := es.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalDeleteOperationTimeout)
	if err != nil {
		return ObjectInfo{}, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	versionFound := true
	objInfo = ObjectInfo{VersionID: opts.VersionID} // version id needed in Delete API response.
	goi, writeQuorum, gerr := es.getObjectInfoAndQuorum(ctx, bucket, object, opts)
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

	if opts.Expiration.Expire {
		action := evalActionFromLifecycle(ctx, *lc, rcfg, goi, false)
		var isErr bool
		switch action {
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

	defer NSUpdated(bucket, object)

	var markDelete bool
	// Determine whether to mark object deleted for replication
	if goi.VersionID != "" {
		markDelete = true
	}

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
		if opts.VersionPurgeStatus() == Complete {
			markDelete = false
		}

		// Version is found but we do not wish to create more delete markers
		// now, since VersionPurgeStatus() is already set, we can let the
		// lower layers decide this. This fixes a regression that was introduced
		// in PR #14555 where !VersionPurgeStatus.Empty() is automatically
		// considered as Delete marker true to avoid listing such objects by
		// regular ListObjects() calls. However for delete replication this
		// ends up being a problem because "upon" a successful delete this
		// ends up creating a new delete marker that is spurious and unnecessary.
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
	if markDelete {
		if opts.Versioned || opts.VersionSuspended {
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
			if opts.Versioned {
				fi.VersionID = mustGetUUID()
				if opts.VersionID != "" {
					fi.VersionID = opts.VersionID
				}
			}
			// versioning suspended means we add `null` version as
			// delete marker. Add delete marker, since we don't have
			// any version specified explicitly. Or if a particular
			// version id needs to be replicated.
			if err = es.deleteObjectVersion(ctx, bucket, object, writeQuorum, fi, opts.DeleteMarker); err != nil {
				return objInfo, toObjectErr(err, bucket, object)
			}
			return fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended), nil
		}
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
	if err = es.deleteObjectVersion(ctx, bucket, object, writeQuorum, dfi, opts.DeleteMarker); err != nil {
		return objInfo, toObjectErr(err, bucket, object)
	}

	return ObjectInfo{
		Bucket:                     bucket,
		Name:                       object,
		VersionID:                  opts.VersionID,
		VersionPurgeStatusInternal: opts.DeleteReplication.VersionPurgeStatusInternal,
		ReplicationStatusInternal:  opts.DeleteReplication.ReplicationStatusInternal,
	}, nil
}

func (es *erasureSingle) PutObjectMetadata(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	if !opts.NoLock {
		// Lock the object before updating metadata.
		lk := es.NewNSLock(bucket, object)
		lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()
		defer lk.Unlock(lkctx.Cancel)
	}

	disks := []StorageAPI{es.disk}

	var metaArr []FileInfo
	var errs []error

	// Read metadata associated with the object from all disks.
	metaArr, errs = readAllFileInfo(ctx, disks, bucket, object, opts.VersionID, false)

	readQuorum, _, err := objectQuorumFromMeta(ctx, metaArr, errs, 0)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(disks, metaArr, errs)

	// Pick latest valid metadata.
	fi, err := pickValidFileInfo(ctx, metaArr, modTime, readQuorum)
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
		if err := opts.EvalMetadataFn(objInfo); err != nil {
			return ObjectInfo{}, err
		}
	}
	for k, v := range objInfo.UserDefined {
		fi.Metadata[k] = v
	}
	fi.ModTime = opts.MTime
	fi.VersionID = opts.VersionID

	if err = es.updateObjectMeta(ctx, bucket, object, fi, onlineDisks...); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	return fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended), nil
}

// PutObjectTags - replace or add tags to an existing object
func (es *erasureSingle) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) (ObjectInfo, error) {
	// Lock the object before updating tags.
	lk := es.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return ObjectInfo{}, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	disks := []StorageAPI{es.disk}

	var metaArr []FileInfo
	var errs []error

	// Read metadata associated with the object from all disks.
	if opts.VersionID != "" {
		metaArr, errs = readAllFileInfo(ctx, disks, bucket, object, opts.VersionID, false)
	} else {
		metaArr, errs = readAllXL(ctx, disks, bucket, object, false)
	}

	readQuorum, _, err := objectQuorumFromMeta(ctx, metaArr, errs, 0)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(disks, metaArr, errs)

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

	filterOnlineDisksInplace(fi, metaArr, onlineDisks)

	fi.Metadata[xhttp.AmzObjectTagging] = tags
	fi.ReplicationState = opts.PutReplicationState()
	for k, v := range opts.UserDefined {
		fi.Metadata[k] = v
	}

	if err = es.updateObjectMeta(ctx, bucket, object, fi, onlineDisks...); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	return fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended), nil
}

// updateObjectMeta will update the metadata of a file.
func (es *erasureSingle) updateObjectMeta(ctx context.Context, bucket, object string, fi FileInfo, onlineDisks ...StorageAPI) error {
	if len(fi.Metadata) == 0 {
		return nil
	}

	g := errgroup.WithNErrs(len(onlineDisks))

	// Start writing `xl.meta` to all disks in parallel.
	for index := range onlineDisks {
		index := index
		g.Go(func() error {
			if onlineDisks[index] == nil {
				return errDiskNotFound
			}
			return onlineDisks[index].UpdateMetadata(ctx, bucket, object, fi)
		}, index)
	}

	// Wait for all the routines.
	mErrs := g.Wait()

	return reduceWriteQuorumErrs(ctx, mErrs, objectOpIgnoredErrs, 1)
}

// DeleteObjectTags - delete object tags from an existing object
func (es *erasureSingle) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	return es.PutObjectTags(ctx, bucket, object, "", opts)
}

// GetObjectTags - get object tags from an existing object
func (es *erasureSingle) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	// GetObjectInfo will return tag value as well
	oi, err := es.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	return tags.ParseObjectTags(oi.UserTags)
}

// TransitionObject - transition object content to target tier.
func (es *erasureSingle) TransitionObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	tgtClient, err := globalTierConfigMgr.getDriver(opts.Transition.Tier)
	if err != nil {
		return err
	}

	// Acquire write lock before starting to transition the object.
	lk := es.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalDeleteOperationTimeout)
	if err != nil {
		return err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	fi, metaArr, onlineDisks, err := es.getObjectFileInfo(ctx, bucket, object, opts, true)
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
	defer NSUpdated(bucket, object)

	destObj, err := genTransitionObjName(bucket)
	if err != nil {
		return err
	}

	pr, pw := xioutil.WaitPipe()
	go func() {
		err := es.getObjectWithFileInfo(ctx, bucket, object, 0, fi.Size, pw, fi, metaArr, onlineDisks)
		pw.CloseWithError(err)
	}()

	var rv remoteVersionID
	rv, err = tgtClient.Put(ctx, destObj, pr, fi.Size)
	pr.CloseWithError(err)
	if err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to transition %s/%s(%s) to %s tier: %w", bucket, object, opts.VersionID, opts.Transition.Tier, err))
		return err
	}
	fi.TransitionStatus = lifecycle.TransitionComplete
	fi.TransitionedObjName = destObj
	fi.TransitionTier = opts.Transition.Tier
	fi.TransitionVersionID = string(rv)
	eventName := event.ObjectTransitionComplete

	// we now know the number of blocks this object needs for data and parity.
	// writeQuorum is dataBlocks + 1
	writeQuorum := fi.Erasure.DataBlocks
	if fi.Erasure.DataBlocks == fi.Erasure.ParityBlocks {
		writeQuorum++
	}

	if err = es.deleteObjectVersion(ctx, bucket, object, writeQuorum, fi, false); err != nil {
		eventName = event.ObjectTransitionFailed
	}

	objInfo := fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended)
	sendEvent(eventArgs{
		EventName:  eventName,
		BucketName: bucket,
		Object:     objInfo,
		Host:       "Internal: [ILM-Transition]",
	})
	auditLogLifecycle(ctx, objInfo, ILMTransition)
	return err
}

// RestoreTransitionedObject - restore transitioned object content locally on this cluster.
// This is similar to PostObjectRestore from AWS GLACIER
// storage class. When PostObjectRestore API is called, a temporary copy of the object
// is restored locally to the bucket on source cluster until the restore expiry date.
// The copy that was transitioned continues to reside in the transitioned tier.
func (es *erasureSingle) RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return es.restoreTransitionedObject(ctx, bucket, object, opts)
}

// update restore status header in the metadata
func (es *erasureSingle) updateRestoreMetadata(ctx context.Context, bucket, object string, objInfo ObjectInfo, opts ObjectOptions, rerr error) error {
	oi := objInfo.Clone()
	oi.metadataOnly = true // Perform only metadata updates.

	if rerr == nil {
		oi.UserDefined[xhttp.AmzRestore] = completedRestoreObj(opts.Transition.RestoreExpiry).String()
	} else { // allow retry in the case of failure to restore
		delete(oi.UserDefined, xhttp.AmzRestore)
	}
	if _, err := es.CopyObject(ctx, bucket, object, bucket, object, oi, ObjectOptions{
		VersionID: oi.VersionID,
	}, ObjectOptions{
		VersionID: oi.VersionID,
	}); err != nil {
		logger.LogIf(ctx, fmt.Errorf("Unable to update transition restore metadata for %s/%s(%s): %s", bucket, object, oi.VersionID, err))
		return err
	}
	return nil
}

// restoreTransitionedObject for multipart object chunks the file stream from remote tier into the same number of parts
// as in the xl.meta for this version and rehydrates the part.n into the fi.DataDir for this version as in the xl.meta
func (es *erasureSingle) restoreTransitionedObject(ctx context.Context, bucket string, object string, opts ObjectOptions) error {
	setRestoreHeaderFn := func(oi ObjectInfo, rerr error) error {
		es.updateRestoreMetadata(ctx, bucket, object, oi, opts, rerr)
		return rerr
	}
	var oi ObjectInfo
	// get the file info on disk for transitioned object
	actualfi, _, _, err := es.getObjectFileInfo(ctx, bucket, object, opts, false)
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
		hashReader, err := hash.NewReader(gr, gr.ObjInfo.Size, "", "", gr.ObjInfo.Size)
		if err != nil {
			return setRestoreHeaderFn(oi, toObjectErr(err, bucket, object))
		}
		pReader := NewPutObjReader(hashReader)
		ropts.UserDefined[xhttp.AmzRestore] = completedRestoreObj(opts.Transition.RestoreExpiry).String()
		_, err = es.PutObject(ctx, bucket, object, pReader, ropts)
		return setRestoreHeaderFn(oi, toObjectErr(err, bucket, object))
	}

	uploadID, err := es.NewMultipartUpload(ctx, bucket, object, ropts)
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
		hr, err := hash.NewReader(gr, partInfo.Size, "", "", partInfo.Size)
		if err != nil {
			return setRestoreHeaderFn(oi, err)
		}
		pInfo, err := es.PutObjectPart(ctx, bucket, object, uploadID, partInfo.Number, NewPutObjReader(hr), ObjectOptions{})
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
	_, err = es.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, ObjectOptions{
		MTime: oi.ModTime,
	})
	return setRestoreHeaderFn(oi, err)
}

func (es *erasureSingle) getUploadIDDir(bucket, object, uploadID string) string {
	return pathJoin(es.getMultipartSHADir(bucket, object), uploadID)
}

func (es *erasureSingle) getMultipartSHADir(bucket, object string) string {
	return getSHA256Hash([]byte(pathJoin(bucket, object)))
}

// checkUploadIDExists - verify if a given uploadID exists and is valid.
func (es *erasureSingle) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string) (err error) {
	defer func() {
		if err == errFileNotFound {
			err = errUploadIDNotFound
		}
	}()

	disks := []StorageAPI{es.disk}

	// Read metadata associated with the object from all disks.
	metaArr, errs := readAllFileInfo(ctx, disks, minioMetaMultipartBucket, es.getUploadIDDir(bucket, object, uploadID), "", false)

	readQuorum, _, err := objectQuorumFromMeta(ctx, metaArr, errs, 0)
	if err != nil {
		return err
	}

	if reducedErr := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum); reducedErr != nil {
		return reducedErr
	}

	// List all online disks.
	_, modTime := listOnlineDisks(disks, metaArr, errs)

	// Pick latest valid metadata.
	_, err = pickValidFileInfo(ctx, metaArr, modTime, readQuorum)
	return err
}

// Removes part given by partName belonging to a mulitpart upload from minioMetaBucket
func (es *erasureSingle) removeObjectPart(bucket, object, uploadID, dataDir string, partNumber int) {
	uploadIDPath := es.getUploadIDDir(bucket, object, uploadID)
	curpartPath := pathJoin(uploadIDPath, dataDir, fmt.Sprintf("part.%d", partNumber))
	storageDisks := []StorageAPI{es.disk}

	g := errgroup.WithNErrs(len(storageDisks))
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		index := index
		g.Go(func() error {
			// Ignoring failure to remove parts that weren't present in CompleteMultipartUpload
			// requests. xl.meta is the authoritative source of truth on which parts constitute
			// the object. The presence of parts that don't belong in the object doesn't affect correctness.
			_ = storageDisks[index].Delete(context.TODO(), minioMetaMultipartBucket, curpartPath, DeleteOptions{
				Recursive: false,
				Force:     false,
			})
			return nil
		}, index)
	}
	g.Wait()
}

// Remove the old multipart uploads on the given disk.
func (es *erasureSingle) cleanupStaleUploadsOnDisk(ctx context.Context, disk StorageAPI, expiry time.Duration) {
	now := time.Now()
	diskPath := disk.Endpoint().Path

	readDirFn(pathJoin(diskPath, minioMetaMultipartBucket), func(shaDir string, typ os.FileMode) error {
		return readDirFn(pathJoin(diskPath, minioMetaMultipartBucket, shaDir), func(uploadIDDir string, typ os.FileMode) error {
			uploadIDPath := pathJoin(shaDir, uploadIDDir)
			fi, err := disk.ReadVersion(ctx, minioMetaMultipartBucket, uploadIDPath, "", false)
			if err != nil {
				return nil
			}
			wait := es.deletedCleanupSleeper.Timer(ctx)
			if now.Sub(fi.ModTime) > expiry {
				es.disk.RenameFile(context.Background(), minioMetaMultipartBucket, uploadIDPath, minioMetaTmpDeletedBucket, mustGetUUID())
			}
			wait()
			return nil
		})
	})

	readDirFn(pathJoin(diskPath, minioMetaTmpBucket), func(tmpDir string, typ os.FileMode) error {
		if tmpDir == ".trash/" { // do not remove .trash/ here, it has its own routines
			return nil
		}
		vi, err := disk.StatVol(ctx, pathJoin(minioMetaTmpBucket, tmpDir))
		if err != nil {
			return nil
		}
		wait := es.deletedCleanupSleeper.Timer(ctx)
		if now.Sub(vi.Created) > expiry {
			disk.Delete(ctx, minioMetaTmpBucket, tmpDir, DeleteOptions{
				Recursive: true,
				Force:     false,
			})
		}
		wait()
		return nil
	})
}

// ListMultipartUploads - lists all the pending multipart
// uploads for a particular object in a bucket.
//
// Implements minimal S3 compatible ListMultipartUploads API. We do
// not support prefix based listing, this is a deliberate attempt
// towards simplification of multipart APIs.
// The resulting ListMultipartsInfo structure is unmarshalled directly as XML.
func (es *erasureSingle) ListMultipartUploads(ctx context.Context, bucket, object, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	if err := checkListMultipartArgs(ctx, bucket, object, keyMarker, uploadIDMarker, delimiter, es); err != nil {
		return ListMultipartsInfo{}, err
	}

	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = object
	result.Delimiter = delimiter

	uploadIDs, err := es.disk.ListDir(ctx, minioMetaMultipartBucket, es.getMultipartSHADir(bucket, object), -1)
	if err != nil {
		if err == errFileNotFound {
			return result, nil
		}
		logger.LogIf(ctx, err)
		return result, toObjectErr(err, bucket, object)
	}

	for i := range uploadIDs {
		uploadIDs[i] = strings.TrimSuffix(uploadIDs[i], SlashSeparator)
	}

	// S3 spec says uploadIDs should be sorted based on initiated time, we need
	// to read the metadata entry.
	var uploads []MultipartInfo

	populatedUploadIds := set.NewStringSet()

	for _, uploadID := range uploadIDs {
		if populatedUploadIds.Contains(uploadID) {
			continue
		}
		fi, err := es.disk.ReadVersion(ctx, minioMetaMultipartBucket, pathJoin(es.getUploadIDDir(bucket, object, uploadID)), "", false)
		if err != nil {
			return result, toObjectErr(err, bucket, object)
		}
		populatedUploadIds.Add(uploadID)
		uploads = append(uploads, MultipartInfo{
			Object:    object,
			UploadID:  uploadID,
			Initiated: fi.ModTime,
		})
	}

	sort.Slice(uploads, func(i int, j int) bool {
		return uploads[i].Initiated.Before(uploads[j].Initiated)
	})

	uploadIndex := 0
	if uploadIDMarker != "" {
		for uploadIndex < len(uploads) {
			if uploads[uploadIndex].UploadID != uploadIDMarker {
				uploadIndex++
				continue
			}
			if uploads[uploadIndex].UploadID == uploadIDMarker {
				uploadIndex++
				break
			}
			uploadIndex++
		}
	}
	for uploadIndex < len(uploads) {
		result.Uploads = append(result.Uploads, uploads[uploadIndex])
		result.NextUploadIDMarker = uploads[uploadIndex].UploadID
		uploadIndex++
		if len(result.Uploads) == maxUploads {
			break
		}
	}

	result.IsTruncated = uploadIndex < len(uploads)

	if !result.IsTruncated {
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
	}

	return result, nil
}

// newMultipartUpload - wrapper for initializing a new multipart
// request; returns a unique upload id.
//
// Internally this function creates 'uploads.json' associated for the
// incoming object at
// '.minio.sys/multipart/bucket/object/uploads.json' on all the
// disks. `uploads.json` carries metadata regarding on-going multipart
// operation(s) on the object.
func (es *erasureSingle) newMultipartUpload(ctx context.Context, bucket string, object string, opts ObjectOptions) (string, error) {
	onlineDisks := []StorageAPI{es.disk}
	parityDrives := 0
	dataDrives := len(onlineDisks) - parityDrives

	// we now know the number of blocks this object needs for data and parity.
	// establish the writeQuorum using this data
	writeQuorum := dataDrives
	if dataDrives == parityDrives {
		writeQuorum++
	}

	// Initialize parts metadata
	partsMetadata := make([]FileInfo, len(onlineDisks))

	fi := newFileInfo(pathJoin(bucket, object), dataDrives, parityDrives)
	fi.VersionID = opts.VersionID
	if opts.Versioned && fi.VersionID == "" {
		fi.VersionID = mustGetUUID()
	}
	fi.DataDir = mustGetUUID()

	// Initialize erasure metadata.
	for index := range partsMetadata {
		partsMetadata[index] = fi
	}

	// Guess content-type from the extension if possible.
	if opts.UserDefined["content-type"] == "" {
		opts.UserDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
	}

	modTime := opts.MTime
	if opts.MTime.IsZero() {
		modTime = UTCNow()
	}

	onlineDisks, partsMetadata = shuffleDisksAndPartsMetadata(onlineDisks, partsMetadata, fi)

	// Fill all the necessary metadata.
	// Update `xl.meta` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Fresh = true
		partsMetadata[index].ModTime = modTime
		partsMetadata[index].Metadata = opts.UserDefined
	}

	uploadID := mustGetUUID()
	uploadIDPath := es.getUploadIDDir(bucket, object, uploadID)

	// Write updated `xl.meta` to all disks.
	if _, err := writeUniqueFileInfo(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum); err != nil {
		return "", toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	// Return success.
	return uploadID, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a
// unique id. The unique id returned here is of UUID form, for each
// subsequent request each UUID is unique.
//
// Implements S3 compatible initiate multipart API.
func (es *erasureSingle) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (string, error) {
	if err := checkNewMultipartArgs(ctx, bucket, object, es); err != nil {
		return "", err
	}

	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}
	return es.newMultipartUpload(ctx, bucket, object, opts)
}

// CopyObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to put object part operation but the source
// data is read from an existing object.
//
// Implements S3 compatible Upload Part Copy API.
func (es *erasureSingle) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (pi PartInfo, e error) {
	partInfo, err := es.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, NewPutObjReader(srcInfo.Reader), dstOpts)
	if err != nil {
		return pi, toObjectErr(err, dstBucket, dstObject)
	}

	// Success.
	return partInfo, nil
}

// PutObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to single put operation but it is part
// of the multipart transaction.
//
// Implements S3 compatible Upload Part API.
func (es *erasureSingle) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *PutObjReader, opts ObjectOptions) (pi PartInfo, err error) {
	if err := checkPutObjectPartArgs(ctx, bucket, object, es); err != nil {
		return PartInfo{}, err
	}

	// Write lock for this part ID.
	// Held throughout the operation.
	partIDLock := es.NewNSLock(bucket, pathJoin(object, uploadID, strconv.Itoa(partID)))
	plkctx, err := partIDLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return PartInfo{}, err
	}
	pctx := plkctx.Context()
	defer partIDLock.Unlock(plkctx.Cancel)

	// Read lock for upload id.
	// Only held while reading the upload metadata.
	uploadIDRLock := es.NewNSLock(bucket, pathJoin(object, uploadID))
	rlkctx, err := uploadIDRLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return PartInfo{}, err
	}
	rctx := rlkctx.Context()
	defer func() {
		if uploadIDRLock != nil {
			uploadIDRLock.RUnlock(rlkctx.Cancel)
		}
	}()

	data := r.Reader
	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(rctx, errInvalidArgument, logger.Application)
		return pi, toObjectErr(errInvalidArgument)
	}

	var partsMetadata []FileInfo
	var errs []error
	uploadIDPath := es.getUploadIDDir(bucket, object, uploadID)

	// Validates if upload ID exists.
	if err = es.checkUploadIDExists(rctx, bucket, object, uploadID); err != nil {
		return pi, toObjectErr(err, bucket, object, uploadID)
	}

	storageDisks := []StorageAPI{es.disk}

	// Read metadata associated with the object from all disks.
	partsMetadata, errs = readAllFileInfo(rctx, storageDisks, minioMetaMultipartBucket,
		uploadIDPath, "", false)

	// Unlock upload id locks before, so others can get it.
	uploadIDRLock.RUnlock(rlkctx.Cancel)
	uploadIDRLock = nil

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(pctx, partsMetadata, errs, 0)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	reducedErr := reduceWriteQuorumErrs(pctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errErasureWriteQuorum {
		return pi, toObjectErr(reducedErr, bucket, object)
	}

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// Pick one from the first valid metadata.
	fi, err := pickValidFileInfo(pctx, partsMetadata, modTime, writeQuorum)
	if err != nil {
		return pi, err
	}

	onlineDisks = shuffleDisks(onlineDisks, fi.Erasure.Distribution)

	// Need a unique name for the part being written in minioMetaBucket to
	// accommodate concurrent PutObjectPart requests

	partSuffix := fmt.Sprintf("part.%d", partID)
	tmpPart := mustGetUUID()
	tmpPartPath := pathJoin(tmpPart, partSuffix)

	// Delete the temporary object part. If PutObjectPart succeeds there would be nothing to delete.
	var online int
	defer func() {
		if online != len(onlineDisks) {
			es.disk.RenameFile(context.Background(), minioMetaTmpBucket, tmpPart, minioMetaTmpDeletedBucket, mustGetUUID())
		}
	}()

	erasure, err := NewErasure(pctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
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
			buffer = es.bp.Get()
			defer es.bp.Put(buffer)
		}
	case size >= fi.Erasure.BlockSize:
		buffer = es.bp.Get()
		defer es.bp.Put(buffer)
	case size < fi.Erasure.BlockSize:
		// No need to allocate fully fi.Erasure.BlockSize buffer if the incoming data is smalles.
		buffer = make([]byte, size, 2*size+int64(fi.Erasure.ParityBlocks+fi.Erasure.DataBlocks-1))
	}

	if len(buffer) > int(fi.Erasure.BlockSize) {
		buffer = buffer[:fi.Erasure.BlockSize]
	}
	writers := make([]io.Writer, len(onlineDisks))
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}
		writers[i] = newBitrotWriter(disk, minioMetaTmpBucket, tmpPartPath, erasure.ShardFileSize(data.Size()), DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	toEncode := io.Reader(data)
	if data.Size() > bigFileThreshold {
		// Add input readahead.
		// We use 2 buffers, so we always have a full buffer of input.
		bufA := es.bp.Get()
		bufB := es.bp.Get()
		defer es.bp.Put(bufA)
		defer es.bp.Put(bufB)
		ra, err := readahead.NewReaderBuffer(data, [][]byte{bufA[:fi.Erasure.BlockSize], bufB[:fi.Erasure.BlockSize]})
		if err == nil {
			toEncode = ra
			defer ra.Close()
		}
	}

	n, err := erasure.Encode(pctx, toEncode, writers, buffer, writeQuorum)
	closeBitrotWriters(writers)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if n < data.Size() {
		return pi, IncompleteBody{Bucket: bucket, Object: object}
	}

	for i := range writers {
		if writers[i] == nil {
			onlineDisks[i] = nil
		}
	}

	// Acquire write lock to update metadata.
	uploadIDWLock := es.NewNSLock(bucket, pathJoin(object, uploadID))
	wlkctx, err := uploadIDWLock.GetLock(pctx, globalOperationTimeout)
	if err != nil {
		return PartInfo{}, err
	}
	wctx := wlkctx.Context()
	defer uploadIDWLock.Unlock(wlkctx.Cancel)

	// Validates if upload ID exists.
	if err = es.checkUploadIDExists(wctx, bucket, object, uploadID); err != nil {
		return pi, toObjectErr(err, bucket, object, uploadID)
	}

	// Rename temporary part file to its final location.
	partPath := pathJoin(uploadIDPath, fi.DataDir, partSuffix)
	onlineDisks, err = renamePart(wctx, onlineDisks, minioMetaTmpBucket, tmpPartPath, minioMetaMultipartBucket, partPath, writeQuorum)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Read metadata again because it might be updated with parallel upload of another part.
	partsMetadata, errs = readAllFileInfo(wctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath, "", false)
	reducedErr = reduceWriteQuorumErrs(wctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errErasureWriteQuorum {
		return pi, toObjectErr(reducedErr, bucket, object)
	}

	// Get current highest version based on re-read partsMetadata.
	onlineDisks, modTime = listOnlineDisks(onlineDisks, partsMetadata, errs)

	// Pick one from the first valid metadata.
	fi, err = pickValidFileInfo(wctx, partsMetadata, modTime, writeQuorum)
	if err != nil {
		return pi, err
	}

	// Once part is successfully committed, proceed with updating erasure metadata.
	fi.ModTime = UTCNow()

	md5hex := r.MD5CurrentHexString()

	var index []byte
	if opts.IndexCB != nil {
		index = opts.IndexCB()
	}

	// Add the current part.
	fi.AddObjectPart(partID, md5hex, n, data.ActualSize(), fi.ModTime, index)

	for i, disk := range onlineDisks {
		if disk == OfflineDisk {
			continue
		}
		partsMetadata[i].Size = fi.Size
		partsMetadata[i].ModTime = fi.ModTime
		partsMetadata[i].Parts = fi.Parts
		partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{
			PartNumber: partID,
			Algorithm:  DefaultBitrotAlgorithm,
			Hash:       bitrotWriterSum(writers[i]),
		})
	}

	// Writes update `xl.meta` format for each disk.
	if _, err = writeUniqueFileInfo(wctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum); err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	online = countOnlineDisks(onlineDisks)

	// Return success.
	return PartInfo{
		PartNumber:   partID,
		ETag:         md5hex,
		LastModified: fi.ModTime,
		Size:         n,
		ActualSize:   data.ActualSize(),
	}, nil
}

// GetMultipartInfo returns multipart metadata uploaded during newMultipartUpload, used
// by callers to verify object states
// - encrypted
// - compressed
func (es *erasureSingle) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	if err := checkListPartsArgs(ctx, bucket, object, es); err != nil {
		return MultipartInfo{}, err
	}

	result := MultipartInfo{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}

	uploadIDLock := es.NewNSLock(bucket, pathJoin(object, uploadID))
	lkctx, err := uploadIDLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return MultipartInfo{}, err
	}
	ctx = lkctx.Context()
	defer uploadIDLock.RUnlock(lkctx.Cancel)

	if err := es.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := es.getUploadIDDir(bucket, object, uploadID)

	storageDisks := []StorageAPI{es.disk}

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, minioMetaMultipartBucket, uploadIDPath, opts.VersionID, false)

	// get Quorum for this object
	readQuorum, _, err := objectQuorumFromMeta(ctx, partsMetadata, errs, 0)
	if err != nil {
		return result, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	reducedErr := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum)
	if reducedErr == errErasureReadQuorum {
		return result, toObjectErr(reducedErr, minioMetaMultipartBucket, uploadIDPath)
	}

	_, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// Pick one from the first valid metadata.
	fi, err := pickValidFileInfo(ctx, partsMetadata, modTime, readQuorum)
	if err != nil {
		return result, err
	}

	result.UserDefined = cloneMSS(fi.Metadata)
	return result, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is marshaled directly into XML and
// replied back to the client.
func (es *erasureSingle) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts ObjectOptions) (result ListPartsInfo, err error) {
	if err := checkListPartsArgs(ctx, bucket, object, es); err != nil {
		return ListPartsInfo{}, err
	}

	uploadIDLock := es.NewNSLock(bucket, pathJoin(object, uploadID))
	lkctx, err := uploadIDLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return ListPartsInfo{}, err
	}
	ctx = lkctx.Context()
	defer uploadIDLock.RUnlock(lkctx.Cancel)

	if err := es.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := es.getUploadIDDir(bucket, object, uploadID)

	storageDisks := []StorageAPI{es.disk}

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, minioMetaMultipartBucket, uploadIDPath, "", false)

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(ctx, partsMetadata, errs, 0)
	if err != nil {
		return result, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	reducedErr := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errErasureWriteQuorum {
		return result, toObjectErr(reducedErr, minioMetaMultipartBucket, uploadIDPath)
	}

	_, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// Pick one from the first valid metadata.
	fi, err := pickValidFileInfo(ctx, partsMetadata, modTime, writeQuorum)
	if err != nil {
		return result, err
	}

	// Populate the result stub.
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker
	result.UserDefined = cloneMSS(fi.Metadata)

	// For empty number of parts or maxParts as zero, return right here.
	if len(fi.Parts) == 0 || maxParts == 0 {
		return result, nil
	}

	// Limit output to maxPartsList.
	if maxParts > maxPartsList {
		maxParts = maxPartsList
	}

	// Only parts with higher part numbers will be listed.
	partIdx := objectPartIndex(fi.Parts, partNumberMarker)
	parts := fi.Parts
	if partIdx != -1 {
		parts = fi.Parts[partIdx+1:]
	}
	count := maxParts
	for _, part := range parts {
		result.Parts = append(result.Parts, PartInfo{
			PartNumber:   part.Number,
			ETag:         part.ETag,
			LastModified: fi.ModTime,
			Size:         part.Size,
		})
		count--
		if count == 0 {
			break
		}
	}
	// If listed entries are more than maxParts, we set IsTruncated as true.
	if len(parts) > len(result.Parts) {
		result.IsTruncated = true
		// Make sure to fill next part number marker if IsTruncated is
		// true for subsequent listing.
		nextPartNumberMarker := result.Parts[len(result.Parts)-1].PartNumber
		result.NextPartNumberMarker = nextPartNumberMarker
	}
	return result, nil
}

// CompleteMultipartUpload - completes an ongoing multipart
// transaction after receiving all the parts indicated by the client.
// Returns an md5sum calculated by concatenating all the individual
// md5sums of all the parts.
//
// Implements S3 compatible Complete multipart API.
func (es *erasureSingle) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, parts []CompletePart, opts ObjectOptions) (oi ObjectInfo, err error) {
	if err = checkCompleteMultipartArgs(ctx, bucket, object, es); err != nil {
		return oi, err
	}

	// Hold read-locks to verify uploaded parts, also disallows
	// parallel part uploads as well.
	uploadIDLock := es.NewNSLock(bucket, pathJoin(object, uploadID))
	rlkctx, err := uploadIDLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return oi, err
	}
	rctx := rlkctx.Context()
	defer uploadIDLock.RUnlock(rlkctx.Cancel)

	if err = es.checkUploadIDExists(rctx, bucket, object, uploadID); err != nil {
		return oi, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := es.getUploadIDDir(bucket, object, uploadID)

	storageDisks := []StorageAPI{es.disk}

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(rctx, storageDisks, minioMetaMultipartBucket, uploadIDPath, "", false)

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(rctx, partsMetadata, errs, 0)
	if err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	reducedErr := reduceWriteQuorumErrs(rctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errErasureWriteQuorum {
		return oi, toObjectErr(reducedErr, bucket, object)
	}

	onlineDisks, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// Pick one from the first valid metadata.
	fi, err := pickValidFileInfo(rctx, partsMetadata, modTime, writeQuorum)
	if err != nil {
		return oi, err
	}

	// Calculate full object size.
	var objectSize int64

	// Calculate consolidated actual size.
	var objectActualSize int64

	// Order online disks in accordance with distribution order.
	// Order parts metadata in accordance with distribution order.
	onlineDisks, partsMetadata = shuffleDisksAndPartsMetadataByIndex(onlineDisks, partsMetadata, fi)

	// Save current erasure metadata for validation.
	currentFI := fi

	// Allocate parts similar to incoming slice.
	fi.Parts = make([]ObjectPartInfo, len(parts))

	// Validate each part and then commit to disk.
	for i, part := range parts {
		partIdx := objectPartIndex(currentFI.Parts, part.PartNumber)
		// All parts should have same part number.
		if partIdx == -1 {
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				GotETag:    part.ETag,
			}
			return oi, invp
		}

		// ensure that part ETag is canonicalized to strip off extraneous quotes
		part.ETag = canonicalizeETag(part.ETag)
		if currentFI.Parts[partIdx].ETag != part.ETag {
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				ExpETag:    currentFI.Parts[partIdx].ETag,
				GotETag:    part.ETag,
			}
			return oi, invp
		}

		// All parts except the last part has to be atleast 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(currentFI.Parts[partIdx].ActualSize) {
			return oi, PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   currentFI.Parts[partIdx].ActualSize,
				PartETag:   part.ETag,
			}
		}

		// Save for total object size.
		objectSize += currentFI.Parts[partIdx].Size

		// Save the consolidated actual size.
		objectActualSize += currentFI.Parts[partIdx].ActualSize

		// Add incoming parts.
		fi.Parts[i] = ObjectPartInfo{
			Number:     part.PartNumber,
			Size:       currentFI.Parts[partIdx].Size,
			ActualSize: currentFI.Parts[partIdx].ActualSize,
			Index:      currentFI.Parts[partIdx].Index,
		}
	}

	// Save the final object size and modtime.
	fi.Size = objectSize
	fi.ModTime = opts.MTime
	if opts.MTime.IsZero() {
		fi.ModTime = UTCNow()
	}

	// Save successfully calculated md5sum.
	fi.Metadata["etag"] = opts.UserDefined["etag"]
	if fi.Metadata["etag"] == "" {
		fi.Metadata["etag"] = getCompleteMultipartMD5(parts)
	}

	// Save the consolidated actual size.
	fi.Metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)

	// Update all erasure metadata, make sure to not modify fields like
	// checksum which are different on each disks.
	for index := range partsMetadata {
		if partsMetadata[index].IsValid() {
			partsMetadata[index].Size = fi.Size
			partsMetadata[index].ModTime = fi.ModTime
			partsMetadata[index].Metadata = fi.Metadata
			partsMetadata[index].Parts = fi.Parts
		}
	}

	// Hold namespace to complete the transaction
	lk := es.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return oi, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	// Write final `xl.meta` at uploadID location
	onlineDisks, err = writeUniqueFileInfo(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum)
	if err != nil {
		return oi, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	// Remove parts that weren't present in CompleteMultipartUpload request.
	for _, curpart := range currentFI.Parts {
		if objectPartIndex(fi.Parts, curpart.Number) == -1 {
			// Delete the missing part files. e.g,
			// Request 1: NewMultipart
			// Request 2: PutObjectPart 1
			// Request 3: PutObjectPart 2
			// Request 4: CompleteMultipartUpload --part 2
			// N.B. 1st part is not present. This part should be removed from the storage.
			es.removeObjectPart(bucket, object, uploadID, fi.DataDir, curpart.Number)
		}
	}

	// Rename the multipart object to final location.
	if onlineDisks, err = renameData(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath,
		partsMetadata, bucket, object, writeQuorum); err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	for i := 0; i < len(onlineDisks); i++ {
		if onlineDisks[i] != nil && onlineDisks[i].IsOnline() {
			// Object info is the same in all disks, so we can pick
			// the first meta from online disk
			fi = partsMetadata[i]
			break
		}
	}

	// we are adding a new version to this object under the namespace lock, so this is the latest version.
	fi.IsLatest = true

	// Success, return object info.
	return fi.ToObjectInfo(bucket, object, opts.Versioned || opts.VersionSuspended), nil
}

// AbortMultipartUpload - aborts an ongoing multipart operation
// signified by the input uploadID. This is an atomic operation
// doesn't require clients to initiate multiple such requests.
//
// All parts are purged from all disks and reference to the uploadID
// would be removed from the system, rollback is not possible on this
// operation.
func (es *erasureSingle) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (err error) {
	if err = checkAbortMultipartArgs(ctx, bucket, object, es); err != nil {
		return err
	}

	lk := es.NewNSLock(bucket, pathJoin(object, uploadID))
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	// Validates if upload ID exists.
	if err := es.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return toObjectErr(err, bucket, object, uploadID)
	}

	// Cleanup all uploaded parts.
	es.disk.RenameFile(ctx, minioMetaMultipartBucket, es.getUploadIDDir(bucket, object, uploadID), minioMetaTmpDeletedBucket, mustGetUUID())

	// Successfully purged.
	return nil
}

func (es *erasureSingle) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	var loi ListObjectsInfo

	// Automatically remove the object/version is an expiry lifecycle rule can be applied
	lc, _ := globalLifecycleSys.Get(bucket)

	// Check if bucket is object locked.
	rcfg, _ := globalBucketObjectLockSys.Get(bucket)

	if len(prefix) > 0 && maxKeys == 1 && delimiter == "" && marker == "" {
		// Optimization for certain applications like
		// - Cohesity
		// - Actifio, Splunk etc.
		// which send ListObjects requests where the actual object
		// itself is the prefix and max-keys=1 in such scenarios
		// we can simply verify locally if such an object exists
		// to avoid the need for ListObjects().
		objInfo, err := es.GetObjectInfo(ctx, bucket, prefix, ObjectOptions{NoLock: true})
		if err == nil {
			if lc != nil {
				action := evalActionFromLifecycle(ctx, *lc, rcfg, objInfo, false)
				switch action {
				case lifecycle.DeleteVersionAction, lifecycle.DeleteAction:
					fallthrough
				case lifecycle.DeleteRestoredAction, lifecycle.DeleteRestoredVersionAction:
					return loi, nil
				}
			}
			loi.Objects = append(loi.Objects, objInfo)
			return loi, nil
		}
	}

	opts := listPathOptions{
		Bucket:      bucket,
		Prefix:      prefix,
		Separator:   delimiter,
		Limit:       maxKeysPlusOne(maxKeys, marker != ""),
		Marker:      marker,
		InclDeleted: false,
		AskDisks:    globalAPIConfig.getListQuorum(),
		Lifecycle:   lc,
		Retention:   rcfg,
	}

	merged, err := es.listPath(ctx, &opts)
	if err != nil && err != io.EOF {
		if !isErrBucketNotFound(err) {
			logger.LogIf(ctx, err)
		}
		return loi, err
	}

	merged.forwardPast(opts.Marker)
	defer merged.truncate(0) // Release when returning

	// Default is recursive, if delimiter is set then list non recursive.
	objects := merged.fileInfos(bucket, prefix, delimiter)
	loi.IsTruncated = err == nil && len(objects) > 0
	if maxKeys > 0 && len(objects) > maxKeys {
		objects = objects[:maxKeys]
		loi.IsTruncated = true
	}
	for _, obj := range objects {
		if obj.IsDir && obj.ModTime.IsZero() && delimiter != "" {
			loi.Prefixes = append(loi.Prefixes, obj.Name)
		} else {
			loi.Objects = append(loi.Objects, obj)
		}
	}
	if loi.IsTruncated {
		last := objects[len(objects)-1]
		loi.NextMarker = opts.encodeMarker(last.Name)
	}
	return loi, nil
}

func (es *erasureSingle) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error) {
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	loi, err := es.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return ListObjectsV2Info{}, err
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

func (es *erasureSingle) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (ListObjectVersionsInfo, error) {
	loi := ListObjectVersionsInfo{}
	if marker == "" && versionMarker != "" {
		return loi, NotImplemented{}
	}

	opts := listPathOptions{
		Bucket:      bucket,
		Prefix:      prefix,
		Separator:   delimiter,
		Limit:       maxKeysPlusOne(maxKeys, marker != ""),
		Marker:      marker,
		InclDeleted: true,
		AskDisks:    "strict",
		Versioned:   true,
	}

	merged, err := es.listPath(ctx, &opts)
	if err != nil && err != io.EOF {
		return loi, err
	}
	defer merged.truncate(0) // Release when returning
	if versionMarker == "" {
		o := listPathOptions{Marker: marker}
		// If we are not looking for a specific version skip it.

		o.parseMarker()
		merged.forwardPast(o.Marker)
	}
	objects := merged.fileInfoVersions(bucket, prefix, delimiter, versionMarker)
	loi.IsTruncated = err == nil && len(objects) > 0
	if maxKeys > 0 && len(objects) > maxKeys {
		objects = objects[:maxKeys]
		loi.IsTruncated = true
	}
	for _, obj := range objects {
		if obj.IsDir && obj.ModTime.IsZero() && delimiter != "" {
			loi.Prefixes = append(loi.Prefixes, obj.Name)
		} else {
			loi.Objects = append(loi.Objects, obj)
		}
	}
	if loi.IsTruncated {
		last := objects[len(objects)-1]
		loi.NextMarker = opts.encodeMarker(last.Name)
		loi.NextVersionIDMarker = last.VersionID
	}
	return loi, nil
}

// Walk a bucket, optionally prefix recursively, until we have returned
// all the content to objectInfo channel, it is callers responsibility
// to allocate a receive channel for ObjectInfo, upon any unhandled
// error walker returns error. Optionally if context.Done() is received
// then Walk() stops the walker.
func (es *erasureSingle) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo, opts ObjectOptions) error {
	if err := checkListObjsArgs(ctx, bucket, prefix, "", es); err != nil {
		// Upon error close the channel.
		close(results)
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		defer close(results)

		versioned := opts.Versioned || opts.VersionSuspended

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			loadEntry := func(entry metaCacheEntry) {
				if entry.isDir() {
					return
				}

				fivs, err := entry.fileInfoVersions(bucket)
				if err != nil {
					cancel()
					return
				}
				if opts.WalkAscending {
					for i := len(fivs.Versions) - 1; i >= 0; i-- {
						version := fivs.Versions[i]
						results <- version.ToObjectInfo(bucket, version.Name, versioned)
					}
					return
				}
				for _, version := range fivs.Versions {
					results <- version.ToObjectInfo(bucket, version.Name, versioned)
				}
			}

			// How to resolve partial results.
			resolver := metadataResolutionParams{
				dirQuorum: 1,
				objQuorum: 1,
				bucket:    bucket,
			}

			path := baseDirFromPrefix(prefix)
			filterPrefix := strings.Trim(strings.TrimPrefix(prefix, path), slashSeparator)
			if path == prefix {
				filterPrefix = ""
			}

			lopts := listPathRawOptions{
				disks:          []StorageAPI{es.disk},
				bucket:         bucket,
				path:           path,
				filterPrefix:   filterPrefix,
				recursive:      true,
				forwardTo:      "",
				minDisks:       1,
				reportNotFound: false,
				agreed:         loadEntry,
				partial: func(entries metaCacheEntries, _ []error) {
					entry, ok := entries.resolve(&resolver)
					if !ok {
						// check if we can get one entry atleast
						// proceed to heal nonetheless.
						entry, _ = entries.firstFound()
					}

					loadEntry(*entry)
				},
				finished: nil,
			}

			if err := listPathRaw(ctx, lopts); err != nil {
				logger.LogIf(ctx, fmt.Errorf("listPathRaw returned %w: opts(%#v)", err, lopts))
				return
			}
		}()
		wg.Wait()
	}()

	return nil
}

// Health - returns current status of the object layer health, for single drive
// its as simple as returning healthy as long as drive is accessible.
func (es *erasureSingle) Health(ctx context.Context, opts HealthOptions) HealthResult {
	_, err := es.disk.DiskInfo(ctx)
	if err != nil {
		return HealthResult{}
	}
	if opts.Maintenance {
		// Single drive cannot be put under maintenance.
		return HealthResult{
			Healthy:     false,
			WriteQuorum: 1,
		}
	}
	return HealthResult{
		Healthy:     true,
		WriteQuorum: 1,
	}
}

// ReadHealth - returns current status of the object layer health for reads,
// for single drive its as simple as returning healthy as long as drive is accessible.
func (es *erasureSingle) ReadHealth(ctx context.Context) bool {
	res := es.Health(ctx, HealthOptions{})
	return res.Healthy
}

// nsScanner will start scanning buckets and send updated totals as they are traversed.
// Updates are sent on a regular basis and the caller *must* consume them.
func (es *erasureSingle) nsScanner(ctx context.Context, buckets []BucketInfo, bf *bloomFilter, wantCycle uint32, updates chan<- dataUsageCache, healScanMode madmin.HealScanMode) error {
	if len(buckets) == 0 {
		return nil
	}

	// Collect disks we can use.
	disks := []StorageAPI{es.disk}

	// Load bucket totals
	oldCache := dataUsageCache{}
	if err := oldCache.load(ctx, es, dataUsageCacheName); err != nil {
		return err
	}

	// New cache..
	cache := dataUsageCache{
		Info: dataUsageCacheInfo{
			Name:      dataUsageRoot,
			NextCycle: oldCache.Info.NextCycle,
		},
		Cache: make(map[string]dataUsageEntry, len(oldCache.Cache)),
	}
	bloom := bf.bytes()

	// Put all buckets into channel.
	bucketCh := make(chan BucketInfo, len(buckets))
	// Add new buckets first
	for _, b := range buckets {
		if oldCache.find(b.Name) == nil {
			bucketCh <- b
		}
	}

	// Add existing buckets.
	for _, b := range buckets {
		e := oldCache.find(b.Name)
		if e != nil {
			cache.replace(b.Name, dataUsageRoot, *e)
			bucketCh <- b
		}
	}

	close(bucketCh)
	bucketResults := make(chan dataUsageEntryInfo, len(disks))

	// Start async collector/saver.
	// This goroutine owns the cache.
	var saverWg sync.WaitGroup
	saverWg.Add(1)
	go func() {
		// Add jitter to the update time so multiple sets don't sync up.
		updateTime := 30*time.Second + time.Duration(float64(10*time.Second)*rand.Float64())
		t := time.NewTicker(updateTime)
		defer t.Stop()
		defer saverWg.Done()
		var lastSave time.Time

		for {
			select {
			case <-ctx.Done():
				// Return without saving.
				return
			case <-t.C:
				if cache.Info.LastUpdate.Equal(lastSave) {
					continue
				}
				logger.LogIf(ctx, cache.save(ctx, es, dataUsageCacheName))
				updates <- cache.clone()
				lastSave = cache.Info.LastUpdate
			case v, ok := <-bucketResults:
				if !ok {
					// Save final state...
					cache.Info.NextCycle = wantCycle
					cache.Info.LastUpdate = time.Now()
					logger.LogIf(ctx, cache.save(ctx, es, dataUsageCacheName))
					updates <- cache
					return
				}
				cache.replace(v.Name, v.Parent, v.Entry)
				cache.Info.LastUpdate = time.Now()
			}
		}
	}()

	// Shuffle disks to ensure a total randomness of bucket/disk association to ensure
	// that objects that are not present in all disks are accounted and ILM applied.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(disks), func(i, j int) { disks[i], disks[j] = disks[j], disks[i] })

	// Start one scanner per disk
	var wg sync.WaitGroup
	wg.Add(len(disks))
	for i := range disks {
		go func(i int) {
			defer wg.Done()
			disk := disks[i]

			for bucket := range bucketCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Load cache for bucket
				cacheName := pathJoin(bucket.Name, dataUsageCacheName)
				cache := dataUsageCache{}
				logger.LogIf(ctx, cache.load(ctx, es, cacheName))
				if cache.Info.Name == "" {
					cache.Info.Name = bucket.Name
				}
				cache.Info.BloomFilter = bloom
				cache.Info.SkipHealing = true
				cache.Info.NextCycle = wantCycle
				if cache.Info.Name != bucket.Name {
					logger.LogIf(ctx, fmt.Errorf("cache name mismatch: %s != %s", cache.Info.Name, bucket.Name))
					cache.Info = dataUsageCacheInfo{
						Name:       bucket.Name,
						LastUpdate: time.Time{},
						NextCycle:  wantCycle,
					}
				}
				// Collect updates.
				updates := make(chan dataUsageEntry, 1)
				var wg sync.WaitGroup
				wg.Add(1)
				go func(name string) {
					defer wg.Done()
					for update := range updates {
						bucketResults <- dataUsageEntryInfo{
							Name:   name,
							Parent: dataUsageRoot,
							Entry:  update,
						}
					}
				}(cache.Info.Name)
				// Calc usage
				before := cache.Info.LastUpdate
				var err error
				cache, err = disk.NSScanner(ctx, cache, updates, healScanMode)
				cache.Info.BloomFilter = nil
				if err != nil {
					if !cache.Info.LastUpdate.IsZero() && cache.Info.LastUpdate.After(before) {
						logger.LogIf(ctx, cache.save(ctx, es, cacheName))
					} else {
						logger.LogIf(ctx, err)
					}
					// This ensures that we don't close
					// bucketResults channel while the
					// updates-collector goroutine still
					// holds a reference to this.
					wg.Wait()
					continue
				}

				wg.Wait()
				var root dataUsageEntry
				if r := cache.root(); r != nil {
					root = cache.flatten(*r)
				}
				t := time.Now()
				bucketResults <- dataUsageEntryInfo{
					Name:   cache.Info.Name,
					Parent: dataUsageRoot,
					Entry:  root,
				}
				// We want to avoid synchronizing up all writes in case
				// the results are piled up.
				time.Sleep(time.Duration(float64(time.Since(t)) * rand.Float64()))
				// Save cache
				logger.LogIf(ctx, cache.save(ctx, es, cacheName))
			}
		}(i)
	}
	wg.Wait()
	close(bucketResults)
	saverWg.Wait()

	return nil
}

func (es *erasureSingle) NSScanner(ctx context.Context, bf *bloomFilter, updates chan<- DataUsageInfo, wantCycle uint32, healScanMode madmin.HealScanMode) error {
	// Updates must be closed before we return.
	defer close(updates)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make([]dataUsageCache, 1)
	var firstErr error

	allBuckets, err := es.ListBuckets(ctx, BucketOptions{})
	if err != nil {
		return err
	}

	if len(allBuckets) == 0 {
		updates <- DataUsageInfo{} // no buckets found update data usage to reflect latest state
		return nil
	}

	// Scanner latest allBuckets first.
	sort.Slice(allBuckets, func(i, j int) bool {
		return allBuckets[i].Created.After(allBuckets[j].Created)
	})

	wg.Add(1)
	go func() {
		updates := make(chan dataUsageCache, 1)
		defer close(updates)
		// Start update collector.
		go func() {
			defer wg.Done()
			for info := range updates {
				mu.Lock()
				results[0] = info
				mu.Unlock()
			}
		}()

		// Start scanner. Blocks until done.
		err := es.nsScanner(ctx, allBuckets, bf, wantCycle, updates, healScanMode)
		if err != nil {
			logger.LogIf(ctx, err)
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			// Cancel remaining...
			cancel()
			mu.Unlock()
			return
		}
	}()

	updateCloser := make(chan chan struct{})
	go func() {
		updateTicker := time.NewTicker(30 * time.Second)
		defer updateTicker.Stop()
		var lastUpdate time.Time

		// We need to merge since we will get the same buckets from each pool.
		// Therefore to get the exact bucket sizes we must merge before we can convert.
		var allMerged dataUsageCache

		update := func() {
			mu.Lock()
			defer mu.Unlock()

			allMerged = dataUsageCache{Info: dataUsageCacheInfo{Name: dataUsageRoot}}
			for _, info := range results {
				if info.Info.LastUpdate.IsZero() {
					// Not filled yet.
					return
				}
				allMerged.merge(info)
			}
			if allMerged.root() != nil && allMerged.Info.LastUpdate.After(lastUpdate) {
				updates <- allMerged.dui(allMerged.Info.Name, allBuckets)
				lastUpdate = allMerged.Info.LastUpdate
			}
		}
		for {
			select {
			case <-ctx.Done():
				return
			case v := <-updateCloser:
				update()
				close(v)
				return
			case <-updateTicker.C:
				update()
			}
		}
	}()

	wg.Wait()
	ch := make(chan struct{})
	select {
	case updateCloser <- ch:
		<-ch
	case <-ctx.Done():
		if firstErr == nil {
			firstErr = ctx.Err()
		}
	}
	return firstErr
}

// GetRawData will return all files with a given raw path to the callback.
// Errors are ignored, only errors from the callback are returned.
// For now only direct file paths are supported.
func (es *erasureSingle) GetRawData(ctx context.Context, volume, file string, fn func(r io.Reader, host string, disk string, filename string, info StatInfo) error) error {
	found := 0
	stats, err := es.disk.StatInfoFile(ctx, volume, file, true)
	if err != nil {
		return err
	}
	for _, si := range stats {
		found++
		var r io.ReadCloser
		if !si.Dir {
			r, err = es.disk.ReadFileStream(ctx, volume, si.Name, 0, si.Size)
			if err != nil {
				continue
			}
		} else {
			r = io.NopCloser(bytes.NewBuffer([]byte{}))
		}
		// Keep disk path instead of ID, to ensure that the downloaded zip file can be
		// easily automated with `minio server hostname{1...n}/disk{1...m}`.
		err = fn(r, es.disk.Hostname(), es.disk.Endpoint().Path, pathJoin(volume, si.Name), si)
		r.Close()
		if err != nil {
			return err
		}
	}

	if found == 0 {
		return errFileNotFound
	}

	return nil
}
