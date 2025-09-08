// Copyright (c) 2015-2025 MinIO, Inc.
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
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/readahead"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/mimedb"
	"github.com/minio/pkg/v3/sync/errgroup"
	"github.com/minio/sio"
)

func (er erasureObjects) getUploadIDDir(bucket, object, uploadID string) string {
	uploadUUID := uploadID
	uploadBytes, err := base64.RawURLEncoding.DecodeString(uploadID)
	if err == nil {
		slc := strings.SplitN(string(uploadBytes), ".", 2)
		if len(slc) == 2 {
			uploadUUID = slc[1]
		}
	}
	return pathJoin(er.getMultipartSHADir(bucket, object), uploadUUID)
}

func (er erasureObjects) getMultipartSHADir(bucket, object string) string {
	return getSHA256Hash([]byte(pathJoin(bucket, object)))
}

// checkUploadIDExists - verify if a given uploadID exists and is valid.
func (er erasureObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string, write bool) (fi FileInfo, metArr []FileInfo, err error) {
	defer func() {
		if errors.Is(err, errFileNotFound) {
			err = errUploadIDNotFound
		}
	}()

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	storageDisks := er.getDisks()

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, bucket, minioMetaMultipartBucket,
		uploadIDPath, "", false, false)

	readQuorum, writeQuorum, err := objectQuorumFromMeta(ctx, partsMetadata, errs, er.defaultParityCount)
	if err != nil {
		return fi, nil, err
	}

	if readQuorum < 0 {
		return fi, nil, errErasureReadQuorum
	}

	if writeQuorum < 0 {
		return fi, nil, errErasureWriteQuorum
	}

	quorum := readQuorum
	if write {
		quorum = writeQuorum
	}

	// List all online disks.
	_, modTime, etag := listOnlineDisks(storageDisks, partsMetadata, errs, quorum)

	if write {
		err = reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	} else {
		err = reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum)
	}
	if err != nil {
		return fi, nil, err
	}

	// Pick one from the first valid metadata.
	fi, err = pickValidFileInfo(ctx, partsMetadata, modTime, etag, quorum)
	return fi, partsMetadata, err
}

// cleanupMultipartPath removes all extraneous files and parts from the multipart folder, this is used per CompleteMultipart.
// do not use this function outside of completeMultipartUpload()
func (er erasureObjects) cleanupMultipartPath(ctx context.Context, paths ...string) {
	storageDisks := er.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		index := index
		g.Go(func() error {
			_ = storageDisks[index].DeleteBulk(ctx, minioMetaMultipartBucket, paths...)
			return nil
		}, index)
	}
	g.Wait()
}

// Clean-up the old multipart uploads. Should be run in a Go routine.
func (er erasureObjects) cleanupStaleUploads(ctx context.Context) {
	// run multiple cleanup's local to this server.
	var wg sync.WaitGroup
	for _, disk := range er.getLocalDisks() {
		if disk != nil {
			wg.Add(1)
			go func(disk StorageAPI) {
				defer wg.Done()
				er.cleanupStaleUploadsOnDisk(ctx, disk)
			}(disk)
		}
	}
	wg.Wait()
}

func (er erasureObjects) deleteAll(ctx context.Context, bucket, prefix string) {
	var wg sync.WaitGroup
	for _, disk := range er.getDisks() {
		if disk == nil {
			continue
		}
		wg.Add(1)
		go func(disk StorageAPI) {
			defer wg.Done()
			disk.Delete(ctx, bucket, prefix, DeleteOptions{
				Recursive: true,
				Immediate: false,
			})
		}(disk)
	}
	wg.Wait()
}

// Remove the old multipart uploads on the given disk.
func (er erasureObjects) cleanupStaleUploadsOnDisk(ctx context.Context, disk StorageAPI) {
	drivePath := disk.Endpoint().Path

	readDirFn(pathJoin(drivePath, minioMetaMultipartBucket), func(shaDir string, typ os.FileMode) error {
		readDirFn(pathJoin(drivePath, minioMetaMultipartBucket, shaDir), func(uploadIDDir string, typ os.FileMode) error {
			uploadIDPath := pathJoin(shaDir, uploadIDDir)
			var modTime time.Time
			// Upload IDs are of the form base64_url(<UUID>x<UnixNano>), we can extract the time from the UUID.
			if b64, err := base64.RawURLEncoding.DecodeString(uploadIDDir); err == nil {
				if split := strings.Split(string(b64), "x"); len(split) == 2 {
					t, err := strconv.ParseInt(split[1], 10, 64)
					if err == nil {
						modTime = time.Unix(0, t)
					}
				}
			}
			// Fallback for older uploads without time in the ID.
			if modTime.IsZero() {
				wait := deleteMultipartCleanupSleeper.Timer(ctx)
				fi, err := disk.ReadVersion(ctx, "", minioMetaMultipartBucket, uploadIDPath, "", ReadOptions{})
				if err != nil {
					return nil
				}
				modTime = fi.ModTime
				wait()
			}
			if time.Since(modTime) < globalAPIConfig.getStaleUploadsExpiry() {
				return nil
			}
			w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
			return w.Run(func() error {
				wait := deleteMultipartCleanupSleeper.Timer(ctx)
				pathUUID := mustGetUUID()
				targetPath := pathJoin(drivePath, minioMetaTmpDeletedBucket, pathUUID)
				renameAll(pathJoin(drivePath, minioMetaMultipartBucket, uploadIDPath), targetPath, pathJoin(drivePath, minioMetaBucket))
				wait()
				return nil
			})
		})
		// Get the modtime of the shaDir.
		vi, err := disk.StatVol(ctx, pathJoin(minioMetaMultipartBucket, shaDir))
		if err != nil {
			return nil
		}
		// Modtime is returned in the Created field. See (*xlStorage).StatVol
		if time.Since(vi.Created) < globalAPIConfig.getStaleUploadsExpiry() {
			return nil
		}
		w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
		return w.Run(func() error {
			wait := deleteMultipartCleanupSleeper.Timer(ctx)
			pathUUID := mustGetUUID()
			targetPath := pathJoin(drivePath, minioMetaTmpDeletedBucket, pathUUID)

			// We are not deleting shaDir recursively here, if shaDir is empty
			// and its older then we can happily delete it.
			Rename(pathJoin(drivePath, minioMetaMultipartBucket, shaDir), targetPath)
			wait()
			return nil
		})
	})

	readDirFn(pathJoin(drivePath, minioMetaTmpBucket), func(tmpDir string, typ os.FileMode) error {
		if strings.HasPrefix(tmpDir, ".trash") {
			// do not remove .trash/ here, it has its own routines
			return nil
		}
		vi, err := disk.StatVol(ctx, pathJoin(minioMetaTmpBucket, tmpDir))
		if err != nil {
			return nil
		}
		w := xioutil.NewDeadlineWorker(globalDriveConfig.GetMaxTimeout())
		return w.Run(func() error {
			wait := deleteMultipartCleanupSleeper.Timer(ctx)
			if time.Since(vi.Created) > globalAPIConfig.getStaleUploadsExpiry() {
				pathUUID := mustGetUUID()
				targetPath := pathJoin(drivePath, minioMetaTmpDeletedBucket, pathUUID)

				renameAll(pathJoin(drivePath, minioMetaTmpBucket, tmpDir), targetPath, pathJoin(drivePath, minioMetaBucket))
			}
			wait()
			return nil
		})
	})
}

// ListMultipartUploads - lists all the pending multipart
// uploads for a particular object in a bucket.
//
// Implements minimal S3 compatible ListMultipartUploads API. We do
// not support prefix based listing, this is a deliberate attempt
// towards simplification of multipart APIs.
// The resulting ListMultipartsInfo structure is unmarshalled directly as XML.
func (er erasureObjects) ListMultipartUploads(ctx context.Context, bucket, object, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	auditObjectErasureSet(ctx, "ListMultipartUploads", object, &er)

	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = object
	result.Delimiter = delimiter

	var uploadIDs []string
	var disk StorageAPI
	disks := er.getOnlineLocalDisks()
	if len(disks) == 0 {
		// If no local, get non-healing disks.
		var ok bool
		if disks, ok = er.getOnlineDisksWithHealing(false); !ok {
			disks = er.getOnlineDisks()
		}
	}

	for _, disk = range disks {
		if disk == nil {
			continue
		}
		if !disk.IsOnline() {
			continue
		}
		uploadIDs, err = disk.ListDir(ctx, bucket, minioMetaMultipartBucket, er.getMultipartSHADir(bucket, object), -1)
		if err != nil {
			if errors.Is(err, errDiskNotFound) {
				continue
			}
			if errors.Is(err, errFileNotFound) {
				return result, nil
			}
			return result, toObjectErr(err, bucket, object)
		}
		break
	}

	for i := range uploadIDs {
		uploadIDs[i] = strings.TrimSuffix(uploadIDs[i], SlashSeparator)
	}

	// S3 spec says uploadIDs should be sorted based on initiated time, we need
	// to read the metadata entry.
	var uploads []MultipartInfo

	populatedUploadIDs := set.NewStringSet()

	for _, uploadID := range uploadIDs {
		if populatedUploadIDs.Contains(uploadID) {
			continue
		}
		// If present, use time stored in ID.
		startTime := time.Now()
		if split := strings.Split(uploadID, "x"); len(split) == 2 {
			t, err := strconv.ParseInt(split[1], 10, 64)
			if err == nil {
				startTime = time.Unix(0, t)
			}
		}
		uploads = append(uploads, MultipartInfo{
			Bucket:    bucket,
			Object:    object,
			UploadID:  base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, "%s.%s", globalDeploymentID(), uploadID)),
			Initiated: startTime,
		})
		populatedUploadIDs.Add(uploadID)
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
func (er erasureObjects) newMultipartUpload(ctx context.Context, bucket string, object string, opts ObjectOptions) (*NewMultipartUploadResult, error) {
	if opts.CheckPrecondFn != nil {
		if !opts.NoLock {
			ns := er.NewNSLock(bucket, object)
			lkctx, err := ns.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			defer ns.Unlock(lkctx)
			opts.NoLock = true
		}

		obj, err := er.getObjectInfo(ctx, bucket, object, opts)
		if err == nil && opts.CheckPrecondFn(obj) {
			return nil, PreConditionFailed{}
		}
		if err != nil && !isErrVersionNotFound(err) && !isErrObjectNotFound(err) && !isErrReadQuorum(err) {
			return nil, err
		}

		// if object doesn't exist return error for If-Match conditional requests
		// If-None-Match should be allowed to proceed for non-existent objects
		if err != nil && opts.HasIfMatch && (isErrObjectNotFound(err) || isErrVersionNotFound(err)) {
			return nil, err
		}
	}

	userDefined := cloneMSS(opts.UserDefined)
	if opts.PreserveETag != "" {
		userDefined["etag"] = opts.PreserveETag
	}
	onlineDisks := er.getDisks()

	// Get parity and data drive count based on storage class metadata
	parityDrives := globalStorageClass.GetParityForSC(userDefined[xhttp.AmzStorageClass])
	if parityDrives < 0 {
		parityDrives = er.defaultParityCount
	}

	if globalStorageClass.AvailabilityOptimized() {
		// If we have offline disks upgrade the number of erasure codes for this object.
		parityOrig := parityDrives

		var offlineDrives int
		for _, disk := range onlineDisks {
			if disk == nil || !disk.IsOnline() {
				parityDrives++
				offlineDrives++
				continue
			}
		}

		if offlineDrives >= (len(onlineDisks)+1)/2 {
			// if offline drives are more than 50% of the drives
			// we have no quorum, we shouldn't proceed just
			// fail at that point.
			return nil, toObjectErr(errErasureWriteQuorum, bucket, object)
		}

		if parityDrives >= len(onlineDisks)/2 {
			parityDrives = len(onlineDisks) / 2
		}

		if parityOrig != parityDrives {
			userDefined[minIOErasureUpgraded] = strconv.Itoa(parityOrig) + "->" + strconv.Itoa(parityDrives)
		}
	}

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

	if ckSum := userDefined[ReplicationSsecChecksumHeader]; ckSum != "" {
		v, err := base64.StdEncoding.DecodeString(ckSum)
		if err == nil {
			fi.Checksum = v
		}
		delete(userDefined, ReplicationSsecChecksumHeader)
	}

	// Initialize erasure metadata.
	for index := range partsMetadata {
		partsMetadata[index] = fi
	}

	// Guess content-type from the extension if possible.
	if userDefined["content-type"] == "" {
		userDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
	}

	// if storageClass is standard no need to save it as part of metadata.
	if userDefined[xhttp.AmzStorageClass] == storageclass.STANDARD {
		delete(userDefined, xhttp.AmzStorageClass)
	}

	if opts.WantChecksum != nil && opts.WantChecksum.Type.IsSet() {
		userDefined[hash.MinIOMultipartChecksum] = opts.WantChecksum.Type.String()
		userDefined[hash.MinIOMultipartChecksumType] = opts.WantChecksum.Type.ObjType()
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
		partsMetadata[index].Metadata = userDefined
	}
	uploadUUID := fmt.Sprintf("%sx%d", mustGetUUID(), modTime.UnixNano())
	uploadID := base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, "%s.%s", globalDeploymentID(), uploadUUID))
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadUUID)

	// Write updated `xl.meta` to all disks.
	if _, err := writeAllMetadata(ctx, onlineDisks, bucket, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum); err != nil {
		return nil, toObjectErr(err, bucket, object)
	}

	return &NewMultipartUploadResult{
		UploadID:     uploadID,
		ChecksumAlgo: userDefined[hash.MinIOMultipartChecksum],
		ChecksumType: userDefined[hash.MinIOMultipartChecksumType],
	}, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a
// unique id. The unique id returned here is of UUID form, for each
// subsequent request each UUID is unique.
//
// Implements S3 compatible initiate multipart API.
func (er erasureObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (*NewMultipartUploadResult, error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "NewMultipartUpload", object, &er)
	}

	return er.newMultipartUpload(ctx, bucket, object, opts)
}

// renamePart - renames multipart part to its relevant location under uploadID.
func (er erasureObjects) renamePart(ctx context.Context, disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, optsMeta []byte, writeQuorum int, skipParent string) ([]StorageAPI, error) {
	paths := []string{
		dstEntry,
		dstEntry + ".meta",
	}

	// cleanup existing paths first across all drives.
	er.cleanupMultipartPath(ctx, paths...)

	g := errgroup.WithNErrs(len(disks))

	// Rename file on all underlying storage disks.
	for index := range disks {
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			return disks[index].RenamePart(ctx, srcBucket, srcEntry, dstBucket, dstEntry, optsMeta, skipParent)
		}, index)
	}

	// Wait for all renames to finish.
	errs := g.Wait()

	err := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if err != nil {
		er.cleanupMultipartPath(ctx, paths...)
	}

	// We can safely allow RenameFile errors up to len(er.getDisks()) - writeQuorum
	// otherwise return failure. Cleanup successful renames.
	return evalDisks(disks, errs), err
}

// PutObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to single put operation but it is part
// of the multipart transaction.
//
// Implements S3 compatible Upload Part API.
func (er erasureObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *PutObjReader, opts ObjectOptions) (pi PartInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "PutObjectPart", object, &er)
	}

	data := r.Reader
	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		bugLogIf(ctx, errInvalidArgument, logger.ErrorKind)
		return pi, toObjectErr(errInvalidArgument)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	// Validates if upload ID exists.
	fi, _, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, true)
	if err != nil {
		if errors.Is(err, errVolumeNotFound) {
			return pi, toObjectErr(err, bucket)
		}
		return pi, toObjectErr(err, bucket, object, uploadID)
	}

	onlineDisks := er.getDisks()
	writeQuorum := fi.WriteQuorum(er.defaultWQuorum())

	if cs := fi.Metadata[hash.MinIOMultipartChecksum]; cs != "" {
		if r.ContentCRCType().String() != cs {
			return pi, InvalidArgument{
				Bucket: bucket,
				Object: fi.Name,
				Err:    fmt.Errorf("checksum missing, want %q, got %q", cs, r.ContentCRCType().String()),
			}
		}
	}
	onlineDisks = shuffleDisks(onlineDisks, fi.Erasure.Distribution)

	// Need a unique name for the part being written in minioMetaBucket to
	// accommodate concurrent PutObjectPart requests

	partSuffix := fmt.Sprintf("part.%d", partID)
	// Random UUID and timestamp for temporary part file.
	tmpPart := fmt.Sprintf("%sx%d", mustGetUUID(), time.Now().UnixNano())
	tmpPartPath := pathJoin(tmpPart, partSuffix)

	// Delete the temporary object part. If PutObjectPart succeeds there would be nothing to delete.
	defer func() {
		if countOnlineDisks(onlineDisks) != len(onlineDisks) {
			er.deleteAll(context.Background(), minioMetaTmpBucket, tmpPart)
		}
	}()

	erasure, err := NewErasure(ctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch size := data.Size(); {
	case size == 0:
		buffer = make([]byte, 1) // Allocate at least a byte to reach EOF
	case size >= fi.Erasure.BlockSize || size == -1:
		if int64(globalBytePoolCap.Load().Width()) < fi.Erasure.BlockSize {
			buffer = make([]byte, fi.Erasure.BlockSize, 2*fi.Erasure.BlockSize)
		} else {
			buffer = globalBytePoolCap.Load().Get()
			defer globalBytePoolCap.Load().Put(buffer)
		}
	case size < fi.Erasure.BlockSize:
		// No need to allocate fully fi.Erasure.BlockSize buffer if the incoming data is smaller.
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
		writers[i] = newBitrotWriter(disk, bucket, minioMetaTmpBucket, tmpPartPath, erasure.ShardFileSize(data.Size()), DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	toEncode := io.Reader(data)
	if data.Size() > bigFileThreshold {
		// Add input readahead.
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
	}

	n, err := erasure.Encode(ctx, toEncode, writers, buffer, writeQuorum)
	closeErrs := closeBitrotWriters(writers)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}
	if closeErr := reduceWriteQuorumErrs(ctx, closeErrs, objectOpIgnoredErrs, writeQuorum); closeErr != nil {
		return pi, toObjectErr(closeErr, bucket, object)
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

	// Rename temporary part file to its final location.
	partPath := pathJoin(uploadIDPath, fi.DataDir, partSuffix)

	md5hex := r.MD5CurrentHexString()
	if opts.PreserveETag != "" {
		md5hex = opts.PreserveETag
	}

	var index []byte
	if opts.IndexCB != nil {
		index = opts.IndexCB()
	}

	actualSize := data.ActualSize()
	if actualSize < 0 {
		_, encrypted := crypto.IsEncrypted(fi.Metadata)
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

	partInfo := ObjectPartInfo{
		Number:     partID,
		ETag:       md5hex,
		Size:       n,
		ActualSize: actualSize,
		ModTime:    UTCNow(),
		Index:      index,
		Checksums:  r.ContentCRC(),
	}

	partFI, err := partInfo.MarshalMsg(nil)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Serialize concurrent part uploads.
	partIDLock := er.NewNSLock(bucket, pathJoin(object, uploadID, strconv.Itoa(partID)))
	plkctx, err := partIDLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return PartInfo{}, err
	}

	ctx = plkctx.Context()
	defer partIDLock.Unlock(plkctx)

	// Read lock for upload id, only held while reading the upload metadata.
	uploadIDRLock := er.NewNSLock(bucket, pathJoin(object, uploadID))
	rlkctx, err := uploadIDRLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return PartInfo{}, err
	}
	ctx = rlkctx.Context()
	defer uploadIDRLock.RUnlock(rlkctx)

	onlineDisks, err = er.renamePart(ctx, onlineDisks, minioMetaTmpBucket, tmpPartPath, minioMetaMultipartBucket, partPath, partFI, writeQuorum, uploadIDPath)
	if err != nil {
		if errors.Is(err, errUploadIDNotFound) {
			return pi, toObjectErr(errUploadIDNotFound, bucket, object, uploadID)
		}
		if errors.Is(err, errFileNotFound) {
			// An in-quorum errFileNotFound means that client stream
			// prematurely closed and we do not find any xl.meta or
			// part.1's - in such a scenario we must return as if client
			// disconnected. This means that erasure.Encode() CreateFile()
			// did not do anything.
			return pi, IncompleteBody{Bucket: bucket, Object: object}
		}

		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Return success.
	return PartInfo{
		PartNumber:        partInfo.Number,
		ETag:              partInfo.ETag,
		LastModified:      partInfo.ModTime,
		Size:              partInfo.Size,
		ActualSize:        partInfo.ActualSize,
		ChecksumCRC32:     partInfo.Checksums["CRC32"],
		ChecksumCRC32C:    partInfo.Checksums["CRC32C"],
		ChecksumSHA1:      partInfo.Checksums["SHA1"],
		ChecksumSHA256:    partInfo.Checksums["SHA256"],
		ChecksumCRC64NVME: partInfo.Checksums["CRC64NVME"],
	}, nil
}

// GetMultipartInfo returns multipart metadata uploaded during newMultipartUpload, used
// by callers to verify object states
// - encrypted
// - compressed
// Does not contain currently uploaded parts by design.
func (er erasureObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "GetMultipartInfo", object, &er)
	}

	result := MultipartInfo{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}

	fi, _, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, false)
	if err != nil {
		if errors.Is(err, errVolumeNotFound) {
			return result, toObjectErr(err, bucket)
		}
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	result.UserDefined = cloneMSS(fi.Metadata)
	return result, nil
}

func (er erasureObjects) listParts(ctx context.Context, onlineDisks []StorageAPI, partPath string, readQuorum int) ([]int, error) {
	g := errgroup.WithNErrs(len(onlineDisks))

	objectParts := make([][]string, len(onlineDisks))
	// List uploaded parts from drives.
	for index := range onlineDisks {
		g.Go(func() (err error) {
			if onlineDisks[index] == nil {
				return errDiskNotFound
			}
			objectParts[index], err = onlineDisks[index].ListDir(ctx, minioMetaMultipartBucket, minioMetaMultipartBucket, partPath, -1)
			return err
		}, index)
	}

	if err := reduceReadQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, readQuorum); err != nil {
		return nil, err
	}

	partQuorumMap := make(map[int]int)
	for _, driveParts := range objectParts {
		partsWithMetaCount := make(map[int]int, len(driveParts))
		// part files can be either part.N or part.N.meta
		for _, partPath := range driveParts {
			var partNum int
			if _, err := fmt.Sscanf(partPath, "part.%d", &partNum); err == nil {
				partsWithMetaCount[partNum]++
				continue
			}
			if _, err := fmt.Sscanf(partPath, "part.%d.meta", &partNum); err == nil {
				partsWithMetaCount[partNum]++
			}
		}
		// Include only part.N.meta files with corresponding part.N
		for partNum, cnt := range partsWithMetaCount {
			if cnt < 2 {
				continue
			}
			partQuorumMap[partNum]++
		}
	}

	var partNums []int
	for partNum, count := range partQuorumMap {
		if count < readQuorum {
			continue
		}
		partNums = append(partNums, partNum)
	}

	sort.Ints(partNums)
	return partNums, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is marshaled directly into XML and
// replied back to the client.
func (er erasureObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts ObjectOptions) (result ListPartsInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "ListObjectParts", object, &er)
	}

	fi, _, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, false)
	if err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	if partNumberMarker < 0 {
		partNumberMarker = 0
	}

	// Limit output to maxPartsList.
	if maxParts > maxPartsList {
		maxParts = maxPartsList
	}

	// Populate the result stub.
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker
	result.UserDefined = cloneMSS(fi.Metadata)
	result.ChecksumAlgorithm = fi.Metadata[hash.MinIOMultipartChecksum]
	result.ChecksumType = fi.Metadata[hash.MinIOMultipartChecksumType]

	if maxParts == 0 {
		return result, nil
	}

	onlineDisks := er.getDisks()
	readQuorum := fi.ReadQuorum(er.defaultRQuorum())
	// Read Part info for all parts
	partPath := pathJoin(uploadIDPath, fi.DataDir) + SlashSeparator

	// List parts in quorum
	partNums, err := er.listParts(ctx, onlineDisks, partPath, readQuorum)
	if err != nil {
		// This means that fi.DataDir, is not yet populated so we
		// return an empty response.
		if errors.Is(err, errFileNotFound) {
			return result, nil
		}
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	if len(partNums) == 0 {
		return result, nil
	}

	start := objectPartIndexNums(partNums, partNumberMarker)
	if partNumberMarker > 0 && start == -1 {
		// Marker not present among what is present on the
		// server, we return an empty list.
		return result, nil
	}

	if partNumberMarker > 0 && start != -1 {
		if start+1 >= len(partNums) {
			// Marker indicates that we are the end
			// of the list, so we simply return empty
			return result, nil
		}

		partNums = partNums[start+1:]
	}

	result.Parts = make([]PartInfo, 0, len(partNums))
	partMetaPaths := make([]string, len(partNums))
	for i, part := range partNums {
		partMetaPaths[i] = pathJoin(partPath, fmt.Sprintf("part.%d.meta", part))
	}

	// Read parts in quorum
	objParts, err := readParts(ctx, onlineDisks, minioMetaMultipartBucket, partMetaPaths,
		partNums, readQuorum)
	if err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	count := maxParts
	for _, objPart := range objParts {
		result.Parts = append(result.Parts, PartInfo{
			PartNumber:        objPart.Number,
			LastModified:      objPart.ModTime,
			ETag:              objPart.ETag,
			Size:              objPart.Size,
			ActualSize:        objPart.ActualSize,
			ChecksumCRC32:     objPart.Checksums["CRC32"],
			ChecksumCRC32C:    objPart.Checksums["CRC32C"],
			ChecksumSHA1:      objPart.Checksums["SHA1"],
			ChecksumSHA256:    objPart.Checksums["SHA256"],
			ChecksumCRC64NVME: objPart.Checksums["CRC64NVME"],
		})
		count--
		if count == 0 {
			break
		}
	}

	if len(objParts) > len(result.Parts) {
		result.IsTruncated = true
		// Make sure to fill next part number marker if IsTruncated is true for subsequent listing.
		result.NextPartNumberMarker = result.Parts[len(result.Parts)-1].PartNumber
	}

	return result, nil
}

func readParts(ctx context.Context, disks []StorageAPI, bucket string, partMetaPaths []string, partNumbers []int, readQuorum int) ([]ObjectPartInfo, error) {
	g := errgroup.WithNErrs(len(disks))

	objectPartInfos := make([][]*ObjectPartInfo, len(disks))
	// Rename file on all underlying storage disks.
	for index := range disks {
		g.Go(func() (err error) {
			if disks[index] == nil {
				return errDiskNotFound
			}
			objectPartInfos[index], err = disks[index].ReadParts(ctx, bucket, partMetaPaths...)
			return err
		}, index)
	}

	if err := reduceReadQuorumErrs(ctx, g.Wait(), objectOpIgnoredErrs, readQuorum); err != nil {
		return nil, err
	}

	partInfosInQuorum := make([]ObjectPartInfo, len(partMetaPaths))
	for pidx := range partMetaPaths {
		// partMetaQuorumMap uses
		//  - path/to/part.N as key to collate errors from failed drives.
		//  - part ETag to collate part metadata
		partMetaQuorumMap := make(map[string]int, len(partNumbers))
		var pinfos []*ObjectPartInfo
		for idx := range disks {
			if len(objectPartInfos[idx]) != len(partMetaPaths) {
				partMetaQuorumMap[partMetaPaths[pidx]]++
				continue
			}

			pinfo := objectPartInfos[idx][pidx]
			if pinfo != nil && pinfo.ETag != "" {
				pinfos = append(pinfos, pinfo)
				partMetaQuorumMap[pinfo.ETag]++
				continue
			}
			partMetaQuorumMap[partMetaPaths[pidx]]++
		}

		var maxQuorum int
		var maxETag string
		var maxPartMeta string
		for etag, quorum := range partMetaQuorumMap {
			if maxQuorum < quorum {
				maxQuorum = quorum
				maxETag = etag
				maxPartMeta = etag
			}
		}
		// found is a representative ObjectPartInfo which either has the maximally occurring ETag or an error.
		var found *ObjectPartInfo
		for _, pinfo := range pinfos {
			if pinfo == nil {
				continue
			}
			if maxETag != "" && pinfo.ETag == maxETag {
				found = pinfo
				break
			}
			if pinfo.ETag == "" && maxPartMeta != "" && path.Base(maxPartMeta) == fmt.Sprintf("part.%d.meta", pinfo.Number) {
				found = pinfo
				break
			}
		}

		if found != nil && found.ETag != "" && partMetaQuorumMap[maxETag] >= readQuorum {
			partInfosInQuorum[pidx] = *found
			continue
		}
		partInfosInQuorum[pidx] = ObjectPartInfo{
			Number: partNumbers[pidx],
			Error: InvalidPart{
				PartNumber: partNumbers[pidx],
			}.Error(),
		}
	}
	return partInfosInQuorum, nil
}

func objPartToPartErr(part ObjectPartInfo) error {
	if strings.Contains(part.Error, "file not found") {
		return InvalidPart{PartNumber: part.Number}
	}
	if strings.Contains(part.Error, "Specified part could not be found") {
		return InvalidPart{PartNumber: part.Number}
	}
	if strings.Contains(part.Error, errErasureReadQuorum.Error()) {
		return errErasureReadQuorum
	}
	return errors.New(part.Error)
}

// CompleteMultipartUpload - completes an ongoing multipart
// transaction after receiving all the parts indicated by the client.
// Returns an md5sum calculated by concatenating all the individual
// md5sums of all the parts.
//
// Implements S3 compatible Complete multipart API.
func (er erasureObjects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, parts []CompletePart, opts ObjectOptions) (oi ObjectInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "CompleteMultipartUpload", object, &er)
	}

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
			return ObjectInfo{}, PreConditionFailed{}
		}
		if err != nil && !isErrVersionNotFound(err) && !isErrObjectNotFound(err) && !isErrReadQuorum(err) {
			return ObjectInfo{}, err
		}

		// if object doesn't exist return error for If-Match conditional requests
		// If-None-Match should be allowed to proceed for non-existent objects
		if err != nil && opts.HasIfMatch && (isErrObjectNotFound(err) || isErrVersionNotFound(err)) {
			return ObjectInfo{}, err
		}
	}

	fi, partsMetadata, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, true)
	if err != nil {
		if errors.Is(err, errVolumeNotFound) {
			return oi, toObjectErr(err, bucket)
		}
		return oi, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	onlineDisks := er.getDisks()
	writeQuorum := fi.WriteQuorum(er.defaultWQuorum())
	readQuorum := fi.ReadQuorum(er.defaultRQuorum())

	// Read Part info for all parts
	partPath := pathJoin(uploadIDPath, fi.DataDir) + SlashSeparator
	partMetaPaths := make([]string, len(parts))
	partNumbers := make([]int, len(parts))
	for idx, part := range parts {
		partMetaPaths[idx] = pathJoin(partPath, fmt.Sprintf("part.%d.meta", part.PartNumber))
		partNumbers[idx] = part.PartNumber
	}

	partInfoFiles, err := readParts(ctx, onlineDisks, minioMetaMultipartBucket, partMetaPaths, partNumbers, readQuorum)
	if err != nil {
		return oi, err
	}

	if len(partInfoFiles) != len(parts) {
		// Should only happen through internal error
		err := fmt.Errorf("unexpected part result count: %d, want %d", len(partInfoFiles), len(parts))
		bugLogIf(ctx, err)
		return oi, toObjectErr(err, bucket, object)
	}

	// Checksum type set when upload started.
	var checksumType hash.ChecksumType
	if cs := fi.Metadata[hash.MinIOMultipartChecksum]; cs != "" {
		checksumType = hash.NewChecksumType(cs, fi.Metadata[hash.MinIOMultipartChecksumType])
		if opts.WantChecksum != nil && !opts.WantChecksum.Type.Is(checksumType) {
			return oi, InvalidArgument{
				Bucket: bucket,
				Object: fi.Name,
				Err:    fmt.Errorf("checksum type mismatch. got %q (%s) expected %q (%s)", checksumType.String(), checksumType.ObjType(), opts.WantChecksum.Type.String(), opts.WantChecksum.Type.ObjType()),
			}
		}
		checksumType |= hash.ChecksumMultipart | hash.ChecksumIncludesMultipart
	}

	var checksumCombined []byte

	// However, in case of encryption, the persisted part ETags don't match
	// what we have sent to the client during PutObjectPart. The reason is
	// that ETags are encrypted. Hence, the client will send a list of complete
	// part ETags of which may not match the ETag of any part. For example
	//   ETag (client):          30902184f4e62dd8f98f0aaff810c626
	//   ETag (server-internal): 20000f00ce5dc16e3f3b124f586ae1d88e9caa1c598415c2759bbb50e84a59f630902184f4e62dd8f98f0aaff810c626
	//
	// Therefore, we adjust all ETags sent by the client to match what is stored
	// on the backend.
	kind, _ := crypto.IsEncrypted(fi.Metadata)

	var objectEncryptionKey []byte
	switch kind {
	case crypto.SSEC:
		if checksumType.IsSet() {
			if opts.EncryptFn == nil {
				return oi, crypto.ErrMissingCustomerKey
			}
			baseKey := opts.EncryptFn("", nil)
			if len(baseKey) != 32 {
				return oi, crypto.ErrInvalidCustomerKey
			}
			objectEncryptionKey, err = decryptObjectMeta(baseKey, bucket, object, fi.Metadata)
			if err != nil {
				return oi, err
			}
		}
	case crypto.S3, crypto.S3KMS:
		objectEncryptionKey, err = decryptObjectMeta(nil, bucket, object, fi.Metadata)
		if err != nil {
			return oi, err
		}
	}
	if len(objectEncryptionKey) == 32 {
		var key crypto.ObjectKey
		copy(key[:], objectEncryptionKey)
		opts.EncryptFn = metadataEncrypter(key)
	}

	for idx, part := range partInfoFiles {
		if part.Error != "" {
			err = objPartToPartErr(part)
			bugLogIf(ctx, err)
			return oi, err
		}

		if parts[idx].PartNumber != part.Number {
			internalLogIf(ctx, fmt.Errorf("part.%d.meta has incorrect corresponding part number: expected %d, got %d", parts[idx].PartNumber, parts[idx].PartNumber, part.Number))
			return oi, InvalidPart{
				PartNumber: part.Number,
			}
		}

		// Add the current part.
		fi.AddObjectPart(part.Number, part.ETag, part.Size, part.ActualSize, part.ModTime, part.Index, part.Checksums)
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

	var checksum hash.Checksum
	checksum.Type = checksumType

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
		expPart := currentFI.Parts[partIdx]

		// ensure that part ETag is canonicalized to strip off extraneous quotes
		part.ETag = canonicalizeETag(part.ETag)
		expETag := tryDecryptETag(objectEncryptionKey, expPart.ETag, kind == crypto.S3)
		if expETag != part.ETag {
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				ExpETag:    expETag,
				GotETag:    part.ETag,
			}
			return oi, invp
		}

		if checksumType.IsSet() {
			crc := expPart.Checksums[checksumType.String()]
			if crc == "" {
				return oi, InvalidPart{
					PartNumber: part.PartNumber,
				}
			}
			wantCS := map[string]string{
				hash.ChecksumCRC32.String():     part.ChecksumCRC32,
				hash.ChecksumCRC32C.String():    part.ChecksumCRC32C,
				hash.ChecksumSHA1.String():      part.ChecksumSHA1,
				hash.ChecksumSHA256.String():    part.ChecksumSHA256,
				hash.ChecksumCRC64NVME.String(): part.ChecksumCRC64NVME,
			}
			if wantCS[checksumType.String()] != crc {
				return oi, InvalidPart{
					PartNumber: part.PartNumber,
					ExpETag:    wantCS[checksumType.String()],
					GotETag:    crc,
				}
			}
			cs := hash.NewChecksumString(checksumType.String(), crc)
			if !cs.Valid() {
				return oi, InvalidPart{
					PartNumber: part.PartNumber,
				}
			}
			if checksumType.FullObjectRequested() {
				if err := checksum.AddPart(*cs, expPart.ActualSize); err != nil {
					return oi, InvalidPart{
						PartNumber: part.PartNumber,
						ExpETag:    "<nil>",
						GotETag:    err.Error(),
					}
				}
			}
			checksumCombined = append(checksumCombined, cs.Raw...)
		}

		// All parts except the last part has to be at least 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(currentFI.Parts[partIdx].ActualSize) {
			return oi, PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   expPart.ActualSize,
				PartETag:   part.ETag,
			}
		}

		// Save for total object size.
		objectSize += expPart.Size

		// Save the consolidated actual size.
		objectActualSize += expPart.ActualSize

		// Add incoming parts.
		fi.Parts[i] = ObjectPartInfo{
			Number:     part.PartNumber,
			Size:       expPart.Size,
			ActualSize: expPart.ActualSize,
			ModTime:    expPart.ModTime,
			Index:      expPart.Index,
			Checksums:  nil, // Not transferred since we do not need it.
		}
	}

	if opts.WantChecksum != nil {
		if checksumType.FullObjectRequested() {
			if opts.WantChecksum.Encoded != checksum.Encoded {
				err := hash.ChecksumMismatch{
					Want: opts.WantChecksum.Encoded,
					Got:  checksum.Encoded,
				}
				return oi, err
			}
		} else {
			err := opts.WantChecksum.Matches(checksumCombined, len(parts))
			if err != nil {
				return oi, err
			}
		}
	}

	// Accept encrypted checksum from incoming request.
	if opts.UserDefined[ReplicationSsecChecksumHeader] != "" {
		if v, err := base64.StdEncoding.DecodeString(opts.UserDefined[ReplicationSsecChecksumHeader]); err == nil {
			fi.Checksum = v
		}
		delete(opts.UserDefined, ReplicationSsecChecksumHeader)
	}

	if checksumType.IsSet() {
		checksumType |= hash.ChecksumMultipart | hash.ChecksumIncludesMultipart
		checksum.Type = checksumType
		if !checksumType.FullObjectRequested() {
			checksum = *hash.NewChecksumFromData(checksumType, checksumCombined)
		}
		fi.Checksum = checksum.AppendTo(nil, checksumCombined)
		if opts.EncryptFn != nil {
			fi.Checksum = opts.EncryptFn("object-checksum", fi.Checksum)
		}
	}
	// Remove superfluous internal headers.
	delete(fi.Metadata, hash.MinIOMultipartChecksum)
	delete(fi.Metadata, hash.MinIOMultipartChecksumType)

	// Save the final object size and modtime.
	fi.Size = objectSize
	fi.ModTime = opts.MTime
	if opts.MTime.IsZero() {
		fi.ModTime = UTCNow()
	}

	// Save successfully calculated md5sum.
	// for replica, newMultipartUpload would have already sent the replication ETag
	if fi.Metadata["etag"] == "" {
		if opts.UserDefined["etag"] != "" {
			fi.Metadata["etag"] = opts.UserDefined["etag"]
		} else { // fallback if not already calculated in handler.
			fi.Metadata["etag"] = getCompleteMultipartMD5(parts)
		}
	}

	// Save the consolidated actual size.
	if opts.ReplicationRequest {
		if v := opts.UserDefined[ReservedMetadataPrefix+"Actual-Object-Size"]; v != "" {
			fi.Metadata[ReservedMetadataPrefix+"actual-size"] = v
		}
	} else {
		fi.Metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)
	}

	if opts.DataMovement {
		fi.SetDataMov()
	}

	// Update all erasure metadata, make sure to not modify fields like
	// checksum which are different on each disks.
	for index := range partsMetadata {
		if partsMetadata[index].IsValid() {
			partsMetadata[index].Size = fi.Size
			partsMetadata[index].ModTime = fi.ModTime
			partsMetadata[index].Metadata = fi.Metadata
			partsMetadata[index].Parts = fi.Parts
			partsMetadata[index].Checksum = fi.Checksum
			partsMetadata[index].Versioned = opts.Versioned || opts.VersionSuspended
		}
	}

	paths := make([]string, 0, len(currentFI.Parts))
	// Remove parts that weren't present in CompleteMultipartUpload request.
	for _, curpart := range currentFI.Parts {
		paths = append(paths, pathJoin(uploadIDPath, currentFI.DataDir, fmt.Sprintf("part.%d.meta", curpart.Number)))

		if objectPartIndex(fi.Parts, curpart.Number) == -1 {
			// Delete the missing part files. e.g,
			// Request 1: NewMultipart
			// Request 2: PutObjectPart 1
			// Request 3: PutObjectPart 2
			// Request 4: CompleteMultipartUpload --part 2
			// N.B. 1st part is not present. This part should be removed from the storage.
			paths = append(paths, pathJoin(uploadIDPath, currentFI.DataDir, fmt.Sprintf("part.%d", curpart.Number)))
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

	er.cleanupMultipartPath(ctx, paths...) // cleanup all part.N.meta, and skipped part.N's before final rename().

	defer func() {
		if err == nil {
			er.deleteAll(context.Background(), minioMetaMultipartBucket, uploadIDPath)
		}
	}()

	// Rename the multipart object to final location.
	onlineDisks, versions, oldDataDir, err := renameData(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath,
		partsMetadata, bucket, object, writeQuorum)
	if err != nil {
		return oi, toObjectErr(err, bucket, object, uploadID)
	}

	if err = er.commitRenameDataDir(ctx, bucket, object, oldDataDir, onlineDisks, writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object, uploadID)
	}

	if !opts.Speedtest && len(versions) > 0 {
		globalMRFState.addPartialOp(PartialOperation{
			Bucket:    bucket,
			Object:    object,
			Queued:    time.Now(),
			Versions:  versions,
			SetIndex:  er.setIndex,
			PoolIndex: er.poolIndex,
		})
	}

	if !opts.Speedtest && len(versions) == 0 {
		// Check if there is any offline disk and add it to the MRF list
		for _, disk := range onlineDisks {
			if disk != nil && disk.IsOnline() {
				continue
			}
			er.addPartial(bucket, object, fi.VersionID)
			break
		}
	}

	for i := range len(onlineDisks) {
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
func (er erasureObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, "AbortMultipartUpload", object, &er)
	}

	// Cleanup all uploaded parts.
	defer er.deleteAll(ctx, minioMetaMultipartBucket, er.getUploadIDDir(bucket, object, uploadID))

	// Validates if upload ID exists.
	_, _, err = er.checkUploadIDExists(ctx, bucket, object, uploadID, false)
	return toObjectErr(err, bucket, object, uploadID)
}
