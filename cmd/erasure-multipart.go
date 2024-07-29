// Copyright (c) 2015-2023 MinIO, Inc.
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

	quorum := readQuorum
	if write {
		quorum = writeQuorum
	}

	// List all online disks.
	_, modTime, etag := listOnlineDisks(storageDisks, partsMetadata, errs, quorum)

	var reducedErr error
	if write {
		reducedErr = reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	} else {
		reducedErr = reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum)
	}
	if reducedErr != nil {
		return fi, nil, reducedErr
	}

	// Pick one from the first valid metadata.
	fi, err = pickValidFileInfo(ctx, partsMetadata, modTime, etag, quorum)
	return fi, partsMetadata, err
}

// cleanMultipartPath removes all extraneous files and parts from the multipart folder, this is used per CompleteMultipart.
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
func (er erasureObjects) cleanupStaleUploads(ctx context.Context, expiry time.Duration) {
	// run multiple cleanup's local to this server.
	var wg sync.WaitGroup
	for _, disk := range er.getLocalDisks() {
		if disk != nil {
			wg.Add(1)
			go func(disk StorageAPI) {
				defer wg.Done()
				er.cleanupStaleUploadsOnDisk(ctx, disk, expiry)
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
func (er erasureObjects) cleanupStaleUploadsOnDisk(ctx context.Context, disk StorageAPI, expiry time.Duration) {
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
			if time.Since(modTime) < expiry {
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
		if time.Since(vi.Created) < expiry {
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
			if time.Since(vi.Created) > expiry {
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
	auditObjectErasureSet(ctx, object, &er)

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
			UploadID:  base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s.%s", globalDeploymentID(), uploadID))),
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

	if userDefined[ReplicationSsecChecksumHeader] != "" {
		fi.Checksum, _ = base64.StdEncoding.DecodeString(userDefined[ReplicationSsecChecksumHeader])
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
	uploadID := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s.%s", globalDeploymentID(), uploadUUID)))
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadUUID)

	// Write updated `xl.meta` to all disks.
	if _, err := writeUniqueFileInfo(ctx, onlineDisks, bucket, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum); err != nil {
		return nil, toObjectErr(err, bucket, object)
	}
	return &NewMultipartUploadResult{
		UploadID:     uploadID,
		ChecksumAlgo: userDefined[hash.MinIOMultipartChecksum],
	}, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a
// unique id. The unique id returned here is of UUID form, for each
// subsequent request each UUID is unique.
//
// Implements S3 compatible initiate multipart API.
func (er erasureObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (*NewMultipartUploadResult, error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, object, &er)
	}

	return er.newMultipartUpload(ctx, bucket, object, opts)
}

// renamePart - renames multipart part to its relevant location under uploadID.
func renamePart(ctx context.Context, disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, writeQuorum int) ([]StorageAPI, error) {
	g := errgroup.WithNErrs(len(disks))

	// Rename file on all underlying storage disks.
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			return disks[index].RenameFile(ctx, srcBucket, srcEntry, dstBucket, dstEntry)
		}, index)
	}

	// Wait for all renames to finish.
	errs := g.Wait()

	// Do not need to undo partial successful operation since those will be cleaned up
	// in 24hrs via multipart cleaner, never rename() back to `.minio.sys/tmp` as there
	// is no way to clean them.

	// We can safely allow RenameFile errors up to len(er.getDisks()) - writeQuorum
	// otherwise return failure. Cleanup successful renames.
	return evalDisks(disks, errs), reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
}

// writeAllDisks - writes 'b' to all provided disks.
// If write cannot reach quorum, the files will be deleted from all disks.
func writeAllDisks(ctx context.Context, disks []StorageAPI, dstBucket, dstEntry string, b []byte, writeQuorum int) ([]StorageAPI, error) {
	g := errgroup.WithNErrs(len(disks))

	// Write file to all underlying storage disks.
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			return disks[index].WriteAll(ctx, dstBucket, dstEntry, b)
		}, index)
	}

	// Wait for all renames to finish.
	errs := g.Wait()

	// We can safely allow RenameFile errors up to len(er.getDisks()) - writeQuorum
	// otherwise return failure. Cleanup successful renames.
	err := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if errors.Is(err, errErasureWriteQuorum) {
		// Remove all written
		g := errgroup.WithNErrs(len(disks))
		for index := range disks {
			if disks[index] == nil || errs[index] != nil {
				continue
			}
			index := index
			g.Go(func() error {
				return disks[index].Delete(ctx, dstBucket, dstEntry, DeleteOptions{Immediate: true})
			}, index)
		}
		// Ignore these errors.
		g.WaitErr()
	}

	return evalDisks(disks, errs), err
}

// PutObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to single put operation but it is part
// of the multipart transaction.
//
// Implements S3 compatible Upload Part API.
func (er erasureObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *PutObjReader, opts ObjectOptions) (pi PartInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, object, &er)
	}

	data := r.Reader
	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		bugLogIf(ctx, errInvalidArgument, logger.ErrorKind)
		return pi, toObjectErr(errInvalidArgument)
	}

	// Read lock for upload id.
	// Only held while reading the upload metadata.
	uploadIDRLock := er.NewNSLock(bucket, pathJoin(object, uploadID))
	rlkctx, err := uploadIDRLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return PartInfo{}, err
	}
	rctx := rlkctx.Context()
	defer uploadIDRLock.RUnlock(rlkctx)

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	// Validates if upload ID exists.
	fi, _, err := er.checkUploadIDExists(rctx, bucket, object, uploadID, true)
	if err != nil {
		if errors.Is(err, errVolumeNotFound) {
			return pi, toObjectErr(err, bucket)
		}
		return pi, toObjectErr(err, bucket, object, uploadID)
	}

	// Write lock for this part ID, only hold it if we are planning to read from the
	// streamto avoid any concurrent updates.
	//
	// Must be held throughout this call.
	partIDLock := er.NewNSLock(bucket, pathJoin(object, uploadID, strconv.Itoa(partID)))
	plkctx, err := partIDLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return PartInfo{}, err
	}

	ctx = plkctx.Context()
	defer partIDLock.Unlock(plkctx)

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
	case size == -1:
		if size := data.ActualSize(); size > 0 && size < fi.Erasure.BlockSize {
			// Account for padding and forced compression overhead and encryption.
			buffer = make([]byte, data.ActualSize()+256+32+32, data.ActualSize()*2+512)
		} else {
			buffer = globalBytePoolCap.Load().Get()
			defer globalBytePoolCap.Load().Put(buffer)
		}
	case size >= fi.Erasure.BlockSize:
		buffer = globalBytePoolCap.Load().Get()
		defer globalBytePoolCap.Load().Put(buffer)
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

	// Rename temporary part file to its final location.
	partPath := pathJoin(uploadIDPath, fi.DataDir, partSuffix)
	onlineDisks, err = renamePart(ctx, onlineDisks, minioMetaTmpBucket, tmpPartPath, minioMetaMultipartBucket, partPath, writeQuorum)
	if err != nil {
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

	md5hex := r.MD5CurrentHexString()
	if opts.PreserveETag != "" {
		md5hex = opts.PreserveETag
	}

	var index []byte
	if opts.IndexCB != nil {
		index = opts.IndexCB()
	}

	partInfo := ObjectPartInfo{
		Number:     partID,
		ETag:       md5hex,
		Size:       n,
		ActualSize: data.ActualSize(),
		ModTime:    UTCNow(),
		Index:      index,
		Checksums:  r.ContentCRC(),
	}

	fi.Parts = []ObjectPartInfo{partInfo}
	partFI, err := fi.MarshalMsg(nil)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Write part metadata to all disks.
	onlineDisks, err = writeAllDisks(ctx, onlineDisks, minioMetaMultipartBucket, partPath+".meta", partFI, writeQuorum)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Return success.
	return PartInfo{
		PartNumber:     partInfo.Number,
		ETag:           partInfo.ETag,
		LastModified:   partInfo.ModTime,
		Size:           partInfo.Size,
		ActualSize:     partInfo.ActualSize,
		ChecksumCRC32:  partInfo.Checksums["CRC32"],
		ChecksumCRC32C: partInfo.Checksums["CRC32C"],
		ChecksumSHA1:   partInfo.Checksums["SHA1"],
		ChecksumSHA256: partInfo.Checksums["SHA256"],
	}, nil
}

// GetMultipartInfo returns multipart metadata uploaded during newMultipartUpload, used
// by callers to verify object states
// - encrypted
// - compressed
// Does not contain currently uploaded parts by design.
func (er erasureObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, object, &er)
	}

	result := MultipartInfo{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}

	uploadIDLock := er.NewNSLock(bucket, pathJoin(object, uploadID))
	lkctx, err := uploadIDLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return MultipartInfo{}, err
	}
	ctx = lkctx.Context()
	defer uploadIDLock.RUnlock(lkctx)

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

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is marshaled directly into XML and
// replied back to the client.
func (er erasureObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts ObjectOptions) (result ListPartsInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, object, &er)
	}

	uploadIDLock := er.NewNSLock(bucket, pathJoin(object, uploadID))
	lkctx, err := uploadIDLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return ListPartsInfo{}, err
	}
	ctx = lkctx.Context()
	defer uploadIDLock.RUnlock(lkctx)

	fi, _, err := er.checkUploadIDExists(ctx, bucket, object, uploadID, false)
	if err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	// Populate the result stub.
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker
	result.UserDefined = cloneMSS(fi.Metadata)
	result.ChecksumAlgorithm = fi.Metadata[hash.MinIOMultipartChecksum]

	if partNumberMarker < 0 {
		partNumberMarker = 0
	}

	// Limit output to maxPartsList.
	if maxParts > maxPartsList-partNumberMarker {
		maxParts = maxPartsList - partNumberMarker
	}

	if maxParts == 0 {
		return result, nil
	}

	// Read Part info for all parts
	partPath := pathJoin(uploadIDPath, fi.DataDir) + "/"
	req := ReadMultipleReq{
		Bucket:       minioMetaMultipartBucket,
		Prefix:       partPath,
		MaxSize:      1 << 20, // Each part should realistically not be > 1MiB.
		MaxResults:   maxParts + 1,
		MetadataOnly: true,
	}

	start := partNumberMarker + 1
	end := start + maxParts

	// Parts are 1 based, so index 0 is part one, etc.
	for i := start; i <= end; i++ {
		req.Files = append(req.Files, fmt.Sprintf("part.%d.meta", i))
	}

	var disk StorageAPI
	disks := er.getOnlineLocalDisks()
	if len(disks) == 0 {
		// using er.getOnlineLocalDisks() has one side-affect where
		// on a pooled setup all disks are remote, add a fallback
		disks = er.getOnlineDisks()
	}

	for _, disk = range disks {
		if disk == nil {
			continue
		}

		if !disk.IsOnline() {
			continue
		}

		break
	}

	g := errgroup.WithNErrs(len(req.Files)).WithConcurrency(32)

	partsInfo := make([]ObjectPartInfo, len(req.Files))
	for i, file := range req.Files {
		file := file
		partN := i + start
		i := i

		g.Go(func() error {
			buf, err := disk.ReadAll(ctx, minioMetaMultipartBucket, pathJoin(partPath, file))
			if err != nil {
				return err
			}

			var pfi FileInfo
			_, err = pfi.UnmarshalMsg(buf)
			if err != nil {
				return err
			}

			if len(pfi.Parts) != 1 {
				return errors.New("invalid number of parts expected 1, got 0")
			}

			if partN != pfi.Parts[0].Number {
				return fmt.Errorf("part.%d.meta has incorrect corresponding part number: expected %d, got %d", partN, partN, pfi.Parts[0].Number)
			}

			partsInfo[i] = pfi.Parts[0]
			return nil
		}, i)
	}

	g.Wait()

	for _, part := range partsInfo {
		if part.Number != 0 && !part.ModTime.IsZero() {
			fi.AddObjectPart(part.Number, part.ETag, part.Size, part.ActualSize, part.ModTime, part.Index, part.Checksums)
		}
	}

	// Only parts with higher part numbers will be listed.
	parts := fi.Parts
	result.Parts = make([]PartInfo, 0, len(parts))
	for _, part := range parts {
		result.Parts = append(result.Parts, PartInfo{
			PartNumber:     part.Number,
			ETag:           part.ETag,
			LastModified:   part.ModTime,
			ActualSize:     part.ActualSize,
			Size:           part.Size,
			ChecksumCRC32:  part.Checksums["CRC32"],
			ChecksumCRC32C: part.Checksums["CRC32C"],
			ChecksumSHA1:   part.Checksums["SHA1"],
			ChecksumSHA256: part.Checksums["SHA256"],
		})
		if len(result.Parts) >= maxParts {
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
func (er erasureObjects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, parts []CompletePart, opts ObjectOptions) (oi ObjectInfo, err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, object, &er)
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
	}

	// Hold write locks to verify uploaded parts, also disallows any
	// parallel PutObjectPart() requests.
	uploadIDLock := er.NewNSLock(bucket, pathJoin(object, uploadID))
	wlkctx, err := uploadIDLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return oi, err
	}
	ctx = wlkctx.Context()
	defer uploadIDLock.Unlock(wlkctx)

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

	// Read Part info for all parts
	partPath := pathJoin(uploadIDPath, fi.DataDir) + "/"
	req := ReadMultipleReq{
		Bucket:       minioMetaMultipartBucket,
		Prefix:       partPath,
		MaxSize:      1 << 20, // Each part should realistically not be > 1MiB.
		Files:        make([]string, 0, len(parts)),
		AbortOn404:   true,
		MetadataOnly: true,
	}
	for _, part := range parts {
		req.Files = append(req.Files, fmt.Sprintf("part.%d.meta", part.PartNumber))
	}
	partInfoFiles, err := readMultipleFiles(ctx, onlineDisks, req, writeQuorum)
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
		checksumType = hash.NewChecksumType(cs)
		if opts.WantChecksum != nil && !opts.WantChecksum.Type.Is(checksumType) {
			return oi, InvalidArgument{
				Bucket: bucket,
				Object: fi.Name,
				Err:    fmt.Errorf("checksum type mismatch"),
			}
		}
	}

	var checksumCombined []byte

	// However, in case of encryption, the persisted part ETags don't match
	// what we have sent to the client during PutObjectPart. The reason is
	// that ETags are encrypted. Hence, the client will send a list of complete
	// part ETags of which non can match the ETag of any part. For example
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

	for i, part := range partInfoFiles {
		partID := parts[i].PartNumber
		if part.Error != "" || !part.Exists {
			return oi, InvalidPart{
				PartNumber: partID,
			}
		}

		var pfi FileInfo
		_, err := pfi.UnmarshalMsg(part.Data)
		if err != nil {
			// Maybe crash or similar.
			bugLogIf(ctx, err)
			return oi, InvalidPart{
				PartNumber: partID,
			}
		}

		partI := pfi.Parts[0]
		partNumber := partI.Number
		if partID != partNumber {
			internalLogIf(ctx, fmt.Errorf("part.%d.meta has incorrect corresponding part number: expected %d, got %d", partID, partID, partI.Number))
			return oi, InvalidPart{
				PartNumber: partID,
			}
		}

		// Add the current part.
		fi.AddObjectPart(partI.Number, partI.ETag, partI.Size, partI.ActualSize, partI.ModTime, partI.Index, partI.Checksums)
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
				hash.ChecksumCRC32.String():  part.ChecksumCRC32,
				hash.ChecksumCRC32C.String(): part.ChecksumCRC32C,
				hash.ChecksumSHA1.String():   part.ChecksumSHA1,
				hash.ChecksumSHA256.String(): part.ChecksumSHA256,
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
		err := opts.WantChecksum.Matches(checksumCombined, len(parts))
		if err != nil {
			return oi, err
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

	// Accept encrypted checksum from incoming request.
	if opts.UserDefined[ReplicationSsecChecksumHeader] != "" {
		if v, err := base64.StdEncoding.DecodeString(opts.UserDefined[ReplicationSsecChecksumHeader]); err == nil {
			fi.Checksum = v
		}
		delete(opts.UserDefined, ReplicationSsecChecksumHeader)
	}

	if checksumType.IsSet() {
		checksumType |= hash.ChecksumMultipart | hash.ChecksumIncludesMultipart
		var cs *hash.Checksum
		cs = hash.NewChecksumFromData(checksumType, checksumCombined)
		fi.Checksum = cs.AppendTo(nil, checksumCombined)
		if opts.EncryptFn != nil {
			fi.Checksum = opts.EncryptFn("object-checksum", fi.Checksum)
		}
	}
	delete(fi.Metadata, hash.MinIOMultipartChecksum) // Not needed in final object.

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
		fi.Metadata[ReservedMetadataPrefix+"actual-size"] = opts.UserDefined["X-Minio-Internal-Actual-Object-Size"]
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
		globalMRFState.addPartialOp(partialOperation{
			bucket:    bucket,
			object:    object,
			queued:    time.Now(),
			versions:  versions,
			setIndex:  er.setIndex,
			poolIndex: er.poolIndex,
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
func (er erasureObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (err error) {
	if !opts.NoAuditLog {
		auditObjectErasureSet(ctx, object, &er)
	}

	lk := er.NewNSLock(bucket, pathJoin(object, uploadID))
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx)

	// Validates if upload ID exists.
	if _, _, err = er.checkUploadIDExists(ctx, bucket, object, uploadID, false); err != nil {
		if errors.Is(err, errVolumeNotFound) {
			return toObjectErr(err, bucket)
		}
		return toObjectErr(err, bucket, object, uploadID)
	}

	// Cleanup all uploaded parts.
	er.deleteAll(ctx, minioMetaMultipartBucket, er.getUploadIDDir(bucket, object, uploadID))

	// Successfully purged.
	return nil
}
