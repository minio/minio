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
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
	"github.com/minio/pkg/mimedb"
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
		if errors.Is(err, errFileNotFound) || errors.Is(err, errVolumeNotFound) {
			err = errUploadIDNotFound
		}
	}()

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	storageDisks := er.getDisks()

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, minioMetaMultipartBucket,
		uploadIDPath, "", false)

	readQuorum, writeQuorum, err := objectQuorumFromMeta(ctx, partsMetadata, errs, er.defaultParityCount)
	if err != nil {
		return fi, nil, err
	}

	// List all online disks.
	_, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	var quorum int
	if write {
		reducedErr := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
		if reducedErr == errErasureWriteQuorum {
			return fi, nil, reducedErr
		}

		quorum = writeQuorum
	} else {
		if reducedErr := reduceReadQuorumErrs(ctx, errs, objectOpIgnoredErrs, readQuorum); reducedErr != nil {
			return fi, nil, reducedErr
		}

		// Pick one from the first valid metadata.
		quorum = readQuorum
	}

	// Pick one from the first valid metadata.
	fi, err = pickValidFileInfo(ctx, partsMetadata, modTime, quorum)

	return fi, partsMetadata, err
}

// Removes part.meta given by partName belonging to a mulitpart upload from minioMetaBucket
func (er erasureObjects) renamePart(ctx context.Context, bucket, object, uploadID, dataDir string, partNumber int) {
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	curpartPath := pathJoin(uploadIDPath, dataDir, fmt.Sprintf("part.%d", partNumber))
	storageDisks := er.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		index := index
		g.Go(func() error {
			return storageDisks[index].RenameFile(ctx, minioMetaMultipartBucket, curpartPath, bucket, pathJoin(object, dataDir, fmt.Sprintf("part.%d", partNumber)))
		}, index)
	}
	g.Wait()
}

// Removes uploadID belonging to a mulitpart upload from minioMetaBucket
func (er erasureObjects) removeUploadID(bucket, object, uploadID string) {
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	storageDisks := er.getDisks()

	g := errgroup.WithNErrs(len(storageDisks))
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		index := index
		g.Go(func() error {
			_ = storageDisks[index].Delete(context.TODO(), minioMetaMultipartBucket, uploadIDPath, DeleteOptions{
				Recursive: true,
				Force:     true,
			})

			return nil
		}, index)
	}
	g.Wait()
}

// Removes part given by partName belonging to a mulitpart upload from minioMetaBucket
func (er erasureObjects) removeObjectPart(bucket, object, uploadID, dataDir string, partNumber int) {
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	curpartPath := pathJoin(uploadIDPath, dataDir, fmt.Sprintf("part.%d", partNumber))
	storageDisks := er.getDisks()

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

// Clean-up the old multipart uploads. Should be run in a Go routine.
func (er erasureObjects) cleanupStaleUploads(ctx context.Context, expiry time.Duration) {
	// run multiple cleanup's local to this server.
	var wg sync.WaitGroup
	for _, disk := range er.getLoadBalancedLocalDisks() {
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
				Force:     false,
			})
		}(disk)
	}
	wg.Wait()
}

// Remove the old multipart uploads on the given disk.
func (er erasureObjects) cleanupStaleUploadsOnDisk(ctx context.Context, disk StorageAPI, expiry time.Duration) {
	now := time.Now()
	diskPath := disk.Endpoint().Path

	readDirFn(pathJoin(diskPath, minioMetaMultipartBucket), func(shaDir string, typ os.FileMode) error {
		return readDirFn(pathJoin(diskPath, minioMetaMultipartBucket, shaDir), func(uploadIDDir string, typ os.FileMode) error {
			uploadIDPath := pathJoin(shaDir, uploadIDDir)
			fi, err := disk.ReadVersion(ctx, minioMetaMultipartBucket, uploadIDPath, "", false)
			if err != nil {
				er.deleteAll(ctx, minioMetaMultipartBucket, uploadIDPath)
				return nil
			}
			wait := deletedCleanupSleeper.Timer(ctx)
			if now.Sub(fi.ModTime) > expiry {
				er.deleteAll(ctx, minioMetaMultipartBucket, uploadIDPath)
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
		wait := deletedCleanupSleeper.Timer(ctx)
		if now.Sub(vi.Created) > expiry {
			er.deleteAll(ctx, minioMetaTmpBucket, tmpDir)
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
func (er erasureObjects) ListMultipartUploads(ctx context.Context, bucket, object, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	auditObjectErasureSet(ctx, object, &er)

	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = object
	result.Delimiter = delimiter

	if object == "" {
		// No object (prefix) provided we are not going to list anything.
		return result, nil
	}

	var uploadIDs []string
	var disk StorageAPI
	for _, disk = range er.getLoadBalancedDisks(true) {
		uploadIDs, err = disk.ListDir(ctx, minioMetaMultipartBucket, er.getMultipartSHADir(bucket, object), -1)
		if err != nil {
			if errors.Is(err, errDiskNotFound) {
				continue
			}
			if errors.Is(err, errFileNotFound) || errors.Is(err, errVolumeNotFound) {
				return result, nil
			}
			logger.LogIf(ctx, err)
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

	populatedUploadIds := set.NewStringSet()

	for _, uploadID := range uploadIDs {
		if populatedUploadIds.Contains(uploadID) {
			continue
		}
		fi, err := disk.ReadVersion(ctx, minioMetaMultipartBucket, pathJoin(er.getUploadIDDir(bucket, object, uploadID)), "", false)
		if err != nil {
			if !IsErrIgnored(err, errFileNotFound, errDiskNotFound) {
				logger.LogIf(ctx, err)
			}
			// Ignore this invalid upload-id since we are listing here
			continue
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
func (er erasureObjects) newMultipartUpload(ctx context.Context, bucket string, object string, opts ObjectOptions) (*NewMultipartUploadResult, error) {
	if opts.CheckPrecondFn != nil {
		// Lock the object before reading.
		lk := er.NewNSLock(bucket, object)
		lkctx, err := lk.GetRLock(ctx, globalOperationTimeout)
		if err != nil {
			return nil, err
		}
		rctx := lkctx.Context()
		obj, err := er.getObjectInfo(rctx, bucket, object, opts)
		lk.RUnlock(lkctx)
		if err != nil && !isErrVersionNotFound(err) {
			return nil, err
		}
		if opts.CheckPrecondFn(obj) {
			return nil, PreConditionFailed{}
		}
	}

	userDefined := cloneMSS(opts.UserDefined)
	if opts.PreserveETag != "" {
		userDefined["etag"] = opts.PreserveETag
	}
	onlineDisks := er.getDisks()
	parityDrives := globalStorageClass.GetParityForSC(userDefined[xhttp.AmzStorageClass])
	if parityDrives < 0 {
		parityDrives = er.defaultParityCount
	}

	parityOrig := parityDrives
	for _, disk := range onlineDisks {
		if parityDrives >= len(onlineDisks)/2 {
			parityDrives = len(onlineDisks) / 2
			break
		}
		if disk == nil {
			parityDrives++
			continue
		}
		di, err := disk.DiskInfo(ctx)
		if err != nil || di.ID == "" {
			parityDrives++
		}
	}
	if parityOrig != parityDrives {
		userDefined[minIOErasureUpgraded] = strconv.Itoa(parityOrig) + "->" + strconv.Itoa(parityDrives)
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

	// Initialize erasure metadata.
	for index := range partsMetadata {
		partsMetadata[index] = fi
	}

	// Guess content-type from the extension if possible.
	if userDefined["content-type"] == "" {
		userDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
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
	uploadUUID := mustGetUUID()
	uploadID := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s.%s", globalDeploymentID, uploadUUID)))
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadUUID)

	// Write updated `xl.meta` to all disks.
	if _, err := writeUniqueFileInfo(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum); err != nil {
		return nil, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
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
	auditObjectErasureSet(ctx, object, &er)

	return er.newMultipartUpload(ctx, bucket, object, opts)
}

// CopyObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to put object part operation but the source
// data is read from an existing object.
//
// Implements S3 compatible Upload Part Copy API.
func (er erasureObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (pi PartInfo, e error) {
	partInfo, err := er.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, NewPutObjReader(srcInfo.Reader), dstOpts)
	if err != nil {
		return pi, toObjectErr(err, dstBucket, dstObject)
	}

	// Success.
	return partInfo, nil
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
	// otherwise return failure.
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
	if err == errErasureWriteQuorum {
		// Remove all written
		g := errgroup.WithNErrs(len(disks))
		for index := range disks {
			if disks[index] == nil || errs[index] != nil {
				continue
			}
			index := index
			g.Go(func() error {
				return disks[index].Delete(ctx, dstBucket, dstEntry, DeleteOptions{Force: true})
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
	auditObjectErasureSet(ctx, object, &er)

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
	fi, partsMetadata, err := er.checkUploadIDExists(rctx, bucket, object, uploadID, true)
	if err != nil {
		return pi, toObjectErr(err, bucket, object, uploadID)
	}

	data := r.Reader
	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(rctx, errInvalidArgument, logger.Application)
		return pi, toObjectErr(errInvalidArgument)
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
	pctx := plkctx.Context()
	defer partIDLock.Unlock(plkctx)

	partSuffix := fmt.Sprintf("part.%d", partID)
	erPL := er.setPlacement(pathJoin(object, partSuffix))
	onlineDisks := erPL.getDisks()
	partPl := newPartPlacement(uint16(erPL.poolIndex), uint16(erPL.setIndex))
	writeQuorum := fi.WriteQuorum(er.defaultWQuorum())

	if cs := fi.Metadata[hash.MinIOMultipartChecksum]; cs != "" {
		if r.ContentCRCType().String() != cs {
			return pi, InvalidArgument{
				Bucket: bucket,
				Object: fi.Name,
				Err:    fmt.Errorf("checksum missing, want %s, got %s", cs, r.ContentCRCType().String()),
			}
		}
	}

	onlineDisks, _ = shuffleDisksAndPartsMetadataByIndex(onlineDisks, partsMetadata, fi)

	// Need a unique name for the part being written in minioMetaBucket to
	// accommodate concurrent PutObjectPart requests

	tmpPart := mustGetUUID()
	tmpPartPath := pathJoin(tmpPart, partSuffix)

	// Delete the temporary object part. If PutObjectPart succeeds there would be nothing to delete.
	var online int
	defer func() {
		if online != len(onlineDisks) {
			er.deleteAll(context.Background(), minioMetaTmpBucket, tmpPart)
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
			buffer = er.bp.Get()
			defer er.bp.Put(buffer)
		}
	case size >= fi.Erasure.BlockSize:
		buffer = er.bp.Get()
		defer er.bp.Put(buffer)
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
		writers[i] = newBitrotWriter(disk, minioMetaTmpBucket, tmpPartPath, erasure.ShardFileSize(data.Size()), DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	toEncode := io.Reader(data)
	if data.Size() > bigFileThreshold {
		// Add input readahead.
		// We use 2 buffers, so we always have a full buffer of input.
		bufA := er.bp.Get()
		bufB := er.bp.Get()
		defer er.bp.Put(bufA)
		defer er.bp.Put(bufB)
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

	// Rename temporary part file to its final location.
	partPath := pathJoin(uploadIDPath, fi.DataDir, partSuffix)
	onlineDisks, err = renamePart(ctx, onlineDisks, minioMetaTmpBucket, tmpPartPath, minioMetaMultipartBucket, partPath, writeQuorum)
	if err != nil {
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
		Placement:  partPl,
	}
	fi.Parts = []ObjectPartInfo{partInfo}
	partFI, err := fi.MarshalMsg(nil)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Write part metadata to all disks, do not use partPlacement for 'part.meta' to write parts
	// to same set of drives where xl.meta is written.
	onlineDisks, err = writeAllDisks(ctx, er.getDisks(), minioMetaMultipartBucket, partPath+".meta", partFI, writeQuorum)
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
	auditObjectErasureSet(ctx, object, &er)

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
	auditObjectErasureSet(ctx, object, &er)

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

	onlineDisks := er.getDisks()
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	if maxParts == 0 {
		return result, nil
	}

	if partNumberMarker < 0 {
		partNumberMarker = 0
	}

	// Limit output to maxPartsList.
	if maxParts > maxPartsList-partNumberMarker {
		maxParts = maxPartsList - partNumberMarker
	}

	// Read Part info for all parts
	partPath := pathJoin(uploadIDPath, fi.DataDir) + "/"
	req := ReadMultipleReq{
		Bucket:     minioMetaMultipartBucket,
		Prefix:     partPath,
		MaxSize:    1 << 20, // Each part should realistically not be > 1MiB.
		MaxResults: maxParts + 1,
	}

	start := partNumberMarker + 1
	end := start + maxParts

	// Parts are natural number based, so index 0 is part one, etc.
	for i := start; i <= end; i++ {
		req.Files = append(req.Files, fmt.Sprintf("part.%d.meta", i))
	}

	writeQuorum := fi.WriteQuorum(er.defaultWQuorum())

	partInfoFiles, err := readMultipleFiles(ctx, onlineDisks, req, writeQuorum)
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
	result.ChecksumAlgorithm = fi.Metadata[hash.MinIOMultipartChecksum]

	// For empty number of parts or maxParts as zero, return right here.
	if len(partInfoFiles) == 0 || maxParts == 0 {
		return result, nil
	}

	for i, part := range partInfoFiles {
		partN := i + partNumberMarker + 1
		if part.Error != "" || !part.Exists {
			continue
		}

		var pfi FileInfo
		_, err := pfi.UnmarshalMsg(part.Data)
		if err != nil {
			// Maybe crash or similar.
			logger.LogIf(ctx, err)
			continue
		}

		partI := pfi.Parts[0]
		if partN != partI.Number {
			logger.LogIf(ctx, fmt.Errorf("part.%d.meta has incorrect corresponding part number: expected %d, got %d", i+1, i+1, partI.Number))
			continue
		}

		// Add the current part.
		fi.AddObjectPart(partI.Number, partI.ETag, partI.Size, partI.ActualSize, partI.ModTime, partI.Index, partI.Checksums, partI.Placement)
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
	auditObjectErasureSet(ctx, object, &er)

	// Hold write locks to verify uploaded parts, also disallows any
	// parallel PutObjectPart() requests.
	uploadIDLock := er.NewNSLock(bucket, pathJoin(object, uploadID))
	wlkctx, err := uploadIDLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return oi, err
	}
	wctx := wlkctx.Context()
	defer uploadIDLock.Unlock(wlkctx)

	fi, partsMetadata, err := er.checkUploadIDExists(wctx, bucket, object, uploadID, true)
	if err != nil {
		return oi, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	onlineDisks := er.getDisks()
	writeQuorum := fi.WriteQuorum(er.defaultWQuorum())

	// Read Part info for all parts
	partPath := pathJoin(uploadIDPath, fi.DataDir) + "/"
	req := ReadMultipleReq{
		Bucket:     minioMetaMultipartBucket,
		Prefix:     partPath,
		MaxSize:    1 << 20, // Each part should realistically not be > 1MiB.
		Files:      make([]string, 0, len(parts)),
		AbortOn404: true,
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
		logger.LogIf(ctx, err)
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
			logger.LogIf(ctx, err)
			return oi, InvalidPart{
				PartNumber: partID,
			}
		}

		partI := pfi.Parts[0]
		partNumber := partI.Number
		if partID != partNumber {
			logger.LogIf(ctx, fmt.Errorf("part.%d.meta has incorrect corresponding part number: expected %d, got %d", partID, partID, partI.Number))
			return oi, InvalidPart{
				PartNumber: partID,
			}
		}

		// Add the current part.
		fi.AddObjectPart(partI.Number, partI.ETag, partI.Size, partI.ActualSize, partI.ModTime, partI.Index, partI.Checksums, partI.Placement)
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
		expETag := tryDecryptETag(objectEncryptionKey, expPart.ETag, kind != crypto.S3)
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
			Placement:  expPart.Placement,
		}
	}

	if opts.WantChecksum != nil {
		err := opts.WantChecksum.Matches(checksumCombined)
		if err != nil {
			return oi, err
		}
	}
	if checksumType.IsSet() {
		cs := hash.NewChecksumFromData(checksumType, checksumCombined)
		fi.Checksum = cs.AppendTo(nil)
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
	fi.Metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)

	// Update all erasure metadata, make sure to not modify fields like
	// checksum which are different on each disks.
	for index := range partsMetadata {
		if partsMetadata[index].IsValid() {
			partsMetadata[index].Size = fi.Size
			partsMetadata[index].ModTime = fi.ModTime
			partsMetadata[index].Metadata = fi.Metadata
			partsMetadata[index].Parts = fi.Parts
			partsMetadata[index].Checksum = fi.Checksum
		}
	}

	// Hold namespace to complete the transaction
	lk := er.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return oi, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx)

	// Remove parts that weren't present in CompleteMultipartUpload request.
	for _, curpart := range currentFI.Parts {
		if objectPartIndex(fi.Parts, curpart.Number) == -1 {
			// Delete the missing part files. e.g,
			// Request 1: NewMultipart
			// Request 2: PutObjectPart 1
			// Request 3: PutObjectPart 2
			// Request 4: CompleteMultipartUpload --part 2
			// N.B. 1st part is not present. This part should be removed from the storage.
			der := er.setByIdx(curpart.Placement.setIdx())
			der.removeObjectPart(bucket, object, uploadID, fi.DataDir, curpart.Number)
		}
	}

	// Always perform 1/10th of the number of parts per CompleteMultipart
	concurrent := len(fi.Parts) / 10
	if concurrent <= 10 {
		// if we cannot get 1/10th then choose the number of
		// objects as concurrent.
		concurrent = len(fi.Parts)
	}

	eg := errgroup.WithNErrs(len(fi.Parts)).WithConcurrency(concurrent)
	for j, part := range fi.Parts {
		j := j
		part := part
		eg.Go(func() error {
			er.setByIdx(part.Placement.setIdx()).renamePart(ctx, bucket, object, uploadID, fi.DataDir, part.Number)
			return nil
		}, j)
	}

	eg.Wait() // wait here..

	if onlineDisks, err = writeUniqueFileInfo(ctx, onlineDisks, bucket, object, partsMetadata, writeQuorum); err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	// We must delete uploadID, only after all parts are renamed().
	defer func() {
		// deduplicate erasure sets to remove uploadID from
		sets := make(map[*erasureObjects]struct{})
		for _, part := range fi.Parts {
			sets[er.setByIdx(part.Placement.setIdx())] = struct{}{}
		}
		eg := errgroup.WithNErrs(len(sets)).WithConcurrency(concurrent)
		i := 0
		for set := range sets {
			set := set
			eg.Go(func() error {
				set.removeUploadID(bucket, object, uploadID)
				return nil
			}, i)
			i++
		}
		eg.Wait()
		er.removeUploadID(bucket, object, uploadID)
	}()

	defer NSUpdated(bucket, object)

	// Check if there is any offline disk and add it to the MRF list
	for _, disk := range onlineDisks {
		if disk != nil && disk.IsOnline() {
			continue
		}
		er.addPartial(bucket, object, fi.VersionID, fi.Size)
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
	auditObjectErasureSet(ctx, object, &er)

	lk := er.NewNSLock(bucket, pathJoin(object, uploadID))
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx)

	// Validates if upload ID exists.
	if _, _, err = er.checkUploadIDExists(ctx, bucket, object, uploadID, false); err != nil {
		return toObjectErr(err, bucket, object, uploadID)
	}

	for i := 1; i <= 10000; i++ {
		erPL := er.setPlacement(pathJoin(object, fmt.Sprintf("part.%d", i)))
		erPL.deleteAll(ctx, minioMetaMultipartBucket, er.getUploadIDDir(bucket, object, uploadID))
	}

	// Successfully purged.
	return nil
}
