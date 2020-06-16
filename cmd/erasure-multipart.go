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
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"

	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/sync/errgroup"
)

func (er erasureObjects) getUploadIDDir(bucket, object, uploadID string) string {
	return pathJoin(er.getMultipartSHADir(bucket, object), uploadID)
}

func (er erasureObjects) getMultipartSHADir(bucket, object string) string {
	return getSHA256Hash([]byte(pathJoin(bucket, object)))
}

// checkUploadIDExists - verify if a given uploadID exists and is valid.
func (er erasureObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string) error {
	_, err := er.getObjectInfo(ctx, minioMetaMultipartBucket, er.getUploadIDDir(bucket, object, uploadID), ObjectOptions{})
	return err
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
			_ = storageDisks[index].DeleteFile(minioMetaMultipartBucket, curpartPath)
			return nil
		}, index)
	}
	g.Wait()
}

// ListMultipartUploads - lists all the pending multipart
// uploads for a particular object in a bucket.
//
// Implements minimal S3 compatible ListMultipartUploads API. We do
// not support prefix based listing, this is a deliberate attempt
// towards simplification of multipart APIs.
// The resulting ListMultipartsInfo structure is unmarshalled directly as XML.
func (er erasureObjects) ListMultipartUploads(ctx context.Context, bucket, object, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, e error) {
	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = object
	result.Delimiter = delimiter

	for _, disk := range er.getLoadBalancedDisks() {
		if disk == nil {
			continue
		}
		uploadIDs, err := disk.ListDir(minioMetaMultipartBucket, er.getMultipartSHADir(bucket, object), -1)
		if err != nil {
			if err == errFileNotFound {
				return result, nil
			}
			logger.LogIf(ctx, err)
			return result, err
		}
		for i := range uploadIDs {
			uploadIDs[i] = strings.TrimSuffix(uploadIDs[i], SlashSeparator)
		}
		sort.Strings(uploadIDs)
		for _, uploadID := range uploadIDs {
			if len(result.Uploads) == maxUploads {
				break
			}
			result.Uploads = append(result.Uploads, MultipartInfo{Object: object, UploadID: uploadID})
		}
		break
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
func (er erasureObjects) newMultipartUpload(ctx context.Context, bucket string, object string, opts ObjectOptions) (string, error) {

	onlineDisks := er.getDisks()
	parityBlocks := globalStorageClass.GetParityForSC(opts.UserDefined[xhttp.AmzStorageClass])
	if parityBlocks == 0 {
		parityBlocks = len(onlineDisks) / 2
	}
	dataBlocks := len(onlineDisks) - parityBlocks

	fi := newFileInfo(object, dataBlocks, parityBlocks)

	// we now know the number of blocks this object needs for data and parity.
	// establish the writeQuorum using this data
	writeQuorum := dataBlocks
	if dataBlocks == parityBlocks {
		writeQuorum = dataBlocks + 1
	}

	if opts.UserDefined["content-type"] == "" {
		contentType := mimedb.TypeByExtension(path.Ext(object))
		opts.UserDefined["content-type"] = contentType
	}

	// Calculate the version to be saved.
	if opts.Versioned {
		fi.VersionID = opts.VersionID
		if fi.VersionID == "" {
			fi.VersionID = mustGetUUID()
		}
	}

	fi.DataDir = mustGetUUID()
	fi.ModTime = UTCNow()
	fi.Metadata = opts.UserDefined

	uploadID := mustGetUUID()
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)
	tempUploadIDPath := uploadID

	// Delete the tmp path later in case we fail to commit (ignore
	// returned errors) - this will be a no-op in case of a commit
	// success.
	defer er.deleteObject(ctx, minioMetaTmpBucket, tempUploadIDPath, writeQuorum)

	var partsMetadata = make([]FileInfo, len(onlineDisks))
	for i := range onlineDisks {
		partsMetadata[i] = fi
	}

	var err error
	// Write updated `xl.meta` to all disks.
	onlineDisks, err = writeUniqueFileInfo(ctx, onlineDisks, minioMetaTmpBucket, tempUploadIDPath, partsMetadata, writeQuorum)
	if err != nil {
		return "", toObjectErr(err, minioMetaTmpBucket, tempUploadIDPath)
	}

	// Attempt to rename temp upload object to actual upload path object
	_, err = rename(ctx, onlineDisks, minioMetaTmpBucket, tempUploadIDPath, minioMetaMultipartBucket, uploadIDPath, true, writeQuorum, nil)
	if err != nil {
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
func (er erasureObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (string, error) {
	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}
	return er.newMultipartUpload(ctx, bucket, object, opts)
}

// CopyObjectPart - reads incoming stream and internally erasure codes
// them. This call is similar to put object part operation but the source
// data is read from an existing object.
//
// Implements S3 compatible Upload Part Copy API.
func (er erasureObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int, startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (pi PartInfo, e error) {
	partInfo, err := er.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, NewPutObjReader(srcInfo.Reader, nil, nil), dstOpts)
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
func (er erasureObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *PutObjReader, opts ObjectOptions) (pi PartInfo, e error) {
	data := r.Reader
	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument, logger.Application)
		return pi, toObjectErr(errInvalidArgument)
	}

	var partsMetadata []FileInfo
	var errs []error
	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	// Validates if upload ID exists.
	if err := er.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return pi, toObjectErr(err, bucket, object, uploadID)
	}

	// Read metadata associated with the object from all disks.
	partsMetadata, errs = readAllFileInfo(ctx, er.getDisks(), minioMetaMultipartBucket,
		uploadIDPath, "")

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(ctx, er, partsMetadata, errs)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	reducedErr := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errErasureWriteQuorum {
		return pi, toObjectErr(reducedErr, bucket, object)
	}

	// List all online disks.
	onlineDisks, modTime := listOnlineDisks(er.getDisks(), partsMetadata, errs)

	// Pick one from the first valid metadata.
	fi, err := pickValidFileInfo(ctx, partsMetadata, modTime, writeQuorum)
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
	defer er.deleteObject(ctx, minioMetaTmpBucket, tmpPart, writeQuorum)

	erasure, err := NewErasure(ctx, fi.Erasure.DataBlocks, fi.Erasure.ParityBlocks, fi.Erasure.BlockSize)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
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

	n, err := erasure.Encode(ctx, data, writers, buffer, fi.Erasure.DataBlocks+1)
	closeBitrotWriters(writers)
	if err != nil {
		return pi, toObjectErr(err, bucket, object)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if n < data.Size() {
		return pi, IncompleteBody{}
	}

	for i := range writers {
		if writers[i] == nil {
			onlineDisks[i] = nil
		}
	}

	// Validates if upload ID exists.
	if err := er.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return pi, toObjectErr(err, bucket, object, uploadID)
	}

	// Rename temporary part file to its final location.
	partPath := pathJoin(uploadIDPath, fi.DataDir, partSuffix)
	onlineDisks, err = rename(ctx, onlineDisks, minioMetaTmpBucket, tmpPartPath, minioMetaMultipartBucket, partPath, false, writeQuorum, nil)
	if err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	// Read metadata again because it might be updated with parallel upload of another part.
	partsMetadata, errs = readAllFileInfo(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath, "")
	reducedErr = reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errErasureWriteQuorum {
		return pi, toObjectErr(reducedErr, bucket, object)
	}

	// Get current highest version based on re-read partsMetadata.
	onlineDisks, modTime = listOnlineDisks(onlineDisks, partsMetadata, errs)

	// Pick one from the first valid metadata.
	fi, err = pickValidFileInfo(ctx, partsMetadata, modTime, writeQuorum)
	if err != nil {
		return pi, err
	}

	// Once part is successfully committed, proceed with updating erasure metadata.
	fi.ModTime = UTCNow()

	md5hex := r.MD5CurrentHexString()

	// Add the current part.
	fi.AddObjectPart(partID, md5hex, n, data.ActualSize())

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
	if _, err = writeUniqueFileInfo(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum); err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	// Return success.
	return PartInfo{
		PartNumber:   partID,
		ETag:         md5hex,
		LastModified: fi.ModTime,
		Size:         fi.Size,
		ActualSize:   data.ActualSize(),
	}, nil
}

// GetMultipartInfo returns multipart metadata uploaded during newMultipartUpload, used
// by callers to verify object states
// - encrypted
// - compressed
func (er erasureObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	result := MultipartInfo{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}

	if err := er.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	storageDisks := er.getDisks()

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, minioMetaMultipartBucket, uploadIDPath, opts.VersionID)

	// get Quorum for this object
	readQuorum, _, err := objectQuorumFromMeta(ctx, er, partsMetadata, errs)
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

	result.UserDefined = fi.Metadata
	return result, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is marshaled directly into XML and
// replied back to the client.
func (er erasureObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts ObjectOptions) (result ListPartsInfo, e error) {
	if err := er.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	storageDisks := er.getDisks()

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, minioMetaMultipartBucket, uploadIDPath, "")

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(ctx, er, partsMetadata, errs)
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
	result.UserDefined = fi.Metadata

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
func (er erasureObjects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, parts []CompletePart, opts ObjectOptions) (oi ObjectInfo, e error) {
	if err := er.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return oi, toObjectErr(err, bucket, object, uploadID)
	}

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
	if er.parentDirIsObject(ctx, bucket, path.Dir(object)) {
		return oi, toObjectErr(errFileParentIsFile, bucket, object)
	}

	defer ObjectPathUpdated(path.Join(bucket, object))

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5 := getCompleteMultipartMD5(parts)

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	storageDisks := er.getDisks()

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, storageDisks, minioMetaMultipartBucket, uploadIDPath, "")

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(ctx, er, partsMetadata, errs)
	if err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	reducedErr := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, writeQuorum)
	if reducedErr == errErasureWriteQuorum {
		return oi, toObjectErr(reducedErr, bucket, object)
	}

	onlineDisks, modTime := listOnlineDisks(storageDisks, partsMetadata, errs)

	// Calculate full object size.
	var objectSize int64

	// Calculate consolidated actual size.
	var objectActualSize int64

	// Pick one from the first valid metadata.
	fi, err := pickValidFileInfo(ctx, partsMetadata, modTime, writeQuorum)
	if err != nil {
		return oi, err
	}

	// Order online disks in accordance with distribution order.
	onlineDisks = shuffleDisks(onlineDisks, fi.Erasure.Distribution)

	// Order parts metadata in accordance with distribution order.
	partsMetadata = shufflePartsMetadata(partsMetadata, fi.Erasure.Distribution)

	// Save current erasure metadata for validation.
	var currentFI = fi

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
		}
	}

	// Save the final object size and modtime.
	fi.Size = objectSize
	fi.ModTime = UTCNow()

	// Save successfully calculated md5sum.
	fi.Metadata["etag"] = s3MD5

	// Save the consolidated actual size.
	fi.Metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)

	// Update all erasure metadata, make sure to not modify fields like
	// checksum which are different on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Size = fi.Size
		partsMetadata[index].ModTime = fi.ModTime
		partsMetadata[index].Metadata = fi.Metadata
		partsMetadata[index].Parts = fi.Parts
	}

	// Write final `xl.meta` at uploadID location
	if onlineDisks, err = writeUniqueFileInfo(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath, partsMetadata, writeQuorum); err != nil {
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
			er.removeObjectPart(bucket, object, uploadID, fi.DataDir, curpart.Number)
		}
	}

	// Rename the multipart object to final location.
	if onlineDisks, err = renameData(ctx, onlineDisks, minioMetaMultipartBucket, uploadIDPath,
		fi.DataDir, bucket, object, writeQuorum, nil); err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	// Check if there is any offline disk and add it to the MRF list
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

	// Success, return object info.
	return fi.ToObjectInfo(bucket, object), nil
}

// AbortMultipartUpload - aborts an ongoing multipart operation
// signified by the input uploadID. This is an atomic operation
// doesn't require clients to initiate multiple such requests.
//
// All parts are purged from all disks and reference to the uploadID
// would be removed from the system, rollback is not possible on this
// operation.
//
// Implements S3 compatible Abort multipart API, slight difference is
// that this is an atomic idempotent operation. Subsequent calls have
// no affect and further requests to the same uploadID would not be honored.
func (er erasureObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	// Validates if upload ID exists.
	if err := er.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return toObjectErr(err, bucket, object, uploadID)
	}

	uploadIDPath := er.getUploadIDDir(bucket, object, uploadID)

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := readAllFileInfo(ctx, er.getDisks(), minioMetaMultipartBucket, uploadIDPath, "")

	// get Quorum for this object
	_, writeQuorum, err := objectQuorumFromMeta(ctx, er, partsMetadata, errs)
	if err != nil {
		return toObjectErr(err, bucket, object, uploadID)
	}

	// Cleanup all uploaded parts.
	if err = er.deleteObject(ctx, minioMetaMultipartBucket, uploadIDPath, writeQuorum); err != nil {
		return toObjectErr(err, bucket, object, uploadID)
	}

	// Successfully purged.
	return nil
}
