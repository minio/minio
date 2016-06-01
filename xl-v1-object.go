package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/pkg/mimedb"
)

/// Object Operations

// GetObject - get an object.
func (xl xlObjects) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}

	// Lock the object before reading.
	nsMutex.RLock(bucket, object)
	defer nsMutex.RUnlock(bucket, object)

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := xl.readAllXLMetadata(bucket, object)
	if err := xl.reduceError(errs); err != nil {
		return toObjectErr(err, bucket, object)
	}

	// List all online disks.
	onlineDisks, _, err := xl.listOnlineDisks(partsMetadata, errs)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Pick one from the first valid metadata.
	xlMeta := partsMetadata[0]
	if !xlMeta.IsValid() {
		for _, partMetadata := range partsMetadata {
			if partMetadata.IsValid() {
				xlMeta = partMetadata
				break
			}
		}
	}

	// Get part index offset.
	partIndex, partOffset, err := xlMeta.ObjectToPartOffset(startOffset)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}

	// Read from all parts.
	for ; partIndex < len(xlMeta.Parts); partIndex++ {
		// Save the current part name and size.
		partName := xlMeta.Parts[partIndex].Name
		partSize := xlMeta.Parts[partIndex].Size

		// Initialize a new erasure with online disks, with previous
		// block distribution for each part reads.
		erasure := newErasure(onlineDisks, xlMeta.Erasure.Distribution)

		// Set previously calculated block checksums and algorithm for validation.
		erasure.SaveAlgo(checkSumAlgorithm(xlMeta, partIndex+1))
		erasure.SaveHashes(xl.metaPartBlockChecksums(partsMetadata, partIndex+1))

		// Data block size.
		blockSize := xlMeta.Erasure.BlockSize

		// Start reading the part name.
		var buffer []byte
		buffer, err = erasure.ReadFile(bucket, pathJoin(object, partName), partSize, blockSize)
		if err != nil {
			return err
		}

		// Copy to client until length requested.
		if length > int64(len(buffer)) {
			var m int64
			m, err = io.Copy(writer, bytes.NewReader(buffer[partOffset:]))
			if err != nil {
				return err
			}
			length -= m
		} else {
			_, err = io.CopyN(writer, bytes.NewReader(buffer[partOffset:]), length)
			if err != nil {
				return err
			}
			return nil
		}

		// Relinquish memory.
		buffer = nil

		// Reset part offset to 0 to read rest of the part from the beginning.
		partOffset = 0
	}

	// Return success.
	return nil
}

// GetObjectInfo - get object info.
func (xl xlObjects) GetObjectInfo(bucket, object string) (ObjectInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return ObjectInfo{}, ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	nsMutex.RLock(bucket, object)
	defer nsMutex.RUnlock(bucket, object)
	info, err := xl.getObjectInfo(bucket, object)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	return info, nil
}

// getObjectInfo - get object info.
func (xl xlObjects) getObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	var xlMeta xlMetaV1
	xlMeta, err = xl.readXLMetadata(bucket, object)
	if err != nil {
		// Return error.
		return ObjectInfo{}, err
	}
	objInfo = ObjectInfo{}
	objInfo.IsDir = false
	objInfo.Bucket = bucket
	objInfo.Name = object
	objInfo.Size = xlMeta.Stat.Size
	objInfo.ModTime = xlMeta.Stat.ModTime
	objInfo.MD5Sum = xlMeta.Meta["md5Sum"]
	objInfo.ContentType = xlMeta.Meta["content-type"]
	objInfo.ContentEncoding = xlMeta.Meta["content-encoding"]
	return objInfo, nil
}

// renameObject - renaming all source objects to destination object across all disks.
func (xl xlObjects) renameObject(srcBucket, srcObject, dstBucket, dstObject string) error {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var errs = make([]error, len(xl.storageDisks))

	// Rename file on all underlying storage disks.
	for index, disk := range xl.storageDisks {
		// Append "/" as srcObject and dstObject are either leaf-dirs or non-leaf-dris.
		// If srcObject is an object instead of prefix we just rename the leaf-dir and
		// not rename the part and metadata files separately.
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := disk.RenameFile(srcBucket, retainSlash(srcObject), dstBucket, retainSlash(dstObject))
			if err != nil {
				errs[index] = err
			}
			errs[index] = nil
		}(index, disk)
	}

	// Wait for all RenameFile to finish.
	wg.Wait()

	// Gather err count.
	var errCount = 0
	for _, err := range errs {
		if err == nil {
			continue
		}
		errCount++
	}
	// We can safely allow RenameFile errors up to len(xl.storageDisks) - xl.writeQuorum
	// otherwise return failure. Cleanup successful renames.
	if errCount > len(xl.storageDisks)-xl.writeQuorum {
		// Special condition if readQuorum exists, then return success.
		if errCount <= len(xl.storageDisks)-xl.readQuorum {
			return nil
		}
		// Rename back the object on disks where RenameFile succeeded
		for index, disk := range xl.storageDisks {
			// Rename back the object in parallel to reduce overall disk latency
			wg.Add(1)
			go func(index int, disk StorageAPI) {
				defer wg.Done()
				if errs[index] != nil {
					return
				}
				_ = disk.RenameFile(dstBucket, retainSlash(dstObject), srcBucket, retainSlash(srcObject))
			}(index, disk)
		}
		wg.Wait()
		return errWriteQuorum
	}
	return nil
}

// PutObject - create an object.
func (xl xlObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify bucket exists.
	if !xl.isBucketExist(bucket) {
		return "", BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}
	// No metadata is set, allocate a new one.
	if metadata == nil {
		metadata = make(map[string]string)
	}
	nsMutex.Lock(bucket, object)
	defer nsMutex.Unlock(bucket, object)

	uniqueID := getUUID()
	tempErasureObj := path.Join(tmpMetaPrefix, uniqueID, "object1")
	tempObj := path.Join(tmpMetaPrefix, uniqueID)

	// Initialize xl meta.
	xlMeta := newXLMetaV1(xl.dataBlocks, xl.parityBlocks)

	// Read metadata associated with the object from all disks.
	partsMetadata, errs := xl.readAllXLMetadata(bucket, object)

	// List all online disks.
	onlineDisks, higherVersion, err := xl.listOnlineDisks(partsMetadata, errs)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Increment version only if we have online disks less than configured storage disks.
	if diskCount(onlineDisks) < len(xl.storageDisks) {
		higherVersion++
	}

	// Initialize a new erasure with online disks and new distribution.
	erasure := newErasure(onlineDisks, xlMeta.Erasure.Distribution)

	// Initialize sha512 hash.
	erasure.InitHash("sha512")

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Allocated blockSized buffer for reading.
	buf := make([]byte, blockSizeV1)
	for {
		var n int
		n, err = io.ReadFull(data, buf)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return "", toObjectErr(err, bucket, object)
		}
		// Update md5 writer.
		md5Writer.Write(buf[:n])
		var m int64
		m, err = erasure.AppendFile(minioMetaBucket, tempErasureObj, buf[:n])
		if err != nil {
			return "", toObjectErr(err, minioMetaBucket, tempErasureObj)
		}
		if m != int64(len(buf[:n])) {
			return "", toObjectErr(errUnexpected, bucket, object)
		}
	}

	// Save additional erasureMetadata.
	modTime := time.Now().UTC()

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	// Update the md5sum if not set with the newly calculated one.
	if len(metadata["md5Sum"]) == 0 {
		metadata["md5Sum"] = newMD5Hex
	}
	// If not set default to "application/octet-stream"
	if metadata["content-type"] == "" {
		contentType := "application/octet-stream"
		if objectExt := filepath.Ext(object); objectExt != "" {
			content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]
			if ok {
				contentType = content.ContentType
			}
		}
		metadata["content-type"] = contentType
	}

	// md5Hex representation.
	md5Hex := metadata["md5Sum"]
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}

	// Check if an object is present as one of the parent dir.
	if xl.parentDirIsObject(bucket, path.Dir(object)) {
		return "", toObjectErr(errFileAccessDenied, bucket, object)
	}

	// Rename if an object already exists to temporary location.
	newUniqueID := getUUID()
	err = xl.renameObject(bucket, object, minioMetaBucket, path.Join(tmpMetaPrefix, newUniqueID))
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Fill all the necessary metadata.
	xlMeta.Meta = metadata
	xlMeta.Stat.Size = size
	xlMeta.Stat.ModTime = modTime
	xlMeta.Stat.Version = higherVersion
	// Add the final part.
	xlMeta.AddObjectPart(1, "object1", newMD5Hex, xlMeta.Stat.Size)

	// Get hash checksums.
	hashChecksums := erasure.GetHashes()

	// Save the checksums.
	checkSums := make([]checkSumInfo, len(xl.storageDisks))
	for index := range xl.storageDisks {
		blockIndex := xlMeta.Erasure.Distribution[index] - 1
		checkSums[blockIndex] = checkSumInfo{
			Name:      "object1",
			Algorithm: "sha512",
			Hash:      hashChecksums[blockIndex],
		}
	}

	// Update all the necessary fields making sure that checkSum field
	// is different for each disks.
	for index := range partsMetadata {
		blockIndex := xlMeta.Erasure.Distribution[index] - 1
		partsMetadata[index] = xlMeta
		partsMetadata[index].Erasure.Checksum = append(partsMetadata[index].Erasure.Checksum, checkSums[blockIndex])
	}

	// Write unique `xl.json` for each disk.
	if err = xl.writeUniqueXLMetadata(minioMetaBucket, tempObj, partsMetadata); err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Rename the successfully written tempoary object to final location.
	err = xl.renameObject(minioMetaBucket, tempObj, bucket, object)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Delete the temporary object.
	xl.deleteObject(minioMetaBucket, path.Join(tmpMetaPrefix, newUniqueID))

	// Return md5sum, successfully wrote object.
	return newMD5Hex, nil
}

// deleteObject - deletes a regular object.
func (xl xlObjects) deleteObject(bucket, object string) error {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(xl.storageDisks))

	for index, disk := range xl.storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := cleanupDir(disk, bucket, object)
			if err != nil {
				dErrs[index] = err
				return
			}
			dErrs[index] = nil
		}(index, disk)
	}

	// Wait for all routines to finish.
	wg.Wait()

	var fileNotFoundCnt, deleteFileErr int
	// Count for specific errors.
	for _, err := range dErrs {
		if err == nil {
			continue
		}
		// If file not found, count them.
		if err == errFileNotFound {
			fileNotFoundCnt++
			continue
		}

		// Update error counter separately.
		deleteFileErr++
	}

	// Return err if all disks report file not found.
	if fileNotFoundCnt == len(xl.storageDisks) {
		return errFileNotFound
	} else if deleteFileErr > len(xl.storageDisks)-xl.writeQuorum {
		// Return errWriteQuorum if errors were more than
		// allowed write quorum.
		return errWriteQuorum
	}

	return nil
}

// DeleteObject - delete the object.
func (xl xlObjects) DeleteObject(bucket, object string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	nsMutex.Lock(bucket, object)
	defer nsMutex.Unlock(bucket, object)
	xl.deleteObject(bucket, object)
	return nil
}
