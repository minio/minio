/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

// GetObject - reads an object erasured coded across multiple
// disks. Supports additional parameters like offset and length
// which is synonymous with HTTP Range requests.
//
// startOffset indicates the location at which the client requested
// object to be read at. length indicates the total length of the
// object requested by client.
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

	// Collect all the previous erasure infos across the disk.
	var eInfos []erasureInfo
	for index := range onlineDisks {
		eInfos = append(eInfos, partsMetadata[index].Erasure)
	}

	// Read from all parts.
	for ; partIndex < len(xlMeta.Parts); partIndex++ {
		// Save the current part name and size.
		partName := xlMeta.Parts[partIndex].Name
		partSize := xlMeta.Parts[partIndex].Size

		// Start reading the part name.
		var buffer []byte
		buffer, err = erasureReadFile(onlineDisks, bucket, pathJoin(object, partName), partName, partSize, eInfos)
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

		// Reset part offset to 0 to read rest of the part from the beginning.
		partOffset = 0
	} // End of read all parts loop.

	// Return success.
	return nil
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
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

// getObjectInfo - wrapper for reading object metadata and constructs ObjectInfo.
func (xl xlObjects) getObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	var xlMeta xlMetaV1
	xlMeta, err = xl.readXLMetadata(bucket, object)
	if err != nil {
		// Return error.
		return ObjectInfo{}, err
	}
	objInfo = ObjectInfo{
		IsDir:           false,
		Bucket:          bucket,
		Name:            object,
		Size:            xlMeta.Stat.Size,
		ModTime:         xlMeta.Stat.ModTime,
		MD5Sum:          xlMeta.Meta["md5Sum"],
		ContentType:     xlMeta.Meta["content-type"],
		ContentEncoding: xlMeta.Meta["content-encoding"],
	}
	return objInfo, nil
}

// renameObject - renames all source objects to destination object
// across all disks in parallel. Additionally if we have errors and do
// not have a readQuorum partially renamed files are renamed back to
// its proper location.
func (xl xlObjects) renameObject(srcBucket, srcObject, dstBucket, dstObject string) error {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var errs = make([]error, len(xl.storageDisks))

	// Rename file on all underlying storage disks.
	for index, disk := range xl.storageDisks {
		if disk == nil {
			errs[index] = errDiskNotFound
			continue
		}
		// Append "/" as srcObject and dstObject are either leaf-dirs or non-leaf-dris.
		// If srcObject is an object instead of prefix we just rename the leaf-dir and
		// not rename the part and metadata files separately.
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := disk.RenameFile(srcBucket, retainSlash(srcObject), dstBucket, retainSlash(dstObject))
			if err != nil && err != errFileNotFound {
				errs[index] = err
			}
		}(index, disk)
	}

	// Wait for all renames to finish.
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
		// Check we have successful read quorum.
		if errCount <= len(xl.storageDisks)-xl.readQuorum {
			return nil // Return success.
		} // else - failed to acquire read quorum.

		// Undo rename object on disks where RenameFile succeeded.
		for index, disk := range xl.storageDisks {
			if disk == nil {
				continue
			}
			// Undo rename object in parallel.
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
		return errXLWriteQuorum
	}
	return nil
}

// PutObject - creates an object upon reading from the input stream
// until EOF, erasure codes the data across all disk and additionally
// writes `xl.json` which carries the necessary metadata for future
// object operations.
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

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Tee reader combines incoming data stream and md5, data read
	// from input stream is written to md5.
	teeReader := io.TeeReader(data, md5Writer)

	// Collect all the previous erasure infos across the disk.
	var eInfos []erasureInfo
	for range onlineDisks {
		eInfos = append(eInfos, xlMeta.Erasure)
	}

	// Erasure code and write across all disks.
	newEInfos, n, err := erasureCreateFile(onlineDisks, minioMetaBucket, tempErasureObj, "object1", teeReader, eInfos)
	if err != nil {
		return "", toObjectErr(err, minioMetaBucket, tempErasureObj)
	}
	if size == -1 {
		size = n
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
			// MD5 mismatch, delete the temporary object.
			xl.deleteObject(minioMetaBucket, tempObj)
			// Returns md5 mismatch.
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
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

	// Update `xl.json` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index] = xlMeta
		partsMetadata[index].Erasure = newEInfos[index]
	}

	// Write unique `xl.json` for each disk.
	if err = xl.writeUniqueXLMetadata(minioMetaBucket, tempObj, partsMetadata); err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Rename the successfully written temporary object to final location.
	err = xl.renameObject(minioMetaBucket, tempObj, bucket, object)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Delete the temporary object.
	xl.deleteObject(minioMetaBucket, path.Join(tmpMetaPrefix, newUniqueID))

	// Return md5sum, successfully wrote object.
	return newMD5Hex, nil
}

// deleteObject - wrapper for delete object, deletes an object from
// all the disks in parallel, including `xl.json` associated with the
// object.
func (xl xlObjects) deleteObject(bucket, object string) error {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(xl.storageDisks))

	for index, disk := range xl.storageDisks {
		if disk == nil {
			dErrs[index] = errDiskNotFound
			continue
		}
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
		// Return errXLWriteQuorum if errors were more than
		// allowed write quorum.
		return errXLWriteQuorum
	}

	return nil
}

// DeleteObject - deletes an object, this call doesn't necessary reply
// any error as it is not necessary for the handler to reply back a
// response to the client request.
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
