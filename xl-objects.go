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
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/minio/minio/pkg/mimedb"
)

const (
	multipartSuffix   = ".minio.multipart"
	multipartMetaFile = "00000" + multipartSuffix
	formatConfigFile  = "format.json"
)

// xlObjects - Implements fs object layer.
type xlObjects struct {
	storage            StorageAPI
	listObjectMap      map[listParams][]*treeWalker
	listObjectMapMutex *sync.Mutex
}

// errMaxDisks - returned for reached maximum of disks.
var errMaxDisks = errors.New("Number of disks are higher than supported maximum count '16'")

// errMinDisks - returned for minimum number of disks.
var errMinDisks = errors.New("Number of disks are smaller than supported minimum count '8'")

// errNumDisks - returned for odd number of disks.
var errNumDisks = errors.New("Number of disks should be multiples of '2'")

const (
	// Maximum erasure blocks.
	maxErasureBlocks = 16
	// Minimum erasure blocks.
	minErasureBlocks = 8
)

func checkSufficientDisks(disks []string) error {
	// Verify total number of disks.
	totalDisks := len(disks)
	if totalDisks > maxErasureBlocks {
		return errMaxDisks
	}
	if totalDisks < minErasureBlocks {
		return errMinDisks
	}

	// isEven function to verify if a given number if even.
	isEven := func(number int) bool {
		return number%2 == 0
	}

	// Verify if we have even number of disks.
	// only combination of 8, 10, 12, 14, 16 are supported.
	if !isEven(totalDisks) {
		return errNumDisks
	}

	return nil
}

// Depending on the disk type network or local, initialize storage layer.
func newStorageLayer(disk string) (storage StorageAPI, err error) {
	if !strings.ContainsRune(disk, ':') || filepath.VolumeName(disk) != "" {
		// Initialize filesystem storage API.
		return newPosix(disk)
	}
	// Initialize rpc client storage API.
	return newRPCClient(disk)
}

// Initialize all storage disks to bootstrap.
func bootstrapDisks(disks []string) ([]StorageAPI, error) {
	storageDisks := make([]StorageAPI, len(disks))
	for index, disk := range disks {
		var err error
		// Intentionally ignore disk not found errors while
		// initializing POSIX, so that we have successfully
		// initialized posix Storage. Subsequent calls to XL/Erasure
		// will manage any errors related to disks.
		storageDisks[index], err = newStorageLayer(disk)
		if err != nil && err != errDiskNotFound {
			return nil, err
		}
	}
	return storageDisks, nil
}

// newXLObjects - initialize new xl object layer.
func newXLObjects(disks []string) (ObjectLayer, error) {
	if err := checkSufficientDisks(disks); err != nil {
		return nil, err
	}

	storageDisks, err := bootstrapDisks(disks)
	if err != nil {
		return nil, err
	}

	// Initialize object layer - like creating minioMetaBucket, cleaning up tmp files etc.
	initObjectLayer(storageDisks...)

	// Load saved XL format.json and validate.
	newDisks, err := loadFormatXL(storageDisks)
	if err != nil {
		switch err {
		case errUnformattedDisk:
			// Save new XL format.
			errSave := initFormatXL(storageDisks)
			if errSave != nil {
				return nil, errSave
			}
			newDisks = storageDisks
		default:
			// errCorruptedDisk - error.
			return nil, fmt.Errorf("Unable to recognize backend format, %s", err)
		}
	}

	// FIXME: healFormatXL(newDisks)

	storage, err := newXL(newDisks)
	if err != nil {
		return nil, err
	}

	// Return successfully initialized object layer.
	return xlObjects{
		storage:            storage,
		listObjectMap:      make(map[listParams][]*treeWalker),
		listObjectMapMutex: &sync.Mutex{},
	}, nil
}

/// Bucket operations

// MakeBucket - make a bucket.
func (xl xlObjects) MakeBucket(bucket string) error {
	nsMutex.Lock(bucket, "")
	defer nsMutex.Unlock(bucket, "")
	return makeBucket(xl.storage, bucket)
}

// GetBucketInfo - get bucket info.
func (xl xlObjects) GetBucketInfo(bucket string) (BucketInfo, error) {
	nsMutex.RLock(bucket, "")
	defer nsMutex.RUnlock(bucket, "")
	return getBucketInfo(xl.storage, bucket)
}

// ListBuckets - list buckets.
func (xl xlObjects) ListBuckets() ([]BucketInfo, error) {
	return listBuckets(xl.storage)
}

// DeleteBucket - delete a bucket.
func (xl xlObjects) DeleteBucket(bucket string) error {
	nsMutex.Lock(bucket, "")
	nsMutex.Unlock(bucket, "")
	return deleteBucket(xl.storage, bucket)
}

/// Object Operations

// GetObject - get an object.
func (xl xlObjects) GetObject(bucket, object string, startOffset int64) (io.ReadCloser, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return nil, BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return nil, ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	nsMutex.RLock(bucket, object)
	defer nsMutex.RUnlock(bucket, object)
	if !isMultipartObject(xl.storage, bucket, object) {
		_, err := xl.storage.StatFile(bucket, object)
		if err == nil {
			var reader io.ReadCloser
			reader, err = xl.storage.ReadFile(bucket, object, startOffset)
			if err != nil {
				return nil, toObjectErr(err, bucket, object)
			}
			return reader, nil
		}
		return nil, toObjectErr(err, bucket, object)
	}
	fileReader, fileWriter := io.Pipe()
	info, err := getMultipartObjectInfo(xl.storage, bucket, object)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}
	partIndex, offset, err := info.GetPartNumberOffset(startOffset)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}

	// Hold a read lock once more which can be released after the following go-routine ends.
	// We hold RLock once more because the current function would return before the go routine below
	// executes and hence releasing the read lock (because of defer'ed nsMutex.RUnlock() call).
	nsMutex.RLock(bucket, object)
	go func() {
		defer nsMutex.RUnlock(bucket, object)
		for ; partIndex < len(info.Parts); partIndex++ {
			part := info.Parts[partIndex]
			r, err := xl.storage.ReadFile(bucket, pathJoin(object, partNumToPartFileName(part.PartNumber)), offset)
			if err != nil {
				fileWriter.CloseWithError(err)
				return
			}
			// Reset offset to 0 as it would be non-0 only for the first loop if startOffset is non-0.
			offset = 0
			if _, err = io.Copy(fileWriter, r); err != nil {
				switch reader := r.(type) {
				case *io.PipeReader:
					reader.CloseWithError(err)
				case io.ReadCloser:
					reader.Close()
				}
				fileWriter.CloseWithError(err)
				return
			}
			// Close the readerCloser that reads multiparts of an object from the xl storage layer.
			// Not closing leaks underlying file descriptors.
			r.Close()
		}
		fileWriter.Close()
	}()
	return fileReader, nil
}

// Return the partsInfo of a special multipart object.
func getMultipartObjectInfo(storage StorageAPI, bucket, object string) (info MultipartObjectInfo, err error) {
	offset := int64(0)
	r, err := storage.ReadFile(bucket, pathJoin(object, multipartMetaFile), offset)
	if err != nil {
		return MultipartObjectInfo{}, err
	}
	decoder := json.NewDecoder(r)
	err = decoder.Decode(&info)
	if err != nil {
		return MultipartObjectInfo{}, err
	}
	return info, nil
}

// Return ObjectInfo.
func (xl xlObjects) getObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	objInfo.Bucket = bucket
	objInfo.Name = object
	// First see if the object was a simple-PUT upload.
	fi, err := xl.storage.StatFile(bucket, object)
	if err != nil {
		if err != errFileNotFound {
			return ObjectInfo{}, err
		}
		var info MultipartObjectInfo
		// Check if the object was multipart upload.
		info, err = getMultipartObjectInfo(xl.storage, bucket, object)
		if err != nil {
			return ObjectInfo{}, err
		}
		objInfo.Size = info.Size
		objInfo.ModTime = info.ModTime
		objInfo.MD5Sum = info.MD5Sum
		objInfo.ContentType = info.ContentType
		objInfo.ContentEncoding = info.ContentEncoding
	} else {
		metadata := make(map[string]string)
		offset := int64(0) // To read entire content
		r, err := xl.storage.ReadFile(bucket, pathJoin(object, "meta.json"), offset)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		decoder := json.NewDecoder(r)
		if err = decoder.Decode(&metadata); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		contentType := metadata["content-type"]
		if len(contentType) == 0 {
			contentType = "application/octet-stream"
			if objectExt := filepath.Ext(object); objectExt != "" {
				content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]
				if ok {
					contentType = content.ContentType
				}
			}
		}
		objInfo.Size = fi.Size
		objInfo.IsDir = fi.Mode.IsDir()
		objInfo.ModTime = fi.ModTime
		objInfo.MD5Sum = metadata["md5Sum"]
		objInfo.ContentType = contentType
		objInfo.ContentEncoding = metadata["content-encoding"]
	}
	return objInfo, nil
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

// PutObject - create an object.
func (xl xlObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	// Verify bucket exists.
	if !isBucketExist(xl.storage, bucket) {
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

	tempObj := path.Join(tmpMetaPrefix, bucket, object)
	fileWriter, err := xl.storage.CreateFile(minioMetaBucket, tempObj)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Initialize md5 writer.
	md5Writer := md5.New()

	// Instantiate a new multi writer.
	multiWriter := io.MultiWriter(md5Writer, fileWriter)

	// Instantiate checksum hashers and create a multiwriter.
	if size > 0 {
		if _, err = io.CopyN(multiWriter, data, size); err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", toObjectErr(clErr, bucket, object)
			}
			return "", toObjectErr(err, bucket, object)
		}
	} else {
		if _, err = io.Copy(multiWriter, data); err != nil {
			if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
				return "", toObjectErr(clErr, bucket, object)
			}
			return "", toObjectErr(err, bucket, object)
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	// Update the md5sum if not set with the newly calculated one.
	if len(metadata["md5Sum"]) == 0 {
		metadata["md5Sum"] = newMD5Hex
	}

	// md5Hex representation.
	md5Hex := metadata["md5Sum"]
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			if err = safeCloseAndRemove(fileWriter); err != nil {
				return "", toObjectErr(err, bucket, object)
			}
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}

	err = fileWriter.Close()
	if err != nil {
		if clErr := safeCloseAndRemove(fileWriter); clErr != nil {
			return "", toObjectErr(clErr, bucket, object)
		}
		return "", toObjectErr(err, bucket, object)
	}

	// Check if an object is present as one of the parent dir.
	if err = xl.parentDirIsObject(bucket, path.Dir(object)); err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Delete if an object already exists.
	// FIXME: rename it to tmp file and delete only after
	// the newly uploaded file is renamed from tmp location to
	// the original location.
	// Verify if the object is a multipart object.
	if isMultipartObject(xl.storage, bucket, object) {
		err = xl.deleteMultipartObject(bucket, object)
		if err != nil {
			return "", toObjectErr(err, bucket, object)
		}
	} else {
		err = xl.deleteObject(bucket, object)
		if err != nil {
			return "", toObjectErr(err, bucket, object)
		}
	}

	err = xl.storage.RenameFile(minioMetaBucket, tempObj, bucket, object)
	if err != nil {
		if dErr := xl.storage.DeleteFile(minioMetaBucket, tempObj); dErr != nil {
			return "", toObjectErr(dErr, bucket, object)
		}
		return "", toObjectErr(err, bucket, object)
	}

	tempMetaJSONFile := path.Join(tmpMetaPrefix, bucket, object, "meta.json")
	metaWriter, err := xl.storage.CreateFile(minioMetaBucket, tempMetaJSONFile)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	encoder := json.NewEncoder(metaWriter)
	err = encoder.Encode(&metadata)
	if err != nil {
		if clErr := safeCloseAndRemove(metaWriter); clErr != nil {
			return "", toObjectErr(clErr, bucket, object)
		}
		return "", toObjectErr(err, bucket, object)
	}
	if err = metaWriter.Close(); err != nil {
		if err = safeCloseAndRemove(metaWriter); err != nil {
			return "", toObjectErr(err, bucket, object)
		}
		return "", toObjectErr(err, bucket, object)
	}

	metaJSONFile := path.Join(object, "meta.json")
	err = xl.storage.RenameFile(minioMetaBucket, tempMetaJSONFile, bucket, metaJSONFile)
	if err != nil {
		if derr := xl.storage.DeleteFile(minioMetaBucket, tempMetaJSONFile); derr != nil {
			return "", toObjectErr(derr, bucket, object)
		}
		return "", toObjectErr(err, bucket, object)
	}

	// Return md5sum, successfully wrote object.
	return newMD5Hex, nil
}

// isMultipartObject - verifies if an object is special multipart file.
func isMultipartObject(storage StorageAPI, bucket, object string) bool {
	_, err := storage.StatFile(bucket, pathJoin(object, multipartMetaFile))
	if err != nil {
		if err == errFileNotFound {
			return false
		}
		errorIf(err, "Failed to stat file "+bucket+pathJoin(object, multipartMetaFile))
		return false
	}
	return true
}

// deleteMultipartObject - deletes only multipart object.
func (xl xlObjects) deleteMultipartObject(bucket, object string) error {
	// Get parts info.
	info, err := getMultipartObjectInfo(xl.storage, bucket, object)
	if err != nil {
		return err
	}
	// Range through all files and delete it.
	var wg = &sync.WaitGroup{}
	var errs = make([]error, len(info.Parts))
	for index, part := range info.Parts {
		wg.Add(1)
		// Start deleting parts in routine.
		go func(index int, part MultipartPartInfo) {
			defer wg.Done()
			partFileName := partNumToPartFileName(part.PartNumber)
			errs[index] = xl.storage.DeleteFile(bucket, pathJoin(object, partFileName))
		}(index, part)
	}
	// Wait for all the deletes to finish.
	wg.Wait()
	// Loop through and validate if any errors, if we are unable to remove any part return
	// "unexpected" error as returning any other error might be misleading. For ex.
	// if DeleteFile() had returned errFileNotFound and we return it, then client would see
	// ObjectNotFound which is misleading.
	for _, err := range errs {
		if err != nil {
			return errUnexpected
		}
	}
	err = xl.storage.DeleteFile(bucket, pathJoin(object, multipartMetaFile))
	if err != nil {
		return err
	}
	return nil
}

// deleteObject - deletes a regular object.
func (xl xlObjects) deleteObject(bucket, object string) error {
	metaJSONFile := path.Join(object, "meta.json")
	// Ignore if meta.json file doesn't exist.
	if err := xl.storage.DeleteFile(bucket, metaJSONFile); err != nil {
		if err != errFileNotFound {
			return err
		}
	}
	if err := xl.storage.DeleteFile(bucket, object); err != nil {
		if err != errFileNotFound {
			return err
		}
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
	// Verify if the object is a multipart object.
	if isMultipartObject(xl.storage, bucket, object) {
		err := xl.deleteMultipartObject(bucket, object)
		if err != nil {
			return toObjectErr(err, bucket, object)
		}
		return nil
	}
	err := xl.deleteObject(bucket, object)
	if err != nil {
		return toObjectErr(err, bucket, object)
	}
	return nil
}

// ListObjects - list all objects at prefix, delimited by '/'.
func (xl xlObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return listObjectsCommon(xl, bucket, prefix, marker, delimiter, maxKeys)
}
