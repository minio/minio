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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/safe"
)

// XL layer structure.
type XL struct {
	ReedSolomon  reedsolomon.Encoder // Erasure encoder/decoder.
	DataShards   int
	ParityShards int
	storageDisks []StorageAPI
	rwLock       *sync.RWMutex
}

// newXL instantiate a new XL.
func newXL(rootPaths ...string) (ObjectAPI, *probe.Error) {
	// Initialize XL.
	xl := &XL{}

	// Root paths.
	totalShards := len(rootPaths)
	isEven := func(number int) bool {
		return number%2 == 0
	}
	if !isEven(totalShards) {
		return nil, probe.NewError(errors.New("Invalid number of directories provided, should be always multiples of '2'"))
	}

	// Calculate data and parity shards.
	dataShards, parityShards := totalShards/2, totalShards/2

	// Initialize reed solomon encoding.
	rs, e := reedsolomon.New(dataShards, parityShards)
	if e != nil {
		return nil, probe.NewError(e)
	}

	// Save the reedsolomon.
	xl.ReedSolomon = rs
	xl.DataShards = dataShards
	xl.ParityShards = parityShards

	// Initialize all storage disks.
	storageDisks := make([]StorageAPI, len(rootPaths))
	for index, rootPath := range rootPaths {
		var e error
		storageDisks[index], e = newStorageDisk(rootPath)
		if e != nil {
			return nil, probe.NewError(e).Trace(rootPaths...)
		}
	}

	// Save all the initialized storage disks.
	xl.storageDisks = storageDisks

	// Read write lock.
	xl.rwLock = &sync.RWMutex{}

	// Return successfully initialized.
	return xl, nil
}

// MakeBucket - make a bucket.
func (xl XL) MakeBucket(bucket string) *probe.Error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	// Make a volume entry on all underlying storage disks.
	for _, disk := range xl.storageDisks {
		if e := disk.MakeVol(bucket); e != nil {
			return probe.NewError(e)
		}
	}
	return nil
}

// DeleteBucket - delete a bucket.
func (xl XL) DeleteBucket(bucket string) *probe.Error {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	for _, disk := range xl.storageDisks {
		if e := disk.DeleteVol(bucket); e != nil {
			return probe.NewError(e)
		}
	}
	return nil
}

// ListBuckets - list buckets.
func (xl XL) ListBuckets() (bucketsInfo []BucketInfo, err *probe.Error) {
	var volsInfo []VolInfo
	for _, disk := range xl.storageDisks {
		var e error
		volsInfo, e = disk.ListVols()
		if e != nil {
			return nil, probe.NewError(e)
		}
	}
	for _, volInfo := range volsInfo {
		bucketsInfo = append(bucketsInfo, BucketInfo{
			Name:    volInfo.Name,
			Created: volInfo.Created,
		})
	}
	return bucketsInfo, nil
}

// GetBucketInfo - get bucket info.
func (xl XL) GetBucketInfo(bucket string) (bucketInfo BucketInfo, err *probe.Error) {
	if !IsValidBucketName(bucket) {
		return BucketInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	var volInfo VolInfo
	for _, disk := range xl.storageDisks {
		var e error
		volInfo, e = disk.StatVol(bucket)
		if e != nil {
			return BucketInfo{}, probe.NewError(e)
		}
	}
	bucketInfo = BucketInfo{
		Name:    volInfo.Name,
		Created: volInfo.Created,
	}
	return bucketInfo, nil
}

// ListObjects - lists all objects for a given prefix, returns up to
// maxKeys number of objects per call.
func (xl XL) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, *probe.Error) {
	result := ListObjectsInfo{}
	// Input validation.
	if !IsValidBucketName(bucket) {
		return result, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectPrefix(prefix) {
		return result, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: prefix})
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != "/" {
		return result, probe.NewError(fmt.Errorf("delimiter '%s' is not supported", delimiter))
	}

	// Marker is set unescape.
	if marker != "" {
		if markerUnescaped, err := url.QueryUnescape(marker); err == nil {
			marker = markerUnescaped
		} else {
			return result, probe.NewError(err)
		}
		if !strings.HasPrefix(marker, prefix) {
			return result, probe.NewError(fmt.Errorf("Invalid combination of marker '%s' and prefix '%s'", marker, prefix))
		}
	}

	// Return empty response for a valid request when maxKeys is 0.
	if maxKeys == 0 {
		return result, nil
	}

	// Over flowing maxkeys - reset to listObjectsLimit.
	if maxKeys < 0 || maxKeys > listObjectsLimit {
		maxKeys = listObjectsLimit
	}

	// TODO
	return result, nil
}

// shardSize return the size of a single shard.
// The first non-zero size is returned,
// or 0 if all shards are size 0.
func shardSize(shards [][]byte) int {
	for _, shard := range shards {
		if len(shard) != 0 {
			return len(shard)
		}
	}
	return 0
}

// calculate the shardSize based on input length and total number of
// data shards.
func getEncodedBlockLen(inputLen, dataShards int) (curShardSize int) {
	curShardSize = (inputLen + dataShards - 1) / dataShards
	return
}

// Object API.

// GetObject - get an object.
func (xl XL) GetObject(bucket, object string, start int64) (io.ReadCloser, *probe.Error) {
	var objMetadata = make(map[string]string)
	var objReaders = make([]io.Reader, len(xl.storageDisks))
	for index, disk := range xl.storageDisks {
		// Do not need to use filepath here, since OS specific path
		// translation happens at storage layer.
		objMetadataFile := path.Join(object, "part.json")

		// Start from the beginning, we are not reading partial
		// metadata files.
		offset := int64(0)

		objMetadataReader, e := disk.ReadFile(bucket, objMetadataFile, offset)
		if e != nil {
			return nil, probe.NewError(e).Trace(objMetadataFile)
		}
		decoder := json.NewDecoder(objMetadataReader)
		if e = decoder.Decode(&objMetadata); e != nil {
			return nil, probe.NewError(e)
		}
		// Do not need to use filepath here, since OS specific path
		// translation happens at storage layer.
		objErasurePart := path.Join(object, fmt.Sprintf("part.%d", index))
		objErasuredPartReader, e := disk.ReadFile(bucket, objErasurePart, offset)
		if e != nil {
			objReaders[index] = nil
		} else {
			objReaders[index] = objErasuredPartReader
		}
	}
	size, e := strconv.ParseInt(objMetadata["obj.size"], 10, 64)
	if e != nil {
		return nil, probe.NewError(e)
	}
	totalShards := xl.DataShards + xl.ParityShards // Total shards.

	// Initialize pipe.
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		var totalLeft = size
		for totalLeft > 0 {
			// Figure out the right blockSize.
			var curBlockSize int
			if erasureBlockSize < totalLeft {
				curBlockSize = erasureBlockSize
			} else {
				curBlockSize = int(totalLeft)
			}
			// Calculate the current shard size.
			curShardSize := getEncodedBlockLen(curBlockSize, xl.DataShards)
			enShards := make([][]byte, totalShards)
			// Loop through all readers and read.
			for index, reader := range objReaders {
				if reader == nil {
					// One of files missing, save it for reconstruction.
					enShards[index] = nil
					continue
				}
				// Initialize shard slice and fill the data from each parts.
				enShards[index] = make([]byte, curShardSize)
				_, e := io.ReadFull(reader, enShards[index])
				if e != nil && e != io.ErrUnexpectedEOF {
					enShards[index] = nil
				}
			}
			if shardSize(enShards) == 0 {
				break
			}
			// Verify the shards.
			ok, e := xl.ReedSolomon.Verify(enShards)
			if e != nil {
				pipeWriter.CloseWithError(e)
				return
			}
			// Verification failed, shards require reconstruction.
			if !ok {
				e = xl.ReedSolomon.Reconstruct(enShards)
				if e != nil {
					pipeWriter.CloseWithError(e)
					return
				}
				// Verify reconstructed shards again.
				ok, e = xl.ReedSolomon.Verify(enShards)
				if e != nil {
					pipeWriter.CloseWithError(e)
					return
				}
				if !ok {
					// Shards cannot be reconstructed, corrupted data.
					e = errors.New("Verification failed after reconstruction, data likely corrupted.")
					pipeWriter.CloseWithError(e)
					return
				}
			}
			// Join the decoded shards.
			e = xl.ReedSolomon.Join(pipeWriter, enShards, curBlockSize)
			if e != nil {
				pipeWriter.CloseWithError(e)
				return
			}
			totalLeft = totalLeft - erasureBlockSize
		}
		// Cleanly end the pipe after a successful decoding.
		pipeWriter.Close()
	}()

	// Return the pipe for the top level caller to start reading.
	return pipeReader, nil
}

// GetObjectInfo - get object info.
func (xl XL) GetObjectInfo(bucket, object string) (ObjectInfo, *probe.Error) {
	// Check bucket name valid.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return ObjectInfo{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	var objMetadata = make(map[string]string)
	for index, disk := range xl.storageDisks {
		// Do not need to use filepath here, since OS specific path
		// translation happens at storage layer.
		objMetadataFile := path.Join(object, "part.json")
		// We are not going to read partial data from metadata file,
		// read the whole file always.
		offset := int64(0)
		objectMetadataReader, e := disk.ReadFile(bucket, objMetadataFile, offset)
		if e != nil {
			return ObjectInfo{}, probe.NewError(e)
		}
		decoder := json.NewDecoder(objectMetadataReader)
		// Unmarshalling failed, file corrupted.
		if e = decoder.Decode(&objMetadata); e != nil {
			return ObjectInfo{}, probe.NewError(e)
		}
		// Do not need to use filepath here, since OS specific path
		// translation happens at storage layer.
		objErasurePart := path.Join(object, fmt.Sprintf("part.%d", index))
		// Validate if all parts are available.
		_, e = disk.StatFile(bucket, objErasurePart)
		if e != nil {
			return ObjectInfo{}, probe.NewError(e)
		}
	}
	modTime, e := time.Parse(timeFormatAMZ, objMetadata["obj.modTime"])
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}
	size, e := strconv.ParseInt(objMetadata["obj.size"], 10, 64)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}
	return ObjectInfo{
		Bucket:       bucket,
		Name:         object,
		ModifiedTime: modTime,
		Size:         size,
		MD5Sum:       objMetadata["obj.md5"],
		ContentType:  objMetadata["obj.contentType"],
	}, nil
}

// Erasure block size.
const erasureBlockSize = 4 * 1024 * 1024 // 4MiB.

// PutObject - create an object.
func (xl XL) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (ObjectInfo, *probe.Error) {
	// Check bucket name valid.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}
	if !IsValidObjectName(object) {
		return ObjectInfo{}, probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// Initialize pipe lines for each disks.
	readers := make([]*io.PipeReader, len(xl.storageDisks))
	writers := make([]*io.PipeWriter, len(xl.storageDisks))
	for index := range xl.storageDisks {
		readers[index], writers[index] = io.Pipe()
	}

	hashWriter := md5.New()
	contentType := metadata["contentType"]
	expectedMD5Sum := metadata["md5"]
	var calculatedMD5Sum string

	// Start encoding and writing in a routine.
	go func() {
		buffer := make([]byte, erasureBlockSize)
		for {
			n, e := io.ReadFull(data, buffer)
			if e != nil {
				if e != io.ErrUnexpectedEOF && e != io.EOF {
					for index := range writers {
						// Close all writers for one error.
						writers[index].CloseWithError(e)
					}
					return
				}
			}
			if e == io.EOF {
				break
			}
			if n > 0 {
				// Add up to the hash we need to validate.
				hashWriter.Write(buffer[0:n])

				shards, e := xl.ReedSolomon.Split(buffer[0:n])
				if e != nil {
					for index := range writers {
						// Close all writers for byte splitting error.
						writers[index].CloseWithError(e)
					}
					return
				}
				e = xl.ReedSolomon.Encode(shards)
				if e != nil {
					for index := range writers {
						// Close all writers even for encoding error.
						writers[index].CloseWithError(e)
					}
					return
				}
				for index, data := range shards {
					_, e = writers[index].Write(data)
					if e != nil {
						for index = range writers {
							// Close all writers even if one of the
							// writers fail.
							writers[index].CloseWithError(e)
						}
						return
					}
				}
			}
		}
		// Only validate if set by the client request.
		calculatedMD5Sum = hex.EncodeToString(hashWriter.Sum(nil))
		if expectedMD5Sum != "" {
			if calculatedMD5Sum != expectedMD5Sum {
				for index := range writers {
					// Close all writers if digest mismatches.
					writers[index].CloseWithError(BadDigest{
						ExpectedMD5:   expectedMD5Sum,
						CalculatedMD5: calculatedMD5Sum,
					})
				}
				return
			}
		}
		// Close all writers.
		for _, writer := range writers {
			writer.Close()
		}
	}()

	// Allocate error channels.
	errChs := make([]chan error, len(xl.storageDisks))
	for index := range errChs {
		errCh := make(chan error, 1)
		errChs[index] = errCh
	}

	// Initialize wait group.
	var wg = &sync.WaitGroup{}

	// Loop through and write each erasured chunks.
	for index, disk := range xl.storageDisks {
		// Do not need to use filepath here, since OS specific path
		// translation happens at storage layer.
		erasureObjectPart := path.Join(object, fmt.Sprintf("part.%d", index))
		// Add go-routine to waitgroup.
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			// Indicate completion of the routine.
			defer wg.Done()

			// Write the erasured chunk.
			writer, e := disk.CreateFile(bucket, erasureObjectPart)
			if e != nil {
				errChs[index] <- e
				return
			}
			// TODO: use io.CopyN with exact shard size.
			_, e = io.Copy(writer, readers[index])
			if e != nil {
				// Purge the files partially written upon error,
				// figure out if the underlying writer supports
				// purging.
				safeCloser, ok := writer.(safe.PurgeCloser)
				if ok {
					safeCloser.PurgeClose()
				}
				errChs[index] <- e
				return
			}
			errChs[index] <- nil
		}(index, disk)
	}
	// Wait for the go-routines to complete.
	wg.Wait()

	for _, errCh := range errChs {
		// Close error channel.
		defer close(errCh)

		// Verify indeed if there is an error.
		if e := <-errCh; e != nil {
			return ObjectInfo{}, probe.NewError(e)
		}
	}

	// Save additional erasureMetadata.
	modTime := time.Now().UTC()
	objMetadata := make(map[string]string)
	objMetadata["obj.md5"] = calculatedMD5Sum
	objMetadata["obj.size"] = strconv.FormatInt(size, 10)
	objMetadata["obj.contentType"] = contentType
	objMetadata["obj.modTime"] = modTime.Format(timeFormatAMZ)
	objMetadata["obj.rs.library"] = "klauspost"
	objMetadata["obj.rs.blockSize"] = strconv.Itoa(erasureBlockSize)
	objMetadata["obj.rs.dataShards"] = strconv.Itoa(xl.DataShards)
	objMetadata["obj.rs.parityShards"] = strconv.Itoa(xl.ParityShards)
	objMetadataBytes, e := json.Marshal(objMetadata)
	if e != nil {
		return ObjectInfo{}, probe.NewError(e)
	}
	for _, disk := range xl.storageDisks {
		// Do not need to use filepath here, since OS specific path
		// translation happens at storage layer.
		objMetadataFile := path.Join(object, "/part.json")

		// Create metadata file.
		writeCloser, e := disk.CreateFile(bucket, objMetadataFile)
		if e != nil {
			return ObjectInfo{}, probe.NewError(e).Trace(bucket, objMetadataFile)
		}
		_, e = io.CopyN(writeCloser, bytes.NewReader(objMetadataBytes), int64(len(objMetadataBytes)))
		if e != nil {
			return ObjectInfo{}, probe.NewError(e).Trace(bucket, objMetadataFile)
		}
		// Close and commit the file.
		writeCloser.Close()
	}
	return ObjectInfo{
		Bucket:       bucket,
		Name:         object,
		ModifiedTime: modTime,
		Size:         size,
		MD5Sum:       calculatedMD5Sum,
		ContentType:  contentType,
	}, nil
}

// DeleteObject - all an object.
func (xl XL) DeleteObject(bucket, object string) *probe.Error {
	// Check bucket name valid
	if !IsValidBucketName(bucket) {
		return probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// Verify object path legal
	if !IsValidObjectName(object) {
		return probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}

	// Loop through and delete each chunks.
	for index, disk := range xl.storageDisks {
		// Do not need to use filepath here, since OS specific path
		// translation happens at storage layer.
		erasureObjectPart := path.Join(object, fmt.Sprintf("part.%d", index))
		e := disk.DeleteFile(bucket, erasureObjectPart)
		if e != nil {
			return probe.NewError(e)
		}
	}
	return nil
}

// Multipart level API.

// NewMultipartUpload -
func (xl XL) NewMultipartUpload(bucket, object string) (string, *probe.Error) {
	// Check bucket name valid
	if !IsValidBucketName(bucket) {
		return "", probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	// Verify object path legal
	if !IsValidObjectName(object) {
		return "", probe.NewError(ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	return "", nil
}

// PutObjectPart -
func (xl XL) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, expectedMD5 string) (string, *probe.Error) {
	return "", nil
}

// CompleteMultipartUpload -
func (xl XL) CompleteMultipartUpload(bucket string, object string, uploadID string, parts []completePart) (ObjectInfo, *probe.Error) {
	return ObjectInfo{}, nil
}

// ListMultipartUploads -
func (xl XL) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, *probe.Error) {
	return ListMultipartsInfo{}, nil
}

// ListObjectParts -
func (xl XL) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (ListPartsInfo, *probe.Error) {
	return ListPartsInfo{}, nil
}

// AbortMultipartUpload -
func (xl XL) AbortMultipartUpload(bucket, object, uploadID string) *probe.Error {
	return nil
}
