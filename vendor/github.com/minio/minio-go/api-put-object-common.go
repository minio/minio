/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015 Minio, Inc.
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

package minio

import (
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"math"
	"os"
)

// Verify if reader is *os.File
func isFile(reader io.Reader) (ok bool) {
	_, ok = reader.(*os.File)
	return
}

// Verify if reader is *minio.Object
func isObject(reader io.Reader) (ok bool) {
	_, ok = reader.(*Object)
	return
}

// Verify if reader is a generic ReaderAt
func isReadAt(reader io.Reader) (ok bool) {
	_, ok = reader.(io.ReaderAt)
	return
}

// shouldUploadPart - verify if part should be uploaded.
func shouldUploadPart(objPart ObjectPart, uploadReq uploadPartReq) bool {
	// If part not found should upload the part.
	if uploadReq.Part == nil {
		return true
	}
	// if size mismatches should upload the part.
	if objPart.Size != uploadReq.Part.Size {
		return true
	}
	// if md5sum mismatches should upload the part.
	if objPart.ETag != uploadReq.Part.ETag {
		return true
	}
	return false
}

// optimalPartInfo - calculate the optimal part info for a given
// object size.
//
// NOTE: Assumption here is that for any object to be uploaded to any S3 compatible
// object storage it will have the following parameters as constants.
//
//  maxPartsCount - 10000
//  minPartSize - 64MiB
//  maxMultipartPutObjectSize - 5TiB
//
func optimalPartInfo(objectSize int64) (totalPartsCount int, partSize int64, lastPartSize int64, err error) {
	// object size is '-1' set it to 5TiB.
	if objectSize == -1 {
		objectSize = maxMultipartPutObjectSize
	}
	// object size is larger than supported maximum.
	if objectSize > maxMultipartPutObjectSize {
		err = ErrEntityTooLarge(objectSize, maxMultipartPutObjectSize, "", "")
		return
	}
	// Use floats for part size for all calculations to avoid
	// overflows during float64 to int64 conversions.
	partSizeFlt := math.Ceil(float64(objectSize / maxPartsCount))
	partSizeFlt = math.Ceil(partSizeFlt/minPartSize) * minPartSize
	// Total parts count.
	totalPartsCount = int(math.Ceil(float64(objectSize) / partSizeFlt))
	// Part size.
	partSize = int64(partSizeFlt)
	// Last part size.
	lastPartSize = objectSize - int64(totalPartsCount-1)*partSize
	return totalPartsCount, partSize, lastPartSize, nil
}

// hashCopyBuffer is identical to hashCopyN except that it doesn't take
// any size argument but takes a buffer argument and reader should be
// of io.ReaderAt interface.
//
// Stages reads from offsets into the buffer, if buffer is nil it is
// initialized to optimalBufferSize.
func hashCopyBuffer(hashAlgorithms map[string]hash.Hash, hashSums map[string][]byte, writer io.Writer, reader io.ReaderAt, buf []byte) (size int64, err error) {
	hashWriter := writer
	for _, v := range hashAlgorithms {
		hashWriter = io.MultiWriter(hashWriter, v)
	}

	// Buffer is nil, initialize.
	if buf == nil {
		buf = make([]byte, optimalReadBufferSize)
	}

	// Offset to start reading from.
	var readAtOffset int64

	// Following block reads data at an offset from the input
	// reader and copies data to into local temporary file.
	for {
		readAtSize, rerr := reader.ReadAt(buf, readAtOffset)
		if rerr != nil {
			if rerr != io.EOF {
				return 0, rerr
			}
		}
		writeSize, werr := hashWriter.Write(buf[:readAtSize])
		if werr != nil {
			return 0, werr
		}
		if readAtSize != writeSize {
			return 0, fmt.Errorf("Read size was not completely written to writer. wanted %d, got %d - %s", readAtSize, writeSize, reportIssue)
		}
		readAtOffset += int64(writeSize)
		size += int64(writeSize)
		if rerr == io.EOF {
			break
		}
	}

	for k, v := range hashAlgorithms {
		hashSums[k] = v.Sum(nil)
	}
	return size, err
}

// hashCopyN - Calculates chosen hashes up to partSize amount of bytes.
func hashCopyN(hashAlgorithms map[string]hash.Hash, hashSums map[string][]byte, writer io.Writer, reader io.Reader, partSize int64) (size int64, err error) {
	hashWriter := writer
	for _, v := range hashAlgorithms {
		hashWriter = io.MultiWriter(hashWriter, v)
	}

	// Copies to input at writer.
	size, err = io.CopyN(hashWriter, reader, partSize)
	if err != nil {
		// If not EOF return error right here.
		if err != io.EOF {
			return 0, err
		}
	}

	for k, v := range hashAlgorithms {
		hashSums[k] = v.Sum(nil)
	}
	return size, err
}

// getUploadID - fetch upload id if already present for an object name
// or initiate a new request to fetch a new upload id.
func (c Client) newUploadID(bucketName, objectName string, metaData map[string][]string) (uploadID string, err error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return "", err
	}
	if err := isValidObjectName(objectName); err != nil {
		return "", err
	}

	// Initiate multipart upload for an object.
	initMultipartUploadResult, err := c.initiateMultipartUpload(bucketName, objectName, metaData)
	if err != nil {
		return "", err
	}
	return initMultipartUploadResult.UploadID, nil
}

// getMpartUploadSession returns the upload id and the uploaded parts to continue a previous upload session
// or initiate a new multipart session if no current one found
func (c Client) getMpartUploadSession(bucketName, objectName string, metaData map[string][]string) (string, map[int]ObjectPart, error) {
	// A map of all uploaded parts.
	var partsInfo map[int]ObjectPart
	var err error

	uploadID, err := c.findUploadID(bucketName, objectName)
	if err != nil {
		return "", nil, err
	}

	if uploadID == "" {
		// Initiates a new multipart request
		uploadID, err = c.newUploadID(bucketName, objectName, metaData)
		if err != nil {
			return "", nil, err
		}
	} else {
		// Fetch previously upload parts and maximum part size.
		partsInfo, err = c.listObjectParts(bucketName, objectName, uploadID)
		if err != nil {
			// When the server returns NoSuchUpload even if its previouls acknowleged the existance of the upload id,
			// initiate a new multipart upload
			if respErr, ok := err.(ErrorResponse); ok && respErr.Code == "NoSuchUpload" {
				uploadID, err = c.newUploadID(bucketName, objectName, metaData)
				if err != nil {
					return "", nil, err
				}
			} else {
				return "", nil, err
			}
		}
	}

	// Allocate partsInfo if not done yet
	if partsInfo == nil {
		partsInfo = make(map[int]ObjectPart)
	}

	return uploadID, partsInfo, nil
}

// computeHash - Calculates hashes for an input read Seeker.
func computeHash(hashAlgorithms map[string]hash.Hash, hashSums map[string][]byte, reader io.ReadSeeker) (size int64, err error) {
	hashWriter := ioutil.Discard
	for _, v := range hashAlgorithms {
		hashWriter = io.MultiWriter(hashWriter, v)
	}

	// If no buffer is provided, no need to allocate just use io.Copy.
	size, err = io.Copy(hashWriter, reader)
	if err != nil {
		return 0, err
	}

	// Seek back reader to the beginning location.
	if _, err := reader.Seek(0, 0); err != nil {
		return 0, err
	}

	for k, v := range hashAlgorithms {
		hashSums[k] = v.Sum(nil)
	}
	return size, nil
}
