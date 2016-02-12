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
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
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
func shouldUploadPart(objPart objectPart, objectParts map[int]objectPart) bool {
	// If part not found should upload the part.
	uploadedPart, found := objectParts[objPart.PartNumber]
	if !found {
		return true
	}
	// if size mismatches should upload the part.
	if objPart.Size != uploadedPart.Size {
		return true
	}
	// if md5sum mismatches should upload the part.
	if objPart.ETag != uploadedPart.ETag {
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
//  minPartSize - 5MiB
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
func (c Client) hashCopyBuffer(writer io.Writer, reader io.ReaderAt, buf []byte) (md5Sum, sha256Sum []byte, size int64, err error) {
	// MD5 and SHA256 hasher.
	var hashMD5, hashSHA256 hash.Hash
	// MD5 and SHA256 hasher.
	hashMD5 = md5.New()
	hashWriter := io.MultiWriter(writer, hashMD5)
	if c.signature.isV4() {
		hashSHA256 = sha256.New()
		hashWriter = io.MultiWriter(writer, hashMD5, hashSHA256)
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
				return nil, nil, 0, rerr
			}
		}
		writeSize, werr := hashWriter.Write(buf[:readAtSize])
		if werr != nil {
			return nil, nil, 0, werr
		}
		if readAtSize != writeSize {
			return nil, nil, 0, fmt.Errorf("Read size was not completely written to writer. wanted %d, got %d - %s", readAtSize, writeSize, reportIssue)
		}
		readAtOffset += int64(writeSize)
		size += int64(writeSize)
		if rerr == io.EOF {
			break
		}
	}

	// Finalize md5 sum and sha256 sum.
	md5Sum = hashMD5.Sum(nil)
	if c.signature.isV4() {
		sha256Sum = hashSHA256.Sum(nil)
	}
	return md5Sum, sha256Sum, size, err
}

// hashCopy is identical to hashCopyN except that it doesn't take
// any size argument.
func (c Client) hashCopy(writer io.Writer, reader io.Reader) (md5Sum, sha256Sum []byte, size int64, err error) {
	// MD5 and SHA256 hasher.
	var hashMD5, hashSHA256 hash.Hash
	// MD5 and SHA256 hasher.
	hashMD5 = md5.New()
	hashWriter := io.MultiWriter(writer, hashMD5)
	if c.signature.isV4() {
		hashSHA256 = sha256.New()
		hashWriter = io.MultiWriter(writer, hashMD5, hashSHA256)
	}

	// Using copyBuffer to copy in large buffers, default buffer
	// for io.Copy of 32KiB is too small.
	size, err = io.Copy(hashWriter, reader)
	if err != nil {
		return nil, nil, 0, err
	}

	// Finalize md5 sum and sha256 sum.
	md5Sum = hashMD5.Sum(nil)
	if c.signature.isV4() {
		sha256Sum = hashSHA256.Sum(nil)
	}
	return md5Sum, sha256Sum, size, err
}

// hashCopyN - Calculates Md5sum and SHA256sum for up to partSize amount of bytes.
func (c Client) hashCopyN(writer io.Writer, reader io.Reader, partSize int64) (md5Sum, sha256Sum []byte, size int64, err error) {
	// MD5 and SHA256 hasher.
	var hashMD5, hashSHA256 hash.Hash
	// MD5 and SHA256 hasher.
	hashMD5 = md5.New()
	hashWriter := io.MultiWriter(writer, hashMD5)
	if c.signature.isV4() {
		hashSHA256 = sha256.New()
		hashWriter = io.MultiWriter(writer, hashMD5, hashSHA256)
	}

	// Copies to input at writer.
	size, err = io.CopyN(hashWriter, reader, partSize)
	if err != nil {
		// If not EOF return error right here.
		if err != io.EOF {
			return nil, nil, 0, err
		}
	}

	// Finalize md5shum and sha256 sum.
	md5Sum = hashMD5.Sum(nil)
	if c.signature.isV4() {
		sha256Sum = hashSHA256.Sum(nil)
	}
	return md5Sum, sha256Sum, size, err
}

// getUploadID - fetch upload id if already present for an object name
// or initiate a new request to fetch a new upload id.
func (c Client) getUploadID(bucketName, objectName, contentType string) (uploadID string, isNew bool, err error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return "", false, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return "", false, err
	}

	// Set content Type to default if empty string.
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Find upload id for previous upload for an object.
	uploadID, err = c.findUploadID(bucketName, objectName)
	if err != nil {
		return "", false, err
	}
	if uploadID == "" {
		// Initiate multipart upload for an object.
		initMultipartUploadResult, err := c.initiateMultipartUpload(bucketName, objectName, contentType)
		if err != nil {
			return "", false, err
		}
		// Save the new upload id.
		uploadID = initMultipartUploadResult.UploadID
		// Indicate that this is a new upload id.
		isNew = true
	}
	return uploadID, isNew, nil
}

// computeHash - Calculates MD5 and SHA256 for an input read Seeker.
func (c Client) computeHash(reader io.ReadSeeker) (md5Sum, sha256Sum []byte, size int64, err error) {
	// MD5 and SHA256 hasher.
	var hashMD5, hashSHA256 hash.Hash
	// MD5 and SHA256 hasher.
	hashMD5 = md5.New()
	hashWriter := io.MultiWriter(hashMD5)
	if c.signature.isV4() {
		hashSHA256 = sha256.New()
		hashWriter = io.MultiWriter(hashMD5, hashSHA256)
	}

	// If no buffer is provided, no need to allocate just use io.Copy.
	size, err = io.Copy(hashWriter, reader)
	if err != nil {
		return nil, nil, 0, err
	}

	// Seek back reader to the beginning location.
	if _, err := reader.Seek(0, 0); err != nil {
		return nil, nil, 0, err
	}

	// Finalize md5shum and sha256 sum.
	md5Sum = hashMD5.Sum(nil)
	if c.signature.isV4() {
		sha256Sum = hashSHA256.Sum(nil)
	}
	return md5Sum, sha256Sum, size, nil
}
