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
	"bytes"
	"io"
	"io/ioutil"
	"sort"
)

// shouldUploadPartReadAt - verify if part should be uploaded.
func shouldUploadPartReadAt(objPart objectPart, objectParts map[int]objectPart) bool {
	// If part not found part should be uploaded.
	uploadedPart, found := objectParts[objPart.PartNumber]
	if !found {
		return true
	}
	// if size mismatches part should be uploaded.
	if uploadedPart.Size != objPart.Size {
		return true
	}
	return false
}

// putObjectMultipartFromReadAt - Uploads files bigger than 5MiB. Supports reader
// of type which implements io.ReaderAt interface (ReadAt method).
//
// NOTE: This function is meant to be used for all readers which
// implement io.ReaderAt which allows us for resuming multipart
// uploads but reading at an offset, which would avoid re-read the
// data which was already uploaded. Internally this function uses
// temporary files for staging all the data, these temporary files are
// cleaned automatically when the caller i.e http client closes the
// stream after uploading all the contents successfully.
func (c Client) putObjectMultipartFromReadAt(bucketName, objectName string, reader io.ReaderAt, size int64, contentType string, progress io.Reader) (n int64, err error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return 0, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return 0, err
	}

	// Get upload id for an object, initiates a new multipart request
	// if it cannot find any previously partially uploaded object.
	uploadID, isNew, err := c.getUploadID(bucketName, objectName, contentType)
	if err != nil {
		return 0, err
	}

	// Total data read and written to server. should be equal to 'size' at the end of the call.
	var totalUploadedSize int64

	// Complete multipart upload.
	var complMultipartUpload completeMultipartUpload

	// A map of all uploaded parts.
	var partsInfo = make(map[int]objectPart)

	// Fetch all parts info previously uploaded.
	if !isNew {
		partsInfo, err = c.listObjectParts(bucketName, objectName, uploadID)
		if err != nil {
			return 0, err
		}
	}

	// Calculate the optimal parts info for a given size.
	totalPartsCount, partSize, lastPartSize, err := optimalPartInfo(size)
	if err != nil {
		return 0, err
	}

	// Used for readability, lastPartNumber is always
	// totalPartsCount.
	lastPartNumber := totalPartsCount

	// partNumber always starts with '1'.
	partNumber := 1

	// Initialize a temporary buffer.
	tmpBuffer := new(bytes.Buffer)

	// Read defaults to reading at 5MiB buffer.
	readBuffer := make([]byte, optimalReadBufferSize)

	// Upload all the missing parts.
	for partNumber <= lastPartNumber {
		// Verify object if its uploaded.
		verifyObjPart := objectPart{
			PartNumber: partNumber,
			Size:       partSize,
		}
		// Special case if we see a last part number, save last part
		// size as the proper part size.
		if partNumber == lastPartNumber {
			verifyObjPart = objectPart{
				PartNumber: lastPartNumber,
				Size:       lastPartSize,
			}
		}

		// Verify if part should be uploaded.
		if !shouldUploadPartReadAt(verifyObjPart, partsInfo) {
			// Increment part number when not uploaded.
			partNumber++
			if progress != nil {
				// Update the progress reader for the skipped part.
				if _, err = io.CopyN(ioutil.Discard, progress, verifyObjPart.Size); err != nil {
					return 0, err
				}
			}
			continue
		}

		// If partNumber was not uploaded we calculate the missing
		// part offset and size. For all other part numbers we
		// calculate offset based on multiples of partSize.
		readOffset := int64(partNumber-1) * partSize
		missingPartSize := partSize

		// As a special case if partNumber is lastPartNumber, we
		// calculate the offset based on the last part size.
		if partNumber == lastPartNumber {
			readOffset = (size - lastPartSize)
			missingPartSize = lastPartSize
		}

		// Get a section reader on a particular offset.
		sectionReader := io.NewSectionReader(reader, readOffset, missingPartSize)

		// Calculates MD5 and SHA256 sum for a section reader.
		var md5Sum, sha256Sum []byte
		var prtSize int64
		md5Sum, sha256Sum, prtSize, err = c.hashCopyBuffer(tmpBuffer, sectionReader, readBuffer)
		if err != nil {
			return 0, err
		}

		var reader io.Reader
		// Update progress reader appropriately to the latest offset
		// as we read from the source.
		reader = newHook(tmpBuffer, progress)

		// Proceed to upload the part.
		var objPart objectPart
		objPart, err = c.uploadPart(bucketName, objectName, uploadID, ioutil.NopCloser(reader),
			partNumber, md5Sum, sha256Sum, prtSize)
		if err != nil {
			// Reset the buffer upon any error.
			tmpBuffer.Reset()
			return 0, err
		}

		// Save successfully uploaded part metadata.
		partsInfo[partNumber] = objPart

		// Increment part number here after successful part upload.
		partNumber++

		// Reset the buffer.
		tmpBuffer.Reset()
	}

	// Loop over uploaded parts to save them in a Parts array before completing the multipart request.
	for _, part := range partsInfo {
		var complPart completePart
		complPart.ETag = part.ETag
		complPart.PartNumber = part.PartNumber
		totalUploadedSize += part.Size
		complMultipartUpload.Parts = append(complMultipartUpload.Parts, complPart)
	}

	// Verify if we uploaded all the data.
	if totalUploadedSize != size {
		return totalUploadedSize, ErrUnexpectedEOF(totalUploadedSize, size, bucketName, objectName)
	}

	// Verify if totalPartsCount is not equal to total list of parts.
	if totalPartsCount != len(complMultipartUpload.Parts) {
		return totalUploadedSize, ErrInvalidParts(totalPartsCount, len(complMultipartUpload.Parts))
	}

	// Sort all completed parts.
	sort.Sort(completedParts(complMultipartUpload.Parts))
	_, err = c.completeMultipartUpload(bucketName, objectName, uploadID, complMultipartUpload)
	if err != nil {
		return totalUploadedSize, err
	}

	// Return final size.
	return totalUploadedSize, nil
}
