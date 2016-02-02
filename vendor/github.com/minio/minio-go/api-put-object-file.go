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
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
)

// FPutObject - Create an object in a bucket, with contents from file at filePath.
func (c Client) FPutObject(bucketName, objectName, filePath, contentType string) (n int64, err error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return 0, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return 0, err
	}

	// Open the referenced file.
	fileReader, err := os.Open(filePath)
	// If any error fail quickly here.
	if err != nil {
		return 0, err
	}
	defer fileReader.Close()

	// Save the file stat.
	fileStat, err := fileReader.Stat()
	if err != nil {
		return 0, err
	}

	// Save the file size.
	fileSize := fileStat.Size()

	// Check for largest object size allowed.
	if fileSize > int64(maxMultipartPutObjectSize) {
		return 0, ErrEntityTooLarge(fileSize, maxMultipartPutObjectSize, bucketName, objectName)
	}

	// NOTE: Google Cloud Storage multipart Put is not compatible with Amazon S3 APIs.
	// Current implementation will only upload a maximum of 5GiB to Google Cloud Storage servers.
	if isGoogleEndpoint(c.endpointURL) {
		if fileSize > int64(maxSinglePutObjectSize) {
			return 0, ErrorResponse{
				Code:       "NotImplemented",
				Message:    fmt.Sprintf("Invalid Content-Length %d for file uploads to Google Cloud Storage.", fileSize),
				Key:        objectName,
				BucketName: bucketName,
			}
		}
		// Do not compute MD5 for Google Cloud Storage. Uploads up to 5GiB in size.
		return c.putObjectNoChecksum(bucketName, objectName, fileReader, fileSize, contentType, nil)
	}

	// NOTE: S3 doesn't allow anonymous multipart requests.
	if isAmazonEndpoint(c.endpointURL) && c.anonymous {
		if fileSize > int64(maxSinglePutObjectSize) {
			return 0, ErrorResponse{
				Code:       "NotImplemented",
				Message:    fmt.Sprintf("For anonymous requests Content-Length cannot be %d.", fileSize),
				Key:        objectName,
				BucketName: bucketName,
			}
		}
		// Do not compute MD5 for anonymous requests to Amazon
		// S3. Uploads up to 5GiB in size.
		return c.putObjectNoChecksum(bucketName, objectName, fileReader, fileSize, contentType, nil)
	}

	// Small object upload is initiated for uploads for input data size smaller than 5MiB.
	if fileSize < minPartSize && fileSize >= 0 {
		return c.putObjectSingle(bucketName, objectName, fileReader, fileSize, contentType, nil)
	}
	// Upload all large objects as multipart.
	n, err = c.putObjectMultipartFromFile(bucketName, objectName, fileReader, fileSize, contentType, nil)
	if err != nil {
		errResp := ToErrorResponse(err)
		// Verify if multipart functionality is not available, if not
		// fall back to single PutObject operation.
		if errResp.Code == "NotImplemented" {
			// If size of file is greater than '5GiB' fail.
			if fileSize > maxSinglePutObjectSize {
				return 0, ErrEntityTooLarge(fileSize, maxSinglePutObjectSize, bucketName, objectName)
			}
			// Fall back to uploading as single PutObject operation.
			return c.putObjectSingle(bucketName, objectName, fileReader, fileSize, contentType, nil)
		}
		return n, err
	}
	return n, nil
}

// putObjectMultipartFromFile - Creates object from contents of *os.File
//
// NOTE: This function is meant to be used for readers with local
// file as in *os.File. This function resumes by skipping all the
// necessary parts which were already uploaded by verifying them
// against MD5SUM of each individual parts. This function also
// effectively utilizes file system capabilities of reading from
// specific sections and not having to create temporary files.
func (c Client) putObjectMultipartFromFile(bucketName, objectName string, fileReader io.ReaderAt, fileSize int64, contentType string, progress io.Reader) (int64, error) {
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
	var completeMultipartUpload completeMultipartUpload

	// A map of all uploaded parts.
	var partsInfo = make(map[int]objectPart)

	// If this session is a continuation of a previous session fetch all
	// previously uploaded parts info.
	if !isNew {
		// Fetch previously upload parts and maximum part size.
		partsInfo, err = c.listObjectParts(bucketName, objectName, uploadID)
		if err != nil {
			return 0, err
		}
	}

	// Calculate the optimal parts info for a given size.
	totalPartsCount, partSize, _, err := optimalPartInfo(fileSize)
	if err != nil {
		return 0, err
	}

	// Part number always starts with '1'.
	partNumber := 1

	for partNumber <= totalPartsCount {
		// Get a section reader on a particular offset.
		sectionReader := io.NewSectionReader(fileReader, totalUploadedSize, partSize)

		// Calculates MD5 and SHA256 sum for a section reader.
		var md5Sum, sha256Sum []byte
		var prtSize int64
		md5Sum, sha256Sum, prtSize, err = c.computeHash(sectionReader)
		if err != nil {
			return 0, err
		}

		var reader io.Reader
		// Update progress reader appropriately to the latest offset
		// as we read from the source.
		reader = newHook(sectionReader, progress)

		// Verify if part should be uploaded.
		if shouldUploadPart(objectPart{
			ETag:       hex.EncodeToString(md5Sum),
			PartNumber: partNumber,
			Size:       prtSize,
		}, partsInfo) {
			// Proceed to upload the part.
			var objPart objectPart
			objPart, err = c.uploadPart(bucketName, objectName, uploadID, ioutil.NopCloser(reader), partNumber,
				md5Sum, sha256Sum, prtSize)
			if err != nil {
				return totalUploadedSize, err
			}
			// Save successfully uploaded part metadata.
			partsInfo[partNumber] = objPart
		} else {
			// Update the progress reader for the skipped part.
			if progress != nil {
				if _, err = io.CopyN(ioutil.Discard, progress, prtSize); err != nil {
					return totalUploadedSize, err
				}
			}
		}

		// Save successfully uploaded size.
		totalUploadedSize += prtSize

		// Increment part number.
		partNumber++
	}

	// Verify if we uploaded all data.
	if totalUploadedSize != fileSize {
		return totalUploadedSize, ErrUnexpectedEOF(totalUploadedSize, fileSize, bucketName, objectName)
	}

	// Loop over uploaded parts to save them in a Parts array before completing the multipart request.
	for _, part := range partsInfo {
		var complPart completePart
		complPart.ETag = part.ETag
		complPart.PartNumber = part.PartNumber
		completeMultipartUpload.Parts = append(completeMultipartUpload.Parts, complPart)
	}

	// Verify if totalPartsCount is not equal to total list of parts.
	if totalPartsCount != len(completeMultipartUpload.Parts) {
		return totalUploadedSize, ErrInvalidParts(partNumber, len(completeMultipartUpload.Parts))
	}

	// Sort all completed parts.
	sort.Sort(completedParts(completeMultipartUpload.Parts))
	_, err = c.completeMultipartUpload(bucketName, objectName, uploadID, completeMultipartUpload)
	if err != nil {
		return totalUploadedSize, err
	}

	// Return final size.
	return totalUploadedSize, nil
}
