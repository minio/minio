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
	"encoding/hex"
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Comprehensive put object operation involving multipart resumable uploads.
//
// Following code handles these types of readers.
//
//  - *os.File
//  - *minio.Object
//  - Any reader which has a method 'ReadAt()'
//
// If we exhaust all the known types, code proceeds to use stream as
// is where each part is re-downloaded, checksummed and verified
// before upload.
func (c Client) putObjectMultipart(bucketName, objectName string, reader io.Reader, size int64, contentType string, progress io.Reader) (n int64, err error) {
	if size > 0 && size >= minPartSize {
		// Verify if reader is *os.File, then use file system functionalities.
		if isFile(reader) {
			return c.putObjectMultipartFromFile(bucketName, objectName, reader.(*os.File), size, contentType, progress)
		}
		// Verify if reader is *minio.Object or io.ReaderAt.
		// NOTE: Verification of object is kept for a specific purpose
		// while it is going to be duck typed similar to io.ReaderAt.
		// It is to indicate that *minio.Object implements io.ReaderAt.
		// and such a functionality is used in the subsequent code
		// path.
		if isObject(reader) || isReadAt(reader) {
			return c.putObjectMultipartFromReadAt(bucketName, objectName, reader.(io.ReaderAt), size, contentType, progress)
		}
	}
	// For any other data size and reader type we do generic multipart
	// approach by staging data in temporary files and uploading them.
	return c.putObjectMultipartStream(bucketName, objectName, reader, size, contentType, progress)
}

// putObjectStream uploads files bigger than 5MiB, and also supports
// special case where size is unknown i.e '-1'.
func (c Client) putObjectMultipartStream(bucketName, objectName string, reader io.Reader, size int64, contentType string, progress io.Reader) (n int64, err error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return 0, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return 0, err
	}

	// Total data read and written to server. should be equal to 'size' at the end of the call.
	var totalUploadedSize int64

	// Complete multipart upload.
	var complMultipartUpload completeMultipartUpload

	// A map of all previously uploaded parts.
	var partsInfo = make(map[int]objectPart)

	// getUploadID for an object, initiates a new multipart request
	// if it cannot find any previously partially uploaded object.
	uploadID, isNew, err := c.getUploadID(bucketName, objectName, contentType)
	if err != nil {
		return 0, err
	}

	// If This session is a continuation of a previous session fetch all
	// previously uploaded parts info.
	if !isNew {
		// Fetch previously uploaded parts and maximum part size.
		partsInfo, err = c.listObjectParts(bucketName, objectName, uploadID)
		if err != nil {
			return 0, err
		}
	}

	// Calculate the optimal parts info for a given size.
	totalPartsCount, partSize, _, err := optimalPartInfo(size)
	if err != nil {
		return 0, err
	}

	// Part number always starts with '1'.
	partNumber := 1

	// Initialize a temporary buffer.
	tmpBuffer := new(bytes.Buffer)

	for partNumber <= totalPartsCount {
		// Calculates MD5 and SHA256 sum while copying partSize bytes
		// into tmpBuffer.
		md5Sum, sha256Sum, prtSize, rErr := c.hashCopyN(tmpBuffer, reader, partSize)
		if rErr != nil {
			if rErr != io.EOF {
				return 0, rErr
			}
		}

		var reader io.Reader
		// Update progress reader appropriately to the latest offset
		// as we read from the source.
		reader = newHook(tmpBuffer, progress)

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
				// Reset the temporary buffer upon any error.
				tmpBuffer.Reset()
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

		// Reset the temporary buffer.
		tmpBuffer.Reset()

		// Save successfully uploaded size.
		totalUploadedSize += prtSize

		// For unknown size, Read EOF we break away.
		// We do not have to upload till totalPartsCount.
		if size < 0 && rErr == io.EOF {
			break
		}

		// Increment part number.
		partNumber++
	}

	// Verify if we uploaded all the data.
	if size > 0 {
		if totalUploadedSize != size {
			return totalUploadedSize, ErrUnexpectedEOF(totalUploadedSize, size, bucketName, objectName)
		}
	}

	// Loop over uploaded parts to save them in a Parts array before completing the multipart request.
	for _, part := range partsInfo {
		var complPart completePart
		complPart.ETag = part.ETag
		complPart.PartNumber = part.PartNumber
		complMultipartUpload.Parts = append(complMultipartUpload.Parts, complPart)
	}

	if size > 0 {
		// Verify if totalPartsCount is not equal to total list of parts.
		if totalPartsCount != len(complMultipartUpload.Parts) {
			return totalUploadedSize, ErrInvalidParts(partNumber, len(complMultipartUpload.Parts))
		}
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

// initiateMultipartUpload - Initiates a multipart upload and returns an upload ID.
func (c Client) initiateMultipartUpload(bucketName, objectName, contentType string) (initiateMultipartUploadResult, error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return initiateMultipartUploadResult{}, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return initiateMultipartUploadResult{}, err
	}

	// Initialize url queries.
	urlValues := make(url.Values)
	urlValues.Set("uploads", "")

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Set ContentType header.
	customHeader := make(http.Header)
	customHeader.Set("Content-Type", contentType)

	reqMetadata := requestMetadata{
		bucketName:   bucketName,
		objectName:   objectName,
		queryValues:  urlValues,
		customHeader: customHeader,
	}

	// Instantiate the request.
	req, err := c.newRequest("POST", reqMetadata)
	if err != nil {
		return initiateMultipartUploadResult{}, err
	}

	// Execute the request.
	resp, err := c.do(req)
	defer closeResponse(resp)
	if err != nil {
		return initiateMultipartUploadResult{}, err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			return initiateMultipartUploadResult{}, httpRespToErrorResponse(resp, bucketName, objectName)
		}
	}
	// Decode xml for new multipart upload.
	initiateMultipartUploadResult := initiateMultipartUploadResult{}
	err = xmlDecoder(resp.Body, &initiateMultipartUploadResult)
	if err != nil {
		return initiateMultipartUploadResult, err
	}
	return initiateMultipartUploadResult, nil
}

// uploadPart - Uploads a part in a multipart upload.
func (c Client) uploadPart(bucketName, objectName, uploadID string, reader io.ReadCloser, partNumber int, md5Sum, sha256Sum []byte, size int64) (objectPart, error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return objectPart{}, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return objectPart{}, err
	}
	if size > maxPartSize {
		return objectPart{}, ErrEntityTooLarge(size, maxPartSize, bucketName, objectName)
	}
	if size <= -1 {
		return objectPart{}, ErrEntityTooSmall(size, bucketName, objectName)
	}
	if partNumber <= 0 {
		return objectPart{}, ErrInvalidArgument("Part number cannot be negative or equal to zero.")
	}
	if uploadID == "" {
		return objectPart{}, ErrInvalidArgument("UploadID cannot be empty.")
	}

	// Get resources properly escaped and lined up before using them in http request.
	urlValues := make(url.Values)
	// Set part number.
	urlValues.Set("partNumber", strconv.Itoa(partNumber))
	// Set upload id.
	urlValues.Set("uploadId", uploadID)

	reqMetadata := requestMetadata{
		bucketName:         bucketName,
		objectName:         objectName,
		queryValues:        urlValues,
		contentBody:        reader,
		contentLength:      size,
		contentMD5Bytes:    md5Sum,
		contentSHA256Bytes: sha256Sum,
	}

	// Instantiate a request.
	req, err := c.newRequest("PUT", reqMetadata)
	if err != nil {
		return objectPart{}, err
	}
	// Execute the request.
	resp, err := c.do(req)
	defer closeResponse(resp)
	if err != nil {
		return objectPart{}, err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			return objectPart{}, httpRespToErrorResponse(resp, bucketName, objectName)
		}
	}
	// Once successfully uploaded, return completed part.
	objPart := objectPart{}
	objPart.Size = size
	objPart.PartNumber = partNumber
	// Trim off the odd double quotes from ETag in the beginning and end.
	objPart.ETag = strings.TrimPrefix(resp.Header.Get("ETag"), "\"")
	objPart.ETag = strings.TrimSuffix(objPart.ETag, "\"")
	return objPart, nil
}

// completeMultipartUpload - Completes a multipart upload by assembling previously uploaded parts.
func (c Client) completeMultipartUpload(bucketName, objectName, uploadID string, complete completeMultipartUpload) (completeMultipartUploadResult, error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return completeMultipartUploadResult{}, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return completeMultipartUploadResult{}, err
	}

	// Initialize url queries.
	urlValues := make(url.Values)
	urlValues.Set("uploadId", uploadID)

	// Marshal complete multipart body.
	completeMultipartUploadBytes, err := xml.Marshal(complete)
	if err != nil {
		return completeMultipartUploadResult{}, err
	}

	// Instantiate all the complete multipart buffer.
	completeMultipartUploadBuffer := bytes.NewBuffer(completeMultipartUploadBytes)
	reqMetadata := requestMetadata{
		bucketName:         bucketName,
		objectName:         objectName,
		queryValues:        urlValues,
		contentBody:        ioutil.NopCloser(completeMultipartUploadBuffer),
		contentLength:      int64(completeMultipartUploadBuffer.Len()),
		contentSHA256Bytes: sum256(completeMultipartUploadBuffer.Bytes()),
	}

	// Instantiate the request.
	req, err := c.newRequest("POST", reqMetadata)
	if err != nil {
		return completeMultipartUploadResult{}, err
	}

	// Execute the request.
	resp, err := c.do(req)
	defer closeResponse(resp)
	if err != nil {
		return completeMultipartUploadResult{}, err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			return completeMultipartUploadResult{}, httpRespToErrorResponse(resp, bucketName, objectName)
		}
	}
	// Decode completed multipart upload response on success.
	completeMultipartUploadResult := completeMultipartUploadResult{}
	err = xmlDecoder(resp.Body, &completeMultipartUploadResult)
	if err != nil {
		return completeMultipartUploadResult, err
	}
	return completeMultipartUploadResult, nil
}
