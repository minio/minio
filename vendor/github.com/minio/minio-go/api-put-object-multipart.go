/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015, 2016 Minio, Inc.
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
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash"
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
func (c Client) putObjectMultipart(bucketName, objectName string, reader io.Reader, size int64, metaData map[string][]string, progress io.Reader) (n int64, err error) {
	if size > 0 && size > minPartSize {
		// Verify if reader is *os.File, then use file system functionalities.
		if isFile(reader) {
			return c.putObjectMultipartFromFile(bucketName, objectName, reader.(*os.File), size, metaData, progress)
		}
		// Verify if reader is *minio.Object or io.ReaderAt.
		// NOTE: Verification of object is kept for a specific purpose
		// while it is going to be duck typed similar to io.ReaderAt.
		// It is to indicate that *minio.Object implements io.ReaderAt.
		// and such a functionality is used in the subsequent code
		// path.
		if isObject(reader) || isReadAt(reader) {
			return c.putObjectMultipartFromReadAt(bucketName, objectName, reader.(io.ReaderAt), size, metaData, progress)
		}
	}
	// For any other data size and reader type we do generic multipart
	// approach by staging data in temporary files and uploading them.
	return c.putObjectMultipartStream(bucketName, objectName, reader, size, metaData, progress)
}

// putObjectMultipartStreamNoChecksum - upload a large object using
// multipart upload and streaming signature for signing payload.
// N B We don't resume an incomplete multipart upload, we overwrite
// existing parts of an incomplete upload.
func (c Client) putObjectMultipartStreamNoChecksum(bucketName, objectName string,
	reader io.Reader, size int64, metadata map[string][]string, progress io.Reader) (int64, error) {

	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return 0, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return 0, err
	}

	// Get the upload id of a previously partially uploaded object or initiate a new multipart upload
	uploadID, err := c.findUploadID(bucketName, objectName)
	if err != nil {
		return 0, err
	}
	if uploadID == "" {
		// Initiates a new multipart request
		uploadID, err = c.newUploadID(bucketName, objectName, metadata)
		if err != nil {
			return 0, err
		}
	}

	// Calculate the optimal parts info for a given size.
	totalPartsCount, partSize, lastPartSize, err := optimalPartInfo(size)
	if err != nil {
		return 0, err
	}

	// Total data read and written to server. should be equal to 'size' at the end of the call.
	var totalUploadedSize int64

	// Initialize parts uploaded map.
	partsInfo := make(map[int]ObjectPart)

	// Part number always starts with '1'.
	var partNumber int
	for partNumber = 1; partNumber <= totalPartsCount; partNumber++ {
		// Update progress reader appropriately to the latest offset
		// as we read from the source.
		hookReader := newHook(reader, progress)

		// Proceed to upload the part.
		if partNumber == totalPartsCount {
			partSize = lastPartSize
		}

		var objPart ObjectPart
		objPart, err = c.uploadPart(bucketName, objectName, uploadID,
			io.LimitReader(hookReader, partSize), partNumber, nil, nil, partSize)
		// For unknown size, Read EOF we break away.
		// We do not have to upload till totalPartsCount.
		if err == io.EOF && size < 0 {
			break
		}

		if err != nil {
			return totalUploadedSize, err
		}

		// Save successfully uploaded part metadata.
		partsInfo[partNumber] = objPart

		// Save successfully uploaded size.
		totalUploadedSize += partSize
	}

	// Verify if we uploaded all the data.
	if size > 0 {
		if totalUploadedSize != size {
			return totalUploadedSize, ErrUnexpectedEOF(totalUploadedSize, size, bucketName, objectName)
		}
	}

	// Complete multipart upload.
	var complMultipartUpload completeMultipartUpload

	// Loop over total uploaded parts to save them in
	// Parts array before completing the multipart request.
	for i := 1; i < partNumber; i++ {
		part, ok := partsInfo[i]
		if !ok {
			return 0, ErrInvalidArgument(fmt.Sprintf("Missing part number %d", i))
		}
		complMultipartUpload.Parts = append(complMultipartUpload.Parts, CompletePart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
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

// putObjectStream uploads files bigger than 64MiB, and also supports
// special case where size is unknown i.e '-1'.
func (c Client) putObjectMultipartStream(bucketName, objectName string, reader io.Reader, size int64, metaData map[string][]string, progress io.Reader) (n int64, err error) {
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

	// Get the upload id of a previously partially uploaded object or initiate a new multipart upload
	uploadID, partsInfo, err := c.getMpartUploadSession(bucketName, objectName, metaData)
	if err != nil {
		return 0, err
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
		// Choose hash algorithms to be calculated by hashCopyN, avoid sha256
		// with non-v4 signature request or HTTPS connection
		hashSums := make(map[string][]byte)
		hashAlgos := make(map[string]hash.Hash)
		hashAlgos["md5"] = md5.New()
		if c.overrideSignerType.IsV4() && !c.secure {
			hashAlgos["sha256"] = sha256.New()
		}

		// Calculates hash sums while copying partSize bytes into tmpBuffer.
		prtSize, rErr := hashCopyN(hashAlgos, hashSums, tmpBuffer, reader, partSize)
		if rErr != nil && rErr != io.EOF {
			return 0, rErr
		}

		var reader io.Reader
		// Update progress reader appropriately to the latest offset
		// as we read from the source.
		reader = newHook(tmpBuffer, progress)

		part, ok := partsInfo[partNumber]

		// Verify if part should be uploaded.
		if !ok || shouldUploadPart(ObjectPart{
			ETag:       hex.EncodeToString(hashSums["md5"]),
			PartNumber: partNumber,
			Size:       prtSize,
		}, uploadPartReq{PartNum: partNumber, Part: &part}) {
			// Proceed to upload the part.
			var objPart ObjectPart
			objPart, err = c.uploadPart(bucketName, objectName, uploadID, reader, partNumber, hashSums["md5"], hashSums["sha256"], prtSize)
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

		// Increment part number.
		partNumber++

		// For unknown size, Read EOF we break away.
		// We do not have to upload till totalPartsCount.
		if size < 0 && rErr == io.EOF {
			break
		}
	}

	// Verify if we uploaded all the data.
	if size > 0 {
		if totalUploadedSize != size {
			return totalUploadedSize, ErrUnexpectedEOF(totalUploadedSize, size, bucketName, objectName)
		}
	}

	// Loop over total uploaded parts to save them in
	// Parts array before completing the multipart request.
	for i := 1; i < partNumber; i++ {
		part, ok := partsInfo[i]
		if !ok {
			return 0, ErrInvalidArgument(fmt.Sprintf("Missing part number %d", i))
		}
		complMultipartUpload.Parts = append(complMultipartUpload.Parts, CompletePart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
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
func (c Client) initiateMultipartUpload(bucketName, objectName string, metaData map[string][]string) (initiateMultipartUploadResult, error) {
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

	// Set ContentType header.
	customHeader := make(http.Header)
	for k, v := range metaData {
		if len(v) > 0 {
			customHeader.Set(k, v[0])
		}
	}

	// Set a default content-type header if the latter is not provided
	if v, ok := metaData["Content-Type"]; !ok || len(v) == 0 {
		customHeader.Set("Content-Type", "application/octet-stream")
	}

	reqMetadata := requestMetadata{
		bucketName:   bucketName,
		objectName:   objectName,
		queryValues:  urlValues,
		customHeader: customHeader,
	}

	// Execute POST on an objectName to initiate multipart upload.
	resp, err := c.executeMethod("POST", reqMetadata)
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
func (c Client) uploadPart(bucketName, objectName, uploadID string, reader io.Reader, partNumber int, md5Sum, sha256Sum []byte, size int64) (ObjectPart, error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return ObjectPart{}, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return ObjectPart{}, err
	}
	if size > maxPartSize {
		return ObjectPart{}, ErrEntityTooLarge(size, maxPartSize, bucketName, objectName)
	}
	if size <= -1 {
		return ObjectPart{}, ErrEntityTooSmall(size, bucketName, objectName)
	}
	if partNumber <= 0 {
		return ObjectPart{}, ErrInvalidArgument("Part number cannot be negative or equal to zero.")
	}
	if uploadID == "" {
		return ObjectPart{}, ErrInvalidArgument("UploadID cannot be empty.")
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

	// Execute PUT on each part.
	resp, err := c.executeMethod("PUT", reqMetadata)
	defer closeResponse(resp)
	if err != nil {
		return ObjectPart{}, err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			return ObjectPart{}, httpRespToErrorResponse(resp, bucketName, objectName)
		}
	}
	// Once successfully uploaded, return completed part.
	objPart := ObjectPart{}
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
	completeMultipartUploadBuffer := bytes.NewReader(completeMultipartUploadBytes)
	reqMetadata := requestMetadata{
		bucketName:         bucketName,
		objectName:         objectName,
		queryValues:        urlValues,
		contentBody:        completeMultipartUploadBuffer,
		contentLength:      int64(len(completeMultipartUploadBytes)),
		contentSHA256Bytes: sum256(completeMultipartUploadBytes),
	}

	// Execute POST to complete multipart upload for an objectName.
	resp, err := c.executeMethod("POST", reqMetadata)
	defer closeResponse(resp)
	if err != nil {
		return completeMultipartUploadResult{}, err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			return completeMultipartUploadResult{}, httpRespToErrorResponse(resp, bucketName, objectName)
		}
	}

	// Read resp.Body into a []bytes to parse for Error response inside the body
	var b []byte
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return completeMultipartUploadResult{}, err
	}
	// Decode completed multipart upload response on success.
	completeMultipartUploadResult := completeMultipartUploadResult{}
	err = xmlDecoder(bytes.NewReader(b), &completeMultipartUploadResult)
	if err != nil {
		// xml parsing failure due to presence an ill-formed xml fragment
		return completeMultipartUploadResult, err
	} else if completeMultipartUploadResult.Bucket == "" {
		// xml's Decode method ignores well-formed xml that don't apply to the type of value supplied.
		// In this case, it would leave completeMultipartUploadResult with the corresponding zero-values
		// of the members.

		// Decode completed multipart upload response on failure
		completeMultipartUploadErr := ErrorResponse{}
		err = xmlDecoder(bytes.NewReader(b), &completeMultipartUploadErr)
		if err != nil {
			// xml parsing failure due to presence an ill-formed xml fragment
			return completeMultipartUploadResult, err
		}
		return completeMultipartUploadResult, completeMultipartUploadErr
	}
	return completeMultipartUploadResult, nil
}
