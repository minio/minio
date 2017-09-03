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
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"

	"github.com/minio/minio-go/pkg/s3utils"
)

func (c Client) putObjectMultipart(bucketName, objectName string, reader io.Reader, size int64,
	metadata map[string][]string, progress io.Reader) (n int64, err error) {
	n, err = c.putObjectMultipartNoStream(bucketName, objectName, reader, metadata, progress)
	if err != nil {
		errResp := ToErrorResponse(err)
		// Verify if multipart functionality is not available, if not
		// fall back to single PutObject operation.
		if errResp.Code == "AccessDenied" && strings.Contains(errResp.Message, "Access Denied") {
			// Verify if size of reader is greater than '5GiB'.
			if size > maxSinglePutObjectSize {
				return 0, ErrEntityTooLarge(size, maxSinglePutObjectSize, bucketName, objectName)
			}
			// Fall back to uploading as single PutObject operation.
			return c.putObjectNoChecksum(bucketName, objectName, reader, size, metadata, progress)
		}
	}
	return n, err
}

func (c Client) putObjectMultipartNoStream(bucketName, objectName string, reader io.Reader, metadata map[string][]string, progress io.Reader) (n int64, err error) {
	// Input validation.
	if err = s3utils.CheckValidBucketName(bucketName); err != nil {
		return 0, err
	}
	if err = s3utils.CheckValidObjectName(objectName); err != nil {
		return 0, err
	}

	// Total data read and written to server. should be equal to
	// 'size' at the end of the call.
	var totalUploadedSize int64

	// Complete multipart upload.
	var complMultipartUpload completeMultipartUpload

	// Calculate the optimal parts info for a given size.
	totalPartsCount, partSize, _, err := optimalPartInfo(-1)
	if err != nil {
		return 0, err
	}

	// Initiate a new multipart upload.
	uploadID, err := c.newUploadID(bucketName, objectName, metadata)
	if err != nil {
		return 0, err
	}

	defer func() {
		if err != nil {
			c.abortMultipartUpload(bucketName, objectName, uploadID)
		}
	}()

	// Part number always starts with '1'.
	partNumber := 1

	// Initialize parts uploaded map.
	partsInfo := make(map[int]ObjectPart)

	// Create a buffer.
	buf := make([]byte, partSize)
	defer debug.FreeOSMemory()

	for partNumber <= totalPartsCount {
		// Choose hash algorithms to be calculated by hashCopyN,
		// avoid sha256 with non-v4 signature request or
		// HTTPS connection.
		hashAlgos, hashSums := c.hashMaterials()

		length, rErr := io.ReadFull(reader, buf)
		if rErr == io.EOF {
			break
		}
		if rErr != nil && rErr != io.ErrUnexpectedEOF {
			return 0, rErr
		}

		// Calculates hash sums while copying partSize bytes into cw.
		for k, v := range hashAlgos {
			v.Write(buf[:length])
			hashSums[k] = v.Sum(nil)
		}

		// Update progress reader appropriately to the latest offset
		// as we read from the source.
		rd := newHook(bytes.NewReader(buf[:length]), progress)

		// Proceed to upload the part.
		var objPart ObjectPart
		objPart, err = c.uploadPart(bucketName, objectName, uploadID, rd, partNumber,
			hashSums["md5"], hashSums["sha256"], int64(length), metadata)
		if err != nil {
			return totalUploadedSize, err
		}

		// Save successfully uploaded part metadata.
		partsInfo[partNumber] = objPart

		// Save successfully uploaded size.
		totalUploadedSize += int64(length)

		// Increment part number.
		partNumber++

		// For unknown size, Read EOF we break away.
		// We do not have to upload till totalPartsCount.
		if rErr == io.EOF {
			break
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
	if _, err = c.completeMultipartUpload(bucketName, objectName, uploadID, complMultipartUpload); err != nil {
		return totalUploadedSize, err
	}

	// Return final size.
	return totalUploadedSize, nil
}

// initiateMultipartUpload - Initiates a multipart upload and returns an upload ID.
func (c Client) initiateMultipartUpload(bucketName, objectName string, metadata map[string][]string) (initiateMultipartUploadResult, error) {
	// Input validation.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return initiateMultipartUploadResult{}, err
	}
	if err := s3utils.CheckValidObjectName(objectName); err != nil {
		return initiateMultipartUploadResult{}, err
	}

	// Initialize url queries.
	urlValues := make(url.Values)
	urlValues.Set("uploads", "")

	// Set ContentType header.
	customHeader := make(http.Header)
	for k, v := range metadata {
		if len(v) > 0 {
			customHeader.Set(k, v[0])
		}
	}

	// Set a default content-type header if the latter is not provided
	if v, ok := metadata["Content-Type"]; !ok || len(v) == 0 {
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

const serverEncryptionKeyPrefix = "x-amz-server-side-encryption"

// uploadPart - Uploads a part in a multipart upload.
func (c Client) uploadPart(bucketName, objectName, uploadID string, reader io.Reader,
	partNumber int, md5Sum, sha256Sum []byte, size int64, metadata map[string][]string) (ObjectPart, error) {
	// Input validation.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return ObjectPart{}, err
	}
	if err := s3utils.CheckValidObjectName(objectName); err != nil {
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

	// Set encryption headers, if any.
	customHeader := make(http.Header)
	for k, v := range metadata {
		if len(v) > 0 {
			if strings.HasPrefix(strings.ToLower(k), serverEncryptionKeyPrefix) {
				customHeader.Set(k, v[0])
			}
		}
	}

	reqMetadata := requestMetadata{
		bucketName:         bucketName,
		objectName:         objectName,
		queryValues:        urlValues,
		customHeader:       customHeader,
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
func (c Client) completeMultipartUpload(bucketName, objectName, uploadID string,
	complete completeMultipartUpload) (completeMultipartUploadResult, error) {
	// Input validation.
	if err := s3utils.CheckValidBucketName(bucketName); err != nil {
		return completeMultipartUploadResult{}, err
	}
	if err := s3utils.CheckValidObjectName(objectName); err != nil {
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
