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
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strings"
)

// getReaderSize - Determine the size of Reader if available.
func getReaderSize(reader io.Reader) (size int64, err error) {
	var result []reflect.Value
	size = -1
	if reader != nil {
		// Verify if there is a method by name 'Size'.
		lenFn := reflect.ValueOf(reader).MethodByName("Size")
		if lenFn.IsValid() {
			if lenFn.Kind() == reflect.Func {
				// Call the 'Size' function and save its return value.
				result = lenFn.Call([]reflect.Value{})
				if result != nil && len(result) == 1 {
					lenValue := result[0]
					if lenValue.IsValid() {
						switch lenValue.Kind() {
						case reflect.Int:
							fallthrough
						case reflect.Int8:
							fallthrough
						case reflect.Int16:
							fallthrough
						case reflect.Int32:
							fallthrough
						case reflect.Int64:
							size = lenValue.Int()
						}
					}
				}
			}
		} else {
			// Fallback to Stat() method, two possible Stat() structs
			// exist.
			switch v := reader.(type) {
			case *os.File:
				var st os.FileInfo
				st, err = v.Stat()
				if err != nil {
					// Handle this case specially for "windows",
					// certain files for example 'Stdin', 'Stdout' and
					// 'Stderr' it is not allowed to fetch file information.
					if runtime.GOOS == "windows" {
						if strings.Contains(err.Error(), "GetFileInformationByHandle") {
							return -1, nil
						}
					}
					return
				}
				// Ignore if input is a directory, throw an error.
				if st.Mode().IsDir() {
					return -1, ErrInvalidArgument("Input file cannot be a directory.")
				}
				// Ignore 'Stdin', 'Stdout' and 'Stderr', since they
				// represent *os.File type but internally do not
				// implement Seekable calls. Ignore them and treat
				// them like a stream with unknown length.
				switch st.Name() {
				case "stdin":
					fallthrough
				case "stdout":
					fallthrough
				case "stderr":
					return
				}
				size = st.Size()
			case *Object:
				var st ObjectInfo
				st, err = v.Stat()
				if err != nil {
					return
				}
				size = st.Size
			}
		}
	}
	// Returns the size here.
	return size, err
}

// completedParts is a collection of parts sortable by their part numbers.
// used for sorting the uploaded parts before completing the multipart request.
type completedParts []completePart

func (a completedParts) Len() int           { return len(a) }
func (a completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

// PutObject creates an object in a bucket.
//
// You must have WRITE permissions on a bucket to create an object.
//
//  - For size smaller than 5MiB PutObject automatically does a single atomic Put operation.
//  - For size larger than 5MiB PutObject automatically does a resumable multipart Put operation.
//  - For size input as -1 PutObject does a multipart Put operation until input stream reaches EOF.
//    Maximum object size that can be uploaded through this operation will be 5TiB.
//
// NOTE: Google Cloud Storage does not implement Amazon S3 Compatible multipart PUT.
// So we fall back to single PUT operation with the maximum limit of 5GiB.
//
// NOTE: For anonymous requests Amazon S3 doesn't allow multipart upload. So we fall back to single PUT operation.
func (c Client) PutObject(bucketName, objectName string, reader io.Reader, contentType string) (n int64, err error) {
	return c.PutObjectWithProgress(bucketName, objectName, reader, contentType, nil)
}

// putObjectNoChecksum special function used Google Cloud Storage. This special function
// is used for Google Cloud Storage since Google's multipart API is not S3 compatible.
func (c Client) putObjectNoChecksum(bucketName, objectName string, reader io.Reader, size int64, contentType string, progress io.Reader) (n int64, err error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return 0, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return 0, err
	}
	if size > maxSinglePutObjectSize {
		return 0, ErrEntityTooLarge(size, maxSinglePutObjectSize, bucketName, objectName)
	}

	// Update progress reader appropriately to the latest offset as we
	// read from the source.
	reader = newHook(reader, progress)

	// This function does not calculate sha256 and md5sum for payload.
	// Execute put object.
	st, err := c.putObjectDo(bucketName, objectName, ioutil.NopCloser(reader), nil, nil, size, contentType)
	if err != nil {
		return 0, err
	}
	if st.Size != size {
		return 0, ErrUnexpectedEOF(st.Size, size, bucketName, objectName)
	}
	return size, nil
}

// putObjectSingle is a special function for uploading single put object request.
// This special function is used as a fallback when multipart upload fails.
func (c Client) putObjectSingle(bucketName, objectName string, reader io.Reader, size int64, contentType string, progress io.Reader) (n int64, err error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return 0, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return 0, err
	}
	if size > maxSinglePutObjectSize {
		return 0, ErrEntityTooLarge(size, maxSinglePutObjectSize, bucketName, objectName)
	}
	// If size is a stream, upload up to 5GiB.
	if size <= -1 {
		size = maxSinglePutObjectSize
	}
	var md5Sum, sha256Sum []byte
	var readCloser io.ReadCloser
	if size <= minPartSize {
		// Initialize a new temporary buffer.
		tmpBuffer := new(bytes.Buffer)
		md5Sum, sha256Sum, size, err = c.hashCopyN(tmpBuffer, reader, size)
		readCloser = ioutil.NopCloser(tmpBuffer)
	} else {
		// Initialize a new temporary file.
		var tmpFile *tempFile
		tmpFile, err = newTempFile("single$-putobject-single")
		if err != nil {
			return 0, err
		}
		md5Sum, sha256Sum, size, err = c.hashCopyN(tmpFile, reader, size)
		// Seek back to beginning of the temporary file.
		if _, err = tmpFile.Seek(0, 0); err != nil {
			return 0, err
		}
		readCloser = tmpFile
	}
	// Return error if its not io.EOF.
	if err != nil {
		if err != io.EOF {
			return 0, err
		}
	}
	// Progress the reader to the size.
	if progress != nil {
		if _, err = io.CopyN(ioutil.Discard, progress, size); err != nil {
			return size, err
		}
	}
	// Execute put object.
	st, err := c.putObjectDo(bucketName, objectName, readCloser, md5Sum, sha256Sum, size, contentType)
	if err != nil {
		return 0, err
	}
	if st.Size != size {
		return 0, ErrUnexpectedEOF(st.Size, size, bucketName, objectName)
	}
	return size, nil
}

// putObjectDo - executes the put object http operation.
// NOTE: You must have WRITE permissions on a bucket to add an object to it.
func (c Client) putObjectDo(bucketName, objectName string, reader io.ReadCloser, md5Sum []byte, sha256Sum []byte, size int64, contentType string) (ObjectInfo, error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return ObjectInfo{}, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return ObjectInfo{}, err
	}

	if size <= -1 {
		return ObjectInfo{}, ErrEntityTooSmall(size, bucketName, objectName)
	}

	if size > maxSinglePutObjectSize {
		return ObjectInfo{}, ErrEntityTooLarge(size, maxSinglePutObjectSize, bucketName, objectName)
	}

	if strings.TrimSpace(contentType) == "" {
		contentType = "application/octet-stream"
	}

	// Set headers.
	customHeader := make(http.Header)
	customHeader.Set("Content-Type", contentType)

	// Populate request metadata.
	reqMetadata := requestMetadata{
		bucketName:         bucketName,
		objectName:         objectName,
		customHeader:       customHeader,
		contentBody:        reader,
		contentLength:      size,
		contentMD5Bytes:    md5Sum,
		contentSHA256Bytes: sha256Sum,
	}
	// Initiate new request.
	req, err := c.newRequest("PUT", reqMetadata)
	if err != nil {
		return ObjectInfo{}, err
	}
	// Execute the request.
	resp, err := c.do(req)
	defer closeResponse(resp)
	if err != nil {
		return ObjectInfo{}, err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			return ObjectInfo{}, httpRespToErrorResponse(resp, bucketName, objectName)
		}
	}

	var metadata ObjectInfo
	// Trim off the odd double quotes from ETag in the beginning and end.
	metadata.ETag = strings.TrimPrefix(resp.Header.Get("ETag"), "\"")
	metadata.ETag = strings.TrimSuffix(metadata.ETag, "\"")
	// A success here means data was written to server successfully.
	metadata.Size = size

	// Return here.
	return metadata, nil
}
