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
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/minio/minio-go/pkg/s3utils"
)

// toInt - converts go value to its integer representation based
// on the value kind if it is an integer.
func toInt(value reflect.Value) (size int64) {
	size = -1
	if value.IsValid() {
		switch value.Kind() {
		case reflect.Int:
			fallthrough
		case reflect.Int8:
			fallthrough
		case reflect.Int16:
			fallthrough
		case reflect.Int32:
			fallthrough
		case reflect.Int64:
			size = value.Int()
		}
	}
	return size
}

// getReaderSize - Determine the size of Reader if available.
func getReaderSize(reader io.Reader) (size int64, err error) {
	size = -1
	if reader == nil {
		return -1, nil
	}
	// Verify if there is a method by name 'Size'.
	sizeFn := reflect.ValueOf(reader).MethodByName("Size")
	// Verify if there is a method by name 'Len'.
	lenFn := reflect.ValueOf(reader).MethodByName("Len")
	if sizeFn.IsValid() {
		if sizeFn.Kind() == reflect.Func {
			// Call the 'Size' function and save its return value.
			result := sizeFn.Call([]reflect.Value{})
			if len(result) == 1 {
				size = toInt(result[0])
			}
		}
	} else if lenFn.IsValid() {
		if lenFn.Kind() == reflect.Func {
			// Call the 'Len' function and save its return value.
			result := lenFn.Call([]reflect.Value{})
			if len(result) == 1 {
				size = toInt(result[0])
			}
		}
	} else {
		// Fallback to Stat() method, two possible Stat() structs exist.
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
			case "stdin", "stdout", "stderr":
				return
			// Ignore read/write stream of os.Pipe() which have unknown length too.
			case "|0", "|1":
				return
			}
			var pos int64
			pos, err = v.Seek(0, 1) // SeekCurrent.
			if err != nil {
				return -1, err
			}
			size = st.Size() - pos
		case *Object:
			var st ObjectInfo
			st, err = v.Stat()
			if err != nil {
				return
			}
			var pos int64
			pos, err = v.Seek(0, 1) // SeekCurrent.
			if err != nil {
				return -1, err
			}
			size = st.Size - pos
		}
	}
	// Returns the size here.
	return size, err
}

// completedParts is a collection of parts sortable by their part numbers.
// used for sorting the uploaded parts before completing the multipart request.
type completedParts []CompletePart

func (a completedParts) Len() int           { return len(a) }
func (a completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

// PutObject creates an object in a bucket.
//
// You must have WRITE permissions on a bucket to create an object.
//
//  - For size smaller than 64MiB PutObject automatically does a
//    single atomic Put operation.
//  - For size larger than 64MiB PutObject automatically does a
//    multipart Put operation.
//  - For size input as -1 PutObject does a multipart Put operation
//    until input stream reaches EOF. Maximum object size that can
//    be uploaded through this operation will be 5TiB.
func (c Client) PutObject(bucketName, objectName string, reader io.Reader, contentType string) (n int64, err error) {
	return c.PutObjectWithMetadata(bucketName, objectName, reader, map[string][]string{
		"Content-Type": []string{contentType},
	}, nil)
}

// PutObjectWithSize - is a helper PutObject similar in behavior to PutObject()
// but takes the size argument explicitly, this function avoids doing reflection
// internally to figure out the size of input stream. Also if the input size is
// lesser than 0 this function returns an error.
func (c Client) PutObjectWithSize(bucketName, objectName string, reader io.Reader, readerSize int64, metadata map[string][]string, progress io.Reader) (n int64, err error) {
	return c.putObjectCommon(bucketName, objectName, reader, readerSize, metadata, progress)
}

// PutObjectWithMetadata using AWS streaming signature V4
func (c Client) PutObjectWithMetadata(bucketName, objectName string, reader io.Reader, metadata map[string][]string, progress io.Reader) (n int64, err error) {
	return c.PutObjectWithProgress(bucketName, objectName, reader, metadata, progress)
}

// PutObjectWithProgress using AWS streaming signature V4
func (c Client) PutObjectWithProgress(bucketName, objectName string, reader io.Reader, metadata map[string][]string, progress io.Reader) (n int64, err error) {
	// Size of the object.
	var size int64

	// Get reader size.
	size, err = getReaderSize(reader)
	if err != nil {
		return 0, err
	}

	return c.putObjectCommon(bucketName, objectName, reader, size, metadata, progress)
}

func (c Client) putObjectCommon(bucketName, objectName string, reader io.Reader, size int64, metadata map[string][]string, progress io.Reader) (n int64, err error) {
	// Check for largest object size allowed.
	if size > int64(maxMultipartPutObjectSize) {
		return 0, ErrEntityTooLarge(size, maxMultipartPutObjectSize, bucketName, objectName)
	}

	// NOTE: Streaming signature is not supported by GCS.
	if s3utils.IsGoogleEndpoint(c.endpointURL) {
		// Do not compute MD5 for Google Cloud Storage.
		return c.putObjectNoChecksum(bucketName, objectName, reader, size, metadata, progress)
	}

	if c.overrideSignerType.IsV2() {
		if size >= 0 && size < minPartSize {
			return c.putObjectNoChecksum(bucketName, objectName, reader, size, metadata, progress)
		}
		return c.putObjectMultipart(bucketName, objectName, reader, size, metadata, progress)
	}

	if size < 0 {
		return c.putObjectMultipartStreamNoLength(bucketName, objectName, reader, metadata, progress)
	}

	if size < minPartSize {
		return c.putObjectNoChecksum(bucketName, objectName, reader, size, metadata, progress)
	}

	// For all sizes greater than 64MiB do multipart.
	return c.putObjectMultipartStream(bucketName, objectName, reader, size, metadata, progress)
}

func (c Client) putObjectMultipartStreamNoLength(bucketName, objectName string, reader io.Reader, metadata map[string][]string,
	progress io.Reader) (n int64, err error) {
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
		length, rErr := io.ReadFull(reader, buf)
		if rErr == io.EOF {
			break
		}
		if rErr != nil && rErr != io.ErrUnexpectedEOF {
			return 0, rErr
		}

		// Update progress reader appropriately to the latest offset
		// as we read from the source.
		rd := newHook(bytes.NewReader(buf[:length]), progress)

		// Proceed to upload the part.
		var objPart ObjectPart
		objPart, err = c.uploadPart(bucketName, objectName, uploadID, rd, partNumber,
			nil, nil, int64(length), metadata)
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
