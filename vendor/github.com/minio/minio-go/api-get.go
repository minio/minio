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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// GetBucketPolicy - get bucket policy at a given path.
func (c Client) GetBucketPolicy(bucketName, objectPrefix string) (bucketPolicy BucketPolicy, err error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return BucketPolicyNone, err
	}
	if err := isValidObjectPrefix(objectPrefix); err != nil {
		return BucketPolicyNone, err
	}
	policy, err := c.getBucketPolicy(bucketName, objectPrefix)
	if err != nil {
		return BucketPolicyNone, err
	}
	if policy.Statements == nil {
		return BucketPolicyNone, nil
	}
	if isBucketPolicyReadWrite(policy.Statements, bucketName, objectPrefix) {
		return BucketPolicyReadWrite, nil
	} else if isBucketPolicyWriteOnly(policy.Statements, bucketName, objectPrefix) {
		return BucketPolicyWriteOnly, nil
	} else if isBucketPolicyReadOnly(policy.Statements, bucketName, objectPrefix) {
		return BucketPolicyReadOnly, nil
	}
	return BucketPolicyNone, nil
}

func (c Client) getBucketPolicy(bucketName string, objectPrefix string) (BucketAccessPolicy, error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return BucketAccessPolicy{}, err
	}
	if err := isValidObjectPrefix(objectPrefix); err != nil {
		return BucketAccessPolicy{}, err
	}
	// Get resources properly escaped and lined up before
	// using them in http request.
	urlValues := make(url.Values)
	urlValues.Set("policy", "")

	// Execute GET on bucket to list objects.
	resp, err := c.executeMethod("GET", requestMetadata{
		bucketName:  bucketName,
		queryValues: urlValues,
	})
	defer closeResponse(resp)
	if err != nil {
		return BucketAccessPolicy{}, err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			errResponse := httpRespToErrorResponse(resp, bucketName, "")
			if ToErrorResponse(errResponse).Code == "NoSuchBucketPolicy" {
				return BucketAccessPolicy{Version: "2012-10-17"}, nil
			}
			return BucketAccessPolicy{}, errResponse
		}
	}
	// Read access policy up to maxAccessPolicySize.
	// http://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html
	// bucket policies are limited to 20KB in size, using a limit reader.
	bucketPolicyBuf, err := ioutil.ReadAll(io.LimitReader(resp.Body, maxAccessPolicySize))
	if err != nil {
		return BucketAccessPolicy{}, err
	}
	policy, err := unMarshalBucketPolicy(bucketPolicyBuf)
	if err != nil {
		return BucketAccessPolicy{}, err
	}
	// Sort the policy actions and resources for convenience.
	for _, statement := range policy.Statements {
		sort.Strings(statement.Actions)
		sort.Strings(statement.Resources)
	}
	return policy, nil
}

// GetObject - returns an seekable, readable object.
func (c Client) GetObject(bucketName, objectName string) (*Object, error) {
	// Input validation.
	if err := isValidBucketName(bucketName); err != nil {
		return nil, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return nil, err
	}

	// Start the request as soon Get is initiated.
	httpReader, objectInfo, err := c.getObject(bucketName, objectName, 0, 0)
	if err != nil {
		return nil, err
	}

	// Create request channel.
	reqCh := make(chan readRequest)
	// Create response channel.
	resCh := make(chan readResponse)
	// Create done channel.
	doneCh := make(chan struct{})

	// This routine feeds partial object data as and when the caller reads.
	go func() {
		defer close(reqCh)
		defer close(resCh)

		// Loop through the incoming control messages and read data.
		for {
			select {
			// When the done channel is closed exit our routine.
			case <-doneCh:
				return
			// Request message.
			case req := <-reqCh:
				// Offset changes fetch the new object at an Offset.
				if req.DidOffsetChange {
					// Read from offset.
					httpReader, _, err = c.getObject(bucketName, objectName, req.Offset, 0)
					if err != nil {
						resCh <- readResponse{
							Error: err,
						}
						return
					}
				}

				// Read at least req.Buffer bytes, if not we have
				// reached our EOF.
				size, err := io.ReadFull(httpReader, req.Buffer)
				if err == io.ErrUnexpectedEOF {
					// If an EOF happens after reading some but not
					// all the bytes ReadFull returns ErrUnexpectedEOF
					err = io.EOF
				}
				// Reply back how much was read.
				resCh <- readResponse{
					Size:  int(size),
					Error: err,
				}
			}
		}
	}()
	// Return the readerAt backed by routine.
	return newObject(reqCh, resCh, doneCh, objectInfo), nil
}

// Read response message container to reply back for the request.
type readResponse struct {
	Size  int
	Error error
}

// Read request message container to communicate with internal
// go-routine.
type readRequest struct {
	Buffer          []byte
	Offset          int64 // readAt offset.
	DidOffsetChange bool
}

// Object represents an open object. It implements Read, ReadAt,
// Seeker, Close for a HTTP stream.
type Object struct {
	// Mutex.
	mutex *sync.Mutex

	// User allocated and defined.
	reqCh      chan<- readRequest
	resCh      <-chan readResponse
	doneCh     chan<- struct{}
	prevOffset int64
	currOffset int64
	objectInfo ObjectInfo

	// Keeps track of closed call.
	isClosed bool

	// Previous error saved for future calls.
	prevErr error
}

// Read reads up to len(p) bytes into p. It returns the number of
// bytes read (0 <= n <= len(p)) and any error encountered. Returns
// io.EOF upon end of file.
func (o *Object) Read(b []byte) (n int, err error) {
	if o == nil {
		return 0, ErrInvalidArgument("Object is nil")
	}

	// Locking.
	o.mutex.Lock()
	defer o.mutex.Unlock()

	// prevErr is previous error saved from previous operation.
	if o.prevErr != nil || o.isClosed {
		return 0, o.prevErr
	}

	// If current offset has reached Size limit, return EOF.
	if o.currOffset >= o.objectInfo.Size {
		return 0, io.EOF
	}

	// Send current information over control channel to indicate we are ready.
	reqMsg := readRequest{}
	// Send the pointer to the buffer over the channel.
	reqMsg.Buffer = b

	// Verify if offset has changed and currOffset is greater than
	// previous offset. Perhaps due to Seek().
	offsetChange := o.prevOffset - o.currOffset
	if offsetChange < 0 {
		offsetChange = -offsetChange
	}
	if offsetChange > 0 {
		// Fetch the new reader at the current offset again.
		reqMsg.Offset = o.currOffset
		reqMsg.DidOffsetChange = true
	} else {
		// No offset changes no need to fetch new reader, continue
		// reading.
		reqMsg.DidOffsetChange = false
		reqMsg.Offset = 0
	}

	// Send read request over the control channel.
	o.reqCh <- reqMsg

	// Get data over the response channel.
	dataMsg := <-o.resCh

	// Bytes read.
	bytesRead := int64(dataMsg.Size)

	// Update current offset.
	o.currOffset += bytesRead

	// Save the current offset as previous offset.
	o.prevOffset = o.currOffset

	if dataMsg.Error == nil {
		// If currOffset read is equal to objectSize
		// We have reached end of file, we return io.EOF.
		if o.currOffset >= o.objectInfo.Size {
			return dataMsg.Size, io.EOF
		}
		return dataMsg.Size, nil
	}

	// Save any error.
	o.prevErr = dataMsg.Error
	return dataMsg.Size, dataMsg.Error
}

// Stat returns the ObjectInfo structure describing object.
func (o *Object) Stat() (ObjectInfo, error) {
	if o == nil {
		return ObjectInfo{}, ErrInvalidArgument("Object is nil")
	}
	// Locking.
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.prevErr != nil || o.isClosed {
		return ObjectInfo{}, o.prevErr
	}

	return o.objectInfo, nil
}

// ReadAt reads len(b) bytes from the File starting at byte offset
// off. It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b). At end of
// file, that error is io.EOF.
func (o *Object) ReadAt(b []byte, offset int64) (n int, err error) {
	if o == nil {
		return 0, ErrInvalidArgument("Object is nil")
	}

	// Locking.
	o.mutex.Lock()
	defer o.mutex.Unlock()

	// prevErr is error which was saved in previous operation.
	if o.prevErr != nil || o.isClosed {
		return 0, o.prevErr
	}

	// If offset is negative and offset is greater than or equal to
	// object size we return EOF.
	if offset < 0 || offset >= o.objectInfo.Size {
		return 0, io.EOF
	}

	// Send current information over control channel to indicate we
	// are ready.
	reqMsg := readRequest{}

	// Send the offset and pointer to the buffer over the channel.
	reqMsg.Buffer = b

	// For ReadAt offset always changes, minor optimization where
	// offset same as currOffset we don't change the offset.
	reqMsg.DidOffsetChange = offset != o.currOffset
	if reqMsg.DidOffsetChange {
		// Set new offset.
		reqMsg.Offset = offset
		// Save new offset as current offset.
		o.currOffset = offset
	}

	// Send read request over the control channel.
	o.reqCh <- reqMsg

	// Get data over the response channel.
	dataMsg := <-o.resCh

	// Bytes read.
	bytesRead := int64(dataMsg.Size)

	// Update current offset.
	o.currOffset += bytesRead

	// Save current offset as previous offset before returning.
	o.prevOffset = o.currOffset

	if dataMsg.Error == nil {
		// If currentOffset is equal to objectSize
		// we have reached end of file, we return io.EOF.
		if o.currOffset >= o.objectInfo.Size {
			return dataMsg.Size, io.EOF
		}
		return dataMsg.Size, nil
	}

	// Save any error.
	o.prevErr = dataMsg.Error
	return dataMsg.Size, dataMsg.Error
}

// Seek sets the offset for the next Read or Write to offset,
// interpreted according to whence: 0 means relative to the
// origin of the file, 1 means relative to the current offset,
// and 2 means relative to the end.
// Seek returns the new offset and an error, if any.
//
// Seeking to a negative offset is an error. Seeking to any positive
// offset is legal, subsequent io operations succeed until the
// underlying object is not closed.
func (o *Object) Seek(offset int64, whence int) (n int64, err error) {
	if o == nil {
		return 0, ErrInvalidArgument("Object is nil")
	}

	// Locking.
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if o.prevErr != nil {
		// At EOF seeking is legal allow only io.EOF, for any other errors we return.
		if o.prevErr != io.EOF {
			return 0, o.prevErr
		}
	}

	// Negative offset is valid for whence of '2'.
	if offset < 0 && whence != 2 {
		return 0, ErrInvalidArgument(fmt.Sprintf("Negative position not allowed for %d.", whence))
	}

	// Save current offset as previous offset.
	o.prevOffset = o.currOffset

	// Switch through whence.
	switch whence {
	default:
		return 0, ErrInvalidArgument(fmt.Sprintf("Invalid whence %d", whence))
	case 0:
		if offset > o.objectInfo.Size {
			return 0, io.EOF
		}
		o.currOffset = offset
	case 1:
		if o.currOffset+offset > o.objectInfo.Size {
			return 0, io.EOF
		}
		o.currOffset += offset
	case 2:
		// Seeking to positive offset is valid for whence '2', but
		// since we are backing a Reader we have reached 'EOF' if
		// offset is positive.
		if offset > 0 {
			return 0, io.EOF
		}
		// Seeking to negative position not allowed for whence.
		if o.objectInfo.Size+offset < 0 {
			return 0, ErrInvalidArgument(fmt.Sprintf("Seeking at negative offset not allowed for %d", whence))
		}
		o.currOffset += offset
	}
	// Return the effective offset.
	return o.currOffset, nil
}

// Close - The behavior of Close after the first call returns error
// for subsequent Close() calls.
func (o *Object) Close() (err error) {
	if o == nil {
		return ErrInvalidArgument("Object is nil")
	}
	// Locking.
	o.mutex.Lock()
	defer o.mutex.Unlock()

	// if already closed return an error.
	if o.isClosed {
		return o.prevErr
	}

	// Close successfully.
	close(o.doneCh)

	// Save for future operations.
	errMsg := "Object is already closed. Bad file descriptor."
	o.prevErr = errors.New(errMsg)
	// Save here that we closed done channel successfully.
	o.isClosed = true
	return nil
}

// newObject instantiates a new *minio.Object*
func newObject(reqCh chan<- readRequest, resCh <-chan readResponse, doneCh chan<- struct{}, objectInfo ObjectInfo) *Object {
	return &Object{
		mutex:      &sync.Mutex{},
		reqCh:      reqCh,
		resCh:      resCh,
		doneCh:     doneCh,
		objectInfo: objectInfo,
	}
}

// getObject - retrieve object from Object Storage.
//
// Additionally this function also takes range arguments to download the specified
// range bytes of an object. Setting offset and length = 0 will download the full object.
//
// For more information about the HTTP Range header.
// go to http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35.
func (c Client) getObject(bucketName, objectName string, offset, length int64) (io.ReadCloser, ObjectInfo, error) {
	// Validate input arguments.
	if err := isValidBucketName(bucketName); err != nil {
		return nil, ObjectInfo{}, err
	}
	if err := isValidObjectName(objectName); err != nil {
		return nil, ObjectInfo{}, err
	}

	customHeader := make(http.Header)
	// Set ranges if length and offset are valid.
	if length > 0 && offset >= 0 {
		customHeader.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	} else if offset > 0 && length == 0 {
		customHeader.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	} else if length < 0 && offset == 0 {
		customHeader.Set("Range", fmt.Sprintf("bytes=%d", length))
	}

	// Execute GET on objectName.
	resp, err := c.executeMethod("GET", requestMetadata{
		bucketName:   bucketName,
		objectName:   objectName,
		customHeader: customHeader,
	})
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	if resp != nil {
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			return nil, ObjectInfo{}, httpRespToErrorResponse(resp, bucketName, objectName)
		}
	}

	// Trim off the odd double quotes from ETag in the beginning and end.
	md5sum := strings.TrimPrefix(resp.Header.Get("ETag"), "\"")
	md5sum = strings.TrimSuffix(md5sum, "\"")

	// Parse the date.
	date, err := time.Parse(http.TimeFormat, resp.Header.Get("Last-Modified"))
	if err != nil {
		msg := "Last-Modified time format not recognized. " + reportIssue
		return nil, ObjectInfo{}, ErrorResponse{
			Code:      "InternalError",
			Message:   msg,
			RequestID: resp.Header.Get("x-amz-request-id"),
			HostID:    resp.Header.Get("x-amz-id-2"),
			Region:    resp.Header.Get("x-amz-bucket-region"),
		}
	}
	// Get content-type.
	contentType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	var objectStat ObjectInfo
	objectStat.ETag = md5sum
	objectStat.Key = objectName
	objectStat.Size = resp.ContentLength
	objectStat.LastModified = date
	objectStat.ContentType = contentType

	// do not close body here, caller will close
	return resp.Body, objectStat, nil
}
