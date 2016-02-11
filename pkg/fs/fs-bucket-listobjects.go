/*
 * Minio Cloud Storage, (C) 2015-2016 Minio, Inc.
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

package fs

import (
	"errors"
	"hash/fnv"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio/pkg/ioutils"
	"github.com/minio/minio/pkg/probe"
)

// listObjectsParams - list objects input parameters.
type listObjectsParams struct {
	// Bucket name to list the objects for.
	Bucket string
	// list all objects with this parameter as common prefix.
	Prefix string
	// list all objects starting with object after marker in
	// lexicographical order.
	Marker string
	// list all objects until the first occurrence of the delimtier
	// after the prefix.
	Delimiter string
	// maximum number of objects returned per listObjects()
	// operation.
	MaxKeys int
}

// listServiceReq
type listServiceReq struct {
	reqParams listObjectsParams
	respCh    chan ListObjectsResult
}

type listWorkerReq struct {
	respCh chan ListObjectsResult
}

// listObjects - list objects lists objects up to maxKeys for a given prefix.
func (fs Filesystem) listObjects(bucket, prefix, marker, delimiter string, maxKeys int) (chan<- listWorkerReq, *probe.Error) {
	quitWalker := make(chan bool)
	reqCh := make(chan listWorkerReq)
	walkerCh := make(chan ObjectMetadata, 1000)
	go func() {
		defer close(walkerCh)
		var walkPath string
		bucketPath := filepath.Join(fs.path, bucket)
		// Bucket path prefix should always end with a separator.
		bucketPathPrefix := bucketPath + string(os.PathSeparator)
		prefixPath := bucketPathPrefix + prefix
		st, err := os.Stat(prefixPath)
		if err != nil && os.IsNotExist(err) {
			walkPath = bucketPath
		} else {
			if st.IsDir() && !strings.HasSuffix(prefix, delimiter) {
				walkPath = bucketPath
			} else {
				walkPath = prefixPath
			}
		}
		ioutils.FTW(walkPath, func(path string, info os.FileInfo, e error) error {
			if e != nil {
				return e
			}
			// Skip special temporary files, kept for multipart transaction.
			if strings.Contains(path, "$multiparts") || strings.Contains(path, "$tmpobject") {
				return nil
			}
			// We don't need to list the walk path.
			if path == walkPath {
				return nil
			}
			if info.IsDir() && delimiter == "" {
				return nil
			}
			// For all incoming directories add a ending separator.
			if info.IsDir() {
				path = path + string(os.PathSeparator)
			}
			// Extract object name.
			objectName := strings.TrimPrefix(path, bucketPathPrefix)
			if strings.HasPrefix(objectName, prefix) {
				object := ObjectMetadata{
					Object:  objectName,
					Created: info.ModTime(),
					Mode:    info.Mode(),
					Size:    info.Size(),
				}
				select {
				// Send object on walker channel.
				case walkerCh <- object:
				case <-quitWalker:
					// Returning error ends the file tree Walk().
					return errors.New("Quit list worker.")
				}
				// If delimiter is set, we stop if current path is a
				// directory.
				if delimiter != "" && info.IsDir() {
					return ioutils.ErrSkipDir
				}
			}
			return nil
		})
	}()

	go func() {
		for {
			select {
			// Timeout after 1 seconds if request did not arrive for
			// the given list parameters.
			case <-time.After(1 * time.Second):
				quitWalker <- true // Quit file path walk if running.
				// Send back the hash for this request.
				fs.timeoutReqCh <- fnvSum(bucket, prefix, marker, delimiter)
				return
			case req, ok := <-reqCh:
				if !ok {
					// If the request channel is closed, no more
					// requests return here.
					return
				}
				resp := ListObjectsResult{}
				var count int
				for object := range walkerCh {
					// Verify if the object is lexically smaller than
					// the marker, we will skip those objects.
					if marker != "" {
						if marker >= object.Object {
							continue
						} else {
							// Reset marker so that we avoid comparing
							// again and again in a loop unecessarily.
							marker = ""
						}
					}
					if delimiter != "" {
						// Prefixes are only valid wth delimiters, and
						// for filesystem backend they are only valid
						// if they are directories.
						if object.Mode.IsDir() {
							resp.Prefixes = append(resp.Prefixes, object.Object)
						} else {
							// Rest of them are treated as files.
							resp.Objects = append(resp.Objects, object)
						}
					} else {
						// In-case of no delimiters, there are no
						// prefixes all are considered to be objects.
						resp.Objects = append(resp.Objects, object)
					}
					count++ // Bump the counter
					// Verify if we have reached the maxKeys requested.
					if count == maxKeys {
						if delimiter != "" {
							// Set the next marker for the next request.
							// This element is set only if you have delimiter set.
							// If response does not include the NextMaker and it is
							// truncated, you can use the value of the last Key in the
							// response as the marker in the subsequent request to get the
							// next set of object keys.
							if len(resp.Objects) > 0 {
								// NextMarker is only set when there
								// are more than maxKeys worth of
								// objects for a given prefix path.
								resp.NextMarker = resp.Objects[len(resp.Objects)-1:][0].Object
							}
						}
						// Set truncated boolean to indicate the
						// client to send the next batch of requests.
						resp.IsTruncated = true
						break
					}
				}
				// Set the marker right here for the new set of the
				// values coming in the from the client.
				marker = resp.NextMarker
				req.respCh <- resp
			}
		}
	}()
	return reqCh, nil
}

// fnvSum calculates a hash for concatenation of all input strings.
func fnvSum(elements ...string) uint32 {
	fnvHash := fnv.New32a()
	for _, element := range elements {
		fnvHash.Write([]byte(element))
	}
	return fnvHash.Sum32()
}

// listObjectsService - list objects service manages various incoming
// list object requests by delegating them to an existing listObjects
// routine or initializes a new listObjects routine.
func (fs *Filesystem) listObjectsService() *probe.Error {
	// Initialize list service request channel.
	listServiceReqCh := make(chan listServiceReq)
	fs.listServiceReqCh = listServiceReqCh

	// Initialize timeout request channel to receive request hashes of
	// timed-out requests.
	timeoutReqCh := make(chan uint32)
	fs.timeoutReqCh = timeoutReqCh

	// Initialize request hash to list worker map.
	reqToListWorkerReqCh := make(map[uint32]chan<- listWorkerReq)

	// Start service in a go routine.
	go func() {
		for {
			select {
			case reqHash := <-timeoutReqCh:
				// For requests which have timed-out, close the worker
				// channels proactively, this may happen for idle
				// workers once in 10seconds.
				listWorkerReqCh, ok := reqToListWorkerReqCh[reqHash]
				if ok {
					close(listWorkerReqCh)
				}
				delete(reqToListWorkerReqCh, reqHash)
			case srvReq := <-listServiceReqCh:
				// Save the params for readability.
				bucket := srvReq.reqParams.Bucket
				prefix := srvReq.reqParams.Prefix
				marker := srvReq.reqParams.Marker
				delimiter := srvReq.reqParams.Delimiter
				maxKeys := srvReq.reqParams.MaxKeys

				// Generate hash.
				reqHash := fnvSum(bucket, prefix, marker, delimiter)
				listWorkerReqCh, ok := reqToListWorkerReqCh[reqHash]
				if !ok {
					var err *probe.Error
					listWorkerReqCh, err = fs.listObjects(bucket, prefix, marker, delimiter, maxKeys)
					if err != nil {
						srvReq.respCh <- ListObjectsResult{}
						return
					}
					reqToListWorkerReqCh[reqHash] = listWorkerReqCh
				}
				respCh := make(chan ListObjectsResult)
				listWorkerReqCh <- listWorkerReq{respCh}
				resp, ok := <-respCh
				if !ok {
					srvReq.respCh <- ListObjectsResult{}
					return
				}
				delete(reqToListWorkerReqCh, reqHash)
				if !resp.IsTruncated {
					close(listWorkerReqCh)
				} else {
					nextMarker := resp.NextMarker
					reqHash = fnvSum(bucket, prefix, nextMarker, delimiter)
					reqToListWorkerReqCh[reqHash] = listWorkerReqCh
				}
				srvReq.respCh <- resp
			}
		}
	}()
	return nil
}

// ListObjects - lists all objects for a given prefix, returns up to
// maxKeys number of objects per call.
func (fs Filesystem) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsResult, *probe.Error) {
	// Input validation.
	if !IsValidBucketName(bucket) {
		return ListObjectsResult{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	bucket = fs.denormalizeBucket(bucket)
	rootPrefix := filepath.Join(fs.path, bucket)
	// Check bucket exists.
	if _, e := os.Stat(rootPrefix); e != nil {
		if os.IsNotExist(e) {
			return ListObjectsResult{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ListObjectsResult{}, probe.NewError(e)
	}

	// Unescape the marker values.
	markerUnescaped, e := url.QueryUnescape(marker)
	if e != nil {
		return ListObjectsResult{}, probe.NewError(e)
	}

	reqParams := listObjectsParams{}
	reqParams.Bucket = bucket
	reqParams.Prefix = filepath.FromSlash(prefix)
	reqParams.Marker = filepath.FromSlash(markerUnescaped)
	reqParams.Delimiter = filepath.FromSlash(delimiter)
	reqParams.MaxKeys = maxKeys

	respCh := make(chan ListObjectsResult)
	fs.listServiceReqCh <- listServiceReq{reqParams, respCh}
	resp := <-respCh

	for i := range resp.Prefixes {
		resp.Prefixes[i] = filepath.ToSlash(resp.Prefixes[i])
	}
	for i := range resp.Objects {
		resp.Objects[i].Object = filepath.ToSlash(resp.Objects[i].Object)
	}
	return resp, nil
}
