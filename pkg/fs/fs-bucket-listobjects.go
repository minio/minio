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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-xl/pkg/probe"
)

type listObjectsParams struct {
	Bucket    string
	Prefix    string
	Marker    string
	Delimiter string
	MaxKeys   int
}

type listServiceReq struct {
	reqParams listObjectsParams
	respCh    chan ListObjectsResult
}

type listWorkerReq struct {
	respCh chan ListObjectsResult
}

// listObjects - list objects lists objects upto maxKeys for a given
// prefix.
func (fs Filesystem) listObjects(bucket, prefix, marker, delimiter string, maxKeys int) (chan<- listWorkerReq, *probe.Error) {
	quitWalker := make(chan bool)
	reqCh := make(chan listWorkerReq)
	walkerCh := make(chan ObjectMetadata)
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
		filepath.Walk(walkPath, func(path string, info os.FileInfo, err error) error {
			// We don't need to list the walk path.
			if path == walkPath {
				return nil
			}
			// For all incoming directories add a ending separator.
			if info.IsDir() {
				path = path + string(os.PathSeparator)
			}
			// Extract object name.
			objectName := strings.TrimPrefix(path, bucketPathPrefix)
			if strings.HasPrefix(objectName, prefix) {
				// For objectName lesser than marker, ignore.
				if marker >= objectName {
					return nil
				}
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
					return filepath.SkipDir
				}
			}
			return nil
		})
	}()

	go func() {
		for {
			select {
			// Timeout after 10 seconds if request did not arrive for
			// the given list parameters.
			case <-time.After(10 * time.Second):
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
					if count == maxKeys {
						resp.IsTruncated = true
						break
					}
					// If object is a directory.
					if object.Mode.IsDir() {
						if delimiter == "" {
							// Skip directories for recursive listing.
							continue
						}
						resp.Prefixes = append(resp.Prefixes, object.Object)
					} else {
						resp.Objects = append(resp.Objects, object)
					}
					// Set the next marker for the next request.
					resp.NextMarker = object.Object
					count++
				}
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

// ListObjects - lists all objects for a given prefix, returns upto
// maxKeys number of objects per call.
func (fs Filesystem) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsResult, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	if !IsValidBucketName(bucket) {
		return ListObjectsResult{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	bucket = fs.denormalizeBucket(bucket)
	rootPrefix := filepath.Join(fs.path, bucket)
	// check bucket exists
	if _, e := os.Stat(rootPrefix); e != nil {
		if os.IsNotExist(e) {
			return ListObjectsResult{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ListObjectsResult{}, probe.NewError(e)
	}

	reqParams := listObjectsParams{}
	reqParams.Bucket = bucket
	reqParams.Prefix = filepath.FromSlash(prefix)
	reqParams.Marker = filepath.FromSlash(marker)
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
	if reqParams.Delimiter == "" {
		// This element is set only if you have delimiter set.
		// If response does not include the NextMaker and it is
		// truncated, you can use the value of the last Key in the
		// response as the marker in the subsequent request to get the
		// next set of object keys.
		resp.NextMarker = ""
	}
	return resp, nil
}
