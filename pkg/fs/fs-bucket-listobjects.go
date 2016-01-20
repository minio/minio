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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-xl/pkg/probe"
)

func (fs Filesystem) listWorker(startReq listObjectsReq) (chan<- listWorkerReq, *probe.Error) {
	bucket := startReq.Bucket
	prefix := startReq.Prefix
	marker := startReq.Marker
	delimiter := startReq.Delimiter
	quitWalker := make(chan bool)
	reqCh := make(chan listWorkerReq)
	walkerCh := make(chan ObjectMetadata)
	go func() {
		var rootPath string
		bucketPath := filepath.Join(fs.path, bucket)
		trimBucketPathPrefix := bucketPath + string(os.PathSeparator)
		prefixPath := trimBucketPathPrefix + prefix
		st, err := os.Stat(prefixPath)
		if err != nil && os.IsNotExist(err) {
			rootPath = bucketPath
		} else {
			if st.IsDir() && !strings.HasSuffix(prefix, delimiter) {
				rootPath = bucketPath
			} else {
				rootPath = prefixPath
			}
		}
		filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
			if path == rootPath {
				return nil
			}
			if info.IsDir() {
				path = path + string(os.PathSeparator)
			}
			objectName := strings.TrimPrefix(path, trimBucketPathPrefix)
			if strings.HasPrefix(objectName, prefix) {
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
				case walkerCh <- object:
					// Do nothing
				case <-quitWalker:
					// Returning error ends the Walk()
					return errors.New("Ending")
				}
				if delimiter != "" && info.IsDir() {
					return filepath.SkipDir
				}
			}
			return nil
		})
		close(walkerCh)
	}()
	go func() {
		resp := ListObjectsResult{}
		for {
			select {
			case <-time.After(10 * time.Second):
				quitWalker <- true
				timeoutReq := listObjectsReq{bucket, prefix, marker, delimiter, 0}
				fs.timeoutReqCh <- timeoutReq
				// FIXME: can there be a race such that sender on reqCh panics?
				return
			case req, ok := <-reqCh:
				if !ok {
					return
				}
				resp = ListObjectsResult{}
				resp.Objects = make([]ObjectMetadata, 0)
				resp.Prefixes = make([]string, 0)
				count := 0
				for object := range walkerCh {
					if count == req.req.MaxKeys {
						resp.IsTruncated = true
						break
					}
					if object.Mode.IsDir() {
						if delimiter == "" {
							// Skip directories for recursive list
							continue
						}
						resp.Prefixes = append(resp.Prefixes, object.Object)
					} else {
						resp.Objects = append(resp.Objects, object)
					}
					resp.NextMarker = object.Object
					count++
				}
				req.respCh <- resp
			}
		}
	}()
	return reqCh, nil
}

func (fs *Filesystem) startListService() *probe.Error {
	listServiceReqCh := make(chan listServiceReq)
	timeoutReqCh := make(chan listObjectsReq)
	reqToListWorkerReqCh := make(map[string](chan<- listWorkerReq))
	reqToStr := func(bucket string, prefix string, marker string, delimiter string) string {
		return strings.Join([]string{bucket, prefix, marker, delimiter}, ":")
	}
	go func() {
		for {
			select {
			case timeoutReq := <-timeoutReqCh:
				reqStr := reqToStr(timeoutReq.Bucket, timeoutReq.Prefix, timeoutReq.Marker, timeoutReq.Delimiter)
				listWorkerReqCh, ok := reqToListWorkerReqCh[reqStr]
				if ok {
					close(listWorkerReqCh)
				}
				delete(reqToListWorkerReqCh, reqStr)
			case serviceReq := <-listServiceReqCh:
				reqStr := reqToStr(serviceReq.req.Bucket, serviceReq.req.Prefix, serviceReq.req.Marker, serviceReq.req.Delimiter)
				listWorkerReqCh, ok := reqToListWorkerReqCh[reqStr]
				if !ok {
					var err *probe.Error
					listWorkerReqCh, err = fs.listWorker(serviceReq.req)
					if err != nil {
						serviceReq.respCh <- ListObjectsResult{}
						return
					}
					reqToListWorkerReqCh[reqStr] = listWorkerReqCh
				}
				respCh := make(chan ListObjectsResult)
				listWorkerReqCh <- listWorkerReq{serviceReq.req, respCh}
				resp, ok := <-respCh
				if !ok {
					serviceReq.respCh <- ListObjectsResult{}
					return
				}
				delete(reqToListWorkerReqCh, reqStr)
				if !resp.IsTruncated {
					close(listWorkerReqCh)
				} else {
					reqStr = reqToStr(serviceReq.req.Bucket, serviceReq.req.Prefix, resp.NextMarker, serviceReq.req.Delimiter)
					reqToListWorkerReqCh[reqStr] = listWorkerReqCh
				}
				serviceReq.respCh <- resp
			}
		}
	}()
	fs.timeoutReqCh = timeoutReqCh
	fs.listServiceReqCh = listServiceReqCh
	return nil
}

// ListObjects -
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

	req := listObjectsReq{}
	req.Bucket = bucket
	req.Prefix = filepath.FromSlash(prefix)
	req.Marker = filepath.FromSlash(marker)
	req.Delimiter = filepath.FromSlash(delimiter)
	req.MaxKeys = maxKeys

	respCh := make(chan ListObjectsResult)
	fs.listServiceReqCh <- listServiceReq{req, respCh}
	resp := <-respCh

	for i := 0; i < len(resp.Prefixes); i++ {
		resp.Prefixes[i] = filepath.ToSlash(resp.Prefixes[i])
	}
	for i := 0; i < len(resp.Objects); i++ {
		resp.Objects[i].Object = filepath.ToSlash(resp.Objects[i].Object)
	}
	if req.Delimiter == "" {
		// This element is set only if you have delimiter set.
		// If response does not include the NextMaker and it is
		// truncated, you can use the value of the last Key in the
		// response as the marker in the subsequent request to get the
		// next set of object keys.
		resp.NextMarker = ""
	}
	return resp, nil
}
