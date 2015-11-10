/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-xl/pkg/probe"
)

func (fs Filesystem) listWorker(startReq ListObjectsReq) (chan<- listWorkerReq, *probe.Error) {
	Separator := string(os.PathSeparator)
	bucket := startReq.Bucket
	prefix := startReq.Prefix
	marker := startReq.Marker
	delimiter := startReq.Delimiter
	quit := make(chan bool)
	if marker != "" {
		return nil, probe.NewError(errors.New("Not supported"))
	}
	if delimiter != "" && delimiter != Separator {
		return nil, probe.NewError(errors.New("Not supported"))
	}
	reqCh := make(chan listWorkerReq)
	walkerCh := make(chan ObjectMetadata)
	go func() {
		rootPath := filepath.Join(fs.path, bucket, prefix)
		stripPath := filepath.Join(fs.path, bucket) + Separator
		filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
			if path == rootPath {
				return nil
			}
			if info.IsDir() {
				path = path + Separator
			}
			objectName := strings.TrimPrefix(path, stripPath)
			object := ObjectMetadata{
				Object:  objectName,
				Created: info.ModTime(),
				Mode:    info.Mode(),
				Size:    info.Size(),
			}
			select {
			case walkerCh <- object:
				// do nothings
			case <-quit:
				fmt.Println("walker got quit")
				// returning error ends the Walk()
				return errors.New("Ending")
			}
			if delimiter == Separator && info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		})
		close(walkerCh)
	}()
	go func() {
		resp := ListObjectsResp{}
		for {
			select {
			case <-time.After(10 * time.Second):
				fmt.Println("worker got timeout")
				quit <- true
				timeoutReq := ListObjectsReq{bucket, prefix, marker, delimiter, 0}
				fmt.Println("after timeout", fs)
				fs.timeoutReqCh <- timeoutReq
				// FIXME: can there be a race such that sender on reqCh panics?
				return
			case req := <-reqCh:
				resp = ListObjectsResp{}
				resp.Objects = make([]ObjectMetadata, 0)
				resp.Prefixes = make([]string, 0)
				count := 0
				for object := range walkerCh {
					if object.Mode.IsDir() {
						if delimiter == "" {
							// skip directories for recursive list
							continue
						}
						resp.Prefixes = append(resp.Prefixes, object.Object)
					} else {
						resp.Objects = append(resp.Objects, object)
					}
					resp.NextMarker = object.Object
					count++
					if count == req.req.MaxKeys {
						resp.IsTruncated = true
						break
					}
				}
				fmt.Println("response objects: ", len(resp.Objects))
				marker = resp.NextMarker
				req.respCh <- resp
			}
		}
	}()
	return reqCh, nil
}

func (fs *Filesystem) startListService() *probe.Error {
	fmt.Println("startListService starting")
	listServiceReqCh := make(chan listServiceReq)
	timeoutReqCh := make(chan ListObjectsReq)
	reqToListWorkerReqCh := make(map[string](chan<- listWorkerReq))
	reqToStr := func(bucket string, prefix string, marker string, delimiter string) string {
		return strings.Join([]string{bucket, prefix, marker, delimiter}, ":")
	}
	go func() {
		for {
			select {
			case timeoutReq := <-timeoutReqCh:
				fmt.Println("listservice got timeout on ", timeoutReq)
				reqStr := reqToStr(timeoutReq.Bucket, timeoutReq.Prefix, timeoutReq.Marker, timeoutReq.Delimiter)
				listWorkerReqCh, ok := reqToListWorkerReqCh[reqStr]
				if ok {
					close(listWorkerReqCh)
				}
				delete(reqToListWorkerReqCh, reqStr)
			case serviceReq := <-listServiceReqCh:
				fmt.Println("serviceReq received", serviceReq)
				fmt.Println("sending to listservicereqch", fs)

				reqStr := reqToStr(serviceReq.req.Bucket, serviceReq.req.Prefix, serviceReq.req.Marker, serviceReq.req.Delimiter)
				listWorkerReqCh, ok := reqToListWorkerReqCh[reqStr]
				if !ok {
					var err *probe.Error
					listWorkerReqCh, err = fs.listWorker(serviceReq.req)
					if err != nil {
						fmt.Println("listWorker returned error", err)
						serviceReq.respCh <- ListObjectsResp{}
						return
					}
					reqToListWorkerReqCh[reqStr] = listWorkerReqCh
				}
				respCh := make(chan ListObjectsResp)
				listWorkerReqCh <- listWorkerReq{serviceReq.req, respCh}
				resp, ok := <-respCh
				if !ok {
					serviceReq.respCh <- ListObjectsResp{}
					fmt.Println("listWorker resp was not ok")
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
func (fs Filesystem) ListObjects(bucket string, req ListObjectsReq) (ListObjectsResp, *probe.Error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	Separator := string(os.PathSeparator)
	if !IsValidBucketName(bucket) {
		return ListObjectsResp{}, probe.NewError(BucketNameInvalid{Bucket: bucket})
	}

	bucket = fs.denormalizeBucket(bucket)
	rootPrefix := filepath.Join(fs.path, bucket)
	// check bucket exists
	if _, e := os.Stat(rootPrefix); e != nil {
		if os.IsNotExist(e) {
			return ListObjectsResp{}, probe.NewError(BucketNotFound{Bucket: bucket})
		}
		return ListObjectsResp{}, probe.NewError(e)
	}

	canonicalize := func(str string) string {
		return strings.Replace(str, "/", string(os.PathSeparator), -1)
	}
	decanonicalize := func(str string) string {
		return strings.Replace(str, string(os.PathSeparator), "/", -1)
	}

	req.Bucket = bucket
	req.Prefix = canonicalize(req.Prefix)
	req.Marker = canonicalize(req.Marker)
	req.Delimiter = canonicalize(req.Delimiter)

	if req.Delimiter != "" && req.Delimiter != Separator {
		return ListObjectsResp{}, probe.NewError(errors.New("not supported"))
	}

	respCh := make(chan ListObjectsResp)
	fs.listServiceReqCh <- listServiceReq{req, respCh}
	resp := <-respCh

	for i := 0; i < len(resp.Prefixes); i++ {
		resp.Prefixes[i] = decanonicalize(resp.Prefixes[i])
	}
	for i := 0; i < len(resp.Objects); i++ {
		resp.Objects[i].Object = decanonicalize(resp.Objects[i].Object)
	}
	if req.Delimiter == "" {
		// unset NextMaker for recursive list
		resp.NextMarker = ""
	}
	return resp, nil
}
