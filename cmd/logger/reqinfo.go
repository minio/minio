/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package logger

import (
	"context"
	"fmt"
	"sync"
)

// Key used for Get/SetReqInfo
type contextKeyType string

const contextLogKey = contextKeyType("miniolog")

// KeyVal - appended to ReqInfo.Tags
type KeyVal struct {
	Key string
	Val string
}

// ReqInfo stores the request info.
type ReqInfo struct {
	RemoteHost string   // Client Host/IP
	UserAgent  string   // User Agent
	RequestID  string   // x-amz-request-id
	API        string   // API name - GetObject PutObject NewMultipartUpload etc.
	BucketName string   // Bucket name
	ObjectName string   // Object name
	tags       []KeyVal // Any additional info not accommodated by above fields
	sync.RWMutex
}

// NewReqInfo :
func NewReqInfo(remoteHost, userAgent, requestID, api, bucket, object string) *ReqInfo {
	req := ReqInfo{}
	req.RemoteHost = remoteHost
	req.UserAgent = userAgent
	req.API = api
	req.RequestID = requestID
	req.BucketName = bucket
	req.ObjectName = object
	return &req
}

// AppendTags - appends key/val to ReqInfo.tags
func (r *ReqInfo) AppendTags(key string, val string) *ReqInfo {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()
	r.tags = append(r.tags, KeyVal{key, val})
	return r
}

// GetTags - returns the user defined tags
func (r *ReqInfo) GetTags() []KeyVal {
	if r == nil {
		return nil
	}
	r.RLock()
	defer r.RUnlock()
	return append([]KeyVal(nil), r.tags...)
}

// SetReqInfo sets ReqInfo in the context.
func SetReqInfo(ctx context.Context, req *ReqInfo) context.Context {
	if ctx == nil {
		LogIf(context.Background(), fmt.Errorf("context is nil"))
		return nil
	}
	return context.WithValue(ctx, contextLogKey, req)
}

// GetReqInfo returns ReqInfo if set.
func GetReqInfo(ctx context.Context) *ReqInfo {
	if ctx != nil {
		r, ok := ctx.Value(contextLogKey).(*ReqInfo)
		if ok {
			return r
		}
	}
	return nil
}
