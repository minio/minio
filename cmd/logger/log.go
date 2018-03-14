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

import "context"

// Key used for ContextSet/Get
const contextKey = "reqInfo"

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
	Tags       []KeyVal // Any additional info not accomodated by above fields
}

// AppendTags - appends key/val to ReqInfo.Tags
func (r *ReqInfo) AppendTags(key string, val string) {
	if r == nil {
		return
	}
	r.Tags = append(r.Tags, KeyVal{key, val})
}

// ContextSet sets ReqInfo in the context.
func ContextSet(ctx context.Context, req *ReqInfo) context.Context {
	return context.WithValue(ctx, contextKey, req)
}

// ContextGet returns ReqInfo if set.
func ContextGet(ctx context.Context) *ReqInfo {
	r, ok := ctx.Value(contextKey).(*ReqInfo)
	if ok {
		return r
	}
	return nil
}
