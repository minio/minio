// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package logger

import (
	"context"
	"fmt"
	"sync"

	"github.com/minio/minio/internal/auth"
)

// Key used for Get/SetReqInfo
type contextKeyType string

const contextLogKey = contextKeyType("miniolog")

// KeyVal - appended to ReqInfo.Tags
type KeyVal struct {
	Key string
	Val string
}

// ObjectVersion object version key/versionId
type ObjectVersion struct {
	ObjectName string
	VersionID  string `json:"VersionId,omitempty"`
}

// ReqInfo stores the request info.
// Reading/writing directly to struct requires appropriate R/W lock.
type ReqInfo struct {
	RemoteHost   string           // Client Host/IP
	Host         string           // Node Host/IP
	UserAgent    string           // User Agent
	DeploymentID string           // x-minio-deployment-id
	RequestID    string           // x-amz-request-id
	API          string           // API name - GetObject PutObject NewMultipartUpload etc.
	BucketName   string           `json:",omitempty"` // Bucket name
	ObjectName   string           `json:",omitempty"` // Object name
	VersionID    string           `json:",omitempty"` // corresponding versionID for the object
	Objects      []ObjectVersion  `json:",omitempty"` // Only set during MultiObject delete handler.
	Cred         auth.Credentials `json:"-"`
	Region       string           `json:"-"`
	Owner        bool             `json:"-"`
	AuthType     string           `json:"-"`
	tags         []KeyVal         // Any additional info not accommodated by above fields
	sync.RWMutex
}

// NewReqInfo :
func NewReqInfo(remoteHost, userAgent, deploymentID, requestID, api, bucket, object string) *ReqInfo {
	return &ReqInfo{
		RemoteHost:   remoteHost,
		UserAgent:    userAgent,
		API:          api,
		DeploymentID: deploymentID,
		RequestID:    requestID,
		BucketName:   bucket,
		ObjectName:   object,
	}
}

// AppendTags - appends key/val to ReqInfo.tags
func (r *ReqInfo) AppendTags(key, val string) *ReqInfo {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()
	r.tags = append(r.tags, KeyVal{key, val})
	return r
}

// SetTags - sets key/val to ReqInfo.tags
func (r *ReqInfo) SetTags(key, val string) *ReqInfo {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()
	// Search of tag key already exists in tags
	var updated bool
	for _, tag := range r.tags {
		if tag.Key == key {
			tag.Val = val
			updated = true
			break
		}
	}
	if !updated {
		// Append to the end of tags list
		r.tags = append(r.tags, KeyVal{key, val})
	}
	return r
}

// GetTags - returns the user defined tags
func (r *ReqInfo) GetTags() []KeyVal {
	if r == nil {
		return nil
	}
	r.RLock()
	defer r.RUnlock()
	return append(make([]KeyVal, 0, len(r.tags)), r.tags...)
}

// GetTagsMap - returns the user defined tags in a map structure
func (r *ReqInfo) GetTagsMap() map[string]string {
	if r == nil {
		return nil
	}
	r.RLock()
	defer r.RUnlock()
	m := make(map[string]string, len(r.tags))
	for _, t := range r.tags {
		m[t.Key] = t.Val
	}
	return m
}

// PopulateTagsMap - returns the user defined tags in a map structure
func (r *ReqInfo) PopulateTagsMap(tagsMap map[string]string) {
	if r == nil {
		return
	}
	if tagsMap == nil {
		return
	}
	r.RLock()
	defer r.RUnlock()
	for _, t := range r.tags {
		tagsMap[t.Key] = t.Val
	}
}

// SetReqInfo sets ReqInfo in the context.
func SetReqInfo(ctx context.Context, req *ReqInfo) context.Context {
	if ctx == nil {
		LogIf(context.Background(), "", fmt.Errorf("context is nil"))
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
		r = &ReqInfo{}
		return r
	}
	return nil
}
