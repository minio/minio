/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package api

import (
	"net/http"
	"sort"
	"strconv"

	"github.com/minio-io/minio/pkg/storage/drivers"
)

// Reply date format
const (
	iso8601Format = "2006-01-02T15:04:05.000Z"
)

// takes an array of Bucketmetadata information for serialization
// input:
// array of bucket metadata
//
// output:
// populated struct that can be serialized to match xml and json api spec output
func generateListBucketsResponse(buckets []drivers.BucketMetadata) ListBucketsResponse {
	var listbuckets []*Bucket
	var data = ListBucketsResponse{}
	var owner = Owner{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, bucket := range buckets {
		var listbucket = &Bucket{}
		listbucket.Name = bucket.Name
		listbucket.CreationDate = bucket.Created.Format(iso8601Format)
		listbuckets = append(listbuckets, listbucket)
	}

	data.Owner = owner
	data.Buckets.Bucket = listbuckets

	return data
}

// itemKey
type itemKey []*Item

func (b itemKey) Len() int           { return len(b) }
func (b itemKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b itemKey) Less(i, j int) bool { return b[i].Key < b[j].Key }

// takes a set of objects and prepares the objects for serialization
// input:
// bucket name
// array of object metadata
// results truncated flag
//
// output:
// populated struct that can be serialized to match xml and json api spec output
func generateListObjectsResponse(bucket string, objects []drivers.ObjectMetadata, bucketResources drivers.BucketResourcesMetadata) ListObjectsResponse {
	var contents []*Item
	var prefixes []*Prefix
	var owner = Owner{}
	var data = ListObjectsResponse{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, object := range objects {
		var content = &Item{}
		if object.Key == "" {
			continue
		}
		content.Key = object.Key
		content.LastModified = object.Created.Format(iso8601Format)
		content.ETag = object.Md5
		content.Size = object.Size
		content.StorageClass = "STANDARD"
		content.Owner = owner
		contents = append(contents, content)
	}
	sort.Sort(itemKey(contents))
	data.Name = bucket
	data.Contents = contents
	data.MaxKeys = bucketResources.Maxkeys
	data.Prefix = bucketResources.Prefix
	data.Delimiter = bucketResources.Delimiter
	data.Marker = bucketResources.Marker
	data.IsTruncated = bucketResources.IsTruncated
	for _, prefix := range bucketResources.CommonPrefixes {
		var prefixItem = &Prefix{}
		prefixItem.Prefix = prefix
		prefixes = append(prefixes, prefixItem)
	}
	data.CommonPrefixes = prefixes
	return data
}

// writeSuccessResponse - write success headers
func writeSuccessResponse(w http.ResponseWriter, acceptsContentType contentType) {
	setCommonHeaders(w, getContentTypeString(acceptsContentType))
	w.WriteHeader(http.StatusOK)
}

// writeErrorRespone - write error headers
func writeErrorResponse(w http.ResponseWriter, req *http.Request, errorType int, acceptsContentType contentType, resource string) {
	error := getErrorCode(errorType)
	// generate error response
	errorResponse := getErrorResponse(error, resource)
	encodedErrorResponse := encodeErrorResponse(errorResponse, acceptsContentType)
	// set common headers
	setCommonHeaders(w, getContentTypeString(acceptsContentType))
	// set content-length to size of error response
	w.Header().Set("Content-Length", strconv.Itoa(len(encodedErrorResponse)))
	// set headers
	w.WriteHeader(error.HTTPStatusCode)
	// write body
	w.Write(encodedErrorResponse)
}
