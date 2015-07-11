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

	"github.com/minio/minio/pkg/donut"
)

// Reply date format
const (
	rfcFormat = "2006-01-02T15:04:05.000Z"
)

// takes an array of Bucketmetadata information for serialization
// input:
// array of bucket metadata
//
// output:
// populated struct that can be serialized to match xml and json api spec output
func generateListBucketsResponse(buckets []donut.BucketMetadata) ListBucketsResponse {
	var listbuckets []*Bucket
	var data = ListBucketsResponse{}
	var owner = Owner{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, bucket := range buckets {
		var listbucket = &Bucket{}
		listbucket.Name = bucket.Name
		listbucket.CreationDate = bucket.Created.Format(rfcFormat)
		listbuckets = append(listbuckets, listbucket)
	}

	data.Owner = owner
	data.Buckets.Bucket = listbuckets

	return data
}

// itemKey
type itemKey []*Object

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
func generateListObjectsResponse(bucket string, objects []donut.ObjectMetadata, bucketResources donut.BucketResourcesMetadata) ListObjectsResponse {
	var contents []*Object
	var prefixes []*CommonPrefix
	var owner = Owner{}
	var data = ListObjectsResponse{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, object := range objects {
		var content = &Object{}
		if object.Object == "" {
			continue
		}
		content.Key = object.Object
		content.LastModified = object.Created.Format(rfcFormat)
		content.ETag = "\"" + object.MD5Sum + "\""
		content.Size = object.Size
		content.StorageClass = "STANDARD"
		content.Owner = owner
		contents = append(contents, content)
	}
	sort.Sort(itemKey(contents))
	// TODO - support EncodingType in xml decoding
	data.Name = bucket
	data.Contents = contents
	data.MaxKeys = bucketResources.Maxkeys
	data.Prefix = bucketResources.Prefix
	data.Delimiter = bucketResources.Delimiter
	data.Marker = bucketResources.Marker
	data.NextMarker = bucketResources.NextMarker
	data.IsTruncated = bucketResources.IsTruncated
	for _, prefix := range bucketResources.CommonPrefixes {
		var prefixItem = &CommonPrefix{}
		prefixItem.Prefix = prefix
		prefixes = append(prefixes, prefixItem)
	}
	data.CommonPrefixes = prefixes
	return data
}

// generateInitiateMultipartUploadResponse
func generateInitiateMultipartUploadResponse(bucket, key, uploadID string) InitiateMultipartUploadResponse {
	return InitiateMultipartUploadResponse{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}
}

// generateCompleteMultipartUploadResponse
func generateCompleteMultpartUploadResponse(bucket, key, location, etag string) CompleteMultipartUploadResponse {
	return CompleteMultipartUploadResponse{
		Location: location,
		Bucket:   bucket,
		Key:      key,
		ETag:     etag,
	}
}

// generateListPartsResult
func generateListPartsResponse(objectMetadata donut.ObjectResourcesMetadata) ListPartsResponse {
	// TODO - support EncodingType in xml decoding
	listPartsResponse := ListPartsResponse{}
	listPartsResponse.Bucket = objectMetadata.Bucket
	listPartsResponse.Key = objectMetadata.Key
	listPartsResponse.UploadID = objectMetadata.UploadID
	listPartsResponse.StorageClass = "STANDARD"
	listPartsResponse.Initiator.ID = "minio"
	listPartsResponse.Initiator.DisplayName = "minio"
	listPartsResponse.Owner.ID = "minio"
	listPartsResponse.Owner.DisplayName = "minio"

	listPartsResponse.MaxParts = objectMetadata.MaxParts
	listPartsResponse.PartNumberMarker = objectMetadata.PartNumberMarker
	listPartsResponse.IsTruncated = objectMetadata.IsTruncated
	listPartsResponse.NextPartNumberMarker = objectMetadata.NextPartNumberMarker

	listPartsResponse.Part = make([]*Part, len(objectMetadata.Part))
	for _, part := range objectMetadata.Part {
		newPart := &Part{}
		newPart.PartNumber = part.PartNumber
		newPart.ETag = "\"" + part.ETag + "\""
		newPart.Size = part.Size
		newPart.LastModified = part.LastModified.Format(rfcFormat)
		listPartsResponse.Part = append(listPartsResponse.Part, newPart)
	}
	return listPartsResponse
}

// generateListMultipartUploadsResponse
func generateListMultipartUploadsResponse(bucket string, metadata donut.BucketMultipartResourcesMetadata) ListMultipartUploadsResponse {
	listMultipartUploadsResponse := ListMultipartUploadsResponse{}
	listMultipartUploadsResponse.Bucket = bucket
	listMultipartUploadsResponse.Delimiter = metadata.Delimiter
	listMultipartUploadsResponse.IsTruncated = metadata.IsTruncated
	listMultipartUploadsResponse.EncodingType = metadata.EncodingType
	listMultipartUploadsResponse.Prefix = metadata.Prefix
	listMultipartUploadsResponse.KeyMarker = metadata.KeyMarker
	listMultipartUploadsResponse.NextKeyMarker = metadata.NextKeyMarker
	listMultipartUploadsResponse.MaxUploads = metadata.MaxUploads
	listMultipartUploadsResponse.NextUploadIDMarker = metadata.NextUploadIDMarker
	listMultipartUploadsResponse.UploadIDMarker = metadata.UploadIDMarker

	listMultipartUploadsResponse.Upload = make([]*Upload, len(metadata.Upload))
	for _, upload := range metadata.Upload {
		newUpload := &Upload{}
		newUpload.UploadID = upload.UploadID
		newUpload.Key = upload.Key
		newUpload.Initiated = upload.Initiated.Format(rfcFormat)
		listMultipartUploadsResponse.Upload = append(listMultipartUploadsResponse.Upload, newUpload)
	}
	return listMultipartUploadsResponse
}

// writeSuccessResponse write success headers
func writeSuccessResponse(w http.ResponseWriter, acceptsContentType contentType) {
	setCommonHeaders(w, getContentTypeString(acceptsContentType), 0)
	w.WriteHeader(http.StatusOK)
}

// writeErrorRespone write error headers
func writeErrorResponse(w http.ResponseWriter, req *http.Request, errorType int, acceptsContentType contentType, resource string) {
	error := getErrorCode(errorType)
	// generate error response
	errorResponse := getErrorResponse(error, resource)
	encodedErrorResponse := encodeErrorResponse(errorResponse, acceptsContentType)
	// set common headers
	setCommonHeaders(w, getContentTypeString(acceptsContentType), len(encodedErrorResponse))
	// write Header
	w.WriteHeader(error.HTTPStatusCode)
	// HEAD should have no body
	if req.Method != "HEAD" {
		// write error body
		w.Write(encodedErrorResponse)
	}
}
