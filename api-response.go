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

package main

import (
	"net/http"

	"github.com/minio/minio/pkg/fs"
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
func generateListBucketsResponse(buckets []fs.BucketMetadata) ListBucketsResponse {
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

// generates an AccessControlPolicy response for the said ACL.
func generateAccessControlPolicyResponse(acl fs.BucketACL) AccessControlPolicyResponse {
	accessCtrlPolicyResponse := AccessControlPolicyResponse{}
	accessCtrlPolicyResponse.Owner = Owner{
		ID:          "minio",
		DisplayName: "minio",
	}
	defaultGrant := Grant{}
	defaultGrant.Grantee.ID = "minio"
	defaultGrant.Grantee.DisplayName = "minio"
	defaultGrant.Permission = "FULL_CONTROL"
	accessCtrlPolicyResponse.AccessControlList.Grant = append(accessCtrlPolicyResponse.AccessControlList.Grant, defaultGrant)
	switch {
	case acl.IsPublicRead():
		publicReadGrant := Grant{}
		publicReadGrant.Grantee.ID = "minio"
		publicReadGrant.Grantee.DisplayName = "minio"
		publicReadGrant.Grantee.URI = "http://acs.amazonaws.com/groups/global/AllUsers"
		publicReadGrant.Permission = "READ"
		accessCtrlPolicyResponse.AccessControlList.Grant = append(accessCtrlPolicyResponse.AccessControlList.Grant, publicReadGrant)
	case acl.IsPublicReadWrite():
		publicReadGrant := Grant{}
		publicReadGrant.Grantee.ID = "minio"
		publicReadGrant.Grantee.DisplayName = "minio"
		publicReadGrant.Grantee.URI = "http://acs.amazonaws.com/groups/global/AllUsers"
		publicReadGrant.Permission = "READ"
		publicReadWriteGrant := Grant{}
		publicReadWriteGrant.Grantee.ID = "minio"
		publicReadWriteGrant.Grantee.DisplayName = "minio"
		publicReadWriteGrant.Grantee.URI = "http://acs.amazonaws.com/groups/global/AllUsers"
		publicReadWriteGrant.Permission = "WRITE"
		accessCtrlPolicyResponse.AccessControlList.Grant = append(accessCtrlPolicyResponse.AccessControlList.Grant, publicReadGrant)
		accessCtrlPolicyResponse.AccessControlList.Grant = append(accessCtrlPolicyResponse.AccessControlList.Grant, publicReadWriteGrant)
	}
	return accessCtrlPolicyResponse
}

// generates an ListObjects response for the said bucket with other enumerated options.
func generateListObjectsResponse(bucket, prefix, marker, delimiter string, maxKeys int, resp fs.ListObjectsResult) ListObjectsResponse {
	var contents []*Object
	var prefixes []*CommonPrefix
	var owner = Owner{}
	var data = ListObjectsResponse{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, object := range resp.Objects {
		var content = &Object{}
		if object.Object == "" {
			continue
		}
		content.Key = object.Object
		content.LastModified = object.Created.Format(rfcFormat)
		if object.Md5 != "" {
			content.ETag = "\"" + object.Md5 + "\""
		}
		content.Size = object.Size
		content.StorageClass = "STANDARD"
		content.Owner = owner
		contents = append(contents, content)
	}
	// TODO - support EncodingType in xml decoding
	data.Name = bucket
	data.Contents = contents

	data.Prefix = prefix
	data.Marker = marker
	data.Delimiter = delimiter
	data.MaxKeys = maxKeys

	data.NextMarker = resp.NextMarker
	data.IsTruncated = resp.IsTruncated
	for _, prefix := range resp.Prefixes {
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
func generateListPartsResponse(objectMetadata fs.ObjectResourcesMetadata) ListPartsResponse {
	// TODO - support EncodingType in xml decoding
	listPartsResponse := ListPartsResponse{}
	listPartsResponse.Bucket = objectMetadata.Bucket
	listPartsResponse.Key = objectMetadata.Object
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
func generateListMultipartUploadsResponse(bucket string, metadata fs.BucketMultipartResourcesMetadata) ListMultipartUploadsResponse {
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
		newUpload.Key = upload.Object
		newUpload.Initiated = upload.Initiated.Format(rfcFormat)
		listMultipartUploadsResponse.Upload = append(listMultipartUploadsResponse.Upload, newUpload)
	}
	return listMultipartUploadsResponse
}

// writeSuccessResponse write success headers and response if any.
func writeSuccessResponse(w http.ResponseWriter, response []byte) {
	setCommonHeaders(w)
	if response == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Write(response)
	w.(http.Flusher).Flush()
}

// writeSuccessNoContent write success headers with http status 204
func writeSuccessNoContent(w http.ResponseWriter) {
	setCommonHeaders(w)
	w.WriteHeader(http.StatusNoContent)
}

// writeErrorRespone write error headers
func writeErrorResponse(w http.ResponseWriter, req *http.Request, errorType int, resource string) {
	error := getErrorCode(errorType)
	// generate error response
	errorResponse := getErrorResponse(error, resource)
	encodedErrorResponse := encodeErrorResponse(errorResponse)
	// set common headers
	setCommonHeaders(w)
	// write Header
	w.WriteHeader(error.HTTPStatusCode)
	// HEAD should have no body, do not attempt to write to it
	if req.Method != "HEAD" {
		// write error body
		w.Write(encodedErrorResponse)
		w.(http.Flusher).Flush()
	}
}
