/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"encoding/xml"
	"net/http"
	"time"

	"github.com/minio/minio/pkg/fs"
)

const (
	// Reply date format
	timeFormatAMZ = "2006-01-02T15:04:05.000Z"
	// Limit number of objects in a given response.
	maxObjectList = 1000
)

// LocationResponse - format for location response.
type LocationResponse struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint" json:"-"`
	Location string   `xml:",chardata"`
}

// AccessControlPolicyResponse - format for get bucket acl response.
type AccessControlPolicyResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AccessControlPolicy" json:"-"`

	AccessControlList struct {
		Grants []Grant `xml:"Grant"`
	}
	Owner Owner
}

// Grant container for grantee and permission.
type Grant struct {
	Grantee struct {
		ID           string
		DisplayName  string
		EmailAddress string
		Type         string
		URI          string
	}
	Permission string
}

// ListObjectsResponse - format for list objects response.
type ListObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult" json:"-"`

	CommonPrefixes []CommonPrefix
	Contents       []Object

	Delimiter string

	// Encoding type used to encode object keys in the response.
	EncodingType string

	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool
	Marker      string
	MaxKeys     int
	Name        string

	// When response is truncated (the IsTruncated element value in the response
	// is true), you can use the key name in this field as marker in the subsequent
	// request to get next set of objects. Server lists objects in alphabetical
	// order Note: This element is returned only if you have delimiter request parameter
	// specified. If response does not include the NextMaker and it is truncated,
	// you can use the value of the last Key in the response as the marker in the
	// subsequent request to get the next set of object keys.
	NextMarker string
	Prefix     string
}

// Part container for part metadata.
type Part struct {
	PartNumber   int
	ETag         string
	LastModified string
	Size         int64
}

// ListPartsResponse - format for list parts response.
type ListPartsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListPartsResult" json:"-"`

	Bucket   string
	Key      string
	UploadID string `xml:"UploadId"`

	Initiator Initiator
	Owner     Owner

	// The class of storage used to store the object.
	StorageClass string

	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool

	// List of parts.
	Parts []Part `xml:"Part"`
}

// ListMultipartUploadsResponse - format for list multipart uploads response.
type ListMultipartUploadsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult" json:"-"`

	Bucket             string
	KeyMarker          string
	UploadIDMarker     string `xml:"UploadIdMarker"`
	NextKeyMarker      string
	NextUploadIDMarker string `xml:"NextUploadIdMarker"`
	EncodingType       string
	MaxUploads         int
	IsTruncated        bool
	Uploads            []Upload `xml:"Upload"`
	Prefix             string
	Delimiter          string
	CommonPrefixes     []CommonPrefix
}

// ListBucketsResponse - format for list buckets response
type ListBucketsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult" json:"-"`
	// Container for one or more buckets.
	Buckets struct {
		Buckets []Bucket `xml:"Bucket"`
	} // Buckets are nested
	Owner Owner
}

// Upload container for in progress multipart upload
type Upload struct {
	Key          string
	UploadID     string `xml:"UploadId"`
	Initiator    Initiator
	Owner        Owner
	StorageClass string
	Initiated    string
}

// CommonPrefix container for prefix response in ListObjectsResponse
type CommonPrefix struct {
	Prefix string
}

// Bucket container for bucket metadata
type Bucket struct {
	Name         string
	CreationDate string // time string of format "2006-01-02T15:04:05.000Z"
}

// Object container for object metadata
type Object struct {
	ETag         string
	Key          string
	LastModified string // time string of format "2006-01-02T15:04:05.000Z"
	Size         int64

	Owner Owner

	// The class of storage used to store the object.
	StorageClass string
}

// CopyObjectResponse container returns ETag and LastModified of the
// successfully copied object
type CopyObjectResponse struct {
	XMLName      xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyObjectResult" json:"-"`
	ETag         string
	LastModified string // time string of format "2006-01-02T15:04:05.000Z"
}

// Initiator inherit from Owner struct, fields are same
type Initiator Owner

// Owner - bucket owner/principal
type Owner struct {
	ID          string
	DisplayName string
}

// InitiateMultipartUploadResponse container for InitiateMultiPartUpload response, provides uploadID to start MultiPart upload
type InitiateMultipartUploadResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult" json:"-"`

	Bucket   string
	Key      string
	UploadID string `xml:"UploadId"`
}

// CompleteMultipartUploadResponse container for completed multipart upload response
type CompleteMultipartUploadResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult" json:"-"`

	Location string
	Bucket   string
	Key      string
	ETag     string
}

// getLocation get URL location.
func getLocation(r *http.Request) string {
	return r.URL.Path
}

// takes an array of Bucketmetadata information for serialization
// input:
// array of bucket metadata
//
// output:
// populated struct that can be serialized to match xml and json api spec output
func generateListBucketsResponse(buckets []fs.BucketMetadata) ListBucketsResponse {
	var listbuckets []Bucket
	var data = ListBucketsResponse{}
	var owner = Owner{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, bucket := range buckets {
		var listbucket = Bucket{}
		listbucket.Name = bucket.Name
		listbucket.CreationDate = bucket.Created.Format(timeFormatAMZ)
		listbuckets = append(listbuckets, listbucket)
	}

	data.Owner = owner
	data.Buckets.Buckets = listbuckets

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
	accessCtrlPolicyResponse.AccessControlList.Grants = append(accessCtrlPolicyResponse.AccessControlList.Grants, defaultGrant)
	switch {
	case acl.IsPublicRead():
		publicReadGrant := Grant{}
		publicReadGrant.Grantee.ID = "minio"
		publicReadGrant.Grantee.DisplayName = "minio"
		publicReadGrant.Grantee.URI = "http://acs.amazonaws.com/groups/global/AllUsers"
		publicReadGrant.Permission = "READ"
		accessCtrlPolicyResponse.AccessControlList.Grants = append(accessCtrlPolicyResponse.AccessControlList.Grants, publicReadGrant)
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
		accessCtrlPolicyResponse.AccessControlList.Grants = append(accessCtrlPolicyResponse.AccessControlList.Grants, publicReadGrant)
		accessCtrlPolicyResponse.AccessControlList.Grants = append(accessCtrlPolicyResponse.AccessControlList.Grants, publicReadWriteGrant)
	}
	return accessCtrlPolicyResponse
}

// generates an ListObjects response for the said bucket with other enumerated options.
func generateListObjectsResponse(bucket, prefix, marker, delimiter string, maxKeys int, resp fs.ListObjectsResult) ListObjectsResponse {
	var contents []Object
	var prefixes []CommonPrefix
	var owner = Owner{}
	var data = ListObjectsResponse{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, object := range resp.Objects {
		var content = Object{}
		if object.Object == "" {
			continue
		}
		content.Key = object.Object
		content.LastModified = object.LastModified.UTC().Format(timeFormatAMZ)
		if object.MD5 != "" {
			content.ETag = "\"" + object.MD5 + "\""
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
		var prefixItem = CommonPrefix{}
		prefixItem.Prefix = prefix
		prefixes = append(prefixes, prefixItem)
	}
	data.CommonPrefixes = prefixes
	return data
}

// generateCopyObjectResponse
func generateCopyObjectResponse(etag string, lastModified time.Time) CopyObjectResponse {
	return CopyObjectResponse{
		ETag:         "\"" + etag + "\"",
		LastModified: lastModified.UTC().Format(timeFormatAMZ),
	}
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

	listPartsResponse.Parts = make([]Part, len(objectMetadata.Part))
	for _, part := range objectMetadata.Part {
		newPart := Part{}
		newPart.PartNumber = part.PartNumber
		newPart.ETag = "\"" + part.ETag + "\""
		newPart.Size = part.Size
		newPart.LastModified = part.LastModified.UTC().Format(timeFormatAMZ)
		listPartsResponse.Parts = append(listPartsResponse.Parts, newPart)
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

	listMultipartUploadsResponse.Uploads = make([]Upload, len(metadata.Upload))
	for _, upload := range metadata.Upload {
		newUpload := Upload{}
		newUpload.UploadID = upload.UploadID
		newUpload.Key = upload.Object
		newUpload.Initiated = upload.Initiated.Format(timeFormatAMZ)
		listMultipartUploadsResponse.Uploads = append(listMultipartUploadsResponse.Uploads, newUpload)
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
	encodedErrorResponse := encodeResponse(errorResponse)
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
