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

package cmd

import (
	"encoding/xml"
	"net/http"
	"path"
	"time"
)

const (
	timeFormatAMZ     = "2006-01-02T15:04:05Z"     // Reply date format
	timeFormatAMZLong = "2006-01-02T15:04:05.000Z" // Reply date format with nanosecond precision.
	maxObjectList     = 1000                       // Limit number of objects in a listObjectsResponse.
	maxUploadsList    = 1000                       // Limit number of uploads in a listUploadsResponse.
	maxPartsList      = 1000                       // Limit number of parts in a listPartsResponse.
)

// LocationResponse - format for location response.
type LocationResponse struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint" json:"-"`
	Location string   `xml:",chardata"`
}

// ListObjectsResponse - format for list objects response.
type ListObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult" json:"-"`

	Name   string
	Prefix string
	Marker string

	// When response is truncated (the IsTruncated element value in the response
	// is true), you can use the key name in this field as marker in the subsequent
	// request to get next set of objects. Server lists objects in alphabetical
	// order Note: This element is returned only if you have delimiter request parameter
	// specified. If response does not include the NextMaker and it is truncated,
	// you can use the value of the last Key in the response as the marker in the
	// subsequent request to get the next set of object keys.
	NextMarker string `xml:"NextMarker,omitempty"`

	MaxKeys   int
	Delimiter string
	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool

	Contents       []Object
	CommonPrefixes []CommonPrefix

	// Encoding type used to encode object keys in the response.
	EncodingType string `xml:"EncodingType,omitempty"`
}

// ListObjectsV2Response - format for list objects response.
type ListObjectsV2Response struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult" json:"-"`

	Name       string
	Prefix     string
	StartAfter string `xml:"StartAfter,omitempty"`
	// When response is truncated (the IsTruncated element value in the response
	// is true), you can use the key name in this field as marker in the subsequent
	// request to get next set of objects. Server lists objects in alphabetical
	// order Note: This element is returned only if you have delimiter request parameter
	// specified. If response does not include the NextMaker and it is truncated,
	// you can use the value of the last Key in the response as the marker in the
	// subsequent request to get the next set of object keys.
	ContinuationToken     string `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string `xml:"NextContinuationToken,omitempty"`

	KeyCount  int
	MaxKeys   int
	Delimiter string
	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool

	Contents       []Object
	CommonPrefixes []CommonPrefix

	// Encoding type used to encode object keys in the response.
	EncodingType string `xml:"EncodingType,omitempty"`
}

// Part container for part metadata.
type Part struct {
	PartNumber   int
	LastModified string
	ETag         string
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
	Delimiter          string
	Prefix             string
	EncodingType       string `xml:"EncodingType,omitempty"`
	MaxUploads         int
	IsTruncated        bool

	// List of pending uploads.
	Uploads []Upload `xml:"Upload"`

	// Delimed common prefixes.
	CommonPrefixes []CommonPrefix
}

// ListBucketsResponse - format for list buckets response
type ListBucketsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult" json:"-"`

	Owner Owner

	// Container for one or more buckets.
	Buckets struct {
		Buckets []Bucket `xml:"Bucket"`
	} // Buckets are nested
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
	Key          string
	LastModified string // time string of format "2006-01-02T15:04:05.000Z"
	ETag         string
	Size         int64

	// Owner of the object.
	Owner Owner

	// The class of storage used to store the object.
	StorageClass string
}

// CopyObjectResponse container returns ETag and LastModified of the successfully copied object
type CopyObjectResponse struct {
	XMLName      xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyObjectResult" json:"-"`
	LastModified string   // time string of format "2006-01-02T15:04:05.000Z"
	ETag         string   // md5sum of the copied object.
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

// DeleteError structure.
type DeleteError struct {
	Code    string
	Message string
	Key     string
}

// DeleteObjectsResponse container for multiple object deletes.
type DeleteObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult" json:"-"`

	// Collection of all deleted objects
	DeletedObjects []ObjectIdentifier `xml:"Deleted,omitempty"`

	// Collection of errors deleting certain objects.
	Errors []DeleteError `xml:"Error,omitempty"`
}

// getLocation get URL location.
func getLocation(r *http.Request) string {
	return path.Clean(r.URL.Path) // Clean any trailing slashes.
}

// getObjectLocation gets the relative URL for an object
func getObjectLocation(bucketName string, key string) string {
	return "/" + bucketName + "/" + key
}

// generates ListBucketsResponse from array of BucketInfo which can be
// serialized to match XML and JSON API spec output.
func generateListBucketsResponse(buckets []BucketInfo) ListBucketsResponse {
	var listbuckets []Bucket
	var data = ListBucketsResponse{}
	var owner = Owner{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, bucket := range buckets {
		var listbucket = Bucket{}
		listbucket.Name = bucket.Name
		listbucket.CreationDate = bucket.Created.Format(timeFormatAMZLong)
		listbuckets = append(listbuckets, listbucket)
	}

	data.Owner = owner
	data.Buckets.Buckets = listbuckets

	return data
}

// generates an ListObjectsV1 response for the said bucket with other enumerated options.
func generateListObjectsV1Response(bucket, prefix, marker, delimiter string, maxKeys int, resp ListObjectsInfo) ListObjectsResponse {
	var contents []Object
	var prefixes []CommonPrefix
	var owner = Owner{}
	var data = ListObjectsResponse{}

	owner.ID = "minio"
	owner.DisplayName = "minio"

	for _, object := range resp.Objects {
		var content = Object{}
		if object.Name == "" {
			continue
		}
		content.Key = object.Name
		content.LastModified = object.ModTime.UTC().Format(timeFormatAMZLong)
		if object.MD5Sum != "" {
			content.ETag = "\"" + object.MD5Sum + "\""
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

// generates an ListObjectsV2 response for the said bucket with other enumerated options.
func generateListObjectsV2Response(bucket, prefix, token, startAfter, delimiter string, fetchOwner bool, maxKeys int, resp ListObjectsInfo) ListObjectsV2Response {
	var contents []Object
	var prefixes []CommonPrefix
	var owner = Owner{}
	var data = ListObjectsV2Response{}

	if fetchOwner {
		owner.ID = "minio"
		owner.DisplayName = "minio"
	}

	for _, object := range resp.Objects {
		var content = Object{}
		if object.Name == "" {
			continue
		}
		content.Key = object.Name
		content.LastModified = object.ModTime.UTC().Format(timeFormatAMZLong)
		if object.MD5Sum != "" {
			content.ETag = "\"" + object.MD5Sum + "\""
		}
		content.Size = object.Size
		content.StorageClass = "STANDARD"
		content.Owner = owner
		contents = append(contents, content)
	}
	// TODO - support EncodingType in xml decoding
	data.Name = bucket
	data.Contents = contents

	data.StartAfter = startAfter
	data.Delimiter = delimiter
	data.Prefix = prefix
	data.MaxKeys = maxKeys
	data.ContinuationToken = token
	data.NextContinuationToken = resp.NextMarker
	data.IsTruncated = resp.IsTruncated
	for _, prefix := range resp.Prefixes {
		var prefixItem = CommonPrefix{}
		prefixItem.Prefix = prefix
		prefixes = append(prefixes, prefixItem)
	}
	data.CommonPrefixes = prefixes
	data.KeyCount = len(data.Contents) + len(data.CommonPrefixes)
	return data
}

// generates CopyObjectResponse from etag and lastModified time.
func generateCopyObjectResponse(etag string, lastModified time.Time) CopyObjectResponse {
	return CopyObjectResponse{
		ETag:         "\"" + etag + "\"",
		LastModified: lastModified.UTC().Format(timeFormatAMZLong),
	}
}

// generates InitiateMultipartUploadResponse for given bucket, key and uploadID.
func generateInitiateMultipartUploadResponse(bucket, key, uploadID string) InitiateMultipartUploadResponse {
	return InitiateMultipartUploadResponse{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}
}

// generates CompleteMultipartUploadResponse for given bucket, key, location and ETag.
func generateCompleteMultpartUploadResponse(bucket, key, location, etag string) CompleteMultipartUploadResponse {
	return CompleteMultipartUploadResponse{
		Location: location,
		Bucket:   bucket,
		Key:      key,
		ETag:     etag,
	}
}

// generates ListPartsResponse from ListPartsInfo.
func generateListPartsResponse(partsInfo ListPartsInfo) ListPartsResponse {
	// TODO - support EncodingType in xml decoding
	listPartsResponse := ListPartsResponse{}
	listPartsResponse.Bucket = partsInfo.Bucket
	listPartsResponse.Key = partsInfo.Object
	listPartsResponse.UploadID = partsInfo.UploadID
	listPartsResponse.StorageClass = "STANDARD"
	listPartsResponse.Initiator.ID = "minio"
	listPartsResponse.Initiator.DisplayName = "minio"
	listPartsResponse.Owner.ID = "minio"
	listPartsResponse.Owner.DisplayName = "minio"

	listPartsResponse.MaxParts = partsInfo.MaxParts
	listPartsResponse.PartNumberMarker = partsInfo.PartNumberMarker
	listPartsResponse.IsTruncated = partsInfo.IsTruncated
	listPartsResponse.NextPartNumberMarker = partsInfo.NextPartNumberMarker

	listPartsResponse.Parts = make([]Part, len(partsInfo.Parts))
	for index, part := range partsInfo.Parts {
		newPart := Part{}
		newPart.PartNumber = part.PartNumber
		newPart.ETag = "\"" + part.ETag + "\""
		newPart.Size = part.Size
		newPart.LastModified = part.LastModified.UTC().Format(timeFormatAMZLong)
		listPartsResponse.Parts[index] = newPart
	}
	return listPartsResponse
}

// generates ListMultipartUploadsResponse for given bucket and ListMultipartsInfo.
func generateListMultipartUploadsResponse(bucket string, multipartsInfo ListMultipartsInfo) ListMultipartUploadsResponse {
	listMultipartUploadsResponse := ListMultipartUploadsResponse{}
	listMultipartUploadsResponse.Bucket = bucket
	listMultipartUploadsResponse.Delimiter = multipartsInfo.Delimiter
	listMultipartUploadsResponse.IsTruncated = multipartsInfo.IsTruncated
	listMultipartUploadsResponse.EncodingType = multipartsInfo.EncodingType
	listMultipartUploadsResponse.Prefix = multipartsInfo.Prefix
	listMultipartUploadsResponse.KeyMarker = multipartsInfo.KeyMarker
	listMultipartUploadsResponse.NextKeyMarker = multipartsInfo.NextKeyMarker
	listMultipartUploadsResponse.MaxUploads = multipartsInfo.MaxUploads
	listMultipartUploadsResponse.NextUploadIDMarker = multipartsInfo.NextUploadIDMarker
	listMultipartUploadsResponse.UploadIDMarker = multipartsInfo.UploadIDMarker
	listMultipartUploadsResponse.CommonPrefixes = make([]CommonPrefix, len(multipartsInfo.CommonPrefixes))
	for index, commonPrefix := range multipartsInfo.CommonPrefixes {
		listMultipartUploadsResponse.CommonPrefixes[index] = CommonPrefix{
			Prefix: commonPrefix,
		}
	}
	listMultipartUploadsResponse.Uploads = make([]Upload, len(multipartsInfo.Uploads))
	for index, upload := range multipartsInfo.Uploads {
		newUpload := Upload{}
		newUpload.UploadID = upload.UploadID
		newUpload.Key = upload.Object
		newUpload.Initiated = upload.Initiated.UTC().Format(timeFormatAMZLong)
		listMultipartUploadsResponse.Uploads[index] = newUpload
	}
	return listMultipartUploadsResponse
}

// generate multi objects delete response.
func generateMultiDeleteResponse(quiet bool, deletedObjects []ObjectIdentifier, errs []DeleteError) DeleteObjectsResponse {
	deleteResp := DeleteObjectsResponse{}
	if !quiet {
		deleteResp.DeletedObjects = deletedObjects
	}
	deleteResp.Errors = errs
	return deleteResp
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
func writeErrorResponse(w http.ResponseWriter, req *http.Request, errorCode APIErrorCode, resource string) {
	apiError := getAPIError(errorCode)
	// set common headers
	setCommonHeaders(w)
	// write Header
	w.WriteHeader(apiError.HTTPStatusCode)
	writeErrorResponseNoHeader(w, req, errorCode, resource)
}

func writeErrorResponseNoHeader(w http.ResponseWriter, req *http.Request, errorCode APIErrorCode, resource string) {
	apiError := getAPIError(errorCode)
	// Generate error response.
	errorResponse := getAPIErrorResponse(apiError, resource)
	encodedErrorResponse := encodeResponse(errorResponse)
	// HEAD should have no body, do not attempt to write to it
	if req.Method != "HEAD" {
		// write error body
		w.Write(encodedErrorResponse)
		w.(http.Flusher).Flush()
	}
}
