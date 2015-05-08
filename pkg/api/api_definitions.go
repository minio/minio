/*
 * Minimalist Object Storage, (C) 2014 Minio, Inc.
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
	"encoding/xml"
)

// Limit number of objects in a given response
const (
	maxObjectList = 1000
)

// ListObjectsResponse - format for list objects response
type ListObjectsResponse struct {
	XMLName xml.Name `xml:"http://doc.s3.amazonaws.com/2006-03-01 ListBucketResult" json:"-"`

	CommonPrefixes []*CommonPrefix
	Contents       []*Object

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
	// request to get next set of objects. Object storage lists objects in alphabetical
	// order Note: This element is returned only if you have delimiter request parameter
	// specified. If response does not include the NextMaker and it is truncated,
	// you can use the value of the last Key in the response as the marker in the
	// subsequent request to get the next set of object keys.
	NextMarker string
	Prefix     string
}

// ListBucketsResponse - format for list buckets response
type ListBucketsResponse struct {
	XMLName xml.Name `xml:"http://doc.s3.amazonaws.com/2006-03-01 ListAllMyBucketsResult" json:"-"`
	// Container for one or more buckets.
	Buckets struct {
		Bucket []*Bucket
	} // Buckets are nested
	Owner Owner
}

// CommonPrefix container for prefix response in ListObjectsResponse
type CommonPrefix struct {
	Prefix string
}

// Bucket container for bucket metadata
type Bucket struct {
	Name         string
	CreationDate string
}

// Object container for object metadata
type Object struct {
	ETag         string
	Key          string
	LastModified string
	Size         int64

	Owner Owner

	// The class of storage used to store the object.
	StorageClass string
}

// Owner - bucket owner/principal
type Owner struct {
	ID          string
	DisplayName string
}

// InitiateMultipartUploadResult container for InitiateMultiPartUpload response, provides uploadID to start MultiPart upload
type InitiateMultipartUploadResult struct {
	XMLName xml.Name `xml:"http://doc.s3.amazonaws.com/2006-03-01 InitiateMultipartUploadResult" json:"-"`

	Bucket   string
	Key      string
	UploadID string `xml:"UploadId"`
}

// CompleteMultipartUpload container for completing multipart upload
type CompleteMultipartUpload struct {
	Part []Part
}

// CompleteMultipartUploadResult container for completed multipart upload response
type CompleteMultipartUploadResult struct {
	XMLName xml.Name `xml:"http://doc.s3.amazonaws.com/2006-03-01 CompleteMultipartUploadResult" json:"-"`

	Location string
	Bucket   string
	Key      string
	ETag     string
}

// Part description of a multipart part
type Part struct {
	PartNumber int
	ETag       string
}

// List of not implemented bucket queries
var notimplementedBucketResourceNames = map[string]bool{
	"policy":         true,
	"cors":           true,
	"lifecycle":      true,
	"location":       true,
	"logging":        true,
	"notification":   true,
	"tagging":        true,
	"versions":       true,
	"requestPayment": true,
	"versioning":     true,
	"website":        true,
}

// List of not implemented object queries
var notimplementedObjectResourceNames = map[string]bool{
	"torrent": true,
}
