/*
 * Minio Cloud Storage, (C) 2014 Minio, Inc.
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

import "encoding/xml"

// Limit number of objects in a given response
const (
	maxObjectList = 1000
)

// AccessControlPolicyResponse - format for get bucket acl response
type AccessControlPolicyResponse struct {
	AccessControlList struct {
		Grant []Grant
	}
	Owner Owner
}

// Grant container for grantee and permission
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

// ListObjectsResponse - format for list objects response
type ListObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult" json:"-"`

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
	// request to get next set of objects. Server lists objects in alphabetical
	// order Note: This element is returned only if you have delimiter request parameter
	// specified. If response does not include the NextMaker and it is truncated,
	// you can use the value of the last Key in the response as the marker in the
	// subsequent request to get the next set of object keys.
	NextMarker string
	Prefix     string
}

// Part container for part metadata
type Part struct {
	PartNumber   int
	ETag         string
	LastModified string
	Size         int64
}

// ListPartsResponse - format for list parts response
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

	// List of parts
	Part []*Part
}

// ListMultipartUploadsResponse - format for list multipart uploads response
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
	Upload             []*Upload
	Prefix             string
	Delimiter          string
	CommonPrefixes     []*CommonPrefix
}

// ListBucketsResponse - format for list buckets response
type ListBucketsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult" json:"-"`
	// Container for one or more buckets.
	Buckets struct {
		Bucket []*Bucket
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

// List of not implemented bucket queries
var notimplementedBucketResourceNames = map[string]bool{
	"policy":         true,
	"cors":           true,
	"lifecycle":      true,
	"location":       true,
	"logging":        true,
	"notification":   true,
	"replication":    true,
	"tagging":        true,
	"versions":       true,
	"requestPayment": true,
	"versioning":     true,
	"website":        true,
}

// List of not implemented object queries
var notimplementedObjectResourceNames = map[string]bool{
	"torrent": true,
	"acl":     true,
	"policy":  true,
}
