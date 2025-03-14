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

package cmd

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/internal/amztime"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/policy"
	xxml "github.com/minio/xxml"
)

const (
	maxObjectList  = 1000  // Limit number of objects in a listObjectsResponse/listObjectsVersionsResponse.
	maxDeleteList  = 1000  // Limit number of objects deleted in a delete call.
	maxUploadsList = 10000 // Limit number of uploads in a listUploadsResponse.
	maxPartsList   = 10000 // Limit number of parts in a listPartsResponse.
)

// LocationResponse - format for location response.
type LocationResponse struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint" json:"-"`
	Location string   `xml:",chardata"`
}

// PolicyStatus captures information returned by GetBucketPolicyStatusHandler
type PolicyStatus struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ PolicyStatus" json:"-"`
	IsPublic string
}

// ListVersionsResponse - format for list bucket versions response.
type ListVersionsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListVersionsResult" json:"-"`

	Name      string
	Prefix    string
	KeyMarker string

	// When response is truncated (the IsTruncated element value in the response
	// is true), you can use the key name in this field as marker in the subsequent
	// request to get next set of objects. Server lists objects in alphabetical
	// order Note: This element is returned only if you have delimiter request parameter
	// specified. If response does not include the NextMaker and it is truncated,
	// you can use the value of the last Key in the response as the marker in the
	// subsequent request to get the next set of object keys.
	NextKeyMarker string `xml:"NextKeyMarker,omitempty"`

	// When the number of responses exceeds the value of MaxKeys,
	// NextVersionIdMarker specifies the first object version not
	// returned that satisfies the search criteria. Use this value
	// for the version-id-marker request parameter in a subsequent request.
	NextVersionIDMarker string `xml:"NextVersionIdMarker"`

	// Marks the last version of the Key returned in a truncated response.
	VersionIDMarker string `xml:"VersionIdMarker"`

	MaxKeys   int
	Delimiter string `xml:"Delimiter,omitempty"`
	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool

	CommonPrefixes []CommonPrefix
	Versions       []ObjectVersion

	// Encoding type used to encode object keys in the response.
	EncodingType string `xml:"EncodingType,omitempty"`
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
	Delimiter string `xml:"Delimiter,omitempty"`
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
	Delimiter string `xml:"Delimiter,omitempty"`
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

	// Checksum values
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
	ChecksumCRC64NVME string `xml:",omitempty"`
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

	ChecksumAlgorithm string
	ChecksumType      string

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
	Delimiter          string `xml:"Delimiter,omitempty"`
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

// ObjectVersion container for object version metadata
type ObjectVersion struct {
	Object
	IsLatest  bool
	VersionID string `xml:"VersionId"`

	isDeleteMarker bool
}

// MarshalXML - marshal ObjectVersion
func (o ObjectVersion) MarshalXML(e *xxml.Encoder, start xxml.StartElement) error {
	if o.isDeleteMarker {
		start.Name.Local = "DeleteMarker"
	} else {
		start.Name.Local = "Version"
	}
	type objectVersionWrapper ObjectVersion
	return e.EncodeElement(objectVersionWrapper(o), start)
}

// DeleteMarkerVersion container for delete marker metadata
type DeleteMarkerVersion struct {
	Key          string
	LastModified string // time string of format "2006-01-02T15:04:05.000Z"

	// Owner of the object.
	Owner Owner

	IsLatest  bool
	VersionID string `xml:"VersionId"`
}

// Metadata metadata items implemented to ensure XML marshaling works.
type Metadata struct {
	Items []struct {
		Key   string
		Value string
	}
}

// Set add items, duplicate items get replaced.
func (s *Metadata) Set(k, v string) {
	for i, item := range s.Items {
		if item.Key == k {
			s.Items[i] = struct {
				Key   string
				Value string
			}{
				Key:   k,
				Value: v,
			}
			return
		}
	}
	s.Items = append(s.Items, struct {
		Key   string
		Value string
	}{
		Key:   k,
		Value: v,
	})
}

type xmlKeyEntry struct {
	XMLName xxml.Name
	Value   string `xml:",chardata"`
}

// MarshalXML - StringMap marshals into XML.
func (s *Metadata) MarshalXML(e *xxml.Encoder, start xxml.StartElement) error {
	if s == nil {
		return nil
	}

	if len(s.Items) == 0 {
		return nil
	}

	if err := e.EncodeToken(start); err != nil {
		return err
	}

	for _, item := range s.Items {
		if err := e.Encode(xmlKeyEntry{
			XMLName: xxml.Name{Local: item.Key},
			Value:   item.Value,
		}); err != nil {
			return err
		}
	}

	return e.EncodeToken(start.End())
}

// ObjectInternalInfo contains some internal information about a given
// object, it will printed in listing calls with enabled metadata.
type ObjectInternalInfo struct {
	K int // Data blocks
	M int // Parity blocks
}

// Object container for object metadata
type Object struct {
	Key          string
	LastModified string // time string of format "2006-01-02T15:04:05.000Z"
	ETag         string
	Size         int64

	// Owner of the object.
	Owner *Owner `xml:"Owner,omitempty"`

	// The class of storage used to store the object.
	StorageClass string

	// UserMetadata user-defined metadata
	UserMetadata *Metadata `xml:"UserMetadata,omitempty"`
	UserTags     string    `xml:"UserTags,omitempty"`

	Internal *ObjectInternalInfo `xml:"Internal,omitempty"`
}

// CopyObjectResponse container returns ETag and LastModified of the successfully copied object
type CopyObjectResponse struct {
	XMLName      xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyObjectResult" json:"-"`
	LastModified string   // time string of format "2006-01-02T15:04:05.000Z"
	ETag         string   // md5sum of the copied object.
}

// CopyObjectPartResponse container returns ETag and LastModified of the successfully copied object
type CopyObjectPartResponse struct {
	XMLName      xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyPartResult" json:"-"`
	LastModified string   // time string of format "2006-01-02T15:04:05.000Z"
	ETag         string   // md5sum of the copied object part.
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

	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
	ChecksumCRC64NVME string `xml:",omitempty"`
}

// DeleteError structure.
type DeleteError struct {
	Code      string
	Message   string
	Key       string
	VersionID string `xml:"VersionId"`
}

// DeleteObjectsResponse container for multiple object deletes.
type DeleteObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult" json:"-"`

	// Collection of all deleted objects
	DeletedObjects []DeletedObject `xml:"Deleted,omitempty"`

	// Collection of errors deleting certain objects.
	Errors []DeleteError `xml:"Error,omitempty"`
}

// PostResponse container for POST object request when success_action_status is set to 201
type PostResponse struct {
	Bucket   string
	Key      string
	ETag     string
	Location string
}

// returns "https" if the tls boolean is true, "http" otherwise.
func getURLScheme(tls bool) string {
	if tls {
		return httpsScheme
	}
	return httpScheme
}

// getObjectLocation gets the fully qualified URL of an object.
func getObjectLocation(r *http.Request, domains []string, bucket, object string) string {
	// unit tests do not have host set.
	if r.Host == "" {
		return path.Clean(r.URL.Path)
	}
	proto := handlers.GetSourceScheme(r)
	if proto == "" {
		proto = getURLScheme(globalIsTLS)
	}
	u := &url.URL{
		Host:   r.Host,
		Path:   path.Join(SlashSeparator, bucket, object),
		Scheme: proto,
	}
	// If domain is set then we need to use bucket DNS style.
	for _, domain := range domains {
		if strings.HasPrefix(r.Host, bucket+"."+domain) {
			u.Path = path.Join(SlashSeparator, object)
			break
		}
	}
	return u.String()
}

// generates ListBucketsResponse from array of BucketInfo which can be
// serialized to match XML and JSON API spec output.
func generateListBucketsResponse(buckets []BucketInfo) ListBucketsResponse {
	listbuckets := make([]Bucket, 0, len(buckets))
	data := ListBucketsResponse{}
	owner := Owner{
		ID:          globalMinioDefaultOwnerID,
		DisplayName: "minio",
	}

	for _, bucket := range buckets {
		listbuckets = append(listbuckets, Bucket{
			Name:         bucket.Name,
			CreationDate: amztime.ISO8601Format(bucket.Created.UTC()),
		})
	}

	data.Owner = owner
	data.Buckets.Buckets = listbuckets

	return data
}

func cleanReservedKeys(metadata map[string]string) map[string]string {
	m := cloneMSS(metadata)

	switch kind, _ := crypto.IsEncrypted(metadata); kind {
	case crypto.S3:
		m[xhttp.AmzServerSideEncryption] = xhttp.AmzEncryptionAES
	case crypto.S3KMS:
		m[xhttp.AmzServerSideEncryption] = xhttp.AmzEncryptionKMS
		m[xhttp.AmzServerSideEncryptionKmsID] = kmsKeyIDFromMetadata(metadata)
		if kmsCtx, ok := metadata[crypto.MetaContext]; ok {
			m[xhttp.AmzServerSideEncryptionKmsContext] = kmsCtx
		}
	case crypto.SSEC:
		m[xhttp.AmzServerSideEncryptionCustomerAlgorithm] = xhttp.AmzEncryptionAES
	}

	var toRemove []string
	for k := range cleanMinioInternalMetadataKeys(m) {
		if stringsHasPrefixFold(k, ReservedMetadataPrefixLower) {
			// Do not need to send any internal metadata
			// values to client.
			toRemove = append(toRemove, k)
			continue
		}

		// https://github.com/google/security-research/security/advisories/GHSA-76wf-9vgp-pj7w
		if equals(k, xhttp.AmzMetaUnencryptedContentLength, xhttp.AmzMetaUnencryptedContentMD5) {
			toRemove = append(toRemove, k)
			continue
		}
	}

	for _, k := range toRemove {
		delete(m, k)
		delete(m, strings.ToLower(k))
	}

	return m
}

// generates an ListBucketVersions response for the said bucket with other enumerated options.
func generateListVersionsResponse(ctx context.Context, bucket, prefix, marker, versionIDMarker, delimiter, encodingType string, maxKeys int, resp ListObjectVersionsInfo, metadata metaCheckFn) ListVersionsResponse {
	versions := make([]ObjectVersion, 0, len(resp.Objects))

	owner := &Owner{
		ID:          globalMinioDefaultOwnerID,
		DisplayName: "minio",
	}
	data := ListVersionsResponse{}
	var lastObjMetaName string
	var tagErr, metaErr APIErrorCode = -1, -1

	for _, object := range resp.Objects {
		if object.Name == "" {
			continue
		}
		// Cache checks for the same object
		if metadata != nil && lastObjMetaName != object.Name {
			tagErr = metadata(object.Name, policy.GetObjectTaggingAction)
			metaErr = metadata(object.Name, policy.GetObjectAction)
			lastObjMetaName = object.Name
		}
		content := ObjectVersion{}
		content.Key = s3EncodeName(object.Name, encodingType)
		content.LastModified = amztime.ISO8601Format(object.ModTime.UTC())
		if object.ETag != "" {
			content.ETag = "\"" + object.ETag + "\""
		}
		content.Size = object.Size
		if object.StorageClass != "" {
			content.StorageClass = filterStorageClass(ctx, object.StorageClass)
		} else {
			content.StorageClass = globalMinioDefaultStorageClass
		}
		if tagErr == ErrNone {
			content.UserTags = object.UserTags
		}
		if metaErr == ErrNone {
			content.UserMetadata = &Metadata{}
			switch kind, _ := crypto.IsEncrypted(object.UserDefined); kind {
			case crypto.S3:
				content.UserMetadata.Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionAES)
			case crypto.S3KMS:
				content.UserMetadata.Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionKMS)
			case crypto.SSEC:
				content.UserMetadata.Set(xhttp.AmzServerSideEncryptionCustomerAlgorithm, xhttp.AmzEncryptionAES)
			}
			for k, v := range cleanReservedKeys(object.UserDefined) {
				content.UserMetadata.Set(k, v)
			}
			content.Internal = &ObjectInternalInfo{
				K: object.DataBlocks,
				M: object.ParityBlocks,
			}
		}
		content.Owner = owner
		content.VersionID = object.VersionID
		if content.VersionID == "" {
			content.VersionID = nullVersionID
		}
		content.IsLatest = object.IsLatest
		content.isDeleteMarker = object.DeleteMarker
		versions = append(versions, content)
	}

	data.Name = bucket
	data.Versions = versions
	data.EncodingType = encodingType
	data.Prefix = s3EncodeName(prefix, encodingType)
	data.KeyMarker = s3EncodeName(marker, encodingType)
	data.Delimiter = s3EncodeName(delimiter, encodingType)
	data.MaxKeys = maxKeys

	data.NextKeyMarker = s3EncodeName(resp.NextMarker, encodingType)
	data.NextVersionIDMarker = resp.NextVersionIDMarker
	data.VersionIDMarker = versionIDMarker
	data.IsTruncated = resp.IsTruncated

	prefixes := make([]CommonPrefix, 0, len(resp.Prefixes))
	for _, prefix := range resp.Prefixes {
		prefixItem := CommonPrefix{}
		prefixItem.Prefix = s3EncodeName(prefix, encodingType)
		prefixes = append(prefixes, prefixItem)
	}
	data.CommonPrefixes = prefixes
	return data
}

// generates an ListObjectsV1 response for the said bucket with other enumerated options.
func generateListObjectsV1Response(ctx context.Context, bucket, prefix, marker, delimiter, encodingType string, maxKeys int, resp ListObjectsInfo) ListObjectsResponse {
	contents := make([]Object, 0, len(resp.Objects))
	owner := &Owner{
		ID:          globalMinioDefaultOwnerID,
		DisplayName: "minio",
	}
	data := ListObjectsResponse{}

	for _, object := range resp.Objects {
		content := Object{}
		if object.Name == "" {
			continue
		}
		content.Key = s3EncodeName(object.Name, encodingType)
		content.LastModified = amztime.ISO8601Format(object.ModTime.UTC())
		if object.ETag != "" {
			content.ETag = "\"" + object.ETag + "\""
		}
		content.Size = object.Size
		if object.StorageClass != "" {
			content.StorageClass = filterStorageClass(ctx, object.StorageClass)
		} else {
			content.StorageClass = globalMinioDefaultStorageClass
		}
		content.Owner = owner
		contents = append(contents, content)
	}
	data.Name = bucket
	data.Contents = contents

	data.EncodingType = encodingType
	data.Prefix = s3EncodeName(prefix, encodingType)
	data.Marker = s3EncodeName(marker, encodingType)
	data.Delimiter = s3EncodeName(delimiter, encodingType)
	data.MaxKeys = maxKeys
	data.NextMarker = s3EncodeName(resp.NextMarker, encodingType)
	data.IsTruncated = resp.IsTruncated

	prefixes := make([]CommonPrefix, 0, len(resp.Prefixes))
	for _, prefix := range resp.Prefixes {
		prefixItem := CommonPrefix{}
		prefixItem.Prefix = s3EncodeName(prefix, encodingType)
		prefixes = append(prefixes, prefixItem)
	}
	data.CommonPrefixes = prefixes
	return data
}

// generates an ListObjectsV2 response for the said bucket with other enumerated options.
func generateListObjectsV2Response(ctx context.Context, bucket, prefix, token, nextToken, startAfter, delimiter, encodingType string, fetchOwner, isTruncated bool, maxKeys int, objects []ObjectInfo, prefixes []string, metadata metaCheckFn) ListObjectsV2Response {
	contents := make([]Object, 0, len(objects))
	var owner *Owner
	if fetchOwner {
		owner = &Owner{
			ID:          globalMinioDefaultOwnerID,
			DisplayName: "minio",
		}
	}

	data := ListObjectsV2Response{}

	for _, object := range objects {
		content := Object{}
		if object.Name == "" {
			continue
		}
		content.Key = s3EncodeName(object.Name, encodingType)
		content.LastModified = amztime.ISO8601Format(object.ModTime.UTC())
		if object.ETag != "" {
			content.ETag = "\"" + object.ETag + "\""
		}
		content.Size = object.Size
		if object.StorageClass != "" {
			content.StorageClass = filterStorageClass(ctx, object.StorageClass)
		} else {
			content.StorageClass = globalMinioDefaultStorageClass
		}
		content.Owner = owner
		if metadata != nil {
			if metadata(object.Name, policy.GetObjectTaggingAction) == ErrNone {
				content.UserTags = object.UserTags
			}
			if metadata(object.Name, policy.GetObjectAction) == ErrNone {
				content.UserMetadata = &Metadata{}
				switch kind, _ := crypto.IsEncrypted(object.UserDefined); kind {
				case crypto.S3:
					content.UserMetadata.Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionAES)
				case crypto.S3KMS:
					content.UserMetadata.Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionKMS)
				case crypto.SSEC:
					content.UserMetadata.Set(xhttp.AmzServerSideEncryptionCustomerAlgorithm, xhttp.AmzEncryptionAES)
				}
				for k, v := range cleanReservedKeys(object.UserDefined) {
					content.UserMetadata.Set(k, v)
				}
				content.Internal = &ObjectInternalInfo{
					K: object.DataBlocks,
					M: object.ParityBlocks,
				}
			}
		}
		contents = append(contents, content)
	}
	data.Name = bucket
	data.Contents = contents

	data.EncodingType = encodingType
	data.StartAfter = s3EncodeName(startAfter, encodingType)
	data.Delimiter = s3EncodeName(delimiter, encodingType)
	data.Prefix = s3EncodeName(prefix, encodingType)
	data.MaxKeys = maxKeys
	data.ContinuationToken = base64.StdEncoding.EncodeToString([]byte(token))
	data.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(nextToken))
	data.IsTruncated = isTruncated

	commonPrefixes := make([]CommonPrefix, 0, len(prefixes))
	for _, prefix := range prefixes {
		prefixItem := CommonPrefix{}
		prefixItem.Prefix = s3EncodeName(prefix, encodingType)
		commonPrefixes = append(commonPrefixes, prefixItem)
	}
	data.CommonPrefixes = commonPrefixes
	data.KeyCount = len(data.Contents) + len(data.CommonPrefixes)
	return data
}

type metaCheckFn = func(name string, action policy.Action) (s3Err APIErrorCode)

// generates CopyObjectResponse from etag and lastModified time.
func generateCopyObjectResponse(etag string, lastModified time.Time) CopyObjectResponse {
	return CopyObjectResponse{
		ETag:         "\"" + etag + "\"",
		LastModified: amztime.ISO8601Format(lastModified.UTC()),
	}
}

// generates CopyObjectPartResponse from etag and lastModified time.
func generateCopyObjectPartResponse(etag string, lastModified time.Time) CopyObjectPartResponse {
	return CopyObjectPartResponse{
		ETag:         "\"" + etag + "\"",
		LastModified: amztime.ISO8601Format(lastModified.UTC()),
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
func generateCompleteMultipartUploadResponse(bucket, key, location string, oi ObjectInfo, h http.Header) CompleteMultipartUploadResponse {
	cs, _ := oi.decryptChecksums(0, h)
	c := CompleteMultipartUploadResponse{
		Location: location,
		Bucket:   bucket,
		Key:      key,
		// AWS S3 quotes the ETag in XML, make sure we are compatible here.
		ETag:              "\"" + oi.ETag + "\"",
		ChecksumSHA1:      cs[hash.ChecksumSHA1.String()],
		ChecksumSHA256:    cs[hash.ChecksumSHA256.String()],
		ChecksumCRC32:     cs[hash.ChecksumCRC32.String()],
		ChecksumCRC32C:    cs[hash.ChecksumCRC32C.String()],
		ChecksumCRC64NVME: cs[hash.ChecksumCRC64NVME.String()],
	}
	return c
}

// generates ListPartsResponse from ListPartsInfo.
func generateListPartsResponse(partsInfo ListPartsInfo, encodingType string) ListPartsResponse {
	listPartsResponse := ListPartsResponse{}
	listPartsResponse.Bucket = partsInfo.Bucket
	listPartsResponse.Key = s3EncodeName(partsInfo.Object, encodingType)
	listPartsResponse.UploadID = partsInfo.UploadID
	listPartsResponse.StorageClass = globalMinioDefaultStorageClass

	// Dumb values not meaningful
	listPartsResponse.Initiator = Initiator{
		ID:          globalMinioDefaultOwnerID,
		DisplayName: globalMinioDefaultOwnerID,
	}
	listPartsResponse.Owner = Owner{
		ID:          globalMinioDefaultOwnerID,
		DisplayName: globalMinioDefaultOwnerID,
	}

	listPartsResponse.MaxParts = partsInfo.MaxParts
	listPartsResponse.PartNumberMarker = partsInfo.PartNumberMarker
	listPartsResponse.IsTruncated = partsInfo.IsTruncated
	listPartsResponse.NextPartNumberMarker = partsInfo.NextPartNumberMarker
	listPartsResponse.ChecksumAlgorithm = partsInfo.ChecksumAlgorithm
	listPartsResponse.ChecksumType = partsInfo.ChecksumType

	listPartsResponse.Parts = make([]Part, len(partsInfo.Parts))
	for index, part := range partsInfo.Parts {
		newPart := Part{}
		newPart.PartNumber = part.PartNumber
		newPart.ETag = "\"" + part.ETag + "\""
		newPart.Size = part.Size
		newPart.LastModified = amztime.ISO8601Format(part.LastModified.UTC())
		newPart.ChecksumCRC32 = part.ChecksumCRC32
		newPart.ChecksumCRC32C = part.ChecksumCRC32C
		newPart.ChecksumSHA1 = part.ChecksumSHA1
		newPart.ChecksumSHA256 = part.ChecksumSHA256
		newPart.ChecksumCRC64NVME = part.ChecksumCRC64NVME
		listPartsResponse.Parts[index] = newPart
	}
	return listPartsResponse
}

// generates ListMultipartUploadsResponse for given bucket and ListMultipartsInfo.
func generateListMultipartUploadsResponse(bucket string, multipartsInfo ListMultipartsInfo, encodingType string) ListMultipartUploadsResponse {
	listMultipartUploadsResponse := ListMultipartUploadsResponse{}
	listMultipartUploadsResponse.Bucket = bucket
	listMultipartUploadsResponse.Delimiter = s3EncodeName(multipartsInfo.Delimiter, encodingType)
	listMultipartUploadsResponse.IsTruncated = multipartsInfo.IsTruncated
	listMultipartUploadsResponse.EncodingType = encodingType
	listMultipartUploadsResponse.Prefix = s3EncodeName(multipartsInfo.Prefix, encodingType)
	listMultipartUploadsResponse.KeyMarker = s3EncodeName(multipartsInfo.KeyMarker, encodingType)
	listMultipartUploadsResponse.NextKeyMarker = s3EncodeName(multipartsInfo.NextKeyMarker, encodingType)
	listMultipartUploadsResponse.MaxUploads = multipartsInfo.MaxUploads
	listMultipartUploadsResponse.NextUploadIDMarker = multipartsInfo.NextUploadIDMarker
	listMultipartUploadsResponse.UploadIDMarker = multipartsInfo.UploadIDMarker
	listMultipartUploadsResponse.CommonPrefixes = make([]CommonPrefix, len(multipartsInfo.CommonPrefixes))
	for index, commonPrefix := range multipartsInfo.CommonPrefixes {
		listMultipartUploadsResponse.CommonPrefixes[index] = CommonPrefix{
			Prefix: s3EncodeName(commonPrefix, encodingType),
		}
	}
	listMultipartUploadsResponse.Uploads = make([]Upload, len(multipartsInfo.Uploads))
	for index, upload := range multipartsInfo.Uploads {
		newUpload := Upload{}
		newUpload.UploadID = upload.UploadID
		newUpload.Key = s3EncodeName(upload.Object, encodingType)
		newUpload.Initiated = amztime.ISO8601Format(upload.Initiated.UTC())
		listMultipartUploadsResponse.Uploads[index] = newUpload
	}
	return listMultipartUploadsResponse
}

// generate multi objects delete response.
func generateMultiDeleteResponse(quiet bool, deletedObjects []DeletedObject, errs []DeleteError) DeleteObjectsResponse {
	deleteResp := DeleteObjectsResponse{}
	if !quiet {
		deleteResp.DeletedObjects = deletedObjects
	}
	deleteResp.Errors = errs
	return deleteResp
}

func writeResponse(w http.ResponseWriter, statusCode int, response []byte, mType mimeType) {
	if statusCode == 0 {
		statusCode = 200
	}
	// Similar check to http.checkWriteHeaderCode
	if statusCode < 100 || statusCode > 999 {
		bugLogIf(context.Background(), fmt.Errorf("invalid WriteHeader code %v", statusCode))
		statusCode = http.StatusInternalServerError
	}
	setCommonHeaders(w)
	if mType != mimeNone {
		w.Header().Set(xhttp.ContentType, string(mType))
	}
	w.Header().Set(xhttp.ContentLength, strconv.Itoa(len(response)))
	w.WriteHeader(statusCode)
	if response != nil {
		w.Write(response)
	}
}

// mimeType represents various MIME type used API responses.
type mimeType string

const (
	// Means no response type.
	mimeNone mimeType = ""
	// Means response type is JSON.
	mimeJSON mimeType = "application/json"
	// Means response type is XML.
	mimeXML mimeType = "application/xml"
)

// writeSuccessResponseJSON writes success headers and response if any,
// with content-type set to `application/json`.
func writeSuccessResponseJSON(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, mimeJSON)
}

// writeSuccessResponseXML writes success headers and response if any,
// with content-type set to `application/xml`.
func writeSuccessResponseXML(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, mimeXML)
}

// writeSuccessNoContent writes success headers with http status 204
func writeSuccessNoContent(w http.ResponseWriter) {
	writeResponse(w, http.StatusNoContent, nil, mimeNone)
}

// writeRedirectSeeOther writes Location header with http status 303
func writeRedirectSeeOther(w http.ResponseWriter, location string) {
	w.Header().Set(xhttp.Location, location)
	writeResponse(w, http.StatusSeeOther, nil, mimeNone)
}

func writeSuccessResponseHeadersOnly(w http.ResponseWriter) {
	writeResponse(w, http.StatusOK, nil, mimeNone)
}

// writeErrorResponse writes error headers
func writeErrorResponse(ctx context.Context, w http.ResponseWriter, err APIError, reqURL *url.URL) {
	switch err.HTTPStatusCode {
	case http.StatusServiceUnavailable, http.StatusTooManyRequests:
		// Set retry-after header to indicate user-agents to retry request after 60 seconds.
		// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
		w.Header().Set(xhttp.RetryAfter, "60")
	}

	switch err.Code {
	case "InvalidRegion":
		err.Description = fmt.Sprintf("Region does not match; expecting '%s'.", globalSite.Region())
	case "AuthorizationHeaderMalformed":
		err.Description = fmt.Sprintf("The authorization header is malformed; the region is wrong; expecting '%s'.", globalSite.Region())
	}

	// Similar check to http.checkWriteHeaderCode
	if err.HTTPStatusCode < 100 || err.HTTPStatusCode > 999 {
		bugLogIf(ctx, fmt.Errorf("invalid WriteHeader code %v from %v", err.HTTPStatusCode, err.Code))
		err.HTTPStatusCode = http.StatusInternalServerError
	}

	// Generate error response.
	errorResponse := getAPIErrorResponse(ctx, err, reqURL.Path,
		w.Header().Get(xhttp.AmzRequestID), w.Header().Get(xhttp.AmzRequestHostID))
	encodedErrorResponse := encodeResponse(errorResponse)
	writeResponse(w, err.HTTPStatusCode, encodedErrorResponse, mimeXML)
}

func writeErrorResponseHeadersOnly(w http.ResponseWriter, err APIError) {
	w.Header().Set(xMinIOErrCodeHeader, err.Code)
	w.Header().Set(xMinIOErrDescHeader, "\""+err.Description+"\"")
	writeResponse(w, err.HTTPStatusCode, nil, mimeNone)
}

func writeErrorResponseString(ctx context.Context, w http.ResponseWriter, err APIError, reqURL *url.URL) {
	// Generate string error response.
	writeResponse(w, err.HTTPStatusCode, []byte(err.Description), mimeNone)
}

// writeErrorResponseJSON - writes error response in JSON format;
// useful for admin APIs.
func writeErrorResponseJSON(ctx context.Context, w http.ResponseWriter, err APIError, reqURL *url.URL) {
	// Generate error response.
	errorResponse := getAPIErrorResponse(ctx, err, reqURL.Path, w.Header().Get(xhttp.AmzRequestID), w.Header().Get(xhttp.AmzRequestHostID))
	encodedErrorResponse := encodeResponseJSON(errorResponse)
	writeResponse(w, err.HTTPStatusCode, encodedErrorResponse, mimeJSON)
}

// writeCustomErrorResponseJSON - similar to writeErrorResponseJSON,
// but accepts the error message directly (this allows messages to be
// dynamically generated.)
func writeCustomErrorResponseJSON(ctx context.Context, w http.ResponseWriter, err APIError,
	errBody string, reqURL *url.URL,
) {
	reqInfo := logger.GetReqInfo(ctx)
	errorResponse := APIErrorResponse{
		Code:       err.Code,
		Message:    errBody,
		Resource:   reqURL.Path,
		BucketName: reqInfo.BucketName,
		Key:        reqInfo.ObjectName,
		RequestID:  w.Header().Get(xhttp.AmzRequestID),
		HostID:     globalDeploymentID(),
	}
	encodedErrorResponse := encodeResponseJSON(errorResponse)
	writeResponse(w, err.HTTPStatusCode, encodedErrorResponse, mimeJSON)
}
