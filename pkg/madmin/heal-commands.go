/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
 *
 */

package madmin

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

// listBucketHealResult container for listObjects response.
type listBucketHealResult struct {
	// A response can contain CommonPrefixes only if you have
	// specified a delimiter.
	CommonPrefixes []commonPrefix
	// Metadata about each object returned.
	Contents  []ObjectInfo
	Delimiter string

	// Encoding type used to encode object keys in the response.
	EncodingType string

	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool
	Marker      string
	MaxKeys     int64
	Name        string

	// When response is truncated (the IsTruncated element value in
	// the response is true), you can use the key name in this field
	// as marker in the subsequent request to get next set of objects.
	// Object storage lists objects in alphabetical order Note: This
	// element is returned only if you have delimiter request
	// parameter specified. If response does not include the NextMaker
	// and it is truncated, you can use the value of the last Key in
	// the response as the marker in the subsequent request to get the
	// next set of object keys.
	NextMarker string
	Prefix     string
}

// commonPrefix container for prefix response.
type commonPrefix struct {
	Prefix string
}

// Owner - bucket owner/principal
type Owner struct {
	ID          string
	DisplayName string
}

// Bucket container for bucket metadata
type Bucket struct {
	Name         string
	CreationDate string // time string of format "2006-01-02T15:04:05.000Z"

	HealBucketInfo *HealBucketInfo `xml:"HealBucketInfo,omitempty"`
}

// ListBucketsHealResponse - format for list buckets response
type ListBucketsHealResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult" json:"-"`

	Owner Owner

	// Container for one or more buckets.
	Buckets struct {
		Buckets []Bucket `xml:"Bucket"`
	} // Buckets are nested
}

// HealStatus - represents different states of healing an object could be in.
type HealStatus int

const (
	// Healthy - Object that is already healthy
	Healthy HealStatus = iota
	// CanHeal - Object can be healed
	CanHeal
	// Corrupted - Object can't be healed
	Corrupted
	// QuorumUnavailable - Object can't be healed until read
	// quorum is available
	QuorumUnavailable
	// CanPartiallyHeal - Object can't be healed completely until
	// disks with missing parts come online
	CanPartiallyHeal
)

// HealBucketInfo - represents healing related information of a bucket.
type HealBucketInfo struct {
	Status HealStatus
}

// BucketInfo - represents bucket metadata.
type BucketInfo struct {
	// Name of the bucket.
	Name string

	// Date and time when the bucket was created.
	Created time.Time

	// Healing information
	HealBucketInfo *HealBucketInfo `xml:"HealBucketInfo,omitempty"`
}

// HealObjectInfo - represents healing related information of an object.
type HealObjectInfo struct {
	Status             HealStatus
	MissingDataCount   int
	MissingParityCount int
}

// ObjectInfo container for object metadata.
type ObjectInfo struct {
	// An ETag is optionally set to md5sum of an object.  In case of multipart objects,
	// ETag is of the form MD5SUM-N where MD5SUM is md5sum of all individual md5sums of
	// each parts concatenated into one string.
	ETag string `json:"etag"`

	Key          string    `json:"name"`         // Name of the object
	LastModified time.Time `json:"lastModified"` // Date and time the object was last modified.
	Size         int64     `json:"size"`         // Size in bytes of the object.
	ContentType  string    `json:"contentType"`  // A standard MIME type describing the format of the object data.

	// Collection of additional metadata on the object.
	// eg: x-amz-meta-*, content-encoding etc.
	Metadata http.Header `json:"metadata"`

	// Owner name.
	Owner struct {
		DisplayName string `json:"name"`
		ID          string `json:"id"`
	} `json:"owner"`

	// The class of storage used to store the object.
	StorageClass string `json:"storageClass"`

	// Error
	Err            error           `json:"-"`
	HealObjectInfo *HealObjectInfo `json:"healObjectInfo,omitempty"`
}

type healQueryKey string

const (
	healBucket    healQueryKey = "bucket"
	healObject    healQueryKey = "object"
	healPrefix    healQueryKey = "prefix"
	healMarker    healQueryKey = "marker"
	healDelimiter healQueryKey = "delimiter"
	healMaxKey    healQueryKey = "max-key"
	healDryRun    healQueryKey = "dry-run"
)

// mkHealQueryVal - helper function to construct heal REST API query params.
func mkHealQueryVal(bucket, prefix, marker, delimiter, maxKeyStr string) url.Values {
	queryVal := make(url.Values)
	queryVal.Set("heal", "")
	queryVal.Set(string(healBucket), bucket)
	queryVal.Set(string(healPrefix), prefix)
	queryVal.Set(string(healMarker), marker)
	queryVal.Set(string(healDelimiter), delimiter)
	queryVal.Set(string(healMaxKey), maxKeyStr)
	return queryVal
}

// listObjectsHeal - issues heal list API request for a batch of maxKeys objects to be healed.
func (adm *AdminClient) listObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (listBucketHealResult, error) {
	// Construct query params.
	maxKeyStr := fmt.Sprintf("%d", maxKeys)
	queryVal := mkHealQueryVal(bucket, prefix, marker, delimiter, maxKeyStr)

	hdrs := make(http.Header)
	hdrs.Set(minioAdminOpHeader, "list-objects")

	reqData := requestData{
		queryValues:   queryVal,
		customHeaders: hdrs,
	}

	// Empty 'list' of objects to be healed.
	toBeHealedObjects := listBucketHealResult{}

	// Execute GET on /?heal to list objects needing heal.
	resp, err := adm.executeMethod("GET", reqData)

	defer closeResponse(resp)
	if err != nil {
		return listBucketHealResult{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return toBeHealedObjects, httpRespToErrorResponse(resp)

	}

	err = xml.NewDecoder(resp.Body).Decode(&toBeHealedObjects)
	return toBeHealedObjects, err
}

// ListObjectsHeal - Lists upto maxKeys objects that needing heal matching bucket, prefix, marker, delimiter.
func (adm *AdminClient) ListObjectsHeal(bucket, prefix string, recursive bool, doneCh <-chan struct{}) (<-chan ObjectInfo, error) {
	// Allocate new list objects channel.
	objectStatCh := make(chan ObjectInfo, 1)
	// Default listing is delimited at "/"
	delimiter := "/"
	if recursive {
		// If recursive we do not delimit.
		delimiter = ""
	}

	// Initiate list objects goroutine here.
	go func(objectStatCh chan<- ObjectInfo) {
		defer close(objectStatCh)
		// Save marker for next request.
		var marker string
		for {
			// Get list of objects a maximum of 1000 per request.
			result, err := adm.listObjectsHeal(bucket, prefix, marker, delimiter, 1000)
			if err != nil {
				objectStatCh <- ObjectInfo{
					Err: err,
				}
				return
			}

			// If contents are available loop through and send over channel.
			for _, object := range result.Contents {
				// Save the marker.
				marker = object.Key
				select {
				// Send object content.
				case objectStatCh <- object:
				// If receives done from the caller, return here.
				case <-doneCh:
					return
				}
			}

			// Send all common prefixes if any.
			// NOTE: prefixes are only present if the request is delimited.
			for _, obj := range result.CommonPrefixes {
				object := ObjectInfo{}
				object.Key = obj.Prefix
				object.Size = 0
				select {
				// Send object prefixes.
				case objectStatCh <- object:
				// If receives done from the caller, return here.
				case <-doneCh:
					return
				}
			}

			// If next marker present, save it for next request.
			if result.NextMarker != "" {
				marker = result.NextMarker
			}

			// Listing ends result is not truncated, return right here.
			if !result.IsTruncated {
				return
			}
		}
	}(objectStatCh)
	return objectStatCh, nil
}

const timeFormatAMZLong = "2006-01-02T15:04:05.000Z" // Reply date format with nanosecond precision.

// ListBucketsHeal - issues heal bucket list API request
func (adm *AdminClient) ListBucketsHeal() ([]BucketInfo, error) {
	queryVal := url.Values{}
	queryVal.Set("heal", "")

	hdrs := make(http.Header)
	hdrs.Set(minioAdminOpHeader, "list-buckets")

	reqData := requestData{
		queryValues:   queryVal,
		customHeaders: hdrs,
	}

	// Execute GET on /?heal to list objects needing heal.
	resp, err := adm.executeMethod("GET", reqData)

	defer closeResponse(resp)
	if err != nil {
		return []BucketInfo{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return []BucketInfo{}, httpRespToErrorResponse(resp)
	}

	var listBucketsHealResult ListBucketsHealResponse

	err = xml.NewDecoder(resp.Body).Decode(&listBucketsHealResult)
	if err != nil {
		return []BucketInfo{}, err
	}

	var bucketsToBeHealed []BucketInfo

	for _, bucket := range listBucketsHealResult.Buckets.Buckets {
		creationDate, err := time.Parse(timeFormatAMZLong, bucket.CreationDate)
		if err != nil {
			return []BucketInfo{}, err
		}
		bucketsToBeHealed = append(bucketsToBeHealed,
			BucketInfo{
				Name:           bucket.Name,
				Created:        creationDate,
				HealBucketInfo: bucket.HealBucketInfo,
			})
	}

	return bucketsToBeHealed, nil
}

// HealBucket - Heal the given bucket
func (adm *AdminClient) HealBucket(bucket string, dryrun bool) error {
	// Construct query params.
	queryVal := url.Values{}
	queryVal.Set("heal", "")
	queryVal.Set(string(healBucket), bucket)
	if dryrun {
		queryVal.Set(string(healDryRun), "")
	}

	hdrs := make(http.Header)
	hdrs.Set(minioAdminOpHeader, "bucket")

	reqData := requestData{
		queryValues:   queryVal,
		customHeaders: hdrs,
	}

	// Execute POST on /?heal&bucket=mybucket to heal a bucket.
	resp, err := adm.executeMethod("POST", reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

// HealResult - represents result of heal-object admin API.
type HealResult struct {
	State HealState `json:"state"`
}

// HealState - different states of heal operation
type HealState int

const (
	// HealNone - none of the disks healed
	HealNone HealState = iota
	// HealPartial - some disks were healed, others were offline
	HealPartial
	// HealOK - all disks were healed
	HealOK
)

// HealObject - Heal the given object.
func (adm *AdminClient) HealObject(bucket, object string, dryrun bool) (HealResult, error) {
	// Construct query params.
	queryVal := url.Values{}
	queryVal.Set("heal", "")
	queryVal.Set(string(healBucket), bucket)
	queryVal.Set(string(healObject), object)
	if dryrun {
		queryVal.Set(string(healDryRun), "")
	}

	hdrs := make(http.Header)
	hdrs.Set(minioAdminOpHeader, "object")

	reqData := requestData{
		queryValues:   queryVal,
		customHeaders: hdrs,
	}

	// Execute POST on /?heal&bucket=mybucket&object=myobject to heal an object.
	resp, err := adm.executeMethod("POST", reqData)

	defer closeResponse(resp)
	if err != nil {
		return HealResult{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return HealResult{}, httpRespToErrorResponse(resp)
	}

	// Healing is not performed so heal object result is empty.
	if dryrun {
		return HealResult{}, nil
	}

	jsonBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return HealResult{}, err
	}

	healResult := HealResult{}
	err = json.Unmarshal(jsonBytes, &healResult)
	if err != nil {
		return HealResult{}, err
	}

	return healResult, nil
}

// HealFormat - heal storage format on available disks.
func (adm *AdminClient) HealFormat(dryrun bool) error {
	queryVal := url.Values{}
	queryVal.Set("heal", "")
	if dryrun {
		queryVal.Set(string(healDryRun), "")
	}

	// Set x-minio-operation to format.
	hdrs := make(http.Header)
	hdrs.Set(minioAdminOpHeader, "format")

	reqData := requestData{
		queryValues:   queryVal,
		customHeaders: hdrs,
	}

	// Execute POST on /?heal to heal storage format.
	resp, err := adm.executeMethod("POST", reqData)

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}
