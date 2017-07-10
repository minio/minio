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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

// HealOpts - collection of options for a heal sequence
type HealOpts struct {
	Recursive      bool `json:"recursive"`
	DryRun         bool `json:"dryRun"`
	Incomplete     bool `json:"incomplete"`
	StatisticsOnly bool `json:"statisticsOnly"`
	// RemoveBadFiles bool `json:"removeBadFiles"`
}

// HealStart - starts a heal task with given parameters
func (adm *AdminClient) HealStart(bucket, prefix string,
	healOpts HealOpts) (
	healPath string, err error) {

	body, err := json.Marshal(healOpts)
	if err != nil {
		return healPath, err
	}

	path := fmt.Sprintf("/v1/heal/%s", bucket)
	if bucket != "" && prefix != "" {
		path += "/" + prefix
	}

	// execute POST request to heal api
	resp, err := adm.executeMethod("POST", requestData{
		relPath:            path,
		contentBody:        bytes.NewReader(body),
		contentSHA256Bytes: sum256(body),
	})
	defer closeResponse(resp)
	if err != nil {
		return healPath, err
	}

	if resp.StatusCode != http.StatusOK {
		return healPath, httpRespToErrorResponse(resp)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return healPath, err
	}

	respMap := make(map[string]string)
	err = json.Unmarshal(respBytes, &respMap)
	if err != nil {
		return healPath, err
	}

	return respMap["healPathId"], nil
}

// HealStop - stop a heal task
func (adm *AdminClient) HealStop(bucket, prefix string) error {

	path := "/v1/heal/" + bucket
	if bucket != "" && prefix != "" {
		path += "/" + prefix
	}

	// execute DELETE request to heal api
	resp, err := adm.executeMethod("DELETE", requestData{
		relPath: path,
	})

	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}
	return nil
}

// HealStatistics - heal sequence summary stats
type HealStatistics struct {
	// Specifies if drive metadata was healed
	DriveMetadataHealed bool `json:"drive_metadata_healed"`

	NumBucketsScanned int64 `json:"buckets_scanned"`

	// Number of buckets which were healed on at least 1 disk
	NumBucketsHealed int64 `json:"buckets_healed"`

	// total bytes of object data scanned (accumulates object
	// size, not part file sizes)
	ObjectBytesScanned int64 `json:"object_bytes_scanned"`

	NumObjectsScanned int64 `json:"objects_scanned"`

	// Number of objects healed on at least 1 disk
	NumObjectsHealed int64 `json:"objects_healed"`

	// Map of number of online drives to number of objects with
	// that many online drives.
	ObjectsByOnlineDrives map[int]int64 `json:"objects_by_online_drives"`

	NumUploadsScanned int64 `json:"multipart_uploads_scanned"`

	// Number of multipart uploads healed on at least 1 disk
	NumUploadsHealed int64 `json:"multipart_uploads_healed"`
}

// NewHealStatistics - returns an initialized statistics object
func NewHealStatistics() *HealStatistics {
	return &HealStatistics{
		ObjectsByOnlineDrives: make(map[int]int64),
	}
}

// UpdateHealStats - update statistics based on given heal result
func (s *HealStatistics) UpdateHealStats(numDisks int, r HealResultItem) error {
	onlineBefore, onlineAfter := r.GetOnlineCounts()
	switch {
	case r.Type == HealItemMetadata && r.Detail == "disk-format":
		s.DriveMetadataHealed = true

	case r.Type == HealItemBucket:
		s.NumBucketsScanned++
		if onlineAfter > onlineBefore {
			s.NumBucketsHealed++
		}

	case r.Type == HealItemMultipartUpload:
		s.NumUploadsScanned++
		if onlineAfter > onlineBefore {
			s.NumUploadsHealed++
		}

	case r.Type == HealItemObject || r.Type == HealItemBucketMetadata:
		s.NumObjectsScanned++
		if onlineAfter > onlineBefore {
			s.NumObjectsHealed++
		}
		s.ObjectsByOnlineDrives[onlineAfter]++

		// accumulate object size
		s.ObjectBytesScanned += r.ObjectSize

	default:
		return fmt.Errorf(
			"Unknown heal result type (this is a bug): %#v", r)
	}
	return nil
}

// HealTaskStatus - status struct for a heal task
type HealTaskStatus struct {
	Summary        string    `json:"Summary"`
	FailureDetail  string    `json:"Detail"`
	StartTime      time.Time `json:"StartTime"`
	HealSettings   HealOpts  `json:"Settings"`
	NumDisks       int       `json:"NumDisks"`
	NumParityDisks int       `json:"NumParityDisks"`

	Items []HealResultItem `json:"Items,omitempty"`

	Statistics HealStatistics `json:"HealStatistics"`
}

// HealItemType - specify the type of heal operation in a healing
// result
type HealItemType string

// HealItemType constants
const (
	HealItemMetadata        HealItemType = "metadata"
	HealItemBucket                       = "bucket"
	HealItemBucketMetadata               = "bucket-metadata"
	HealItemObject                       = "object"
	HealItemMultipartUpload              = "multipart-upload"
)

// Drive state constants
const (
	OnlineDriveState  string = "online"
	OfflineDriveState        = "offline"
	CorruptDriveState        = "corrupt"
)

// HealResultItem - struct for an individual heal result item
type HealResultItem struct {
	ResultIndex int64        `json:"resultId"`
	Type        HealItemType `json:"type"`
	Bucket      string       `json:"bucket"`
	Object      string       `json:"object"`
	Detail      string       `json:"detail"`
	DriveInfo   struct {
		// below maps are from drive endpoint to drive state
		Before map[string]string `json:"before"`
		After  map[string]string `json:"after"`
	} `json:"drives"`
	ObjectSize int64 `json:"objectSize"`
}

// InitDrives - initialize maps used to represent drive info
func (hri *HealResultItem) InitDrives() {
	hri.DriveInfo.Before = make(map[string]string)
	hri.DriveInfo.After = make(map[string]string)
}

// GetOnlineCounts - returns the number of online disks before and
// after heal
func (hri *HealResultItem) GetOnlineCounts() (b, a int) {
	if hri == nil {
		return
	}
	for _, v := range hri.DriveInfo.Before {
		if v == OnlineDriveState {
			b++
		}
	}
	for _, v := range hri.DriveInfo.After {
		if v == OnlineDriveState {
			a++
		}
	}
	return
}

// HealStatus - Fetch status of a heal sequence
func (adm *AdminClient) HealStatus(bucket, prefix string, marker int64) (
	res HealTaskStatus, err error) {

	path := "/v1/heal/" + bucket
	if bucket != "" && prefix != "" {
		path += "/" + prefix
	}

	queryVal := make(url.Values)
	if marker != -1 {
		queryVal.Set("marker", fmt.Sprintf("%d", marker))
	}
	// execute POST request to heal api
	resp, err := adm.executeMethod("GET", requestData{
		relPath:     path,
		queryValues: queryVal,
	})

	defer closeResponse(resp)
	if err != nil {
		return res, err
	}

	if resp.StatusCode != http.StatusOK {
		return res, httpRespToErrorResponse(resp)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return res, err
	}

	err = json.Unmarshal(respBytes, &res)
	return res, err
}
