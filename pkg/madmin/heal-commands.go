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
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

// HealOpts - collection of options for a heal sequence
type HealOpts struct {
	Recursive bool `json:"recursive"`
	DryRun    bool `json:"dryRun"`
}

// HealStartSuccess - holds information about a successfully started
// heal operation
type HealStartSuccess struct {
	ClientToken   string    `json:"clientToken"`
	ClientAddress string    `json:"clientAddress"`
	StartTime     time.Time `json:"startTime"`
}

// HealTaskStatus - status struct for a heal task
type HealTaskStatus struct {
	Summary       string    `json:"summary"`
	FailureDetail string    `json:"detail"`
	StartTime     time.Time `json:"startTime"`
	HealSettings  HealOpts  `json:"settings"`
	NumDisks      int       `json:"numDisks"`

	Items []HealResultItem `json:"items,omitempty"`
}

// HealItemType - specify the type of heal operation in a healing
// result
type HealItemType string

// HealItemType constants
const (
	HealItemMetadata       HealItemType = "metadata"
	HealItemBucket                      = "bucket"
	HealItemBucketMetadata              = "bucket-metadata"
	HealItemObject                      = "object"
)

// Drive state constants
const (
	DriveStateOk      string = "ok"
	DriveStateOffline        = "offline"
	DriveStateCorrupt        = "corrupt"
	DriveStateMissing        = "missing"
)

// HealResultItem - struct for an individual heal result item
type HealResultItem struct {
	ResultIndex  int64        `json:"resultId"`
	Type         HealItemType `json:"type"`
	Bucket       string       `json:"bucket"`
	Object       string       `json:"object"`
	Detail       string       `json:"detail"`
	ParityBlocks int          `json:"parityBlocks,omitempty"`
	DataBlocks   int          `json:"dataBlocks,omitempty"`
	DiskCount    int          `json:"diskCount"`
	DriveInfo    struct {
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
		if v == DriveStateOk {
			b++
		}
	}
	for _, v := range hri.DriveInfo.After {
		if v == DriveStateOk {
			a++
		}
	}
	return
}

// Heal - API endpoint to start heal and to fetch status
func (adm *AdminClient) Heal(bucket, prefix string, healOpts HealOpts,
	clientToken string, forceStart bool) (
	healStart HealStartSuccess, healTaskStatus HealTaskStatus, err error) {

	body, err := json.Marshal(healOpts)
	if err != nil {
		return healStart, healTaskStatus, err
	}

	path := fmt.Sprintf("/v1/heal/%s", bucket)
	if bucket != "" && prefix != "" {
		path += "/" + prefix
	}

	// execute POST request to heal api
	queryVals := make(url.Values)
	var contentBody io.Reader
	if clientToken != "" {
		queryVals.Set("clientToken", clientToken)
		body = []byte{}
	} else {
		// Set a body only if clientToken is not given
		contentBody = bytes.NewReader(body)
	}
	if forceStart {
		queryVals.Set("forceStart", "true")
	}

	resp, err := adm.executeMethod("POST", requestData{
		relPath:            path,
		contentBody:        contentBody,
		contentSHA256Bytes: sum256(body),
		queryValues:        queryVals,
	})
	defer closeResponse(resp)
	if err != nil {
		return healStart, healTaskStatus, err
	}

	if resp.StatusCode != http.StatusOK {
		return healStart, healTaskStatus, httpRespToErrorResponse(resp)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return healStart, healTaskStatus, err
	}

	// Was it a status request?
	if clientToken == "" {
		err = json.Unmarshal(respBytes, &healStart)
	} else {
		err = json.Unmarshal(respBytes, &healTaskStatus)
	}
	return healStart, healTaskStatus, err
}
