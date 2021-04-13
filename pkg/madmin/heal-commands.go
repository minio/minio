/*
 * MinIO Cloud Storage, (C) 2017-2020 MinIO, Inc.
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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"time"
)

// HealScanMode represents the type of healing scan
type HealScanMode int

const (
	// HealUnknownScan default is unknown
	HealUnknownScan HealScanMode = iota

	// HealNormalScan checks if parts are present and not outdated
	HealNormalScan

	// HealDeepScan checks for parts bitrot checksums
	HealDeepScan
)

// HealOpts - collection of options for a heal sequence
type HealOpts struct {
	Recursive bool         `json:"recursive"`
	DryRun    bool         `json:"dryRun"`
	Remove    bool         `json:"remove"`
	Recreate  bool         `json:"recreate"` // only used when bucket needs to be healed
	ScanMode  HealScanMode `json:"scanMode"`
}

// Equal returns true if no is same as o.
func (o HealOpts) Equal(no HealOpts) bool {
	if o.Recursive != no.Recursive {
		return false
	}
	if o.DryRun != no.DryRun {
		return false
	}
	if o.Remove != no.Remove {
		return false
	}
	return o.ScanMode == no.ScanMode
}

// HealStartSuccess - holds information about a successfully started
// heal operation
type HealStartSuccess struct {
	StartTime     time.Time `json:"startTime"`
	ClientToken   string    `json:"clientToken"`
	ClientAddress string    `json:"clientAddress"`
}

// HealStopSuccess - holds information about a successfully stopped
// heal operation.
type HealStopSuccess HealStartSuccess

// HealTaskStatus - status struct for a heal task
type HealTaskStatus struct {
	StartTime     time.Time        `json:"startTime"`
	Summary       string           `json:"summary"`
	FailureDetail string           `json:"detail"`
	Items         []HealResultItem `json:"items,omitempty"`
	HealSettings  HealOpts         `json:"settings"`
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
	DriveStateOk          string = "ok"
	DriveStateOffline            = "offline"
	DriveStateCorrupt            = "corrupt"
	DriveStateMissing            = "missing"
	DriveStatePermission         = "permission-denied"
	DriveStateFaulty             = "faulty"
	DriveStateUnknown            = "unknown"
	DriveStateUnformatted        = "unformatted" // only returned by disk
)

// HealDriveInfo - struct for an individual drive info item.
type HealDriveInfo struct {
	UUID     string `json:"uuid"`
	Endpoint string `json:"endpoint"`
	State    string `json:"state"`
}

// HealResultItem - struct for an individual heal result item
type HealResultItem struct {
	Type      HealItemType `json:"type"`
	Bucket    string       `json:"bucket"`
	Object    string       `json:"object"`
	VersionID string       `json:"versionId"`
	Detail    string       `json:"detail"`
	Before    struct {
		Drives []HealDriveInfo `json:"drives"` // below slices are from drive info.

	} `json:"before"`
	After struct {
		Drives []HealDriveInfo `json:"drives"`
	} `json:"after"`
	ResultIndex  int64 `json:"resultId"`
	DiskCount    int   `json:"diskCount"`
	SetCount     int   `json:"setCount"`
	DataBlocks   int   `json:"dataBlocks,omitempty"`
	ParityBlocks int   `json:"parityBlocks,omitempty"`
	ObjectSize   int64 `json:"objectSize"`
}

// GetMissingCounts - returns the number of missing disks before
// and after heal
func (hri *HealResultItem) GetMissingCounts() (b, a int) {
	if hri == nil {
		return
	}
	for _, v := range hri.Before.Drives {
		if v.State == DriveStateMissing {
			b++
		}
	}
	for _, v := range hri.After.Drives {
		if v.State == DriveStateMissing {
			a++
		}
	}
	return
}

// GetOfflineCounts - returns the number of offline disks before
// and after heal
func (hri *HealResultItem) GetOfflineCounts() (b, a int) {
	if hri == nil {
		return
	}
	for _, v := range hri.Before.Drives {
		if v.State == DriveStateOffline {
			b++
		}
	}
	for _, v := range hri.After.Drives {
		if v.State == DriveStateOffline {
			a++
		}
	}
	return
}

// GetCorruptedCounts - returns the number of corrupted disks before
// and after heal
func (hri *HealResultItem) GetCorruptedCounts() (b, a int) {
	if hri == nil {
		return
	}
	for _, v := range hri.Before.Drives {
		if v.State == DriveStateCorrupt {
			b++
		}
	}
	for _, v := range hri.After.Drives {
		if v.State == DriveStateCorrupt {
			a++
		}
	}
	return
}

// GetOnlineCounts - returns the number of online disks before
// and after heal
func (hri *HealResultItem) GetOnlineCounts() (b, a int) {
	if hri == nil {
		return
	}
	for _, v := range hri.Before.Drives {
		if v.State == DriveStateOk {
			b++
		}
	}
	for _, v := range hri.After.Drives {
		if v.State == DriveStateOk {
			a++
		}
	}
	return
}

// Heal - API endpoint to start heal and to fetch status
// forceStart and forceStop are mutually exclusive, you can either
// set one of them to 'true'. If both are set 'forceStart' will be
// honored.
func (adm *AdminClient) Heal(ctx context.Context, bucket, prefix string,
	healOpts HealOpts, clientToken string, forceStart, forceStop bool) (
	healStart HealStartSuccess, healTaskStatus HealTaskStatus, err error) {

	if forceStart && forceStop {
		return healStart, healTaskStatus, ErrInvalidArgument("forceStart and forceStop set to true is not allowed")
	}

	body, err := json.Marshal(healOpts)
	if err != nil {
		return healStart, healTaskStatus, err
	}

	path := fmt.Sprintf(adminAPIPrefix+"/heal/%s", bucket)
	if bucket != "" && prefix != "" {
		path += "/" + prefix
	}

	// execute POST request to heal api
	queryVals := make(url.Values)
	if clientToken != "" {
		queryVals.Set("clientToken", clientToken)
		body = []byte{}
	}

	// Anyone can be set, either force start or forceStop.
	if forceStart {
		queryVals.Set("forceStart", "true")
	} else if forceStop {
		queryVals.Set("forceStop", "true")
	}

	resp, err := adm.executeMethod(ctx,
		http.MethodPost, requestData{
			relPath:     path,
			content:     body,
			queryValues: queryVals,
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
		// As a special operation forceStop would return a
		// similar struct as healStart will have the
		// heal sequence information about the heal which
		// was stopped.
		err = json.Unmarshal(respBytes, &healStart)
	} else {
		err = json.Unmarshal(respBytes, &healTaskStatus)
	}
	if err != nil {
		// May be the server responded with error after success
		// message, handle it separately here.
		var errResp ErrorResponse
		err = json.Unmarshal(respBytes, &errResp)
		if err != nil {
			// Unknown structure return error anyways.
			return healStart, healTaskStatus, err
		}
		return healStart, healTaskStatus, errResp
	}
	return healStart, healTaskStatus, nil
}

// BgHealState represents the status of the background heal
type BgHealState struct {
	HealDisks []string
	Sets      []SetStatus `json:"sets"` // SetStatus contains information for each set.

	ScannedItemsCount int64
}

// SetStatus contains information about the heal status of a set.
type SetStatus struct {
	ID           string `json:"id"`
	HealStatus   string `json:"heal_status"`
	HealPriority string `json:"heal_priority"`
	Disks        []Disk `json:"disks"`
	PoolIndex    int    `json:"pool_index"`
	SetIndex     int    `json:"set_index"`
}

// HealingDisk contains information about
type HealingDisk struct {
	Started       time.Time `json:"started"`
	LastUpdate    time.Time `json:"last_update"`
	Object        string    `json:"current_object"`
	Bucket        string    `json:"current_bucket"`
	Endpoint      string    `json:"endpoint"`
	Path          string    `json:"path"`
	ID            string    `json:"id"`
	QueuedBuckets []string  `json:"queued_buckets"` // Last object scanned.
	// Filled on startup/restarts.

	HealedBuckets []string `json:"healed_buckets"` // Filled during heal.

	ObjectsFailed uint64 `json:"objects_failed"`
	BytesDone     uint64 `json:"bytes_done"`
	BytesFailed   uint64 `json:"bytes_failed"`
	DiskIndex     int    `json:"disk_index"`
	SetIndex      int    `json:"set_index"`
	PoolIndex     int    `json:"pool_index"`
	ObjectsHealed uint64 `json:"objects_healed"`
}

// Merge others into b.
func (b *BgHealState) Merge(others ...BgHealState) {
	for _, other := range others {
		b.ScannedItemsCount += other.ScannedItemsCount
		if len(b.Sets) == 0 {
			b.Sets = make([]SetStatus, len(other.Sets))
			copy(b.Sets, other.Sets)
			continue
		}

		// Add disk if not present.
		// If present select the one with latest lastupdate.
		addSet := func(set SetStatus) {
			for eSetIdx, existing := range b.Sets {
				if existing.ID != set.ID {
					continue
				}
				if len(existing.Disks) < len(set.Disks) {
					b.Sets[eSetIdx].Disks = set.Disks
				}
				if len(existing.Disks) < len(set.Disks) {
					return
				}
				for i, disk := range set.Disks {
					// Disks should be the same.
					if disk.HealInfo != nil {
						existing.Disks[i].HealInfo = disk.HealInfo
					}
				}
				return
			}
			b.Sets = append(b.Sets, set)
		}
		for _, disk := range other.Sets {
			addSet(disk)
		}
	}
	sort.Slice(b.Sets, func(i, j int) bool {
		if b.Sets[i].PoolIndex != b.Sets[j].PoolIndex {
			return b.Sets[i].PoolIndex < b.Sets[j].PoolIndex
		}
		return b.Sets[i].SetIndex < b.Sets[j].SetIndex
	})
}

// BackgroundHealStatus returns the background heal status of the
// current server or cluster.
func (adm *AdminClient) BackgroundHealStatus(ctx context.Context) (BgHealState, error) {
	// Execute POST request to background heal status api
	resp, err := adm.executeMethod(ctx,
		http.MethodPost,
		requestData{relPath: adminAPIPrefix + "/background-heal/status"})
	if err != nil {
		return BgHealState{}, err
	}
	defer closeResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return BgHealState{}, httpRespToErrorResponse(resp)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return BgHealState{}, err
	}

	var healState BgHealState

	err = json.Unmarshal(respBytes, &healState)
	if err != nil {
		return BgHealState{}, err
	}
	return healState, nil
}
