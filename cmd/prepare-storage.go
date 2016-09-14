/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"time"

	"github.com/minio/minio-go/pkg/set"
)

// Channel where minioctl heal handler would notify if it were successful. This
// would be used by waitForFormattingDisks routine to check if it's worth
// retrying loadAllFormats.
var globalWakeupCh chan struct{}

func init() {
	globalWakeupCh = make(chan struct{}, 1)
}

/*

  Following table lists different possible states the backend could be in.

  * In a single-node, multi-disk setup, "Online" would refer to disks' status.

  * In a multi-node setup, it could refer to disks' or network connectivity
  between the nodes, or both.

  +----------+--------------------------+-----------------------+
  | Online   | Format status  		| Course of action      |
  |          | 		             	|                   	|
  -----------+--------------------------+-----------------------+
  | All      | All Formatted	 	|  			|
  +----------+--------------------------+  initObjectLayer 	|
  | Quorum   | Quorum Formatted    	|                   	|
  +----------+--------------------------+-----------------------+
  | All      | Quorum     		|  Print message saying |
  |	     | Formatted,   		|  "Heal via minioctl"  |
  |	     | some unformatted		|  and initObjectLayer  |
  +----------+--------------------------+-----------------------+
  | All      | None Formatted           |  FormatDisks          |
  |	     |				|  and initObjectLayer  |
  |          | 		             	|                   	|
  +----------+--------------------------+-----------------------+
  |	     |      			|  Wait for notify from |
  | Quorum   | 				|  "Heal via minioctl"  |
  |          | Quorum UnFormatted   	|                   	|
  +----------+--------------------------+-----------------------+
  | No       |          		|  Wait till enough     |
  | Quorum   |          _  		|  nodes are online and |
  |	     |                          |  one of the above     |
  |          |				|  sections apply       |
  +----------+--------------------------+-----------------------+

  N B A disk can be in one of the following states.
   - Unformatted
   - Formatted
   - Corrupted
   - Offline

*/

// InitActions - a type synonym for enumerating initialization activities.
type InitActions int

const (
	// FormatDisks - see above table for disk states where it is applicable.
	FormatDisks InitActions = iota

	// WaitForHeal - Wait for disks to heal.
	WaitForHeal

	// WaitForQuorum - Wait for quorum number of disks to be online.
	WaitForQuorum

	// WaitForAll - Wait for all disks to be online.
	WaitForAll

	// WaitForFormatting - Wait for formatting to be triggered from the '1st' server in the cluster.
	WaitForFormatting

	// InitObjectLayer - Initialize object layer.
	InitObjectLayer

	// Abort initialization of object layer since there aren't enough good
	// copies of format.json to recover.
	Abort
)

func prepForInit(disks []string, sErrs []error, diskCount int) InitActions {
	// Count errors by error value.
	errMap := make(map[error]int)
	for _, err := range sErrs {
		errMap[err]++
	}

	quorum := diskCount/2 + 1
	disksOffline := errMap[errDiskNotFound]
	disksFormatted := errMap[nil]
	disksUnformatted := errMap[errUnformattedDisk]
	disksCorrupted := errMap[errCorruptedFormat]

	// All disks are unformatted, proceed to formatting disks.
	if disksUnformatted == diskCount {
		// Only the first server formats an uninitialized setup, others wait for notification.
		if isLocalStorage(disks[0]) {
			return FormatDisks
		}
		return WaitForFormatting
	} else if disksUnformatted >= quorum {
		if disksUnformatted+disksOffline == diskCount {
			return WaitForAll
		}
		// Some disks possibly corrupted.
		return WaitForHeal
	}

	// Already formatted, proceed to initialization of object layer.
	if disksFormatted == diskCount {
		return InitObjectLayer
	} else if disksFormatted >= quorum {
		if (disksFormatted+disksOffline == diskCount) ||
			(disksFormatted+disksUnformatted == diskCount) {
			return InitObjectLayer
		}
		// Some disks possibly corrupted.
		return WaitForHeal
	}

	// No Quorum.
	if disksOffline >= quorum {
		return WaitForQuorum
	}

	// There is quorum or more corrupted disks, there is not enough good
	// disks to reconstruct format.json.
	if disksCorrupted >= quorum {
		return Abort
	}
	// Some of the formatted disks are possibly offline.
	return WaitForHeal
}

func retryFormattingDisks(disks []string, storageDisks []StorageAPI) ([]StorageAPI, error) {
	nextBackoff := time.Duration(0)
	var err error
	done := false
	for !done {
		select {
		case <-time.After(nextBackoff * time.Second):
			// Attempt to load all `format.json`.
			_, sErrs := loadAllFormats(storageDisks)
			switch prepForInit(disks, sErrs, len(storageDisks)) {
			case Abort:
				err = errCorruptedFormat
				done = true
			case FormatDisks:
				err = initFormatXL(storageDisks)
				done = true
			case InitObjectLayer:
				err = nil
				done = true
			}
		case <-globalWakeupCh:
			// Reset nextBackoff to reduce the subsequent wait and re-read
			// format.json from all disks again.
			nextBackoff = 0
		}
	}
	if err != nil {
		return nil, err
	}
	return storageDisks, nil
}

func waitForFormattingDisks(disks, ignoredDisks []string) ([]StorageAPI, error) {
	// FS Setup
	if len(disks) == 1 {
		storage, err := newStorageAPI(disks[0])
		if err != nil && err != errDiskNotFound {
			return nil, err
		}
		return []StorageAPI{storage}, nil
	}

	// XL Setup
	if err := checkSufficientDisks(disks); err != nil {
		return nil, err
	}

	disksSet := set.NewStringSet()
	if len(ignoredDisks) > 0 {
		disksSet = set.CreateStringSet(ignoredDisks...)
	}
	// Bootstrap disks.
	storageDisks := make([]StorageAPI, len(disks))
	for index, disk := range disks {
		// Check if disk is ignored.
		if disksSet.Contains(disk) {
			// Set this situation as disk not found.
			storageDisks[index] = nil
			continue
		}
		// Intentionally ignore disk not found errors. XL is designed
		// to handle these errors internally.
		storage, err := newStorageAPI(disk)
		if err != nil && err != errDiskNotFound {
			return nil, err
		}
		storageDisks[index] = storage
	}
	// Start wait loop retrying formatting disks.
	return retryFormattingDisks(disks, storageDisks)
}
