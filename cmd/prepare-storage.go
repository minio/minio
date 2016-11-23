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
	"errors"
	"net/url"
	"time"

	"github.com/minio/mc/pkg/console"
)

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
  |	     | Formatted,   		|   "Heal via control"  |
  |	     | some unformatted		|  and initObjectLayer  |
  +----------+--------------------------+-----------------------+
  | All      | None Formatted           |  FormatDisks          |
  |	     |				|  and initObjectLayer  |
  |          | 		             	|                   	|
  +----------+--------------------------+-----------------------+
  | No       |          		|  Wait till enough     |
  | Quorum   |          _  		|  nodes are online and |
  |	     |                          |  one of the above     |
  |          |				|  sections apply       |
  +----------+--------------------------+-----------------------+
  |	     |      			|                       |
  | Quorum   | Quorum UnFormatted   	|        Abort        	|
  +----------+--------------------------+-----------------------+

  A disk can be in one of the following states.
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

	// WaitForConfig - Wait for all servers to have the same config including (credentials, version and time).
	WaitForConfig

	// InitObjectLayer - Initialize object layer.
	InitObjectLayer

	// Abort initialization of object layer since there aren't enough good
	// copies of format.json to recover.
	Abort
)

// Quick error to actions converts looking for specific errors which need to
// be returned quickly and server should wait instead.
func quickErrToActions(errMap map[error]int) InitActions {
	var action InitActions
	switch {
	case errMap[errInvalidAccessKeyID] > 0:
		fallthrough
	case errMap[errAuthentication] > 0:
		fallthrough
	case errMap[errServerVersionMismatch] > 0:
		fallthrough
	case errMap[errServerTimeMismatch] > 0:
		action = WaitForConfig
	}
	return action
}

// Preparatory initialization stage for XL validates known errors.
// Converts them into specific actions. These actions have special purpose
// which caller decides on what needs to be done.
func prepForInitXL(firstDisk bool, sErrs []error, diskCount int) InitActions {
	// Count errors by error value.
	errMap := make(map[error]int)
	for _, err := range sErrs {
		errMap[err]++
	}

	// Validates and converts specific config errors into WaitForConfig.
	if quickErrToActions(errMap) == WaitForConfig {
		return WaitForConfig
	}

	quorum := diskCount/2 + 1
	disksOffline := errMap[errDiskNotFound]
	disksFormatted := errMap[nil]
	disksUnformatted := errMap[errUnformattedDisk]
	disksCorrupted := errMap[errCorruptedFormat]

	// No Quorum lots of offline disks, wait for quorum.
	if disksOffline >= quorum {
		return WaitForQuorum
	}

	// There is quorum or more corrupted disks, there is not enough good
	// disks to reconstruct format.json.
	if disksCorrupted >= quorum {
		return Abort
	}

	// All disks are unformatted, proceed to formatting disks.
	if disksUnformatted == diskCount {
		// Only the first server formats an uninitialized setup, others wait for notification.
		if firstDisk { // First node always initializes.
			return FormatDisks
		}
		return WaitForFormatting
	}

	// Total disks unformatted are in quorum verify if we have some offline disks.
	if disksUnformatted >= quorum {
		// Some disks offline and some disks unformatted, wait for all of them to come online.
		if disksUnformatted+disksFormatted+disksOffline == diskCount {
			return WaitForAll
		}
		// Some disks possibly corrupted and too many unformatted disks.
		return Abort
	}

	// Already formatted and in quorum, proceed to initialization of object layer.
	if disksFormatted >= quorum {
		if disksFormatted+disksOffline == diskCount {
			return InitObjectLayer
		}
		// Some of the formatted disks are possibly corrupted or unformatted, heal them.
		return WaitForHeal
	} // Exhausted all our checks, un-handled errors perhaps we Abort.
	return WaitForQuorum
}

// Prints retry message upon a specific retry count.
func printRetryMsg(sErrs []error, storageDisks []StorageAPI) {
	for i, sErr := range sErrs {
		switch sErr {
		case errDiskNotFound, errFaultyDisk, errFaultyRemoteDisk:
			console.Printf("Disk %s is still unreachable, with error %s\n", storageDisks[i], sErr)
		}
	}
}

// Implements a jitter backoff loop for formatting all disks during
// initialization of the server.
func retryFormattingDisks(firstDisk bool, endpoints []*url.URL, storageDisks []StorageAPI) error {
	if len(endpoints) == 0 {
		return errInvalidArgument
	}
	if storageDisks == nil {
		return errInvalidArgument
	}

	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{}, 1)

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	// Wait on the jitter retry loop.
	retryTimerCh := newRetryTimer(time.Second, time.Second*30, MaxJitter, doneCh)
	for {
		select {
		case retryCount := <-retryTimerCh:
			// Attempt to load all `format.json` from all disks.
			formatConfigs, sErrs := loadAllFormats(storageDisks)
			if retryCount > 5 {
				// After 5 retry attempts we start printing actual errors
				// for disks not being available.
				printRetryMsg(sErrs, storageDisks)
			}
			// Check if this is a XL or distributed XL, anything > 1 is considered XL backend.
			if len(formatConfigs) > 1 {
				switch prepForInitXL(firstDisk, sErrs, len(storageDisks)) {
				case Abort:
					return errCorruptedFormat
				case FormatDisks:
					console.Eraseline()
					printFormatMsg(endpoints, storageDisks, printOnceFn())
					return initFormatXL(storageDisks)
				case InitObjectLayer:
					console.Eraseline()
					// Validate formats load before proceeding forward.
					err := genericFormatCheck(formatConfigs, sErrs)
					if err == nil {
						printRegularMsg(endpoints, storageDisks, printOnceFn())
					}
					return err
				case WaitForHeal:
					// Validate formats load before proceeding forward.
					err := genericFormatCheck(formatConfigs, sErrs)
					if err == nil {
						printHealMsg(endpoints, storageDisks, printOnceFn())
					}
					return err
				case WaitForQuorum:
					console.Printf(
						"Initializing data volume. Waiting for minimum %d servers to come online.\n",
						len(storageDisks)/2+1,
					)
				case WaitForConfig:
					// Print configuration errors.
					printConfigErrMsg(storageDisks, sErrs, printOnceFn())
				case WaitForAll:
					console.Println("Initializing data volume for first time. Waiting for other servers to come online")
				case WaitForFormatting:
					console.Println("Initializing data volume for first time. Waiting for first server to come online")
				}
				continue
			} // else We have FS backend now. Check fs format as well now.
			if isFormatFound(formatConfigs) {
				console.Eraseline()
				// Validate formats load before proceeding forward.
				return genericFormatCheck(formatConfigs, sErrs)
			} // else initialize the format for FS.
			return initFormatFS(storageDisks[0])
		case <-globalServiceDoneCh:
			return errors.New("Initializing data volumes gracefully stopped")
		}
	}
}

// Initialize storage disks based on input arguments.
func initStorageDisks(endpoints []*url.URL) ([]StorageAPI, error) {
	// Bootstrap disks.
	storageDisks := make([]StorageAPI, len(endpoints))
	for index, ep := range endpoints {
		if ep == nil {
			return nil, errInvalidArgument
		}
		// Intentionally ignore disk not found errors. XL is designed
		// to handle these errors internally.
		storage, err := newStorageAPI(ep)
		if err != nil && err != errDiskNotFound {
			return nil, err
		}
		storageDisks[index] = storage
	}
	return storageDisks, nil
}

// Format disks before initialization object layer.
func waitForFormatDisks(firstDisk bool, endpoints []*url.URL, storageDisks []StorageAPI) (formattedDisks []StorageAPI, err error) {
	if len(endpoints) == 0 {
		return nil, errInvalidArgument
	}
	firstEndpoint := endpoints[0]
	if firstEndpoint == nil {
		return nil, errInvalidArgument
	}
	if storageDisks == nil {
		return nil, errInvalidArgument
	}
	// Start retry loop retrying until disks are formatted properly, until we have reached
	// a conditional quorum of formatted disks.
	err = retryFormattingDisks(firstDisk, endpoints, storageDisks)
	if err != nil {
		return nil, err
	}
	// Initialize the disk into a formatted disks wrapper.
	formattedDisks = make([]StorageAPI, len(storageDisks))
	for i, storage := range storageDisks {
		formattedDisks[i] = &retryStorage{storage}
	}
	return formattedDisks, nil
}
