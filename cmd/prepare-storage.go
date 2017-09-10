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
	"fmt"
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

	// SuggestToHeal - Prints heal message and initialize object layer.
	SuggestToHeal

	// WaitForQuorum - Wait for quorum number of disks to be online.
	WaitForQuorum

	// WaitForAll - Wait for all disks to be online.
	WaitForAll

	// WaitForFormatting - Wait for formatting to be triggered
	// from the '1st' server in the cluster.
	WaitForFormatting

	// WaitForConfig - Wait for all servers to have the same config
	// including (credentials, version and time).
	WaitForConfig

	// InitObjectLayer - Initialize object layer.
	InitObjectLayer

	// Abort initialization of object layer since there aren't enough good
	// copies of format.json to recover.
	Abort
)

// Quick error to actions converts looking for specific errors
// which need to be returned quickly and server should wait instead.
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

// Logic used in this function is as shown below.
//
// ---- Possible states and handled conditions -----
//
// - Formatted setup
//   - InitObjectLayer when `disksFormatted >= readQuorum`
//   - Wait for quorum when `disksFormatted < readQuorum && disksFormatted + disksOffline >= readQuorum`
//     (we don't know yet if there are unformatted disks)
//   - Wait for heal when `disksFormatted >= readQuorum && disksUnformatted > 0`
//     (here we know there is at least one unformatted disk which requires healing)
//
// - Unformatted setup
//   - Format/Wait for format when `disksUnformatted == diskCount`
//
// - Wait for all when `disksUnformatted + disksFormatted + diskOffline == diskCount`
//
// Under all other conditions should lead to server initialization aborted.
func prepForInitXL(firstDisk bool, sErrs []error, diskCount int) InitActions {
	// Count errors by error value.
	errMap := make(map[error]int)
	for _, err := range sErrs {
		errMap[errorCause(err)]++
	}

	// Validates and converts specific config errors into WaitForConfig.
	if quickErrToActions(errMap) == WaitForConfig {
		return WaitForConfig
	}

	readQuorum := diskCount / 2
	disksOffline := errMap[errDiskNotFound]
	disksFormatted := errMap[nil]
	disksUnformatted := errMap[errUnformattedDisk]

	// No Quorum lots of offline disks, wait for quorum.
	if disksOffline > readQuorum {
		return WaitForQuorum
	}

	// All disks are unformatted, proceed to formatting disks.
	if disksUnformatted == diskCount {
		// Only the first server formats an uninitialized setup, others wait for notification.
		if firstDisk { // First node always initializes.
			return FormatDisks
		}
		return WaitForFormatting
	}

	// Already formatted and in quorum, proceed to initialization of object layer.
	if disksFormatted >= readQuorum {
		if disksFormatted+disksOffline == diskCount {
			return InitObjectLayer
		}

		// Some of the formatted disks are possibly corrupted or unformatted,
		// let user know to heal them.
		return SuggestToHeal
	}

	// Some unformatted, some disks formatted and some disks are offline but we don't
	// quorum to decide. This is an undecisive state - wait for all of offline disks
	// to be online to figure out the course of action.
	if disksUnformatted+disksFormatted+disksOffline == diskCount {
		return WaitForAll
	}

	// Exhausted all our checks, un-handled situations such as some disks corrupted we Abort.
	return Abort
}

// Prints retry message upon a specific retry count.
func printRetryMsg(sErrs []error, storageDisks []StorageAPI) {
	for i, sErr := range sErrs {
		switch sErr {
		case errDiskNotFound, errFaultyDisk, errFaultyRemoteDisk:
			errorIf(sErr, "Disk %s is still unreachable", storageDisks[i])
		}
	}
}

// Maximum retry attempts.
const maxRetryAttempts = 5

// Implements a jitter backoff loop for formatting all disks during
// initialization of the server.
func retryFormattingXLDisks(firstDisk bool, endpoints EndpointList, storageDisks []StorageAPI) error {
	if len(endpoints) == 0 {
		return errInvalidArgument
	}
	if storageDisks == nil {
		return errInvalidArgument
	}

	// Done channel is used to close any lingering retry routine, as soon
	// as this function returns.
	doneCh := make(chan struct{})

	// Indicate to our retry routine to exit cleanly, upon this function return.
	defer close(doneCh)

	// prepare getElapsedTime() to calculate elapsed time since we started trying formatting disks.
	// All times are rounded to avoid showing milli, micro and nano seconds
	formatStartTime := time.Now().Round(time.Second)
	getElapsedTime := func() string {
		return time.Now().Round(time.Second).Sub(formatStartTime).String()
	}

	// Wait on the jitter retry loop.
	retryTimerCh := newRetryTimerSimple(doneCh)
	for {
		select {
		case retryCount := <-retryTimerCh:
			// Attempt to load all `format.json` from all disks.
			formatConfigs, sErrs := loadAllFormats(storageDisks)
			if retryCount > maxRetryAttempts {
				// After max retry attempts we start printing
				// actual errors for disks not being available.
				printRetryMsg(sErrs, storageDisks)
			}

			// Pre-emptively check if one of the formatted disks
			// is invalid. This function returns success for the
			// most part unless one of the formats is not consistent
			// with expected XL format. For example if a user is
			// trying to pool FS backend into an XL set.
			if index, err := checkFormatXLValues(formatConfigs); err != nil {
				// We will perhaps print and retry for the first 5 attempts
				// because in XL initialization first server is the one which
				// initializes the erasure set. This check ensures that the
				// rest of the other servers do get a chance to see that the
				// first server has a wrong format and exit gracefully.
				// refer - https://github.com/minio/minio/issues/4140
				if retryCount > maxRetryAttempts {
					errorIf(err, "%s : Detected disk in unexpected format",
						storageDisks[index])
					continue
				}
				return err
			}

			// Check if this is a XL or distributed XL, anything > 1 is considered XL backend.
			switch prepForInitXL(firstDisk, sErrs, len(storageDisks)) {
			case Abort:
				return fmt.Errorf("%s", combineDiskErrs(storageDisks, sErrs))
			case FormatDisks:
				console.Eraseline()
				printFormatMsg(endpoints, storageDisks, printOnceFn())
				return initFormatXL(storageDisks)
			case InitObjectLayer:
				console.Eraseline()
				// Validate formats loaded before proceeding forward.
				err := genericFormatCheckXL(formatConfigs, sErrs)
				if err == nil {
					printRegularMsg(endpoints, storageDisks, printOnceFn())
				}
				return err
			case SuggestToHeal:
				// Validate formats loaded before proceeding forward.
				err := genericFormatCheckXL(formatConfigs, sErrs)
				if err == nil {
					printHealMsg(endpoints, storageDisks, printOnceFn())
				}
				return err
			case WaitForQuorum:
				console.Printf(
					"Initializing data volume. Waiting for minimum %d servers to come online. (elapsed %s)\n",
					len(storageDisks)/2+1, getElapsedTime(),
				)
			case WaitForConfig:
				// Print configuration errors.
				printConfigErrMsg(storageDisks, sErrs, printOnceFn())
			case WaitForAll:
				console.Printf("Initializing data volume for first time. Waiting for other servers to come online (elapsed %s)\n", getElapsedTime())
			case WaitForFormatting:
				console.Printf("Initializing data volume for first time. Waiting for first server to come online (elapsed %s)\n", getElapsedTime())
			}
		case <-globalServiceDoneCh:
			return errors.New("Initializing data volumes gracefully stopped")
		}
	}
}

// Initialize storage disks based on input arguments.
func initStorageDisks(endpoints EndpointList) ([]StorageAPI, error) {
	// Bootstrap disks.
	storageDisks := make([]StorageAPI, len(endpoints))
	for index, endpoint := range endpoints {
		// Intentionally ignore disk not found errors. XL is designed
		// to handle these errors internally.
		storage, err := newStorageAPI(endpoint)
		if err != nil && err != errDiskNotFound {
			return nil, err
		}
		storageDisks[index] = storage
	}
	return storageDisks, nil
}

// Format disks before initialization of object layer.
func waitForFormatXLDisks(firstDisk bool, endpoints EndpointList, storageDisks []StorageAPI) (formattedDisks []StorageAPI, err error) {
	if len(endpoints) == 0 {
		return nil, errInvalidArgument
	}
	if storageDisks == nil {
		return nil, errInvalidArgument
	}

	// Retryable disks before formatting, we need to have a larger
	// retry window so that we wait enough amount of time before
	// the disks come online.
	retryDisks := make([]StorageAPI, len(storageDisks))
	for i, storage := range storageDisks {
		retryDisks[i] = &retryStorage{
			remoteStorage:    storage,
			maxRetryAttempts: globalStorageInitRetryThreshold,
			retryUnit:        time.Second,
			retryCap:         time.Second * 30, // 30 seconds.
			offlineTimestamp: UTCNow(),
		}
	}

	// Start retry loop retrying until disks are formatted properly, until we have reached
	// a conditional quorum of formatted disks.
	err = retryFormattingXLDisks(firstDisk, endpoints, retryDisks)
	if err != nil {
		return nil, err
	}

	// Initialize the disk into a formatted disks wrapper.
	formattedDisks = make([]StorageAPI, len(storageDisks))
	for i, storage := range storageDisks {
		// After formatting is done we need a smaller time
		// window and lower retry value before formatting.
		formattedDisks[i] = &retryStorage{
			remoteStorage:    storage,
			maxRetryAttempts: globalStorageRetryThreshold,
			retryUnit:        time.Millisecond,
			retryCap:         time.Millisecond * 5, // 5 milliseconds.
			offlineTimestamp: UTCNow(),             // Set timestamp to prevent immediate marking as offline
		}
	}

	// Success.
	return formattedDisks, nil
}
