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

import "time"

// commonTime returns a maximally occurring time from a list of time.
func commonTime(modTimes []time.Time) (modTime time.Time) {
	var maxima int // Counter for remembering max occurrence of elements.
	timeOccurenceMap := make(map[time.Time]int)
	// Ignore the uuid sentinel and count the rest.
	for _, time := range modTimes {
		if time == timeSentinel {
			continue
		}
		timeOccurenceMap[time]++
	}
	// Find the common cardinality from previously collected
	// occurrences of elements.
	for time, count := range timeOccurenceMap {
		if count > maxima {
			maxima = count
			modTime = time
		}
	}
	// Return the collected common uuid.
	return modTime
}

// Beginning of unix time is treated as sentinel value here.
var timeSentinel = time.Unix(0, 0).UTC()

// Boot modTimes up to disk count, setting the value to time sentinel.
func bootModtimes(diskCount int) []time.Time {
	modTimes := make([]time.Time, diskCount)
	// Boots up all the modtimes.
	for i := range modTimes {
		modTimes[i] = timeSentinel
	}
	return modTimes
}

// Extracts list of times from xlMetaV1 slice and returns, skips
// slice elements which have errors. As a special error
// errFileNotFound is treated as a initial good condition.
func listObjectModtimes(partsMetadata []xlMetaV1, errs []error) (modTimes []time.Time) {
	modTimes = bootModtimes(len(partsMetadata))
	// Set a new time value, specifically set when
	// error == errFileNotFound (this is needed when this is a
	// fresh PutObject).
	timeNow := time.Now().UTC()
	for index, metadata := range partsMetadata {
		if errs[index] == nil {
			// Once the file is found, save the uuid saved on disk.
			modTimes[index] = metadata.Stat.ModTime
		} else if errs[index] == errFileNotFound {
			// Once the file is not found then the epoch is current time.
			modTimes[index] = timeNow
		}
	}
	return modTimes
}

// Returns slice of online disks needed.
// - slice returing readable disks.
// - modTime of the Object
func listOnlineDisks(disks []StorageAPI, partsMetadata []xlMetaV1, errs []error) (onlineDisks []StorageAPI, modTime time.Time) {
	onlineDisks = make([]StorageAPI, len(disks))

	// List all the file commit ids from parts metadata.
	modTimes := listObjectModtimes(partsMetadata, errs)

	// Reduce list of UUIDs to a single common value.
	modTime = commonTime(modTimes)

	// Create a new online disks slice, which have common uuid.
	for index, t := range modTimes {
		if t == modTime {
			onlineDisks[index] = disks[index]
		} else {
			onlineDisks[index] = nil
		}
	}
	return onlineDisks, modTime
}

// Return disks with the outdated or missing object.
func outDatedDisks(disks []StorageAPI, partsMetadata []xlMetaV1, errs []error) (outDatedDisks []StorageAPI) {
	outDatedDisks = make([]StorageAPI, len(disks))
	latestDisks, _ := listOnlineDisks(disks, partsMetadata, errs)
	for index, disk := range latestDisks {
		if errorCause(errs[index]) == errFileNotFound {
			outDatedDisks[index] = disks[index]
			continue
		}
		if errs[index] != nil {
			continue
		}
		if disk == nil {
			outDatedDisks[index] = disks[index]
		}
	}
	return outDatedDisks
}

// Returns if the object should be healed.
func xlShouldHeal(partsMetadata []xlMetaV1, errs []error) bool {
	modTime := commonTime(listObjectModtimes(partsMetadata, errs))
	for index := range partsMetadata {
		if errs[index] == errDiskNotFound {
			continue
		}
		if errs[index] != nil {
			return true
		}
		if modTime != partsMetadata[index].Stat.ModTime {
			return true
		}
	}
	return false
}
