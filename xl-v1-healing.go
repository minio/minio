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

package main

import "sync"

// Get the highest integer from a given integer slice.
func highestInt(intSlice []int64, highestInt int64) (highestInteger int64) {
	highestInteger = highestInt
	for _, integer := range intSlice {
		if highestInteger < integer {
			highestInteger = integer
			break
		}
	}
	return highestInteger
}

// Extracts objects versions from xlMetaV1 slice and returns version slice.
func listObjectVersions(partsMetadata []xlMetaV1, errs []error) (versions []int64) {
	versions = make([]int64, len(partsMetadata))
	for index, metadata := range partsMetadata {
		if errs[index] == nil {
			versions[index] = metadata.Stat.Version
		} else if errs[index] == errFileNotFound {
			versions[index] = 1
		} else {
			versions[index] = -1
		}
	}
	return versions
}

// Reads all `xl.json` metadata as a xlMetaV1 slice.
// Returns error slice indicating the failed metadata reads.
func (xl xlObjects) readAllXLMetadata(bucket, object string) ([]xlMetaV1, []error) {
	errs := make([]error, len(xl.storageDisks))
	metadataArray := make([]xlMetaV1, len(xl.storageDisks))
	var wg = &sync.WaitGroup{}
	// Read `xl.json` parallelly across disks.
	for index, disk := range xl.storageDisks {
		if disk == nil {
			errs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Read `xl.json` in routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			var err error
			metadataArray[index], err = readXLMeta(disk, bucket, object)
			if err != nil {
				errs[index] = err
				return
			}
		}(index, disk)
	}

	// Wait for all the routines to finish.
	wg.Wait()

	// Return all the metadata.
	return metadataArray, errs
}

func (xl xlObjects) shouldHeal(onlineDisks []StorageAPI) (heal bool) {
	onlineDiskCount := diskCount(onlineDisks)
	// If online disks count is lesser than configured disks, most
	// probably we need to heal the file, additionally verify if the
	// count is lesser than readQuorum, if not we throw an error.
	if onlineDiskCount < len(xl.storageDisks) {
		// Online disks lesser than total storage disks, needs to be
		// healed. unless we do not have readQuorum.
		heal = true
		// Verify if online disks count are lesser than readQuorum
		// threshold, return an error.
		if onlineDiskCount < xl.readQuorum {
			errorIf(errXLReadQuorum, "Unable to establish read quorum, disks are offline.")
			return false
		}
	}
	return heal
}

// Returns slice of online disks needed.
// - slice returing readable disks.
// - xlMetaV1
// - bool value indicating if healing is needed.
// - error if any.
func (xl xlObjects) listOnlineDisks(partsMetadata []xlMetaV1, errs []error) (onlineDisks []StorageAPI, version int64, err error) {
	onlineDisks = make([]StorageAPI, len(xl.storageDisks))
	// Do we have read Quorum?.
	if !isQuorum(errs, xl.readQuorum) {
		return nil, 0, errXLReadQuorum
	}

	// List all the file versions from partsMetadata list.
	versions := listObjectVersions(partsMetadata, errs)

	// Get highest object version.
	highestVersion := highestInt(versions, int64(1))

	// Pick online disks with version set to highestVersion.
	for index, version := range versions {
		if version == highestVersion {
			onlineDisks[index] = xl.storageDisks[index]
		} else {
			onlineDisks[index] = nil
		}
	}
	return onlineDisks, highestVersion, nil
}
