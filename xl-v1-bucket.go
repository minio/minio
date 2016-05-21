package main

import (
	"sort"
	"sync"
)

/// Bucket operations

// MakeBucket - make a bucket.
func (xl xlObjects) MakeBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	nsMutex.Lock(bucket, "")
	defer nsMutex.Unlock(bucket, "")

	// Err counters.
	createVolErr := 0       // Count generic create vol errs.
	volumeExistsErrCnt := 0 // Count all errVolumeExists errs.

	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(xl.storageDisks))

	// Make a volume entry on all underlying storage disks.
	for index, disk := range xl.storageDisks {
		wg.Add(1)
		// Make a volume inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := disk.MakeVol(bucket)
			if err != nil {
				dErrs[index] = err
				return
			}
			dErrs[index] = nil
		}(index, disk)
	}

	// Wait for all make vol to finish.
	wg.Wait()

	// Loop through all the concocted errors.
	for _, err := range dErrs {
		if err == nil {
			continue
		}
		// if volume already exists, count them.
		if err == errVolumeExists {
			volumeExistsErrCnt++
			continue
		}

		// Update error counter separately.
		createVolErr++
	}

	// Return err if all disks report volume exists.
	if volumeExistsErrCnt == len(xl.storageDisks) {
		return toObjectErr(errVolumeExists, bucket)
	} else if createVolErr > len(xl.storageDisks)-xl.writeQuorum {
		// Return errWriteQuorum if errors were more than
		// allowed write quorum.
		return toObjectErr(errWriteQuorum, bucket)
	}
	return nil
}

// getAllBucketInfo - list bucket info from all disks.
// Returns error slice indicating the failed volume stat operations.
func (xl xlObjects) getAllBucketInfo(bucketName string) ([]BucketInfo, []error) {
	// Create errs and volInfo slices of storageDisks size.
	var errs = make([]error, len(xl.storageDisks))
	var volsInfo = make([]VolInfo, len(xl.storageDisks))

	// Allocate a new waitgroup.
	var wg = &sync.WaitGroup{}
	for index, disk := range xl.storageDisks {
		wg.Add(1)
		// Stat volume on all the disks in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			volInfo, err := disk.StatVol(bucketName)
			if err != nil {
				errs[index] = err
				return
			}
			volsInfo[index] = volInfo
			errs[index] = nil
		}(index, disk)
	}

	// Wait for all the Stat operations to finish.
	wg.Wait()

	// Return the concocted values.
	var bucketsInfo = make([]BucketInfo, len(xl.storageDisks))
	for _, volInfo := range volsInfo {
		if IsValidBucketName(volInfo.Name) {
			bucketsInfo = append(bucketsInfo, BucketInfo{
				Name:    volInfo.Name,
				Created: volInfo.Created,
			})
		}
	}
	return bucketsInfo, errs
}

// listAllBucketInfo - list all stat volume info from all disks.
// Returns
// - stat volume info for all online disks.
// - boolean to indicate if healing is necessary.
// - error if any.
func (xl xlObjects) listAllBucketInfo(bucketName string) ([]BucketInfo, bool, error) {
	bucketsInfo, errs := xl.getAllBucketInfo(bucketName)
	notFoundCount := 0
	for _, err := range errs {
		if err == errVolumeNotFound {
			notFoundCount++
			// If we have errors with file not found greater than allowed read
			// quorum we return err as errFileNotFound.
			if notFoundCount > len(xl.storageDisks)-xl.readQuorum {
				return nil, false, errVolumeNotFound
			}
		}
	}

	// Calculate online disk count.
	onlineDiskCount := 0
	for index := range errs {
		if errs[index] == nil {
			onlineDiskCount++
		}
	}

	var heal bool
	// If online disks count is lesser than configured disks, most
	// probably we need to heal the file, additionally verify if the
	// count is lesser than readQuorum, if not we throw an error.
	if onlineDiskCount < len(xl.storageDisks) {
		// Online disks lesser than total storage disks, needs to be
		// healed. unless we do not have readQuorum.
		heal = true
		// Verify if online disks count are lesser than readQuorum
		// threshold, return an error if yes.
		if onlineDiskCount < xl.readQuorum {
			return nil, false, errReadQuorum
		}
	}

	// Return success.
	return bucketsInfo, heal, nil
}

// Checks whether bucket exists.
func (xl xlObjects) isBucketExist(bucketName string) bool {
	// Check whether bucket exists.
	_, _, err := xl.listAllBucketInfo(bucketName)
	if err != nil {
		if err == errVolumeNotFound {
			return false
		}
		errorIf(err, "Stat failed on bucket "+bucketName+".")
		return false
	}
	return true
}

// GetBucketInfo - get bucket info.
func (xl xlObjects) GetBucketInfo(bucket string) (BucketInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketInfo{}, BucketNameInvalid{Bucket: bucket}
	}

	nsMutex.RLock(bucket, "")
	defer nsMutex.RUnlock(bucket, "")

	// List and figured out if we need healing.
	bucketsInfo, heal, err := xl.listAllBucketInfo(bucket)
	if err != nil {
		return BucketInfo{}, toObjectErr(err, bucket)
	}

	// Heal for missing entries.
	if heal {
		go func() {
			// Create bucket if missing on disks.
			for index, bktInfo := range bucketsInfo {
				if bktInfo.Name != "" {
					continue
				}
				// Bucketinfo name would be an empty string, create it.
				xl.storageDisks[index].MakeVol(bucket)
			}
		}()
	}

	// Loop through all statVols, calculate the actual usage values.
	var total, free int64
	var bucketInfo BucketInfo
	for _, bucketInfo = range bucketsInfo {
		if bucketInfo.Name == "" {
			continue
		}
		free += bucketInfo.Free
		total += bucketInfo.Total
	}
	// Update the aggregated values.
	bucketInfo.Free = free
	bucketInfo.Total = total

	return BucketInfo{
		Name:    bucket,
		Created: bucketInfo.Created,
		Total:   bucketInfo.Total,
		Free:    bucketInfo.Free,
	}, nil
}

func (xl xlObjects) listBuckets() ([]BucketInfo, error) {
	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Success vols map carries successful results of ListVols from each disks.
	var successVols = make([][]VolInfo, len(xl.storageDisks))
	for index, disk := range xl.storageDisks {
		wg.Add(1) // Add each go-routine to wait for.
		go func(index int, disk StorageAPI) {
			// Indicate wait group as finished.
			defer wg.Done()

			// Initiate listing.
			volsInfo, _ := disk.ListVols()
			successVols[index] = volsInfo
		}(index, disk)
	}

	// For all the list volumes running in parallel to finish.
	wg.Wait()

	// Loop through success vols and get aggregated usage values.
	var volsInfo []VolInfo
	var total, free int64
	for _, volsInfo = range successVols {
		var volInfo VolInfo
		for _, volInfo = range volsInfo {
			if volInfo.Name == "" {
				continue
			}
			if !IsValidBucketName(volInfo.Name) {
				continue
			}
			break
		}
		free += volInfo.Free
		total += volInfo.Total
	}

	// Save the updated usage values back into the vols.
	for index, volInfo := range volsInfo {
		volInfo.Free = free
		volInfo.Total = total
		volsInfo[index] = volInfo
	}

	// NOTE: The assumption here is that volumes across all disks in
	// readQuorum have consistent view i.e they all have same number
	// of buckets. This is essentially not verified since healing
	// should take care of this.
	var bucketsInfo []BucketInfo
	for _, volInfo := range volsInfo {
		// StorageAPI can send volume names which are incompatible
		// with buckets, handle it and skip them.
		if !IsValidBucketName(volInfo.Name) {
			continue
		}
		bucketsInfo = append(bucketsInfo, BucketInfo{
			Name:    volInfo.Name,
			Created: volInfo.Created,
			Total:   volInfo.Total,
			Free:    volInfo.Free,
		})
	}
	return bucketsInfo, nil
}

// ListBuckets - list buckets.
func (xl xlObjects) ListBuckets() ([]BucketInfo, error) {
	bucketInfos, err := xl.listBuckets()
	if err != nil {
		return nil, toObjectErr(err)
	}
	sort.Sort(byBucketName(bucketInfos))
	return bucketInfos, nil
}

// DeleteBucket - delete a bucket.
func (xl xlObjects) DeleteBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	nsMutex.Lock(bucket, "")
	nsMutex.Unlock(bucket, "")

	// Collect if all disks report volume not found.
	var volumeNotFoundErrCnt int

	var wg = &sync.WaitGroup{}
	var dErrs = make([]error, len(xl.storageDisks))

	// Remove a volume entry on all underlying storage disks.
	for index, disk := range xl.storageDisks {
		wg.Add(1)
		// Delete volume inside a go-routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := disk.DeleteVol(bucket)
			if err != nil {
				dErrs[index] = err
				return
			}
			dErrs[index] = nil
		}(index, disk)
	}

	// Wait for all the delete vols to finish.
	wg.Wait()

	// Loop through concocted errors and return anything unusual.
	for _, err := range dErrs {
		if err != nil {
			// We ignore error if errVolumeNotFound or errDiskNotFound
			if err == errVolumeNotFound || err == errDiskNotFound {
				volumeNotFoundErrCnt++
				continue
			}
			return toObjectErr(err, bucket)
		}
	}

	// Return err if all disks report volume not found.
	if volumeNotFoundErrCnt == len(xl.storageDisks) {
		return toObjectErr(errVolumeNotFound, bucket)
	}

	return nil
}
