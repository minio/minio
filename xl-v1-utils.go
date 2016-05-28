package main

import (
	"path"
	"sync"
)

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (xl xlObjects) parentDirIsObject(bucket, parent string) bool {
	var isParentDirObject func(string) bool
	isParentDirObject = func(p string) bool {
		if p == "." {
			return false
		}
		if xl.isObject(bucket, p) {
			// If there is already a file at prefix "p" return error.
			return true
		}
		// Check if there is a file as one of the parent paths.
		return isParentDirObject(path.Dir(p))
	}
	return isParentDirObject(parent)
}

func (xl xlObjects) isObject(bucket, prefix string) bool {
	// Create errs and volInfo slices of storageDisks size.
	var errs = make([]error, len(xl.storageDisks))

	// Allocate a new waitgroup.
	var wg = &sync.WaitGroup{}
	for index, disk := range xl.storageDisks {
		wg.Add(1)
		// Stat file on all the disks in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			_, err := disk.StatFile(bucket, path.Join(prefix, xlMetaJSONFile))
			if err != nil {
				errs[index] = err
				return
			}
			errs[index] = nil
		}(index, disk)
	}

	// Wait for all the Stat operations to finish.
	wg.Wait()

	var errFileNotFoundCount int
	for _, err := range errs {
		if err != nil {
			if err == errFileNotFound {
				errFileNotFoundCount++
				// If we have errors with file not found greater than allowed read
				// quorum we return err as errFileNotFound.
				if errFileNotFoundCount > len(xl.storageDisks)-xl.readQuorum {
					return false
				}
				continue
			}
			errorIf(err, "Unable to access file "+path.Join(bucket, prefix))
			return false
		}
	}
	return true
}

// statPart - stat a part file.
func (xl xlObjects) statPart(bucket, objectPart string) (fileInfo FileInfo, err error) {
	// Count for errors encountered.
	var xlJSONErrCount = 0

	// Return the first success entry based on the selected random disk.
	for xlJSONErrCount < len(xl.storageDisks) {
		// Choose a random disk on each attempt.
		disk := xl.getRandomDisk()
		fileInfo, err = disk.StatFile(bucket, objectPart)
		if err == nil {
			return fileInfo, nil
		}
		xlJSONErrCount++ // Update error count.
	}
	return FileInfo{}, err
}
