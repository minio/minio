/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"

	errors2 "github.com/minio/minio/pkg/errors"
)

const (
	// Represents Cache format json holding details on all other cache drives in use.
	formatCache = "cache"

	// formatCacheV1.Cache.Version
	formatCacheVersionV1 = "1"

	formatMetaVersion1 = "1"
)

// Represents the current cache structure with list of
// disks comprising the disk cache
// formatCacheV1 - structure holds format config version '1'.
type formatCacheV1 struct {
	formatMetaV1
	Cache struct {
		Version string `json:"version"` // Version of 'cache' format.
		This    string `json:"this"`    // This field carries assigned disk uuid.
		// Disks field carries the input disk order generated the first
		// time when fresh disks were supplied.
		Disks []string `json:"disks"`
	} `json:"cache"` // Cache field holds cache format.
}

// Used to detect the version of "cache" format.
type formatCacheVersionDetect struct {
	Cache struct {
		Version string `json:"version"`
	} `json:"cache"`
}

// Return a slice of format, to be used to format uninitialized disks.
func newFormatCacheV1(drives []string) []*formatCacheV1 {
	diskCount := len(drives)
	var disks = make([]string, diskCount)

	var formats = make([]*formatCacheV1, diskCount)

	for i := 0; i < diskCount; i++ {
		format := &formatCacheV1{}
		format.Version = formatMetaVersion1
		format.Format = formatCache
		format.Cache.Version = formatCacheVersionV1
		format.Cache.This = mustGetUUID()
		formats[i] = format
		disks[i] = formats[i].Cache.This
	}
	for i := 0; i < diskCount; i++ {
		format := formats[i]
		format.Cache.Disks = disks
	}
	return formats
}

// Returns format.Cache.Version
func formatCacheGetVersion(r io.ReadSeeker) (string, error) {
	format := &formatCacheVersionDetect{}
	if err := jsonLoad(r, format); err != nil {
		return "", err
	}
	return format.Cache.Version, nil
}

// Creates a new cache format.json if unformatted.
func createFormatCache(fsFormatPath string, format *formatCacheV1) error {
	// open file using READ & WRITE permission
	var file, err = os.OpenFile(fsFormatPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return errors2.Trace(err)
	}
	// Close the locked file upon return.
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return errors2.Trace(err)
	}
	if fi.Size() != 0 {
		// format.json already got created because of another minio process's createFormatCache()
		return nil
	}
	return jsonSave(file, format)
}

// This function creates a cache format file on disk and returns a slice
// of format cache config
func initFormatCache(drives []string) (formats []*formatCacheV1, err error) {
	nformats := newFormatCacheV1(drives)
	for i, drive := range drives {
		// Disallow relative paths, figure out absolute paths.
		cfsPath, err := filepath.Abs(drive)
		if err != nil {
			return nil, err
		}

		fi, err := os.Stat(cfsPath)
		if err == nil {
			if !fi.IsDir() {
				return nil, syscall.ENOTDIR
			}
		}
		if os.IsNotExist(err) {
			// Disk not found create it.
			err = os.MkdirAll(cfsPath, 0777)
			if err != nil {
				return nil, err
			}
		}

		cacheFormatPath := pathJoin(drive, formatConfigFile)
		// Fresh disk - create format.json for this cfs
		if err = createFormatCache(cacheFormatPath, nformats[i]); err != nil {
			return nil, err
		}
	}
	return nformats, nil
}

func loadFormatCache(drives []string) (formats []*formatCacheV1, err error) {
	var errs []error
	for _, drive := range drives {
		cacheFormatPath := pathJoin(drive, formatConfigFile)
		f, perr := os.Open(cacheFormatPath)
		if perr != nil {
			formats = append(formats, nil)
			errs = append(errs, perr)
			continue
		}
		defer f.Close()
		format, perr := formatMetaCacheV1(f)
		if perr != nil {
			// format could not be unmarshalled.
			formats = append(formats, nil)
			errs = append(errs, perr)
			continue
		}
		formats = append(formats, format)
	}
	for _, perr := range errs {
		if perr != nil {
			err = perr
		}
	}
	return formats, err
}

// unmarshalls the cache format.json into formatCacheV1
func formatMetaCacheV1(r io.ReadSeeker) (*formatCacheV1, error) {
	format := &formatCacheV1{}
	if err := jsonLoad(r, format); err != nil {
		return nil, err
	}
	return format, nil
}

func checkFormatCacheValue(format *formatCacheV1) error {
	// Validate format version and format type.
	if format.Version != formatMetaVersion1 {
		return fmt.Errorf("Unsupported version of cache format [%s] found", format.Version)
	}
	if format.Format != formatCache {
		return fmt.Errorf("Unsupported cache format [%s] found", format.Format)
	}
	if format.Cache.Version != formatCacheVersionV1 {
		return fmt.Errorf("Unsupported Cache backend format found [%s]", format.Cache.Version)
	}
	return nil
}

func checkFormatCacheValues(formats []*formatCacheV1) (int, error) {

	for i, formatCache := range formats {
		if formatCache == nil {
			continue
		}
		if err := checkFormatCacheValue(formatCache); err != nil {
			return i, err
		}
		if len(formats) != len(formatCache.Cache.Disks) {
			return i, fmt.Errorf("Expected number of cache drives %d , got  %d",
				len(formatCache.Cache.Disks), len(formats))
		}
	}
	return -1, nil
}

// checkCacheDisksConsistency - checks if "This" disk uuid on each disk is consistent with all "Disks" slices
// across disks.
func checkCacheDiskConsistency(formats []*formatCacheV1) error {
	var disks = make([]string, len(formats))
	// Collect currently available disk uuids.
	for index, format := range formats {
		if format == nil {
			disks[index] = ""
			continue
		}
		disks[index] = format.Cache.This
	}
	for i, format := range formats {
		if format == nil {
			continue
		}
		j := findCacheDiskIndex(disks[i], format.Cache.Disks)
		if j == -1 {
			return fmt.Errorf("UUID on positions %d:%d do not match with , expected %s", i, j, disks[i])
		}
		if i != j {
			return fmt.Errorf("UUID on positions %d:%d do not match with , expected %s got %s", i, j, disks[i], format.Cache.Disks[j])
		}
	}
	return nil
}

// checkCacheDisksSliceConsistency - validate cache Disks order if they are consistent.
func checkCacheDisksSliceConsistency(formats []*formatCacheV1) error {
	var sentinelDisks []string
	// Extract first valid Disks slice.
	for _, format := range formats {
		if format == nil {
			continue
		}
		sentinelDisks = format.Cache.Disks
		break
	}
	for _, format := range formats {
		if format == nil {
			continue
		}
		currentDisks := format.Cache.Disks
		if !reflect.DeepEqual(sentinelDisks, currentDisks) {
			return errors.New("inconsistent cache drives found")
		}
	}
	return nil
}

// findCacheDiskIndex returns position of cache disk in JBOD.
func findCacheDiskIndex(disk string, disks []string) int {
	for index, uuid := range disks {
		if uuid == disk {
			return index
		}
	}
	return -1
}

// validate whether cache drives order has changed
func validateCacheFormats(formats []*formatCacheV1) error {
	if _, err := checkFormatCacheValues(formats); err != nil {
		return err
	}
	if err := checkCacheDisksSliceConsistency(formats); err != nil {
		return err
	}
	return checkCacheDiskConsistency(formats)
}

// return true if all of the list of cache drives are
// fresh disks
func cacheDrivesUnformatted(drives []string) bool {
	count := 0
	for _, drive := range drives {
		cacheFormatPath := pathJoin(drive, formatConfigFile)

		// // Disallow relative paths, figure out absolute paths.
		cfsPath, err := filepath.Abs(cacheFormatPath)
		if err != nil {
			continue
		}

		fi, err := os.Stat(cfsPath)
		if err == nil {
			if !fi.IsDir() {
				continue
			}
		}
		if os.IsNotExist(err) {
			count++
			continue
		}
	}
	return count == len(drives)
}

// create format.json for each cache drive if fresh disk or load format from disk
// Then validate the format for all drives in the cache to ensure order
// of cache drives has not changed.
func loadAndValidateCacheFormat(drives []string) (formats []*formatCacheV1, err error) {
	if cacheDrivesUnformatted(drives) {
		formats, err = initFormatCache(drives)
	} else {
		formats, err = loadFormatCache(drives)
	}
	if err != nil {
		return formats, err
	}
	return formats, validateCacheFormats(formats)
}
