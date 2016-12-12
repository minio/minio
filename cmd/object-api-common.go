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
	"net"
	"net/url"
	"runtime"
	"strings"
	"sync"

	humanize "github.com/dustin/go-humanize"
)

const (
	// Block size used for all internal operations version 1.
	blockSizeV1 = 10 * humanize.MiByte

	// Staging buffer read size for all internal operations version 1.
	readSizeV1 = 1 * humanize.MiByte

	// Buckets meta prefix.
	bucketMetaPrefix = "buckets"
)

// Global object layer mutex, used for safely updating object layer.
var globalObjLayerMutex *sync.Mutex

// Global object layer, only accessed by newObjectLayerFn().
var globalObjectAPI ObjectLayer

func init() {
	// Initialize this once per server initialization.
	globalObjLayerMutex = &sync.Mutex{}
}

// House keeping code for FS/XL and distributed Minio setup.
func houseKeeping(storageDisks []StorageAPI) error {
	var wg = &sync.WaitGroup{}

	// Initialize errs to collect errors inside go-routine.
	var errs = make([]error, len(storageDisks))

	// Initialize all disks in parallel.
	for index, disk := range storageDisks {
		if disk == nil {
			continue
		}
		if _, ok := disk.(*networkStorage); ok {
			// Skip remote disks.
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			// Indicate this wait group is done.
			defer wg.Done()

			// Cleanup all temp entries upon start.
			err := cleanupDir(disk, minioMetaTmpBucket, "")
			if err != nil {
				if !isErrIgnored(errorCause(err), errDiskNotFound, errVolumeNotFound, errFileNotFound) {
					errs[index] = err
				}
			}
		}(index, disk)
	}

	// Wait for all cleanup to finish.
	wg.Wait()

	// Return upon first error.
	for _, err := range errs {
		if err == nil {
			continue
		}
		return toObjectErr(err, minioMetaTmpBucket, "*")
	}

	// Return success here.
	return nil
}

// Check if a network path is local to this node.
func isLocalStorage(ep *url.URL) bool {
	if ep.Host == "" {
		return true
	}
	if globalMinioHost != "" && globalMinioPort != "" {
		// if --address host:port was specified for distXL we short
		// circuit only the endPoint that matches host:port
		if net.JoinHostPort(globalMinioHost, globalMinioPort) == ep.Host {
			return true
		}
		return false
	}
	// Split host to extract host information.
	host, _, err := net.SplitHostPort(ep.Host)
	if err != nil {
		errorIf(err, "Cannot split host port")
		return false
	}
	// Resolve host to address to check if the IP is loopback.
	// If address resolution fails, assume it's a non-local host.
	addrs, err := net.LookupHost(host)
	if err != nil {
		errorIf(err, "Failed to lookup host")
		return false
	}
	for _, addr := range addrs {
		if ip := net.ParseIP(addr); ip.IsLoopback() {
			return true
		}
	}
	iaddrs, err := net.InterfaceAddrs()
	if err != nil {
		errorIf(err, "Unable to list interface addresses")
		return false
	}
	for _, addr := range addrs {
		for _, iaddr := range iaddrs {
			ip, _, err := net.ParseCIDR(iaddr.String())
			if err != nil {
				errorIf(err, "Unable to parse CIDR")
				return false
			}
			if ip.String() == addr {
				return true
			}

		}
	}
	return false
}

// Fetch the path component from *url.URL*.
func getPath(ep *url.URL) string {
	if ep == nil {
		return ""
	}
	var diskPath string
	// For windows ep.Path is usually empty
	if runtime.GOOS == "windows" {
		switch ep.Scheme {
		case "":
			// Eg. "minio server .\export"
			diskPath = ep.Path
		case "http", "https":
			// For full URLs windows drive is part of URL path.
			// Eg: http://ip:port/C:\mydrive
			// For windows trim off the preceding "/".
			diskPath = ep.Path[1:]
		default:
			// For the rest url splits drive letter into
			// Scheme contruct the disk path back.
			diskPath = ep.Scheme + ":" + ep.Opaque
		}
	} else {
		// For other operating systems ep.Path is non empty.
		diskPath = ep.Path
	}
	return diskPath
}

// Depending on the disk type network or local, initialize storage API.
func newStorageAPI(ep *url.URL) (storage StorageAPI, err error) {
	if isLocalStorage(ep) {
		return newPosix(getPath(ep))
	}
	return newStorageRPC(ep)
}

var initMetaVolIgnoredErrs = append(baseIgnoredErrs, errVolumeExists)

// Initializes meta volume on all input storage disks.
func initMetaVolume(storageDisks []StorageAPI) error {
	// This happens for the first time, but keep this here since this
	// is the only place where it can be made expensive optimizing all
	// other calls. Create minio meta volume, if it doesn't exist yet.
	var wg = &sync.WaitGroup{}

	// Initialize errs to collect errors inside go-routine.
	var errs = make([]error, len(storageDisks))

	// Initialize all disks in parallel.
	for index, disk := range storageDisks {
		if disk == nil {
			// Ignore create meta volume on disks which are not found.
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			// Indicate this wait group is done.
			defer wg.Done()

			// Attempt to create `.minio.sys`.
			err := disk.MakeVol(minioMetaBucket)
			if err != nil {
				if !isErrIgnored(err, initMetaVolIgnoredErrs...) {
					errs[index] = err
					return
				}
			}
			err = disk.MakeVol(minioMetaTmpBucket)
			if err != nil {
				if !isErrIgnored(err, initMetaVolIgnoredErrs...) {
					errs[index] = err
					return
				}
			}
			err = disk.MakeVol(minioMetaMultipartBucket)
			if err != nil {
				if !isErrIgnored(err, initMetaVolIgnoredErrs...) {
					errs[index] = err
					return
				}
			}
		}(index, disk)
	}

	// Wait for all cleanup to finish.
	wg.Wait()

	// Return upon first error.
	for _, err := range errs {
		if err == nil {
			continue
		}
		return toObjectErr(err, minioMetaBucket)
	}

	// Return success here.
	return nil
}

// Cleanup a directory recursively.
func cleanupDir(storage StorageAPI, volume, dirPath string) error {
	var delFunc func(string) error
	// Function to delete entries recursively.
	delFunc = func(entryPath string) error {
		if !strings.HasSuffix(entryPath, slashSeparator) {
			// Delete the file entry.
			return traceError(storage.DeleteFile(volume, entryPath))
		}

		// If it's a directory, list and call delFunc() for each entry.
		entries, err := storage.ListDir(volume, entryPath)
		// If entryPath prefix never existed, safe to ignore.
		if err == errFileNotFound {
			return nil
		} else if err != nil { // For any other errors fail.
			return traceError(err)
		} // else on success..

		// Recurse and delete all other entries.
		for _, entry := range entries {
			if err = delFunc(pathJoin(entryPath, entry)); err != nil {
				return err
			}
		}
		return nil
	}
	err := delFunc(retainSlash(pathJoin(dirPath)))
	return err
}
