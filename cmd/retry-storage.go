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

	"github.com/minio/minio/pkg/disk"
)

const (
	// Attempt to retry only this many number of times before
	// giving up on the remote disk entirely during initialization.
	globalStorageInitRetryThreshold = 4

	// Attempt to retry only this many number of times before
	// giving up on the remote disk entirely after initialization.
	globalStorageRetryThreshold = 1

	// Interval to check health status of a node whether it has
	// come back up online
	globalStorageHealthCheckInterval = 5 * time.Minute
)

// Converts rpc.ServerError to underlying error. This function is
// written so that the storageAPI errors are consistent across network
// disks as well.
func retryToStorageErr(err error) error {
	if err == errDiskNotFoundFromNetError || err == errDiskNotFoundFromRPCShutdown {
		return errDiskNotFound
	}
	return err
}

// Retry storage is an instance of StorageAPI which
// additionally verifies upon network shutdown if the
// underlying storage is available and is really
// formatted. After the initialization phase it will
// also cache when the underlying storage is offline
// to prevent needless calls and recheck the health of
// underlying storage in regular intervals.
type retryStorage struct {
	remoteStorage    StorageAPI
	maxRetryAttempts int
	retryUnit        time.Duration
	retryCap         time.Duration
	offline          bool      // Mark whether node is offline
	offlineTimestamp time.Time // Last timestamp of checking status of node
}

// String representation of remoteStorage.
func (f *retryStorage) String() string {
	return f.remoteStorage.String()
}

// Reconnects to underlying remote storage.
func (f *retryStorage) Init() (err error) {
	return retryToStorageErr(f.remoteStorage.Init())
}

// Closes the underlying remote storage connection.
func (f *retryStorage) Close() (err error) {
	return retryToStorageErr(f.remoteStorage.Close())
}

// Return whether the underlying remote storage is offline
// and, if so, try to reconnect at regular intervals to
// restore the connection
func (f *retryStorage) IsOffline() bool {
	// Check if offline and whether enough time has lapsed since most recent check
	if f.offline && UTCNow().Sub(f.offlineTimestamp) >= globalStorageHealthCheckInterval {
		f.offlineTimestamp = UTCNow() // reset timestamp

		if e := f.reInit(nil); e == nil {
			// Connection has been re-established
			f.offline = false // Mark node as back online
		}
	}
	return f.offline
}

// DiskInfo - a retryable implementation of disk info.
func (f *retryStorage) DiskInfo() (info disk.Info, err error) {
	if f.IsOffline() {
		return info, errDiskNotFound
	}
	info, err = f.remoteStorage.DiskInfo()
	if f.reInitUponDiskNotFound(err) {
		info, err = f.remoteStorage.DiskInfo()
		return info, retryToStorageErr(err)
	}
	return info, retryToStorageErr(err)
}

// MakeVol - a retryable implementation of creating a volume.
func (f *retryStorage) MakeVol(volume string) (err error) {
	if f.IsOffline() {
		return errDiskNotFound
	}
	err = f.remoteStorage.MakeVol(volume)
	if f.reInitUponDiskNotFound(err) {
		return retryToStorageErr(f.remoteStorage.MakeVol(volume))
	}
	return retryToStorageErr(err)
}

// ListVols - a retryable implementation of listing all the volumes.
func (f *retryStorage) ListVols() (vols []VolInfo, err error) {
	if f.IsOffline() {
		return vols, errDiskNotFound
	}
	vols, err = f.remoteStorage.ListVols()
	if f.reInitUponDiskNotFound(err) {
		vols, err = f.remoteStorage.ListVols()
		return vols, retryToStorageErr(err)
	}
	return vols, retryToStorageErr(err)
}

// StatVol - a retryable implementation of stating a volume.
func (f *retryStorage) StatVol(volume string) (vol VolInfo, err error) {
	if f.IsOffline() {
		return vol, errDiskNotFound
	}
	vol, err = f.remoteStorage.StatVol(volume)
	if f.reInitUponDiskNotFound(err) {
		vol, err = f.remoteStorage.StatVol(volume)
		return vol, retryToStorageErr(err)
	}
	return vol, retryToStorageErr(err)
}

// DeleteVol - a retryable implementation of deleting a volume.
func (f *retryStorage) DeleteVol(volume string) (err error) {
	if f.IsOffline() {
		return errDiskNotFound
	}
	err = f.remoteStorage.DeleteVol(volume)
	if f.reInitUponDiskNotFound(err) {
		return retryToStorageErr(f.remoteStorage.DeleteVol(volume))
	}
	return retryToStorageErr(err)
}

// PrepareFile - a retryable implementation of preparing a file.
func (f *retryStorage) PrepareFile(volume, path string, length int64) (err error) {
	if f.IsOffline() {
		return errDiskNotFound
	}
	err = f.remoteStorage.PrepareFile(volume, path, length)
	if f.reInitUponDiskNotFound(err) {
		return retryToStorageErr(f.remoteStorage.PrepareFile(volume, path, length))
	}
	return retryToStorageErr(err)
}

// AppendFile - a retryable implementation of append to a file.
func (f *retryStorage) AppendFile(volume, path string, buffer []byte) (err error) {
	if f.IsOffline() {
		return errDiskNotFound
	}
	err = f.remoteStorage.AppendFile(volume, path, buffer)
	if f.reInitUponDiskNotFound(err) {
		return retryToStorageErr(f.remoteStorage.AppendFile(volume, path, buffer))
	}
	return retryToStorageErr(err)
}

// StatFile - a retryable implementation of stating a file.
func (f *retryStorage) StatFile(volume, path string) (fileInfo FileInfo, err error) {
	if f.IsOffline() {
		return fileInfo, errDiskNotFound
	}
	fileInfo, err = f.remoteStorage.StatFile(volume, path)
	if f.reInitUponDiskNotFound(err) {
		fileInfo, err = f.remoteStorage.StatFile(volume, path)
		return fileInfo, retryToStorageErr(err)
	}
	return fileInfo, retryToStorageErr(err)
}

// ReadAll - a retryable implementation of reading all the content from a file.
func (f *retryStorage) ReadAll(volume, path string) (buf []byte, err error) {
	if f.IsOffline() {
		return buf, errDiskNotFound
	}
	buf, err = f.remoteStorage.ReadAll(volume, path)
	if f.reInitUponDiskNotFound(err) {
		buf, err = f.remoteStorage.ReadAll(volume, path)
		return buf, retryToStorageErr(err)
	}
	return buf, retryToStorageErr(err)
}

// ReadFile - a retryable implementation of reading at offset from a file.
func (f *retryStorage) ReadFile(volume, path string, offset int64, buffer []byte, verifier *BitrotVerifier) (m int64, err error) {
	if f.IsOffline() {
		return m, errDiskNotFound
	}
	m, err = f.remoteStorage.ReadFile(volume, path, offset, buffer, verifier)
	if f.reInitUponDiskNotFound(err) {
		m, err = f.remoteStorage.ReadFile(volume, path, offset, buffer, verifier)
		return m, retryToStorageErr(err)
	}
	return m, retryToStorageErr(err)
}

// ListDir - a retryable implementation of listing directory entries.
func (f *retryStorage) ListDir(volume, path string) (entries []string, err error) {
	if f.IsOffline() {
		return entries, errDiskNotFound
	}
	entries, err = f.remoteStorage.ListDir(volume, path)
	if f.reInitUponDiskNotFound(err) {
		entries, err = f.remoteStorage.ListDir(volume, path)
		return entries, retryToStorageErr(err)
	}
	return entries, retryToStorageErr(err)
}

// DeleteFile - a retryable implementation of deleting a file.
func (f *retryStorage) DeleteFile(volume, path string) (err error) {
	if f.IsOffline() {
		return errDiskNotFound
	}
	err = f.remoteStorage.DeleteFile(volume, path)
	if f.reInitUponDiskNotFound(err) {
		return retryToStorageErr(f.remoteStorage.DeleteFile(volume, path))
	}
	return retryToStorageErr(err)
}

// RenameFile - a retryable implementation of renaming a file.
func (f *retryStorage) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	if f.IsOffline() {
		return errDiskNotFound
	}
	err = f.remoteStorage.RenameFile(srcVolume, srcPath, dstVolume, dstPath)
	if f.reInitUponDiskNotFound(err) {
		return retryToStorageErr(f.remoteStorage.RenameFile(srcVolume, srcPath, dstVolume, dstPath))
	}
	return retryToStorageErr(err)
}

// Try to reinitialize the connection when we have some form of DiskNotFound error
func (f *retryStorage) reInitUponDiskNotFound(err error) bool {
	if err == errDiskNotFound || err == errDiskNotFoundFromNetError || err == errDiskNotFoundFromRPCShutdown {
		return f.reInit(err) == nil
	}
	return false
}

// Connect and attempt to load the format from a disconnected node,
// attempts three times before giving up.
func (f *retryStorage) reInit(e error) (err error) {

	// Only after initialization and minimum of one interval
	// has passed (to prevent marking a node as offline right
	// after initialization), check whether node has gone offline
	if f.maxRetryAttempts == globalStorageRetryThreshold &&
		UTCNow().Sub(f.offlineTimestamp) >= globalStorageHealthCheckInterval {
		if e == errDiskNotFoundFromNetError { // Make node offline due to network error
			f.offline = true // Marking node offline
			f.offlineTimestamp = UTCNow()
			return errDiskNotFound
		}
		// Continue for other errors like RPC shutdown (and retry connection below)
	}

	// Close the underlying connection.
	f.remoteStorage.Close() // Error here is purposefully ignored.

	// Done channel is used to close any lingering retry routine, as soon
	// as this function returns.
	doneCh := make(chan struct{})
	defer close(doneCh)

	for i := range newRetryTimer(f.retryUnit, f.retryCap, doneCh) {
		// Initialize and make a new login attempt.
		err = f.remoteStorage.Init()
		if err != nil {
			// No need to return error until the retry count
			// threshold has reached.
			if i < f.maxRetryAttempts {
				continue
			}
			return err
		}

		// Attempt to load format to see if the disk is really
		// a formatted disk and part of the cluster.
		_, err = loadFormat(f.remoteStorage)
		if err != nil {
			// No need to return error until the retry count
			// threshold has reached.
			if i < f.maxRetryAttempts {
				continue
			}
			return err
		}

		// Login and loading format was a success, break and proceed forward.
		break
	}
	return err
}
