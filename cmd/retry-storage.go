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
)

// Retry storage is an instance of StorageAPI which
// additionally verifies upon network shutdown if the
// underlying storage is available and is really
// formatted.
type retryStorage struct {
	remoteStorage    StorageAPI
	maxRetryAttempts int
	retryUnit        time.Duration
	retryCap         time.Duration
}

// String representation of remoteStorage.
func (f retryStorage) String() string {
	return f.remoteStorage.String()
}

// Reconncts to underlying remote storage.
func (f retryStorage) Init() (err error) {
	return f.remoteStorage.Init()
}

// Closes the underlying remote storage connection.
func (f retryStorage) Close() (err error) {
	return f.remoteStorage.Close()
}

// DiskInfo - a retryable implementation of disk info.
func (f retryStorage) DiskInfo() (info disk.Info, err error) {
	info, err = f.remoteStorage.DiskInfo()
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.DiskInfo()
		}
	}
	return info, err
}

// MakeVol - a retryable implementation of creating a volume.
func (f retryStorage) MakeVol(volume string) (err error) {
	err = f.remoteStorage.MakeVol(volume)
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.MakeVol(volume)
		}
	}
	return err
}

// ListVols - a retryable implementation of listing all the volumes.
func (f retryStorage) ListVols() (vols []VolInfo, err error) {
	vols, err = f.remoteStorage.ListVols()
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.ListVols()
		}
	}
	return vols, err
}

// StatVol - a retryable implementation of stating a volume.
func (f retryStorage) StatVol(volume string) (vol VolInfo, err error) {
	vol, err = f.remoteStorage.StatVol(volume)
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.StatVol(volume)
		}
	}
	return vol, err
}

// DeleteVol - a retryable implementation of deleting a volume.
func (f retryStorage) DeleteVol(volume string) (err error) {
	err = f.remoteStorage.DeleteVol(volume)
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.DeleteVol(volume)
		}
	}
	return err
}

// PrepareFile - a retryable implementation of preparing a file.
func (f retryStorage) PrepareFile(volume, path string, length int64) (err error) {
	err = f.remoteStorage.PrepareFile(volume, path, length)
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.PrepareFile(volume, path, length)
		}
	}
	return err
}

// AppendFile - a retryable implementation of append to a file.
func (f retryStorage) AppendFile(volume, path string, buffer []byte) (err error) {
	err = f.remoteStorage.AppendFile(volume, path, buffer)
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.AppendFile(volume, path, buffer)
		}
	}
	return err
}

// StatFile - a retryable implementation of stating a file.
func (f retryStorage) StatFile(volume, path string) (fileInfo FileInfo, err error) {
	fileInfo, err = f.remoteStorage.StatFile(volume, path)
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.StatFile(volume, path)
		}
	}
	return fileInfo, err
}

// ReadAll - a retryable implementation of reading all the content from a file.
func (f retryStorage) ReadAll(volume, path string) (buf []byte, err error) {
	buf, err = f.remoteStorage.ReadAll(volume, path)
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.ReadAll(volume, path)
		}
	}
	return buf, err
}

// ReadFile - a retryable implementation of reading at offset from a file.
func (f retryStorage) ReadFile(volume, path string, offset int64, buffer []byte) (m int64, err error) {
	m, err = f.remoteStorage.ReadFile(volume, path, offset, buffer)
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.ReadFile(volume, path, offset, buffer)
		}
	}
	return m, err
}

// ListDir - a retryable implementation of listing directory entries.
func (f retryStorage) ListDir(volume, path string) (entries []string, err error) {
	entries, err = f.remoteStorage.ListDir(volume, path)
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.ListDir(volume, path)
		}
	}
	return entries, err
}

// DeleteFile - a retryable implementation of deleting a file.
func (f retryStorage) DeleteFile(volume, path string) (err error) {
	err = f.remoteStorage.DeleteFile(volume, path)
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.DeleteFile(volume, path)
		}
	}
	return err
}

// RenameFile - a retryable implementation of renaming a file.
func (f retryStorage) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	err = f.remoteStorage.RenameFile(srcVolume, srcPath, dstVolume, dstPath)
	if err == errDiskNotFound {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.RenameFile(srcVolume, srcPath, dstVolume, dstPath)
		}
	}
	return err
}

// Connect and attempt to load the format from a disconnected node,
// attempts three times before giving up.
func (f retryStorage) reInit() (err error) {
	// Close the underlying connection.
	f.remoteStorage.Close() // Error here is purposefully ignored.

	doneCh := make(chan struct{})
	defer close(doneCh)
	for i := range newRetryTimer(f.retryUnit, f.retryCap, MaxJitter, doneCh) {
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
