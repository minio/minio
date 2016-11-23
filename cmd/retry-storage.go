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
	"net/rpc"

	"github.com/minio/minio/pkg/disk"
)

// Retry storage is an instance of StorageAPI which
// additionally verifies upon network shutdown if the
// underlying storage is available and is really
// formatted.
type retryStorage struct {
	remoteStorage StorageAPI
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
	if err == rpc.ErrShutdown {
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
	if err == rpc.ErrShutdown {
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
	if err == rpc.ErrShutdown {
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
	if err == rpc.ErrShutdown {
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
	if err == rpc.ErrShutdown {
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
	if err == rpc.ErrShutdown {
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
	if err == rpc.ErrShutdown {
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
	if err == rpc.ErrShutdown {
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
	if err == rpc.ErrShutdown {
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
	if err == rpc.ErrShutdown {
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
	if err == rpc.ErrShutdown {
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
	if err == rpc.ErrShutdown {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.DeleteFile(volume, path)
		}
	}
	return err
}

// Connect and attempt to load the format from a disconnected node.
func (f retryStorage) reInit() (err error) {
	err = f.remoteStorage.Close()
	if err != nil {
		return err
	}
	err = f.remoteStorage.Init()
	if err == nil {
		_, err = loadFormat(f.remoteStorage)
		// For load format returning network shutdown
		// we now treat it like disk not available.
		if err == rpc.ErrShutdown {
			err = errDiskNotFound
		}
		return err
	}
	if err == rpc.ErrShutdown {
		err = errDiskNotFound
	}
	return err
}

// RenameFile - a retryable implementation of renaming a file.
func (f retryStorage) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	err = f.remoteStorage.RenameFile(srcVolume, srcPath, dstVolume, dstPath)
	if err == rpc.ErrShutdown {
		err = f.reInit()
		if err == nil {
			return f.remoteStorage.RenameFile(srcVolume, srcPath, dstVolume, dstPath)
		}
	}
	return err
}
