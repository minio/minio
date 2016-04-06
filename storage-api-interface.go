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

import "io"

// StorageAPI interface.
type StorageAPI interface {
	// Volume operations.
	MakeVol(volume string) (err error)
	ListVols() (vols []VolInfo, err error)
	StatVol(volume string) (vol VolInfo, err error)
	DeleteVol(volume string) (err error)

	// File operations.
	ListFiles(volume, prefix, marker string, recursive bool, count int) (files []FileInfo, isEOF bool, err error)
	ReadFile(volume string, path string, offset int64) (readCloser io.ReadCloser, err error)
	CreateFile(volume string, path string) (writeCloser io.WriteCloser, err error)
	StatFile(volume string, path string) (file FileInfo, err error)
	DeleteFile(volume string, path string) (err error)
}
