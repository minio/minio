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

import "strings"

// Common initialization needed for both object layers.
func initObjectLayer(storageDisks ...StorageAPI) error {
	// This happens for the first time, but keep this here since this
	// is the only place where it can be made expensive optimizing all
	// other calls. Create minio meta volume, if it doesn't exist yet.
	for _, storage := range storageDisks {
		if err := storage.MakeVol(minioMetaBucket); err != nil {
			if err != errVolumeExists && err != errDiskNotFound {
				return toObjectErr(err, minioMetaBucket)
			}
		}
		// Cleanup all temp entries upon start.
		err := cleanupDir(storage, minioMetaBucket, tmpMetaPrefix)
		if err != nil {
			return toObjectErr(err, minioMetaBucket, tmpMetaPrefix)
		}
	}
	return nil
}

// Cleanup a directory recursively.
func cleanupDir(storage StorageAPI, volume, dirPath string) error {
	var delFunc func(string) error
	// Function to delete entries recursively.
	delFunc = func(entryPath string) error {
		if !strings.HasSuffix(entryPath, slashSeparator) {
			// No trailing "/" means that this is a file which can be deleted.
			return storage.DeleteFile(volume, entryPath)
		}
		// If it's a directory, list and call delFunc() for each entry.
		entries, err := storage.ListDir(volume, entryPath)
		if err != nil {
			if err == errFileNotFound {
				// if dirPath prefix never existed.
				return nil
			}
			return err
		}
		for _, entry := range entries {
			err = delFunc(pathJoin(entryPath, entry))
			if err != nil {
				return err
			}
		}
		return nil
	}
	return delFunc(retainSlash(pathJoin(dirPath)))
}
