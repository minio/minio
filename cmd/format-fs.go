/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"context"
	"os"
	"path"
	"time"

	"github.com/minio/minio/cmd/format"
	"github.com/minio/minio/pkg/lock"
)

// initFormatFS - initializes or loads format.json for minio FS mode.
// It returns read lock to format.json so that another minio FS in NAS does not migrate
// to newer version of format.json; error is returned if wrong format.json i.e. format or
// version mismatch.
//
// It performs below steps;
// 1. Read format.json with read lock.  If it is valid, return the read lock.
// 2. If format.json is not found or migration required, try to get write lock.
// 3. If it is already locked by another minio process in NAS, go to step (1) because
//    another minio process in NAS is trying to create it. Write lock is acquired only
//    if no one has read/write lock.
// 4. By holding write lock here, read format.json (because another minio process in
//    NAS could have created just before getting write lock).
// 5. If it is valid, go to step (1) to do with read lock.
// 6. If format.json is not found or migration required, create/upgrade format.json.
// 7. Go to step (1) to read format.json with read lock.
func initFormatFS(ctx context.Context, fsPath string) (lockedConfigFile *lock.RLockedFile, err error) {
	configFile := path.Join(fsPath, minioMetaBucket, formatConfigFile)

	var f *format.FSV2
	var migrated bool

	writeConfig := func() error {
		var lockedFile *lock.LockedFile

		// Open format.json with write lock.
		if lockedFile, err = lock.TryLockedOpenFile(configFile, os.O_RDWR|os.O_CREATE, os.ModePerm); err != nil {
			return err
		}
		defer lockedFile.Close()

		var fi os.FileInfo
		if fi, err = lockedFile.Stat(); err != nil {
			return err
		}

		// As non-empty format.json exists, check whether it is valid.
		if fi.Size() != 0 {
			if f, migrated, err = format.ParseFS(lockedFile.File); err != nil {
				return err
			}

			// As existing format.json is valid, no need to create/update it.
			if !migrated {
				return nil
			}
		}

		// Migrate backend if format.json is migrated.
		if migrated {
			if err = fsRemoveAll(ctx, path.Join(fsPath, minioMetaMultipartBucket)); err != nil {
				return err
			}

			if err = os.MkdirAll(path.Join(fsPath, minioMetaMultipartBucket), os.ModePerm); err != nil {
				return err
			}
		}

		// Save format in format.json.
		return jsonSave(lockedFile.File, f)
	}

	for {
		// Open format.json with read lock.
		if lockedConfigFile, err = lock.RLockedOpenFile(configFile); err != nil {
			if !os.IsNotExist(err) {
				return nil, err
			}
		} else {
			// As format.json exists, check whether it is valid.
			var fi os.FileInfo
			if fi, err = lockedConfigFile.Stat(); err != nil {
				return nil, err
			}

			if fi.Size() != 0 {
				if f, migrated, err = format.ParseFS(lockedConfigFile.File); err != nil {
					return nil, err
				}

				// As existing format.json is valid, return read lock.
				if !migrated {
					return lockedConfigFile, nil
				}
			}
		}

		// Release read lock to get write lock for writing migrated format configuration.
		if lockedConfigFile != nil {
			lockedConfigFile.Close()
		}

		// If format.json does not exist or empty, create new configuration.
		if f == nil {
			f = format.NewFSV2()
		}

		if err = writeConfig(); err != nil {
			if err != lock.ErrAlreadyLocked {
				return nil, err
			}

			// Retry with little delay.
			time.Sleep(100 * time.Millisecond)
		}
	}
}
