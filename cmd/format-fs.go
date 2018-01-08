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
	"fmt"
	"io"
	"os"
	"path"
	"time"

	errors2 "github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/lock"
)

// FS format version strings.
const (
	formatBackendFS   = "fs"
	formatFSVersionV1 = "1"
	formatFSVersionV2 = "2"
)

// formatFSV1 - structure holds format version '1'.
type formatFSV1 struct {
	formatMetaV1
	FS struct {
		Version string `json:"version"`
	} `json:"fs"`
}

// formatFSV2 - structure is same as formatFSV1. But the multipart backend
// structure is flat instead of hierarchy now.
// In .minio.sys/multipart we have:
// sha256(bucket/object)/uploadID/[fs.json, 1.etag, 2.etag ....]
type formatFSV2 = formatFSV1

// Used to detect the version of "fs" format.
type formatFSVersionDetect struct {
	FS struct {
		Version string `json:"version"`
	} `json:"fs"`
}

// Returns the latest "fs" format V1
func newFormatFSV1() (format *formatFSV1) {
	f := &formatFSV1{}
	f.Version = formatMetaVersionV1
	f.Format = formatBackendFS
	f.FS.Version = formatFSVersionV1
	return f
}

// Returns the latest "fs" format V2
func newFormatFSV2() (format *formatFSV2) {
	f := &formatFSV2{}
	f.Version = formatMetaVersionV1
	f.Format = formatBackendFS
	f.FS.Version = formatFSVersionV2
	return f
}

// Returns the field formatMetaV1.Format i.e the string "fs" which is never likely to change.
// We do not use this function in XL to get the format as the file is not fcntl-locked on XL.
func formatMetaGetFormatBackendFS(r io.ReadSeeker) (string, error) {
	format := &formatMetaV1{}
	if err := jsonLoad(r, format); err != nil {
		return "", err
	}
	if format.Version == formatMetaVersionV1 {
		return format.Format, nil
	}
	return "", fmt.Errorf(`format.Version expected: %s, got: %s`, formatMetaVersionV1, format.Version)
}

// Returns formatFS.FS.Version
func formatFSGetVersion(r io.ReadSeeker) (string, error) {
	format := &formatFSVersionDetect{}
	if err := jsonLoad(r, format); err != nil {
		return "", err
	}
	return format.FS.Version, nil
}

// Migrate from V1 to V2. V2 implements new backend format for multipart
// uploads. Delete the previous multipart directory.
func formatFSMigrateV1ToV2(wlk *lock.LockedFile, fsPath string) error {
	version, err := formatFSGetVersion(wlk)
	if err != nil {
		return err
	}

	if version != formatFSVersionV1 {
		return fmt.Errorf(`format.json version expected %s, found %s`, formatFSVersionV1, version)
	}

	if err = fsRemoveAll(path.Join(fsPath, minioMetaMultipartBucket)); err != nil {
		return err
	}

	if err = os.MkdirAll(path.Join(fsPath, minioMetaMultipartBucket), 0755); err != nil {
		return err
	}

	return jsonSave(wlk.File, newFormatFSV2())
}

// Migrate the "fs" backend.
// Migration should happen when formatFSV1.FS.Version changes. This version
// can change when there is a change to the struct formatFSV1.FS or if there
// is any change in the backend file system tree structure.
func formatFSMigrate(wlk *lock.LockedFile, fsPath string) error {
	// Add any migration code here in case we bump format.FS.Version
	version, err := formatFSGetVersion(wlk)
	if err != nil {
		return err
	}

	switch version {
	case formatFSVersionV1:
		if err = formatFSMigrateV1ToV2(wlk, fsPath); err != nil {
			return err
		}
		fallthrough
	case formatFSVersionV2:
		// We are at the latest version.
	}

	// Make sure that the version is what we expect after the migration.
	version, err = formatFSGetVersion(wlk)
	if err != nil {
		return err
	}
	if version != formatFSVersionV2 {
		return fmt.Errorf(`%s file: expected FS version: %s, found FS version: %s`, formatConfigFile, formatFSVersionV2, version)
	}
	return nil
}

// Creates a new format.json if unformatted.
func createFormatFS(fsFormatPath string) error {
	// Attempt a write lock on formatConfigFile `format.json`
	// file stored in minioMetaBucket(.minio.sys) directory.
	lk, err := lock.TryLockedOpenFile(fsFormatPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return errors2.Trace(err)
	}
	// Close the locked file upon return.
	defer lk.Close()

	fi, err := lk.Stat()
	if err != nil {
		return errors2.Trace(err)
	}
	if fi.Size() != 0 {
		// format.json already got created because of another minio process's createFormatFS()
		return nil
	}

	return jsonSave(lk.File, newFormatFSV1())
}

// This function returns a read-locked format.json reference to the caller.
// The file descriptor should be kept open throughout the life
// of the process so that another minio process does not try to
// migrate the backend when we are actively working on the backend.
func initFormatFS(fsPath string) (rlk *lock.RLockedFile, err error) {
	fsFormatPath := pathJoin(fsPath, minioMetaBucket, formatConfigFile)
	// Any read on format.json should be done with read-lock.
	// Any write on format.json should be done with write-lock.
	for {
		isEmpty := false
		rlk, err := lock.RLockedOpenFile(fsFormatPath)
		if err == nil {
			// format.json can be empty in a rare condition when another
			// minio process just created the file but could not hold lock
			// and write to it.
			var fi os.FileInfo
			fi, err = rlk.Stat()
			if err != nil {
				return nil, errors2.Trace(err)
			}
			isEmpty = fi.Size() == 0
		}
		if os.IsNotExist(err) || isEmpty {
			if err == nil {
				rlk.Close()
			}
			// Fresh disk - create format.json
			err = createFormatFS(fsFormatPath)
			if err == lock.ErrAlreadyLocked {
				// Lock already present, sleep and attempt again.
				// Can happen in a rare situation when a parallel minio process
				// holds the lock and creates format.json
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if err != nil {
				return nil, errors2.Trace(err)
			}
			// After successfully creating format.json try to hold a read-lock on
			// the file.
			continue
		}
		if err != nil {
			return nil, errors2.Trace(err)
		}

		formatBackend, err := formatMetaGetFormatBackendFS(rlk)
		if err != nil {
			return nil, errors2.Trace(err)
		}
		if formatBackend != formatBackendFS {
			return nil, fmt.Errorf(`%s file: expected format-type: %s, found: %s`, formatConfigFile, formatBackendFS, formatBackend)
		}
		version, err := formatFSGetVersion(rlk)
		if err != nil {
			return nil, err
		}
		if version != formatFSVersionV2 {
			// Format needs migration
			rlk.Close()
			// Hold write lock during migration so that we do not disturb any
			// minio processes running in parallel.
			wlk, err := lock.TryLockedOpenFile(fsFormatPath, os.O_RDWR, 0)
			if err == lock.ErrAlreadyLocked {
				// Lock already present, sleep and attempt again.
				wlk.Close()
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if err != nil {
				return nil, err
			}
			err = formatFSMigrate(wlk, fsPath)
			wlk.Close()
			if err != nil {
				// Migration failed, bail out so that the user can observe what happened.
				return nil, err
			}
			// Successfully migrated, now try to hold a read-lock on format.json
			continue
		}

		return rlk, nil
	}
}
