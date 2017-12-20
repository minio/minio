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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	errors2 "github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/lock"
)

// FS format version strings.
const (
	formatBackendFS   = "fs"
	formatFSVersionV1 = "1"
)

// formatFSV1 - structure holds format version '1'.
type formatFSV1 struct {
	formatMetaV1
	FS struct {
		Version string `json:"version"`
	} `json:"fs"`
}

// Used to detect the version of "fs" format.
type formatFSVersionDetect struct {
	FS struct {
		Version string `json:"version"`
	} `json:"fs"`
}

// Returns the latest "fs" format.
func newFormatFSV1() (format *formatFSV1) {
	f := &formatFSV1{}
	f.Version = formatMetaVersionV1
	f.Format = formatBackendFS
	f.FS.Version = formatFSVersionV1
	return f
}

// Save to fs format.json
func formatFSSave(f *os.File, data interface{}) error {
	b, err := json.Marshal(data)
	if err != nil {
		return errors2.Trace(err)
	}
	if err = f.Truncate(0); err != nil {
		return errors2.Trace(err)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err = f.Write(b)
	if err != nil {
		return errors2.Trace(err)
	}
	return nil
}

// Returns the field formatMetaV1.Format i.e the string "fs" which is never likely to change.
// We do not use this function in XL to get the format as the file is not fcntl-locked on XL.
func formatMetaGetFormatBackendFS(r io.ReadSeeker) (string, error) {
	format := &formatMetaV1{}
	if err := jsonLoadFromSeeker(r, format); err != nil {
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
	if err := jsonLoadFromSeeker(r, format); err != nil {
		return "", err
	}
	return format.FS.Version, nil
}

// Migrate the "fs" backend.
// Migration should happen when formatFSV1.FS.Version changes. This version
// can change when there is a change to the struct formatFSV1.FS or if there
// is any change in the backend file system tree structure.
func formatFSMigrate(wlk *lock.LockedFile) error {
	// Add any migration code here in case we bump format.FS.Version

	// Make sure that the version is what we expect after the migration.
	version, err := formatFSGetVersion(wlk)
	if err != nil {
		return err
	}
	if version != formatFSVersionV1 {
		return fmt.Errorf(`%s file: expected FS version: %s, found FS version: %s`, formatConfigFile, formatFSVersionV1, version)
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

	return formatFSSave(lk.File, newFormatFSV1())
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
		if version != formatFSVersionV1 {
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
			err = formatFSMigrate(wlk)
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
