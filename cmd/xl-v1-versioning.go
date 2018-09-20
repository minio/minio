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
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"crypto/sha1"
	"github.com/minio/minio/cmd/logger"
	"encoding/base64"
)

// XL versioning constants.
const (
	// XL meta version.
	xlVersioningVersion = "1.0.0"

	// XL meta format string.
	xlVersioningFormat = "xl"

	// XL json versions file
	xlVersioningJSONFile = "versions.json"

	// Add new constants here.
)

// A xlVersioningV1 represents `versioning.json` metadata header.
type xlVersioningV1 struct {
	Version string `json:"version"` // Version of the current `versioning.json`.
	Format  string `json:"format"`  // Format of the current `versioning.json`.
	// Minio release tag for current object `versioning.json`.
	Minio struct {
		Release string `json:"release"`
	} `json:"minio"`
	ModTime        time.Time         `json:"modTime"` // ModTime of the object `versioning.json`.
	ObjectVersions []xlObjectVersion `json:"objectVersions"`
}

type xlObjectVersion struct {
	Id           string    `json:"id"`                     // Object version id
	DeleteMarker bool      `json:"deleteMarker,omitempty"` // Delete marker for this version
	TimeStamp    time.Time `json:"timeStamp"`              // Timestamp for this version
}

// newXLVersioningV1 - initializes new xlVersioningV1, adds version
func newXLVersioningV1() (xlVersioning xlVersioningV1) {
	xlVersioning = xlVersioningV1{}
	xlVersioning.Version = xlVersioningVersion
	xlVersioning.Format = xlVersioningFormat
	xlVersioning.Minio.Release = ReleaseTag
	xlVersioning.ModTime = time.Now().UTC()
	return xlVersioning
}

// IsValid - tells if the format is sane by validating the version
// string, format and erasure info fields.
func (m xlVersioningV1) IsValid() bool {
	return isXLVersioningFormatValid(m.Version, m.Format)
}

// DeriveVersionId derives a pseudo-random, yet deterministic, versionId
// It is meant to generate identical versionIds across replicated buckets
func (m xlVersioningV1) DeriveVersionId(object, etag string) string {

	h := sha1.New()
	// Derive hash from concatenation of the base of the key name of object, an index and the etag
	// Note that the etag can be empty for delete markers
	s := fmt.Sprintf("%s;%d;%s", path.Base(object), len(m.ObjectVersions)+1, etag)
	h.Write([]byte(s))
	bts := h.Sum(nil)

	return base64.RawURLEncoding.EncodeToString(bts)
}

// FindVersion gets the corresponding index of a version (if any)
func (m xlVersioningV1) FindVersion(version string) (idx int, found bool) {
	idx = len(m.ObjectVersions)
	for i, vo := range m.ObjectVersions {
		if version == vo.Id {
			idx, found = i, true
			return
		}
	}
	return
}

// Verifies if the backend format versioning is sane by validating
// the version string and format style.
func isXLVersioningFormatValid(version, format string) bool {
	return (version == xlVersioningVersion) && (format == xlVersioningFormat)
}

// pickValidXLVersioning - picks one valid xlVersioning content and returns from a
// slice of xlmeta content.
func pickValidXLVersioning(ctx context.Context, metaArr []xlVersioningV1, modTime time.Time) (xmv xlVersioningV1, e error) {
	// Pick latest valid metadata.
	for _, meta := range metaArr {
		if meta.IsValid() && meta.ModTime.Equal(modTime) {
			return meta, nil
		}
	}
	err := fmt.Errorf("No valid versioning.json present")
	logger.LogIf(ctx, err)
	return xmv, err
}

// list of all errors that can be ignored in a versioning operation.
var objVersioningOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errVolumeNotFound, errFileNotFound, errFileAccessDenied, errCorruptedFormat)

// deleteXLVersioning - deletes `versioning.json` on a single disk.
func deleteXLVersioning(ctx context.Context, disk StorageAPI, bucket, prefix string) error {
	jsonFile := path.Join(prefix, xlVersioningJSONFile)
	err := disk.DeleteFile(bucket, jsonFile)
	logger.LogIf(ctx, err)
	return err
}

// writeXLVersioning - writes `versioning.json` to a single disk.
func writeXLVersioning(ctx context.Context, disk StorageAPI, bucket, prefix string, xlVersioning xlVersioningV1) error {
	jsonFile := path.Join(prefix, xlVersioningJSONFile)

	// Marshal json.
	metadataBytes, err := json.Marshal(&xlVersioning)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	// Persist marshaled data.
	err = disk.AppendFile(bucket, jsonFile, metadataBytes)
	logger.LogIf(ctx, err)
	return err
}

// deleteAllXLVersioning - deletes all partially written `versioning.json` depending on errs.
func deleteAllXLVersioning(ctx context.Context, disks []StorageAPI, bucket, prefix string, errs []error) {
	var wg = &sync.WaitGroup{}
	// Delete all the `versioning.json` left over.
	for index, disk := range disks {
		if disk == nil {
			continue
		}
		// Undo rename object in parallel.
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			if errs[index] != nil {
				return
			}
			_ = deleteXLVersioning(ctx, disk, bucket, prefix)
		}(index, disk)
	}
	wg.Wait()
}

// Rename `versioning.json` content to destination location for each disk in order.
func renameXLVersioning(ctx context.Context, disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, quorum int) ([]StorageAPI, error) {
	isDir := false
	srcXLJSON := path.Join(srcEntry, xlVersioningJSONFile)
	dstXLJSON := path.Join(dstEntry, xlVersioningJSONFile)
	return rename(ctx, disks, srcBucket, srcXLJSON, dstBucket, dstXLJSON, isDir, quorum, []error{errFileNotFound})
}

// writeUniqueXLVersioning - writes unique `versioning.json` content for each disk in order.
func writeUniqueXLVersioning(ctx context.Context, disks []StorageAPI, bucket, prefix string, xlVersionings []xlVersioningV1, quorum int) ([]StorageAPI, error) {
	var wg = &sync.WaitGroup{}
	var mErrs = make([]error, len(disks))

	// Start writing `versioning.json` to all disks in parallel.
	for index, disk := range disks {
		if disk == nil {
			logger.LogIf(ctx, errDiskNotFound)
			mErrs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Write `versioning.json` in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()

			// Write unique `versioning.json` for a disk at index.
			err := writeXLVersioning(ctx, disk, bucket, prefix, xlVersionings[index])
			if err != nil {
				mErrs[index] = err
			}
		}(index, disk)
	}

	// Wait for all the routines.
	wg.Wait()

	err := reduceWriteQuorumErrs(ctx, mErrs, objectOpIgnoredErrs, quorum)
	if err == errXLWriteQuorum {
		// Delete all `versioning.json` successfully renamed.
		deleteAllXLVersioning(ctx, disks, bucket, prefix, mErrs)
	}
	return evalDisks(disks, mErrs), err
}

// writeSameXLVersioning - write `versioning.json` on all disks in order.
func writeSameXLVersioning(ctx context.Context, disks []StorageAPI, bucket, prefix string, xlVersioning xlVersioningV1, writeQuorum int) ([]StorageAPI, error) {
	var wg = &sync.WaitGroup{}
	var mErrs = make([]error, len(disks))

	// Start writing `versioning.json` to all disks in parallel.
	for index, disk := range disks {
		if disk == nil {
			logger.LogIf(ctx, errDiskNotFound)
			mErrs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Write `versioning.json` in a routine.
		go func(index int, disk StorageAPI, metadata xlVersioningV1) {
			defer wg.Done()

			// Write xl metadata.
			err := writeXLVersioning(ctx, disk, bucket, prefix, metadata)
			if err != nil {
				mErrs[index] = err
			}
		}(index, disk, xlVersioning)
	}

	// Wait for all the routines.
	wg.Wait()

	err := reduceWriteQuorumErrs(ctx, mErrs, objectOpIgnoredErrs, writeQuorum)
	if err == errXLWriteQuorum {
		// Delete all `versioning.json` successfully renamed.
		deleteAllXLVersioning(ctx, disks, bucket, prefix, mErrs)
	}
	return evalDisks(disks, mErrs), err
}
