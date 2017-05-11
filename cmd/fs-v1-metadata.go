/*
 * Minio Cloud Storage, (C) 2016, 2017, 2017 Minio, Inc.
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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	pathutil "path"
	"sort"
	"strings"

	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/pkg/lock"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/tidwall/gjson"
)

// FS format, and object metadata.
const (
	// fs.json object metadata.
	fsMetaJSONFile = "fs.json"
	// format.json FS format metadata.
	fsFormatJSONFile = "format.json"
)

// FS metadata constants.
const (
	// FS backend meta 1.0.0 version.
	fsMetaVersion100 = "1.0.0"

	// FS backend meta 1.0.1 version.
	fsMetaVersion = "1.0.1"

	// FS backend meta format.
	fsMetaFormat = "fs"

	// FS backend format version.
	fsFormatVersion = fsFormatV2

	// Add more constants here.
)

// A fsMetaV1 represents a metadata header mapping keys to sets of values.
type fsMetaV1 struct {
	Version string `json:"version"`
	Format  string `json:"format"`
	Minio   struct {
		Release string `json:"release"`
	} `json:"minio"`
	// Metadata map for current object `fs.json`.
	Meta  map[string]string `json:"meta,omitempty"`
	Parts []objectPartInfo  `json:"parts,omitempty"`
}

// IsValid - tells if the format is sane by validating the version
// string and format style.
func (m fsMetaV1) IsValid() bool {
	return isFSMetaValid(m.Version, m.Format)
}

// Verifies if the backend format metadata is sane by validating
// the version string and format style.
func isFSMetaValid(version, format string) bool {
	return ((version == fsMetaVersion || version == fsMetaVersion100) &&
		format == fsMetaFormat)
}

// Converts metadata to object info.
func (m fsMetaV1) ToObjectInfo(bucket, object string, fi os.FileInfo) ObjectInfo {
	if len(m.Meta) == 0 {
		m.Meta = make(map[string]string)
	}

	// Guess content-type from the extension if possible.
	if m.Meta["content-type"] == "" {
		if objectExt := pathutil.Ext(object); objectExt != "" {
			if content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]; ok {
				m.Meta["content-type"] = content.ContentType
			}
		}
	}

	objInfo := ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}

	// We set file info only if its valid.
	objInfo.ModTime = timeSentinel
	if fi != nil {
		objInfo.ModTime = fi.ModTime()
		objInfo.Size = fi.Size()
		objInfo.IsDir = fi.IsDir()
	}

	// Extract etag from metadata.
	objInfo.ETag = extractETag(m.Meta)
	objInfo.ContentType = m.Meta["content-type"]
	objInfo.ContentEncoding = m.Meta["content-encoding"]

	// etag/md5Sum has already been extracted. We need to
	// remove to avoid it from appearing as part of
	// response headers. e.g, X-Minio-* or X-Amz-*.
	objInfo.UserDefined = cleanMetaETag(m.Meta)

	// Success..
	return objInfo
}

// ObjectPartIndex - returns the index of matching object part number.
func (m fsMetaV1) ObjectPartIndex(partNumber int) (partIndex int) {
	for i, part := range m.Parts {
		if partNumber == part.Number {
			partIndex = i
			return partIndex
		}
	}
	return -1
}

// AddObjectPart - add a new object part in order.
func (m *fsMetaV1) AddObjectPart(partNumber int, partName string, partETag string, partSize int64) {
	partInfo := objectPartInfo{
		Number: partNumber,
		Name:   partName,
		ETag:   partETag,
		Size:   partSize,
	}

	// Update part info if it already exists.
	for i, part := range m.Parts {
		if partNumber == part.Number {
			m.Parts[i] = partInfo
			return
		}
	}

	// Proceed to include new part info.
	m.Parts = append(m.Parts, partInfo)

	// Parts in fsMeta should be in sorted order by part number.
	sort.Sort(byObjectPartNumber(m.Parts))
}

func (m *fsMetaV1) WriteTo(lk *lock.LockedFile) (n int64, err error) {
	var metadataBytes []byte
	metadataBytes, err = json.Marshal(m)
	if err != nil {
		return 0, traceError(err)
	}

	if err = lk.Truncate(0); err != nil {
		return 0, traceError(err)
	}

	if _, err = lk.Write(metadataBytes); err != nil {
		return 0, traceError(err)
	}

	// Success.
	return int64(len(metadataBytes)), nil
}

func parseFSVersion(fsMetaBuf []byte) string {
	return gjson.GetBytes(fsMetaBuf, "version").String()
}

func parseFSFormat(fsMetaBuf []byte) string {
	return gjson.GetBytes(fsMetaBuf, "format").String()
}

func parseFSRelease(fsMetaBuf []byte) string {
	return gjson.GetBytes(fsMetaBuf, "minio.release").String()
}

func parseFSMetaMap(fsMetaBuf []byte) map[string]string {
	// Get xlMetaV1.Meta map.
	metaMapResult := gjson.GetBytes(fsMetaBuf, "meta").Map()
	metaMap := make(map[string]string)
	for key, valResult := range metaMapResult {
		metaMap[key] = valResult.String()
	}
	return metaMap
}

func parseFSParts(fsMetaBuf []byte) []objectPartInfo {
	// Parse the FS Parts.
	partsResult := gjson.GetBytes(fsMetaBuf, "parts").Array()
	partInfo := make([]objectPartInfo, len(partsResult))
	for i, p := range partsResult {
		info := objectPartInfo{}
		info.Number = int(p.Get("number").Int())
		info.Name = p.Get("name").String()
		info.ETag = p.Get("etag").String()
		info.Size = p.Get("size").Int()
		partInfo[i] = info
	}
	return partInfo
}

func (m *fsMetaV1) ReadFrom(lk *lock.LockedFile) (n int64, err error) {
	var fsMetaBuf []byte
	fi, err := lk.Stat()
	if err != nil {
		return 0, traceError(err)
	}

	fsMetaBuf, err = ioutil.ReadAll(io.NewSectionReader(lk, 0, fi.Size()))
	if err != nil {
		return 0, traceError(err)
	}

	if len(fsMetaBuf) == 0 {
		return 0, traceError(io.EOF)
	}

	// obtain version.
	m.Version = parseFSVersion(fsMetaBuf)

	// obtain format.
	m.Format = parseFSFormat(fsMetaBuf)

	// Verify if the format is valid, return corrupted format
	// for unrecognized formats.
	if !isFSMetaValid(m.Version, m.Format) {
		return 0, traceError(errCorruptedFormat)
	}

	// obtain metadata.
	m.Meta = parseFSMetaMap(fsMetaBuf)

	// obtain parts info list.
	m.Parts = parseFSParts(fsMetaBuf)

	// obtain minio release date.
	m.Minio.Release = parseFSRelease(fsMetaBuf)

	// Success.
	return int64(len(fsMetaBuf)), nil
}

// FS format version strings.
const (
	fsFormatV1 = "1" // Previous format.
	fsFormatV2 = "2" // Current format.
	// Proceed to add "3" when we
	// change the backend format in future.
)

// newFSMetaV1 - initializes new fsMetaV1.
func newFSMetaV1() (fsMeta fsMetaV1) {
	fsMeta = fsMetaV1{}
	fsMeta.Version = fsMetaVersion
	fsMeta.Format = fsMetaFormat
	fsMeta.Minio.Release = ReleaseTag
	return fsMeta
}

// newFSFormatV2 - initializes new formatConfigV1 with FS format version 2.
func newFSFormatV2() (format *formatConfigV1) {
	return &formatConfigV1{
		Version: "1",
		Format:  "fs",
		FS: &fsFormat{
			Version: fsFormatV2,
		},
	}
}

// Checks if input format is version 1 and 2.
func isFSValidFormat(formatCfg *formatConfigV1) bool {
	// Supported format versions.
	var supportedFormatVersions = []string{
		fsFormatV1,
		fsFormatV2,
		// New supported versions here.
	}

	// Check for supported format versions.
	for _, version := range supportedFormatVersions {
		if formatCfg.FS.Version == version {
			return true
		}
	}
	return false
}

// errFSFormatOld- old fs format.
var errFSFormatOld = errors.New("old FS format found")

// Checks if the loaded `format.json` is valid and
// is expected to be of the requested version.
func checkFormatFS(format *formatConfigV1, formatVersion string) error {
	if format == nil {
		return errUnexpected
	}

	// Validate if we have the same format.
	if format.Format != "fs" {
		return fmt.Errorf("Unable to recognize backend format, Disk is not in FS format. %s", format.Format)
	}

	// Check if format is currently supported.
	if !isFSValidFormat(format) {
		return errCorruptedFormat
	}

	// Check for format version is current.
	if format.FS.Version != formatVersion {
		return errFSFormatOld
	}

	return nil
}

// This is just kept as reference, there is no sanity
// check for FS format in version "1".
func checkFormatSanityFSV1(fsPath string) error {
	return nil
}

// Check for sanity of FS format in version "2".
func checkFormatSanityFSV2(fsPath string) error {
	buckets, err := readDir(pathJoin(fsPath, minioMetaBucket, bucketConfigPrefix))
	if err != nil && err != errFileNotFound {
		return err
	}

	// Attempt to validate all the buckets have a sanitized backend.
	for _, bucket := range buckets {
		entries, rerr := readDir(pathJoin(fsPath, minioMetaBucket, bucketConfigPrefix, bucket))
		if rerr != nil {
			return rerr
		}

		var expectedConfigs = append(bucketMetadataConfigs, objectMetaPrefix+"/")
		entriesSet := set.CreateStringSet(entries...)
		expectedConfigsSet := set.CreateStringSet(expectedConfigs...)

		// Entries found shouldn't be more than total
		// expected config directories, files.
		if len(entriesSet) > len(expectedConfigsSet) {
			return errCorruptedFormat
		}

		// Look for the difference between entries and the
		// expected config set, resulting entries if they
		// intersect with original entries set we know
		// that the backend has unexpected files.
		if !entriesSet.Difference(expectedConfigsSet).IsEmpty() {
			return errCorruptedFormat
		}
	}
	return nil
}

// Check for sanity of FS format for a given version.
func checkFormatSanityFS(fsPath string, fsFormatVersion string) (err error) {
	switch fsFormatVersion {
	case fsFormatV2:
		err = checkFormatSanityFSV2(fsPath)
	default:
		err = errCorruptedFormat
	}
	return err
}

// Initializes a new `format.json` if not present, validates `format.json`
// if already present and migrates to newer version if necessary. Returns
// the final format version.
func initFormatFS(fsPath, fsUUID string) (err error) {
	fsFormatPath := pathJoin(fsPath, minioMetaBucket, fsFormatJSONFile)

	// fsFormatJSONFile - format.json file stored in minioMetaBucket(.minio.sys) directory.
	lk, err := lock.LockedOpenFile(preparePath(fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return traceError(err)
	}
	defer lk.Close()

	var format = &formatConfigV1{}
	_, err = format.ReadFrom(lk)
	// For all unexpected errors, we return.
	if err != nil && errorCause(err) != io.EOF {
		return traceError(fmt.Errorf("Unable to load 'format.json', %s", err))
	}

	// If we couldn't read anything, The disk is unformatted.
	if errorCause(err) == io.EOF {
		err = errUnformattedDisk
		format = newFSFormatV2()
	} else {
		// Validate loaded `format.json`.
		err = checkFormatFS(format, fsFormatVersion)
		if err != nil && err != errFSFormatOld {
			return traceError(fmt.Errorf("Unable to validate 'format.json', %s", err))
		}
	}

	// Disk is in old format migrate object metadata.
	if err == errFSFormatOld {
		if merr := migrateFSObject(fsPath, fsUUID); merr != nil {
			return merr
		}

		// Initialize format v2.
		format = newFSFormatV2()
	}

	// Rewrite or write format.json depending on if disk
	// unformatted and if format is old.
	if err == errUnformattedDisk || err == errFSFormatOld {
		if _, err = format.WriteTo(lk); err != nil {
			return traceError(fmt.Errorf("Unable to initialize 'format.json', %s", err))
		}
	}

	// Check for sanity.
	return checkFormatSanityFS(fsPath, format.FS.Version)
}

// Return if the part info in uploadedParts and completeParts are same.
func isPartsSame(uploadedParts []objectPartInfo, completeParts []completePart) bool {
	if len(uploadedParts) != len(completeParts) {
		return false
	}

	for i := range completeParts {
		if uploadedParts[i].Number != completeParts[i].PartNumber ||
			uploadedParts[i].ETag != completeParts[i].ETag {
			return false
		}
	}

	return true
}
