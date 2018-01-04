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
	"io"
	"io/ioutil"
	"os"
	pathutil "path"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/lock"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/tidwall/gjson"
)

// FS format, and object metadata.
const (
	// fs.json object metadata.
	fsMetaJSONFile = "fs.json"
)

// FS metadata constants.
const (
	// FS backend meta 1.0.0 version.
	fsMetaVersion100 = "1.0.0"

	// FS backend meta 1.0.1 version.
	fsMetaVersion = "1.0.1"

	// FS backend meta format.
	fsMetaFormat = "fs"

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

	if hasSuffix(object, slashSeparator) {
		m.Meta["etag"] = emptyETag // For directories etag is d41d8cd98f00b204e9800998ecf8427e
		m.Meta["content-type"] = "application/octet-stream"
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
		if fi.IsDir() {
			// Directory is always 0 bytes in S3 API, treat it as such.
			objInfo.Size = 0
			objInfo.IsDir = fi.IsDir()
		}
	}

	// Extract etag from metadata.
	objInfo.ETag = extractETag(m.Meta)
	objInfo.ContentType = m.Meta["content-type"]
	objInfo.ContentEncoding = m.Meta["content-encoding"]

	// etag/md5Sum has already been extracted. We need to
	// remove to avoid it from appearing as part of
	// response headers. e.g, X-Minio-* or X-Amz-*.
	objInfo.UserDefined = cleanMetadata(m.Meta)

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
		return 0, errors.Trace(err)
	}

	if err = lk.Truncate(0); err != nil {
		return 0, errors.Trace(err)
	}

	if _, err = lk.Write(metadataBytes); err != nil {
		return 0, errors.Trace(err)
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
		return 0, errors.Trace(err)
	}

	fsMetaBuf, err = ioutil.ReadAll(io.NewSectionReader(lk, 0, fi.Size()))
	if err != nil {
		return 0, errors.Trace(err)
	}

	if len(fsMetaBuf) == 0 {
		return 0, errors.Trace(io.EOF)
	}

	// obtain version.
	m.Version = parseFSVersion(fsMetaBuf)

	// obtain format.
	m.Format = parseFSFormat(fsMetaBuf)

	// Verify if the format is valid, return corrupted format
	// for unrecognized formats.
	if !isFSMetaValid(m.Version, m.Format) {
		return 0, errors.Trace(errCorruptedFormat)
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

// newFSMetaV1 - initializes new fsMetaV1.
func newFSMetaV1() (fsMeta fsMetaV1) {
	fsMeta = fsMetaV1{}
	fsMeta.Version = fsMetaVersion
	fsMeta.Format = fsMetaFormat
	fsMeta.Minio.Release = ReleaseTag
	return fsMeta
}

// Check if disk has already a valid format, holds a read lock and
// upon success returns it to the caller to be closed.
func checkLockedValidFormatFS(fsPath string) (*lock.RLockedFile, error) {
	fsFormatPath := pathJoin(fsPath, minioMetaBucket, formatConfigFile)

	rlk, err := lock.RLockedOpenFile((fsFormatPath))
	if err != nil {
		if os.IsNotExist(err) {
			// If format.json not found then
			// its an unformatted disk.
			return nil, errors.Trace(errUnformattedDisk)
		}
		return nil, errors.Trace(err)
	}

	var format = &formatConfigV1{}
	if err = format.LoadFormat(rlk.LockedFile); err != nil {
		rlk.Close()
		return nil, err
	}

	// Check format FS.
	if err = format.CheckFS(); err != nil {
		rlk.Close()
		return nil, err
	}

	//  Always return read lock here and should be closed by the caller.
	return rlk, errors.Trace(err)
}

// Creates a new format.json if unformatted.
func createFormatFS(fsPath string) error {
	fsFormatPath := pathJoin(fsPath, minioMetaBucket, formatConfigFile)

	// Attempt a write lock on formatConfigFile `format.json`
	// file stored in minioMetaBucket(.minio.sys) directory.
	lk, err := lock.TryLockedOpenFile((fsFormatPath), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return errors.Trace(err)
	}
	// Close the locked file upon return.
	defer lk.Close()

	// Load format on disk, checks if we are unformatted
	// writes the new format.json
	var format = &formatConfigV1{}
	err = format.LoadFormat(lk)
	if errors.Cause(err) == errUnformattedDisk {
		_, err = newFSFormat().WriteTo(lk)
		return err
	}
	return err
}

func initFormatFS(fsPath string) (rlk *lock.RLockedFile, err error) {
	// This loop validates format.json by holding a read lock and
	// proceeds if disk unformatted to hold non-blocking WriteLock
	// If for some reason non-blocking WriteLock fails and the error
	// is lock.ErrAlreadyLocked i.e some other process is holding a
	// lock we retry in the loop again.
	for {
		// Validate the `format.json` for expected values.
		rlk, err = checkLockedValidFormatFS(fsPath)
		switch {
		case err == nil:
			// Holding a read lock ensures that any write lock operation
			// is blocked if attempted in-turn avoiding corruption on
			// the backend disk.
			return rlk, nil
		case errors.Cause(err) == errUnformattedDisk:
			if err = createFormatFS(fsPath); err != nil {
				// Existing write locks detected.
				if errors.Cause(err) == lock.ErrAlreadyLocked {
					// Lock already present, sleep and attempt again.
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// Unexpected error, return.
				return nil, err
			}

			// Loop will continue to attempt a read-lock on `format.json`.
		default:
			// Unhandled error return.
			return nil, err
		}
	}
}

// Return if the part info in uploadedParts and CompleteParts are same.
func isPartsSame(uploadedParts []objectPartInfo, CompleteParts []CompletePart) bool {
	if len(uploadedParts) != len(CompleteParts) {
		return false
	}

	for i := range CompleteParts {
		if uploadedParts[i].Number != CompleteParts[i].PartNumber ||
			uploadedParts[i].ETag != CompleteParts[i].ETag {
			return false
		}
	}

	return true
}
