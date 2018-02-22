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
	"io"
	"io/ioutil"
	"os"
	pathutil "path"
	"strings"

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

	// All the parts per object.
	objInfo.Parts = m.Parts

	// Success..
	return objInfo
}

func (m *fsMetaV1) WriteTo(lk *lock.LockedFile) (n int64, err error) {
	if err = jsonSave(lk, m); err != nil {
		return 0, err
	}
	fi, err := lk.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
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

func parseFSPartsArray(fsMetaBuf []byte) []objectPartInfo {
	// Get xlMetaV1.Parts array
	var partsArray []objectPartInfo

	partsArrayResult := gjson.GetBytes(fsMetaBuf, "parts")
	partsArrayResult.ForEach(func(key, part gjson.Result) bool {
		partJSON := part.String()
		number := gjson.Get(partJSON, "number").Int()
		name := gjson.Get(partJSON, "name").String()
		etag := gjson.Get(partJSON, "etag").String()
		size := gjson.Get(partJSON, "size").Int()
		partsArray = append(partsArray, objectPartInfo{
			Number: int(number),
			Name:   name,
			ETag:   etag,
			Size:   size,
		})
		return true
	})
	return partsArray
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

	// obtain parts information
	m.Parts = parseFSPartsArray(fsMetaBuf)

	// obtain metadata.
	m.Meta = parseFSMetaMap(fsMetaBuf)

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
