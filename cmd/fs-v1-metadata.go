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
	"strings"

	"github.com/minio/minio/pkg/lock"
	"github.com/minio/minio/pkg/mimedb"
)

const (
	fsMetaJSONFile   = "fs.json"
	fsFormatJSONFile = "format.json"
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

	// We set file into only if its valid.
	objInfo.ModTime = timeSentinel
	if fi != nil {
		objInfo.ModTime = fi.ModTime()
		objInfo.Size = fi.Size()
		objInfo.IsDir = fi.IsDir()
	}

	objInfo.MD5Sum = m.Meta["md5Sum"]
	objInfo.ContentType = m.Meta["content-type"]
	objInfo.ContentEncoding = m.Meta["content-encoding"]

	// md5Sum has already been extracted into objInfo.MD5Sum.  We
	// need to remove it from m.Meta to avoid it from appearing as
	// part of response headers. e.g, X-Minio-* or X-Amz-*.
	delete(m.Meta, "md5Sum")

	// Save all the other userdefined API.
	objInfo.UserDefined = m.Meta

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
	sortParts(m.Parts)
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

func (m *fsMetaV1) ReadFrom(lk *lock.LockedFile) (n int64, err error) {
	var metadataBytes []byte
	fi, err := lk.Stat()
	if err != nil {
		return 0, traceError(err)
	}

	metadataBytes, err = ioutil.ReadAll(io.NewSectionReader(lk, 0, fi.Size()))
	if err != nil {
		return 0, traceError(err)
	}

	if len(metadataBytes) == 0 {
		return 0, traceError(io.EOF)
	}

	// Decode `fs.json` into fsMeta structure.
	if err = json.Unmarshal(metadataBytes, m); err != nil {
		return 0, traceError(err)
	}

	// Success.
	return int64(len(metadataBytes)), nil
}

// FS metadata constants.
const (
	// FS backend meta version.
	fsMetaVersion = "1.0.0"

	// FS backend meta format.
	fsMetaFormat = "fs"

	// Add more constants here.
)

// newFSMetaV1 - initializes new fsMetaV1.
func newFSMetaV1() (fsMeta fsMetaV1) {
	fsMeta = fsMetaV1{}
	fsMeta.Version = fsMetaVersion
	fsMeta.Format = fsMetaFormat
	fsMeta.Minio.Release = ReleaseTag
	return fsMeta
}

// newFSFormatV1 - initializes new formatConfigV1 with FS format info.
func newFSFormatV1() (format *formatConfigV1) {
	return &formatConfigV1{
		Version: "1",
		Format:  "fs",
		FS: &fsFormat{
			Version: "1",
		},
	}
}

// loads format.json from minioMetaBucket if it exists.
func loadFormatFS(fsPath string) (*formatConfigV1, error) {
	rlk, err := lock.RLockedOpenFile(pathJoin(fsPath, minioMetaBucket, fsFormatJSONFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errUnformattedDisk
		}
		return nil, err
	}
	defer rlk.Close()

	formatBytes, err := ioutil.ReadAll(rlk)
	if err != nil {
		return nil, err
	}

	format := &formatConfigV1{}
	if err = json.Unmarshal(formatBytes, format); err != nil {
		return nil, err
	}

	return format, nil
}

// writes FS format (format.json) into minioMetaBucket.
func saveFormatFS(formatPath string, fsFormat *formatConfigV1) error {
	metadataBytes, err := json.Marshal(fsFormat)
	if err != nil {
		return err
	}

	// fsFormatJSONFile - format.json file stored in minioMetaBucket(.minio.sys) directory.
	lk, err := lock.LockedOpenFile(preparePath(formatPath), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer lk.Close()

	if _, err = lk.Write(metadataBytes); err != nil {
		return err
	}

	// Success.
	return nil
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
