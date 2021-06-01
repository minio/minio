// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	pathutil "path"
	"time"

	jsoniter "github.com/json-iterator/go"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/lock"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/mimedb"
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
	fsMetaVersion101 = "1.0.1"

	// FS backend meta 1.0.2
	// Removed the fields "Format" and "MinIO" from fsMetaV1 as they were unused. Added "Checksum" field - to be used in future for bit-rot protection.
	fsMetaVersion = "1.0.2"

	// Add more constants here.
)

// FSChecksumInfoV1 - carries checksums of individual blocks on disk.
type FSChecksumInfoV1 struct {
	Algorithm string
	Blocksize int64
	Hashes    [][]byte
}

// MarshalJSON marshals the FSChecksumInfoV1 struct
func (c FSChecksumInfoV1) MarshalJSON() ([]byte, error) {
	type checksuminfo struct {
		Algorithm string   `json:"algorithm"`
		Blocksize int64    `json:"blocksize"`
		Hashes    []string `json:"hashes"`
	}
	var hashes []string
	for _, h := range c.Hashes {
		hashes = append(hashes, hex.EncodeToString(h))
	}
	info := checksuminfo{
		Algorithm: c.Algorithm,
		Hashes:    hashes,
		Blocksize: c.Blocksize,
	}
	return json.Marshal(info)
}

// UnmarshalJSON unmarshals the the given data into the FSChecksumInfoV1 struct
func (c *FSChecksumInfoV1) UnmarshalJSON(data []byte) error {
	type checksuminfo struct {
		Algorithm string   `json:"algorithm"`
		Blocksize int64    `json:"blocksize"`
		Hashes    []string `json:"hashes"`
	}

	var info checksuminfo
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err := json.Unmarshal(data, &info)
	if err != nil {
		return err
	}
	c.Algorithm = info.Algorithm
	c.Blocksize = info.Blocksize
	var hashes [][]byte
	for _, hashStr := range info.Hashes {
		h, err := hex.DecodeString(hashStr)
		if err != nil {
			return err
		}
		hashes = append(hashes, h)
	}
	c.Hashes = hashes
	return nil
}

// A fsMetaV1 represents a metadata header mapping keys to sets of values.
type fsMetaV1 struct {
	Version string `json:"version"`
	// checksums of blocks on disk.
	Checksum FSChecksumInfoV1 `json:"checksum,omitempty"`
	// Metadata map for current object.
	Meta map[string]string `json:"meta,omitempty"`
	// parts info for current object - used in encryption.
	Parts []ObjectPartInfo `json:"parts,omitempty"`
}

// IsValid - tells if the format is sane by validating the version
// string and format style.
func (m fsMetaV1) IsValid() bool {
	return isFSMetaValid(m.Version)
}

// Verifies if the backend format metadata is same by validating
// the version string.
func isFSMetaValid(version string) bool {
	return (version == fsMetaVersion || version == fsMetaVersion100 || version == fsMetaVersion101)
}

// Converts metadata to object info.
func (m fsMetaV1) ToObjectInfo(bucket, object string, fi os.FileInfo) ObjectInfo {
	if len(m.Meta) == 0 {
		m.Meta = make(map[string]string)
	}

	// Guess content-type from the extension if possible.
	if m.Meta["content-type"] == "" {
		m.Meta["content-type"] = mimedb.TypeByExtension(pathutil.Ext(object))
	}

	if HasSuffix(object, SlashSeparator) {
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

	objInfo.ETag = extractETag(m.Meta)
	objInfo.ContentType = m.Meta["content-type"]
	objInfo.ContentEncoding = m.Meta["content-encoding"]
	if storageClass, ok := m.Meta[xhttp.AmzStorageClass]; ok {
		objInfo.StorageClass = storageClass
	} else {
		objInfo.StorageClass = globalMinioDefaultStorageClass
	}
	var (
		t time.Time
		e error
	)
	if exp, ok := m.Meta["expires"]; ok {
		if t, e = time.Parse(http.TimeFormat, exp); e == nil {
			objInfo.Expires = t.UTC()
		}
	}

	// Add user tags to the object info
	objInfo.UserTags = m.Meta[xhttp.AmzObjectTagging]

	// etag/md5Sum has already been extracted. We need to
	// remove to avoid it from appearing as part of
	// response headers. e.g, X-Minio-* or X-Amz-*.
	// Tags have also been extracted, we remove that as well.
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

func (m *fsMetaV1) ReadFrom(ctx context.Context, lk *lock.LockedFile) (n int64, err error) {
	var fsMetaBuf []byte
	fi, err := lk.Stat()
	if err != nil {
		logger.LogIf(ctx, err)
		return 0, err
	}

	fsMetaBuf, err = ioutil.ReadAll(io.NewSectionReader(lk, 0, fi.Size()))
	if err != nil {
		logger.LogIf(ctx, err)
		return 0, err
	}

	if len(fsMetaBuf) == 0 {
		return 0, io.EOF
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(fsMetaBuf, m); err != nil {
		return 0, err
	}

	// Verify if the format is valid, return corrupted format
	// for unrecognized formats.
	if !isFSMetaValid(m.Version) {
		logger.GetReqInfo(ctx).AppendTags("file", lk.Name())
		logger.LogIf(ctx, errCorruptedFormat)
		return 0, errCorruptedFormat
	}

	// Success.
	return int64(len(fsMetaBuf)), nil
}

// newFSMetaV1 - initializes new fsMetaV1.
func newFSMetaV1() (fsMeta fsMetaV1) {
	fsMeta = fsMetaV1{}
	fsMeta.Version = fsMetaVersion
	return fsMeta
}
