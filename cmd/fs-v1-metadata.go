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

package cmd

import (
	"encoding/json"
	"sort"
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

// readFSMetadata - returns the object metadata `fs.json` content.
func readFSMetadata(disk StorageAPI, bucket, filePath string) (fsMeta fsMetaV1, err error) {
	// Read all `fs.json`.
	buf, err := disk.ReadAll(bucket, filePath)
	if err != nil {
		return fsMetaV1{}, traceError(err)
	}

	// Decode `fs.json` into fsMeta structure.
	if err = json.Unmarshal(buf, &fsMeta); err != nil {
		return fsMetaV1{}, traceError(err)
	}

	// Success.
	return fsMeta, nil
}

// Write fsMeta to fs.json or fs-append.json.
func writeFSMetadata(disk StorageAPI, bucket, filePath string, fsMeta fsMetaV1) error {
	tmpPath := mustGetUUID()
	metadataBytes, err := json.Marshal(fsMeta)
	if err != nil {
		return traceError(err)
	}
	if err = disk.AppendFile(minioMetaTmpBucket, tmpPath, metadataBytes); err != nil {
		return traceError(err)
	}
	err = disk.RenameFile(minioMetaTmpBucket, tmpPath, bucket, filePath)
	if err != nil {
		err = disk.DeleteFile(minioMetaTmpBucket, tmpPath)
		if err != nil {
			return traceError(err)
		}
	}
	return nil
}

// newFSMetaV1 - initializes new fsMetaV1.
func newFSMetaV1() (fsMeta fsMetaV1) {
	fsMeta = fsMetaV1{}
	fsMeta.Version = "1.0.0"
	fsMeta.Format = "fs"
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

// isFSFormat - returns whether given formatConfigV1 is FS type or not.
func isFSFormat(format *formatConfigV1) bool {
	return format.Format == "fs"
}

// writes FS format (format.json) into minioMetaBucket.
func saveFSFormatData(storage StorageAPI, fsFormat *formatConfigV1) error {
	metadataBytes, err := json.Marshal(fsFormat)
	if err != nil {
		return err
	}
	// fsFormatJSONFile - format.json file stored in minioMetaBucket(.minio) directory.
	if err = storage.AppendFile(minioMetaBucket, fsFormatJSONFile, metadataBytes); err != nil {
		return err
	}
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
