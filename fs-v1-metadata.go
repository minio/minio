package main

import (
	"encoding/json"
	"path"
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
	Parts []objectPartInfo `json:"parts,omitempty"`
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
func (fs fsObjects) readFSMetadata(bucket, object string) (fsMeta fsMetaV1, err error) {
	var buffer []byte
	buffer, err = readAll(fs.storage, bucket, path.Join(object, fsMetaJSONFile))
	if err != nil {
		return fsMetaV1{}, err
	}
	err = json.Unmarshal(buffer, &fsMeta)
	if err != nil {
		return fsMetaV1{}, err
	}
	return fsMeta, nil
}

// newFSMetaV1 - initializes new fsMetaV1.
func newFSMetaV1() (fsMeta fsMetaV1) {
	fsMeta = fsMetaV1{}
	fsMeta.Version = "1"
	fsMeta.Format = "fs"
	fsMeta.Minio.Release = minioReleaseTag
	return fsMeta
}

// newFSFormatV1 - initializes new formatConfigV1 with FS format info.
func newFSFormatV1() (format formatConfigV1) {
	return formatConfigV1{
		Version: "1",
		Format:  "fs",
		FS: &fsFormat{
			Version: "1",
		},
	}
}

// writes FS format (format.json) into minioMetaBucket.
func writeFSFormatData(storage StorageAPI, fsFormat formatConfigV1) error {
	metadataBytes, err := json.Marshal(fsFormat)
	if err != nil {
		return err
	}
	// fsFormatJSONFile - format.json file stored in minioMetaBucket(.minio) directory.
	n, err := storage.AppendFile(minioMetaBucket, fsFormatJSONFile, metadataBytes)
	if err != nil {
		return err
	}
	if n != int64(len(metadataBytes)) {
		return errUnexpected
	}
	return nil
}

// writeFSMetadata - writes `fs.json` metadata.
func (fs fsObjects) writeFSMetadata(bucket, prefix string, fsMeta fsMetaV1) error {
	metadataBytes, err := json.Marshal(fsMeta)
	if err != nil {
		return err
	}
	n, err := fs.storage.AppendFile(bucket, path.Join(prefix, fsMetaJSONFile), metadataBytes)
	if err != nil {
		return err
	}
	if n != int64(len(metadataBytes)) {
		return errUnexpected
	}
	return nil
}
