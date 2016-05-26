package main

import (
	"bytes"
	"encoding/json"
	"io"
	"path"
	"sort"
)

const (
	fsMetaJSONFile = "fs.json"
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

// ReadFrom - read from implements io.ReaderFrom interface for
// unmarshalling fsMetaV1.
func (m *fsMetaV1) ReadFrom(reader io.Reader) (n int64, err error) {
	var buffer bytes.Buffer
	n, err = buffer.ReadFrom(reader)
	if err != nil {
		return 0, err
	}
	err = json.Unmarshal(buffer.Bytes(), m)
	return n, err
}

// WriteTo - write to implements io.WriterTo interface for marshalling fsMetaV1.
func (m fsMetaV1) WriteTo(writer io.Writer) (n int64, err error) {
	metadataBytes, err := json.Marshal(m)
	if err != nil {
		return 0, err
	}
	p, err := writer.Write(metadataBytes)
	return int64(p), err
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
	sort.Sort(byPartNumber(m.Parts))
}

// readFSMetadata - returns the object metadata `fs.json` content.
func (fs fsObjects) readFSMetadata(bucket, object string) (fsMeta fsMetaV1, err error) {
	r, err := fs.storage.ReadFile(bucket, path.Join(object, fsMetaJSONFile), int64(0))
	if err != nil {
		return fsMetaV1{}, err
	}
	defer r.Close()
	_, err = fsMeta.ReadFrom(r)
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

// writeFSMetadata - writes `fs.json` metadata.
func (fs fsObjects) writeFSMetadata(bucket, prefix string, fsMeta fsMetaV1) error {
	w, err := fs.storage.CreateFile(bucket, path.Join(prefix, fsMetaJSONFile))
	if err != nil {
		return err
	}
	_, err = fsMeta.WriteTo(w)
	if err != nil {
		if mErr := safeCloseAndRemove(w); mErr != nil {
			return mErr
		}
		return err
	}
	if err = w.Close(); err != nil {
		if mErr := safeCloseAndRemove(w); mErr != nil {
			return mErr
		}
		return err
	}
	return nil
}
