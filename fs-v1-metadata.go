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

// SearchObjectPart - search object part name and etag.
func (m fsMetaV1) SearchObjectPart(number int) int {
	for i, part := range m.Parts {
		if number == part.Number {
			return i
		}
	}
	return -1
}

// AddObjectPart - add a new object part in order.
func (m *fsMetaV1) AddObjectPart(number int, name string, etag string, size int64) {
	m.Parts = append(m.Parts, objectPartInfo{
		Number: number,
		Name:   name,
		ETag:   etag,
		Size:   size,
	})
	sort.Sort(byPartNumber(m.Parts))
}

// readFSMetadata - read `fs.json`.
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

// writeFSMetadata - write `fs.json`.
func (fs fsObjects) writeFSMetadata(bucket, prefix string, fsMeta fsMetaV1) error {
	// Initialize metadata map, save all erasure related metadata.
	fsMeta.Minio.Release = minioReleaseTag
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
