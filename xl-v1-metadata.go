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

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"path"
	"sort"
	"sync"
	"time"
)

// Erasure block size.
const erasureBlockSize = 4 * 1024 * 1024 // 4MiB.

// objectPartInfo Info of each part kept in the multipart metadata
// file after CompleteMultipartUpload() is called.
type objectPartInfo struct {
	Name string `json:"name"`
	ETag string `json:"etag"`
	Size int64  `json:"size"`
}

// A xlMetaV1 represents a metadata header mapping keys to sets of values.
type xlMetaV1 struct {
	Version string `json:"version"`
	Format  string `json:"format"`
	Stat    struct {
		Size    int64     `json:"size"`
		ModTime time.Time `json:"modTime"`
		Version int64     `json:"version"`
	} `json:"stat"`
	Erasure struct {
		DataBlocks   int   `json:"data"`
		ParityBlocks int   `json:"parity"`
		BlockSize    int64 `json:"blockSize"`
		Index        int   `json:"index"`
		Distribution []int `json:"distribution"`
	} `json:"erasure"`
	Checksum struct {
		Enable bool `json:"enable"`
	} `json:"checksum"`
	Minio struct {
		Release string `json:"release"`
	} `json:"minio"`
	Meta  map[string]string `json:"meta"`
	Parts []objectPartInfo  `json:"parts,omitempty"`
}

// ReadFrom - read from implements io.ReaderFrom interface for
// unmarshalling xlMetaV1.
func (m *xlMetaV1) ReadFrom(reader io.Reader) (n int64, err error) {
	var buffer bytes.Buffer
	n, err = buffer.ReadFrom(reader)
	if err != nil {
		return 0, err
	}
	err = json.Unmarshal(buffer.Bytes(), m)
	return n, err
}

// WriteTo - write to implements io.WriterTo interface for marshalling xlMetaV1.
func (m xlMetaV1) WriteTo(writer io.Writer) (n int64, err error) {
	metadataBytes, err := json.Marshal(m)
	if err != nil {
		return 0, err
	}
	p, err := writer.Write(metadataBytes)
	return int64(p), err
}

// byPartName is a collection satisfying sort.Interface.
type byPartName []objectPartInfo

func (t byPartName) Len() int           { return len(t) }
func (t byPartName) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t byPartName) Less(i, j int) bool { return t[i].Name < t[j].Name }

// SearchObjectPart - searches for part name and etag, returns the
// index if found.
func (m xlMetaV1) SearchObjectPart(name string, etag string) int {
	for i, part := range m.Parts {
		if name == part.Name && etag == part.ETag {
			return i
		}
	}
	return -1
}

// AddObjectPart - add a new object part in order.
func (m *xlMetaV1) AddObjectPart(name string, etag string, size int64) {
	m.Parts = append(m.Parts, objectPartInfo{
		Name: name,
		ETag: etag,
		Size: size,
	})
	sort.Sort(byPartName(m.Parts))
}

// getPartNumberOffset - given an offset for the whole object, return the part and offset in that part.
func (m xlMetaV1) getPartNumberOffset(offset int64) (partNumber int, partOffset int64, err error) {
	partOffset = offset
	for i, part := range m.Parts {
		partNumber = i
		if part.Size == 0 {
			return partNumber, partOffset, nil
		}
		if partOffset < part.Size {
			return partNumber, partOffset, nil
		}
		partOffset -= part.Size
	}
	// Offset beyond the size of the object
	err = errUnexpected
	return 0, 0, err
}

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (xl xlObjects) parentDirIsObject(bucket, parent string) bool {
	var isParentDirObject func(string) bool
	isParentDirObject = func(p string) bool {
		if p == "." {
			return false
		}
		if xl.isObject(bucket, p) {
			// If there is already a file at prefix "p" return error.
			return true
		}
		// Check if there is a file as one of the parent paths.
		return isParentDirObject(path.Dir(p))
	}
	return isParentDirObject(parent)
}

func (xl xlObjects) isObject(bucket, prefix string) bool {
	// Create errs and volInfo slices of storageDisks size.
	var errs = make([]error, len(xl.storageDisks))

	// Allocate a new waitgroup.
	var wg = &sync.WaitGroup{}
	for index, disk := range xl.storageDisks {
		wg.Add(1)
		// Stat file on all the disks in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			_, err := disk.StatFile(bucket, path.Join(prefix, xlMetaJSONFile))
			if err != nil {
				errs[index] = err
				return
			}
			errs[index] = nil
		}(index, disk)
	}

	// Wait for all the Stat operations to finish.
	wg.Wait()

	var errFileNotFoundCount int
	for _, err := range errs {
		if err != nil {
			if err == errFileNotFound {
				errFileNotFoundCount++
				// If we have errors with file not found greater than allowed read
				// quorum we return err as errFileNotFound.
				if errFileNotFoundCount > len(xl.storageDisks)-xl.readQuorum {
					return false
				}
				continue
			}
			errorIf(err, "Unable to access file "+path.Join(bucket, prefix))
			return false
		}
	}
	return true
}

// readXLMetadata - read xl metadata.
func readXLMetadata(disk StorageAPI, bucket, object string) (xlMeta xlMetaV1, err error) {
	r, err := disk.ReadFile(bucket, path.Join(object, xlMetaJSONFile), int64(0))
	if err != nil {
		return xlMetaV1{}, err
	}
	defer r.Close()
	_, err = xlMeta.ReadFrom(r)
	if err != nil {
		return xlMetaV1{}, err
	}
	return xlMeta, nil
}

// deleteXLJson - delete `xl.json` on all disks.
func (xl xlObjects) deleteXLMetadata(bucket, object string) error {
	return xl.deleteObject(bucket, path.Join(object, xlMetaJSONFile))
}

// renameXLJson - rename `xl.json` on all disks.
func (xl xlObjects) renameXLMetadata(srcBucket, srcPrefix, dstBucket, dstPrefix string) error {
	return xl.renameObject(srcBucket, path.Join(srcPrefix, xlMetaJSONFile), dstBucket, path.Join(dstPrefix, xlMetaJSONFile))
}

// getDiskDistribution - get disk distribution.
func (xl xlObjects) getDiskDistribution() []int {
	var distribution = make([]int, len(xl.storageDisks))
	for index := range xl.storageDisks {
		distribution[index] = index + 1
	}
	return distribution
}

// writeXLJson - write `xl.json` on all disks in order.
func (xl xlObjects) writeXLMetadata(bucket, prefix string, xlMeta xlMetaV1) error {
	var wg = &sync.WaitGroup{}
	var mErrs = make([]error, len(xl.storageDisks))

	// Initialize metadata map, save all erasure related metadata.
	xlMeta.Minio.Release = minioReleaseTag
	xlMeta.Erasure.DataBlocks = xl.dataBlocks
	xlMeta.Erasure.ParityBlocks = xl.parityBlocks
	xlMeta.Erasure.BlockSize = erasureBlockSize
	xlMeta.Erasure.Distribution = xl.getDiskDistribution()

	for index, disk := range xl.storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI, metadata xlMetaV1) {
			defer wg.Done()

			metaJSONFile := path.Join(prefix, xlMetaJSONFile)
			metaWriter, mErr := disk.CreateFile(bucket, metaJSONFile)
			if mErr != nil {
				mErrs[index] = mErr
				return
			}

			// Save the order.
			metadata.Erasure.Index = index + 1
			_, mErr = metadata.WriteTo(metaWriter)
			if mErr != nil {
				if mErr = safeCloseAndRemove(metaWriter); mErr != nil {
					mErrs[index] = mErr
					return
				}
				mErrs[index] = mErr
				return
			}
			if mErr = metaWriter.Close(); mErr != nil {
				if mErr = safeCloseAndRemove(metaWriter); mErr != nil {
					mErrs[index] = mErr
					return
				}
				mErrs[index] = mErr
				return
			}
			mErrs[index] = nil
		}(index, disk, xlMeta)
	}

	// Wait for all the routines.
	wg.Wait()

	// FIXME: check for quorum.
	// Loop through concocted errors and return the first one.
	for _, err := range mErrs {
		if err == nil {
			continue
		}
		return err
	}
	return nil
}
