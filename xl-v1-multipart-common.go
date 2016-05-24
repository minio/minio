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
	"strings"
	"sync"
	"time"

	"github.com/skyrings/skyring-common/tools/uuid"
)

// uploadInfo -
type uploadInfo struct {
	UploadID  string    `json:"uploadId"`
	Initiated time.Time `json:"initiated"`
}

// uploadsV1 -
type uploadsV1 struct {
	Version string       `json:"version"`
	Format  string       `json:"format"`
	Uploads []uploadInfo `json:"uploadIds"`
}

// byInitiatedTime is a collection satisfying sort.Interface.
type byInitiatedTime []uploadInfo

func (t byInitiatedTime) Len() int      { return len(t) }
func (t byInitiatedTime) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t byInitiatedTime) Less(i, j int) bool {
	return t[i].Initiated.After(t[j].Initiated)
}

// AddUploadID - adds a new upload id in order of its initiated time.
func (u *uploadsV1) AddUploadID(uploadID string, initiated time.Time) {
	u.Uploads = append(u.Uploads, uploadInfo{
		UploadID:  uploadID,
		Initiated: initiated,
	})
	sort.Sort(byInitiatedTime(u.Uploads))
}

func (u uploadsV1) SearchUploadID(uploadID string) int {
	for i, u := range u.Uploads {
		if u.UploadID == uploadID {
			return i
		}
	}
	return -1
}

// ReadFrom - read from implements io.ReaderFrom interface for unmarshalling uploads.
func (u *uploadsV1) ReadFrom(reader io.Reader) (n int64, err error) {
	var buffer bytes.Buffer
	n, err = buffer.ReadFrom(reader)
	if err != nil {
		return 0, err
	}
	err = json.Unmarshal(buffer.Bytes(), &u)
	return n, err
}

// WriteTo - write to implements io.WriterTo interface for marshalling uploads.
func (u uploadsV1) WriteTo(writer io.Writer) (n int64, err error) {
	metadataBytes, err := json.Marshal(&u)
	if err != nil {
		return 0, err
	}
	m, err := writer.Write(metadataBytes)
	return int64(m), err
}

// getUploadIDs - get saved upload id's.
func getUploadIDs(bucket, object string, storageDisks ...StorageAPI) (uploadIDs uploadsV1, err error) {
	uploadJSONPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	var errs = make([]error, len(storageDisks))
	var uploads = make([]uploadsV1, len(storageDisks))
	var wg = &sync.WaitGroup{}

	for index, disk := range storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			r, rErr := disk.ReadFile(minioMetaBucket, uploadJSONPath, int64(0))
			if rErr != nil {
				errs[index] = rErr
				return
			}
			defer r.Close()
			_, rErr = uploads[index].ReadFrom(r)
			if rErr != nil {
				errs[index] = rErr
				return
			}
			errs[index] = nil
		}(index, disk)
	}
	wg.Wait()

	for _, err = range errs {
		if err != nil {
			return uploadsV1{}, err
		}
	}

	// FIXME: Do not know if it should pick the picks the first successful one and returns.
	return uploads[0], nil
}

func updateUploadJSON(bucket, object string, uploadIDs uploadsV1, storageDisks ...StorageAPI) error {
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	var errs = make([]error, len(storageDisks))
	var wg = &sync.WaitGroup{}

	for index, disk := range storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			w, wErr := disk.CreateFile(minioMetaBucket, uploadsPath)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			_, wErr = uploadIDs.WriteTo(w)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			if wErr = w.Close(); wErr != nil {
				if clErr := safeCloseAndRemove(w); clErr != nil {
					errs[index] = clErr
					return
				}
				errs[index] = wErr
				return
			}
		}(index, disk)
	}

	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// writeUploadJSON - create `uploads.json` or update it with new uploadID.
func writeUploadJSON(bucket, object, uploadID string, initiated time.Time, storageDisks ...StorageAPI) error {
	uploadsPath := path.Join(mpartMetaPrefix, bucket, object, uploadsJSONFile)
	tmpUploadsPath := path.Join(tmpMetaPrefix, bucket, object, uploadsJSONFile)

	var errs = make([]error, len(storageDisks))
	var wg = &sync.WaitGroup{}

	uploadIDs, err := getUploadIDs(bucket, object, storageDisks...)
	if err != nil && err != errFileNotFound {
		return err
	}
	uploadIDs.Version = "1"
	uploadIDs.Format = "xl"
	uploadIDs.AddUploadID(uploadID, initiated)

	for index, disk := range storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			w, wErr := disk.CreateFile(minioMetaBucket, tmpUploadsPath)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			_, wErr = uploadIDs.WriteTo(w)
			if wErr != nil {
				errs[index] = wErr
				return
			}
			if wErr = w.Close(); wErr != nil {
				if clErr := safeCloseAndRemove(w); clErr != nil {
					errs[index] = clErr
					return
				}
				errs[index] = wErr
				return
			}
			wErr = disk.RenameFile(minioMetaBucket, tmpUploadsPath, minioMetaBucket, uploadsPath)
			if wErr != nil {
				if dErr := disk.DeleteFile(minioMetaBucket, tmpUploadsPath); dErr != nil {
					errs[index] = dErr
					return
				}
				errs[index] = wErr
				return
			}
			errs[index] = nil
		}(index, disk)
	}

	wg.Wait()

	for _, err = range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// Wrapper which removes all the uploaded parts.
func cleanupUploadedParts(bucket, object, uploadID string, storageDisks ...StorageAPI) error {
	var errs = make([]error, len(storageDisks))
	var wg = &sync.WaitGroup{}
	for index, disk := range storageDisks {
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			err := cleanupDir(disk, minioMetaBucket, path.Join(mpartMetaPrefix, bucket, object, uploadID))
			if err != nil {
				errs[index] = err
				return
			}
			errs[index] = nil
		}(index, disk)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// listUploadsInfo - list all uploads info.
func (xl xlObjects) listUploadsInfo(prefixPath string) (uploads []uploadInfo, err error) {
	disk := xl.getRandomDisk()
	splitPrefixes := strings.SplitN(prefixPath, "/", 3)
	uploadIDs, err := getUploadIDs(splitPrefixes[1], splitPrefixes[2], disk)
	if err != nil {
		if err == errFileNotFound {
			return []uploadInfo{}, nil
		}
		return nil, err
	}
	uploads = uploadIDs.Uploads
	return uploads, nil
}

// listMetaBucketMultipart - list all objects at a given prefix inside minioMetaBucket.
func (xl xlObjects) listMetaBucketMultipart(prefixPath string, markerPath string, recursive bool, maxKeys int) (objInfos []ObjectInfo, eof bool, err error) {
	walker := xl.lookupTreeWalkXL(listParams{minioMetaBucket, recursive, markerPath, prefixPath})
	if walker == nil {
		walker = xl.startTreeWalkXL(minioMetaBucket, prefixPath, markerPath, recursive)
	}

	// newMaxKeys tracks the size of entries which are going to be
	// returned back.
	var newMaxKeys int

	// Following loop gathers and filters out special files inside minio meta volume.
	for {
		walkResult, ok := <-walker.ch
		if !ok {
			// Closed channel.
			eof = true
			break
		}
		// For any walk error return right away.
		if walkResult.err != nil {
			// File not found or Disk not found is a valid case.
			if walkResult.err == errFileNotFound || walkResult.err == errDiskNotFound {
				return nil, true, nil
			}
			return nil, false, toObjectErr(walkResult.err, minioMetaBucket, prefixPath)
		}
		objInfo := walkResult.objInfo
		var uploads []uploadInfo
		if objInfo.IsDir {
			// List all the entries if fi.Name is a leaf directory, if
			// fi.Name is not a leaf directory then the resulting
			// entries are empty.
			uploads, err = xl.listUploadsInfo(objInfo.Name)
			if err != nil {
				return nil, false, err
			}
		}
		if len(uploads) > 0 {
			for _, upload := range uploads {
				objInfos = append(objInfos, ObjectInfo{
					Name:    path.Join(objInfo.Name, upload.UploadID),
					ModTime: upload.Initiated,
				})
				newMaxKeys++
				// If we have reached the maxKeys, it means we have listed
				// everything that was requested.
				if newMaxKeys == maxKeys {
					break
				}
			}
		} else {
			// We reach here for a non-recursive case non-leaf entry
			// OR recursive case with fi.Name.
			if !objInfo.IsDir { // Do not skip non-recursive case directory entries.
				// Validate if 'fi.Name' is incomplete multipart.
				if !strings.HasSuffix(objInfo.Name, xlMetaJSONFile) {
					continue
				}
				objInfo.Name = path.Dir(objInfo.Name)
			}
			objInfos = append(objInfos, objInfo)
			newMaxKeys++
			// If we have reached the maxKeys, it means we have listed
			// everything that was requested.
			if newMaxKeys == maxKeys {
				break
			}
		}
	}

	if !eof && len(objInfos) != 0 {
		// EOF has not reached, hence save the walker channel to the map so that the walker go routine
		// can continue from where it left off for the next list request.
		lastObjInfo := objInfos[len(objInfos)-1]
		markerPath = lastObjInfo.Name
		xl.saveTreeWalkXL(listParams{minioMetaBucket, recursive, markerPath, prefixPath}, walker)
	}

	// Return entries here.
	return objInfos, eof, nil
}

// FIXME: Currently the code sorts based on keyName/upload-id which is
// not correct based on the S3 specs. According to s3 specs we are
// supposed to only lexically sort keyNames and then for keyNames with
// multiple upload ids should be sorted based on the initiated time.
// Currently this case is not handled.

// listMultipartUploadsCommon - lists all multipart uploads, common
// function for both object layers.
func (xl xlObjects) listMultipartUploadsCommon(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	result := ListMultipartsInfo{}
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListMultipartsInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	if !xl.isBucketExist(bucket) {
		return ListMultipartsInfo{}, BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectPrefix(prefix) {
		return ListMultipartsInfo{}, ObjectNameInvalid{Bucket: bucket, Object: prefix}
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return ListMultipartsInfo{}, UnsupportedDelimiter{
			Delimiter: delimiter,
		}
	}
	// Verify if marker has prefix.
	if keyMarker != "" && !strings.HasPrefix(keyMarker, prefix) {
		return ListMultipartsInfo{}, InvalidMarkerPrefixCombination{
			Marker: keyMarker,
			Prefix: prefix,
		}
	}
	if uploadIDMarker != "" {
		if strings.HasSuffix(keyMarker, slashSeparator) {
			return result, InvalidUploadIDKeyCombination{
				UploadIDMarker: uploadIDMarker,
				KeyMarker:      keyMarker,
			}
		}
		id, err := uuid.Parse(uploadIDMarker)
		if err != nil {
			return result, err
		}
		if id.IsZero() {
			return result, MalformedUploadID{
				UploadID: uploadIDMarker,
			}
		}
	}

	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	result.IsTruncated = true
	result.MaxUploads = maxUploads

	// Not using path.Join() as it strips off the trailing '/'.
	multipartPrefixPath := pathJoin(mpartMetaPrefix, pathJoin(bucket, prefix))
	if prefix == "" {
		// Should have a trailing "/" if prefix is ""
		// For ex. multipartPrefixPath should be "multipart/bucket/" if prefix is ""
		multipartPrefixPath += slashSeparator
	}
	multipartMarkerPath := ""
	if keyMarker != "" {
		keyMarkerPath := pathJoin(pathJoin(bucket, keyMarker), uploadIDMarker)
		multipartMarkerPath = pathJoin(mpartMetaPrefix, keyMarkerPath)
	}

	// List all the multipart files at prefixPath, starting with marker keyMarkerPath.
	objInfos, eof, err := xl.listMetaBucketMultipart(multipartPrefixPath, multipartMarkerPath, recursive, maxUploads)
	if err != nil {
		return ListMultipartsInfo{}, err
	}

	// Loop through all the received files fill in the multiparts result.
	for _, objInfo := range objInfos {
		var objectName string
		var uploadID string
		if objInfo.IsDir {
			// All directory entries are common prefixes.
			uploadID = "" // Upload ids are empty for CommonPrefixes.
			objectName = strings.TrimPrefix(objInfo.Name, retainSlash(pathJoin(mpartMetaPrefix, bucket)))
			result.CommonPrefixes = append(result.CommonPrefixes, objectName)
		} else {
			uploadID = path.Base(objInfo.Name)
			objectName = strings.TrimPrefix(path.Dir(objInfo.Name), retainSlash(pathJoin(mpartMetaPrefix, bucket)))
			result.Uploads = append(result.Uploads, uploadMetadata{
				Object:    objectName,
				UploadID:  uploadID,
				Initiated: objInfo.ModTime,
			})
		}
		result.NextKeyMarker = objectName
		result.NextUploadIDMarker = uploadID
	}
	result.IsTruncated = !eof
	if !result.IsTruncated {
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
	}
	return result, nil
}

// isUploadIDExists - verify if a given uploadID exists and is valid.
func (xl xlObjects) isUploadIDExists(bucket, object, uploadID string) bool {
	uploadIDPath := path.Join(mpartMetaPrefix, bucket, object, uploadID)
	return xl.isObject(minioMetaBucket, uploadIDPath)
}
