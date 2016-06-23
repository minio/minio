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
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/mimedb"
)

// fsObjects - Implements fs object layer.
type fsObjects struct {
	storage            StorageAPI
	physicalDisk       string
	listObjectMap      map[listParams][]*treeWalkerFS
	listObjectMapMutex *sync.Mutex
}

// creates format.json, the FS format info in minioMetaBucket.
func initFormatFS(storageDisk StorageAPI) error {
	return writeFSFormatData(storageDisk, newFSFormatV1())
}

// loads format.json from minioMetaBucket if it exists.
func loadFormatFS(storageDisk StorageAPI) (format formatConfigV1, err error) {
	// Allocate 32k buffer, this is sufficient for the most of `format.json`.
	buf := make([]byte, 32*1024)

	// Allocate a new `format.json` buffer writer.
	var buffer = new(bytes.Buffer)

	// Reads entire `format.json`.
	if err = copyBuffer(buffer, storageDisk, minioMetaBucket, fsFormatJSONFile, buf); err != nil {
		return formatConfigV1{}, err
	}

	// Unmarshal format config.
	d := json.NewDecoder(buffer)
	if err = d.Decode(&format); err != nil {
		return formatConfigV1{}, err
	}

	// Return structured `format.json`.
	return format, nil
}

// Should be called when process shuts down.
func shutdownFS(storage StorageAPI) {
	_, err := storage.ListDir(minioMetaBucket, mpartMetaPrefix)
	if err != errFileNotFound {
		// Multipart directory is not empty hence do not remove .minio volume.
		os.Exit(0)
	}
	prefix := ""
	if err := cleanupDir(storage, minioMetaBucket, prefix); err != nil {
		os.Exit(0)
		return
	}
	storage.DeleteVol(minioMetaBucket)
	os.Exit(0)
}

// newFSObjects - initialize new fs object layer.
func newFSObjects(disk string) (ObjectLayer, error) {
	storage, err := newStorageAPI(disk)
	if err != nil {
		return nil, err
	}

	// Runs house keeping code, like creating minioMetaBucket, cleaning up tmp files etc.
	fsHouseKeeping(storage)

	// loading format.json from minioMetaBucket.
	// Note: The format.json content is ignored, reserved for future use.
	_, err = loadFormatFS(storage)
	if err != nil {
		if err == errFileNotFound {
			// format.json doesn't exist, create it inside minioMetaBucket.
			err = initFormatFS(storage)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	// Register the callback that should be called when the process shuts down.
	registerShutdown(func() {
		shutdownFS(storage)
	})

	// Return successfully initialized object layer.
	return fsObjects{
		storage:            storage,
		physicalDisk:       disk,
		listObjectMap:      make(map[listParams][]*treeWalkerFS),
		listObjectMapMutex: &sync.Mutex{},
	}, nil
}

// StorageInfo - returns underlying storage statistics.
func (fs fsObjects) StorageInfo() StorageInfo {
	info, err := disk.GetInfo(fs.physicalDisk)
	fatalIf(err, "Unable to get disk info "+fs.physicalDisk)
	return StorageInfo{
		Total: info.Total,
		Free:  info.Free,
	}
}

/// Bucket operations

// MakeBucket - make a bucket.
func (fs fsObjects) MakeBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if err := fs.storage.MakeVol(bucket); err != nil {
		return toObjectErr(err, bucket)
	}
	return nil
}

// GetBucketInfo - get bucket info.
func (fs fsObjects) GetBucketInfo(bucket string) (BucketInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	vi, err := fs.storage.StatVol(bucket)
	if err != nil {
		return BucketInfo{}, toObjectErr(err, bucket)
	}
	return BucketInfo{
		Name:    bucket,
		Created: vi.Created,
	}, nil
}

// ListBuckets - list buckets.
func (fs fsObjects) ListBuckets() ([]BucketInfo, error) {
	var bucketInfos []BucketInfo
	vols, err := fs.storage.ListVols()
	if err != nil {
		return nil, toObjectErr(err)
	}
	for _, vol := range vols {
		// StorageAPI can send volume names which are incompatible
		// with buckets, handle it and skip them.
		if !IsValidBucketName(vol.Name) {
			continue
		}
		bucketInfos = append(bucketInfos, BucketInfo{
			Name:    vol.Name,
			Created: vol.Created,
		})
	}
	sort.Sort(byBucketName(bucketInfos))
	return bucketInfos, nil
}

// DeleteBucket - delete a bucket.
func (fs fsObjects) DeleteBucket(bucket string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if err := fs.storage.DeleteVol(bucket); err != nil {
		return toObjectErr(err, bucket)
	}
	return nil
}

/// Object Operations

// GetObject - get an object.
func (fs fsObjects) GetObject(bucket, object string, offset int64, length int64, writer io.Writer) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	var totalLeft = length
	buf := make([]byte, 32*1024) // Allocate a 32KiB staging buffer.
	for totalLeft > 0 {
		// Figure out the right size for the buffer.
		var curSize int64
		if blockSizeV1 < totalLeft {
			curSize = blockSizeV1
		} else {
			curSize = totalLeft
		}
		// Reads the file at offset.
		n, err := fs.storage.ReadFile(bucket, object, offset, buf[:curSize])
		if err != nil {
			return toObjectErr(err, bucket, object)
		}
		// Write to response writer.
		m, err := writer.Write(buf[:n])
		if err != nil {
			return toObjectErr(err, bucket, object)
		}
		totalLeft -= int64(m)
		offset += int64(m)
	} // Success.
	return nil
}

// GetObjectInfo - get object info.
func (fs fsObjects) GetObjectInfo(bucket, object string) (ObjectInfo, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ObjectInfo{}, (BucketNameInvalid{Bucket: bucket})
	}
	// Verify if object is valid.
	if !IsValidObjectName(object) {
		return ObjectInfo{}, (ObjectNameInvalid{Bucket: bucket, Object: object})
	}
	fi, err := fs.storage.StatFile(bucket, object)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Guess content-type from the extension if possible.
	contentType := ""
	if objectExt := filepath.Ext(object); objectExt != "" {
		if content, ok := mimedb.DB[strings.ToLower(strings.TrimPrefix(objectExt, "."))]; ok {
			contentType = content.ContentType
		}
	}

	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     fi.ModTime,
		Size:        fi.Size,
		IsDir:       fi.Mode.IsDir(),
		ContentType: contentType,
		MD5Sum:      "", // Read from metadata.
	}, nil
}

// PutObject - create an object.
func (fs fsObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string) (string, error) {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return "", BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return "", ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	}

	uniqueID := getUUID()

	// Uploaded object will first be written to the temporary location which will eventually
	// be renamed to the actual location. It is first written to the temporary location
	// so that cleaning it up will be easy if the server goes down.
	tempObj := path.Join(tmpMetaPrefix, uniqueID)

	// Initialize md5 writer.
	md5Writer := md5.New()

	if size == 0 {
		// For size 0 we write a 0byte file.
		err := fs.storage.AppendFile(minioMetaBucket, tempObj, []byte(""))
		if err != nil {
			return "", toObjectErr(err, bucket, object)
		}
	} else {
		// Allocate a buffer to Read() the object upload stream.
		buf := make([]byte, 32*1024)
		// Read the buffer till io.EOF and append the read data to
		// the temporary file.
		for {
			n, rErr := data.Read(buf)
			if rErr != nil && rErr != io.EOF {
				return "", toObjectErr(rErr, bucket, object)
			}
			if n > 0 {
				// Update md5 writer.
				md5Writer.Write(buf[:n])
				wErr := fs.storage.AppendFile(minioMetaBucket, tempObj, buf[:n])
				if wErr != nil {
					return "", toObjectErr(wErr, bucket, object)
				}
			}
			if rErr == io.EOF {
				break
			}
		}
	}

	newMD5Hex := hex.EncodeToString(md5Writer.Sum(nil))
	// md5Hex representation.
	var md5Hex string
	if len(metadata) != 0 {
		md5Hex = metadata["md5Sum"]
	}
	if md5Hex != "" {
		if newMD5Hex != md5Hex {
			return "", BadDigest{md5Hex, newMD5Hex}
		}
	}

	// Entire object was written to the temp location, now it's safe to rename it
	// to the actual location.
	err := fs.storage.RenameFile(minioMetaBucket, tempObj, bucket, object)
	if err != nil {
		return "", toObjectErr(err, bucket, object)
	}

	// Return md5sum, successfully wrote object.
	return newMD5Hex, nil
}

func (fs fsObjects) DeleteObject(bucket, object string) error {
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}
	if !IsValidObjectName(object) {
		return ObjectNameInvalid{Bucket: bucket, Object: object}
	}
	if err := fs.storage.DeleteFile(bucket, object); err != nil {
		return toObjectErr(err, bucket, object)
	}
	return nil
}

// Checks whether bucket exists.
func isBucketExist(storage StorageAPI, bucketName string) bool {
	// Check whether bucket exists.
	_, err := storage.StatVol(bucketName)
	if err != nil {
		if err == errVolumeNotFound {
			return false
		}
		errorIf(err, "Stat failed on bucket "+bucketName+".")
		return false
	}
	return true
}

func (fs fsObjects) listObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	// Convert entry to FileInfo
	entryToFileInfo := func(entry string) (fileInfo FileInfo, err error) {
		if strings.HasSuffix(entry, slashSeparator) {
			// Object name needs to be full path.
			fileInfo.Name = entry
			fileInfo.Mode = os.ModeDir
			return
		}
		if fileInfo, err = fs.storage.StatFile(bucket, entry); err != nil {
			return
		}
		// Object name needs to be full path.
		fileInfo.Name = entry
		return
	}

	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		return ListObjectsInfo{}, BucketNameInvalid{Bucket: bucket}
	}
	// Verify if bucket exists.
	if !isBucketExist(fs.storage, bucket) {
		return ListObjectsInfo{}, BucketNotFound{Bucket: bucket}
	}
	if !IsValidObjectPrefix(prefix) {
		return ListObjectsInfo{}, ObjectNameInvalid{Bucket: bucket, Object: prefix}
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != slashSeparator {
		return ListObjectsInfo{}, UnsupportedDelimiter{
			Delimiter: delimiter,
		}
	}
	// Verify if marker has prefix.
	if marker != "" {
		if !strings.HasPrefix(marker, prefix) {
			return ListObjectsInfo{}, InvalidMarkerPrefixCombination{
				Marker: marker,
				Prefix: prefix,
			}
		}
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return ListObjectsInfo{}, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if delimiter == slashSeparator && prefix == slashSeparator {
		return ListObjectsInfo{}, nil
	}

	// Over flowing count - reset to maxObjectList.
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == slashSeparator {
		recursive = false
	}

	walker := fs.lookupTreeWalk(listParams{bucket, recursive, marker, prefix})
	if walker == nil {
		walker = fs.startTreeWalk(bucket, prefix, marker, recursive, func(bucket, object string) bool {
			return !strings.HasSuffix(object, slashSeparator)
		})
	}
	var fileInfos []FileInfo
	var eof bool
	var nextMarker string
	for i := 0; i < maxKeys; {
		walkResult, ok := <-walker.ch
		if !ok {
			// Closed channel.
			eof = true
			break
		}
		// For any walk error return right away.
		if walkResult.err != nil {
			// File not found is a valid case.
			if walkResult.err == errFileNotFound {
				return ListObjectsInfo{}, nil
			}
			return ListObjectsInfo{}, toObjectErr(walkResult.err, bucket, prefix)
		}
		fileInfo, err := entryToFileInfo(walkResult.entry)
		if err != nil {
			return ListObjectsInfo{}, nil
		}
		nextMarker = fileInfo.Name
		fileInfos = append(fileInfos, fileInfo)
		if walkResult.end {
			eof = true
			break
		}
		i++
	}
	params := listParams{bucket, recursive, nextMarker, prefix}
	if !eof {
		fs.saveTreeWalk(params, walker)
	}

	result := ListObjectsInfo{IsTruncated: !eof}
	for _, fileInfo := range fileInfos {
		// With delimiter set we fill in NextMarker and Prefixes.
		if delimiter == slashSeparator {
			result.NextMarker = fileInfo.Name
			if fileInfo.Mode.IsDir() {
				result.Prefixes = append(result.Prefixes, fileInfo.Name)
				continue
			}
		}
		result.Objects = append(result.Objects, ObjectInfo{
			Name:    fileInfo.Name,
			ModTime: fileInfo.ModTime,
			Size:    fileInfo.Size,
			IsDir:   false,
		})
	}
	return result, nil
}

// ListObjects - list all objects.
func (fs fsObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return fs.listObjects(bucket, prefix, marker, delimiter, maxKeys)
}
