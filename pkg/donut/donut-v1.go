/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/crypto/sha512"
	"github.com/minio/minio/pkg/donut/disk"
	"github.com/minio/minio/pkg/iodine"
)

// config files used inside Donut
const (
	// bucket, object metadata
	bucketMetadataConfig = "bucketMetadata.json"
	objectMetadataConfig = "objectMetadata.json"

	// versions
	objectMetadataVersion = "1.0.0"
	bucketMetadataVersion = "1.0.0"
)

/// v1 API functions

// makeBucket - make a new bucket
func (donut API) makeBucket(bucket string, acl BucketACL) error {
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return iodine.New(InvalidArgument{}, nil)
	}
	return donut.makeDonutBucket(bucket, acl.String())
}

// getBucketMetadata - get bucket metadata
func (donut API) getBucketMetadata(bucketName string) (BucketMetadata, error) {
	if err := donut.listDonutBuckets(); err != nil {
		return BucketMetadata{}, iodine.New(err, nil)
	}
	if _, ok := donut.buckets[bucketName]; !ok {
		return BucketMetadata{}, iodine.New(BucketNotFound{Bucket: bucketName}, nil)
	}
	metadata, err := donut.getDonutBucketMetadata()
	if err != nil {
		return BucketMetadata{}, iodine.New(err, nil)
	}
	return metadata.Buckets[bucketName], nil
}

// setBucketMetadata - set bucket metadata
func (donut API) setBucketMetadata(bucketName string, bucketMetadata map[string]string) error {
	if err := donut.listDonutBuckets(); err != nil {
		return iodine.New(err, nil)
	}
	metadata, err := donut.getDonutBucketMetadata()
	if err != nil {
		return iodine.New(err, nil)
	}
	oldBucketMetadata := metadata.Buckets[bucketName]
	acl, ok := bucketMetadata["acl"]
	if !ok {
		return iodine.New(InvalidArgument{}, nil)
	}
	oldBucketMetadata.ACL = BucketACL(acl)
	metadata.Buckets[bucketName] = oldBucketMetadata
	return donut.setDonutBucketMetadata(metadata)
}

// listBuckets - return list of buckets
func (donut API) listBuckets() (map[string]BucketMetadata, error) {
	if err := donut.listDonutBuckets(); err != nil {
		return nil, iodine.New(err, nil)
	}
	metadata, err := donut.getDonutBucketMetadata()
	if err != nil {
		// intentionally left out the error when Donut is empty
		// but we need to revisit this area in future - since we need
		// to figure out between acceptable and unacceptable errors
		return make(map[string]BucketMetadata), nil
	}
	if metadata == nil {
		return make(map[string]BucketMetadata), nil
	}
	return metadata.Buckets, nil
}

// listObjects - return list of objects
func (donut API) listObjects(bucket, prefix, marker, delimiter string, maxkeys int) (ListObjectsResults, error) {
	errParams := map[string]string{
		"bucket":    bucket,
		"prefix":    prefix,
		"marker":    marker,
		"delimiter": delimiter,
		"maxkeys":   strconv.Itoa(maxkeys),
	}
	if err := donut.listDonutBuckets(); err != nil {
		return ListObjectsResults{}, iodine.New(err, errParams)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return ListObjectsResults{}, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	listObjects, err := donut.buckets[bucket].ListObjects(prefix, marker, delimiter, maxkeys)
	if err != nil {
		return ListObjectsResults{}, iodine.New(err, errParams)
	}
	return listObjects, nil
}

// putObject - put object
func (donut API) putObject(bucket, object, expectedMD5Sum string, reader io.Reader, metadata map[string]string, signature *Signature) (ObjectMetadata, error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return ObjectMetadata{}, iodine.New(InvalidArgument{}, errParams)
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return ObjectMetadata{}, iodine.New(InvalidArgument{}, errParams)
	}
	if err := donut.listDonutBuckets(); err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return ObjectMetadata{}, iodine.New(BucketNotFound{Bucket: bucket}, nil)
	}
	bucketMeta, err := donut.getDonutBucketMetadata()
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := bucketMeta.Buckets[bucket].BucketObjects[object]; ok {
		return ObjectMetadata{}, iodine.New(ObjectExists{Object: object}, errParams)
	}
	objMetadata, err := donut.buckets[bucket].WriteObject(object, reader, expectedMD5Sum, metadata, signature)
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	bucketMeta.Buckets[bucket].BucketObjects[object] = struct{}{}
	if err := donut.setDonutBucketMetadata(bucketMeta); err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	return objMetadata, nil
}

// putObject - put object
func (donut API) putObjectPart(bucket, object, expectedMD5Sum, uploadID string, partID int, reader io.Reader, metadata map[string]string, signature *Signature) (PartMetadata, error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return PartMetadata{}, iodine.New(InvalidArgument{}, errParams)
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return PartMetadata{}, iodine.New(InvalidArgument{}, errParams)
	}
	if err := donut.listDonutBuckets(); err != nil {
		return PartMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return PartMetadata{}, iodine.New(BucketNotFound{Bucket: bucket}, nil)
	}
	bucketMeta, err := donut.getDonutBucketMetadata()
	if err != nil {
		return PartMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := bucketMeta.Buckets[bucket].Multiparts[object]; !ok {
		return PartMetadata{}, iodine.New(InvalidUploadID{UploadID: uploadID}, nil)
	}
	if _, ok := bucketMeta.Buckets[bucket].BucketObjects[object]; ok {
		return PartMetadata{}, iodine.New(ObjectExists{Object: object}, errParams)
	}
	objectPart := object + "/" + "multipart" + "/" + strconv.Itoa(partID)
	objmetadata, err := donut.buckets[bucket].WriteObject(objectPart, reader, expectedMD5Sum, metadata, signature)
	if err != nil {
		return PartMetadata{}, iodine.New(err, errParams)
	}
	partMetadata := PartMetadata{
		PartNumber:   partID,
		LastModified: objmetadata.Created,
		ETag:         objmetadata.MD5Sum,
		Size:         objmetadata.Size,
	}
	multipartSession := bucketMeta.Buckets[bucket].Multiparts[object]
	multipartSession.Parts[strconv.Itoa(partID)] = partMetadata
	bucketMeta.Buckets[bucket].Multiparts[object] = multipartSession
	if err := donut.setDonutBucketMetadata(bucketMeta); err != nil {
		return PartMetadata{}, iodine.New(err, errParams)
	}
	return partMetadata, nil
}

// getObject - get object
func (donut API) getObject(bucket, object string) (reader io.ReadCloser, size int64, err error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return nil, 0, iodine.New(InvalidArgument{}, errParams)
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return nil, 0, iodine.New(InvalidArgument{}, errParams)
	}
	if err := donut.listDonutBuckets(); err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return nil, 0, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	return donut.buckets[bucket].ReadObject(object)
}

// getObjectMetadata - get object metadata
func (donut API) getObjectMetadata(bucket, object string) (ObjectMetadata, error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if err := donut.listDonutBuckets(); err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return ObjectMetadata{}, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	bucketMeta, err := donut.getDonutBucketMetadata()
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := bucketMeta.Buckets[bucket].BucketObjects[object]; !ok {
		return ObjectMetadata{}, iodine.New(ObjectNotFound{Object: object}, errParams)
	}
	objectMetadata, err := donut.buckets[bucket].GetObjectMetadata(object)
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, nil)
	}
	return objectMetadata, nil
}

// newMultipartUpload - new multipart upload request
func (donut API) newMultipartUpload(bucket, object, contentType string) (string, error) {
	errParams := map[string]string{
		"bucket":      bucket,
		"object":      object,
		"contentType": contentType,
	}
	if err := donut.listDonutBuckets(); err != nil {
		return "", iodine.New(err, errParams)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return "", iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	allbuckets, err := donut.getDonutBucketMetadata()
	if err != nil {
		return "", iodine.New(err, errParams)
	}
	bucketMetadata := allbuckets.Buckets[bucket]
	multiparts := make(map[string]MultiPartSession)
	if len(bucketMetadata.Multiparts) > 0 {
		multiparts = bucketMetadata.Multiparts
	}

	id := []byte(strconv.Itoa(rand.Int()) + bucket + object + time.Now().String())
	uploadIDSum := sha512.Sum512(id)
	uploadID := base64.URLEncoding.EncodeToString(uploadIDSum[:])[:47]

	multipartSession := MultiPartSession{
		UploadID:   uploadID,
		Initiated:  time.Now().UTC(),
		Parts:      make(map[string]PartMetadata),
		TotalParts: 0,
	}
	multiparts[object] = multipartSession
	bucketMetadata.Multiparts = multiparts
	allbuckets.Buckets[bucket] = bucketMetadata

	if err := donut.setDonutBucketMetadata(allbuckets); err != nil {
		return "", iodine.New(err, errParams)
	}

	return uploadID, nil
}

// listObjectParts list all object parts
func (donut API) listObjectParts(bucket, object string, resources ObjectResourcesMetadata) (ObjectResourcesMetadata, error) {
	errParams := map[string]string{
		"bucket": bucket,
		"object": object,
	}
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return ObjectResourcesMetadata{}, iodine.New(InvalidArgument{}, errParams)
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return ObjectResourcesMetadata{}, iodine.New(InvalidArgument{}, errParams)
	}
	if err := donut.listDonutBuckets(); err != nil {
		return ObjectResourcesMetadata{}, iodine.New(err, nil)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return ObjectResourcesMetadata{}, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	allBuckets, err := donut.getDonutBucketMetadata()
	if err != nil {
		return ObjectResourcesMetadata{}, iodine.New(err, errParams)
	}
	bucketMetadata := allBuckets.Buckets[bucket]
	if _, ok := bucketMetadata.Multiparts[object]; !ok {
		return ObjectResourcesMetadata{}, iodine.New(InvalidUploadID{UploadID: resources.UploadID}, errParams)
	}
	if bucketMetadata.Multiparts[object].UploadID != resources.UploadID {
		return ObjectResourcesMetadata{}, iodine.New(InvalidUploadID{UploadID: resources.UploadID}, errParams)
	}
	objectResourcesMetadata := resources
	objectResourcesMetadata.Bucket = bucket
	objectResourcesMetadata.Key = object
	var parts []*PartMetadata
	var startPartNumber int
	switch {
	case objectResourcesMetadata.PartNumberMarker == 0:
		startPartNumber = 1
	default:
		startPartNumber = objectResourcesMetadata.PartNumberMarker
	}
	for i := startPartNumber; i <= bucketMetadata.Multiparts[object].TotalParts; i++ {
		if len(parts) > objectResourcesMetadata.MaxParts {
			sort.Sort(partNumber(parts))
			objectResourcesMetadata.IsTruncated = true
			objectResourcesMetadata.Part = parts
			objectResourcesMetadata.NextPartNumberMarker = i
			return objectResourcesMetadata, nil
		}
		part, ok := bucketMetadata.Multiparts[object].Parts[strconv.Itoa(i)]
		if !ok {
			return ObjectResourcesMetadata{}, iodine.New(InvalidPart{}, nil)
		}
		parts = append(parts, &part)
	}
	sort.Sort(partNumber(parts))
	objectResourcesMetadata.Part = parts
	return objectResourcesMetadata, nil
}

// completeMultipartUpload complete an incomplete multipart upload
func (donut API) completeMultipartUpload(bucket, object, uploadID string, data io.Reader, signature *Signature) (ObjectMetadata, error) {
	errParams := map[string]string{
		"bucket":   bucket,
		"object":   object,
		"uploadID": uploadID,
	}
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return ObjectMetadata{}, iodine.New(InvalidArgument{}, errParams)
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return ObjectMetadata{}, iodine.New(InvalidArgument{}, errParams)
	}
	if err := donut.listDonutBuckets(); err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return ObjectMetadata{}, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	allBuckets, err := donut.getDonutBucketMetadata()
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	bucketMetadata := allBuckets.Buckets[bucket]
	if _, ok := bucketMetadata.Multiparts[object]; !ok {
		return ObjectMetadata{}, iodine.New(InvalidUploadID{UploadID: uploadID}, errParams)
	}
	if bucketMetadata.Multiparts[object].UploadID != uploadID {
		return ObjectMetadata{}, iodine.New(InvalidUploadID{UploadID: uploadID}, errParams)
	}
	partBytes, err := ioutil.ReadAll(data)
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, errParams)
	}
	if signature != nil {
		ok, err := signature.DoesSignatureMatch(hex.EncodeToString(sha256.Sum256(partBytes)[:]))
		if err != nil {
			return ObjectMetadata{}, iodine.New(err, errParams)
		}
		if !ok {
			return ObjectMetadata{}, iodine.New(SignatureDoesNotMatch{}, errParams)
		}
	}
	parts := &CompleteMultipartUpload{}
	if err := xml.Unmarshal(partBytes, parts); err != nil {
		return ObjectMetadata{}, iodine.New(MalformedXML{}, errParams)
	}
	if !sort.IsSorted(completedParts(parts.Part)) {
		return ObjectMetadata{}, iodine.New(InvalidPartOrder{}, errParams)
	}
	for _, part := range parts.Part {
		if part.ETag != bucketMetadata.Multiparts[object].Parts[strconv.Itoa(part.PartNumber)].ETag {
			return ObjectMetadata{}, iodine.New(InvalidPart{}, errParams)
		}
	}
	var finalETagBytes []byte
	var finalSize int64
	totalParts := strconv.Itoa(bucketMetadata.Multiparts[object].TotalParts)
	for _, part := range bucketMetadata.Multiparts[object].Parts {
		partETagBytes, err := hex.DecodeString(part.ETag)
		if err != nil {
			return ObjectMetadata{}, iodine.New(err, errParams)
		}
		finalETagBytes = append(finalETagBytes, partETagBytes...)
		finalSize += part.Size
	}
	finalETag := hex.EncodeToString(finalETagBytes)
	objMetadata := ObjectMetadata{}
	objMetadata.MD5Sum = finalETag + "-" + totalParts
	objMetadata.Object = object
	objMetadata.Bucket = bucket
	objMetadata.Size = finalSize
	objMetadata.Created = bucketMetadata.Multiparts[object].Parts[totalParts].LastModified
	return objMetadata, nil
}

// listMultipartUploads list all multipart uploads
func (donut API) listMultipartUploads(bucket string, resources BucketMultipartResourcesMetadata) (BucketMultipartResourcesMetadata, error) {
	errParams := map[string]string{
		"bucket": bucket,
	}
	if err := donut.listDonutBuckets(); err != nil {
		return BucketMultipartResourcesMetadata{}, iodine.New(err, errParams)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return BucketMultipartResourcesMetadata{}, iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	allbuckets, err := donut.getDonutBucketMetadata()
	if err != nil {
		return BucketMultipartResourcesMetadata{}, iodine.New(err, errParams)
	}
	bucketMetadata := allbuckets.Buckets[bucket]
	var uploads []*UploadMetadata
	for key, session := range bucketMetadata.Multiparts {
		if strings.HasPrefix(key, resources.Prefix) {
			if len(uploads) > resources.MaxUploads {
				sort.Sort(byKey(uploads))
				resources.Upload = uploads
				resources.NextKeyMarker = key
				resources.NextUploadIDMarker = session.UploadID
				resources.IsTruncated = true
				return resources, nil
			}
			// uploadIDMarker is ignored if KeyMarker is empty
			switch {
			case resources.KeyMarker != "" && resources.UploadIDMarker == "":
				if key > resources.KeyMarker {
					upload := new(UploadMetadata)
					upload.Key = key
					upload.UploadID = session.UploadID
					upload.Initiated = session.Initiated
					uploads = append(uploads, upload)
				}
			case resources.KeyMarker != "" && resources.UploadIDMarker != "":
				if session.UploadID > resources.UploadIDMarker {
					if key >= resources.KeyMarker {
						upload := new(UploadMetadata)
						upload.Key = key
						upload.UploadID = session.UploadID
						upload.Initiated = session.Initiated
						uploads = append(uploads, upload)
					}
				}
			default:
				upload := new(UploadMetadata)
				upload.Key = key
				upload.UploadID = session.UploadID
				upload.Initiated = session.Initiated
				uploads = append(uploads, upload)
			}
		}
	}
	sort.Sort(byKey(uploads))
	resources.Upload = uploads
	return resources, nil
}

// abortMultipartUpload - abort a incomplete multipart upload
func (donut API) abortMultipartUpload(bucket, object, uploadID string) error {
	errParams := map[string]string{
		"bucket":   bucket,
		"object":   object,
		"uploadID": uploadID,
	}
	if err := donut.listDonutBuckets(); err != nil {
		return iodine.New(err, errParams)
	}
	if _, ok := donut.buckets[bucket]; !ok {
		return iodine.New(BucketNotFound{Bucket: bucket}, errParams)
	}
	allbuckets, err := donut.getDonutBucketMetadata()
	if err != nil {
		return iodine.New(err, errParams)
	}
	bucketMetadata := allbuckets.Buckets[bucket]
	if _, ok := bucketMetadata.Multiparts[object]; !ok {
		return iodine.New(InvalidUploadID{UploadID: uploadID}, errParams)
	}
	if bucketMetadata.Multiparts[object].UploadID != uploadID {
		return iodine.New(InvalidUploadID{UploadID: uploadID}, errParams)
	}
	delete(bucketMetadata.Multiparts, object)

	allbuckets.Buckets[bucket] = bucketMetadata
	if err := donut.setDonutBucketMetadata(allbuckets); err != nil {
		return iodine.New(err, errParams)
	}

	return nil
}

//// internal functions

// getBucketMetadataWriters -
func (donut API) getBucketMetadataWriters() ([]io.WriteCloser, error) {
	var writers []io.WriteCloser
	for _, node := range donut.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		writers = make([]io.WriteCloser, len(disks))
		for order, disk := range disks {
			bucketMetaDataWriter, err := disk.CreateFile(filepath.Join(donut.config.DonutName, bucketMetadataConfig))
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			writers[order] = bucketMetaDataWriter
		}
	}
	return writers, nil
}

// getBucketMetadataReaders - readers are returned in map rather than slice
func (donut API) getBucketMetadataReaders() (map[int]io.ReadCloser, error) {
	readers := make(map[int]io.ReadCloser)
	disks := make(map[int]disk.Disk)
	var err error
	for _, node := range donut.nodes {
		nDisks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		for k, v := range nDisks {
			disks[k] = v
		}
	}
	var bucketMetaDataReader io.ReadCloser
	for order, disk := range disks {
		bucketMetaDataReader, err = disk.OpenFile(filepath.Join(donut.config.DonutName, bucketMetadataConfig))
		if err != nil {
			continue
		}
		readers[order] = bucketMetaDataReader
	}
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return readers, nil
}

// setDonutBucketMetadata -
func (donut API) setDonutBucketMetadata(metadata *AllBuckets) error {
	writers, err := donut.getBucketMetadataWriters()
	if err != nil {
		return iodine.New(err, nil)
	}
	for _, writer := range writers {
		jenc := json.NewEncoder(writer)
		if err := jenc.Encode(metadata); err != nil {
			CleanupWritersOnError(writers)
			return iodine.New(err, nil)
		}
	}
	for _, writer := range writers {
		writer.Close()
	}
	return nil
}

// getDonutBucketMetadata -
func (donut API) getDonutBucketMetadata() (*AllBuckets, error) {
	metadata := &AllBuckets{}
	var err error
	readers, err := donut.getBucketMetadataReaders()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	for _, reader := range readers {
		defer reader.Close()
	}
	for _, reader := range readers {
		jenc := json.NewDecoder(reader)
		if err = jenc.Decode(metadata); err == nil {
			return metadata, nil
		}
	}
	return nil, iodine.New(err, nil)
}

// makeDonutBucket -
func (donut API) makeDonutBucket(bucketName, acl string) error {
	if err := donut.listDonutBuckets(); err != nil {
		return iodine.New(err, nil)
	}
	if _, ok := donut.buckets[bucketName]; ok {
		return iodine.New(BucketExists{Bucket: bucketName}, nil)
	}
	bucket, bucketMetadata, err := newBucket(bucketName, acl, donut.config.DonutName, donut.nodes)
	if err != nil {
		return iodine.New(err, nil)
	}
	nodeNumber := 0
	donut.buckets[bucketName] = bucket
	for _, node := range donut.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return iodine.New(err, nil)
		}
		for order, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", bucketName, nodeNumber, order)
			err := disk.MakeDir(filepath.Join(donut.config.DonutName, bucketSlice))
			if err != nil {
				return iodine.New(err, nil)
			}
		}
		nodeNumber = nodeNumber + 1
	}
	metadata, err := donut.getDonutBucketMetadata()
	if err != nil {
		if os.IsNotExist(iodine.ToError(err)) {
			metadata := new(AllBuckets)
			metadata.Buckets = make(map[string]BucketMetadata)
			metadata.Buckets[bucketName] = bucketMetadata
			err = donut.setDonutBucketMetadata(metadata)
			if err != nil {
				return iodine.New(err, nil)
			}
			return nil
		}
		return iodine.New(err, nil)
	}
	metadata.Buckets[bucketName] = bucketMetadata
	err = donut.setDonutBucketMetadata(metadata)
	if err != nil {
		return iodine.New(err, nil)
	}
	return nil
}

// listDonutBuckets -
func (donut API) listDonutBuckets() error {
	var disks map[int]disk.Disk
	var err error
	for _, node := range donut.nodes {
		disks, err = node.ListDisks()
		if err != nil {
			return iodine.New(err, nil)
		}
	}
	var dirs []os.FileInfo
	for _, disk := range disks {
		dirs, err = disk.ListDir(donut.config.DonutName)
		if err == nil {
			break
		}
	}
	// if all disks are missing then return error
	if err != nil {
		return iodine.New(err, nil)
	}
	for _, dir := range dirs {
		splitDir := strings.Split(dir.Name(), "$")
		if len(splitDir) < 3 {
			return iodine.New(CorruptedBackend{Backend: dir.Name()}, nil)
		}
		bucketName := splitDir[0]
		// we dont need this once we cache from makeDonutBucket()
		bucket, _, err := newBucket(bucketName, "private", donut.config.DonutName, donut.nodes)
		if err != nil {
			return iodine.New(err, nil)
		}
		donut.buckets[bucketName] = bucket
	}
	return nil
}
