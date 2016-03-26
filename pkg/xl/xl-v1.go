// +build ignore

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

package xl

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
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/s3/signature4"
	"github.com/minio/minio/pkg/xl/block"
)

// config files used inside XL
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
func (xl API) makeBucket(bucket string, acl BucketACL) *probe.Error {
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return probe.NewError(InvalidArgument{})
	}
	return xl.makeXLBucket(bucket, acl.String())
}

// getBucketMetadata - get bucket metadata
func (xl API) getBucketMetadata(bucketName string) (BucketMetadata, *probe.Error) {
	if err := xl.listXLBuckets(); err != nil {
		return BucketMetadata{}, err.Trace()
	}
	if _, ok := xl.buckets[bucketName]; !ok {
		return BucketMetadata{}, probe.NewError(BucketNotFound{Bucket: bucketName})
	}
	metadata, err := xl.getXLBucketMetadata()
	if err != nil {
		return BucketMetadata{}, err.Trace()
	}
	return metadata.Buckets[bucketName], nil
}

// setBucketMetadata - set bucket metadata
func (xl API) setBucketMetadata(bucketName string, bucketMetadata map[string]string) *probe.Error {
	if err := xl.listXLBuckets(); err != nil {
		return err.Trace()
	}
	metadata, err := xl.getXLBucketMetadata()
	if err != nil {
		return err.Trace()
	}
	oldBucketMetadata := metadata.Buckets[bucketName]
	acl, ok := bucketMetadata["acl"]
	if !ok {
		return probe.NewError(InvalidArgument{})
	}
	oldBucketMetadata.ACL = BucketACL(acl)
	metadata.Buckets[bucketName] = oldBucketMetadata
	return xl.setXLBucketMetadata(metadata)
}

// listBuckets - return list of buckets
func (xl API) listBuckets() (map[string]BucketMetadata, *probe.Error) {
	if err := xl.listXLBuckets(); err != nil {
		return nil, err.Trace()
	}
	metadata, err := xl.getXLBucketMetadata()
	if err != nil {
		// intentionally left out the error when XL is empty
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
func (xl API) listObjects(bucket, prefix, marker, delimiter string, maxkeys int) (ListObjectsResults, *probe.Error) {
	if err := xl.listXLBuckets(); err != nil {
		return ListObjectsResults{}, err.Trace()
	}
	if _, ok := xl.buckets[bucket]; !ok {
		return ListObjectsResults{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	listObjects, err := xl.buckets[bucket].ListObjects(prefix, marker, delimiter, maxkeys)
	if err != nil {
		return ListObjectsResults{}, err.Trace()
	}
	return listObjects, nil
}

// putObject - put object
func (xl API) putObject(bucket, object, expectedMD5Sum string, reader io.Reader, size int64, metadata map[string]string, signature *signature4.Sign) (ObjectMetadata, *probe.Error) {
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return ObjectMetadata{}, probe.NewError(InvalidArgument{})
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return ObjectMetadata{}, probe.NewError(InvalidArgument{})
	}
	if err := xl.listXLBuckets(); err != nil {
		return ObjectMetadata{}, err.Trace()
	}
	if _, ok := xl.buckets[bucket]; !ok {
		return ObjectMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	bucketMeta, err := xl.getXLBucketMetadata()
	if err != nil {
		return ObjectMetadata{}, err.Trace()
	}
	if _, ok := bucketMeta.Buckets[bucket].BucketObjects[object]; ok {
		return ObjectMetadata{}, probe.NewError(ObjectExists{Object: object})
	}
	objMetadata, err := xl.buckets[bucket].WriteObject(object, reader, size, expectedMD5Sum, metadata, signature)
	if err != nil {
		return ObjectMetadata{}, err.Trace()
	}
	bucketMeta.Buckets[bucket].BucketObjects[object] = struct{}{}
	if err := xl.setXLBucketMetadata(bucketMeta); err != nil {
		return ObjectMetadata{}, err.Trace()
	}
	return objMetadata, nil
}

// putObject - put object
func (xl API) putObjectPart(bucket, object, expectedMD5Sum, uploadID string, partID int, reader io.Reader, size int64, metadata map[string]string, signature *signature4.Sign) (PartMetadata, *probe.Error) {
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return PartMetadata{}, probe.NewError(InvalidArgument{})
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return PartMetadata{}, probe.NewError(InvalidArgument{})
	}
	if err := xl.listXLBuckets(); err != nil {
		return PartMetadata{}, err.Trace()
	}
	if _, ok := xl.buckets[bucket]; !ok {
		return PartMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	bucketMeta, err := xl.getXLBucketMetadata()
	if err != nil {
		return PartMetadata{}, err.Trace()
	}
	if _, ok := bucketMeta.Buckets[bucket].Multiparts[object]; !ok {
		return PartMetadata{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	if _, ok := bucketMeta.Buckets[bucket].BucketObjects[object]; ok {
		return PartMetadata{}, probe.NewError(ObjectExists{Object: object})
	}
	objectPart := object + "/" + "multipart" + "/" + strconv.Itoa(partID)
	objmetadata, err := xl.buckets[bucket].WriteObject(objectPart, reader, size, expectedMD5Sum, metadata, signature)
	if err != nil {
		return PartMetadata{}, err.Trace()
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
	if err := xl.setXLBucketMetadata(bucketMeta); err != nil {
		return PartMetadata{}, err.Trace()
	}
	return partMetadata, nil
}

// getObject - get object
func (xl API) getObject(bucket, object string) (reader io.ReadCloser, size int64, err *probe.Error) {
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return nil, 0, probe.NewError(InvalidArgument{})
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return nil, 0, probe.NewError(InvalidArgument{})
	}
	if err := xl.listXLBuckets(); err != nil {
		return nil, 0, err.Trace()
	}
	if _, ok := xl.buckets[bucket]; !ok {
		return nil, 0, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	return xl.buckets[bucket].ReadObject(object)
}

// getObjectMetadata - get object metadata
func (xl API) getObjectMetadata(bucket, object string) (ObjectMetadata, *probe.Error) {
	if err := xl.listXLBuckets(); err != nil {
		return ObjectMetadata{}, err.Trace()
	}
	if _, ok := xl.buckets[bucket]; !ok {
		return ObjectMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	bucketMeta, err := xl.getXLBucketMetadata()
	if err != nil {
		return ObjectMetadata{}, err.Trace()
	}
	if _, ok := bucketMeta.Buckets[bucket].BucketObjects[object]; !ok {
		return ObjectMetadata{}, probe.NewError(ObjectNotFound{Object: object})
	}
	objectMetadata, err := xl.buckets[bucket].GetObjectMetadata(object)
	if err != nil {
		return ObjectMetadata{}, err.Trace()
	}
	return objectMetadata, nil
}

// newMultipartUpload - new multipart upload request
func (xl API) newMultipartUpload(bucket, object, contentType string) (string, *probe.Error) {
	if err := xl.listXLBuckets(); err != nil {
		return "", err.Trace()
	}
	if _, ok := xl.buckets[bucket]; !ok {
		return "", probe.NewError(BucketNotFound{Bucket: bucket})
	}
	allbuckets, err := xl.getXLBucketMetadata()
	if err != nil {
		return "", err.Trace()
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

	if err := xl.setXLBucketMetadata(allbuckets); err != nil {
		return "", err.Trace()
	}

	return uploadID, nil
}

// listObjectParts list all object parts
func (xl API) listObjectParts(bucket, object string, resources ObjectResourcesMetadata) (ObjectResourcesMetadata, *probe.Error) {
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return ObjectResourcesMetadata{}, probe.NewError(InvalidArgument{})
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return ObjectResourcesMetadata{}, probe.NewError(InvalidArgument{})
	}
	if err := xl.listXLBuckets(); err != nil {
		return ObjectResourcesMetadata{}, err.Trace()
	}
	if _, ok := xl.buckets[bucket]; !ok {
		return ObjectResourcesMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	allBuckets, err := xl.getXLBucketMetadata()
	if err != nil {
		return ObjectResourcesMetadata{}, err.Trace()
	}
	bucketMetadata := allBuckets.Buckets[bucket]
	if _, ok := bucketMetadata.Multiparts[object]; !ok {
		return ObjectResourcesMetadata{}, probe.NewError(InvalidUploadID{UploadID: resources.UploadID})
	}
	if bucketMetadata.Multiparts[object].UploadID != resources.UploadID {
		return ObjectResourcesMetadata{}, probe.NewError(InvalidUploadID{UploadID: resources.UploadID})
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
			return ObjectResourcesMetadata{}, probe.NewError(InvalidPart{})
		}
		parts = append(parts, &part)
	}
	sort.Sort(partNumber(parts))
	objectResourcesMetadata.Part = parts
	return objectResourcesMetadata, nil
}

// completeMultipartUpload complete an incomplete multipart upload
func (xl API) completeMultipartUpload(bucket, object, uploadID string, data io.Reader, signature *signature4.Sign) (ObjectMetadata, *probe.Error) {
	if bucket == "" || strings.TrimSpace(bucket) == "" {
		return ObjectMetadata{}, probe.NewError(InvalidArgument{})
	}
	if object == "" || strings.TrimSpace(object) == "" {
		return ObjectMetadata{}, probe.NewError(InvalidArgument{})
	}
	if err := xl.listXLBuckets(); err != nil {
		return ObjectMetadata{}, err.Trace()
	}
	if _, ok := xl.buckets[bucket]; !ok {
		return ObjectMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	allBuckets, err := xl.getXLBucketMetadata()
	if err != nil {
		return ObjectMetadata{}, err.Trace()
	}
	bucketMetadata := allBuckets.Buckets[bucket]
	if _, ok := bucketMetadata.Multiparts[object]; !ok {
		return ObjectMetadata{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	if bucketMetadata.Multiparts[object].UploadID != uploadID {
		return ObjectMetadata{}, probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	var partBytes []byte
	{
		var err error
		partBytes, err = ioutil.ReadAll(data)
		if err != nil {
			return ObjectMetadata{}, probe.NewError(err)
		}
	}
	if signature != nil {
		partHashBytes := sha256.Sum256(partBytes)
		ok, err := signature.DoesSignatureMatch(hex.EncodeToString(partHashBytes[:]))
		if err != nil {
			return ObjectMetadata{}, err.Trace()
		}
		if !ok {
			return ObjectMetadata{}, probe.NewError(SignDoesNotMatch{})
		}
	}
	parts := &CompleteMultipartUpload{}
	if err := xml.Unmarshal(partBytes, parts); err != nil {
		return ObjectMetadata{}, probe.NewError(MalformedXML{})
	}
	if !sort.IsSorted(completedParts(parts.Part)) {
		return ObjectMetadata{}, probe.NewError(InvalidPartOrder{})
	}
	for _, part := range parts.Part {
		if strings.Trim(part.ETag, "\"") != bucketMetadata.Multiparts[object].Parts[strconv.Itoa(part.PartNumber)].ETag {
			return ObjectMetadata{}, probe.NewError(InvalidPart{})
		}
	}
	var finalETagBytes []byte
	var finalSize int64
	totalParts := strconv.Itoa(bucketMetadata.Multiparts[object].TotalParts)
	for _, part := range bucketMetadata.Multiparts[object].Parts {
		partETagBytes, err := hex.DecodeString(part.ETag)
		if err != nil {
			return ObjectMetadata{}, probe.NewError(err)
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
func (xl API) listMultipartUploads(bucket string, resources BucketMultipartResourcesMetadata) (BucketMultipartResourcesMetadata, *probe.Error) {
	if err := xl.listXLBuckets(); err != nil {
		return BucketMultipartResourcesMetadata{}, err.Trace()
	}
	if _, ok := xl.buckets[bucket]; !ok {
		return BucketMultipartResourcesMetadata{}, probe.NewError(BucketNotFound{Bucket: bucket})
	}
	allbuckets, err := xl.getXLBucketMetadata()
	if err != nil {
		return BucketMultipartResourcesMetadata{}, err.Trace()
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
func (xl API) abortMultipartUpload(bucket, object, uploadID string) *probe.Error {
	if err := xl.listXLBuckets(); err != nil {
		return err.Trace()
	}
	if _, ok := xl.buckets[bucket]; !ok {
		return probe.NewError(BucketNotFound{Bucket: bucket})
	}
	allbuckets, err := xl.getXLBucketMetadata()
	if err != nil {
		return err.Trace()
	}
	bucketMetadata := allbuckets.Buckets[bucket]
	if _, ok := bucketMetadata.Multiparts[object]; !ok {
		return probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	if bucketMetadata.Multiparts[object].UploadID != uploadID {
		return probe.NewError(InvalidUploadID{UploadID: uploadID})
	}
	delete(bucketMetadata.Multiparts, object)

	allbuckets.Buckets[bucket] = bucketMetadata
	if err := xl.setXLBucketMetadata(allbuckets); err != nil {
		return err.Trace()
	}

	return nil
}

//// internal functions

// getBucketMetadataWriters -
func (xl API) getBucketMetadataWriters() ([]io.WriteCloser, *probe.Error) {
	var writers []io.WriteCloser
	for _, node := range xl.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, err.Trace()
		}
		writers = make([]io.WriteCloser, len(disks))
		for order, disk := range disks {
			bucketMetaDataWriter, err := disk.CreateFile(filepath.Join(xl.config.XLName, bucketMetadataConfig))
			if err != nil {
				return nil, err.Trace()
			}
			writers[order] = bucketMetaDataWriter
		}
	}
	return writers, nil
}

// getBucketMetadataReaders - readers are returned in map rather than slice
func (xl API) getBucketMetadataReaders() (map[int]io.ReadCloser, *probe.Error) {
	readers := make(map[int]io.ReadCloser)
	disks := make(map[int]block.Block)
	var err *probe.Error
	for _, node := range xl.nodes {
		nDisks := make(map[int]block.Block)
		nDisks, err = node.ListDisks()
		if err != nil {
			return nil, err.Trace()
		}
		for k, v := range nDisks {
			disks[k] = v
		}
	}
	var bucketMetaDataReader io.ReadCloser
	for order, disk := range disks {
		bucketMetaDataReader, err = disk.Open(filepath.Join(xl.config.XLName, bucketMetadataConfig))
		if err != nil {
			continue
		}
		readers[order] = bucketMetaDataReader
	}
	if err != nil {
		return nil, err.Trace()
	}
	return readers, nil
}

// setXLBucketMetadata -
func (xl API) setXLBucketMetadata(metadata *AllBuckets) *probe.Error {
	writers, err := xl.getBucketMetadataWriters()
	if err != nil {
		return err.Trace()
	}
	for _, writer := range writers {
		jenc := json.NewEncoder(writer)
		if err := jenc.Encode(metadata); err != nil {
			CleanupWritersOnError(writers)
			return probe.NewError(err)
		}
	}
	for _, writer := range writers {
		writer.Close()
	}
	return nil
}

// getXLBucketMetadata -
func (xl API) getXLBucketMetadata() (*AllBuckets, *probe.Error) {
	metadata := &AllBuckets{}
	readers, err := xl.getBucketMetadataReaders()
	if err != nil {
		return nil, err.Trace()
	}
	for _, reader := range readers {
		defer reader.Close()
	}
	{
		var err error
		for _, reader := range readers {
			jenc := json.NewDecoder(reader)
			if err = jenc.Decode(metadata); err == nil {
				return metadata, nil
			}
		}
		return nil, probe.NewError(err)
	}
}

// makeXLBucket -
func (xl API) makeXLBucket(bucketName, acl string) *probe.Error {
	if err := xl.listXLBuckets(); err != nil {
		return err.Trace()
	}
	if _, ok := xl.buckets[bucketName]; ok {
		return probe.NewError(BucketExists{Bucket: bucketName})
	}
	bkt, bucketMetadata, err := newBucket(bucketName, acl, xl.config.XLName, xl.nodes)
	if err != nil {
		return err.Trace()
	}
	nodeNumber := 0
	xl.buckets[bucketName] = bkt
	for _, node := range xl.nodes {
		disks := make(map[int]block.Block)
		disks, err = node.ListDisks()
		if err != nil {
			return err.Trace()
		}
		for order, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", bucketName, nodeNumber, order)
			err := disk.MakeDir(filepath.Join(xl.config.XLName, bucketSlice))
			if err != nil {
				return err.Trace()
			}
		}
		nodeNumber = nodeNumber + 1
	}
	var metadata *AllBuckets
	metadata, err = xl.getXLBucketMetadata()
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			metadata = new(AllBuckets)
			metadata.Buckets = make(map[string]BucketMetadata)
			metadata.Buckets[bucketName] = bucketMetadata
			err = xl.setXLBucketMetadata(metadata)
			if err != nil {
				return err.Trace()
			}
			return nil
		}
		return err.Trace()
	}
	metadata.Buckets[bucketName] = bucketMetadata
	err = xl.setXLBucketMetadata(metadata)
	if err != nil {
		return err.Trace()
	}
	return nil
}

// listXLBuckets -
func (xl API) listXLBuckets() *probe.Error {
	var disks map[int]block.Block
	var err *probe.Error
	for _, node := range xl.nodes {
		disks, err = node.ListDisks()
		if err != nil {
			return err.Trace()
		}
	}
	var dirs []os.FileInfo
	for _, disk := range disks {
		dirs, err = disk.ListDir(xl.config.XLName)
		if err == nil {
			break
		}
	}
	// if all disks are missing then return error
	if err != nil {
		return err.Trace()
	}
	for _, dir := range dirs {
		splitDir := strings.Split(dir.Name(), "$")
		if len(splitDir) < 3 {
			return probe.NewError(CorruptedBackend{Backend: dir.Name()})
		}
		bucketName := splitDir[0]
		// we dont need this once we cache from makeXLBucket()
		bkt, _, err := newBucket(bucketName, "private", xl.config.XLName, xl.nodes)
		if err != nil {
			return err.Trace()
		}
		xl.buckets[bucketName] = bkt
	}
	return nil
}
