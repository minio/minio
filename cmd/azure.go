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
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
)

const globalAzureAPIVersion = "2016-05-31"

// AzureObjects - Implements Object layer for Azure blob storage.
type AzureObjects struct {
	client storage.BlobStorageClient // Azure sdk client
}

// Convert azure errors to minio object layer errors.
func azureToObjectError(err error, params ...string) error {
	bucket := ""
	object := ""
	if len(params) >= 1 {
		bucket = params[0]
	}
	if len(params) == 2 {
		object = params[1]
	}
	actualErr := err
	traceErr, isTraceErr := err.(*Error)
	if isTraceErr {
		actualErr = traceErr.e
	}

	azureErr, ok := actualErr.(storage.AzureStorageServiceError)
	if !ok {
		// We don't interpret non Azure errors. As azure errors will
		// have StatusCode to help to convert to object errors.
		return err
	}
	var objErr error
	switch azureErr.StatusCode {
	case http.StatusNotFound:
		objErr = ObjectNotFound{bucket, object}
	}
	if isTraceErr {
		// Incase the passed err was a trace Error then just replace the
		// encapsulated error.
		traceErr.e = objErr
		return traceErr
	}
	return objErr
}

// Inits azure blob storage client and returns AzureObjects.
func newAzureLayer(account, key string) (ObjectLayer, error) {
	useHTTPS := true
	c, err := storage.NewClient(account, key, storage.DefaultBaseURL, globalAzureAPIVersion, useHTTPS)
	if err != nil {
		return AzureObjects{}, err
	}
	client := c.GetBlobService()
	return &AzureObjects{client}, nil
}

// Shutdown - Not relevant to Azure.
func (a AzureObjects) Shutdown() error {
	return nil
}

// StorageInfo - Not relevant to Azure.
func (a AzureObjects) StorageInfo() StorageInfo {
	return StorageInfo{}
}

// MakeBucket - Create bucket.
func (a AzureObjects) MakeBucket(bucket string) error {
	return azureToObjectError(traceError(a.client.CreateContainer(bucket, storage.ContainerAccessTypePrivate)), bucket)
}

// GetBucketInfo - Get bucket metadata.
func (a AzureObjects) GetBucketInfo(bucket string) (BucketInfo, error) {
	// Azure does not have an equivalent call, hence use ListContainers.
	resp, err := a.client.ListContainers(storage.ListContainersParameters{
		Prefix: bucket,
	})
	if err != nil {
		return BucketInfo{}, azureToObjectError(traceError(err), bucket)
	}
	for _, container := range resp.Containers {
		if container.Name == bucket {
			t, e := time.Parse(time.RFC1123, container.Properties.LastModified)
			if e != nil {
				continue
			}
			return BucketInfo{
				Name:    bucket,
				Created: t,
			}, nil
		}
	}
	return BucketInfo{}, traceError(BucketNotFound{})
}

// ListBuckets - Use Azure equivalent ListContainers.
func (a AzureObjects) ListBuckets() (buckets []BucketInfo, err error) {
	resp, err := a.client.ListContainers(storage.ListContainersParameters{})
	if err != nil {
		return nil, azureToObjectError(traceError(err))
	}
	for _, container := range resp.Containers {
		t, e := time.Parse(time.RFC1123, container.Properties.LastModified)
		if e != nil {
			return nil, traceError(e)
		}
		buckets = append(buckets, BucketInfo{
			Name:    container.Name,
			Created: t,
		})
	}
	return buckets, nil
}

// DeleteBucket - Use Azure equivalent DeleteContainer.
func (a AzureObjects) DeleteBucket(bucket string) error {
	return azureToObjectError(traceError(a.client.DeleteContainer(bucket)), bucket)
}

// ListObjects - Use Azure equivalent ListBlobs.
func (a AzureObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	resp, err := a.client.ListBlobs(bucket, storage.ListBlobsParameters{
		Prefix:     prefix,
		Marker:     marker,
		Delimiter:  delimiter,
		MaxResults: uint(maxKeys),
	})
	if err != nil {
		return result, azureToObjectError(traceError(err), bucket, prefix)
	}
	result.IsTruncated = resp.NextMarker != ""
	result.NextMarker = resp.NextMarker
	for _, object := range resp.Blobs {
		t, e := time.Parse(time.RFC1123, object.Properties.LastModified)
		if e != nil {
			continue
		}
		result.Objects = append(result.Objects, ObjectInfo{
			Bucket:          bucket,
			Name:            object.Name,
			ModTime:         t,
			Size:            object.Properties.ContentLength,
			MD5Sum:          object.Properties.Etag,
			ContentType:     object.Properties.ContentType,
			ContentEncoding: object.Properties.ContentEncoding,
		})
	}
	result.Prefixes = resp.BlobPrefixes
	return result, nil
}

// GetObject - Use Azure equivalent GetBlobRange.
func (a AzureObjects) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	var byteRange string

	if length == -1 {
		byteRange = fmt.Sprintf("%d-", startOffset)
	} else {
		byteRange = fmt.Sprintf("%d-%d", startOffset, startOffset+length-1)
	}

	rc, err := a.client.GetBlobRange(bucket, object, byteRange, nil)
	if err != nil {
		return azureToObjectError(traceError(err), bucket, object)
	}
	_, err = io.Copy(writer, rc)
	rc.Close()
	return traceError(err)
}

// GetObjectInfo - Use Azure equivalent GetBlobProperties.
func (a AzureObjects) GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	prop, err := a.client.GetBlobProperties(bucket, object)
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}
	t, err := time.Parse(time.RFC1123, prop.LastModified)
	if err != nil {
		return objInfo, traceError(err)
	}
	objInfo.Bucket = bucket
	objInfo.ContentEncoding = prop.ContentEncoding
	objInfo.ContentType = prop.ContentType
	objInfo.MD5Sum = prop.Etag
	objInfo.ModTime = t
	objInfo.Name = object
	objInfo.Size = prop.ContentLength
	return
}

// PutObject - Use Azure equivalent CreateBlockBlobFromReader.
func (a AzureObjects) PutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error) {
	err = a.client.CreateBlockBlobFromReader(bucket, object, uint64(size), data, nil)
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}
	return a.GetObjectInfo(bucket, object)
}

// CopyObject - Use Azure equivalent CopyBlob.
func (a AzureObjects) CopyObject(srcBucket, srcObject, destBucket, destObject string, metadata map[string]string) (objInfo ObjectInfo, err error) {
	err = a.client.CopyBlob(destBucket, destObject, a.client.GetBlobURL(srcBucket, srcObject))
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), srcBucket, srcObject)
	}
	return a.GetObjectInfo(destBucket, destObject)
}

// DeleteObject - Use azure equivalent DeleteBlob.
func (a AzureObjects) DeleteObject(bucket, object string) error {
	err := a.client.DeleteBlob(bucket, object, nil)
	if err != nil {
		return azureToObjectError(traceError(err), bucket, object)
	}

	return nil
}

// ListMultipartUploads - Incomplete implementation, for now just return the prefix if it is an incomplete upload.
func (a AzureObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	// FIXME: Full ListMultipartUploads is not supported yet. It is supported just enough to help our client libs to
	// support re-uploads.
	result.MaxUploads = maxUploads
	result.Prefix = prefix
	result.Delimiter = delimiter
	resp, err := a.ListObjectParts(bucket, prefix, prefix, 1, 1000)
	if err != nil {
		// In case ListObjectParts returns error, it would mean that no such incomplete upload exists on
		// azure storage - in which case we return nil. This plays a role for mc and our client libs. i.e client
		// does ListMultipartUploads to figure the previous uploadID to continue uploading from where it left off.
		// If we return error the upload never starts and always returns error.
		return result, nil
	}
	if len(resp.Parts) > 0 {
		result.Uploads = []uploadMetadata{{prefix, prefix, time.Now().UTC(), ""}}
	}
	return result, nil
}

// NewMultipartUpload - Use Azure equivalent CreateBlockBlob.
func (a AzureObjects) NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error) {
	// Azure doesn't return a unique upload ID and we use object name in place of it. Azure allows multiple uploads to
	// co-exist as long as the user keeps the blocks uploaded (in block blobs) unique amongst concurrent upload attempts.
	// Each concurrent client, keeps its own blockID list which it can commit.
	uploadID = object
	return uploadID, azureToObjectError(traceError(a.client.CreateBlockBlob(bucket, object)))
}

// CopyObjectPart - Not implemented.
func (a AzureObjects) CopyObjectPart(srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error) {
	return info, traceError(NotImplemented{})
}

// Encode partID+md5Hex to a blockID.
func azureGetBlockID(partID int, md5Hex string) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%.5d.%s", partID, md5Hex)))
}

// Decode blockID to partID+md5Hex.
func azureParseBlockID(blockID string) (int, string, error) {
	idByte, err := base64.StdEncoding.DecodeString(blockID)
	if err != nil {
		return 0, "", traceError(err)
	}
	idStr := string(idByte)
	splitRes := strings.Split(idStr, ".")
	if len(splitRes) != 2 {
		return 0, "", traceError(errUnexpected)
	}
	partID, err := strconv.Atoi(splitRes[0])
	if err != nil {
		return 0, "", traceError(err)
	}
	return partID, splitRes[1], nil
}

// PutObjectPart - Use Azure equivalent PutBlockWithLength.
func (a AzureObjects) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string, sha256sum string) (info PartInfo, err error) {
	id := azureGetBlockID(partID, md5Hex)
	err = a.client.PutBlockWithLength(bucket, object, id, uint64(size), data, nil)
	if err != nil {
		return info, azureToObjectError(traceError(err), bucket, object)
	}
	info.PartNumber = partID
	info.ETag = md5Hex
	info.LastModified = time.Now().UTC()
	info.Size = size
	return info, nil
}

// ListObjectParts - Use Azure equivalent GetBlockList.
func (a AzureObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error) {
	resp, err := a.client.GetBlockList(bucket, object, storage.BlockListTypeUncommitted)
	if err != nil {
		return result, azureToObjectError(traceError(err), bucket, object)
	}
	for _, part := range resp.UncommittedBlocks {
		partID, md5Hex, err := azureParseBlockID(part.Name)
		if err != nil {
			return result, err
		}
		result.Parts = append(result.Parts, PartInfo{
			partID,
			time.Now().UTC(),
			md5Hex,
			part.Size,
		})
	}
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	return result, nil
}

// AbortMultipartUpload - Not Implemented.
// There is no corresponding API in azure to abort an incomplete upload. The uncommmitted blocks
// gets deleted after one week.
func (a AzureObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	return traceError(NotImplemented{})
}

// CompleteMultipartUpload - Use Azure equivalent PutBlockList.
func (a AzureObjects) CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []completePart) (objInfo ObjectInfo, err error) {
	var blocks []storage.Block
	for _, part := range uploadedParts {
		blocks = append(blocks, storage.Block{
			ID:     azureGetBlockID(part.PartNumber, part.ETag),
			Status: storage.BlockStatusUncommitted,
		})
	}
	err = a.client.PutBlockList(bucket, object, blocks)
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}
	return a.GetObjectInfo(bucket, object)
}

// HealBucket - Not relevant.
func (a AzureObjects) HealBucket(bucket string) error {
	return traceError(NotImplemented{})
}

// ListBucketsHeal - Not relevant.
func (a AzureObjects) ListBucketsHeal() (buckets []BucketInfo, err error) {
	return nil, traceError(NotImplemented{})
}

// HealObject - Not relevant.
func (a AzureObjects) HealObject(bucket, object string) error {
	return traceError(NotImplemented{})
}

// ListObjectsHeal - Not relevant.
func (a AzureObjects) ListObjectsHeal(bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	return ListObjectsInfo{}, traceError(NotImplemented{})
}
