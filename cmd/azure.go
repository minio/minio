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

// AzureObjects - Azure Object layer
type AzureObjects struct {
	client storage.BlobStorageClient // Azure sdk client
}

func newAzureLayer(account, key string) (ObjectLayer, error) {
	useHTTPS := true
	apiVersion := "2016-05-31"
	c, err := storage.NewClient(account, key, storage.DefaultBaseURL, apiVersion, useHTTPS)
	if err != nil {
		return AzureObjects{}, err
	}
	client := c.GetBlobService()
	return &AzureObjects{client}, nil
}

// Shutdown - Not relavant to Azure.
func (a AzureObjects) Shutdown() error {
	return nil
}

// StorageInfo - Not relavant to Azure.
func (a AzureObjects) StorageInfo() StorageInfo {
	return StorageInfo{}
}

// MakeBucket - Create bucket.
func (a AzureObjects) MakeBucket(bucket string) error {
	return traceError(a.client.CreateContainer(bucket, storage.ContainerAccessTypePrivate))
}

// GetBucketInfo - Get bucket metadata.
func (a AzureObjects) GetBucketInfo(bucket string) (BucketInfo, error) {
	// Azure does not have an equivalent call, hence use ListContainers.
	resp, err := a.client.ListContainers(storage.ListContainersParameters{
		Prefix: bucket,
	})
	if err != nil {
		return BucketInfo{}, traceError(err)
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
		return nil, traceError(err)
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
	return traceError(a.client.DeleteContainer(bucket))
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
		return result, traceError(err)
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
	byteRange := fmt.Sprintf("%d-%d", startOffset, startOffset+length-1)
	if length == -1 {
		byteRange = fmt.Sprintf("%d-", startOffset)
	}
	rc, err := a.client.GetBlobRange(bucket, object, byteRange, nil)
	if err != nil {
		if e, ok := err.(storage.AzureStorageServiceError); ok {
			if e.StatusCode == http.StatusNotFound {
				return traceError(ObjectNotFound{bucket, object})
			}
		}
		return traceError(err)
	}
	io.Copy(writer, rc)
	rc.Close()
	return nil
}

// GetObjectInfo - Use Azure equivalent GetBlobProperties.
func (a AzureObjects) GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	prop, err := a.client.GetBlobProperties(bucket, object)
	if err != nil {
		if e, ok := err.(storage.AzureStorageServiceError); ok {
			if e.StatusCode == http.StatusNotFound {
				return objInfo, traceError(ObjectNotFound{bucket, object})
			}
		}
		return objInfo, traceError(err)
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
		return objInfo, traceError(err)
	}
	return a.GetObjectInfo(bucket, object)
}

// CopyObject - Use Azure equivalent CopyBlob.
func (a AzureObjects) CopyObject(srcBucket, srcObject, destBucket, destObject string, metadata map[string]string) (objInfo ObjectInfo, err error) {
	err = a.client.CopyBlob(destBucket, destObject, a.client.GetBlobURL(srcBucket, srcObject))
	if err != nil {
		if e, ok := err.(storage.AzureStorageServiceError); ok {
			if e.StatusCode == http.StatusNotFound {
				return objInfo, traceError(ObjectNotFound{srcBucket, srcObject})
			}
		}
		return objInfo, traceError(err)
	}
	return a.GetObjectInfo(destBucket, destObject)
}

// DeleteObject - Use azure equivalent DeleteBlob.
func (a AzureObjects) DeleteObject(bucket, object string) error {
	err := a.client.DeleteBlob(bucket, object, nil)
	if err != nil {
		if e, ok := err.(storage.AzureStorageServiceError); ok {
			if e.StatusCode == http.StatusNotFound {
				return traceError(ObjectNotFound{bucket, object})
			}
		}
		return traceError(err)
	}

	return nil
}

// ListMultipartUploads - Incomplete implementation, for now just return the prefix if it is an incomplete upload.
func (a AzureObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	result.MaxUploads = maxUploads
	result.Prefix = prefix
	result.Delimiter = delimiter
	resp, err := a.ListObjectParts(bucket, prefix, prefix, 1, 1000)
	if err != nil {
		return result, nil
	}
	if len(resp.Parts) > 0 {
		result.Uploads = []uploadMetadata{{prefix, prefix, time.Now(), ""}}
	}
	return result, nil
}

// NewMultipartUpload - Use Azure equivalent CreateBlockBlob.
func (a AzureObjects) NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error) {
	uploadID = object
	return uploadID, traceError(a.client.CreateBlockBlob(bucket, object))
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
		return info, traceError(err)
	}
	info.PartNumber = partID
	info.ETag = md5Hex
	info.LastModified = time.Now()
	info.Size = size
	return info, nil
}

// ListObjectParts - Use Azure equivalent GetBlockList.
func (a AzureObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error) {
	resp, err := a.client.GetBlockList(bucket, object, storage.BlockListTypeUncommitted)
	if err != nil {
		return result, traceError(err)
	}
	for _, part := range resp.UncommittedBlocks {
		partID, md5Hex, err := azureParseBlockID(part.Name)
		if err != nil {
			return result, traceError(err)
		}
		result.Parts = append(result.Parts, PartInfo{
			partID,
			time.Now(),
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
		return objInfo, traceError(err)
	}
	objInfo, err = a.GetObjectInfo(bucket, object)
	if err != nil {
	}
	return objInfo, err
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
