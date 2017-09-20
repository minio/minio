/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio-go/pkg/policy"
)

const globalAzureAPIVersion = "2016-05-31"
const azureBlockSize = 100 * humanize.MiByte
const metadataObjectNameTemplate = globalMinioSysTmp + "multipart/v1/%s.%x/azure.json"

// Canonicalize the metadata headers, without this azure-sdk calculates
// incorrect signature. This attempt to canonicalize is to convert
// any HTTP header which is of form say `accept-encoding` should be
// converted to `Accept-Encoding` in its canonical form.
// Also replaces X-Amz-Meta prefix with X-Ms-Meta as Azure expects user
// defined metadata to have X-Ms-Meta prefix.
func s3ToAzureHeaders(headers map[string]string) (newHeaders map[string]string) {
	gatewayHeaders := map[string]string{
		"X-Amz-Meta-X-Amz-Key":     "X-Amz-Meta-x_minio_key",
		"X-Amz-Meta-X-Amz-Matdesc": "X-Amz-Meta-x_minio_matdesc",
		"X-Amz-Meta-X-Amz-Iv":      "X-Amz-Meta-x_minio_iv",
	}

	newHeaders = make(map[string]string)
	for k, v := range headers {
		k = http.CanonicalHeaderKey(k)
		if nk, ok := gatewayHeaders[k]; ok {
			k = nk
		}
		if strings.HasPrefix(k, "X-Amz-Meta") {
			k = strings.Replace(k, "X-Amz-Meta", "X-Ms-Meta", -1)
		}
		newHeaders[k] = v
	}
	return newHeaders
}

// Prefix user metadata with "X-Amz-Meta-".
// client.GetBlobMetadata() already strips "X-Ms-Meta-"
func azureToS3Metadata(meta map[string]string) (newMeta map[string]string) {
	gatewayHeaders := map[string]string{
		"X-Amz-Meta-x_minio_key":     "X-Amz-Meta-X-Amz-Key",
		"X-Amz-Meta-x_minio_matdesc": "X-Amz-Meta-X-Amz-Matdesc",
		"X-Amz-Meta-x_minio_iv":      "X-Amz-Meta-X-Amz-Iv",
	}

	newMeta = make(map[string]string)

	for k, v := range meta {
		k = "X-Amz-Meta-" + k
		if nk, ok := gatewayHeaders[k]; ok {
			k = nk
		}
		newMeta[k] = v
	}
	return newMeta
}

// Append "-1" to etag so that clients do not interpret it as MD5.
func azureToS3ETag(etag string) string {
	return canonicalizeETag(etag) + "-1"
}

// azureObjects - Implements Object layer for Azure blob storage.
type azureObjects struct {
	client storage.BlobStorageClient // Azure sdk client
}

// Convert azure errors to minio object layer errors.
func azureToObjectError(err error, params ...string) error {
	if err == nil {
		return nil
	}

	e, ok := err.(*Error)
	if !ok {
		// Code should be fixed if this function is called without doing traceError()
		// Else handling different situations in this function makes this function complicated.
		errorIf(err, "Expected type *Error")
		return err
	}

	err = e.e
	bucket := ""
	object := ""
	if len(params) >= 1 {
		bucket = params[0]
	}
	if len(params) == 2 {
		object = params[1]
	}

	azureErr, ok := err.(storage.AzureStorageServiceError)
	if !ok {
		// We don't interpret non Azure errors. As azure errors will
		// have StatusCode to help to convert to object errors.
		return e
	}

	switch azureErr.Code {
	case "ContainerAlreadyExists":
		err = BucketExists{Bucket: bucket}
	case "InvalidResourceName":
		err = BucketNameInvalid{Bucket: bucket}
	case "RequestBodyTooLarge":
		err = PartTooBig{}
	case "InvalidMetadata":
		err = UnsupportedMetadata{}
	default:
		switch azureErr.StatusCode {
		case http.StatusNotFound:
			if object != "" {
				err = ObjectNotFound{bucket, object}
			} else {
				err = BucketNotFound{Bucket: bucket}
			}
		case http.StatusBadRequest:
			err = BucketNameInvalid{Bucket: bucket}
		}
	}
	e.e = err
	return e
}

// mustGetAzureUploadID - returns new upload ID which is hex encoded 8 bytes random value.
func mustGetAzureUploadID() string {
	var id [8]byte

	n, err := io.ReadFull(rand.Reader, id[:])
	if err != nil {
		panic(fmt.Errorf("unable to generate upload ID for azure. %s", err))
	}
	if n != len(id) {
		panic(fmt.Errorf("insufficient random data (expected: %d, read: %d)", len(id), n))
	}

	return fmt.Sprintf("%x", id[:])
}

// checkAzureUploadID - returns error in case of given string is upload ID.
func checkAzureUploadID(uploadID string) (err error) {
	if len(uploadID) != 16 {
		return traceError(MalformedUploadID{uploadID})
	}

	if _, err = hex.DecodeString(uploadID); err != nil {
		return traceError(MalformedUploadID{uploadID})
	}

	return nil
}

// Encode partID, subPartNumber, uploadID and md5Hex to blockID.
func azureGetBlockID(partID, subPartNumber int, uploadID, md5Hex string) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%05d.%02d.%s.%s", partID, subPartNumber, uploadID, md5Hex)))
}

// Parse blockID into partID, subPartNumber and md5Hex.
func azureParseBlockID(blockID string) (partID, subPartNumber int, uploadID, md5Hex string, err error) {
	var blockIDBytes []byte
	if blockIDBytes, err = base64.StdEncoding.DecodeString(blockID); err != nil {
		return
	}

	tokens := strings.Split(string(blockIDBytes), ".")
	if len(tokens) != 4 {
		err = fmt.Errorf("invalid block id '%s'", string(blockIDBytes))
		return
	}

	if partID, err = strconv.Atoi(tokens[0]); err != nil || partID <= 0 {
		err = fmt.Errorf("invalid part number in block id '%s'", string(blockIDBytes))
		return
	}

	if subPartNumber, err = strconv.Atoi(tokens[1]); err != nil || subPartNumber <= 0 {
		err = fmt.Errorf("invalid sub-part number in block id '%s'", string(blockIDBytes))
		return
	}

	uploadID = tokens[2]
	md5Hex = tokens[3]

	return
}

// Inits azure blob storage client and returns AzureObjects.
func newAzureLayer(host string) (GatewayLayer, error) {
	var err error
	var endpoint = storage.DefaultBaseURL
	var secure = true

	// If user provided some parameters
	if host != "" {
		endpoint, secure, err = parseGatewayEndpoint(host)
		if err != nil {
			return nil, err
		}
	}

	creds := serverConfig.GetCredential()
	c, err := storage.NewClient(creds.AccessKey, creds.SecretKey, endpoint, globalAzureAPIVersion, secure)
	if err != nil {
		return &azureObjects{}, err
	}
	c.HTTPClient = &http.Client{Transport: newCustomHTTPTransport()}

	return &azureObjects{
		client: c.GetBlobService(),
	}, nil
}

// Shutdown - save any gateway metadata to disk
// if necessary and reload upon next restart.
func (a *azureObjects) Shutdown() error {
	return nil
}

// StorageInfo - Not relevant to Azure backend.
func (a *azureObjects) StorageInfo() (si StorageInfo) {
	return si
}

// MakeBucketWithLocation - Create a new container on azure backend.
func (a *azureObjects) MakeBucketWithLocation(bucket, location string) error {
	err := a.client.CreateContainer(bucket, storage.ContainerAccessTypePrivate)
	return azureToObjectError(traceError(err), bucket)
}

// GetBucketInfo - Get bucket metadata..
func (a *azureObjects) GetBucketInfo(bucket string) (bi BucketInfo, e error) {
	// Azure does not have an equivalent call, hence use ListContainers.
	resp, err := a.client.ListContainers(storage.ListContainersParameters{
		Prefix: bucket,
	})
	if err != nil {
		return bi, azureToObjectError(traceError(err), bucket)
	}
	for _, container := range resp.Containers {
		if container.Name == bucket {
			t, e := time.Parse(time.RFC1123, container.Properties.LastModified)
			if e == nil {
				return BucketInfo{
					Name:    bucket,
					Created: t,
				}, nil
			} // else continue
		}
	}
	return bi, traceError(BucketNotFound{Bucket: bucket})
}

// ListBuckets - Lists all azure containers, uses Azure equivalent ListContainers.
func (a *azureObjects) ListBuckets() (buckets []BucketInfo, err error) {
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

// DeleteBucket - delete a container on azure, uses Azure equivalent DeleteContainer.
func (a *azureObjects) DeleteBucket(bucket string) error {
	return azureToObjectError(traceError(a.client.DeleteContainer(bucket)), bucket)
}

// ListObjects - lists all blobs on azure with in a container filtered by prefix
// and marker, uses Azure equivalent ListBlobs.
func (a *azureObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
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
		if strings.HasPrefix(object.Name, globalMinioSysTmp) {
			continue
		}
		t, e := time.Parse(time.RFC1123, object.Properties.LastModified)
		if e != nil {
			continue
		}
		result.Objects = append(result.Objects, ObjectInfo{
			Bucket:          bucket,
			Name:            object.Name,
			ModTime:         t,
			Size:            object.Properties.ContentLength,
			ETag:            azureToS3ETag(object.Properties.Etag),
			ContentType:     object.Properties.ContentType,
			ContentEncoding: object.Properties.ContentEncoding,
		})
	}

	// Remove minio.sys.tmp prefix.
	for i, prefix := range resp.BlobPrefixes {
		if prefix == globalMinioSysTmp {
			resp.BlobPrefixes = append(resp.BlobPrefixes[:i], resp.BlobPrefixes[i+1:]...)
			break
		}
	}
	result.Prefixes = resp.BlobPrefixes
	return result, nil
}

// ListObjectsV2 - list all blobs in Azure bucket filtered by prefix
func (a *azureObjects) ListObjectsV2(bucket, prefix, continuationToken string, fetchOwner bool, delimiter string, maxKeys int) (result ListObjectsV2Info, err error) {
	resp, err := a.client.ListBlobs(bucket, storage.ListBlobsParameters{
		Prefix:     prefix,
		Marker:     continuationToken,
		Delimiter:  delimiter,
		MaxResults: uint(maxKeys),
	})
	if err != nil {
		return result, azureToObjectError(traceError(err), bucket, prefix)
	}
	// If NextMarker is not empty, this means response is truncated and NextContinuationToken should be set
	if resp.NextMarker != "" {
		result.IsTruncated = true
		result.NextContinuationToken = resp.NextMarker
	}
	for _, object := range resp.Blobs {
		if strings.HasPrefix(object.Name, globalMinioSysTmp) {
			continue
		}
		t, e := time.Parse(time.RFC1123, object.Properties.LastModified)
		if e != nil {
			continue
		}
		result.Objects = append(result.Objects, ObjectInfo{
			Bucket:          bucket,
			Name:            object.Name,
			ModTime:         t,
			Size:            object.Properties.ContentLength,
			ETag:            azureToS3ETag(object.Properties.Etag),
			ContentType:     object.Properties.ContentType,
			ContentEncoding: object.Properties.ContentEncoding,
		})
	}

	// Remove minio.sys.tmp prefix.
	for i, prefix := range resp.BlobPrefixes {
		if prefix == globalMinioSysTmp {
			resp.BlobPrefixes = append(resp.BlobPrefixes[:i], resp.BlobPrefixes[i+1:]...)
			break
		}
	}
	result.Prefixes = resp.BlobPrefixes
	return result, nil
}

// GetObject - reads an object from azure. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (a *azureObjects) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) error {
	byteRange := fmt.Sprintf("%d-", startOffset)
	if length > 0 && startOffset > 0 {
		byteRange = fmt.Sprintf("%d-%d", startOffset, startOffset+length-1)
	}

	var rc io.ReadCloser
	var err error
	if startOffset == 0 && length == 0 {
		rc, err = a.client.GetBlob(bucket, object)
	} else {
		rc, err = a.client.GetBlobRange(bucket, object, byteRange, nil)
	}
	if err != nil {
		return azureToObjectError(traceError(err), bucket, object)
	}
	_, err = io.Copy(writer, rc)
	rc.Close()
	return traceError(err)
}

// GetObjectInfo - reads blob metadata properties and replies back ObjectInfo,
// uses zure equivalent GetBlobProperties.
func (a *azureObjects) GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	blobMeta, err := a.client.GetBlobMetadata(bucket, object)
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}

	meta := azureToS3Metadata(blobMeta)

	prop, err := a.client.GetBlobProperties(bucket, object)
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}
	t, err := time.Parse(time.RFC1123, prop.LastModified)
	if err != nil {
		return objInfo, traceError(err)
	}

	if prop.ContentEncoding != "" {
		meta["Content-Encoding"] = prop.ContentEncoding
	}
	meta["Content-Type"] = prop.ContentType

	objInfo = ObjectInfo{
		Bucket:      bucket,
		UserDefined: meta,
		ETag:        azureToS3ETag(prop.Etag),
		ModTime:     t,
		Name:        object,
		Size:        prop.ContentLength,
	}

	return objInfo, nil
}

// PutObject - Create a new blob with the incoming data,
// uses Azure equivalent CreateBlockBlobFromReader.
func (a *azureObjects) PutObject(bucket, object string, data *HashReader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	delete(metadata, "etag")
	err = a.client.CreateBlockBlobFromReader(bucket, object, uint64(data.Size()), data, s3ToAzureHeaders(metadata))
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}
	if err = data.Verify(); err != nil {
		a.client.DeleteBlob(bucket, object, nil)
		return ObjectInfo{}, azureToObjectError(traceError(err))
	}
	return a.GetObjectInfo(bucket, object)
}

// CopyObject - Copies a blob from source container to destination container.
// Uses Azure equivalent CopyBlob API.
func (a *azureObjects) CopyObject(srcBucket, srcObject, destBucket, destObject string, metadata map[string]string) (objInfo ObjectInfo, err error) {
	err = a.client.CopyBlob(destBucket, destObject, a.client.GetBlobURL(srcBucket, srcObject))
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), srcBucket, srcObject)
	}
	return a.GetObjectInfo(destBucket, destObject)
}

// DeleteObject - Deletes a blob on azure container, uses Azure
// equivalent DeleteBlob API.
func (a *azureObjects) DeleteObject(bucket, object string) error {
	err := a.client.DeleteBlob(bucket, object, nil)
	if err != nil {
		return azureToObjectError(traceError(err), bucket, object)
	}
	return nil
}

// ListMultipartUploads - It's decided not to support List Multipart Uploads, hence returning empty result.
func (a *azureObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	// It's decided not to support List Multipart Uploads, hence returning empty result.
	return result, nil
}

type azureMultipartMetadata struct {
	Name     string            `json:"name"`
	Metadata map[string]string `json:"metadata"`
}

func getAzureMetadataObjectName(objectName, uploadID string) string {
	return fmt.Sprintf(metadataObjectNameTemplate, uploadID, sha256.Sum256([]byte(objectName)))
}

func (a *azureObjects) checkUploadIDExists(bucketName, objectName, uploadID string) (err error) {
	_, err = a.client.GetBlobMetadata(bucketName, getAzureMetadataObjectName(objectName, uploadID))
	err = azureToObjectError(traceError(err), bucketName, objectName)
	oerr := ObjectNotFound{bucketName, objectName}
	if errorCause(err) == oerr {
		err = traceError(InvalidUploadID{})
	}
	return err
}

// NewMultipartUpload - Use Azure equivalent CreateBlockBlob.
func (a *azureObjects) NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error) {
	if metadata == nil {
		metadata = make(map[string]string)
	}

	uploadID = mustGetAzureUploadID()
	if err = a.checkUploadIDExists(bucket, object, uploadID); err == nil {
		return "", traceError(errors.New("Upload ID name collision"))
	}
	metadataObject := getAzureMetadataObjectName(object, uploadID)
	metadata = s3ToAzureHeaders(metadata)

	var jsonData []byte
	if jsonData, err = json.Marshal(azureMultipartMetadata{Name: object, Metadata: metadata}); err != nil {
		return "", traceError(err)
	}

	err = a.client.CreateBlockBlobFromReader(bucket, metadataObject, uint64(len(jsonData)), bytes.NewBuffer(jsonData), nil)
	if err != nil {
		return "", azureToObjectError(traceError(err), bucket, metadataObject)
	}

	return uploadID, nil
}

// CopyObjectPart - Not implemented.
func (a *azureObjects) CopyObjectPart(srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error) {
	return info, traceError(NotImplemented{})
}

// PutObjectPart - Use Azure equivalent PutBlockWithLength.
func (a *azureObjects) PutObjectPart(bucket, object, uploadID string, partID int, data *HashReader) (info PartInfo, err error) {
	if err = a.checkUploadIDExists(bucket, object, uploadID); err != nil {
		return info, err
	}

	if err = checkAzureUploadID(uploadID); err != nil {
		return info, err
	}

	etag := data.md5Sum
	if etag == "" {
		// Generate random ETag.
		etag = getMD5Hash([]byte(mustGetUUID()))
	}

	subPartSize, subPartNumber := int64(azureBlockSize), 1
	for remainingSize := data.Size(); remainingSize >= 0; remainingSize -= subPartSize {
		// Allow to create zero sized part.
		if remainingSize == 0 && subPartNumber > 1 {
			break
		}

		if remainingSize < subPartSize {
			subPartSize = remainingSize
		}

		id := azureGetBlockID(partID, subPartNumber, uploadID, etag)
		err = a.client.PutBlockWithLength(bucket, object, id, uint64(subPartSize), io.LimitReader(data, subPartSize), nil)
		if err != nil {
			return info, azureToObjectError(traceError(err), bucket, object)
		}
		subPartNumber++
	}
	if err = data.Verify(); err != nil {
		a.client.DeleteBlob(bucket, object, nil)
		return info, azureToObjectError(traceError(err), bucket, object)
	}

	info.PartNumber = partID
	info.ETag = etag
	info.LastModified = UTCNow()
	info.Size = data.Size()
	return info, nil
}

// ListObjectParts - Use Azure equivalent GetBlockList.
func (a *azureObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error) {
	if err = a.checkUploadIDExists(bucket, object, uploadID); err != nil {
		return result, err
	}

	// It's decided not to support List Object Parts, hence returning empty result.
	return result, nil
}

// AbortMultipartUpload - Not Implemented.
// There is no corresponding API in azure to abort an incomplete upload. The uncommmitted blocks
// gets deleted after one week.
func (a *azureObjects) AbortMultipartUpload(bucket, object, uploadID string) (err error) {
	if err = a.checkUploadIDExists(bucket, object, uploadID); err != nil {
		return err
	}

	return a.client.DeleteBlob(bucket, getAzureMetadataObjectName(object, uploadID), nil)
}

// CompleteMultipartUpload - Use Azure equivalent PutBlockList.
func (a *azureObjects) CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []completePart) (objInfo ObjectInfo, err error) {
	metadataObject := getAzureMetadataObjectName(object, uploadID)
	if err = a.checkUploadIDExists(bucket, object, uploadID); err != nil {
		return objInfo, err
	}

	if err = checkAzureUploadID(uploadID); err != nil {
		return objInfo, err
	}

	var metadataReader io.Reader
	if metadataReader, err = a.client.GetBlob(bucket, metadataObject); err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, metadataObject)
	}

	var metadata azureMultipartMetadata
	if err = json.NewDecoder(metadataReader).Decode(&metadata); err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, metadataObject)
	}

	meta := metadata.Metadata
	defer func() {
		if err != nil {
			return
		}

		derr := a.client.DeleteBlob(bucket, metadataObject, nil)
		errorIf(derr, "unable to remove meta data object for upload ID %s", uploadID)
	}()

	resp, err := a.client.GetBlockList(bucket, object, storage.BlockListTypeUncommitted)
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}

	getBlocks := func(partNumber int, etag string) (blocks []storage.Block, size int64, err error) {
		for _, part := range resp.UncommittedBlocks {
			var partID int
			var readUploadID string
			var md5Hex string
			if partID, _, readUploadID, md5Hex, err = azureParseBlockID(part.Name); err != nil {
				return nil, 0, err
			}

			if partNumber == partID && uploadID == readUploadID && etag == md5Hex {
				blocks = append(blocks, storage.Block{
					ID:     part.Name,
					Status: storage.BlockStatusUncommitted,
				})

				size += part.Size
			}
		}

		if len(blocks) == 0 {
			return nil, 0, InvalidPart{}
		}

		return blocks, size, nil
	}

	var allBlocks []storage.Block
	partSizes := make([]int64, len(uploadedParts))
	for i, part := range uploadedParts {
		var blocks []storage.Block
		var size int64
		blocks, size, err = getBlocks(part.PartNumber, part.ETag)
		if err != nil {
			return objInfo, traceError(err)
		}

		allBlocks = append(allBlocks, blocks...)
		partSizes[i] = size
	}

	// Error out if parts except last part sizing < 5MiB.
	for i, size := range partSizes[:len(partSizes)-1] {
		if size < globalMinPartSize {
			return objInfo, traceError(PartTooSmall{
				PartNumber: uploadedParts[i].PartNumber,
				PartSize:   size,
				PartETag:   uploadedParts[i].ETag,
			})
		}
	}

	err = a.client.PutBlockList(bucket, object, allBlocks)
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}
	if len(meta) > 0 {
		prop := storage.BlobHeaders{
			ContentMD5:      meta["Content-Md5"],
			ContentLanguage: meta["Content-Language"],
			ContentEncoding: meta["Content-Encoding"],
			ContentType:     meta["Content-Type"],
			CacheControl:    meta["Cache-Control"],
		}
		err = a.client.SetBlobProperties(bucket, object, prop)
		if err != nil {
			return objInfo, azureToObjectError(traceError(err), bucket, object)
		}
		err = a.client.SetBlobMetadata(bucket, object, nil, meta)
		if err != nil {
			return objInfo, azureToObjectError(traceError(err), bucket, object)
		}
	}
	return a.GetObjectInfo(bucket, object)
}

// Copied from github.com/Azure/azure-sdk-for-go/storage/blob.go
func azureListBlobsGetParameters(p storage.ListBlobsParameters) url.Values {
	out := url.Values{}

	if p.Prefix != "" {
		out.Set("prefix", p.Prefix)
	}
	if p.Delimiter != "" {
		out.Set("delimiter", p.Delimiter)
	}
	if p.Marker != "" {
		out.Set("marker", p.Marker)
	}
	if p.Include != "" {
		out.Set("include", p.Include)
	}
	if p.MaxResults != 0 {
		out.Set("maxresults", fmt.Sprintf("%v", p.MaxResults))
	}
	if p.Timeout != 0 {
		out.Set("timeout", fmt.Sprintf("%v", p.Timeout))
	}

	return out
}

// SetBucketPolicies - Azure supports three types of container policies:
// storage.ContainerAccessTypeContainer - readonly in minio terminology
// storage.ContainerAccessTypeBlob - readonly without listing in minio terminology
// storage.ContainerAccessTypePrivate - none in minio terminology
// As the common denominator for minio and azure is readonly and none, we support
// these two policies at the bucket level.
func (a *azureObjects) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	var policies []BucketAccessPolicy

	for prefix, policy := range policy.GetPolicies(policyInfo.Statements, bucket) {
		policies = append(policies, BucketAccessPolicy{
			Prefix: prefix,
			Policy: policy,
		})
	}
	prefix := bucket + "/*" // For all objects inside the bucket.
	if len(policies) != 1 {
		return traceError(NotImplemented{})
	}
	if policies[0].Prefix != prefix {
		return traceError(NotImplemented{})
	}
	if policies[0].Policy != policy.BucketPolicyReadOnly {
		return traceError(NotImplemented{})
	}
	perm := storage.ContainerPermissions{
		AccessType:     storage.ContainerAccessTypeContainer,
		AccessPolicies: nil,
	}
	err := a.client.SetContainerPermissions(bucket, perm, 0, "")
	return azureToObjectError(traceError(err), bucket)
}

// GetBucketPolicies - Get the container ACL and convert it to canonical []bucketAccessPolicy
func (a *azureObjects) GetBucketPolicies(bucket string) (policy.BucketAccessPolicy, error) {
	policyInfo := policy.BucketAccessPolicy{Version: "2012-10-17"}
	perm, err := a.client.GetContainerPermissions(bucket, 0, "")
	if err != nil {
		return policy.BucketAccessPolicy{}, azureToObjectError(traceError(err), bucket)
	}
	switch perm.AccessType {
	case storage.ContainerAccessTypePrivate:
		return policy.BucketAccessPolicy{}, traceError(PolicyNotFound{Bucket: bucket})
	case storage.ContainerAccessTypeContainer:
		policyInfo.Statements = policy.SetPolicy(policyInfo.Statements, policy.BucketPolicyReadOnly, bucket, "")
	default:
		return policy.BucketAccessPolicy{}, azureToObjectError(traceError(NotImplemented{}))
	}
	return policyInfo, nil
}

// DeleteBucketPolicies - Set the container ACL to "private"
func (a *azureObjects) DeleteBucketPolicies(bucket string) error {
	perm := storage.ContainerPermissions{
		AccessType:     storage.ContainerAccessTypePrivate,
		AccessPolicies: nil,
	}
	err := a.client.SetContainerPermissions(bucket, perm, 0, "")
	return azureToObjectError(traceError(err))
}
