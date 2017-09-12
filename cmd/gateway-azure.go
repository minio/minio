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
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/sha256-simd"
)

const globalAzureAPIVersion = "2016-05-31"
const azureBlockSize = 100 * humanize.MiByte

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

// To store metadata during NewMultipartUpload which will be used after
// CompleteMultipartUpload to call SetBlobMetadata.
type azureMultipartMetaInfo struct {
	meta map[string]map[string]string
	*sync.Mutex
}

// Return metadata map of the multipart object.
func (a *azureMultipartMetaInfo) get(key string) map[string]string {
	a.Lock()
	defer a.Unlock()
	return a.meta[key]
}

// Set metadata map for the multipart object.
func (a *azureMultipartMetaInfo) set(key string, value map[string]string) {
	a.Lock()
	defer a.Unlock()
	a.meta[key] = value
}

// Delete metadata map for the multipart object.
func (a *azureMultipartMetaInfo) del(key string) {
	a.Lock()
	defer a.Unlock()
	delete(a.meta, key)
}

// azureObjects - Implements Object layer for Azure blob storage.
type azureObjects struct {
	client   storage.BlobStorageClient // Azure sdk client
	metaInfo azureMultipartMetaInfo
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
		metaInfo: azureMultipartMetaInfo{
			meta:  make(map[string]map[string]string),
			Mutex: &sync.Mutex{},
		},
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
func (a *azureObjects) PutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error) {
	var sha256Writer hash.Hash
	var md5sumWriter hash.Hash

	var writers []io.Writer

	md5sum := metadata["etag"]
	delete(metadata, "etag")

	teeReader := data

	if sha256sum != "" {
		sha256Writer = sha256.New()
		writers = append(writers, sha256Writer)
	}

	if md5sum != "" {
		md5sumWriter = md5.New()
		writers = append(writers, md5sumWriter)
	}

	if len(writers) > 0 {
		teeReader = io.TeeReader(data, io.MultiWriter(writers...))
	}

	err = a.client.CreateBlockBlobFromReader(bucket, object, uint64(size), teeReader, s3ToAzureHeaders(metadata))
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}

	if md5sum != "" {
		newMD5sum := hex.EncodeToString(md5sumWriter.Sum(nil))
		if newMD5sum != md5sum {
			a.client.DeleteBlob(bucket, object, nil)
			return ObjectInfo{}, azureToObjectError(traceError(BadDigest{md5sum, newMD5sum}))
		}
	}

	if sha256sum != "" {
		newSHA256sum := hex.EncodeToString(sha256Writer.Sum(nil))
		if newSHA256sum != sha256sum {
			a.client.DeleteBlob(bucket, object, nil)
			return ObjectInfo{}, azureToObjectError(traceError(SHA256Mismatch{}))
		}
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

// ListMultipartUploads - Incomplete implementation, for now just return the prefix if it is an incomplete upload.
// FIXME: Full ListMultipartUploads is not supported yet. It is supported just enough to help our client libs to
// support re-uploads. a.client.ListBlobs() can be made to return entries which include uncommitted blobs using
// which we need to filter out the committed blobs to get the list of uncommitted blobs.
func (a *azureObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
	result.MaxUploads = maxUploads
	result.Prefix = prefix
	result.Delimiter = delimiter
	meta := a.metaInfo.get(prefix)
	if meta == nil {
		// In case minio was restarted after NewMultipartUpload and before CompleteMultipartUpload we expect
		// the client to do a fresh upload so that any metadata like content-type are sent again in the
		// NewMultipartUpload.
		return result, nil
	}
	result.Uploads = []uploadMetadata{{prefix, prefix, UTCNow(), "", nil}}
	return result, nil
}

// NewMultipartUpload - Use Azure equivalent CreateBlockBlob.
func (a *azureObjects) NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error) {
	// Azure doesn't return a unique upload ID and we use object name in place of it. Azure allows multiple uploads to
	// co-exist as long as the user keeps the blocks uploaded (in block blobs) unique amongst concurrent upload attempts.
	// Each concurrent client, keeps its own blockID list which it can commit.
	uploadID = object
	if metadata == nil {
		// Store an empty map as a placeholder else ListObjectParts/PutObjectPart will not work properly.
		metadata = make(map[string]string)
	} else {
		metadata = s3ToAzureHeaders(metadata)
	}
	a.metaInfo.set(uploadID, metadata)
	return uploadID, nil
}

// CopyObjectPart - Not implemented.
func (a *azureObjects) CopyObjectPart(srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error) {
	return info, traceError(NotImplemented{})
}

// Encode partID, subPartNumber and md5Hex to blockID.
func azureGetBlockID(partID, subPartNumber int, md5Hex string) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%05d.%02d.%s", partID, subPartNumber, md5Hex)))
}

// Parse blockID into partID, subPartNumber and md5Hex.
func azureParseBlockID(blockID string) (partID, subPartNumber int, md5Hex string, err error) {
	var blockIDBytes []byte
	if blockIDBytes, err = base64.StdEncoding.DecodeString(blockID); err != nil {
		return
	}

	if _, err = fmt.Sscanf(string(blockIDBytes), "%05d.%02d.%s", &partID, &subPartNumber, &md5Hex); err != nil {
		err = fmt.Errorf("invalid block id '%s'", string(blockIDBytes))
	}

	return
}

// PutObjectPart - Use Azure equivalent PutBlockWithLength.
func (a *azureObjects) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string, sha256sum string) (info PartInfo, err error) {
	if meta := a.metaInfo.get(uploadID); meta == nil {
		return info, traceError(InvalidUploadID{})
	}
	var sha256Writer hash.Hash
	var md5sumWriter hash.Hash
	var etag string

	var writers []io.Writer

	if sha256sum != "" {
		sha256Writer = sha256.New()
		writers = append(writers, sha256Writer)
	}

	if md5Hex != "" {
		md5sumWriter = md5.New()
		writers = append(writers, md5sumWriter)
		etag = md5Hex
	} else {
		// Generate random ETag.
		etag = getMD5Hash([]byte(mustGetUUID()))
	}

	teeReader := data

	if len(writers) > 0 {
		teeReader = io.TeeReader(data, io.MultiWriter(writers...))
	}

	subPartSize := int64(azureBlockSize)
	subPartNumber := 1
	for remainingSize := size; remainingSize >= 0; remainingSize -= subPartSize {
		// Allow to create zero sized part.
		if remainingSize == 0 && subPartNumber > 1 {
			break
		}

		if remainingSize < subPartSize {
			subPartSize = remainingSize
		}

		id := azureGetBlockID(partID, subPartNumber, etag)
		err = a.client.PutBlockWithLength(bucket, object, id, uint64(subPartSize), io.LimitReader(teeReader, subPartSize), nil)
		if err != nil {
			return info, azureToObjectError(traceError(err), bucket, object)
		}

		subPartNumber++
	}

	if md5Hex != "" {
		newMD5sum := hex.EncodeToString(md5sumWriter.Sum(nil))
		if newMD5sum != md5Hex {
			a.client.DeleteBlob(bucket, object, nil)
			return PartInfo{}, azureToObjectError(traceError(BadDigest{md5Hex, newMD5sum}))
		}
	}

	if sha256sum != "" {
		newSHA256sum := hex.EncodeToString(sha256Writer.Sum(nil))
		if newSHA256sum != sha256sum {
			return PartInfo{}, azureToObjectError(traceError(SHA256Mismatch{}))
		}
	}

	info.PartNumber = partID
	info.ETag = etag
	info.LastModified = UTCNow()
	info.Size = size
	return info, nil
}

// ListObjectParts - Use Azure equivalent GetBlockList.
func (a *azureObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error) {
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts

	if meta := a.metaInfo.get(uploadID); meta == nil {
		return result, nil
	}
	resp, err := a.client.GetBlockList(bucket, object, storage.BlockListTypeUncommitted)
	if err != nil {
		return result, azureToObjectError(traceError(err), bucket, object)
	}
	tmpMaxParts := 0
	partCount := 0 // Used for figuring out IsTruncated.
	nextPartNumberMarker := 0
	for _, part := range resp.UncommittedBlocks {
		if tmpMaxParts == maxParts {
			// Also takes care of the case if maxParts = 0
			break
		}
		partCount++
		partID, _, md5Hex, err := azureParseBlockID(part.Name)
		if err != nil {
			return result, err
		}
		if partID <= partNumberMarker {
			continue
		}
		result.Parts = append(result.Parts, PartInfo{
			partID,
			UTCNow(),
			md5Hex,
			part.Size,
		})
		tmpMaxParts++
		nextPartNumberMarker = partID
	}
	if partCount < len(resp.UncommittedBlocks) {
		result.IsTruncated = true
		result.NextPartNumberMarker = nextPartNumberMarker
	}

	return result, nil
}

// AbortMultipartUpload - Not Implemented.
// There is no corresponding API in azure to abort an incomplete upload. The uncommmitted blocks
// gets deleted after one week.
func (a *azureObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	a.metaInfo.del(uploadID)
	return nil
}

// CompleteMultipartUpload - Use Azure equivalent PutBlockList.
func (a *azureObjects) CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []completePart) (objInfo ObjectInfo, err error) {
	meta := a.metaInfo.get(uploadID)
	if meta == nil {
		return objInfo, traceError(InvalidUploadID{uploadID})
	}

	resp, err := a.client.GetBlockList(bucket, object, storage.BlockListTypeUncommitted)
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}

	getBlocks := func(partNumber int, etag string) (blocks []storage.Block, size int64, err error) {
		for _, part := range resp.UncommittedBlocks {
			var partID int
			var md5Hex string
			if partID, _, md5Hex, err = azureParseBlockID(part.Name); err != nil {
				return nil, 0, err
			}

			if partNumber == partID && etag == md5Hex {
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
	a.metaInfo.del(uploadID)
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
