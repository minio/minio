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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/sha256-simd"
)

const globalAzureAPIVersion = "2016-05-31"

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

// AzureObjects - Implements Object layer for Azure blob storage.
type AzureObjects struct {
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
func newAzureLayer(account, key string) (GatewayLayer, error) {
	useHTTPS := true
	c, err := storage.NewClient(account, key, storage.DefaultBaseURL, globalAzureAPIVersion, useHTTPS)
	if err != nil {
		return AzureObjects{}, err
	}
	return &AzureObjects{
		client: c.GetBlobService(),
		metaInfo: azureMultipartMetaInfo{
			meta:  make(map[string]map[string]string),
			Mutex: &sync.Mutex{},
		},
	}, nil
}

// Shutdown - save any gateway metadata to disk
// if necessary and reload upon next restart.
func (a AzureObjects) Shutdown() error {
	// TODO
	return nil
}

// StorageInfo - Not relevant to Azure backend.
func (a AzureObjects) StorageInfo() StorageInfo {
	return StorageInfo{}
}

// MakeBucket - Create a new container on azure backend.
func (a AzureObjects) MakeBucket(bucket string) error {
	err := a.client.CreateContainer(bucket, storage.ContainerAccessTypePrivate)
	return azureToObjectError(traceError(err), bucket)
}

// GetBucketInfo - Get bucket metadata..
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
			if e == nil {
				return BucketInfo{
					Name:    bucket,
					Created: t,
				}, nil
			} // else continue
		}
	}
	return BucketInfo{}, traceError(BucketNotFound{Bucket: bucket})
}

// ListBuckets - Lists all azure containers, uses Azure equivalent ListContainers.
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

// DeleteBucket - delete a container on azure, uses Azure equivalent DeleteContainer.
func (a AzureObjects) DeleteBucket(bucket string) error {
	return azureToObjectError(traceError(a.client.DeleteContainer(bucket)), bucket)
}

// ListObjects - lists all blobs on azure with in a container filtered by prefix
// and marker, uses Azure equivalent ListBlobs.
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
			MD5Sum:          canonicalizeETag(object.Properties.Etag),
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
func (a AzureObjects) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) error {
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
func (a AzureObjects) GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	prop, err := a.client.GetBlobProperties(bucket, object)
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}
	t, err := time.Parse(time.RFC1123, prop.LastModified)
	if err != nil {
		return objInfo, traceError(err)
	}
	objInfo = ObjectInfo{
		Bucket:      bucket,
		UserDefined: make(map[string]string),
		MD5Sum:      canonicalizeETag(prop.Etag),
		ModTime:     t,
		Name:        object,
		Size:        prop.ContentLength,
	}
	if prop.ContentEncoding != "" {
		objInfo.UserDefined["Content-Encoding"] = prop.ContentEncoding
	}
	objInfo.UserDefined["Content-Type"] = prop.ContentType
	return objInfo, nil
}

// Canonicalize the metadata headers, without this azure-sdk calculates
// incorrect signature. This attempt to canonicalize is to convert
// any HTTP header which is of form say `accept-encoding` should be
// converted to `Accept-Encoding` in its canonical form.
func canonicalMetadata(metadata map[string]string) (canonical map[string]string) {
	canonical = make(map[string]string)
	for k, v := range metadata {
		canonical[http.CanonicalHeaderKey(k)] = v
	}
	return canonical
}

// PutObject - Create a new blob with the incoming data,
// uses Azure equivalent CreateBlockBlobFromReader.
func (a AzureObjects) PutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, err error) {
	var sha256Writer hash.Hash
	teeReader := data
	if sha256sum != "" {
		sha256Writer = sha256.New()
		teeReader = io.TeeReader(data, sha256Writer)
	}

	delete(metadata, "md5Sum")

	err = a.client.CreateBlockBlobFromReader(bucket, object, uint64(size), teeReader, canonicalMetadata(metadata))
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}

	if sha256sum != "" {
		newSHA256sum := hex.EncodeToString(sha256Writer.Sum(nil))
		if newSHA256sum != sha256sum {
			a.client.DeleteBlob(bucket, object, nil)
			return ObjectInfo{}, traceError(SHA256Mismatch{})
		}
	}

	return a.GetObjectInfo(bucket, object)
}

// CopyObject - Copies a blob from source container to destination container.
// Uses Azure equivalent CopyBlob API.
func (a AzureObjects) CopyObject(srcBucket, srcObject, destBucket, destObject string, metadata map[string]string) (objInfo ObjectInfo, err error) {
	err = a.client.CopyBlob(destBucket, destObject, a.client.GetBlobURL(srcBucket, srcObject))
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), srcBucket, srcObject)
	}
	return a.GetObjectInfo(destBucket, destObject)
}

// DeleteObject - Deletes a blob on azure container, uses Azure
// equivalent DeleteBlob API.
func (a AzureObjects) DeleteObject(bucket, object string) error {
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
func (a AzureObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error) {
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
	result.Uploads = []uploadMetadata{{prefix, prefix, time.Now().UTC(), "", nil}}
	return result, nil
}

// NewMultipartUpload - Use Azure equivalent CreateBlockBlob.
func (a AzureObjects) NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error) {
	// Azure doesn't return a unique upload ID and we use object name in place of it. Azure allows multiple uploads to
	// co-exist as long as the user keeps the blocks uploaded (in block blobs) unique amongst concurrent upload attempts.
	// Each concurrent client, keeps its own blockID list which it can commit.
	uploadID = object
	if metadata == nil {
		// Store an empty map as a placeholder else ListObjectParts/PutObjectPart will not work properly.
		metadata = make(map[string]string)
	} else {
		metadata = canonicalMetadata(metadata)
	}
	a.metaInfo.set(uploadID, metadata)
	return uploadID, nil
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
	if meta := a.metaInfo.get(uploadID); meta == nil {
		return info, traceError(InvalidUploadID{})
	}
	var sha256Writer hash.Hash
	if sha256sum != "" {
		sha256Writer = sha256.New()
	}

	teeReader := io.TeeReader(data, sha256Writer)

	id := azureGetBlockID(partID, md5Hex)
	err = a.client.PutBlockWithLength(bucket, object, id, uint64(size), teeReader, nil)
	if err != nil {
		return info, azureToObjectError(traceError(err), bucket, object)
	}

	if sha256sum != "" {
		newSHA256sum := hex.EncodeToString(sha256Writer.Sum(nil))
		if newSHA256sum != sha256sum {
			return PartInfo{}, traceError(SHA256Mismatch{})
		}
	}

	info.PartNumber = partID
	info.ETag = md5Hex
	info.LastModified = time.Now().UTC()
	info.Size = size
	return info, nil
}

// ListObjectParts - Use Azure equivalent GetBlockList.
func (a AzureObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error) {
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
		partID, md5Hex, err := azureParseBlockID(part.Name)
		if err != nil {
			return result, err
		}
		if partID <= partNumberMarker {
			continue
		}
		result.Parts = append(result.Parts, PartInfo{
			partID,
			time.Now().UTC(),
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
func (a AzureObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	a.metaInfo.del(uploadID)
	return nil
}

// CompleteMultipartUpload - Use Azure equivalent PutBlockList.
func (a AzureObjects) CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []completePart) (objInfo ObjectInfo, err error) {
	meta := a.metaInfo.get(uploadID)
	if meta == nil {
		return objInfo, traceError(InvalidUploadID{uploadID})
	}
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
	}
	a.metaInfo.del(uploadID)
	return a.GetObjectInfo(bucket, object)
}

func anonErrToObjectErr(statusCode int, params ...string) error {
	bucket := ""
	object := ""
	if len(params) >= 1 {
		bucket = params[0]
	}
	if len(params) == 2 {
		object = params[1]
	}

	switch statusCode {
	case http.StatusNotFound:
		if object != "" {
			return ObjectNotFound{bucket, object}
		}
		return BucketNotFound{Bucket: bucket}
	case http.StatusBadRequest:
		if object != "" {
			return ObjectNameInvalid{bucket, object}
		}
		return BucketNameInvalid{Bucket: bucket}
	}
	return errUnexpected
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
func (a AzureObjects) SetBucketPolicies(bucket string, policies []BucketAccessPolicy) error {
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
func (a AzureObjects) GetBucketPolicies(bucket string) ([]BucketAccessPolicy, error) {
	perm, err := a.client.GetContainerPermissions(bucket, 0, "")
	if err != nil {
		return nil, azureToObjectError(traceError(err), bucket)
	}
	switch perm.AccessType {
	case storage.ContainerAccessTypePrivate:
		return nil, nil
	case storage.ContainerAccessTypeContainer:
		return []BucketAccessPolicy{{"", policy.BucketPolicyReadOnly}}, nil
	}
	return nil, azureToObjectError(traceError(NotImplemented{}))
}

// DeleteBucketPolicies - Set the container ACL to "private"
func (a AzureObjects) DeleteBucketPolicies(bucket string) error {
	perm := storage.ContainerPermissions{
		AccessType:     storage.ContainerAccessTypePrivate,
		AccessPolicies: nil,
	}
	err := a.client.SetContainerPermissions(bucket, perm, 0, "")
	return azureToObjectError(traceError(err))
}
