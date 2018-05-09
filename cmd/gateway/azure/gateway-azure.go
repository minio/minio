/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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

package azure

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	humanize "github.com/dustin/go-humanize"
	"github.com/minio/cli"
	miniogopolicy "github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/policy/condition"
	sha256 "github.com/minio/sha256-simd"

	minio "github.com/minio/minio/cmd"
)

const (
	globalAzureAPIVersion      = "2016-05-31"
	azureBlockSize             = 100 * humanize.MiByte
	azureS3MinPartSize         = 5 * humanize.MiByte
	metadataObjectNameTemplate = minio.GatewayMinioSysTmp + "multipart/v1/%s.%x/azure.json"
	azureBackend               = "azure"
	azureMarkerPrefix          = "{minio}"
)

func init() {
	const azureGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [ENDPOINT]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENDPOINT:
  Azure server endpoint. Default ENDPOINT is https://core.windows.net

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of Azure storage.
     MINIO_SECRET_KEY: Password or secret key of Azure storage.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.

  UPDATE:
     MINIO_UPDATE: To turn off in-place upgrades, set this value to "off".

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to Minio host domain name.

EXAMPLES:
  1. Start minio gateway server for Azure Blob Storage backend.
      $ export MINIO_ACCESS_KEY=azureaccountname
      $ export MINIO_SECRET_KEY=azureaccountkey
      $ {{.HelpName}}

  2. Start minio gateway server for Azure Blob Storage backend on custom endpoint.
      $ export MINIO_ACCESS_KEY=azureaccountname
      $ export MINIO_SECRET_KEY=azureaccountkey
      $ {{.HelpName}} https://azure.example.com

  3. Start minio gateway server for Azure Blob Storage backend with edge caching enabled.
      $ export MINIO_ACCESS_KEY=azureaccountname
      $ export MINIO_SECRET_KEY=azureaccountkey
      $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
      $ export MINIO_CACHE_EXCLUDE="bucket1/*;*.png"
      $ export MINIO_CACHE_EXPIRY=40
      $ {{.HelpName}}

`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               azureBackend,
		Usage:              "Microsoft Azure Blob Storage.",
		Action:             azureGatewayMain,
		CustomHelpTemplate: azureGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Returns true if marker was returned by Azure, i.e prefixed with
// {minio}
func isAzureMarker(marker string) bool {
	return strings.HasPrefix(marker, azureMarkerPrefix)
}

// Handler for 'minio gateway azure' command line.
func azureGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	host := ctx.Args().First()
	// Validate gateway arguments.
	logger.FatalIf(minio.ValidateGatewayArguments(ctx.GlobalString("address"), host), "Invalid argument")

	minio.StartGateway(ctx, &Azure{host})
}

// Azure implements Gateway.
type Azure struct {
	host string
}

// Name implements Gateway interface.
func (g *Azure) Name() string {
	return azureBackend
}

// NewGatewayLayer initializes azure blob storage client and returns AzureObjects.
func (g *Azure) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	var err error
	var endpoint = storage.DefaultBaseURL
	var secure = true

	// If user provided some parameters
	if g.host != "" {
		endpoint, secure, err = minio.ParseGatewayEndpoint(g.host)
		if err != nil {
			return nil, err
		}
	}

	if endpoint == fmt.Sprintf("%s.blob.%s", creds.AccessKey, storage.DefaultBaseURL) {
		// If, by mistake, user provides endpoint as accountname.blob.core.windows.net
		endpoint = storage.DefaultBaseURL
	}
	c, err := storage.NewClient(creds.AccessKey, creds.SecretKey, endpoint, globalAzureAPIVersion, secure)
	if err != nil {
		return &azureObjects{}, err
	}
	c.HTTPClient = &http.Client{Transport: minio.NewCustomHTTPTransport()}

	return &azureObjects{
		client: c.GetBlobService(),
	}, nil
}

// Production - Azure gateway is production ready.
func (g *Azure) Production() bool {
	return true
}

// s3MetaToAzureProperties converts metadata meant for S3 PUT/COPY
// object into Azure data structures - BlobMetadata and
// BlobProperties.
//
// BlobMetadata contains user defined key-value pairs and each key is
// automatically prefixed with `X-Ms-Meta-` by the Azure SDK. S3
// user-metadata is translated to Azure metadata by removing the
// `X-Amz-Meta-` prefix.
//
// BlobProperties contains commonly set metadata for objects such as
// Content-Encoding, etc. Such metadata that is accepted by S3 is
// copied into BlobProperties.
//
// Header names are canonicalized as in http.Header.
func s3MetaToAzureProperties(ctx context.Context, s3Metadata map[string]string) (storage.BlobMetadata,
	storage.BlobProperties, error) {
	for k := range s3Metadata {
		if strings.Contains(k, "--") {
			logger.LogIf(ctx, minio.UnsupportedMetadata{})
			return storage.BlobMetadata{}, storage.BlobProperties{}, minio.UnsupportedMetadata{}
		}
	}

	// Encoding technique for each key is used here is as follows
	// Each '-' is converted to '_'
	// Each '_' is converted to '__'
	// With this basic assumption here are some of the expected
	// translations for these keys.
	// i: 'x-S3cmd_attrs' -> o: 'x_s3cmd__attrs' (mixed)
	// i: 'x__test__value' -> o: 'x____test____value' (double '_')
	encodeKey := func(key string) string {
		tokens := strings.Split(key, "_")
		for i := range tokens {
			tokens[i] = strings.Replace(tokens[i], "-", "_", -1)
		}
		return strings.Join(tokens, "__")
	}
	var blobMeta storage.BlobMetadata = make(map[string]string)
	var props storage.BlobProperties
	for k, v := range s3Metadata {
		k = http.CanonicalHeaderKey(k)
		switch {
		case strings.HasPrefix(k, "X-Amz-Meta-"):
			// Strip header prefix, to let Azure SDK
			// handle it for storage.
			k = strings.Replace(k, "X-Amz-Meta-", "", 1)
			blobMeta[encodeKey(k)] = v

		// All cases below, extract common metadata that is
		// accepted by S3 into BlobProperties for setting on
		// Azure - see
		// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
		case k == "Cache-Control":
			props.CacheControl = v
		case k == "Content-Disposition":
			props.ContentDisposition = v
		case k == "Content-Encoding":
			props.ContentEncoding = v
		case k == "Content-Length":
			// assume this doesn't fail
			props.ContentLength, _ = strconv.ParseInt(v, 10, 64)
		case k == "Content-Md5":
			props.ContentMD5 = v
		case k == "Content-Type":
			props.ContentType = v
		}
	}
	return blobMeta, props, nil
}

// azurePropertiesToS3Meta converts Azure metadata/properties to S3
// metadata. It is the reverse of s3MetaToAzureProperties. Azure's
// `.GetMetadata()` lower-cases all header keys, so this is taken into
// account by this function.
func azurePropertiesToS3Meta(meta storage.BlobMetadata, props storage.BlobProperties) map[string]string {
	// Decoding technique for each key is used here is as follows
	// Each '_' is converted to '-'
	// Each '__' is converted to '_'
	// With this basic assumption here are some of the expected
	// translations for these keys.
	// i: 'x_s3cmd__attrs' -> o: 'x-s3cmd_attrs' (mixed)
	// i: 'x____test____value' -> o: 'x__test__value' (double '_')
	decodeKey := func(key string) string {
		tokens := strings.Split(key, "__")
		for i := range tokens {
			tokens[i] = strings.Replace(tokens[i], "_", "-", -1)
		}
		return strings.Join(tokens, "_")
	}

	s3Metadata := make(map[string]string)
	for k, v := range meta {
		// k's `x-ms-meta-` prefix is already stripped by
		// Azure SDK, so we add the AMZ prefix.
		k = "X-Amz-Meta-" + decodeKey(k)
		k = http.CanonicalHeaderKey(k)
		s3Metadata[k] = v
	}

	// Add each property from BlobProperties that is supported by
	// S3 PUT/COPY common metadata.
	if props.CacheControl != "" {
		s3Metadata["Cache-Control"] = props.CacheControl
	}
	if props.ContentDisposition != "" {
		s3Metadata["Content-Disposition"] = props.ContentDisposition
	}
	if props.ContentEncoding != "" {
		s3Metadata["Content-Encoding"] = props.ContentEncoding
	}
	if props.ContentLength != 0 {
		s3Metadata["Content-Length"] = fmt.Sprintf("%d", props.ContentLength)
	}
	if props.ContentMD5 != "" {
		s3Metadata["Content-MD5"] = props.ContentMD5
	}
	if props.ContentType != "" {
		s3Metadata["Content-Type"] = props.ContentType
	}
	return s3Metadata
}

// azureObjects - Implements Object layer for Azure blob storage.
type azureObjects struct {
	minio.GatewayUnsupported
	client storage.BlobStorageClient // Azure sdk client
}

// Convert azure errors to minio object layer errors.
func azureToObjectError(err error, params ...string) error {
	if err == nil {
		return nil
	}

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
		return err
	}

	switch azureErr.Code {
	case "ContainerAlreadyExists":
		err = minio.BucketExists{Bucket: bucket}
	case "InvalidResourceName":
		err = minio.BucketNameInvalid{Bucket: bucket}
	case "RequestBodyTooLarge":
		err = minio.PartTooBig{}
	case "InvalidMetadata":
		err = minio.UnsupportedMetadata{}
	default:
		switch azureErr.StatusCode {
		case http.StatusNotFound:
			if object != "" {
				err = minio.ObjectNotFound{
					Bucket: bucket,
					Object: object,
				}
			} else {
				err = minio.BucketNotFound{Bucket: bucket}
			}
		case http.StatusBadRequest:
			err = minio.BucketNameInvalid{Bucket: bucket}
		}
	}
	return err
}

// getAzureUploadID - returns new upload ID which is hex encoded 8 bytes random value.
// this 8 byte restriction is needed because Azure block id has a restriction of length
// upto 8 bytes.
func getAzureUploadID() (string, error) {
	var id [8]byte

	n, err := io.ReadFull(rand.Reader, id[:])
	if err != nil {
		return "", err
	}
	if n != len(id) {
		return "", fmt.Errorf("Unexpected random data size. Expected: %d, read: %d)", len(id), n)
	}

	return hex.EncodeToString(id[:]), nil
}

// checkAzureUploadID - returns error in case of given string is upload ID.
func checkAzureUploadID(ctx context.Context, uploadID string) (err error) {
	if len(uploadID) != 16 {
		logger.LogIf(ctx, minio.MalformedUploadID{
			UploadID: uploadID,
		})
		return minio.MalformedUploadID{
			UploadID: uploadID,
		}
	}

	if _, err = hex.DecodeString(uploadID); err != nil {
		logger.LogIf(ctx, minio.MalformedUploadID{
			UploadID: uploadID,
		})
		return minio.MalformedUploadID{
			UploadID: uploadID,
		}
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

// Shutdown - save any gateway metadata to disk
// if necessary and reload upon next restart.
func (a *azureObjects) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo - Not relevant to Azure backend.
func (a *azureObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo) {
	return si
}

// MakeBucketWithLocation - Create a new container on azure backend.
func (a *azureObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	// Verify if bucket (container-name) is valid.
	// IsValidBucketName has same restrictions as container names mentioned
	// in azure documentation, so we will simply use the same function here.
	// Ref - https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
	if !minio.IsValidBucketName(bucket) {
		logger.LogIf(ctx, minio.BucketNameInvalid{Bucket: bucket})
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	container := a.client.GetContainerReference(bucket)
	err := container.Create(&storage.CreateContainerOptions{
		Access: storage.ContainerAccessTypePrivate,
	})
	logger.LogIf(ctx, err)
	return azureToObjectError(err, bucket)
}

// GetBucketInfo - Get bucket metadata..
func (a *azureObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, e error) {
	// Azure does not have an equivalent call, hence use
	// ListContainers with prefix
	resp, err := a.client.ListContainers(storage.ListContainersParameters{
		Prefix: bucket,
	})
	if err != nil {
		logger.LogIf(ctx, err)
		return bi, azureToObjectError(err, bucket)
	}
	for _, container := range resp.Containers {
		if container.Name == bucket {
			t, e := time.Parse(time.RFC1123, container.Properties.LastModified)
			if e == nil {
				return minio.BucketInfo{
					Name:    bucket,
					Created: t,
				}, nil
			} // else continue
		}
	}
	logger.LogIf(ctx, minio.BucketNotFound{Bucket: bucket})
	return bi, minio.BucketNotFound{Bucket: bucket}
}

// ListBuckets - Lists all azure containers, uses Azure equivalent ListContainers.
func (a *azureObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	resp, err := a.client.ListContainers(storage.ListContainersParameters{})
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, azureToObjectError(err)
	}
	for _, container := range resp.Containers {
		t, e := time.Parse(time.RFC1123, container.Properties.LastModified)
		if e != nil {
			logger.LogIf(ctx, e)
			return nil, e
		}
		buckets = append(buckets, minio.BucketInfo{
			Name:    container.Name,
			Created: t,
		})
	}
	return buckets, nil
}

// DeleteBucket - delete a container on azure, uses Azure equivalent DeleteContainer.
func (a *azureObjects) DeleteBucket(ctx context.Context, bucket string) error {
	container := a.client.GetContainerReference(bucket)
	err := container.Delete(nil)
	logger.LogIf(ctx, err)
	return azureToObjectError(err, bucket)
}

// ListObjects - lists all blobs on azure with in a container filtered by prefix
// and marker, uses Azure equivalent ListBlobs.
// To accommodate S3-compatible applications using
// ListObjectsV1 to use object keys as markers to control the
// listing of objects, we use the following encoding scheme to
// distinguish between Azure continuation tokens and application
// supplied markers.
//
// - NextMarker in ListObjectsV1 response is constructed by
//   prefixing "{minio}" to the Azure continuation token,
//   e.g, "{minio}CgRvYmoz"
//
// - Application supplied markers are used as-is to list
//   object keys that appear after it in the lexicographical order.
func (a *azureObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	var objects []minio.ObjectInfo
	var prefixes []string

	azureListMarker := ""
	if isAzureMarker(marker) {
		// If application is using Azure continuation token we should
		// strip the azureTokenPrefix we added in the previous list response.
		azureListMarker = strings.TrimPrefix(marker, azureMarkerPrefix)
	}

	container := a.client.GetContainerReference(bucket)
	for len(objects) == 0 && len(prefixes) == 0 {
		resp, err := container.ListBlobs(storage.ListBlobsParameters{
			Prefix:     prefix,
			Marker:     azureListMarker,
			Delimiter:  delimiter,
			MaxResults: uint(maxKeys),
		})
		if err != nil {
			logger.LogIf(ctx, err)
			return result, azureToObjectError(err, bucket, prefix)
		}

		for _, blob := range resp.Blobs {
			if delimiter == "" && strings.HasPrefix(blob.Name, minio.GatewayMinioSysTmp) {
				// We filter out minio.GatewayMinioSysTmp entries in the recursive listing.
				continue
			}
			if !isAzureMarker(marker) && blob.Name <= marker {
				// If the application used ListObjectsV1 style marker then we
				// skip all the entries till we reach the marker.
				continue
			}
			objects = append(objects, minio.ObjectInfo{
				Bucket:          bucket,
				Name:            blob.Name,
				ModTime:         time.Time(blob.Properties.LastModified),
				Size:            blob.Properties.ContentLength,
				ETag:            minio.ToS3ETag(blob.Properties.Etag),
				ContentType:     blob.Properties.ContentType,
				ContentEncoding: blob.Properties.ContentEncoding,
			})
		}

		for _, blobPrefix := range resp.BlobPrefixes {
			if blobPrefix == minio.GatewayMinioSysTmp {
				// We don't do strings.HasPrefix(blob.Name, minio.GatewayMinioSysTmp) here so that
				// we can use tools like mc to inspect the contents of minio.sys.tmp/
				// It is OK to allow listing of minio.sys.tmp/ in non-recursive mode as it aids in debugging.
				continue
			}
			if !isAzureMarker(marker) && blobPrefix <= marker {
				// If the application used ListObjectsV1 style marker then we
				// skip all the entries till we reach the marker.
				continue
			}
			prefixes = append(prefixes, blobPrefix)
		}

		azureListMarker = resp.NextMarker
		if azureListMarker == "" {
			// Reached end of listing.
			break
		}
	}

	result.Objects = objects
	result.Prefixes = prefixes
	if azureListMarker != "" {
		// We add the {minio} prefix so that we know in the subsequent request that this
		// marker is a azure continuation token and not ListObjectV1 marker.
		result.NextMarker = azureMarkerPrefix + azureListMarker
		result.IsTruncated = true
	}
	return result, nil
}

// ListObjectsV2 - list all blobs in Azure bucket filtered by prefix
func (a *azureObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	marker := continuationToken
	if startAfter != "" {
		marker = startAfter
	}

	var resultV1 minio.ListObjectsInfo
	resultV1, err = a.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return result, err
	}

	result.Objects = resultV1.Objects
	result.Prefixes = resultV1.Prefixes
	result.ContinuationToken = continuationToken
	result.NextContinuationToken = resultV1.NextMarker
	result.IsTruncated = (resultV1.NextMarker != "")
	return result, nil
}

// GetObject - reads an object from azure. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (a *azureObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) error {
	// startOffset cannot be negative.
	if startOffset < 0 {
		logger.LogIf(ctx, minio.InvalidRange{})
		return azureToObjectError(minio.InvalidRange{}, bucket, object)
	}

	blobRange := &storage.BlobRange{Start: uint64(startOffset)}
	if length > 0 {
		blobRange.End = uint64(startOffset + length - 1)
	}

	blob := a.client.GetContainerReference(bucket).GetBlobReference(object)
	var rc io.ReadCloser
	var err error
	if startOffset == 0 && length == 0 {
		rc, err = blob.Get(nil)
	} else {
		rc, err = blob.GetRange(&storage.GetBlobRangeOptions{
			Range: blobRange,
		})
	}
	if err != nil {
		logger.LogIf(ctx, err)
		return azureToObjectError(err, bucket, object)
	}
	_, err = io.Copy(writer, rc)
	rc.Close()
	logger.LogIf(ctx, err)
	return err
}

// GetObjectInfo - reads blob metadata properties and replies back minio.ObjectInfo,
// uses zure equivalent GetBlobProperties.
func (a *azureObjects) GetObjectInfo(ctx context.Context, bucket, object string) (objInfo minio.ObjectInfo, err error) {
	blob := a.client.GetContainerReference(bucket).GetBlobReference(object)
	err = blob.GetProperties(nil)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, azureToObjectError(err, bucket, object)
	}

	return minio.ObjectInfo{
		Bucket:          bucket,
		UserDefined:     azurePropertiesToS3Meta(blob.Metadata, blob.Properties),
		ETag:            minio.ToS3ETag(blob.Properties.Etag),
		ModTime:         time.Time(blob.Properties.LastModified),
		Name:            object,
		Size:            blob.Properties.ContentLength,
		ContentType:     blob.Properties.ContentType,
		ContentEncoding: blob.Properties.ContentEncoding,
	}, nil
}

// PutObject - Create a new blob with the incoming data,
// uses Azure equivalent CreateBlockBlobFromReader.
func (a *azureObjects) PutObject(ctx context.Context, bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo minio.ObjectInfo, err error) {
	blob := a.client.GetContainerReference(bucket).GetBlobReference(object)
	blob.Metadata, blob.Properties, err = s3MetaToAzureProperties(ctx, metadata)
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}
	err = blob.CreateBlockBlobFromReader(data, nil)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, azureToObjectError(err, bucket, object)
	}
	return a.GetObjectInfo(ctx, bucket, object)
}

// CopyObject - Copies a blob from source container to destination container.
// Uses Azure equivalent CopyBlob API.
func (a *azureObjects) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo) (objInfo minio.ObjectInfo, err error) {
	srcBlobURL := a.client.GetContainerReference(srcBucket).GetBlobReference(srcObject).GetURL()
	destBlob := a.client.GetContainerReference(destBucket).GetBlobReference(destObject)
	azureMeta, props, err := s3MetaToAzureProperties(ctx, srcInfo.UserDefined)
	if err != nil {
		return objInfo, azureToObjectError(err, srcBucket, srcObject)
	}
	destBlob.Metadata = azureMeta
	err = destBlob.Copy(srcBlobURL, nil)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, azureToObjectError(err, srcBucket, srcObject)
	}
	// Azure will copy metadata from the source object when an empty metadata map is provided.
	// To handle the case where the source object should be copied without its metadata,
	// the metadata must be removed from the dest. object after the copy completes
	if len(azureMeta) == 0 && len(destBlob.Metadata) != 0 {
		destBlob.Metadata = azureMeta
		err = destBlob.SetMetadata(nil)
		if err != nil {
			logger.LogIf(ctx, err)
			return objInfo, azureToObjectError(err, srcBucket, srcObject)
		}
	}
	destBlob.Properties = props
	err = destBlob.SetProperties(nil)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, azureToObjectError(err, srcBucket, srcObject)
	}
	return a.GetObjectInfo(ctx, destBucket, destObject)
}

// DeleteObject - Deletes a blob on azure container, uses Azure
// equivalent DeleteBlob API.
func (a *azureObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	blob := a.client.GetContainerReference(bucket).GetBlobReference(object)
	err := blob.Delete(nil)
	if err != nil {
		return azureToObjectError(err, bucket, object)
	}
	return nil
}

// ListMultipartUploads - It's decided not to support List Multipart Uploads, hence returning empty result.
func (a *azureObjects) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
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

func (a *azureObjects) checkUploadIDExists(ctx context.Context, bucketName, objectName, uploadID string) (err error) {
	blob := a.client.GetContainerReference(bucketName).GetBlobReference(
		getAzureMetadataObjectName(objectName, uploadID))
	err = blob.GetMetadata(nil)
	logger.LogIf(ctx, err)
	err = azureToObjectError(err, bucketName, objectName)
	oerr := minio.ObjectNotFound{
		Bucket: bucketName,
		Object: objectName,
	}
	if err == oerr {
		logger.LogIf(ctx, minio.InvalidUploadID{UploadID: uploadID})
		err = minio.InvalidUploadID{
			UploadID: uploadID,
		}
	}
	return err
}

// NewMultipartUpload - Use Azure equivalent CreateBlockBlob.
func (a *azureObjects) NewMultipartUpload(ctx context.Context, bucket, object string, metadata map[string]string) (uploadID string, err error) {
	uploadID, err = getAzureUploadID()
	if err != nil {
		logger.LogIf(ctx, err)
		return "", err
	}
	metadataObject := getAzureMetadataObjectName(object, uploadID)

	var jsonData []byte
	if jsonData, err = json.Marshal(azureMultipartMetadata{Name: object, Metadata: metadata}); err != nil {
		logger.LogIf(ctx, err)
		return "", err
	}

	blob := a.client.GetContainerReference(bucket).GetBlobReference(metadataObject)
	err = blob.CreateBlockBlobFromReader(bytes.NewBuffer(jsonData), nil)
	if err != nil {
		logger.LogIf(ctx, err)
		return "", azureToObjectError(err, bucket, metadataObject)
	}

	return uploadID, nil
}

// PutObjectPart - Use Azure equivalent PutBlockWithLength.
func (a *azureObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *hash.Reader) (info minio.PartInfo, err error) {
	if err = a.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return info, err
	}

	if err = checkAzureUploadID(ctx, uploadID); err != nil {
		return info, err
	}

	etag := data.MD5HexString()
	if etag == "" {
		etag = minio.GenETag()
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
		blob := a.client.GetContainerReference(bucket).GetBlobReference(object)
		err = blob.PutBlockWithLength(id, uint64(subPartSize), io.LimitReader(data, subPartSize), nil)
		if err != nil {
			logger.LogIf(ctx, err)
			return info, azureToObjectError(err, bucket, object)
		}
		subPartNumber++
	}

	info.PartNumber = partID
	info.ETag = etag
	info.LastModified = minio.UTCNow()
	info.Size = data.Size()
	return info, nil
}

// ListObjectParts - Use Azure equivalent GetBlockList.
func (a *azureObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int) (result minio.ListPartsInfo, err error) {
	if err = a.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts

	objBlob := a.client.GetContainerReference(bucket).GetBlobReference(object)
	resp, err := objBlob.GetBlockList(storage.BlockListTypeUncommitted, nil)
	azureErr, ok := err.(storage.AzureStorageServiceError)
	if ok && azureErr.StatusCode == http.StatusNotFound {
		// If no parts are uploaded yet then we return empty list.
		return result, nil
	}
	if err != nil {
		logger.LogIf(ctx, err)
		return result, azureToObjectError(err, bucket, object)
	}
	// Build a sorted list of parts and return the requested entries.
	partsMap := make(map[int]minio.PartInfo)
	for _, block := range resp.UncommittedBlocks {
		var partNumber int
		var parsedUploadID string
		var md5Hex string
		if partNumber, _, parsedUploadID, md5Hex, err = azureParseBlockID(block.Name); err != nil {
			logger.LogIf(ctx, fmt.Errorf("Unexpected error"))
			return result, azureToObjectError(fmt.Errorf("Unexpected error"), bucket, object)
		}
		if parsedUploadID != uploadID {
			continue
		}
		part, ok := partsMap[partNumber]
		if !ok {
			partsMap[partNumber] = minio.PartInfo{
				PartNumber: partNumber,
				Size:       block.Size,
				ETag:       md5Hex,
			}
			continue
		}
		if part.ETag != md5Hex {
			// If two parts of same partNumber were uploaded with different contents
			// return error as we won't be able to decide which the latest part is.
			logger.LogIf(ctx, fmt.Errorf("Unexpected error"))
			return result, azureToObjectError(fmt.Errorf("Unexpected error"), bucket, object)
		}
		part.Size += block.Size
		partsMap[partNumber] = part
	}
	var parts []minio.PartInfo
	for _, part := range partsMap {
		parts = append(parts, part)
	}
	sort.Slice(parts, func(i int, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})
	partsCount := 0
	i := 0
	if partNumberMarker != 0 {
		// If the marker was set, skip the entries till the marker.
		for _, part := range parts {
			i++
			if part.PartNumber == partNumberMarker {
				break
			}
		}
	}
	for partsCount < maxParts && i < len(parts) {
		result.Parts = append(result.Parts, parts[i])
		i++
		partsCount++
	}

	if i < len(parts) {
		result.IsTruncated = true
		if partsCount != 0 {
			result.NextPartNumberMarker = result.Parts[partsCount-1].PartNumber
		}
	}
	result.PartNumberMarker = partNumberMarker
	return result, nil
}

// AbortMultipartUpload - Not Implemented.
// There is no corresponding API in azure to abort an incomplete upload. The uncommmitted blocks
// gets deleted after one week.
func (a *azureObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	if err = a.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return err
	}

	blob := a.client.GetContainerReference(bucket).GetBlobReference(
		getAzureMetadataObjectName(object, uploadID))
	return blob.Delete(nil)
}

// CompleteMultipartUpload - Use Azure equivalent PutBlockList.
func (a *azureObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart) (objInfo minio.ObjectInfo, err error) {
	metadataObject := getAzureMetadataObjectName(object, uploadID)
	if err = a.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return objInfo, err
	}

	if err = checkAzureUploadID(ctx, uploadID); err != nil {
		return objInfo, err
	}

	var metadataReader io.Reader
	blob := a.client.GetContainerReference(bucket).GetBlobReference(metadataObject)
	if metadataReader, err = blob.Get(nil); err != nil {
		logger.LogIf(ctx, err)
		return objInfo, azureToObjectError(err, bucket, metadataObject)
	}

	var metadata azureMultipartMetadata
	if err = json.NewDecoder(metadataReader).Decode(&metadata); err != nil {
		logger.LogIf(ctx, err)
		return objInfo, azureToObjectError(err, bucket, metadataObject)
	}

	defer func() {
		if err != nil {
			return
		}

		blob := a.client.GetContainerReference(bucket).GetBlobReference(metadataObject)
		derr := blob.Delete(nil)
		logger.GetReqInfo(ctx).AppendTags("uploadID", uploadID)
		logger.LogIf(ctx, derr)
	}()

	objBlob := a.client.GetContainerReference(bucket).GetBlobReference(object)
	resp, err := objBlob.GetBlockList(storage.BlockListTypeUncommitted, nil)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, azureToObjectError(err, bucket, object)
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
			return nil, 0, minio.InvalidPart{}
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
			logger.LogIf(ctx, err)
			return objInfo, err
		}

		allBlocks = append(allBlocks, blocks...)
		partSizes[i] = size
	}

	// Error out if parts except last part sizing < 5MiB.
	for i, size := range partSizes[:len(partSizes)-1] {
		if size < azureS3MinPartSize {
			logger.LogIf(ctx, minio.PartTooSmall{
				PartNumber: uploadedParts[i].PartNumber,
				PartSize:   size,
				PartETag:   uploadedParts[i].ETag,
			})
			return objInfo, minio.PartTooSmall{
				PartNumber: uploadedParts[i].PartNumber,
				PartSize:   size,
				PartETag:   uploadedParts[i].ETag,
			}
		}
	}

	err = objBlob.PutBlockList(allBlocks, nil)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, azureToObjectError(err, bucket, object)
	}
	if len(metadata.Metadata) > 0 {
		objBlob.Metadata, objBlob.Properties, err = s3MetaToAzureProperties(ctx, metadata.Metadata)
		if err != nil {
			logger.LogIf(ctx, err)
			return objInfo, azureToObjectError(err, bucket, object)
		}
		err = objBlob.SetProperties(nil)
		if err != nil {
			logger.LogIf(ctx, err)
			return objInfo, azureToObjectError(err, bucket, object)
		}
		err = objBlob.SetMetadata(nil)
		if err != nil {
			logger.LogIf(ctx, err)
			return objInfo, azureToObjectError(err, bucket, object)
		}
	}
	return a.GetObjectInfo(ctx, bucket, object)
}

// SetBucketPolicy - Azure supports three types of container policies:
// storage.ContainerAccessTypeContainer - readonly in minio terminology
// storage.ContainerAccessTypeBlob - readonly without listing in minio terminology
// storage.ContainerAccessTypePrivate - none in minio terminology
// As the common denominator for minio and azure is readonly and none, we support
// these two policies at the bucket level.
func (a *azureObjects) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	policyInfo, err := minio.PolicyToBucketAccessPolicy(bucketPolicy)
	if err != nil {
		// This should not happen.
		logger.LogIf(ctx, err)
		return azureToObjectError(err, bucket)
	}

	var policies []minio.BucketAccessPolicy
	for prefix, policy := range miniogopolicy.GetPolicies(policyInfo.Statements, bucket, "") {
		policies = append(policies, minio.BucketAccessPolicy{
			Prefix: prefix,
			Policy: policy,
		})
	}
	prefix := bucket + "/*" // For all objects inside the bucket.
	if len(policies) != 1 {
		logger.LogIf(ctx, minio.NotImplemented{})
		return minio.NotImplemented{}
	}
	if policies[0].Prefix != prefix {
		logger.LogIf(ctx, minio.NotImplemented{})
		return minio.NotImplemented{}
	}
	if policies[0].Policy != miniogopolicy.BucketPolicyReadOnly {
		logger.LogIf(ctx, minio.NotImplemented{})
		return minio.NotImplemented{}
	}
	perm := storage.ContainerPermissions{
		AccessType:     storage.ContainerAccessTypeContainer,
		AccessPolicies: nil,
	}
	container := a.client.GetContainerReference(bucket)
	err = container.SetPermissions(perm, nil)
	logger.LogIf(ctx, err)
	return azureToObjectError(err, bucket)
}

// GetBucketPolicy - Get the container ACL and convert it to canonical []bucketAccessPolicy
func (a *azureObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	container := a.client.GetContainerReference(bucket)
	perm, err := container.GetPermissions(nil)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, azureToObjectError(err, bucket)
	}

	if perm.AccessType == storage.ContainerAccessTypePrivate {
		logger.LogIf(ctx, minio.BucketPolicyNotFound{Bucket: bucket})
		return nil, minio.BucketPolicyNotFound{Bucket: bucket}
	} else if perm.AccessType != storage.ContainerAccessTypeContainer {
		logger.LogIf(ctx, minio.NotImplemented{})
		return nil, azureToObjectError(minio.NotImplemented{})
	}

	return &policy.Policy{
		Version: policy.DefaultVersion,
		Statements: []policy.Statement{
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(
					policy.GetBucketLocationAction,
					policy.ListBucketAction,
					policy.GetObjectAction,
				),
				policy.NewResourceSet(
					policy.NewResource(bucket, ""),
					policy.NewResource(bucket, "*"),
				),
				condition.NewFunctions(),
			),
		},
	}, nil
}

// DeleteBucketPolicy - Set the container ACL to "private"
func (a *azureObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	perm := storage.ContainerPermissions{
		AccessType:     storage.ContainerAccessTypePrivate,
		AccessPolicies: nil,
	}
	container := a.client.GetContainerReference(bucket)
	err := container.SetPermissions(perm, nil)
	logger.LogIf(ctx, err)
	return azureToObjectError(err)
}
