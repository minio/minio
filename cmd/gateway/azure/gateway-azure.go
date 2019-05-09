/*
 * MinIO Cloud Storage, (C) 2017, 2018 MinIO, Inc.
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
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/go-autorest/autorest/azure"
	humanize "github.com/dustin/go-humanize"
	"github.com/minio/cli"
	miniogopolicy "github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
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
	metadataPartNamePrefix     = minio.GatewayMinioSysTmp + "multipart/v1/%s.%x"
	maxPartsCount              = 10000
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

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to MinIO host domain name.

  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.
     MINIO_CACHE_MAXUSE: Maximum permitted usage of the cache in percentage (0-100).

EXAMPLES:
  1. Start minio gateway server for Azure Blob Storage backend.
     $ export MINIO_ACCESS_KEY=azureaccountname
     $ export MINIO_SECRET_KEY=azureaccountkey
     $ {{.HelpName}}

  2. Start minio gateway server for Azure Blob Storage backend on custom endpoint.
     $ export MINIO_ACCESS_KEY=azureaccountname
     $ export MINIO_SECRET_KEY=azureaccountkey
     $ {{.HelpName}} https://azureaccountname.blob.custom.azure.endpoint

  3. Start minio gateway server for Azure Blob Storage backend with edge caching enabled.
     $ export MINIO_ACCESS_KEY=azureaccountname
     $ export MINIO_SECRET_KEY=azureaccountkey
     $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
     $ export MINIO_CACHE_EXCLUDE="bucket1/*;*.png"
     $ export MINIO_CACHE_EXPIRY=40
     $ export MINIO_CACHE_MAXUSE=80
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

// All known cloud environments of Azure
var azureEnvs = []azure.Environment{
	azure.PublicCloud,
	azure.USGovernmentCloud,
	azure.ChinaCloud,
	azure.GermanCloud,
}

// NewGatewayLayer initializes azure blob storage client and returns AzureObjects.
func (g *Azure) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	var err error
	// The default endpoint is the public cloud
	var endpoint = azure.PublicCloud.StorageEndpointSuffix
	var secure = true

	// Load the endpoint url if supplied by the user.
	if g.host != "" {
		endpoint, secure, err = minio.ParseGatewayEndpoint(g.host)
		if err != nil {
			return nil, err
		}
		// Reformat the full account storage endpoint to the base format.
		//   e.g. testazure.blob.core.windows.net => core.windows.net
		endpoint = strings.ToLower(endpoint)
		for _, env := range azureEnvs {
			if strings.Contains(endpoint, env.StorageEndpointSuffix) {
				endpoint = env.StorageEndpointSuffix
				break
			}
		}
	}

	c, err := storage.NewClient(creds.AccessKey, creds.SecretKey, endpoint, globalAzureAPIVersion, secure)
	if err != nil {
		return &azureObjects{}, err
	}

	c.AddToUserAgent(fmt.Sprintf("APN/1.0 MinIO/1.0 MinIO/%s", minio.Version))
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
		case k == "Content-Language":
			props.ContentLanguage = v
		}
	}
	return blobMeta, props, nil
}

const (
	partMetaVersionV1 = "1"
)

// partMetadataV1 struct holds the part specific metadata for
// multipart operations.
type partMetadataV1 struct {
	Version  string   `json:"version"`
	Size     int64    `json:"Size"`
	BlockIDs []string `json:"blockIDs"`
	ETag     string   `json:"etag"`
}

// Returns the initialized part metadata struct
func newPartMetaV1(uploadID string, partID int) (partMeta *partMetadataV1) {
	p := &partMetadataV1{}
	p.Version = partMetaVersionV1
	return p
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
	if props.ContentLanguage != "" {
		s3Metadata["Content-Language"] = props.ContentLanguage
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
		return minio.MalformedUploadID{
			UploadID: uploadID,
		}
	}

	if _, err = hex.DecodeString(uploadID); err != nil {
		return minio.MalformedUploadID{
			UploadID: uploadID,
		}
	}

	return nil
}

// parses partID from part metadata file name
func parseAzurePart(metaPartFileName, prefix string) (partID int, err error) {
	partStr := strings.TrimPrefix(metaPartFileName, prefix+"/")
	if partID, err = strconv.Atoi(partStr); err != nil || partID <= 0 {
		err = fmt.Errorf("invalid part number in block id '%s'", string(partID))
		return
	}
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
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	container := a.client.GetContainerReference(bucket)
	err := container.Create(&storage.CreateContainerOptions{
		Access: storage.ContainerAccessTypePrivate,
	})
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
	return bi, minio.BucketNotFound{Bucket: bucket}
}

// ListBuckets - Lists all azure containers, uses Azure equivalent ListContainers.
func (a *azureObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	resp, err := a.client.ListContainers(storage.ListContainersParameters{})
	if err != nil {
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
			// Populate correct ETag's if possible, this code primarily exists
			// because AWS S3 indicates that
			//
			// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
			//
			// Objects created by the PUT Object, POST Object, or Copy operation,
			// or through the AWS Management Console, and are encrypted by SSE-S3
			// or plaintext, have ETags that are an MD5 digest of their object data.
			//
			// Some applications depend on this behavior refer https://github.com/minio/minio/issues/6550
			// So we handle it here and make this consistent.
			etag := minio.ToS3ETag(blob.Properties.Etag)
			switch {
			case blob.Properties.ContentMD5 != "":
				b, err := base64.StdEncoding.DecodeString(blob.Properties.ContentMD5)
				if err == nil {
					etag = hex.EncodeToString(b)
				}
			case blob.Metadata["md5sum"] != "":
				etag = blob.Metadata["md5sum"]
				delete(blob.Metadata, "md5sum")
			}

			objects = append(objects, minio.ObjectInfo{
				Bucket:          bucket,
				Name:            blob.Name,
				ModTime:         time.Time(blob.Properties.LastModified),
				Size:            blob.Properties.ContentLength,
				ETag:            etag,
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
	if marker == "" {
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

// GetObjectNInfo - returns object info and locked object ReadCloser
func (a *azureObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	var objInfo minio.ObjectInfo
	objInfo, err = a.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	var startOffset, length int64
	startOffset, length, err = rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		err := a.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(err)
	}()
	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts.CheckCopyPrecondFn, pipeCloser)
}

// GetObject - reads an object from azure. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (a *azureObjects) GetObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	// startOffset cannot be negative.
	if startOffset < 0 {
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
		return azureToObjectError(err, bucket, object)
	}
	_, err = io.Copy(writer, rc)
	rc.Close()
	return err
}

// GetObjectInfo - reads blob metadata properties and replies back minio.ObjectInfo,
// uses zure equivalent GetBlobProperties.
func (a *azureObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	blob := a.client.GetContainerReference(bucket).GetBlobReference(object)
	err = blob.GetProperties(nil)
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}

	// Populate correct ETag's if possible, this code primarily exists
	// because AWS S3 indicates that
	//
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
	//
	// Objects created by the PUT Object, POST Object, or Copy operation,
	// or through the AWS Management Console, and are encrypted by SSE-S3
	// or plaintext, have ETags that are an MD5 digest of their object data.
	//
	// Some applications depend on this behavior refer https://github.com/minio/minio/issues/6550
	// So we handle it here and make this consistent.
	etag := minio.ToS3ETag(blob.Properties.Etag)
	switch {
	case blob.Properties.ContentMD5 != "":
		b, err := base64.StdEncoding.DecodeString(blob.Properties.ContentMD5)
		if err == nil {
			etag = hex.EncodeToString(b)
		}
	case blob.Metadata["md5sum"] != "":
		etag = blob.Metadata["md5sum"]
		delete(blob.Metadata, "md5sum")
	}

	return minio.ObjectInfo{
		Bucket:          bucket,
		UserDefined:     azurePropertiesToS3Meta(blob.Metadata, blob.Properties),
		ETag:            etag,
		ModTime:         time.Time(blob.Properties.LastModified),
		Name:            object,
		Size:            blob.Properties.ContentLength,
		ContentType:     blob.Properties.ContentType,
		ContentEncoding: blob.Properties.ContentEncoding,
	}, nil
}

// PutObject - Create a new blob with the incoming data,
// uses Azure equivalent CreateBlockBlobFromReader.
func (a *azureObjects) PutObject(ctx context.Context, bucket, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	data := r.Reader
	if data.Size() < azureBlockSize/10 {
		blob := a.client.GetContainerReference(bucket).GetBlobReference(object)
		blob.Metadata, blob.Properties, err = s3MetaToAzureProperties(ctx, opts.UserDefined)
		if err != nil {
			return objInfo, azureToObjectError(err, bucket, object)
		}
		if err = blob.CreateBlockBlobFromReader(data, nil); err != nil {
			return objInfo, azureToObjectError(err, bucket, object)
		}
		return a.GetObjectInfo(ctx, bucket, object, opts)
	}

	blockIDs := make(map[string]string)

	blob := a.client.GetContainerReference(bucket).GetBlobReference(object)
	subPartSize, subPartNumber := int64(azureBlockSize), 1
	for remainingSize := data.Size(); remainingSize >= 0; remainingSize -= subPartSize {
		// Allow to create zero sized part.
		if remainingSize == 0 && subPartNumber > 1 {
			break
		}

		if remainingSize < subPartSize {
			subPartSize = remainingSize
		}

		id := base64.StdEncoding.EncodeToString([]byte(minio.MustGetUUID()))
		blockIDs[id] = ""
		if err = blob.PutBlockWithLength(id, uint64(subPartSize), io.LimitReader(data, subPartSize), nil); err != nil {
			return objInfo, azureToObjectError(err, bucket, object)
		}
		subPartNumber++
	}

	objBlob := a.client.GetContainerReference(bucket).GetBlobReference(object)
	resp, err := objBlob.GetBlockList(storage.BlockListTypeUncommitted, nil)
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}
	getBlocks := func(blocksMap map[string]string) (blocks []storage.Block, size int64, aerr error) {
		for _, part := range resp.UncommittedBlocks {
			if _, ok := blocksMap[part.Name]; ok {
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

	var blocks []storage.Block
	blocks, _, err = getBlocks(blockIDs)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, err
	}

	if err = objBlob.PutBlockList(blocks, nil); err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}

	if len(opts.UserDefined) == 0 {
		opts.UserDefined = map[string]string{}
	}

	// Save md5sum for future processing on the object.
	opts.UserDefined["x-amz-meta-md5sum"] = r.MD5CurrentHexString()
	objBlob.Metadata, objBlob.Properties, err = s3MetaToAzureProperties(ctx, opts.UserDefined)
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}
	if err = objBlob.SetProperties(nil); err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}
	if err = objBlob.SetMetadata(nil); err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}

	return a.GetObjectInfo(ctx, bucket, object, opts)
}

// CopyObject - Copies a blob from source container to destination container.
// Uses Azure equivalent CopyBlob API.
func (a *azureObjects) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if srcOpts.CheckCopyPrecondFn != nil && srcOpts.CheckCopyPrecondFn(srcInfo, "") {
		return minio.ObjectInfo{}, minio.PreConditionFailed{}
	}
	srcBlobURL := a.client.GetContainerReference(srcBucket).GetBlobReference(srcObject).GetURL()
	destBlob := a.client.GetContainerReference(destBucket).GetBlobReference(destObject)
	azureMeta, props, err := s3MetaToAzureProperties(ctx, srcInfo.UserDefined)
	if err != nil {
		return objInfo, azureToObjectError(err, srcBucket, srcObject)
	}
	destBlob.Metadata = azureMeta
	err = destBlob.Copy(srcBlobURL, nil)
	if err != nil {
		return objInfo, azureToObjectError(err, srcBucket, srcObject)
	}
	// Azure will copy metadata from the source object when an empty metadata map is provided.
	// To handle the case where the source object should be copied without its metadata,
	// the metadata must be removed from the dest. object after the copy completes
	if len(azureMeta) == 0 && len(destBlob.Metadata) != 0 {
		destBlob.Metadata = azureMeta
		err = destBlob.SetMetadata(nil)
		if err != nil {
			return objInfo, azureToObjectError(err, srcBucket, srcObject)
		}
	}
	destBlob.Properties = props
	err = destBlob.SetProperties(nil)
	if err != nil {
		return objInfo, azureToObjectError(err, srcBucket, srcObject)
	}
	return a.GetObjectInfo(ctx, destBucket, destObject, dstOpts)
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

// gets the name of part metadata file for multipart upload operations
func getAzureMetadataPartName(objectName, uploadID string, partID int) string {
	partMetaPrefix := getAzureMetadataPartPrefix(uploadID, objectName)
	return path.Join(partMetaPrefix, fmt.Sprintf("%d", partID))
}

// gets the prefix of part metadata file
func getAzureMetadataPartPrefix(uploadID, objectName string) string {
	return fmt.Sprintf(metadataPartNamePrefix, uploadID, sha256.Sum256([]byte(objectName)))
}

func (a *azureObjects) checkUploadIDExists(ctx context.Context, bucketName, objectName, uploadID string) (err error) {
	blob := a.client.GetContainerReference(bucketName).GetBlobReference(
		getAzureMetadataObjectName(objectName, uploadID))
	err = blob.GetMetadata(nil)
	err = azureToObjectError(err, bucketName, objectName)
	oerr := minio.ObjectNotFound{
		Bucket: bucketName,
		Object: objectName,
	}
	if err == oerr {
		err = minio.InvalidUploadID{
			UploadID: uploadID,
		}
	}
	return err
}

// NewMultipartUpload - Use Azure equivalent CreateBlockBlob.
func (a *azureObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	uploadID, err = getAzureUploadID()
	if err != nil {
		logger.LogIf(ctx, err)
		return "", err
	}
	metadataObject := getAzureMetadataObjectName(object, uploadID)

	var jsonData []byte
	if jsonData, err = json.Marshal(azureMultipartMetadata{Name: object, Metadata: opts.UserDefined}); err != nil {
		logger.LogIf(ctx, err)
		return "", err
	}

	blob := a.client.GetContainerReference(bucket).GetBlobReference(metadataObject)
	err = blob.CreateBlockBlobFromReader(bytes.NewBuffer(jsonData), nil)
	if err != nil {
		return "", azureToObjectError(err, bucket, metadataObject)
	}

	return uploadID, nil
}

// PutObjectPart - Use Azure equivalent PutBlockWithLength.
func (a *azureObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	data := r.Reader
	if err = a.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return info, err
	}

	if err = checkAzureUploadID(ctx, uploadID); err != nil {
		return info, err
	}

	partMetaV1 := newPartMetaV1(uploadID, partID)
	subPartSize, subPartNumber := int64(azureBlockSize), 1
	for remainingSize := data.Size(); remainingSize >= 0; remainingSize -= subPartSize {
		// Allow to create zero sized part.
		if remainingSize == 0 && subPartNumber > 1 {
			break
		}

		if remainingSize < subPartSize {
			subPartSize = remainingSize
		}

		id := base64.StdEncoding.EncodeToString([]byte(minio.MustGetUUID()))
		partMetaV1.BlockIDs = append(partMetaV1.BlockIDs, id)

		blob := a.client.GetContainerReference(bucket).GetBlobReference(object)
		err = blob.PutBlockWithLength(id, uint64(subPartSize), io.LimitReader(data, subPartSize), nil)
		if err != nil {
			return info, azureToObjectError(err, bucket, object)
		}
		subPartNumber++
	}

	partMetaV1.ETag = r.MD5CurrentHexString()
	partMetaV1.Size = data.Size()

	// maintain per part md5sum in a temporary part metadata file until upload
	// is finalized.
	metadataObject := getAzureMetadataPartName(object, uploadID, partID)
	var jsonData []byte
	if jsonData, err = json.Marshal(partMetaV1); err != nil {
		logger.LogIf(ctx, err)
		return info, err
	}

	blob := a.client.GetContainerReference(bucket).GetBlobReference(metadataObject)
	err = blob.CreateBlockBlobFromReader(bytes.NewBuffer(jsonData), nil)
	if err != nil {
		return info, azureToObjectError(err, bucket, metadataObject)
	}

	info.PartNumber = partID
	info.ETag = partMetaV1.ETag
	info.LastModified = minio.UTCNow()
	info.Size = data.Size()
	return info, nil
}

// ListObjectParts - Use Azure equivalent GetBlockList.
func (a *azureObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	if err = a.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts

	var parts []minio.PartInfo
	var marker, delimiter string
	maxKeys := maxPartsCount
	if partNumberMarker == 0 {
		maxKeys = maxParts
	}
	prefix := getAzureMetadataPartPrefix(uploadID, object)
	container := a.client.GetContainerReference(bucket)
	resp, err := container.ListBlobs(storage.ListBlobsParameters{
		Prefix:     prefix,
		Marker:     marker,
		Delimiter:  delimiter,
		MaxResults: uint(maxKeys),
	})
	if err != nil {
		return result, azureToObjectError(err, bucket, prefix)
	}

	for _, blob := range resp.Blobs {
		if delimiter == "" && !strings.HasPrefix(blob.Name, minio.GatewayMinioSysTmp) {
			// We filter out non minio.GatewayMinioSysTmp entries in the recursive listing.
			continue
		}
		// filter temporary metadata file for blob
		if strings.HasSuffix(blob.Name, "azure.json") {
			continue
		}
		if !isAzureMarker(marker) && blob.Name <= marker {
			// If the application used ListObjectsV1 style marker then we
			// skip all the entries till we reach the marker.
			continue
		}
		partNumber, err := parseAzurePart(blob.Name, prefix)
		if err != nil {
			return result, azureToObjectError(fmt.Errorf("Unexpected error"), bucket, object)
		}
		var metadata partMetadataV1
		var metadataReader io.Reader
		blob := a.client.GetContainerReference(bucket).GetBlobReference(blob.Name)
		if metadataReader, err = blob.Get(nil); err != nil {
			return result, azureToObjectError(fmt.Errorf("Unexpected error"), bucket, object)
		}
		if err = json.NewDecoder(metadataReader).Decode(&metadata); err != nil {
			logger.LogIf(ctx, err)
			return result, azureToObjectError(err, bucket, object)
		}
		parts = append(parts, minio.PartInfo{
			PartNumber: partNumber,
			Size:       metadata.Size,
			ETag:       metadata.ETag,
		})
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
	var partNumberMarker int
	for {
		lpi, err := a.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxPartsCount, minio.ObjectOptions{})
		if err != nil {
			break
		}
		for _, part := range lpi.Parts {
			pblob := a.client.GetContainerReference(bucket).GetBlobReference(
				getAzureMetadataPartName(object, uploadID, part.PartNumber))
			pblob.Delete(nil)
		}
		partNumberMarker = lpi.NextPartNumberMarker
		if !lpi.IsTruncated {
			break
		}
	}

	blob := a.client.GetContainerReference(bucket).GetBlobReference(
		getAzureMetadataObjectName(object, uploadID))
	return blob.Delete(nil)
}

// CompleteMultipartUpload - Use Azure equivalent PutBlockList.
func (a *azureObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
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
		return objInfo, azureToObjectError(err, bucket, metadataObject)
	}

	var metadata azureMultipartMetadata
	if err = json.NewDecoder(metadataReader).Decode(&metadata); err != nil {
		logger.LogIf(ctx, err)
		return objInfo, azureToObjectError(err, bucket, metadataObject)
	}

	objBlob := a.client.GetContainerReference(bucket).GetBlobReference(object)

	var allBlocks []storage.Block
	for i, part := range uploadedParts {
		var partMetadataReader io.Reader
		var partMetadata partMetadataV1
		partMetadataObject := getAzureMetadataPartName(object, uploadID, part.PartNumber)
		pblob := a.client.GetContainerReference(bucket).GetBlobReference(partMetadataObject)
		if partMetadataReader, err = pblob.Get(nil); err != nil {
			return objInfo, azureToObjectError(err, bucket, partMetadataObject)
		}

		if err = json.NewDecoder(partMetadataReader).Decode(&partMetadata); err != nil {
			logger.LogIf(ctx, err)
			return objInfo, azureToObjectError(err, bucket, partMetadataObject)
		}

		if partMetadata.ETag != part.ETag {
			return objInfo, minio.InvalidPart{}
		}
		for _, blockID := range partMetadata.BlockIDs {
			allBlocks = append(allBlocks, storage.Block{ID: blockID, Status: storage.BlockStatusUncommitted})
		}
		if i < (len(uploadedParts)-1) && partMetadata.Size < azureS3MinPartSize {
			return objInfo, minio.PartTooSmall{
				PartNumber: uploadedParts[i].PartNumber,
				PartSize:   partMetadata.Size,
				PartETag:   uploadedParts[i].ETag,
			}
		}
	}

	err = objBlob.PutBlockList(allBlocks, nil)
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}
	objBlob.Metadata, objBlob.Properties, err = s3MetaToAzureProperties(ctx, metadata.Metadata)
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}
	objBlob.Metadata["md5sum"] = cmd.ComputeCompleteMultipartMD5(uploadedParts)
	err = objBlob.SetProperties(nil)
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}
	err = objBlob.SetMetadata(nil)
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}
	var partNumberMarker int
	for {
		lpi, err := a.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxPartsCount, minio.ObjectOptions{})
		if err != nil {
			break
		}
		for _, part := range lpi.Parts {
			pblob := a.client.GetContainerReference(bucket).GetBlobReference(
				getAzureMetadataPartName(object, uploadID, part.PartNumber))
			pblob.Delete(nil)
		}
		partNumberMarker = lpi.NextPartNumberMarker
		if !lpi.IsTruncated {
			break
		}
	}

	blob = a.client.GetContainerReference(bucket).GetBlobReference(metadataObject)
	derr := blob.Delete(nil)
	logger.GetReqInfo(ctx).AppendTags("uploadID", uploadID)
	logger.LogIf(ctx, derr)

	return a.GetObjectInfo(ctx, bucket, object, minio.ObjectOptions{})
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
		return minio.NotImplemented{}
	}
	if policies[0].Prefix != prefix {
		return minio.NotImplemented{}
	}
	if policies[0].Policy != miniogopolicy.BucketPolicyReadOnly {
		return minio.NotImplemented{}
	}
	perm := storage.ContainerPermissions{
		AccessType:     storage.ContainerAccessTypeContainer,
		AccessPolicies: nil,
	}
	container := a.client.GetContainerReference(bucket)
	err = container.SetPermissions(perm, nil)
	return azureToObjectError(err, bucket)
}

// GetBucketPolicy - Get the container ACL and convert it to canonical []bucketAccessPolicy
func (a *azureObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	container := a.client.GetContainerReference(bucket)
	perm, err := container.GetPermissions(nil)
	if err != nil {
		return nil, azureToObjectError(err, bucket)
	}

	if perm.AccessType == storage.ContainerAccessTypePrivate {
		return nil, minio.BucketPolicyNotFound{Bucket: bucket}
	} else if perm.AccessType != storage.ContainerAccessTypeContainer {
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
	return azureToObjectError(err)
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (a *azureObjects) IsCompressionSupported() bool {
	return false
}
