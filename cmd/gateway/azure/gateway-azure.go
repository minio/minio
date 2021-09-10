/*
 * MinIO Object Storage (c) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	humanize "github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/madmin-go"
	miniogopolicy "github.com/minio/minio-go/v7/pkg/policy"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/bucket/policy"
	"github.com/minio/pkg/bucket/policy/condition"
	"github.com/minio/pkg/env"
)

const (
	azureDefaultUploadChunkSizeMB = 25
	azureDownloadRetryAttempts    = 5
	azureS3MinPartSize            = 5 * humanize.MiByte
	metadataObjectNameTemplate    = minio.GatewayMinioSysTmp + "multipart/v1/%s.%x/azure.json"
	azureMarkerPrefix             = "{minio}"
	metadataPartNamePrefix        = minio.GatewayMinioSysTmp + "multipart/v1/%s.%x"
	maxPartsCount                 = 10000
)

var (
	azureUploadChunkSize   int
	azureUploadConcurrency int
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

EXAMPLES:
  1. Start minio gateway server for Azure Blob Storage backend on custom endpoint.
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}azureaccountname
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}azureaccountkey
     {{.Prompt}} {{.HelpName}} https://azureaccountname.blob.custom.azure.endpoint

  2. Start minio gateway server for Azure Blob Storage backend with edge caching enabled.
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}azureaccountname
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}azureaccountkey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_DRIVES{{.AssignmentOperator}}"/mnt/drive1,/mnt/drive2,/mnt/drive3,/mnt/drive4"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_EXCLUDE{{.AssignmentOperator}}"bucket1/*,*.png"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_QUOTA{{.AssignmentOperator}}90
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_AFTER{{.AssignmentOperator}}3
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_LOW{{.AssignmentOperator}}75
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_HIGH{{.AssignmentOperator}}85
     {{.Prompt}} {{.HelpName}}

`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               minio.AzureBackendGateway,
		Usage:              "Microsoft Azure Blob Storage",
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

	serverAddr := ctx.GlobalString("address")
	if serverAddr == "" || serverAddr == ":"+minio.GlobalMinioDefaultPort {
		serverAddr = ctx.String("address")
	}
	// Validate gateway arguments.
	logger.FatalIf(minio.ValidateGatewayArguments(serverAddr, host), "Invalid argument")

	minio.StartGateway(ctx, &Azure{host})
}

// Azure implements Gateway.
type Azure struct {
	host string
}

// Name implements Gateway interface.
func (g *Azure) Name() string {
	return minio.AzureBackendGateway
}

// NewGatewayLayer initializes azure blob storage client and returns AzureObjects.
func (g *Azure) NewGatewayLayer(creds madmin.Credentials) (minio.ObjectLayer, error) {
	var err error

	// Override credentials from the Azure storage environment variables if specified
	if acc, key := env.Get("AZURE_STORAGE_ACCOUNT", creds.AccessKey), env.Get("AZURE_STORAGE_KEY", creds.SecretKey); acc != "" && key != "" {
		creds = madmin.Credentials{
			AccessKey: acc,
			SecretKey: key,
		}
	}

	endpointURL, err := parseStorageEndpoint(g.host, creds.AccessKey)
	if err != nil {
		return nil, err
	}

	azureUploadChunkSize, err = env.GetInt("MINIO_AZURE_CHUNK_SIZE_MB", azureDefaultUploadChunkSizeMB)
	if err != nil {
		return nil, err
	}
	azureUploadChunkSize *= humanize.MiByte
	if azureUploadChunkSize <= 0 || azureUploadChunkSize > 100*humanize.MiByte {
		return nil, fmt.Errorf("MINIO_AZURE_CHUNK_SIZE_MB should be an integer value between 0 and 100")
	}

	azureUploadConcurrency, err = env.GetInt("MINIO_AZURE_UPLOAD_CONCURRENCY", 4)
	if err != nil {
		return nil, err
	}

	credential, err := azblob.NewSharedKeyCredential(creds.AccessKey, creds.SecretKey)
	if err != nil {
		if _, ok := err.(base64.CorruptInputError); ok {
			return &azureObjects{}, errors.New("invalid Azure credentials")
		}
		return &azureObjects{}, err
	}

	metrics := minio.NewMetrics()

	t := &minio.MetricsTransport{
		Transport: minio.NewGatewayHTTPTransport(),
		Metrics:   metrics,
	}

	httpClient := &http.Client{Transport: t}
	userAgent := fmt.Sprintf("APN/1.0 MinIO/1.0 MinIO/%s", minio.Version)

	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			// Azure SDK recommends to set a timeout of 60 seconds per MB of data so we
			// calculate here the timeout for the configured upload chunck size.
			TryTimeout: time.Duration(azureUploadChunkSize/humanize.MiByte) * 60 * time.Second,
		},
		HTTPSender: pipeline.FactoryFunc(func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
			return func(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
				request.Header.Set("User-Agent", userAgent)
				resp, err := httpClient.Do(request.WithContext(ctx))
				return pipeline.NewHTTPResponse(resp), err
			}
		}),
	})

	client := azblob.NewServiceURL(*endpointURL, pipeline)

	return &azureObjects{
		endpoint:   endpointURL,
		httpClient: httpClient,
		client:     client,
		metrics:    metrics,
	}, nil
}

func parseStorageEndpoint(host string, accountName string) (*url.URL, error) {
	var endpoint string

	// Load the endpoint url if supplied by the user.
	if host != "" {
		host, secure, err := minio.ParseGatewayEndpoint(host)
		if err != nil {
			return nil, err
		}

		var protocol string
		if secure {
			protocol = "https"
		} else {
			protocol = "http"
		}

		// for containerized storage deployments like Azurite or IoT Edge Storage,
		// account resolution isn't handled via a hostname prefix like
		// `http://${account}.host/${path}` but instead via a route prefix like
		// `http://host/${account}/${path}` so adjusting for that here
		if !strings.HasPrefix(host, fmt.Sprintf("%s.", accountName)) {
			host = fmt.Sprintf("%s/%s", host, accountName)
		}

		endpoint = fmt.Sprintf("%s://%s", protocol, host)
	} else {
		endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	}

	return url.Parse(endpoint)
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
func s3MetaToAzureProperties(ctx context.Context, s3Metadata map[string]string) (azblob.Metadata, azblob.BlobHTTPHeaders, error) {
	for k := range s3Metadata {
		if strings.Contains(k, "--") {
			return azblob.Metadata{}, azblob.BlobHTTPHeaders{}, minio.UnsupportedMetadata{}
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
	var blobMeta azblob.Metadata = make(map[string]string)
	var err error
	var props azblob.BlobHTTPHeaders
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
		case k == "Content-Md5":
			props.ContentMD5, err = base64.StdEncoding.DecodeString(v)
		case k == "Content-Type":
			props.ContentType = v
		case k == "Content-Language":
			props.ContentLanguage = v
		}
	}
	return blobMeta, props, err
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

func s3StorageClassToAzureTier(sc string) azblob.AccessTierType {
	switch sc {
	case "REDUCED_REDUNDANCY":
		return azblob.AccessTierCool
	case "STANDARD":
		return azblob.AccessTierHot
	}
	return azblob.AccessTierHot
}

func azureTierToS3StorageClass(tierType string) string {
	switch azblob.AccessTierType(tierType) {
	case azblob.AccessTierCool:
		return "REDUCED_REDUNDANCY"
	case azblob.AccessTierHot:
		return "STANDARD"
	default:
		return "STANDARD"
	}

}

// azurePropertiesToS3Meta converts Azure metadata/properties to S3
// metadata. It is the reverse of s3MetaToAzureProperties. Azure's
// `.GetMetadata()` lower-cases all header keys, so this is taken into
// account by this function.
func azurePropertiesToS3Meta(meta azblob.Metadata, props azblob.BlobHTTPHeaders, contentLength int64) map[string]string {
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
	if contentLength != 0 {
		s3Metadata["Content-Length"] = fmt.Sprintf("%d", contentLength)
	}
	if len(props.ContentMD5) != 0 {
		s3Metadata["Content-MD5"] = base64.StdEncoding.EncodeToString(props.ContentMD5)
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
	endpoint   *url.URL
	httpClient *http.Client
	metrics    *minio.BackendMetrics
	client     azblob.ServiceURL // Azure sdk client
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

	azureErr, ok := err.(azblob.StorageError)
	if !ok {
		// We don't interpret non Azure errors. As azure errors will
		// have StatusCode to help to convert to object errors.
		return err
	}

	serviceCode := string(azureErr.ServiceCode())
	statusCode := azureErr.Response().StatusCode

	return azureCodesToObjectError(err, serviceCode, statusCode, bucket, object)
}

func azureCodesToObjectError(err error, serviceCode string, statusCode int, bucket string, object string) error {
	switch serviceCode {
	case "ContainerNotFound", "ContainerBeingDeleted":
		err = minio.BucketNotFound{Bucket: bucket}
	case "ContainerAlreadyExists":
		err = minio.BucketExists{Bucket: bucket}
	case "InvalidResourceName":
		err = minio.BucketNameInvalid{Bucket: bucket}
	case "RequestBodyTooLarge":
		err = minio.PartTooBig{}
	case "InvalidMetadata":
		err = minio.UnsupportedMetadata{}
	case "BlobAccessTierNotSupportedForAccountType":
		err = minio.NotImplemented{}
	case "OutOfRangeInput":
		err = minio.ObjectNameInvalid{
			Bucket: bucket,
			Object: object,
		}
	default:
		switch statusCode {
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
	partStr := strings.TrimPrefix(metaPartFileName, prefix+minio.SlashSeparator)
	if partID, err = strconv.Atoi(partStr); err != nil || partID <= 0 {
		err = fmt.Errorf("invalid part number in block id '%d'", partID)
		return
	}
	return
}

// GetMetrics returns this gateway's metrics
func (a *azureObjects) GetMetrics(ctx context.Context) (*minio.BackendMetrics, error) {
	return a.metrics, nil
}

// Shutdown - save any gateway metadata to disk
// if necessary and reload upon next restart.
func (a *azureObjects) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo - Not relevant to Azure backend.
func (a *azureObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo, _ []error) {
	si.Backend.Type = madmin.Gateway
	host := a.endpoint.Host
	if a.endpoint.Port() == "" {
		host = a.endpoint.Host + ":" + a.endpoint.Scheme
	}
	si.Backend.GatewayOnline = minio.IsBackendOnline(ctx, host)
	return si, nil
}

// MakeBucketWithLocation - Create a new container on azure backend.
func (a *azureObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	// Filter out unsupported features in Azure and return immediately with NotImplemented error
	if opts.LockEnabled || opts.VersioningEnabled || strings.ContainsAny(bucket, ".") {
		return minio.NotImplemented{}
	}

	// Verify if bucket (container-name) is valid.
	// IsValidBucketName has same restrictions as container names mentioned
	// in azure documentation, so we will simply use the same function here.
	// Ref - https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
	if !minio.IsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	containerURL := a.client.NewContainerURL(bucket)
	_, err := containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	return azureToObjectError(err, bucket)
}

// GetBucketInfo - Get bucket metadata..
func (a *azureObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, e error) {
	// Azure does not have an equivalent call, hence use
	// ListContainers with prefix

	marker := azblob.Marker{}

	for marker.NotDone() {
		resp, err := a.client.ListContainersSegment(ctx, marker, azblob.ListContainersSegmentOptions{
			Prefix: bucket,
		})

		if err != nil {
			return bi, azureToObjectError(err, bucket)
		}

		for _, container := range resp.ContainerItems {
			if container.Name == bucket {
				t := container.Properties.LastModified
				return minio.BucketInfo{
					Name:    bucket,
					Created: t,
				}, nil
			} // else continue
		}

		marker = resp.NextMarker
	}
	return bi, minio.BucketNotFound{Bucket: bucket}
}

// ListBuckets - Lists all azure containers, uses Azure equivalent `ServiceURL.ListContainersSegment`.
func (a *azureObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	marker := azblob.Marker{}

	for marker.NotDone() {
		resp, err := a.client.ListContainersSegment(ctx, marker, azblob.ListContainersSegmentOptions{})

		if err != nil {
			return nil, azureToObjectError(err)
		}

		for _, container := range resp.ContainerItems {
			t := container.Properties.LastModified
			buckets = append(buckets, minio.BucketInfo{
				Name:    container.Name,
				Created: t,
			})
		}

		marker = resp.NextMarker
	}
	return buckets, nil
}

// DeleteBucket - delete a container on azure, uses Azure equivalent `ContainerURL.Delete`.
func (a *azureObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	if !forceDelete {
		// Check if the container is empty before deleting it.
		result, err := a.ListObjects(ctx, bucket, "", "", "", 1)
		if err != nil {
			return azureToObjectError(err, bucket)
		}
		if len(result.Objects) > 0 {
			return minio.BucketNotEmpty{Bucket: bucket}
		}
	}

	containerURL := a.client.NewContainerURL(bucket)
	_, err := containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	return azureToObjectError(err, bucket)
}

// ListObjects - lists all blobs on azure with in a container filtered by prefix
// and marker, uses Azure equivalent `ContainerURL.ListBlobsHierarchySegment`.
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

	azureListMarker := azblob.Marker{}
	if isAzureMarker(marker) {
		// If application is using Azure continuation token we should
		// strip the azureTokenPrefix we added in the previous list response.
		azureMarker := strings.TrimPrefix(marker, azureMarkerPrefix)
		azureListMarker.Val = &azureMarker
	}

	containerURL := a.client.NewContainerURL(bucket)
	for len(objects) == 0 && len(prefixes) == 0 {
		resp, err := containerURL.ListBlobsHierarchySegment(ctx, azureListMarker, delimiter, azblob.ListBlobsSegmentOptions{
			Prefix:     prefix,
			MaxResults: int32(maxKeys),
		})
		if err != nil {
			return result, azureToObjectError(err, bucket, prefix)
		}

		for _, blob := range resp.Segment.BlobItems {
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
			etag := minio.ToS3ETag(string(blob.Properties.Etag))
			switch {
			case len(blob.Properties.ContentMD5) != 0:
				etag = hex.EncodeToString(blob.Properties.ContentMD5)
			case blob.Metadata["md5sum"] != "":
				etag = blob.Metadata["md5sum"]
				delete(blob.Metadata, "md5sum")
			}

			objects = append(objects, minio.ObjectInfo{
				Bucket:          bucket,
				Name:            blob.Name,
				ModTime:         blob.Properties.LastModified,
				Size:            *blob.Properties.ContentLength,
				ETag:            etag,
				ContentType:     *blob.Properties.ContentType,
				ContentEncoding: *blob.Properties.ContentEncoding,
				UserDefined:     blob.Metadata,
			})
		}

		for _, blobPrefix := range resp.Segment.BlobPrefixes {
			if blobPrefix.Name == minio.GatewayMinioSysTmp {
				// We don't do strings.HasPrefix(blob.Name, minio.GatewayMinioSysTmp) here so that
				// we can use tools like mc to inspect the contents of minio.sys.tmp/
				// It is OK to allow listing of minio.sys.tmp/ in non-recursive mode as it aids in debugging.
				continue
			}
			if !isAzureMarker(marker) && blobPrefix.Name <= marker {
				// If the application used ListObjectsV1 style marker then we
				// skip all the entries till we reach the marker.
				continue
			}
			prefixes = append(prefixes, blobPrefix.Name)
		}

		azureListMarker = resp.NextMarker
		if !azureListMarker.NotDone() {
			// Reached end of listing.
			break
		}
	}

	result.Objects = objects
	result.Prefixes = prefixes
	if azureListMarker.NotDone() {
		// We add the {minio} prefix so that we know in the subsequent request that this
		// marker is a azure continuation token and not ListObjectV1 marker.
		result.NextMarker = azureMarkerPrefix + *azureListMarker.Val
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

	if startOffset != 0 || length != objInfo.Size {
		delete(objInfo.UserDefined, "Content-MD5")
	}

	pr, pw := io.Pipe()
	go func() {
		err := a.getObject(ctx, bucket, object, startOffset, length, pw, objInfo.InnerETag, opts)
		pw.CloseWithError(err)
	}()
	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts, pipeCloser)
}

// GetObject - reads an object from azure. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (a *azureObjects) getObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	// startOffset cannot be negative.
	if startOffset < 0 {
		return azureToObjectError(minio.InvalidRange{}, bucket, object)
	}

	accessCond := azblob.BlobAccessConditions{}
	if etag != "" {
		accessCond.ModifiedAccessConditions.IfMatch = azblob.ETag(etag)
	}

	blobURL := a.client.NewContainerURL(bucket).NewBlobURL(object)
	blob, err := blobURL.Download(ctx, startOffset, length, accessCond, false)
	if err != nil {
		return azureToObjectError(err, bucket, object)
	}

	rc := blob.Body(azblob.RetryReaderOptions{MaxRetryRequests: azureDownloadRetryAttempts})

	_, err = io.Copy(writer, rc)
	rc.Close()
	return err
}

// GetObjectInfo - reads blob metadata properties and replies back minio.ObjectInfo,
// uses Azure equivalent `BlobURL.GetProperties`.
func (a *azureObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	blobURL := a.client.NewContainerURL(bucket).NewBlobURL(object)
	blob, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}

	realETag := string(blob.ETag())

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
	etag := minio.ToS3ETag(realETag)
	metadata := blob.NewMetadata()
	contentMD5 := blob.ContentMD5()
	switch {
	case len(contentMD5) != 0:
		etag = hex.EncodeToString(contentMD5)
	case metadata["md5sum"] != "":
		etag = metadata["md5sum"]
		delete(metadata, "md5sum")
	}

	return minio.ObjectInfo{
		Bucket:          bucket,
		UserDefined:     azurePropertiesToS3Meta(metadata, blob.NewHTTPHeaders(), blob.ContentLength()),
		ETag:            etag,
		InnerETag:       realETag,
		ModTime:         blob.LastModified(),
		Name:            object,
		Size:            blob.ContentLength(),
		ContentType:     blob.ContentType(),
		ContentEncoding: blob.ContentEncoding(),
		StorageClass:    azureTierToS3StorageClass(blob.AccessTier()),
	}, nil
}

// PutObject - Create a new blob with the incoming data,
// uses Azure equivalent `UploadStreamToBlockBlob`.
func (a *azureObjects) PutObject(ctx context.Context, bucket, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	data := r.Reader
	if opts.UserDefined == nil {
		opts.UserDefined = map[string]string{}
	}

	metadata, properties, err := s3MetaToAzureProperties(ctx, opts.UserDefined)
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}

	blobURL := a.client.NewContainerURL(bucket).NewBlockBlobURL(object)

	_, err = azblob.UploadStreamToBlockBlob(ctx, data, blobURL, azblob.UploadStreamToBlockBlobOptions{
		MaxBuffers:      azureUploadConcurrency,
		BlobHTTPHeaders: properties,
		Metadata:        metadata,
	})
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}
	// Query the blob's properties and metadata
	get, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}
	// Update the blob's metadata with Content-MD5 after the upload
	metadata = get.NewMetadata()
	metadata["md5sum"] = r.MD5CurrentHexString()
	_, err = blobURL.SetMetadata(ctx, metadata, azblob.BlobAccessConditions{})
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}
	return a.GetObjectInfo(ctx, bucket, object, opts)
}

// CopyObject - Copies a blob from source container to destination container.
// Uses Azure equivalent `BlobURL.StartCopyFromURL`.
func (a *azureObjects) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if srcOpts.CheckPrecondFn != nil && srcOpts.CheckPrecondFn(srcInfo) {
		return minio.ObjectInfo{}, minio.PreConditionFailed{}
	}
	srcBlob := a.client.NewContainerURL(srcBucket).NewBlobURL(srcObject)
	srcBlobURL := srcBlob.URL()

	srcProps, err := srcBlob.GetProperties(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		return objInfo, azureToObjectError(err, srcBucket, srcObject)
	}
	destBlob := a.client.NewContainerURL(destBucket).NewBlobURL(destObject)

	azureMeta, props, err := s3MetaToAzureProperties(ctx, srcInfo.UserDefined)
	if err != nil {
		return objInfo, azureToObjectError(err, srcBucket, srcObject)
	}
	props.ContentMD5 = srcProps.ContentMD5()
	azureMeta["md5sum"] = srcInfo.ETag
	res, err := destBlob.StartCopyFromURL(ctx, srcBlobURL, azureMeta, azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{})
	if err != nil {
		return objInfo, azureToObjectError(err, srcBucket, srcObject)
	}
	// StartCopyFromURL is an asynchronous operation so need to poll for completion,
	// see https://docs.microsoft.com/en-us/rest/api/storageservices/copy-blob#remarks.
	copyStatus := res.CopyStatus()
	for copyStatus != azblob.CopyStatusSuccess {
		destProps, err := destBlob.GetProperties(ctx, azblob.BlobAccessConditions{})
		if err != nil {
			return objInfo, azureToObjectError(err, srcBucket, srcObject)
		}
		copyStatus = destProps.CopyStatus()
	}

	// Azure will copy metadata from the source object when an empty metadata map is provided.
	// To handle the case where the source object should be copied without its metadata,
	// the metadata must be removed from the dest. object after the copy completes
	if len(azureMeta) == 0 {
		_, err = destBlob.SetMetadata(ctx, azureMeta, azblob.BlobAccessConditions{})
		if err != nil {
			return objInfo, azureToObjectError(err, srcBucket, srcObject)
		}
	}

	_, err = destBlob.SetHTTPHeaders(ctx, props, azblob.BlobAccessConditions{})
	if err != nil {
		return objInfo, azureToObjectError(err, srcBucket, srcObject)
	}

	if _, ok := srcInfo.UserDefined["x-amz-storage-class"]; ok {
		_, err = destBlob.SetTier(ctx, s3StorageClassToAzureTier(srcInfo.UserDefined["x-amz-storage-class"]),
			azblob.LeaseAccessConditions{})
		if err != nil {
			return objInfo, azureToObjectError(err, srcBucket, srcObject)
		}
	}

	return a.GetObjectInfo(ctx, destBucket, destObject, dstOpts)
}

// DeleteObject - Deletes a blob on azure container, uses Azure
// equivalent `BlobURL.Delete`.
func (a *azureObjects) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	blob := a.client.NewContainerURL(bucket).NewBlobURL(object)
	_, err := blob.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		err = azureToObjectError(err, bucket, object)
		if !errors.Is(err, minio.ObjectNotFound{Bucket: bucket, Object: object}) {
			return minio.ObjectInfo{}, err
		}
	}
	return minio.ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, nil
}

func (a *azureObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]minio.DeletedObject, len(objects))
	for idx, object := range objects {
		_, errs[idx] = a.DeleteObject(ctx, bucket, object.ObjectName, opts)
		dobjects[idx] = minio.DeletedObject{
			ObjectName: object.ObjectName,
		}
	}
	return dobjects, errs
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
	blobURL := a.client.NewContainerURL(bucketName).NewBlobURL(
		getAzureMetadataObjectName(objectName, uploadID))
	_, err = blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
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

// NewMultipartUpload - Use Azure equivalent `BlobURL.Upload`.
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

	blobURL := a.client.NewContainerURL(bucket).NewBlockBlobURL(metadataObject)
	_, err = blobURL.Upload(ctx, bytes.NewReader(jsonData), azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	if err != nil {
		return "", azureToObjectError(err, bucket, metadataObject)
	}

	return uploadID, nil
}

func (a *azureObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, uploadID string, partID int,
	startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (info minio.PartInfo, err error) {
	return a.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}

// PutObjectPart - Use Azure equivalent `BlobURL.StageBlock`.
func (a *azureObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	data := r.Reader
	if err = a.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return info, err
	}

	if err = checkAzureUploadID(ctx, uploadID); err != nil {
		return info, err
	}

	partMetaV1 := newPartMetaV1(uploadID, partID)
	subPartSize, subPartNumber := int64(azureUploadChunkSize), 1
	for remainingSize := data.Size(); remainingSize > 0; remainingSize -= subPartSize {
		if remainingSize < subPartSize {
			subPartSize = remainingSize
		}

		id := base64.StdEncoding.EncodeToString([]byte(minio.MustGetUUID()))
		blobURL := a.client.NewContainerURL(bucket).NewBlockBlobURL(object)
		body, err := ioutil.ReadAll(io.LimitReader(data, subPartSize))
		if err != nil {
			return info, azureToObjectError(err, bucket, object)
		}
		_, err = blobURL.StageBlock(ctx, id, bytes.NewReader(body), azblob.LeaseAccessConditions{}, nil)
		if err != nil {
			return info, azureToObjectError(err, bucket, object)
		}
		partMetaV1.BlockIDs = append(partMetaV1.BlockIDs, id)
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

	blobURL := a.client.NewContainerURL(bucket).NewBlockBlobURL(metadataObject)
	_, err = blobURL.Upload(ctx, bytes.NewReader(jsonData), azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	if err != nil {
		return info, azureToObjectError(err, bucket, metadataObject)
	}

	info.PartNumber = partID
	info.ETag = partMetaV1.ETag
	info.LastModified = minio.UTCNow()
	info.Size = data.Size()
	return info, nil
}

// GetMultipartInfo returns multipart info of the uploadId of the object
func (a *azureObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (result minio.MultipartInfo, err error) {
	if err = a.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	return result, nil
}

// ListObjectParts - Use Azure equivalent `ContainerURL.ListBlobsHierarchySegment`.
func (a *azureObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	if err = a.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts

	azureListMarker := ""
	marker := azblob.Marker{Val: &azureListMarker}

	var parts []minio.PartInfo
	var delimiter string
	maxKeys := maxPartsCount
	if partNumberMarker == 0 {
		maxKeys = maxParts
	}
	prefix := getAzureMetadataPartPrefix(uploadID, object)
	containerURL := a.client.NewContainerURL(bucket)
	resp, err := containerURL.ListBlobsHierarchySegment(ctx, marker, delimiter, azblob.ListBlobsSegmentOptions{
		Prefix:     prefix,
		MaxResults: int32(maxKeys),
	})
	if err != nil {
		return result, azureToObjectError(err, bucket, prefix)
	}

	for _, blob := range resp.Segment.BlobItems {
		if delimiter == "" && !strings.HasPrefix(blob.Name, minio.GatewayMinioSysTmp) {
			// We filter out non minio.GatewayMinioSysTmp entries in the recursive listing.
			continue
		}
		// filter temporary metadata file for blob
		if strings.HasSuffix(blob.Name, "azure.json") {
			continue
		}
		if !isAzureMarker(*marker.Val) && blob.Name <= *marker.Val {
			// If the application used ListObjectsV1 style marker then we
			// skip all the entries till we reach the marker.
			continue
		}
		partNumber, err := parseAzurePart(blob.Name, prefix)
		if err != nil {
			return result, azureToObjectError(fmt.Errorf("Unexpected error"), bucket, object)
		}
		var metadata partMetadataV1
		blobURL := containerURL.NewBlobURL(blob.Name)
		blob, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
		if err != nil {
			return result, azureToObjectError(fmt.Errorf("Unexpected error"), bucket, object)
		}
		metadataReader := blob.Body(azblob.RetryReaderOptions{MaxRetryRequests: azureDownloadRetryAttempts})
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
func (a *azureObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (err error) {
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
			pblob := a.client.NewContainerURL(bucket).NewBlobURL(
				getAzureMetadataPartName(object, uploadID, part.PartNumber))
			pblob.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		}
		partNumberMarker = lpi.NextPartNumberMarker
		if !lpi.IsTruncated {
			break
		}
	}

	blobURL := a.client.NewContainerURL(bucket).NewBlobURL(
		getAzureMetadataObjectName(object, uploadID))
	_, err = blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	return err
}

// CompleteMultipartUpload - Use Azure equivalent `BlobURL.CommitBlockList`.
func (a *azureObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	metadataObject := getAzureMetadataObjectName(object, uploadID)
	if err = a.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return objInfo, err
	}

	if err = checkAzureUploadID(ctx, uploadID); err != nil {
		return objInfo, err
	}

	blobURL := a.client.NewContainerURL(bucket).NewBlobURL(metadataObject)
	blob, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, metadataObject)
	}

	var metadata azureMultipartMetadata
	metadataReader := blob.Body(azblob.RetryReaderOptions{MaxRetryRequests: azureDownloadRetryAttempts})
	if err = json.NewDecoder(metadataReader).Decode(&metadata); err != nil {
		logger.LogIf(ctx, err)
		return objInfo, azureToObjectError(err, bucket, metadataObject)
	}

	objBlob := a.client.NewContainerURL(bucket).NewBlockBlobURL(object)

	var allBlocks []string
	for i, part := range uploadedParts {
		var partMetadata partMetadataV1
		partMetadataObject := getAzureMetadataPartName(object, uploadID, part.PartNumber)
		pblobURL := a.client.NewContainerURL(bucket).NewBlobURL(partMetadataObject)
		pblob, err := pblobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
		if err != nil {
			return objInfo, azureToObjectError(err, bucket, partMetadataObject)
		}

		partMetadataReader := pblob.Body(azblob.RetryReaderOptions{MaxRetryRequests: azureDownloadRetryAttempts})
		if err = json.NewDecoder(partMetadataReader).Decode(&partMetadata); err != nil {
			logger.LogIf(ctx, err)
			return objInfo, azureToObjectError(err, bucket, partMetadataObject)
		}

		if partMetadata.ETag != part.ETag {
			return objInfo, minio.InvalidPart{}
		}
		allBlocks = append(allBlocks, partMetadata.BlockIDs...)
		if i < (len(uploadedParts)-1) && partMetadata.Size < azureS3MinPartSize {
			return objInfo, minio.PartTooSmall{
				PartNumber: uploadedParts[i].PartNumber,
				PartSize:   partMetadata.Size,
				PartETag:   uploadedParts[i].ETag,
			}
		}
	}

	objMetadata, objProperties, err := s3MetaToAzureProperties(ctx, metadata.Metadata)
	if err != nil {
		return objInfo, azureToObjectError(err, bucket, object)
	}
	objMetadata["md5sum"] = minio.ComputeCompleteMultipartMD5(uploadedParts)

	_, err = objBlob.CommitBlockList(ctx, allBlocks, objProperties, objMetadata, azblob.BlobAccessConditions{})
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
			pblob := a.client.NewContainerURL(bucket).NewBlobURL(
				getAzureMetadataPartName(object, uploadID, part.PartNumber))
			pblob.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
		}
		partNumberMarker = lpi.NextPartNumberMarker
		if !lpi.IsTruncated {
			break
		}
	}

	_, derr := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	logger.GetReqInfo(ctx).AppendTags("uploadID", uploadID)
	logger.LogIf(ctx, derr)

	return a.GetObjectInfo(ctx, bucket, object, minio.ObjectOptions{})
}

// SetBucketPolicy - Azure supports three types of container policies:
// azblob.PublicAccessContainer - readonly in minio terminology
// azblob.PublicAccessBlob - readonly without listing in minio terminology
// azblob.PublicAccessNone - none in minio terminology
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
	perm := azblob.PublicAccessContainer
	container := a.client.NewContainerURL(bucket)
	_, err = container.SetAccessPolicy(ctx, perm, nil, azblob.ContainerAccessConditions{})
	return azureToObjectError(err, bucket)
}

// GetBucketPolicy - Get the container ACL and convert it to canonical []bucketAccessPolicy
func (a *azureObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	container := a.client.NewContainerURL(bucket)
	perm, err := container.GetAccessPolicy(ctx, azblob.LeaseAccessConditions{})
	if err != nil {
		return nil, azureToObjectError(err, bucket)
	}

	permAccessType := perm.BlobPublicAccess()

	if permAccessType == azblob.PublicAccessNone {
		return nil, minio.BucketPolicyNotFound{Bucket: bucket}
	} else if permAccessType != azblob.PublicAccessContainer {
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
	perm := azblob.PublicAccessNone
	containerURL := a.client.NewContainerURL(bucket)
	_, err := containerURL.SetAccessPolicy(ctx, perm, nil, azblob.ContainerAccessConditions{})
	return azureToObjectError(err)
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (a *azureObjects) IsCompressionSupported() bool {
	return false
}
