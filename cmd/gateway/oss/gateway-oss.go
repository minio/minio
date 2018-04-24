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

package oss

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/dustin/go-humanize"

	"github.com/minio/cli"
	miniogopolicy "github.com/minio/minio-go/pkg/policy"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/policy/condition"
)

const (
	ossS3MinPartSize = 5 * humanize.MiByte
	ossMaxParts      = 1000
	ossMaxKeys       = 1000
	ossBackend       = "oss"
)

func init() {
	const ossGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [ENDPOINT]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENDPOINT:
  OSS server endpoint. Default ENDPOINT is https://oss.aliyuncs.com

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of OSS storage.
     MINIO_SECRET_KEY: Password or secret key of OSS storage.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  UPDATE:
     MINIO_UPDATE: To turn off in-place upgrades, set this value to "off".

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to Minio host domain name.
	
  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.

EXAMPLES:
  1. Start minio gateway server for Aliyun OSS backend.
      $ export MINIO_ACCESS_KEY=accesskey
      $ export MINIO_SECRET_KEY=secretkey
      $ {{.HelpName}}

  2. Start minio gateway server for Aliyun OSS backend on custom endpoint.
      $ export MINIO_ACCESS_KEY=Q3AM3UQ867SPQQA43P2F
      $ export MINIO_SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
      $ {{.HelpName}} https://oss.example.com

  3. Start minio gateway server for Aliyun OSS backend with edge caching enabled.
      $ export MINIO_ACCESS_KEY=accesskey
      $ export MINIO_SECRET_KEY=secretkey
      $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
      $ export MINIO_CACHE_EXCLUDE="bucket1/*;*.png"
      $ export MINIO_CACHE_EXPIRY=40
      $ {{.HelpName}}
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               "oss",
		Usage:              "Alibaba Cloud (Aliyun) Object Storage Service (OSS).",
		Action:             ossGatewayMain,
		CustomHelpTemplate: ossGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway oss' command line.
func ossGatewayMain(ctx *cli.Context) {
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, ossBackend, 1)
	}

	// Validate gateway arguments.
	host := ctx.Args().First()
	logger.FatalIf(minio.ValidateGatewayArguments(ctx.GlobalString("address"), host), "Invalid argument")

	minio.StartGateway(ctx, &OSS{host})
}

// OSS implements Gateway.
type OSS struct {
	host string
}

// Name implements Gateway interface.
func (g *OSS) Name() string {
	return ossBackend
}

// NewGatewayLayer implements Gateway interface and returns OSS ObjectLayer.
func (g *OSS) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	var err error

	// Regions and endpoints
	// https://www.alibabacloud.com/help/doc-detail/31837.htm
	if g.host == "" {
		g.host = "https://oss.aliyuncs.com"
	}

	// Initialize oss client object.
	client, err := oss.New(g.host, creds.AccessKey, creds.SecretKey)
	if err != nil {
		return nil, err
	}

	return &ossObjects{
		Client: client,
	}, nil
}

// Production - oss is production ready.
func (g *OSS) Production() bool {
	return true
}

// appendS3MetaToOSSOptions converts metadata meant for S3 PUT/COPY
// object into oss.Option.
//
// S3 user-metadata is translated to OSS metadata by removing the
// `X-Amz-Meta-` prefix and converted into `X-Oss-Meta-`.
//
// Header names are canonicalized as in http.Header.
func appendS3MetaToOSSOptions(ctx context.Context, opts []oss.Option, s3Metadata map[string]string) ([]oss.Option, error) {
	if opts == nil {
		opts = make([]oss.Option, 0, len(s3Metadata))
	}
	for k, v := range s3Metadata {
		k = http.CanonicalHeaderKey(k)

		switch {
		case strings.HasPrefix(k, "X-Amz-Meta-"):
			metaKey := k[len("X-Amz-Meta-"):]
			// NOTE(timonwong): OSS won't allow headers with underscore(_).
			if strings.Contains(metaKey, "_") {
				logger.LogIf(ctx, minio.UnsupportedMetadata{})
				return nil, minio.UnsupportedMetadata{}
			}
			opts = append(opts, oss.Meta(metaKey, v))
		case k == "X-Amz-Acl":
			// Valid values: public-read, private, and public-read-write
			opts = append(opts, oss.ObjectACL(oss.ACLType(v)))
		case k == "X-Amz-Server-Side​-Encryption":
			opts = append(opts, oss.ServerSideEncryption(v))
		case k == "X-Amz-Copy-Source-If-Match":
			opts = append(opts, oss.CopySourceIfMatch(v))
		case k == "X-Amz-Copy-Source-If-None-Match":
			opts = append(opts, oss.CopySourceIfNoneMatch(v))
		case k == "X-Amz-Copy-Source-If-Unmodified-Since":
			if v, err := http.ParseTime(v); err == nil {
				opts = append(opts, oss.CopySourceIfUnmodifiedSince(v))
			}
		case k == "X-Amz-Copy-Source-If-Modified-Since":
			if v, err := http.ParseTime(v); err == nil {
				opts = append(opts, oss.CopySourceIfModifiedSince(v))
			}
		case k == "Accept-Encoding":
			opts = append(opts, oss.AcceptEncoding(v))
		case k == "Cache-Control":
			opts = append(opts, oss.CacheControl(v))
		case k == "Content-Disposition":
			opts = append(opts, oss.ContentDisposition(v))
		case k == "Content-Encoding":
			opts = append(opts, oss.ContentEncoding(v))
		case k == "Content-Length":
			if v, err := strconv.ParseInt(v, 10, 64); err == nil {
				opts = append(opts, oss.ContentLength(v))
			}
		case k == "Content-MD5":
			opts = append(opts, oss.ContentMD5(v))
		case k == "Content-Type":
			opts = append(opts, oss.ContentType(v))
		case k == "Expires":
			if v, err := http.ParseTime(v); err == nil {
				opts = append(opts, oss.Expires(v))
			}
		}
	}

	return opts, nil
}

// ossMetaToS3Meta converts OSS metadata to S3 metadata.
// It is the reverse of appendS3MetaToOSSOptions.
func ossHeaderToS3Meta(header http.Header) map[string]string {
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
	for k := range header {
		k = http.CanonicalHeaderKey(k)
		switch {
		case strings.HasPrefix(k, oss.HTTPHeaderOssMetaPrefix):
			// Add amazon s3 meta prefix
			metaKey := k[len(oss.HTTPHeaderOssMetaPrefix):]
			metaKey = "X-Amz-Meta-" + decodeKey(metaKey)
			metaKey = http.CanonicalHeaderKey(metaKey)
			s3Metadata[metaKey] = header.Get(k)
		case k == "Cache-Control":
			fallthrough
		case k == "Content-Encoding":
			fallthrough
		case k == "Content-Disposition":
			fallthrough
		case k == "Content-Length":
			fallthrough
		case k == "Content-MD5":
			fallthrough
		case k == "Content-Type":
			s3Metadata[k] = header.Get(k)
		}
	}

	return s3Metadata
}

// ossToObjectError converts OSS errors to minio object layer errors.
func ossToObjectError(err error, params ...string) error {
	if err == nil {
		return nil
	}

	bucket := ""
	object := ""
	uploadID := ""
	switch len(params) {
	case 3:
		uploadID = params[2]
		fallthrough
	case 2:
		object = params[1]
		fallthrough
	case 1:
		bucket = params[0]
	}

	ossErr, ok := err.(oss.ServiceError)
	if !ok {
		// We don't interpret non OSS errors. As oss errors will
		// have StatusCode to help to convert to object errors.
		return err
	}

	switch ossErr.Code {
	case "BucketAlreadyExists":
		err = minio.BucketAlreadyOwnedByYou{Bucket: bucket}
	case "BucketNotEmpty":
		err = minio.BucketNotEmpty{Bucket: bucket}
	case "InvalidBucketName":
		err = minio.BucketNameInvalid{Bucket: bucket}
	case "NoSuchBucket":
		err = minio.BucketNotFound{Bucket: bucket}
	case "NoSuchKey":
		if object != "" {
			err = minio.ObjectNotFound{Bucket: bucket, Object: object}
		} else {
			err = minio.BucketNotFound{Bucket: bucket}
		}
	case "InvalidObjectName":
		err = minio.ObjectNameInvalid{Bucket: bucket, Object: object}
	case "AccessDenied":
		err = minio.PrefixAccessDenied{Bucket: bucket, Object: object}
	case "NoSuchUpload":
		err = minio.InvalidUploadID{UploadID: uploadID}
	case "EntityTooSmall":
		err = minio.PartTooSmall{}
	case "SignatureDoesNotMatch":
		err = minio.SignatureDoesNotMatch{}
	case "InvalidPart":
		err = minio.InvalidPart{}
	}

	return err
}

// ossObjects implements gateway for Aliyun Object Storage Service.
type ossObjects struct {
	minio.GatewayUnsupported
	Client *oss.Client
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *ossObjects) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo is not relevant to OSS backend.
func (l *ossObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo) {
	return
}

// ossIsValidBucketName verifies whether a bucket name is valid.
func ossIsValidBucketName(bucket string) bool {
	// dot is not allowed in bucket name
	if strings.Contains(bucket, ".") {
		return false
	}
	if !minio.IsValidBucketName(bucket) {
		return false
	}
	return true
}

// MakeBucketWithLocation creates a new container on OSS backend.
func (l *ossObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	if !ossIsValidBucketName(bucket) {
		logger.LogIf(ctx, minio.BucketNameInvalid{Bucket: bucket})
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	err := l.Client.CreateBucket(bucket)
	logger.LogIf(ctx, err)
	return ossToObjectError(err, bucket)
}

// ossGeBucketInfo gets bucket metadata.
func ossGeBucketInfo(ctx context.Context, client *oss.Client, bucket string) (bi minio.BucketInfo, err error) {
	bgir, err := client.GetBucketInfo(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return bi, ossToObjectError(err, bucket)
	}

	return minio.BucketInfo{
		Name:    bgir.BucketInfo.Name,
		Created: bgir.BucketInfo.CreationDate,
	}, nil
}

// GetBucketInfo gets bucket metadata.
func (l *ossObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	return ossGeBucketInfo(ctx, l.Client, bucket)
}

// ListBuckets lists all OSS buckets.
func (l *ossObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	marker := oss.Marker("")
	for {
		lbr, err := l.Client.ListBuckets(marker)
		if err != nil {
			logger.LogIf(ctx, err)
			return nil, ossToObjectError(err)
		}

		for _, bi := range lbr.Buckets {
			buckets = append(buckets, minio.BucketInfo{
				Name:    bi.Name,
				Created: bi.CreationDate,
			})
		}

		marker = oss.Marker(lbr.NextMarker)
		if !lbr.IsTruncated {
			break
		}
	}

	return buckets, nil
}

// DeleteBucket deletes a bucket on OSS.
func (l *ossObjects) DeleteBucket(ctx context.Context, bucket string) error {
	err := l.Client.DeleteBucket(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return ossToObjectError(err, bucket)
	}
	return nil
}

// fromOSSClientObjectProperties converts oss ObjectProperties to ObjectInfo.
func fromOSSClientObjectProperties(bucket string, o oss.ObjectProperties) minio.ObjectInfo {
	// NOTE(timonwong): No Content-Type and user defined metadata.
	// https://www.alibabacloud.com/help/doc-detail/31965.htm

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    o.Key,
		ModTime: o.LastModified,
		Size:    o.Size,
		ETag:    minio.ToS3ETag(o.ETag),
	}
}

// fromOSSClientListObjectsResult converts oss ListBucketResult to ListObjectsInfo.
func fromOSSClientListObjectsResult(bucket string, lor oss.ListObjectsResult) minio.ListObjectsInfo {
	objects := make([]minio.ObjectInfo, len(lor.Objects))
	for i, oi := range lor.Objects {
		objects[i] = fromOSSClientObjectProperties(bucket, oi)
	}

	prefixes := make([]string, len(lor.CommonPrefixes))
	copy(prefixes, lor.CommonPrefixes)

	return minio.ListObjectsInfo{
		IsTruncated: lor.IsTruncated,
		NextMarker:  lor.NextMarker,
		Objects:     objects,
		Prefixes:    prefixes,
	}
}

// ossListObjects lists all blobs in OSS bucket filtered by prefix.
func ossListObjects(ctx context.Context, client *oss.Client, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	buck, err := client.Bucket(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return loi, ossToObjectError(err, bucket)
	}

	// maxKeys should default to 1000 or less.
	if maxKeys == 0 || maxKeys > ossMaxKeys {
		maxKeys = ossMaxKeys
	}

	lor, err := buck.ListObjects(oss.Prefix(prefix), oss.Marker(marker), oss.Delimiter(delimiter), oss.MaxKeys(maxKeys))
	if err != nil {
		logger.LogIf(ctx, err)
		return loi, ossToObjectError(err, bucket)
	}

	return fromOSSClientListObjectsResult(bucket, lor), nil
}

// ossListObjectsV2 lists all blobs in OSS bucket filtered by prefix.
func ossListObjectsV2(ctx context.Context, client *oss.Client, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	// fetchOwner and startAfter are not supported and unused.
	marker := continuationToken

	resultV1, err := ossListObjects(ctx, client, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, err
	}

	return minio.ListObjectsV2Info{
		Objects:               resultV1.Objects,
		Prefixes:              resultV1.Prefixes,
		ContinuationToken:     continuationToken,
		NextContinuationToken: resultV1.NextMarker,
		IsTruncated:           resultV1.IsTruncated,
	}, nil
}

// ListObjects lists all blobs in OSS bucket filtered by prefix.
func (l *ossObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	return ossListObjects(ctx, l.Client, bucket, prefix, marker, delimiter, maxKeys)
}

// ListObjectsV2 lists all blobs in OSS bucket filtered by prefix
func (l *ossObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	return ossListObjectsV2(ctx, l.Client, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
}

// ossGetObject reads an object on OSS. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func ossGetObject(ctx context.Context, client *oss.Client, bucket, key string, startOffset, length int64, writer io.Writer, etag string) error {
	if length < 0 && length != -1 {
		logger.LogIf(ctx, fmt.Errorf("Invalid argument"))
		return ossToObjectError(fmt.Errorf("Invalid argument"), bucket, key)
	}

	bkt, err := client.Bucket(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return ossToObjectError(err, bucket, key)
	}

	var opts []oss.Option
	if startOffset >= 0 && length >= 0 {
		opts = append(opts, oss.Range(startOffset, startOffset+length-1))
	}

	object, err := bkt.GetObject(key, opts...)
	if err != nil {
		logger.LogIf(ctx, err)
		return ossToObjectError(err, bucket, key)
	}
	defer object.Close()

	if _, err := io.Copy(writer, object); err != nil {
		logger.LogIf(ctx, err)
		return ossToObjectError(err, bucket, key)
	}
	return nil
}

// GetObject reads an object on OSS. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *ossObjects) GetObject(ctx context.Context, bucket, key string, startOffset, length int64, writer io.Writer, etag string) error {
	return ossGetObject(ctx, l.Client, bucket, key, startOffset, length, writer, etag)
}

func translatePlainError(err error) error {
	errString := err.Error()

	switch errString {
	case "oss: service returned without a response body (404 Not Found)":
		return oss.ServiceError{Code: "NoSuchKey"}
	case "oss: service returned without a response body (400 Bad Request)":
		return oss.ServiceError{Code: "AccessDenied"}
	}

	return err
}

// ossGetObjectInfo reads object info and replies back ObjectInfo.
func ossGetObjectInfo(ctx context.Context, client *oss.Client, bucket, object string) (objInfo minio.ObjectInfo, err error) {
	bkt, err := client.Bucket(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, ossToObjectError(err, bucket, object)
	}

	header, err := bkt.GetObjectDetailedMeta(object)
	if err != nil {
		logger.LogIf(ctx, translatePlainError(err))
		return objInfo, ossToObjectError(translatePlainError(err), bucket, object)
	}

	// Build S3 metadata from OSS metadata
	userDefined := ossHeaderToS3Meta(header)

	modTime, _ := http.ParseTime(header.Get("Last-Modified"))
	size, _ := strconv.ParseInt(header.Get("Content-Length"), 10, 64)

	return minio.ObjectInfo{
		Bucket:          bucket,
		Name:            object,
		ModTime:         modTime,
		Size:            size,
		ETag:            minio.ToS3ETag(header.Get("ETag")),
		UserDefined:     userDefined,
		ContentType:     header.Get("Content-Type"),
		ContentEncoding: header.Get("Content-Encoding"),
	}, nil
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (l *ossObjects) GetObjectInfo(ctx context.Context, bucket, object string) (objInfo minio.ObjectInfo, err error) {
	return ossGetObjectInfo(ctx, l.Client, bucket, object)
}

// ossPutObject creates a new object with the incoming data.
func ossPutObject(ctx context.Context, client *oss.Client, bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo minio.ObjectInfo, err error) {
	bkt, err := client.Bucket(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, ossToObjectError(err, bucket, object)
	}

	// Build OSS metadata
	opts, err := appendS3MetaToOSSOptions(ctx, nil, metadata)
	if err != nil {
		return objInfo, ossToObjectError(err, bucket, object)
	}

	err = bkt.PutObject(object, data, opts...)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, ossToObjectError(err, bucket, object)
	}

	return ossGetObjectInfo(ctx, client, bucket, object)
}

// PutObject creates a new object with the incoming data.
func (l *ossObjects) PutObject(ctx context.Context, bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo minio.ObjectInfo, err error) {
	return ossPutObject(ctx, l.Client, bucket, object, data, metadata)
}

// CopyObject copies an object from source bucket to a destination bucket.
func (l *ossObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo) (objInfo minio.ObjectInfo, err error) {
	bkt, err := l.Client.Bucket(srcBucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, ossToObjectError(err, srcBucket, srcObject)
	}

	opts := make([]oss.Option, 0, len(srcInfo.UserDefined)+1)
	// Set this header such that following CopyObject() always sets the right metadata on the destination.
	// metadata input is already a trickled down value from interpreting x-oss-metadata-directive at
	// handler layer. So what we have right now is supposed to be applied on the destination object anyways.
	// So preserve it by adding "REPLACE" directive to save all the metadata set by CopyObject API.
	opts = append(opts, oss.MetadataDirective(oss.MetaReplace))

	// Build OSS metadata
	opts, err = appendS3MetaToOSSOptions(ctx, opts, srcInfo.UserDefined)
	if err != nil {
		return objInfo, ossToObjectError(err, srcBucket, srcObject)
	}

	if _, err = bkt.CopyObjectTo(dstBucket, dstObject, srcObject, opts...); err != nil {
		logger.LogIf(ctx, err)
		return objInfo, ossToObjectError(err, srcBucket, srcObject)
	}
	return l.GetObjectInfo(ctx, dstBucket, dstObject)
}

// DeleteObject deletes a blob in bucket.
func (l *ossObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	bkt, err := l.Client.Bucket(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return ossToObjectError(err, bucket, object)
	}

	err = bkt.DeleteObject(object)
	if err != nil {
		logger.LogIf(ctx, err)
		return ossToObjectError(err, bucket, object)
	}
	return nil
}

// fromOSSClientListMultipartsInfo converts oss ListMultipartUploadResult to ListMultipartsInfo
func fromOSSClientListMultipartsInfo(lmur oss.ListMultipartUploadResult) minio.ListMultipartsInfo {
	uploads := make([]minio.MultipartInfo, len(lmur.Uploads))
	for i, um := range lmur.Uploads {
		uploads[i] = minio.MultipartInfo{
			Object:    um.Key,
			UploadID:  um.UploadID,
			Initiated: um.Initiated,
		}
	}

	commonPrefixes := make([]string, len(lmur.CommonPrefixes))
	copy(commonPrefixes, lmur.CommonPrefixes)

	return minio.ListMultipartsInfo{
		KeyMarker:          lmur.KeyMarker,
		UploadIDMarker:     lmur.UploadIDMarker,
		NextKeyMarker:      lmur.NextKeyMarker,
		NextUploadIDMarker: lmur.NextUploadIDMarker,
		MaxUploads:         lmur.MaxUploads,
		IsTruncated:        lmur.IsTruncated,
		Uploads:            uploads,
		Prefix:             lmur.Prefix,
		Delimiter:          lmur.Delimiter,
		CommonPrefixes:     commonPrefixes,
	}
}

// ListMultipartUploads lists all multipart uploads.
func (l *ossObjects) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, err error) {
	bkt, err := l.Client.Bucket(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return lmi, ossToObjectError(err, bucket)
	}

	lmur, err := bkt.ListMultipartUploads(oss.Prefix(prefix), oss.KeyMarker(keyMarker), oss.UploadIDMarker(uploadIDMarker),
		oss.Delimiter(delimiter), oss.MaxUploads(maxUploads))
	if err != nil {
		logger.LogIf(ctx, err)
		return lmi, ossToObjectError(err, bucket)
	}

	return fromOSSClientListMultipartsInfo(lmur), nil
}

// NewMultipartUpload upload object in multiple parts.
func (l *ossObjects) NewMultipartUpload(ctx context.Context, bucket, object string, metadata map[string]string) (uploadID string, err error) {
	bkt, err := l.Client.Bucket(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return uploadID, ossToObjectError(err, bucket, object)
	}

	// Build OSS metadata
	opts, err := appendS3MetaToOSSOptions(ctx, nil, metadata)
	if err != nil {
		return uploadID, ossToObjectError(err, bucket, object)
	}

	lmur, err := bkt.InitiateMultipartUpload(object, opts...)
	if err != nil {
		logger.LogIf(ctx, err)
		return uploadID, ossToObjectError(err, bucket, object)
	}

	return lmur.UploadID, nil
}

// PutObjectPart puts a part of object in bucket.
func (l *ossObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *hash.Reader) (pi minio.PartInfo, err error) {
	bkt, err := l.Client.Bucket(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return pi, ossToObjectError(err, bucket, object)
	}

	imur := oss.InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      object,
		UploadID: uploadID,
	}
	size := data.Size()
	up, err := bkt.UploadPart(imur, data, size, partID)
	if err != nil {
		logger.LogIf(ctx, err)
		return pi, ossToObjectError(err, bucket, object)
	}

	return minio.PartInfo{
		Size: size,
		ETag: minio.ToS3ETag(up.ETag),
		// NOTE(timonwong): LastModified is not supported
		PartNumber: up.PartNumber,
	}, nil
}

func ossBuildListObjectPartsParams(uploadID string, partNumberMarker, maxParts int) map[string]interface{} {
	return map[string]interface{}{
		"uploadId":           uploadID,
		"part-number-marker": strconv.Itoa(partNumberMarker),
		"max-parts":          strconv.Itoa(maxParts),
	}
}

// fromOSSClientListPartsInfo converts OSS ListUploadedPartsResult to ListPartsInfo
func fromOSSClientListPartsInfo(lupr oss.ListUploadedPartsResult, partNumberMarker int) minio.ListPartsInfo {
	parts := make([]minio.PartInfo, len(lupr.UploadedParts))
	for i, up := range lupr.UploadedParts {
		parts[i] = minio.PartInfo{
			PartNumber:   up.PartNumber,
			LastModified: up.LastModified,
			ETag:         minio.ToS3ETag(up.ETag),
			Size:         int64(up.Size),
		}
	}

	nextPartNumberMarker, _ := strconv.Atoi(lupr.NextPartNumberMarker)
	return minio.ListPartsInfo{
		Bucket:               lupr.Bucket,
		Object:               lupr.Key,
		UploadID:             lupr.UploadID,
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             lupr.MaxParts,
		IsTruncated:          lupr.IsTruncated,
		Parts:                parts,
	}
}

func ossListObjectParts(client *oss.Client, bucket, object, uploadID string, partNumberMarker, maxParts int) (lupr oss.ListUploadedPartsResult, err error) {
	params := ossBuildListObjectPartsParams(uploadID, partNumberMarker, maxParts)
	resp, err := client.Conn.Do("GET", bucket, object, params, nil, nil, 0, nil)
	if err != nil {
		return lupr, err
	}

	defer func() {
		// always drain output (response body)
		io.CopyN(ioutil.Discard, resp.Body, 512)
		resp.Body.Close()
	}()

	err = xml.NewDecoder(resp.Body).Decode(&lupr)
	if err != nil {
		return lupr, err
	}
	return lupr, nil
}

// CopyObjectPart creates a part in a multipart upload by copying
// existing object or a part of it.
func (l *ossObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string,
	partID int, startOffset, length int64, srcInfo minio.ObjectInfo) (p minio.PartInfo, err error) {

	bkt, err := l.Client.Bucket(destBucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return p, ossToObjectError(err, destBucket)
	}

	// Build OSS metadata
	opts, err := appendS3MetaToOSSOptions(ctx, nil, srcInfo.UserDefined)
	if err != nil {
		return p, ossToObjectError(err, srcBucket, srcObject)
	}

	completePart, err := bkt.UploadPartCopy(oss.InitiateMultipartUploadResult{
		Key:      destObject,
		UploadID: uploadID,
	}, srcBucket, srcObject, startOffset, length, partID, opts...)

	if err != nil {
		logger.LogIf(ctx, err)
		return p, ossToObjectError(err, srcBucket, srcObject)
	}

	p.PartNumber = completePart.PartNumber
	p.ETag = minio.ToS3ETag(completePart.ETag)
	return p, nil
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (l *ossObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int) (lpi minio.ListPartsInfo, err error) {
	lupr, err := ossListObjectParts(l.Client, bucket, object, uploadID, partNumberMarker, maxParts)
	if err != nil {
		logger.LogIf(ctx, err)
		return lpi, ossToObjectError(err, bucket, object, uploadID)
	}

	return fromOSSClientListPartsInfo(lupr, partNumberMarker), nil
}

// AbortMultipartUpload aborts a ongoing multipart upload.
func (l *ossObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	bkt, err := l.Client.Bucket(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return ossToObjectError(err, bucket, object)
	}

	err = bkt.AbortMultipartUpload(oss.InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      object,
		UploadID: uploadID,
	})
	if err != nil {
		logger.LogIf(ctx, err)
		return ossToObjectError(err, bucket, object)
	}
	return nil
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object.
func (l *ossObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart) (oi minio.ObjectInfo, err error) {
	client := l.Client
	bkt, err := client.Bucket(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, ossToObjectError(err, bucket, object)
	}

	// Error out if uploadedParts except last part sizing < 5MiB.
	// NOTE(timonwong): Actually, OSS wont't throw EntityTooSmall error, doing this check just for mint :(
	var partNumberMarker int
	lupr := oss.ListUploadedPartsResult{IsTruncated: true}
	for lupr.IsTruncated {
		lupr, err = ossListObjectParts(client, bucket, object, uploadID, partNumberMarker, ossMaxParts)
		if err != nil {
			logger.LogIf(ctx, err)
			return oi, ossToObjectError(err, bucket, object, uploadID)
		}

		uploadedParts := lupr.UploadedParts
		if !lupr.IsTruncated {
			if len(uploadedParts) < 1 {
				uploadedParts = nil
			} else {
				uploadedParts = uploadedParts[:len(uploadedParts)-1]
			}
		}

		for _, part := range uploadedParts {
			if part.Size < ossS3MinPartSize {
				logger.LogIf(ctx, minio.PartTooSmall{
					PartNumber: part.PartNumber,
					PartSize:   int64(part.Size),
					PartETag:   minio.ToS3ETag(part.ETag),
				})
				return oi, minio.PartTooSmall{
					PartNumber: part.PartNumber,
					PartSize:   int64(part.Size),
					PartETag:   minio.ToS3ETag(part.ETag),
				}
			}
		}

		partNumberMarker, _ = strconv.Atoi(lupr.NextPartNumberMarker)
	}

	imur := oss.InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      object,
		UploadID: uploadID,
	}
	parts := make([]oss.UploadPart, len(uploadedParts))
	for i, up := range uploadedParts {
		parts[i] = oss.UploadPart{
			PartNumber: up.PartNumber,
			ETag:       strings.TrimSuffix(up.ETag, "-1"), // Trim "-1" suffix in ETag as PutObjectPart().
		}
	}

	_, err = bkt.CompleteMultipartUpload(imur, parts)
	if err != nil {
		logger.LogIf(ctx, err)
		return oi, ossToObjectError(err, bucket, object)
	}

	return l.GetObjectInfo(ctx, bucket, object)
}

// SetBucketPolicy sets policy on bucket.
// OSS supports three types of bucket policies:
// oss.ACLPublicReadWrite: readwrite in minio terminology
// oss.ACLPublicRead: readonly in minio terminology
// oss.ACLPrivate: none in minio terminology
func (l *ossObjects) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	policyInfo, err := minio.PolicyToBucketAccessPolicy(bucketPolicy)
	if err != nil {
		// This should not happen.
		return ossToObjectError(err, bucket)
	}

	bucketPolicies := miniogopolicy.GetPolicies(policyInfo.Statements, bucket, "")
	if len(bucketPolicies) != 1 {
		logger.LogIf(ctx, minio.NotImplemented{})
		return minio.NotImplemented{}
	}

	prefix := bucket + "/*" // For all objects inside the bucket.
	for policyPrefix, bucketPolicy := range bucketPolicies {
		if policyPrefix != prefix {
			logger.LogIf(ctx, minio.NotImplemented{})
			return minio.NotImplemented{}
		}

		var acl oss.ACLType
		switch bucketPolicy {
		case miniogopolicy.BucketPolicyNone:
			acl = oss.ACLPrivate
		case miniogopolicy.BucketPolicyReadOnly:
			acl = oss.ACLPublicRead
		case miniogopolicy.BucketPolicyReadWrite:
			acl = oss.ACLPublicReadWrite
		default:
			logger.LogIf(ctx, minio.NotImplemented{})
			return minio.NotImplemented{}
		}

		err := l.Client.SetBucketACL(bucket, acl)
		if err != nil {
			logger.LogIf(ctx, err)
			return ossToObjectError(err, bucket)
		}
	}

	return nil
}

// GetBucketPolicy will get policy on bucket.
func (l *ossObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	result, err := l.Client.GetBucketACL(bucket)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, ossToObjectError(err)
	}

	var readOnly, readWrite bool
	switch result.ACL {
	case string(oss.ACLPrivate):
		// By default, all buckets starts with a "private" policy.
		logger.LogIf(ctx, minio.BucketPolicyNotFound{})
		return nil, ossToObjectError(minio.BucketPolicyNotFound{}, bucket)
	case string(oss.ACLPublicRead):
		readOnly = true
	case string(oss.ACLPublicReadWrite):
		readWrite = true
	default:
		logger.LogIf(ctx, minio.NotImplemented{})
		return nil, minio.NotImplemented{}
	}

	actionSet := policy.NewActionSet()
	if readOnly {
		actionSet.Add(policy.GetBucketLocationAction)
		actionSet.Add(policy.ListBucketAction)
		actionSet.Add(policy.GetObjectAction)
	}
	if readWrite {
		actionSet.Add(policy.GetBucketLocationAction)
		actionSet.Add(policy.ListBucketAction)
		actionSet.Add(policy.GetObjectAction)
		actionSet.Add(policy.ListBucketMultipartUploadsAction)
		actionSet.Add(policy.AbortMultipartUploadAction)
		actionSet.Add(policy.DeleteObjectAction)
		actionSet.Add(policy.ListMultipartUploadPartsAction)
		actionSet.Add(policy.PutObjectAction)
	}

	return &policy.Policy{
		Version: policy.DefaultVersion,
		Statements: []policy.Statement{
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				actionSet,
				policy.NewResourceSet(
					policy.NewResource(bucket, ""),
					policy.NewResource(bucket, "*"),
				),
				condition.NewFunctions(),
			),
		},
	}, nil
}

// DeleteBucketPolicy deletes all policies on bucket.
func (l *ossObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	err := l.Client.SetBucketACL(bucket, oss.ACLPrivate)
	if err != nil {
		logger.LogIf(ctx, err)
		return ossToObjectError(err, bucket)
	}
	return nil
}
