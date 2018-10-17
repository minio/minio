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

package s3

import (
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/minio/cli"
	miniogo "github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/credentials"
	"github.com/minio/minio-go/pkg/s3utils"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/policy"

	minio "github.com/minio/minio/cmd"
)

const (
	s3Backend = "s3"
)

func init() {
	const s3GatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [ENDPOINT]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENDPOINT:
  S3 server endpoint. Default ENDPOINT is https://s3.amazonaws.com

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of S3 storage.
     MINIO_SECRET_KEY: Password or secret key of S3 storage.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to Minio host domain name.

  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.
     MINIO_CACHE_MAXUSE: Maximum permitted usage of the cache in percentage (0-100).

  LOGGER:
     MINIO_LOGGER_HTTP_ENDPOINT: HTTP endpoint URL to log all incoming requests.

EXAMPLES:
  1. Start minio gateway server for AWS S3 backend.
     $ export MINIO_ACCESS_KEY=accesskey
     $ export MINIO_SECRET_KEY=secretkey
     $ {{.HelpName}}

  2. Start minio gateway server for S3 backend on custom endpoint.
     $ export MINIO_ACCESS_KEY=Q3AM3UQ867SPQQA43P2F
     $ export MINIO_SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
     $ {{.HelpName}} https://play.minio.io:9000

  3. Start minio gateway server for AWS S3 backend logging all requests to http endpoint.
     $ export MINIO_ACCESS_KEY=Q3AM3UQ867SPQQA43P2F
     $ export MINIO_SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
     $ export MINIO_LOGGER_HTTP_ENDPOINT="http://localhost:8000/"
     $ {{.HelpName}} https://play.minio.io:9000

  4. Start minio gateway server for AWS S3 backend with edge caching enabled.
     $ export MINIO_ACCESS_KEY=accesskey
     $ export MINIO_SECRET_KEY=secretkey
     $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
     $ export MINIO_CACHE_EXCLUDE="bucket1/*;*.png"
     $ export MINIO_CACHE_EXPIRY=40
     $ export MINIO_CACHE_MAXUSE=80
     $ {{.HelpName}}

  4. Start minio gateway server for AWS S3 backend using AWS environment variables.
     NOTE: The access and secret key in this case will authenticate with Minio instead
     of AWS and AWS envs will be used to authenticate to AWS S3.
     $ export AWS_ACCESS_KEY_ID=aws_access_key
     $ export AWS_SECRET_ACCESS_KEY=aws_secret_key
     $ export MINIO_ACCESS_KEY=accesskey
     $ export MINIO_SECRET_KEY=secretkey
     $ {{.HelpName}}
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               s3Backend,
		Usage:              "Amazon Simple Storage Service (S3).",
		Action:             s3GatewayMain,
		CustomHelpTemplate: s3GatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway s3' command line.
func s3GatewayMain(ctx *cli.Context) {
	args := ctx.Args()
	if !ctx.Args().Present() {
		args = cli.Args{"https://s3.amazonaws.com"}
	}

	// Validate gateway arguments.
	logger.FatalIf(minio.ValidateGatewayArguments(ctx.GlobalString("address"), args.First()), "Invalid argument")

	// Start the gateway..
	minio.StartGateway(ctx, &S3{args.First()})
}

// S3 implements Gateway.
type S3 struct {
	host string
}

// Name implements Gateway interface.
func (g *S3) Name() string {
	return s3Backend
}

const letterBytes = "abcdefghijklmnopqrstuvwxyz01234569"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// randString generates random names and prepends them with a known prefix.
func randString(n int, src rand.Source, prefix string) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return prefix + string(b[0:30-len(prefix)])
}

// newS3 - Initializes a new client by auto probing S3 server signature.
func newS3(url string) (*miniogo.Core, error) {
	if url == "" {
		url = "https://s3.amazonaws.com"
	}

	// Override default params if the host is provided
	endpoint, secure, err := minio.ParseGatewayEndpoint(url)
	if err != nil {
		return nil, err
	}

	// Chains all credential types, in the following order:
	//  - AWS env vars (i.e. AWS_ACCESS_KEY_ID)
	//  - IAM profile based credentials. (performs an HTTP
	//    call to a pre-defined endpoint, only valid inside
	//    configured ec2 instances)
	//  - AWS creds file (i.e. AWS_SHARED_CREDENTIALS_FILE or ~/.aws/credentials)
	//  - Static credentials provided by user (i.e. MINIO_ACCESS_KEY)
	creds := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.EnvAWS{},
		&credentials.IAM{
			Client: &http.Client{
				Transport: minio.NewCustomHTTPTransport(),
			},
		},
		&credentials.FileAWSCredentials{},
		&credentials.EnvMinio{},
	})

	clnt, err := miniogo.NewWithCredentials(endpoint, creds, secure, "")
	if err != nil {
		return nil, err
	}

	probeBucketName := randString(60, rand.NewSource(time.Now().UnixNano()), "probe-bucket-sign-")
	// Check if the provided keys are valid.
	if _, err = clnt.BucketExists(probeBucketName); err != nil {
		return nil, err
	}

	return &miniogo.Core{Client: clnt}, nil
}

// NewGatewayLayer returns s3 ObjectLayer.
func (g *S3) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	// creds are ignored here, since S3 gateway implements chaining
	// all credentials.
	clnt, err := newS3(g.host)
	if err != nil {
		return nil, err
	}

	return &s3Objects{
		Client: clnt,
	}, nil
}

// Production - s3 gateway is production ready.
func (g *S3) Production() bool {
	return true
}

// s3Objects implements gateway for Minio and S3 compatible object storage servers.
type s3Objects struct {
	minio.GatewayUnsupported
	Client *miniogo.Core
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *s3Objects) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo is not relevant to S3 backend.
func (l *s3Objects) StorageInfo(ctx context.Context) (si minio.StorageInfo) {
	return si
}

// MakeBucket creates a new container on S3 backend.
func (l *s3Objects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	// Verify if bucket name is valid.
	// We are using a separate helper function here to validate bucket
	// names instead of IsValidBucketName() because there is a possibility
	// that certains users might have buckets which are non-DNS compliant
	// in us-east-1 and we might severely restrict them by not allowing
	// access to these buckets.
	// Ref - http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
	if s3utils.CheckValidBucketName(bucket) != nil {
		return minio.BucketNameInvalid{Bucket: bucket}
	}

	err := l.Client.MakeBucket(bucket, location)
	if err != nil {
		return minio.ErrorRespToObjectError(err, bucket)
	}
	return err
}

// GetBucketInfo gets bucket metadata..
func (l *s3Objects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, e error) {
	buckets, err := l.Client.ListBuckets()
	if err != nil {
		return bi, minio.ErrorRespToObjectError(err, bucket)
	}

	for _, bi := range buckets {
		if bi.Name != bucket {
			continue
		}

		return minio.BucketInfo{
			Name:    bi.Name,
			Created: bi.CreationDate,
		}, nil
	}

	return bi, minio.BucketNotFound{Bucket: bucket}
}

// ListBuckets lists all S3 buckets
func (l *s3Objects) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	buckets, err := l.Client.ListBuckets()
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err)
	}

	b := make([]minio.BucketInfo, len(buckets))
	for i, bi := range buckets {
		b[i] = minio.BucketInfo{
			Name:    bi.Name,
			Created: bi.CreationDate,
		}
	}

	return b, err
}

// DeleteBucket deletes a bucket on S3
func (l *s3Objects) DeleteBucket(ctx context.Context, bucket string) error {
	err := l.Client.RemoveBucket(bucket)
	if err != nil {
		return minio.ErrorRespToObjectError(err, bucket)
	}
	return nil
}

// ListObjects lists all blobs in S3 bucket filtered by prefix
func (l *s3Objects) ListObjects(ctx context.Context, bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, e error) {
	result, err := l.Client.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, minio.ErrorRespToObjectError(err, bucket)
	}

	return minio.FromMinioClientListBucketResult(bucket, result), nil
}

// ListObjectsV2 lists all blobs in S3 bucket filtered by prefix
func (l *s3Objects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, e error) {
	result, err := l.Client.ListObjectsV2(bucket, prefix, continuationToken, fetchOwner, delimiter, maxKeys, startAfter)
	if err != nil {
		return loi, minio.ErrorRespToObjectError(err, bucket)
	}

	return minio.FromMinioClientListBucketV2Result(bucket, result), nil
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (l *s3Objects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	var objInfo minio.ObjectInfo
	objInfo, err = l.GetObjectInfo(ctx, bucket, object, opts)
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
		err := l.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(err)
	}()
	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, pipeCloser), nil
}

// GetObject reads an object from S3. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *s3Objects) GetObject(ctx context.Context, bucket string, key string, startOffset int64, length int64, writer io.Writer, etag string, o minio.ObjectOptions) error {
	if length < 0 && length != -1 {
		return minio.ErrorRespToObjectError(minio.InvalidRange{}, bucket, key)
	}

	opts := miniogo.GetObjectOptions{}
	opts.ServerSideEncryption = o.ServerSideEncryption

	if startOffset >= 0 && length >= 0 {
		if err := opts.SetRange(startOffset, startOffset+length-1); err != nil {
			logger.LogIf(ctx, err)
			return minio.ErrorRespToObjectError(err, bucket, key)
		}
	}
	object, _, err := l.Client.GetObject(bucket, key, opts)
	if err != nil {
		return minio.ErrorRespToObjectError(err, bucket, key)
	}
	defer object.Close()

	if _, err := io.Copy(writer, object); err != nil {
		logger.LogIf(ctx, err)
		return minio.ErrorRespToObjectError(err, bucket, key)
	}
	return nil
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (l *s3Objects) GetObjectInfo(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	oi, err := l.Client.StatObject(bucket, object, miniogo.StatObjectOptions{miniogo.GetObjectOptions{ServerSideEncryption: opts.ServerSideEncryption}})
	if err != nil {
		return minio.ObjectInfo{}, minio.ErrorRespToObjectError(err, bucket, object)
	}

	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}

// PutObject creates a new object with the incoming data,
func (l *s3Objects) PutObject(ctx context.Context, bucket string, object string, data *hash.Reader, metadata map[string]string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	oi, err := l.Client.PutObject(bucket, object, data, data.Size(), data.MD5Base64String(), data.SHA256HexString(), minio.ToMinioClientMetadata(metadata), opts.ServerSideEncryption)
	if err != nil {
		return objInfo, minio.ErrorRespToObjectError(err, bucket, object)
	}
	// On success, populate the key & metadata so they are present in the notification
	oi.Key = object
	oi.Metadata = minio.ToMinioClientObjectInfoMetadata(metadata)

	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}

// CopyObject copies an object from source bucket to a destination bucket.
func (l *s3Objects) CopyObject(ctx context.Context, srcBucket string, srcObject string, dstBucket string, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	// Set this header such that following CopyObject() always sets the right metadata on the destination.
	// metadata input is already a trickled down value from interpreting x-amz-metadata-directive at
	// handler layer. So what we have right now is supposed to be applied on the destination object anyways.
	// So preserve it by adding "REPLACE" directive to save all the metadata set by CopyObject API.
	srcInfo.UserDefined["x-amz-metadata-directive"] = "REPLACE"
	srcInfo.UserDefined["x-amz-copy-source-if-match"] = srcInfo.ETag
	if _, err = l.Client.CopyObject(srcBucket, srcObject, dstBucket, dstObject, srcInfo.UserDefined); err != nil {
		return objInfo, minio.ErrorRespToObjectError(err, srcBucket, srcObject)
	}
	return l.GetObjectInfo(ctx, dstBucket, dstObject, dstOpts)
}

// DeleteObject deletes a blob in bucket
func (l *s3Objects) DeleteObject(ctx context.Context, bucket string, object string) error {
	err := l.Client.RemoveObject(bucket, object)
	if err != nil {
		return minio.ErrorRespToObjectError(err, bucket, object)
	}

	return nil
}

// ListMultipartUploads lists all multipart uploads.
func (l *s3Objects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, e error) {
	result, err := l.Client.ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		return lmi, err
	}

	return minio.FromMinioClientListMultipartsInfo(result), nil
}

// NewMultipartUpload upload object in multiple parts
func (l *s3Objects) NewMultipartUpload(ctx context.Context, bucket string, object string, metadata map[string]string, o minio.ObjectOptions) (uploadID string, err error) {
	// Create PutObject options
	opts := miniogo.PutObjectOptions{UserMetadata: metadata, ServerSideEncryption: o.ServerSideEncryption}
	uploadID, err = l.Client.NewMultipartUpload(bucket, object, opts)
	if err != nil {
		return uploadID, minio.ErrorRespToObjectError(err, bucket, object)
	}
	return uploadID, nil
}

// PutObjectPart puts a part of object in bucket
func (l *s3Objects) PutObjectPart(ctx context.Context, bucket string, object string, uploadID string, partID int, data *hash.Reader, opts minio.ObjectOptions) (pi minio.PartInfo, e error) {
	info, err := l.Client.PutObjectPart(bucket, object, uploadID, partID, data, data.Size(), data.MD5Base64String(), data.SHA256HexString(), opts.ServerSideEncryption)
	if err != nil {
		return pi, minio.ErrorRespToObjectError(err, bucket, object)
	}

	return minio.FromMinioClientObjectPart(info), nil
}

// CopyObjectPart creates a part in a multipart upload by copying
// existing object or a part of it.
func (l *s3Objects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string,
	partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (p minio.PartInfo, err error) {

	srcInfo.UserDefined = map[string]string{
		"x-amz-copy-source-if-match": srcInfo.ETag,
	}
	completePart, err := l.Client.CopyObjectPart(srcBucket, srcObject, destBucket, destObject,
		uploadID, partID, startOffset, length, srcInfo.UserDefined)
	if err != nil {
		return p, minio.ErrorRespToObjectError(err, srcBucket, srcObject)
	}
	p.PartNumber = completePart.PartNumber
	p.ETag = completePart.ETag
	return p, nil
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (l *s3Objects) ListObjectParts(ctx context.Context, bucket string, object string, uploadID string, partNumberMarker int, maxParts int) (lpi minio.ListPartsInfo, e error) {
	result, err := l.Client.ListObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
	if err != nil {
		return lpi, err
	}

	return minio.FromMinioClientListPartsInfo(result), nil
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (l *s3Objects) AbortMultipartUpload(ctx context.Context, bucket string, object string, uploadID string) error {
	err := l.Client.AbortMultipartUpload(bucket, object, uploadID)
	return minio.ErrorRespToObjectError(err, bucket, object)
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (l *s3Objects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, uploadedParts []minio.CompletePart) (oi minio.ObjectInfo, e error) {
	err := l.Client.CompleteMultipartUpload(bucket, object, uploadID, minio.ToMinioClientCompleteParts(uploadedParts))
	if err != nil {
		return oi, minio.ErrorRespToObjectError(err, bucket, object)
	}

	return l.GetObjectInfo(ctx, bucket, object, minio.ObjectOptions{})
}

// SetBucketPolicy sets policy on bucket
func (l *s3Objects) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	data, err := json.Marshal(bucketPolicy)
	if err != nil {
		// This should not happen.
		logger.LogIf(ctx, err)
		return minio.ErrorRespToObjectError(err, bucket)
	}

	if err := l.Client.SetBucketPolicy(bucket, string(data)); err != nil {
		return minio.ErrorRespToObjectError(err, bucket)
	}

	return nil
}

// GetBucketPolicy will get policy on bucket
func (l *s3Objects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	data, err := l.Client.GetBucketPolicy(bucket)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err, bucket)
	}

	bucketPolicy, err := policy.ParseConfig(strings.NewReader(data), bucket)
	return bucketPolicy, minio.ErrorRespToObjectError(err, bucket)
}

// DeleteBucketPolicy deletes all policies on bucket
func (l *s3Objects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	if err := l.Client.SetBucketPolicy(bucket, ""); err != nil {
		return minio.ErrorRespToObjectError(err, bucket, "")
	}
	return nil
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (l *s3Objects) IsCompressionSupported() bool {
	return false
}
