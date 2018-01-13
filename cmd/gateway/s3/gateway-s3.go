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

package s3

import (
	"io"

	"github.com/minio/cli"
	miniogo "github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio-go/pkg/s3utils"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/hash"

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

  UPDATE:
     MINIO_UPDATE: To turn off in-place upgrades, set this value to "off".

EXAMPLES:
  1. Start minio gateway server for AWS S3 backend.
      $ export MINIO_ACCESS_KEY=accesskey
      $ export MINIO_SECRET_KEY=secretkey
      $ {{.HelpName}}

  2. Start minio gateway server for S3 backend on custom endpoint.
      $ export MINIO_ACCESS_KEY=Q3AM3UQ867SPQQA43P2F
      $ export MINIO_SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
      $ {{.HelpName}} https://play.minio.io:9000
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
	// Validate gateway arguments.
	host := ctx.Args().First()
	// Validate gateway arguments.
	minio.FatalIf(minio.ValidateGatewayArguments(ctx.GlobalString("address"), host), "Invalid argument")

	minio.StartGateway(ctx, &S3{host})
}

// S3 implements Gateway.
type S3 struct {
	host string
}

// Name implements Gateway interface.
func (g *S3) Name() string {
	return s3Backend
}

// NewGatewayLayer returns s3 gatewaylayer.
func (g *S3) NewGatewayLayer(creds auth.Credentials) (minio.GatewayLayer, error) {
	var err error
	var endpoint string
	var secure = true

	// Validate host parameters.
	if g.host != "" {
		// Override default params if the host is provided
		endpoint, secure, err = minio.ParseGatewayEndpoint(g.host)
		if err != nil {
			return nil, err
		}
	}

	// Default endpoint parameters
	if endpoint == "" {
		endpoint = "s3.amazonaws.com"
	}

	// Initialize minio client object.
	client, err := miniogo.NewCore(endpoint, creds.AccessKey, creds.SecretKey, secure)
	if err != nil {
		return nil, err
	}

	anonClient, err := miniogo.NewCore(endpoint, "", "", secure)
	if err != nil {
		return nil, err
	}
	anonClient.SetCustomTransport(minio.NewCustomHTTPTransport())

	return &s3Objects{
		Client:     client,
		anonClient: anonClient,
	}, nil
}

// Production - s3 gateway is not production ready.
func (g *S3) Production() bool {
	return false
}

// s3Objects implements gateway for Minio and S3 compatible object storage servers.
type s3Objects struct {
	minio.GatewayUnsupported
	Client     *miniogo.Core
	anonClient *miniogo.Core
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *s3Objects) Shutdown() error {
	return nil
}

// StorageInfo is not relevant to S3 backend.
func (l *s3Objects) StorageInfo() (si minio.StorageInfo) {
	return si
}

// MakeBucket creates a new container on S3 backend.
func (l *s3Objects) MakeBucketWithLocation(bucket, location string) error {
	err := l.Client.MakeBucket(bucket, location)
	if err != nil {
		return minio.ErrorRespToObjectError(errors.Trace(err), bucket)
	}
	return err
}

// GetBucketInfo gets bucket metadata..
func (l *s3Objects) GetBucketInfo(bucket string) (bi minio.BucketInfo, e error) {
	// Verify if bucket name is valid.
	// We are using a separate helper function here to validate bucket
	// names instead of IsValidBucketName() because there is a possibility
	// that certains users might have buckets which are non-DNS compliant
	// in us-east-1 and we might severely restrict them by not allowing
	// access to these buckets.
	// Ref - http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
	if s3utils.CheckValidBucketName(bucket) != nil {
		return bi, errors.Trace(minio.BucketNameInvalid{Bucket: bucket})
	}

	buckets, err := l.Client.ListBuckets()
	if err != nil {
		return bi, minio.ErrorRespToObjectError(errors.Trace(err), bucket)
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

	return bi, errors.Trace(minio.BucketNotFound{Bucket: bucket})
}

// ListBuckets lists all S3 buckets
func (l *s3Objects) ListBuckets() ([]minio.BucketInfo, error) {
	buckets, err := l.Client.ListBuckets()
	if err != nil {
		return nil, minio.ErrorRespToObjectError(errors.Trace(err))
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
func (l *s3Objects) DeleteBucket(bucket string) error {
	err := l.Client.RemoveBucket(bucket)
	if err != nil {
		return minio.ErrorRespToObjectError(errors.Trace(err), bucket)
	}
	return nil
}

// ListObjects lists all blobs in S3 bucket filtered by prefix
func (l *s3Objects) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, e error) {
	result, err := l.Client.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, minio.ErrorRespToObjectError(errors.Trace(err), bucket)
	}

	return minio.FromMinioClientListBucketResult(bucket, result), nil
}

// ListObjectsV2 lists all blobs in S3 bucket filtered by prefix
func (l *s3Objects) ListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, e error) {
	result, err := l.Client.ListObjectsV2(bucket, prefix, continuationToken, fetchOwner, delimiter, maxKeys)
	if err != nil {
		return loi, minio.ErrorRespToObjectError(errors.Trace(err), bucket)
	}

	return minio.FromMinioClientListBucketV2Result(bucket, result), nil
}

// GetObject reads an object from S3. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *s3Objects) GetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer, etag string) error {
	if length < 0 && length != -1 {
		return minio.ErrorRespToObjectError(errors.Trace(minio.InvalidRange{}), bucket, key)
	}

	opts := miniogo.GetObjectOptions{}
	if startOffset >= 0 && length >= 0 {
		if err := opts.SetRange(startOffset, startOffset+length-1); err != nil {
			return minio.ErrorRespToObjectError(errors.Trace(err), bucket, key)
		}
	}
	object, _, err := l.Client.GetObject(bucket, key, opts)
	if err != nil {
		return minio.ErrorRespToObjectError(errors.Trace(err), bucket, key)
	}
	defer object.Close()

	if _, err := io.Copy(writer, object); err != nil {
		return minio.ErrorRespToObjectError(errors.Trace(err), bucket, key)
	}
	return nil
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (l *s3Objects) GetObjectInfo(bucket string, object string) (objInfo minio.ObjectInfo, err error) {
	oi, err := l.Client.StatObject(bucket, object, miniogo.StatObjectOptions{})
	if err != nil {
		return minio.ObjectInfo{}, minio.ErrorRespToObjectError(errors.Trace(err), bucket, object)
	}

	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}

// PutObject creates a new object with the incoming data,
func (l *s3Objects) PutObject(bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo minio.ObjectInfo, err error) {
	oi, err := l.Client.PutObject(bucket, object, data, data.Size(), data.MD5Base64String(), data.SHA256HexString(), minio.ToMinioClientMetadata(metadata))
	if err != nil {
		return objInfo, minio.ErrorRespToObjectError(errors.Trace(err), bucket, object)
	}

	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}

// CopyObject copies an object from source bucket to a destination bucket.
func (l *s3Objects) CopyObject(srcBucket string, srcObject string, dstBucket string, dstObject string, metadata map[string]string, srcEtag string) (objInfo minio.ObjectInfo, err error) {
	// Set this header such that following CopyObject() always sets the right metadata on the destination.
	// metadata input is already a trickled down value from interpreting x-amz-metadata-directive at
	// handler layer. So what we have right now is supposed to be applied on the destination object anyways.
	// So preserve it by adding "REPLACE" directive to save all the metadata set by CopyObject API.
	metadata["x-amz-metadata-directive"] = "REPLACE"
	if _, err = l.Client.CopyObject(srcBucket, srcObject, dstBucket, dstObject, metadata); err != nil {
		return objInfo, minio.ErrorRespToObjectError(errors.Trace(err), srcBucket, srcObject)
	}
	return l.GetObjectInfo(dstBucket, dstObject)
}

// DeleteObject deletes a blob in bucket
func (l *s3Objects) DeleteObject(bucket string, object string) error {
	err := l.Client.RemoveObject(bucket, object)
	if err != nil {
		return minio.ErrorRespToObjectError(errors.Trace(err), bucket, object)
	}

	return nil
}

// ListMultipartUploads lists all multipart uploads.
func (l *s3Objects) ListMultipartUploads(bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, e error) {
	result, err := l.Client.ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		return lmi, err
	}

	return minio.FromMinioClientListMultipartsInfo(result), nil
}

// NewMultipartUpload upload object in multiple parts
func (l *s3Objects) NewMultipartUpload(bucket string, object string, metadata map[string]string) (uploadID string, err error) {
	// Create PutObject options
	opts := miniogo.PutObjectOptions{UserMetadata: metadata}
	uploadID, err = l.Client.NewMultipartUpload(bucket, object, opts)
	if err != nil {
		return uploadID, minio.ErrorRespToObjectError(errors.Trace(err), bucket, object)
	}
	return uploadID, nil
}

// PutObjectPart puts a part of object in bucket
func (l *s3Objects) PutObjectPart(bucket string, object string, uploadID string, partID int, data *hash.Reader) (pi minio.PartInfo, e error) {
	info, err := l.Client.PutObjectPart(bucket, object, uploadID, partID, data, data.Size(), data.MD5Base64String(), data.SHA256HexString())
	if err != nil {
		return pi, minio.ErrorRespToObjectError(errors.Trace(err), bucket, object)
	}

	return minio.FromMinioClientObjectPart(info), nil
}

// CopyObjectPart creates a part in a multipart upload by copying
// existing object or a part of it.
func (l *s3Objects) CopyObjectPart(srcBucket, srcObject, destBucket, destObject, uploadID string,
	partID int, startOffset, length int64, metadata map[string]string, srcEtag string) (p minio.PartInfo, err error) {

	completePart, err := l.Client.CopyObjectPart(srcBucket, srcObject, destBucket, destObject,
		uploadID, partID, startOffset, length, metadata)
	if err != nil {
		return p, minio.ErrorRespToObjectError(errors.Trace(err), srcBucket, srcObject)
	}
	p.PartNumber = completePart.PartNumber
	p.ETag = completePart.ETag
	return p, nil
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (l *s3Objects) ListObjectParts(bucket string, object string, uploadID string, partNumberMarker int, maxParts int) (lpi minio.ListPartsInfo, e error) {
	result, err := l.Client.ListObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
	if err != nil {
		return lpi, err
	}

	return minio.FromMinioClientListPartsInfo(result), nil
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (l *s3Objects) AbortMultipartUpload(bucket string, object string, uploadID string) error {
	err := l.Client.AbortMultipartUpload(bucket, object, uploadID)
	return minio.ErrorRespToObjectError(errors.Trace(err), bucket, object)
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (l *s3Objects) CompleteMultipartUpload(bucket string, object string, uploadID string, uploadedParts []minio.CompletePart) (oi minio.ObjectInfo, e error) {
	err := l.Client.CompleteMultipartUpload(bucket, object, uploadID, minio.ToMinioClientCompleteParts(uploadedParts))
	if err != nil {
		return oi, minio.ErrorRespToObjectError(errors.Trace(err), bucket, object)
	}

	return l.GetObjectInfo(bucket, object)
}

// SetBucketPolicies sets policy on bucket
func (l *s3Objects) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	if err := l.Client.PutBucketPolicy(bucket, policyInfo); err != nil {
		return minio.ErrorRespToObjectError(errors.Trace(err), bucket, "")
	}

	return nil
}

// GetBucketPolicies will get policy on bucket
func (l *s3Objects) GetBucketPolicies(bucket string) (policy.BucketAccessPolicy, error) {
	policyInfo, err := l.Client.GetBucketPolicy(bucket)
	if err != nil {
		return policy.BucketAccessPolicy{}, minio.ErrorRespToObjectError(errors.Trace(err), bucket, "")
	}
	return policyInfo, nil
}

// DeleteBucketPolicies deletes all policies on bucket
func (l *s3Objects) DeleteBucketPolicies(bucket string) error {
	if err := l.Client.PutBucketPolicy(bucket, policy.BucketAccessPolicy{}); err != nil {
		return minio.ErrorRespToObjectError(errors.Trace(err), bucket, "")
	}
	return nil
}
