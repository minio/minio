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

package b2

import (
	"context"
	"crypto/sha1"
	"fmt"
	"hash"
	"io"
	"io/ioutil"

	"strings"
	"sync"
	"time"

	b2 "github.com/minio/blazer/base"
	"github.com/minio/cli"
	miniogopolicy "github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	h2 "github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/policy/condition"

	minio "github.com/minio/minio/cmd"
)

// Supported bucket types by B2 backend.
const (
	bucketTypePrivate  = "allPrivate"
	bucketTypeReadOnly = "allPublic"
	b2Backend          = "b2"
)

func init() {
	const b2GatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}}
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: B2 account id.
     MINIO_SECRET_KEY: B2 application key.

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
  1. Start minio gateway server for B2 backend.
      $ export MINIO_ACCESS_KEY=accountID
      $ export MINIO_SECRET_KEY=applicationKey
      $ {{.HelpName}}

  2. Start minio gateway server for B2 backend with edge caching enabled.
      $ export MINIO_ACCESS_KEY=accountID
      $ export MINIO_SECRET_KEY=applicationKey
      $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
      $ export MINIO_CACHE_EXCLUDE="bucket1/*;*.png"
      $ export MINIO_CACHE_EXPIRY=40
      $ {{.HelpName}}
`
	minio.RegisterGatewayCommand(cli.Command{
		Name:               b2Backend,
		Usage:              "Backblaze B2.",
		Action:             b2GatewayMain,
		CustomHelpTemplate: b2GatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway b2' command line.
func b2GatewayMain(ctx *cli.Context) {
	minio.StartGateway(ctx, &B2{})
}

// B2 implements Minio Gateway
type B2 struct{}

// Name implements Gateway interface.
func (g *B2) Name() string {
	return b2Backend
}

// NewGatewayLayer returns b2 gateway layer, implements ObjectLayer interface to
// talk to B2 remote backend.
func (g *B2) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	ctx := context.Background()
	client, err := b2.AuthorizeAccount(ctx, creds.AccessKey, creds.SecretKey, b2.Transport(minio.NewCustomHTTPTransport()))
	if err != nil {
		return nil, err
	}

	return &b2Objects{
		creds:    creds,
		b2Client: client,
		ctx:      ctx,
	}, nil
}

// Production - Ready for production use.
func (g *B2) Production() bool {
	return true
}

// b2Object implements gateway for Minio and BackBlaze B2 compatible object storage servers.
type b2Objects struct {
	minio.GatewayUnsupported
	mu       sync.Mutex
	creds    auth.Credentials
	b2Client *b2.B2
	ctx      context.Context
}

// Convert B2 errors to minio object layer errors.
func b2ToObjectError(err error, params ...string) error {
	if err == nil {
		return nil
	}
	bucket := ""
	object := ""
	uploadID := ""
	if len(params) >= 1 {
		bucket = params[0]
	}
	if len(params) == 2 {
		object = params[1]
	}
	if len(params) == 3 {
		uploadID = params[2]
	}

	// Following code is a non-exhaustive check to convert
	// B2 errors into S3 compatible errors.
	//
	// For a more complete information - https://www.backblaze.com/b2/docs/
	statusCode, code, msg := b2.Code(err)
	if statusCode == 0 {
		// We don't interpret non B2 errors. B2 errors have statusCode
		// to help us convert them to S3 object errors.
		return err
	}

	switch code {
	case "duplicate_bucket_name":
		err = minio.BucketAlreadyOwnedByYou{Bucket: bucket}
	case "bad_request":
		if object != "" {
			err = minio.ObjectNameInvalid{
				Bucket: bucket,
				Object: object,
			}
		} else if bucket != "" {
			err = minio.BucketNotFound{Bucket: bucket}
		}
	case "bad_json":
		if object != "" {
			err = minio.ObjectNameInvalid{
				Bucket: bucket,
				Object: object,
			}
		} else if bucket != "" {
			err = minio.BucketNameInvalid{Bucket: bucket}
		}
	case "bad_bucket_id":
		err = minio.BucketNotFound{Bucket: bucket}
	case "file_not_present", "not_found":
		err = minio.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	case "cannot_delete_non_empty_bucket":
		err = minio.BucketNotEmpty{Bucket: bucket}
	}

	// Special interpretation like this is required for Multipart sessions.
	if strings.Contains(msg, "No active upload for") && uploadID != "" {
		err = minio.InvalidUploadID{UploadID: uploadID}
	}

	return err
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *b2Objects) Shutdown(ctx context.Context) error {
	// TODO
	return nil
}

// StorageInfo is not relevant to B2 backend.
func (l *b2Objects) StorageInfo(ctx context.Context) (si minio.StorageInfo) {
	return si
}

// MakeBucket creates a new container on B2 backend.
func (l *b2Objects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	// location is ignored for B2 backend.

	// All buckets are set to private by default.
	_, err := l.b2Client.CreateBucket(l.ctx, bucket, bucketTypePrivate, nil, nil)
	logger.LogIf(ctx, err)
	return b2ToObjectError(err, bucket)
}

func (l *b2Objects) reAuthorizeAccount(ctx context.Context) error {
	client, err := b2.AuthorizeAccount(l.ctx, l.creds.AccessKey, l.creds.SecretKey, b2.Transport(minio.NewCustomHTTPTransport()))
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.b2Client.Update(client)
	l.mu.Unlock()
	return nil
}

// listBuckets is a wrapper similar to ListBuckets, which re-authorizes
// the account and updates the B2 client safely. Once successfully
// authorized performs the call again and returns list of buckets.
// For any errors which are not actionable we return an error.
func (l *b2Objects) listBuckets(ctx context.Context, err error) ([]*b2.Bucket, error) {
	if err != nil {
		if b2.Action(err) != b2.ReAuthenticate {
			return nil, err
		}
		if rerr := l.reAuthorizeAccount(ctx); rerr != nil {
			return nil, rerr
		}
	}
	bktList, lerr := l.b2Client.ListBuckets(l.ctx)
	if lerr != nil {
		return l.listBuckets(ctx, lerr)
	}
	return bktList, nil
}

// Bucket - is a helper which provides a *Bucket instance
// for performing an API operation. B2 API doesn't
// provide a direct way to access the bucket so we need
// to employ following technique.
func (l *b2Objects) Bucket(ctx context.Context, bucket string) (*b2.Bucket, error) {
	bktList, err := l.listBuckets(ctx, nil)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, b2ToObjectError(err, bucket)
	}
	for _, bkt := range bktList {
		if bkt.Name == bucket {
			return bkt, nil
		}
	}
	logger.LogIf(ctx, minio.BucketNotFound{Bucket: bucket})
	return nil, minio.BucketNotFound{Bucket: bucket}
}

// GetBucketInfo gets bucket metadata..
func (l *b2Objects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	if _, err = l.Bucket(ctx, bucket); err != nil {
		return bi, err
	}
	return minio.BucketInfo{
		Name:    bucket,
		Created: time.Unix(0, 0),
	}, nil
}

// ListBuckets lists all B2 buckets
func (l *b2Objects) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	bktList, err := l.listBuckets(ctx, nil)
	if err != nil {
		return nil, err
	}
	var bktInfo []minio.BucketInfo
	for _, bkt := range bktList {
		bktInfo = append(bktInfo, minio.BucketInfo{
			Name:    bkt.Name,
			Created: time.Unix(0, 0),
		})
	}
	return bktInfo, nil
}

// DeleteBucket deletes a bucket on B2
func (l *b2Objects) DeleteBucket(ctx context.Context, bucket string) error {
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return err
	}
	err = bkt.DeleteBucket(l.ctx)
	logger.LogIf(ctx, err)
	return b2ToObjectError(err, bucket)
}

// ListObjects lists all objects in B2 bucket filtered by prefix, returns upto at max 1000 entries at a time.
func (l *b2Objects) ListObjects(ctx context.Context, bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return loi, err
	}
	files, next, lerr := bkt.ListFileNames(l.ctx, maxKeys, marker, prefix, delimiter)
	if lerr != nil {
		logger.LogIf(ctx, lerr)
		return loi, b2ToObjectError(lerr, bucket)
	}
	loi.IsTruncated = next != ""
	loi.NextMarker = next
	for _, file := range files {
		switch file.Status {
		case "folder":
			loi.Prefixes = append(loi.Prefixes, file.Name)
		case "upload":
			loi.Objects = append(loi.Objects, minio.ObjectInfo{
				Bucket:      bucket,
				Name:        file.Name,
				ModTime:     file.Timestamp,
				Size:        file.Size,
				ETag:        minio.ToS3ETag(file.Info.ID),
				ContentType: file.Info.ContentType,
				UserDefined: file.Info.Info,
			})
		}
	}
	return loi, nil
}

// ListObjectsV2 lists all objects in B2 bucket filtered by prefix, returns upto max 1000 entries at a time.
func (l *b2Objects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	// fetchOwner, startAfter are not supported and unused.
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return loi, err
	}
	files, next, lerr := bkt.ListFileNames(l.ctx, maxKeys, continuationToken, prefix, delimiter)
	if lerr != nil {
		logger.LogIf(ctx, lerr)
		return loi, b2ToObjectError(lerr, bucket)
	}
	loi.IsTruncated = next != ""
	loi.ContinuationToken = continuationToken
	loi.NextContinuationToken = next
	for _, file := range files {
		switch file.Status {
		case "folder":
			loi.Prefixes = append(loi.Prefixes, file.Name)
		case "upload":
			loi.Objects = append(loi.Objects, minio.ObjectInfo{
				Bucket:      bucket,
				Name:        file.Name,
				ModTime:     file.Timestamp,
				Size:        file.Size,
				ETag:        minio.ToS3ETag(file.Info.ID),
				ContentType: file.Info.ContentType,
				UserDefined: file.Info.Info,
			})
		}
	}
	return loi, nil
}

// GetObject reads an object from B2. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *b2Objects) GetObject(ctx context.Context, bucket string, object string, startOffset int64, length int64, writer io.Writer, etag string) error {
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return err
	}
	reader, err := bkt.DownloadFileByName(l.ctx, object, startOffset, length)
	if err != nil {
		logger.LogIf(ctx, err)
		return b2ToObjectError(err, bucket, object)
	}
	defer reader.Close()
	_, err = io.Copy(writer, reader)
	logger.LogIf(ctx, err)
	return b2ToObjectError(err, bucket, object)
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (l *b2Objects) GetObjectInfo(ctx context.Context, bucket string, object string) (objInfo minio.ObjectInfo, err error) {
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return objInfo, err
	}
	f, err := bkt.DownloadFileByName(l.ctx, object, 0, 1)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, b2ToObjectError(err, bucket, object)
	}
	f.Close()
	fi, err := bkt.File(f.ID, object).GetFileInfo(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, b2ToObjectError(err, bucket, object)
	}
	return minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ETag:        minio.ToS3ETag(fi.ID),
		Size:        fi.Size,
		ModTime:     fi.Timestamp,
		ContentType: fi.ContentType,
		UserDefined: fi.Info,
	}, nil
}

// In B2 - You must always include the X-Bz-Content-Sha1 header with
// your upload request. The value you provide can be:
// (1) the 40-character hex checksum of the file,
// (2) the string hex_digits_at_end, or
// (3) the string do_not_verify.
// For more reference - https://www.backblaze.com/b2/docs/uploading.html
//
// In our case we are going to use (2) option
const sha1AtEOF = "hex_digits_at_end"

// With the second option mentioned above, you append the 40-character hex sha1
// to the end of the request body, immediately after the contents of the file
// being uploaded. Note that the content length is the size of the file plus 40
// of the original size of the reader.
//
// newB2Reader implements a B2 compatible reader by wrapping the hash.Reader into
// a new io.Reader which will emit out the sha1 hex digits at io.EOF.
// It also means that your overall content size is now original size + 40 bytes.
// Additionally this reader also verifies Hash encapsulated inside hash.Reader
// at io.EOF if the verification failed we return an error and do not send
// the content to server.
func newB2Reader(r *h2.Reader, size int64) *Reader {
	return &Reader{
		r:        r,
		size:     size,
		sha1Hash: sha1.New(),
	}
}

// Reader - is a Reader wraps the hash.Reader which will emit out the sha1
// hex digits at io.EOF. It also means that your overall content size is
// now original size + 40 bytes. Additionally this reader also verifies
// Hash encapsulated inside hash.Reader at io.EOF if the verification
// failed we return an error and do not send the content to server.
type Reader struct {
	r        *h2.Reader
	size     int64
	sha1Hash hash.Hash

	isEOF bool
	buf   *strings.Reader
}

// Size - Returns the total size of Reader.
func (nb *Reader) Size() int64 { return nb.size + 40 }
func (nb *Reader) Read(p []byte) (int, error) {
	if nb.isEOF {
		return nb.buf.Read(p)
	}
	// Read into hash to update the on going checksum.
	n, err := io.TeeReader(nb.r, nb.sha1Hash).Read(p)
	if err == io.EOF {
		// Stream is not corrupted on this end
		// now fill in the last 40 bytes of sha1 hex
		// so that the server can verify the stream on
		// their end.
		err = nil
		nb.isEOF = true
		nb.buf = strings.NewReader(fmt.Sprintf("%x", nb.sha1Hash.Sum(nil)))
	}
	return n, err
}

// PutObject uploads the single upload to B2 backend by using *b2_upload_file* API, uploads upto 5GiB.
func (l *b2Objects) PutObject(ctx context.Context, bucket string, object string, data *h2.Reader, metadata map[string]string) (objInfo minio.ObjectInfo, err error) {
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return objInfo, err
	}
	contentType := metadata["content-type"]
	delete(metadata, "content-type")

	var u *b2.URL
	u, err = bkt.GetUploadURL(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, b2ToObjectError(err, bucket, object)
	}

	hr := newB2Reader(data, data.Size())
	var f *b2.File
	f, err = u.UploadFile(l.ctx, hr, int(hr.Size()), object, contentType, sha1AtEOF, metadata)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, b2ToObjectError(err, bucket, object)
	}

	var fi *b2.FileInfo
	fi, err = f.GetFileInfo(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, b2ToObjectError(err, bucket, object)
	}

	return minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ETag:        minio.ToS3ETag(fi.ID),
		Size:        fi.Size,
		ModTime:     fi.Timestamp,
		ContentType: fi.ContentType,
		UserDefined: fi.Info,
	}, nil
}

// DeleteObject deletes a blob in bucket
func (l *b2Objects) DeleteObject(ctx context.Context, bucket string, object string) error {
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return err
	}
	reader, err := bkt.DownloadFileByName(l.ctx, object, 0, 1)
	if err != nil {
		logger.LogIf(ctx, err)
		return b2ToObjectError(err, bucket, object)
	}
	io.Copy(ioutil.Discard, reader)
	reader.Close()
	err = bkt.File(reader.ID, object).DeleteFileVersion(l.ctx)
	logger.LogIf(ctx, err)
	return b2ToObjectError(err, bucket, object)
}

// ListMultipartUploads lists all multipart uploads.
func (l *b2Objects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string,
	delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, err error) {
	// keyMarker, prefix, delimiter are all ignored, Backblaze B2 doesn't support any
	// of these parameters only equivalent parameter is uploadIDMarker.
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return lmi, err
	}
	// The maximum number of files to return from this call.
	// The default value is 100, and the maximum allowed is 100.
	if maxUploads > 100 {
		maxUploads = 100
	}
	largeFiles, nextMarker, err := bkt.ListUnfinishedLargeFiles(l.ctx, uploadIDMarker, maxUploads)
	if err != nil {
		logger.LogIf(ctx, err)
		return lmi, b2ToObjectError(err, bucket)
	}
	lmi = minio.ListMultipartsInfo{
		MaxUploads: maxUploads,
	}
	if nextMarker != "" {
		lmi.IsTruncated = true
		lmi.NextUploadIDMarker = nextMarker
	}
	for _, largeFile := range largeFiles {
		lmi.Uploads = append(lmi.Uploads, minio.MultipartInfo{
			Object:    largeFile.Name,
			UploadID:  largeFile.ID,
			Initiated: largeFile.Timestamp,
		})
	}
	return lmi, nil
}

// NewMultipartUpload upload object in multiple parts, uses B2's LargeFile upload API.
// Large files can range in size from 5MB to 10TB.
// Each large file must consist of at least 2 parts, and all of the parts except the
// last one must be at least 5MB in size. The last part must contain at least one byte.
// For more information - https://www.backblaze.com/b2/docs/large_files.html
func (l *b2Objects) NewMultipartUpload(ctx context.Context, bucket string, object string, metadata map[string]string) (string, error) {
	var uploadID string
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return uploadID, err
	}

	contentType := metadata["content-type"]
	delete(metadata, "content-type")
	lf, err := bkt.StartLargeFile(l.ctx, object, contentType, metadata)
	if err != nil {
		logger.LogIf(ctx, err)
		return uploadID, b2ToObjectError(err, bucket, object)
	}

	return lf.ID, nil
}

// PutObjectPart puts a part of object in bucket, uses B2's LargeFile upload API.
func (l *b2Objects) PutObjectPart(ctx context.Context, bucket string, object string, uploadID string, partID int, data *h2.Reader) (pi minio.PartInfo, err error) {
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return pi, err
	}

	fc, err := bkt.File(uploadID, object).CompileParts(0, nil).GetUploadPartURL(l.ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return pi, b2ToObjectError(err, bucket, object, uploadID)
	}

	hr := newB2Reader(data, data.Size())
	sha1, err := fc.UploadPart(l.ctx, hr, sha1AtEOF, int(hr.Size()), partID)
	if err != nil {
		logger.LogIf(ctx, err)
		return pi, b2ToObjectError(err, bucket, object, uploadID)
	}

	return minio.PartInfo{
		PartNumber:   partID,
		LastModified: minio.UTCNow(),
		ETag:         minio.ToS3ETag(sha1),
		Size:         data.Size(),
	}, nil
}

// ListObjectParts returns all object parts for specified object in specified bucket, uses B2's LargeFile upload API.
func (l *b2Objects) ListObjectParts(ctx context.Context, bucket string, object string, uploadID string, partNumberMarker int, maxParts int) (lpi minio.ListPartsInfo, err error) {
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return lpi, err
	}
	lpi = minio.ListPartsInfo{
		Bucket:           bucket,
		Object:           object,
		UploadID:         uploadID,
		MaxParts:         maxParts,
		PartNumberMarker: partNumberMarker,
	}
	// startPartNumber must be in the range 1 - 10000 for B2.
	partNumberMarker++
	partsList, next, err := bkt.File(uploadID, object).ListParts(l.ctx, partNumberMarker, maxParts)
	if err != nil {
		logger.LogIf(ctx, err)
		return lpi, b2ToObjectError(err, bucket, object, uploadID)
	}
	if next != 0 {
		lpi.IsTruncated = true
		lpi.NextPartNumberMarker = next
	}
	for _, part := range partsList {
		lpi.Parts = append(lpi.Parts, minio.PartInfo{
			PartNumber: part.Number,
			ETag:       minio.ToS3ETag(part.SHA1),
			Size:       part.Size,
		})
	}
	return lpi, nil
}

// AbortMultipartUpload aborts a on going multipart upload, uses B2's LargeFile upload API.
func (l *b2Objects) AbortMultipartUpload(ctx context.Context, bucket string, object string, uploadID string) error {
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return err
	}
	err = bkt.File(uploadID, object).CompileParts(0, nil).CancelLargeFile(l.ctx)
	logger.LogIf(ctx, err)
	return b2ToObjectError(err, bucket, object, uploadID)
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object, uses B2's LargeFile upload API.
func (l *b2Objects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, uploadedParts []minio.CompletePart) (oi minio.ObjectInfo, err error) {
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return oi, err
	}
	hashes := make(map[int]string)
	for i, uploadedPart := range uploadedParts {
		// B2 requires contigous part numbers starting with 1, they do not support
		// hand picking part numbers, we return an S3 compatible error instead.
		if i+1 != uploadedPart.PartNumber {
			logger.LogIf(ctx, minio.InvalidPart{})
			return oi, b2ToObjectError(minio.InvalidPart{}, bucket, object, uploadID)
		}

		// Trim "-1" suffix in ETag as PutObjectPart() treats B2 returned SHA1 as ETag.
		hashes[uploadedPart.PartNumber] = strings.TrimSuffix(uploadedPart.ETag, "-1")
	}

	if _, err = bkt.File(uploadID, object).CompileParts(0, hashes).FinishLargeFile(l.ctx); err != nil {
		logger.LogIf(ctx, err)
		return oi, b2ToObjectError(err, bucket, object, uploadID)
	}

	return l.GetObjectInfo(ctx, bucket, object)
}

// SetBucketPolicy - B2 supports 2 types of bucket policies:
// bucketType.AllPublic - bucketTypeReadOnly means that anybody can download the files is the bucket;
// bucketType.AllPrivate - bucketTypePrivate means that you need an authorization token to download them.
// Default is AllPrivate for all buckets.
func (l *b2Objects) SetBucketPolicy(ctx context.Context, bucket string, bucketPolicy *policy.Policy) error {
	policyInfo, err := minio.PolicyToBucketAccessPolicy(bucketPolicy)
	if err != nil {
		// This should not happen.
		return b2ToObjectError(err, bucket)
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
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return err
	}
	bkt.Type = bucketTypeReadOnly
	_, err = bkt.Update(l.ctx)
	logger.LogIf(ctx, err)
	return b2ToObjectError(err)
}

// GetBucketPolicy, returns the current bucketType from B2 backend and convert
// it into S3 compatible bucket policy info.
func (l *b2Objects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return nil, err
	}

	// bkt.Type can also be snapshot, but it is only allowed through B2 browser console,
	// just return back as policy not found for all cases.
	// CreateBucket always sets the value to allPrivate by default.
	if bkt.Type != bucketTypeReadOnly {
		logger.LogIf(ctx, minio.BucketPolicyNotFound{Bucket: bucket})
		return nil, minio.BucketPolicyNotFound{Bucket: bucket}
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

// DeleteBucketPolicy - resets the bucketType of bucket on B2 to 'allPrivate'.
func (l *b2Objects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	bkt, err := l.Bucket(ctx, bucket)
	if err != nil {
		return err
	}
	bkt.Type = bucketTypePrivate
	_, err = bkt.Update(l.ctx)
	logger.LogIf(ctx, err)
	return b2ToObjectError(err)
}
