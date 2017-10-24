package cmd

import (
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/minio/cli"
	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio/pkg/hash"
)

const (
	ossBackend = "oss"
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

EXAMPLES:
  1. Start minio gateway server for Aliyun OSS backend.
      $ export MINIO_ACCESS_KEY=accesskey
      $ export MINIO_SECRET_KEY=secretkey
      $ {{.HelpName}}

  2. Start minio gateway server for Aliyun OSS backend on custom endpoint.
      $ export MINIO_ACCESS_KEY=Q3AM3UQ867SPQQA43P2F
      $ export MINIO_SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
      $ {{.HelpName}} https://oss.example.com

`

	MustRegisterGatewayCommand(cli.Command{
		Name:               "oss",
		Usage:              "Alibaba Cloud (Aliyun) Object Storage Service (OSS).",
		Action:             ossGatewayMain,
		CustomHelpTemplate: ossGatewayTemplate,
		Flags:              append(serverFlags, globalFlags...),
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
	fatalIf(validateGatewayArguments(ctx.GlobalString("address"), host), "Invalid argument")

	startGateway(ctx, &OSSGateway{host})
}

// OSSGateway implements Gateway.
type OSSGateway struct {
	host string
}

// Name implements Gateway interface.
func (g *OSSGateway) Name() string {
	return ossBackend
}

// NewGatewayLayer implements Gateway interface and returns OSS GatewayLayer.
func (g *OSSGateway) NewGatewayLayer() (GatewayLayer, error) {
	return newOSSGatewayLayer(g.host)
}

// s3MetaToOSSOptions converts metadata meant for S3 PUT/COPY
// object into oss.Option.
//
// S3 user-metadata is translated to OSS metadata by removing the
// `X-Amz-Meta-` prefix and converted into `X-Oss-Meta-`.
//
// Header names are canonicalized as in http.Header.
func s3MetaToOSSOptions(s3Metadata map[string]string) ([]oss.Option, error) {
	opts := make([]oss.Option, 0, len(s3Metadata))
	for k, v := range s3Metadata {
		k = http.CanonicalHeaderKey(k)

		switch {
		case strings.Contains(k, "--"):
			return nil, traceError(UnsupportedMetadata{})
		case strings.HasPrefix(k, "X-Amz-Meta-"):
			opts = append(opts, oss.Meta(k[len("X-Amz-Meta-"):], v))
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
// It is the reverse of s3MetaToOSSOptions.
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

	ossErr, ok := err.(oss.ServiceError)
	if !ok {
		// We don't interpret non OSS errors. As oss errors will
		// have StatusCode to help to convert to object errors.
		return e
	}

	switch ossErr.Code {
	case "BucketAlreadyExists":
		err = BucketAlreadyOwnedByYou{}
	case "BucketNotEmpty":
		err = BucketNotEmpty{}
	case "InvalidBucketName":
		err = BucketNameInvalid{Bucket: bucket}
	case "NoSuchBucket":
		err = BucketNotFound{Bucket: bucket}
	case "NoSuchKey":
		if object != "" {
			err = ObjectNotFound{Bucket: bucket, Object: object}
		} else {
			err = BucketNotFound{Bucket: bucket}
		}
	case "InvalidObjectName":
		err = ObjectNameInvalid{}
	case "AccessDenied":
		err = PrefixAccessDenied{
			Bucket: bucket,
			Object: object,
		}
	case "NoSuchUpload":
		err = InvalidUploadID{}
	case "EntityTooSmall":
		err = PartTooSmall{}
	}

	e.e = err
	return e
}

// ossObjects implements gateway for Aliyun Object Storage Service.
type ossObjects struct {
	gatewayUnsupported
	Client     *oss.Client
	anonClient *oss.Client
}

// newOSSGatewayLayer returns OSS gatewaylayer.
func newOSSGatewayLayer(host string) (GatewayLayer, error) {
	var err error

	// Regions and endpoints
	// https://www.alibabacloud.com/help/doc-detail/31837.htm
	if host == "" {
		host = "https://oss.aliyuncs.com"
	}

	creds := serverConfig.GetCredential()
	// Initialize oss client object.
	client, err := oss.New(host, creds.AccessKey, creds.SecretKey)
	if err != nil {
		return nil, err
	}

	anonClient, err := oss.New(host, "", "")
	if err != nil {
		return nil, err
	}

	return &ossObjects{
		Client:     client,
		anonClient: anonClient,
	}, nil
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *ossObjects) Shutdown() error {
	return nil
}

// StorageInfo is not relevant to OSS backend.
func (l *ossObjects) StorageInfo() (si StorageInfo) {
	return
}

// MakeBucketWithLocation creates a new container on OSS backend.
func (l *ossObjects) MakeBucketWithLocation(bucket, location string) error {
	err := l.Client.CreateBucket(bucket)
	return ossToObjectError(traceError(err), bucket)
}

// ossGeBucketInfo gets bucket metadata.
func ossGeBucketInfo(client *oss.Client, bucket string) (bi BucketInfo, err error) {
	// Verify if bucket (container-name) is valid. here.
	// IsValidBucketName has same restrictions as container names mentioned
	// in azure documentation, so we will simply use the same function here.
	// https://www.alibabacloud.com/help/doc-detail/31827.htm#h2-bucket
	if !IsValidBucketName(bucket) {
		return bi, traceError(BucketNameInvalid{Bucket: bucket})
	}

	bgir, err := client.GetBucketInfo(bucket)
	if err != nil {
		return bi, ossToObjectError(traceError(err), bucket)
	}

	return BucketInfo{
		Name:    bgir.BucketInfo.Name,
		Created: bgir.BucketInfo.CreationDate,
	}, nil
}

// GetBucketInfo gets bucket metadata.
func (l *ossObjects) GetBucketInfo(bucket string) (bi BucketInfo, err error) {
	return ossGeBucketInfo(l.Client, bucket)
}

// ListBuckets lists all OSS buckets.
func (l *ossObjects) ListBuckets() (buckets []BucketInfo, err error) {
	marker := oss.Marker("")
	for {
		lbr, err := l.Client.ListBuckets(marker)
		if err != nil {
			return nil, ossToObjectError(traceError(err))
		}

		for _, bi := range lbr.Buckets {
			buckets = append(buckets, BucketInfo{
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
func (l *ossObjects) DeleteBucket(bucket string) error {
	err := l.Client.DeleteBucket(bucket)
	if err != nil {
		return ossToObjectError(traceError(err), bucket)
	}
	return nil
}

// fromOSSClientObjectProperties converts oss ObjectProperties to ObjectInfo.
func fromOSSClientObjectProperties(bucket string, o oss.ObjectProperties) ObjectInfo {
	// NOTE(timonwong): No Content-Type and user defined metadata.
	// https://www.alibabacloud.com/help/doc-detail/31965.htm

	return ObjectInfo{
		Bucket:  bucket,
		Name:    o.Key,
		ModTime: o.LastModified,
		Size:    o.Size,
		ETag:    canonicalizeETag(o.ETag),
	}
}

// fromOSSClientListObjectsResult converts oss ListBucketResult to ListObjectsInfo.
func fromOSSClientListObjectsResult(bucket string, lor oss.ListObjectsResult) ListObjectsInfo {
	objects := make([]ObjectInfo, len(lor.Objects))
	for i, oi := range lor.Objects {
		objects[i] = fromOSSClientObjectProperties(bucket, oi)
	}

	prefixes := make([]string, len(lor.CommonPrefixes))
	copy(prefixes, lor.CommonPrefixes)

	return ListObjectsInfo{
		IsTruncated: lor.IsTruncated,
		NextMarker:  lor.NextMarker,
		Objects:     objects,
		Prefixes:    prefixes,
	}
}

// ossListObjects lists all blobs in OSS bucket filtered by prefix.
func ossListObjects(client *oss.Client, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	buck, err := client.Bucket(bucket)
	if err != nil {
		return loi, ossToObjectError(traceError(err), bucket)
	}

	lor, err := buck.ListObjects(oss.Prefix(prefix), oss.Marker(marker), oss.Delimiter(delimiter), oss.MaxKeys(maxKeys))
	if err != nil {
		return loi, ossToObjectError(traceError(err), bucket)
	}

	return fromOSSClientListObjectsResult(bucket, lor), nil
}

// ossListObjectsV2 lists all blobs in OSS bucket filtered by prefix.
func ossListObjectsV2(client *oss.Client, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi ListObjectsV2Info, err error) {
	// fetchOwner and startAfter are not supported and unused.
	marker := continuationToken

	resultV1, err := ossListObjects(client, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, err
	}

	return ListObjectsV2Info{
		Objects:               resultV1.Objects,
		Prefixes:              resultV1.Prefixes,
		ContinuationToken:     continuationToken,
		NextContinuationToken: resultV1.NextMarker,
		IsTruncated:           resultV1.IsTruncated,
	}, nil
}

// ListObjects lists all blobs in OSS bucket filtered by prefix.
func (l *ossObjects) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, err error) {
	return ossListObjects(l.Client, bucket, prefix, marker, delimiter, maxKeys)
}

// ListObjectsV2 lists all blobs in OSS bucket filtered by prefix
func (l *ossObjects) ListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi ListObjectsV2Info, err error) {
	return ossListObjectsV2(l.Client, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
}

// ossGetObject reads an object on OSS. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func ossGetObject(client *oss.Client, bucket, key string, startOffset, length int64, writer io.Writer) error {
	if length < 0 && length != -1 {
		return ossToObjectError(traceError(errInvalidArgument), bucket, key)
	}

	bkt, err := client.Bucket(bucket)
	if err != nil {
		return ossToObjectError(traceError(err), bucket, key)
	}

	var opts []oss.Option
	if startOffset >= 0 && length >= 0 {
		opts = append(opts, oss.Range(startOffset, startOffset+length-1))
	}

	object, err := bkt.GetObject(key, opts...)
	if err != nil {
		return ossToObjectError(traceError(err), bucket, key)
	}
	defer object.Close()

	if _, err := io.Copy(writer, object); err != nil {
		return ossToObjectError(traceError(err), bucket, key)
	}
	return nil
}

// GetObject reads an object on OSS. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *ossObjects) GetObject(bucket, key string, startOffset, length int64, writer io.Writer) error {
	return ossGetObject(l.Client, bucket, key, startOffset, length, writer)
}

// ossGetObjectInfo reads object info and replies back ObjectInfo.
func ossGetObjectInfo(client *oss.Client, bucket, object string) (objInfo ObjectInfo, err error) {
	bkt, err := client.Bucket(bucket)
	if err != nil {
		return objInfo, ossToObjectError(traceError(err), bucket, object)
	}

	header, err := bkt.GetObjectDetailedMeta(object)
	if err != nil {
		return objInfo, ossToObjectError(traceError(err), bucket, object)
	}

	// Build S3 metadata from OSS metadata
	userDefined := ossHeaderToS3Meta(header)

	modTime, _ := http.ParseTime(header.Get("Last-Modified"))
	size, _ := strconv.ParseInt(header.Get("Content-Length"), 10, 64)
	return ObjectInfo{
		Bucket:          bucket,
		Name:            object,
		ModTime:         modTime,
		Size:            size,
		ETag:            canonicalizeETag(header.Get("ETag")),
		UserDefined:     userDefined,
		ContentType:     header.Get("Content-Type"),
		ContentEncoding: header.Get("Content-Encoding"),
	}, nil
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (l *ossObjects) GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	return ossGetObjectInfo(l.Client, bucket, object)
}

// ossPutObject creates a new object with the incoming data.
func ossPutObject(client *oss.Client, bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	bkt, err := client.Bucket(bucket)
	if err != nil {
		return objInfo, ossToObjectError(traceError(err), bucket, object)
	}

	// Build OSS metadata
	opts, err := s3MetaToOSSOptions(metadata)
	if err != nil {
		return objInfo, ossToObjectError(err, bucket, object)
	}

	err = bkt.PutObject(object, data, opts...)
	if err != nil {
		return objInfo, ossToObjectError(traceError(err), bucket, object)
	}

	return ossGetObjectInfo(client, bucket, object)
}

// PutObject creates a new object with the incoming data.
func (l *ossObjects) PutObject(bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	return ossPutObject(l.Client, bucket, object, data, metadata)
}

// CopyObject copies an object from source bucket to a destination bucket.
func (l *ossObjects) CopyObject(srcBucket, srcObject, dstBucket, dstObject string, metadata map[string]string) (objInfo ObjectInfo, err error) {
	bkt, err := l.Client.Bucket(srcBucket)
	if err != nil {
		return objInfo, ossToObjectError(traceError(err), srcBucket, srcObject)
	}

	// Set this header such that following CopyObject() always sets the right metadata on the destination.
	// metadata input is already a trickled down value from interpreting x-oss-metadata-directive at
	// handler layer. So what we have right now is supposed to be applied on the destination object anyways.
	// So preserve it by adding "REPLACE" directive to save all the metadata set by CopyObject API.
	if _, err = bkt.CopyObjectTo(dstBucket, dstObject, srcObject, oss.MetadataDirective(oss.MetaReplace)); err != nil {
		return objInfo, ossToObjectError(traceError(err), srcBucket, srcObject)
	}
	return l.GetObjectInfo(dstBucket, dstObject)
}

// DeleteObject deletes a blob in bucket.
func (l *ossObjects) DeleteObject(bucket, object string) error {
	bkt, err := l.Client.Bucket(bucket)
	if err != nil {
		return ossToObjectError(traceError(err), bucket, object)
	}

	err = bkt.DeleteObject(object)
	if err != nil {
		return ossToObjectError(traceError(err), bucket, object)
	}
	return nil
}

// fromOSSClientListMultipartsInfo converts oss ListMultipartUploadResult to ListMultipartsInfo
func fromOSSClientListMultipartsInfo(lmur oss.ListMultipartUploadResult) ListMultipartsInfo {
	uploads := make([]uploadMetadata, len(lmur.Uploads))
	for i, um := range lmur.Uploads {
		uploads[i] = uploadMetadata{
			Object:    um.Key,
			UploadID:  um.UploadID,
			Initiated: um.Initiated,
		}
	}

	commonPrefixes := make([]string, len(lmur.CommonPrefixes))
	copy(commonPrefixes, lmur.CommonPrefixes)

	return ListMultipartsInfo{
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
func (l *ossObjects) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (lmi ListMultipartsInfo, err error) {
	bkt, err := l.Client.Bucket(bucket)
	if err != nil {
		return lmi, ossToObjectError(traceError(err), bucket)
	}

	lmur, err := bkt.ListMultipartUploads(oss.Prefix(prefix), oss.KeyMarker(keyMarker), oss.UploadIDMarker(uploadIDMarker), oss.Delimiter(delimiter), oss.MaxUploads(maxUploads))
	if err != nil {
		return lmi, ossToObjectError(traceError(err), bucket)
	}

	return fromOSSClientListMultipartsInfo(lmur), nil
}

// NewMultipartUpload upload object in multiple parts.
func (l *ossObjects) NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error) {
	bkt, err := l.Client.Bucket(bucket)
	if err != nil {
		return uploadID, ossToObjectError(traceError(err), bucket, object)
	}

	// Build OSS metadata
	opts, err := s3MetaToOSSOptions(metadata)
	if err != nil {
		return uploadID, ossToObjectError(err, bucket, object)
	}

	lmur, err := bkt.InitiateMultipartUpload(object, opts...)
	if err != nil {
		return uploadID, ossToObjectError(traceError(err), bucket, object)
	}

	return lmur.UploadID, nil
}

// PutObjectPart puts a part of object in bucket.
func (l *ossObjects) PutObjectPart(bucket, object, uploadID string, partID int, data *hash.Reader) (pi PartInfo, err error) {
	bkt, err := l.Client.Bucket(bucket)
	if err != nil {
		return pi, ossToObjectError(traceError(err), bucket, object)
	}

	imur := oss.InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      object,
		UploadID: uploadID,
	}
	size := data.Size()
	up, err := bkt.UploadPart(imur, data, size, partID)
	if err != nil {
		return pi, ossToObjectError(traceError(err), bucket, object)
	}

	return PartInfo{
		Size: size,
		ETag: canonicalizeETag(up.ETag),
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
func fromOSSClientListPartsInfo(lupr oss.ListUploadedPartsResult, partNumberMarker int) ListPartsInfo {
	parts := make([]PartInfo, len(lupr.UploadedParts))
	for i, up := range lupr.UploadedParts {
		parts[i] = PartInfo{
			PartNumber:   up.PartNumber,
			LastModified: up.LastModified,
			ETag:         canonicalizeETag(up.ETag),
			Size:         int64(up.Size),
		}
	}

	nextPartNumberMarker, _ := strconv.Atoi(lupr.NextPartNumberMarker)
	return ListPartsInfo{
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

// ListObjectParts returns all object parts for specified object in specified bucket
func (l *ossObjects) ListObjectParts(bucket, object, uploadID string, partNumberMarker, maxParts int) (lpi ListPartsInfo, err error) {
	params := ossBuildListObjectPartsParams(uploadID, partNumberMarker, maxParts)
	resp, err := l.Client.Conn.Do("GET", bucket, object, params, nil, nil, 0, nil)
	if err != nil {
		return lpi, ossToObjectError(traceError(err), bucket, object)
	}

	defer func() {
		// always drain output (response body)
		io.CopyN(ioutil.Discard, resp.Body, 512)
		resp.Body.Close()
	}()

	var lupr oss.ListUploadedPartsResult
	err = xml.NewDecoder(resp.Body).Decode(&lupr)
	if err != nil {
		return lpi, ossToObjectError(traceError(err), bucket, object)
	}

	return fromOSSClientListPartsInfo(lupr, partNumberMarker), nil
}

// AbortMultipartUpload aborts a ongoing multipart upload.
func (l *ossObjects) AbortMultipartUpload(bucket, object, uploadID string) error {
	bkt, err := l.Client.Bucket(bucket)
	if err != nil {
		return ossToObjectError(traceError(err), bucket, object)
	}

	err = bkt.AbortMultipartUpload(oss.InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      object,
		UploadID: uploadID,
	})
	if err != nil {
		return ossToObjectError(traceError(err), bucket, object)
	}
	return nil
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object.
func (l *ossObjects) CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []completePart) (oi ObjectInfo, err error) {
	bkt, err := l.Client.Bucket(bucket)
	if err != nil {
		return oi, ossToObjectError(traceError(err), bucket, object)
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
			ETag:       up.ETag,
		}
	}
	_, err = bkt.CompleteMultipartUpload(imur, parts)
	if err != nil {
		return oi, ossToObjectError(traceError(err), bucket, object)
	}

	return l.GetObjectInfo(bucket, object)
}

// SetBucketPolicies sets policy on bucket.
// OSS supports three types of bucket policies:
// oss.ACLPublicReadWrite: readwrite in minio terminology
// oss.ACLPublicRead: readonly in minio terminology
// oss.ACLPrivate: none in minio terminology
func (l *ossObjects) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	bucketPolicies := policy.GetPolicies(policyInfo.Statements, bucket)
	if len(bucketPolicies) != 1 {
		return traceError(NotImplemented{})
	}

	prefix := bucket + "/*" // For all objects inside the bucket.
	for policyPrefix, bucketPolicy := range bucketPolicies {
		if policyPrefix != prefix {
			return traceError(NotImplemented{})
		}

		var acl oss.ACLType
		switch bucketPolicy {
		case policy.BucketPolicyNone:
			acl = oss.ACLPrivate
		case policy.BucketPolicyReadOnly:
			acl = oss.ACLPublicRead
		case policy.BucketPolicyReadWrite:
			acl = oss.ACLPublicReadWrite
		default:
			return traceError(NotImplemented{})
		}

		err := l.Client.SetBucketACL(bucket, acl)
		if err != nil {
			return ossToObjectError(traceError(err), bucket)
		}
	}

	return traceError(NotImplemented{})
}

// GetBucketPolicies will get policy on bucket.
func (l *ossObjects) GetBucketPolicies(bucket string) (policy.BucketAccessPolicy, error) {
	result, err := l.Client.GetBucketACL(bucket)
	if err != nil {
		return policy.BucketAccessPolicy{}, ossToObjectError(traceError(err))
	}

	policyInfo := policy.BucketAccessPolicy{Version: "2012-10-17"}
	switch result.ACL {
	case string(oss.ACLPrivate):
		// This is a no-op
	case string(oss.ACLPublicRead):
		policyInfo.Statements = policy.SetPolicy(policyInfo.Statements, policy.BucketPolicyReadOnly, bucket, "")
	case string(oss.ACLPublicReadWrite):
		policyInfo.Statements = policy.SetPolicy(policyInfo.Statements, policy.BucketPolicyReadWrite, bucket, "")
	default:
		return policy.BucketAccessPolicy{}, traceError(NotImplemented{})
	}

	return policyInfo, nil
}

// DeleteBucketPolicies deletes all policies on bucket.
func (l *ossObjects) DeleteBucketPolicies(bucket string) error {
	err := l.Client.SetBucketACL(bucket, oss.ACLPrivate)
	if err != nil {
		return ossToObjectError(traceError(err), bucket)
	}
	return nil
}
