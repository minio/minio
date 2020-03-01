/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package hbase

import (
	"context"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v6/pkg/s3utils"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/env"
)

const (
	hbaseBackend = "hbase"

	hbaseSeparator = minio.SlashSeparator
)

func init() {
	hbaseGatewayTemplate := `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} HBase Zookeeper Addresses [HBase Zookeeper Addresses...]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
HBase-NAMENODE:
  HBase namenode URI

EXAMPLES:
  1. Start minio gateway server for HBase backend
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.HelpName}} zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181

  2. Start minio gateway server for HBase with edge caching enabled
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_DRIVES{{.AssignmentOperator}}"/mnt/drive1,/mnt/drive2,/mnt/drive3,/mnt/drive4"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_EXCLUDE{{.AssignmentOperator}}"bucket1/*,*.png"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_QUOTA{{.AssignmentOperator}}90 
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_AFTER{{.AssignmentOperator}}3
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_LOW{{.AssignmentOperator}}75
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_HIGH{{.AssignmentOperator}}85
     {{.Prompt}} {{.HelpName}} zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181`

	_ = minio.RegisterGatewayCommand(cli.Command{
		Name:               hbaseBackend,
		Usage:              "HBase",
		Action:             hbaseGatewayMain,
		CustomHelpTemplate: hbaseGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway hbase' command line.
func hbaseGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, hbaseBackend, 1)
	}

	minio.StartGateway(ctx, &HBase{args: ctx.Args()})
}

// HBase implements Gateway.
type HBase struct {
	args []string
}

// Name implements Gateway interface.
func (h *HBase) Name() string {
	return hbaseBackend
}

// NewGatewayLayer returns hbase gatewaylayer.
func (h *HBase) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	zkAddr := env.Get("HBASE_ZK_ADDR", "localhost")
	client := NewHBaseClient(zkAddr)

	// TODO: set a test key to verify if hbase is connectable.

	return &ObjectLayer{client: client, listPool: minio.NewTreeWalkPool(time.Minute * 30)}, nil
}

// Production - hbase gateway is production ready.
func (h *HBase) Production() bool {
	return true
}

// ObjectLayer implements gateway for Minio and S3 compatible object storage servers.
type ObjectLayer struct {
	minio.GatewayUnsupported
	client   Client
	listPool *minio.TreeWalkPool
}

func (o *ObjectLayer) Shutdown(ctx context.Context) error {
	return o.client.Close(ctx)
}

func (o *ObjectLayer) StorageInfo(ctx context.Context, _ bool) minio.StorageInfo {
	return minio.StorageInfo{}
}

func (o *ObjectLayer) DeleteBucket(ctx context.Context, bucket string) error {
	err := o.client.DeleteBucket(ctx, bucket)
	if err != nil {
		return errors.Wrapf(err, "delete bucket '%v' failed", bucket)
	}
	return nil
}

func (o *ObjectLayer) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	err := o.client.CreateBucket(ctx, bucket)
	if err != nil {
		return errors.Wrapf(err, "create bucket '%v' failed", bucket)
	}
	return nil
}

func (o *ObjectLayer) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	fi, err := o.client.GetBucketInfo(ctx, bucket)
	if err != nil {
		return bi, errors.Wrapf(err, "get bucket info '%v' failed", bucket)
	}

	// As Client.GetBucketInfo() doesn't carry anything other than ModTime(), use ModTime() as CreatedTime.
	return minio.BucketInfo{
		Name:    bucket,
		Created: fi.ModTime(),
	}, nil
}

func (o *ObjectLayer) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	entries, err := o.client.ListBuckets(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "list buckets failed")
	}

	for _, entry := range entries {
		// Ignore all reserved bucket names and invalid bucket names.
		if isReservedOrInvalidBucket(entry.Name(), false) {
			continue
		}
		buckets = append(buckets, minio.BucketInfo{
			Name: entry.Name(),
			// As Client.GetBucketInfo() doesnt carry CreatedTime, use ModTime() as CreatedTime.
			Created: entry.ModTime(),
		})
	}

	// Sort bucket infos by bucket name.
	sort.Sort(byBucketName(buckets))
	return buckets, nil
}

func (o *ObjectLayer) listDirFactory(ctx context.Context) minio.ListDirFunc {
	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (entries []string) {
		fis, err := o.client.ListObjects(ctx, bucket, prefixDir)
		if err != nil {
			logger.LogIf(context.Background(), err)
			return
		}
		for _, fi := range fis {
			if fi.IsDir() {
				entries = append(entries, fi.Name()+hbaseSeparator)
			} else {
				entries = append(entries, fi.Name())
			}
		}
		return minio.FilterMatchingPrefix(entries, prefixEntry)
	}

	// Return list factory instance.
	return listDir
}

// ListObjects lists all blobs in HBase bucket filtered by prefix.
func (o *ObjectLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	getObjectInfo := func(ctx context.Context, bucket, entry string) (minio.ObjectInfo, error) {
		fi, err := o.client.GetObjectInfo(ctx, bucket, entry)
		if err != nil {
			return minio.ObjectInfo{}, errors.Wrapf(err, "get object info '%v' in bucket '%v' failed", entry, bucket)
		}

		return minio.ObjectInfo{
			Bucket:  bucket,
			Name:    entry,
			ModTime: fi.ModTime(),
			Size:    fi.Size(),
			IsDir:   fi.IsDir(),
			AccTime: time.Now(),
		}, nil
	}

	return minio.ListObjects(ctx, o, bucket, prefix, marker, delimiter, maxKeys, o.listPool, o.listDirFactory(ctx), getObjectInfo, getObjectInfo)
}

// ListObjectsV2 lists all blobs in HBase bucket filtered by prefix
func (o *ObjectLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	// fetchOwner is not supported and unused.
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}
	resultV1, err := o.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
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

func (o *ObjectLayer) DeleteObject(ctx context.Context, bucket, object string) error {
	err := o.client.DeleteObject(ctx, bucket, object)
	if err != nil {
		return errors.Wrapf(err, "delete object '%v' in bucket '%v' failed", object, bucket)
	}
	return nil
}

func (o *ObjectLayer) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	errs := make([]error, len(objects))
	for idx, object := range objects {
		errs[idx] = o.DeleteObject(ctx, bucket, object)
	}
	return errs, nil
}

func (o *ObjectLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, _h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	objInfo, err := o.GetObjectInfo(ctx, bucket, object, opts)
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
		nerr := o.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		_ = pw.CloseWithError(nerr)
	}()

	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { _ = pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts.CheckCopyPrecondFn, pipeCloser)

}

func (o *ObjectLayer) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.ObjectInfo, error) {
	cpSrcDstSame := minio.IsStringEqual(minio.PathJoin(srcBucket, srcObject), minio.PathJoin(dstBucket, dstObject))
	if cpSrcDstSame {
		return o.GetObjectInfo(ctx, srcBucket, srcObject, minio.ObjectOptions{})
	}

	return o.PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, minio.ObjectOptions{
		ServerSideEncryption: dstOpts.ServerSideEncryption,
		UserDefined:          srcInfo.UserDefined,
	})
}

func (o *ObjectLayer) GetObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	_path := minio.PathJoin(bucket, object)
	reader, err := o.client.GetObject(ctx, bucket, object)
	if err != nil {
		return errors.Wrapf(err, "get object '%v' in bucket '%v' failed", object, bucket)
	}

	_, err = io.Copy(writer, io.NewSectionReader(reader, startOffset, length))
	if err != nil {
		return errors.Wrapf(err, "copy object '%v' from offset '%v' with length '%v' failed", _path, startOffset, length)
	}
	return nil
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (o *ObjectLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	fi, err := o.client.GetObjectInfo(ctx, bucket, object)
	if err != nil {
		return objInfo, errors.Wrapf(err, "get object info '%v' in bucket '%v' failed", object, bucket)
	}

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: time.Now(),
	}, nil
}

func (o *ObjectLayer) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	fi, err := o.client.PutObject(ctx, bucket, object, r)
	if err != nil {
		return objInfo, errors.Wrapf(err, "put object '%v' in bucket '%v' failed", object, bucket)
	}

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    r.MD5CurrentHexString(),
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: time.Now(),
	}, nil
}

func (o *ObjectLayer) NewMultipartUpload(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	uploadID = minio.MustGetUUID()
	if err = o.client.CreateMultipartObject(ctx, bucket, object, true); err != nil {
		return uploadID, errors.Wrapf(err, "create multiple part object '%v' in bucket '%v' failed", object, bucket)
	}

	return uploadID, nil
}

func (o *ObjectLayer) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int,
	startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.PartInfo, error) {
	return o.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}

func (o *ObjectLayer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	_, err = o.client.PutMultipartObjectPart(ctx, bucket, object, partID, r)
	if err != nil {
		return info, errors.Wrapf(err, "put the part '%v' of multiple part object '%v' in bucket '%v' failed", partID, object, bucket)
	}

	info.PartNumber = partID
	info.ETag = r.MD5CurrentHexString()
	info.LastModified = minio.UTCNow()
	info.Size = r.Reader.Size()

	return info, nil
}

func (o *ObjectLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	fi, err := o.client.PutMultipartObjectComplete(ctx, bucket, object)
	if err != nil {
		return objInfo, errors.Wrapf(err, "complete multiple part object '%v' in bucket '%v' failed", object, bucket)
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5 := minio.ComputeCompleteMultipartMD5(parts)

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    s3MD5,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: time.Now(),
	}, nil
}

func (o *ObjectLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	err = o.client.DeleteObject(ctx, bucket, object)
	if err != nil {
		return errors.Wrapf(err, "abort multiple part object '%v' in bucket '%v' failed", object, bucket)
	}
	return nil
}

// IsReady returns whether the layer is ready to take requests.
func (o *ObjectLayer) IsReady(_ context.Context) bool {
	return true
}
