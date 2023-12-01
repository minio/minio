/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package juicefs

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/fs" //nolint:gofumpt
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/minio/cli"
	"github.com/minio/madmin-go"
	minio "github.com/minio/minio/cmd"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/minio/minio-go/v7/pkg/s3utils"
)

const (
	sep        = "/"
	metaBucket = ".sys"
)

var mctx meta.Context
var logger = utils.GetLogger("juicefs")

func init() {
	const juicefsGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} META-URL
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
META-URL:
  The address of JuiceFS metadata engine, please refer to https://juicefs.com/docs/community/databases_for_metadata for more information.

EXAMPLES:
  1. Start MinIO gateway server for JuiceFS backend
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.HelpName}} redis://localhost:6379/1


For more information, please visit: https://juicefs.com/docs/community/s3_gateway
`

	selfFlags := []cli.Flag{
		&cli.StringFlag{
			Name:  "access-log",
			Usage: "path for JuiceFS access log",
		},
		&cli.BoolFlag{
			Name:  "no-banner",
			Usage: "disable MinIO startup information",
		},
		&cli.BoolFlag{
			Name:  "multi-buckets",
			Usage: "use top level of directories as buckets",
		},
		&cli.BoolFlag{
			Name:  "keep-etag",
			Usage: "keep the ETag for uploaded objects",
		},
		&cli.StringFlag{
			Name:  "umask",
			Value: "022",
			Usage: "umask for new files and directories in octal",
		},
	}

	compoundFlags := [][]cli.Flag{
		globalFlags(),
		clientFlags(),
		cacheFlags(0),
		selfFlags,
		shareInfoFlags(),
	}

	minio.RegisterGatewayCommand(cli.Command{
		Name:               minio.JuiceFSGateway,
		Usage:              fmt.Sprintf("JuiceFS Distributed File System (JuiceFS %s)", strings.TrimSuffix(version.Version(), "+unknown")),
		Action:             juicefsGatewayMain,
		CustomHelpTemplate: juicefsGatewayTemplate,
		HideHelpCommand:    true,
		Flags:              expandFlags(compoundFlags),
	})
}

func juicefsGatewayMain(ctx *cli.Context) {
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, minio.JuiceFSGateway, 1)
	}
	minio.StartGateway(ctx, &JfsObjects{ctx: ctx})
}

type Config struct {
	MultiBucket bool
	KeepEtag    bool
	Mode        uint16
	DirMode     uint16
}

type JfsObjects struct {
	ctx *cli.Context
	minio.GatewayUnsupported
	conf     *vfs.Config
	fs       *fs.FileSystem
	listPool *minio.TreeWalkPool
	gConf    *Config
}

func (n *JfsObjects) Name() string {
	return minio.JuiceFSGateway
}

func (n *JfsObjects) NewGatewayLayer(creds madmin.Credentials) (minio.ObjectLayer, error) {
	setup(n.ctx, 1)
	addr := n.ctx.Args().Get(0)
	removePassword(addr)
	m, store, conf := initForSvc(n.ctx, "s3gateway", addr)

	jfs, err := fs.NewFileSystem(conf, m, store)
	if err != nil {
		panic(fmt.Errorf("initialize failed: %s", err))
	}
	umask, err := strconv.ParseUint(n.ctx.String("umask"), 8, 16)
	if err != nil {
		logger.Fatalf("invalid umask %s: %s", n.ctx.String("umask"), err)
	}
	mctx = meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	jfsObj := &JfsObjects{fs: jfs, conf: conf, listPool: minio.NewTreeWalkPool(time.Minute * 30), gConf: &Config{MultiBucket: n.ctx.Bool("multi-buckets"), KeepEtag: n.ctx.Bool("keep-etag"), Mode: uint16(0666 &^ umask), DirMode: uint16(0777 &^ umask)}}
	go jfsObj.cleanup()
	return jfsObj, nil
}

func (n *JfsObjects) IsCompressionSupported() bool {
	return n.conf.Chunk.Compress != "" && n.conf.Chunk.Compress != "none"
}

func (n *JfsObjects) IsEncryptionSupported() bool {
	return false
}

// IsReady returns whether the layer is ready to take requests.
func (n *JfsObjects) IsReady(_ context.Context) bool {
	return true
}

func (n *JfsObjects) Shutdown(ctx context.Context) error {
	return n.fs.Close()
}

func (n *JfsObjects) StorageInfo(ctx context.Context) (info minio.StorageInfo, errors []error) {
	sinfo := minio.StorageInfo{}
	sinfo.Backend.Type = madmin.Gateway
	sinfo.Backend.GatewayOnline = true
	return sinfo, nil
}

func jfsToObjectErr(ctx context.Context, err error, params ...string) error {
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

	if eno, ok := err.(syscall.Errno); !ok {
		logger.Errorf("error: %s bucket: %s, object: %s, uploadID: %s", err, bucket, object, uploadID)
		return err
	} else if eno == 0 {
		return nil
	}

	switch {
	case fs.IsNotExist(err):
		if uploadID != "" {
			return minio.InvalidUploadID{
				UploadID: uploadID,
			}
		}
		if object != "" {
			return minio.ObjectNotFound{Bucket: bucket, Object: object}
		}
		return minio.BucketNotFound{Bucket: bucket}
	case fs.IsExist(err):
		if object != "" {
			return minio.PrefixAccessDenied{Bucket: bucket, Object: object}
		}
		return minio.BucketAlreadyOwnedByYou{Bucket: bucket}
	case fs.IsNotEmpty(err):
		if object != "" {
			return minio.PrefixAccessDenied{Bucket: bucket, Object: object}
		}
		return minio.BucketNotEmpty{Bucket: bucket}
	default:
		logger.Errorf("other error: %s bucket: %s, object: %s, uploadID: %s", err, bucket, object, uploadID)
		return err
	}
}

// isValidBucketName verifies whether a bucket name is valid.
func (n *JfsObjects) isValidBucketName(bucket string) bool {
	if !n.gConf.MultiBucket && bucket != n.conf.Format.Name {
		return false
	}
	return s3utils.CheckValidBucketNameStrict(bucket) == nil
}

func (n *JfsObjects) path(p ...string) string {
	if len(p) > 0 && p[0] == n.conf.Format.Name {
		p = p[1:]
	}
	return sep + minio.PathJoin(p...)
}

func (n *JfsObjects) tpath(p ...string) string {
	return sep + metaBucket + n.path(p...)
}

func (n *JfsObjects) upath(bucket, uploadID string) string {
	return n.tpath(bucket, "uploads", uploadID)
}

func (n *JfsObjects) ppath(bucket, uploadID, part string) string {
	return n.tpath(bucket, "uploads", uploadID, part)
}

func (n *JfsObjects) DeleteBucket(ctx context.Context, bucket string, opts minio.DeleteBucketOptions) error {
	if !n.isValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	if !n.gConf.MultiBucket {
		return minio.BucketNotEmpty{Bucket: bucket}
	}
	eno := n.fs.Delete(mctx, n.path(bucket))
	return jfsToObjectErr(ctx, eno, bucket)
}

func (n *JfsObjects) MakeBucketWithLocation(ctx context.Context, bucket string, options minio.BucketOptions) error {
	if !n.isValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	if !n.gConf.MultiBucket {
		return nil
	}
	eno := n.fs.Mkdir(mctx, n.path(bucket), n.gConf.DirMode)
	return jfsToObjectErr(ctx, eno, bucket)
}

func (n *JfsObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	if !n.isValidBucketName(bucket) {
		return bi, minio.BucketNameInvalid{Bucket: bucket}
	}
	fi, eno := n.fs.Stat(mctx, n.path(bucket))
	if eno == 0 {
		bi = minio.BucketInfo{
			Name:    bucket,
			Created: time.Unix(fi.Atime()/1000, 0),
		}
	}
	return bi, jfsToObjectErr(ctx, eno, bucket)
}

// Ignores all reserved bucket names or invalid bucket names.
func isReservedOrInvalidBucket(bucketEntry string, strict bool) bool {
	if err := s3utils.CheckValidBucketName(bucketEntry); err != nil {
		return true
	}
	return bucketEntry == metaBucket
}

func (n *JfsObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	if !n.gConf.MultiBucket {
		fi, eno := n.fs.Stat(mctx, "/")
		if eno != 0 {
			return nil, jfsToObjectErr(ctx, eno)
		}
		buckets = []minio.BucketInfo{{
			Name:    n.conf.Format.Name,
			Created: time.Unix(fi.Atime()/1000, 0),
		}}
		return buckets, nil
	}
	f, eno := n.fs.Open(mctx, sep, 0)
	if eno != 0 {
		return nil, jfsToObjectErr(ctx, eno)
	}
	defer f.Close(mctx)
	entries, eno := f.Readdir(mctx, 10000)
	if eno != 0 {
		return nil, jfsToObjectErr(ctx, eno)
	}

	for _, entry := range entries {
		// Ignore all reserved bucket names and invalid bucket names.
		if isReservedOrInvalidBucket(entry.Name(), false) || !n.isValidBucketName(entry.Name()) {
			continue
		}
		if entry.IsDir() {
			buckets = append(buckets, minio.BucketInfo{
				Name:    entry.Name(),
				Created: time.Unix(entry.(*fs.FileStat).Atime()/1000, 0),
			})
		}
	}

	// Sort bucket infos by bucket name.
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})
	return buckets, nil
}

func (n *JfsObjects) isObjectDir(ctx context.Context, bucket, object string) bool {
	f, eno := n.fs.Open(mctx, n.path(bucket, object), 0)
	if eno != 0 {
		return false
	}
	defer f.Close(mctx)

	fis, err := f.Readdir(mctx, 0)
	if err != 0 {
		return false
	}
	return len(fis) == 0
}

func (n *JfsObjects) isLeafDir(bucket, leafPath string) bool {
	return n.isObjectDir(context.Background(), bucket, leafPath)
}

func (n *JfsObjects) isLeaf(bucket, leafPath string) bool {
	return !strings.HasSuffix(leafPath, "/")
}

func (n *JfsObjects) listDirFactory() minio.ListDirFunc {
	return func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []string, delayIsLeaf bool) {
		f, eno := n.fs.Open(mctx, n.path(bucket, prefixDir), 0)
		if eno != 0 {
			return fs.IsNotExist(eno), nil, false
		}
		defer f.Close(mctx)
		fis, eno := f.Readdir(mctx, 0)
		if eno != 0 {
			return
		}
		if len(fis) == 0 {
			return true, nil, false
		}
		root := n.path(bucket, prefixDir) == "/"
		for _, fi := range fis {
			if root && len(fi.Name()) == len(metaBucket) && fi.Name() == metaBucket {
				continue
			}
			if fi.IsDir() {
				entries = append(entries, fi.Name()+sep)
			} else {
				entries = append(entries, fi.Name())
			}
		}
		entries, delayIsLeaf = minio.FilterListEntries(bucket, prefixDir, entries, prefixEntry, n.isLeaf)
		return false, entries, delayIsLeaf
	}
}

func (n *JfsObjects) checkBucket(ctx context.Context, bucket string) error {
	if !n.isValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	if _, eno := n.fs.Stat(mctx, n.path(bucket)); eno != 0 {
		return jfsToObjectErr(ctx, eno, bucket)
	}
	return nil
}

// ListObjects lists all blobs in JFS bucket filtered by prefix.
func (n *JfsObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	if err := n.checkBucket(ctx, bucket); err != nil {
		return loi, err
	}
	getObjectInfo := func(ctx context.Context, bucket, object string) (obj minio.ObjectInfo, err error) {
		fi, eno := n.fs.Stat(mctx, n.path(bucket, object))
		if eno == 0 {
			obj = minio.ObjectInfo{
				Bucket:  bucket,
				Name:    object,
				ModTime: fi.ModTime(),
				Size:    fi.Size(),
				IsDir:   fi.IsDir(),
				AccTime: fi.ModTime(),
			}

			// replace links to external file systems with empty files
			if eno == syscall.ENOTSUP {
				now := time.Now()
				obj = minio.ObjectInfo{
					Bucket:  bucket,
					Name:    object,
					ModTime: now,
					Size:    0,
					IsDir:   false,
					AccTime: now,
				}
				eno = 0
			}
		}
		return obj, jfsToObjectErr(ctx, eno, bucket, object)
	}

	if maxKeys == 0 {
		maxKeys = -1 // list as many objects as possible
	}
	return minio.ListObjects(ctx, n, bucket, prefix, marker, delimiter, maxKeys, n.listPool, n.listDirFactory(), n.isLeaf, n.isLeafDir, getObjectInfo, getObjectInfo)
}

// ListObjectsV2 lists all blobs in JFS bucket filtered by prefix
func (n *JfsObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	if !n.isValidBucketName(bucket) {
		return minio.ListObjectsV2Info{}, minio.BucketNameInvalid{Bucket: bucket}
	}
	// fetchOwner is not supported and unused.
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}
	resultV1, err := n.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err == nil {
		loi = minio.ListObjectsV2Info{
			Objects:               resultV1.Objects,
			Prefixes:              resultV1.Prefixes,
			ContinuationToken:     continuationToken,
			NextContinuationToken: resultV1.NextMarker,
			IsTruncated:           resultV1.IsTruncated,
		}
	}
	return loi, err
}

func (n *JfsObjects) DeleteObject(ctx context.Context, bucket, object string, options minio.ObjectOptions) (info minio.ObjectInfo, err error) {
	if err = n.checkBucket(ctx, bucket); err != nil {
		return
	}
	info.Bucket = bucket
	info.Name = object
	p := n.path(bucket, object)
	root := n.path(bucket)
	for p != root {
		if eno := n.fs.Delete(mctx, p); eno != 0 {
			if fs.IsNotEmpty(eno) {
				err = nil
			} else {
				err = eno
			}
			break
		}
		p = path.Dir(p)
	}
	return info, jfsToObjectErr(ctx, err, bucket, object)
}

func (n *JfsObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, options minio.ObjectOptions) (objs []minio.DeletedObject, errs []error) {
	objs = make([]minio.DeletedObject, len(objects))
	errs = make([]error, len(objects))
	for idx, object := range objects {
		_, errs[idx] = n.DeleteObject(ctx, bucket, object.ObjectName, options)
		if errs[idx] == nil {
			objs[idx] = minio.DeletedObject{
				ObjectName: object.ObjectName,
			}
		}
	}
	return
}

type fReader struct {
	*fs.File
}

func (f *fReader) Read(b []byte) (int, error) {
	return f.File.Read(mctx, b)
}

func (n *JfsObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	objInfo, err := n.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	var startOffset, length int64
	startOffset, length, err = rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return
	}
	f, eno := n.fs.Open(mctx, n.path(bucket, object), 0)
	if eno != 0 {
		return nil, jfsToObjectErr(ctx, eno, bucket, object)
	}
	_, _ = f.Seek(mctx, startOffset, 0)
	r := &io.LimitedReader{R: &fReader{f}, N: length}
	closer := func() { _ = f.Close(mctx) }
	return minio.NewGetObjectReaderFromReader(r, objInfo, opts, closer)
}

func (n *JfsObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (info minio.ObjectInfo, err error) {
	if err = n.checkBucket(ctx, srcBucket); err != nil {
		return
	}
	if err = n.checkBucket(ctx, dstBucket); err != nil {
		return
	}
	dst := n.path(dstBucket, dstObject)
	src := n.path(srcBucket, srcObject)
	if minio.IsStringEqual(src, dst) {
		return n.GetObjectInfo(ctx, srcBucket, srcObject, minio.ObjectOptions{})
	}
	tmp := n.tpath(dstBucket, "tmp", minio.MustGetUUID())
	_ = n.mkdirAll(ctx, path.Dir(tmp), os.FileMode(n.gConf.DirMode))
	f, eno := n.fs.Create(mctx, tmp, n.gConf.Mode)
	if eno != 0 {
		logger.Errorf("create %s: %s", tmp, eno)
		return
	}

	defer func() {
		_ = f.Close(mctx)
		_ = n.fs.Delete(mctx, tmp)
	}()

	_, eno = n.fs.CopyFileRange(mctx, src, 0, tmp, 0, 1<<63)
	if eno != 0 {
		err = jfsToObjectErr(ctx, eno, srcBucket, srcObject)
		logger.Errorf("copy %s to %s: %s", src, tmp, err)
		return
	}
	_ = n.mkdirAll(ctx, path.Dir(dst), os.FileMode(n.gConf.DirMode))
	eno = n.fs.Rename(mctx, tmp, dst, 0)
	if eno != 0 {
		err = jfsToObjectErr(ctx, eno, srcBucket, srcObject)
		logger.Errorf("rename %s to %s: %s", tmp, dst, err)
		return
	}
	fi, eno := n.fs.Stat(mctx, dst)
	if eno != 0 {
		err = jfsToObjectErr(ctx, eno, dstBucket, dstObject)
		return
	}

	var etag []byte
	if n.gConf.KeepEtag {
		etag, _ = n.fs.GetXattr(mctx, src, s3Etag)
		if len(etag) != 0 {
			eno = n.fs.SetXattr(mctx, dst, s3Etag, etag, 0)
			if eno != 0 {
				logger.Warnf("set xattr error, path: %s,xattr: %s,value: %s,flags: %d", dst, s3Etag, etag, 0)
			}
		}
	}

	return minio.ObjectInfo{
		Bucket:  dstBucket,
		Name:    dstObject,
		ETag:    string(etag),
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: fi.ModTime(),
	}, nil
}

var buffPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 1<<17)
		return &buf
	},
}

func (n *JfsObjects) GetObject(ctx context.Context, bucket, object string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) (err error) {
	if err = n.checkBucket(ctx, bucket); err != nil {
		return
	}
	f, eno := n.fs.Open(mctx, n.path(bucket, object), vfs.MODE_MASK_R)
	if eno != 0 {
		return jfsToObjectErr(ctx, eno, bucket, object)
	}
	defer func() { _ = f.Close(mctx) }()
	var buf = buffPool.Get().(*[]byte)
	defer buffPool.Put(buf)
	_, _ = f.Seek(mctx, startOffset, 0)
	for length > 0 {
		l := int64(len(*buf))
		if l > length {
			l = length
		}
		n, e := f.Read(mctx, (*buf)[:l])
		if n == 0 {
			if e != io.EOF {
				err = e
			}
			break
		}
		if _, err = writer.Write((*buf)[:n]); err != nil {
			break
		}
		length -= int64(n)
	}
	return jfsToObjectErr(ctx, err, bucket, object)
}

func (n *JfsObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if err = n.checkBucket(ctx, bucket); err != nil {
		return
	}
	fi, eno := n.fs.Stat(mctx, n.path(bucket, object))
	if eno != 0 {
		err = jfsToObjectErr(ctx, eno, bucket, object)
		return
	}
	// put /dir1/key1; head /dir1 return 404; head /dir1/ return 200
	if strings.HasSuffix(object, sep) && !fi.IsDir() || !strings.HasSuffix(object, sep) && fi.IsDir() {
		err = jfsToObjectErr(ctx, syscall.ENOENT, bucket, object)
		return
	}
	var etag []byte
	if n.gConf.KeepEtag && !fi.IsDir() {
		etag, _ = n.fs.GetXattr(mctx, n.path(bucket, object), s3Etag)
	}
	size := fi.Size()
	var contentType string
	if fi.IsDir() {
		size = 0
		contentType = "application/octet-stream"
	}
	return minio.ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		ModTime:     fi.ModTime(),
		Size:        size,
		IsDir:       fi.IsDir(),
		AccTime:     fi.ModTime(),
		ETag:        string(etag),
		ContentType: contentType,
	}, nil
}

func (n *JfsObjects) mkdirAll(ctx context.Context, p string, mode os.FileMode) error {
	if fi, eno := n.fs.Stat(mctx, p); eno == 0 {
		if !fi.IsDir() {
			return fmt.Errorf("%s is not directory", p)
		}
		return nil
	}
	eno := n.fs.Mkdir(mctx, p, uint16(mode))
	if eno != 0 && fs.IsNotExist(eno) {
		if err := n.mkdirAll(ctx, path.Dir(p), os.FileMode(n.gConf.DirMode)); err != nil {
			return err
		}
		eno = n.fs.Mkdir(mctx, p, uint16(mode))
	}
	if eno != 0 && fs.IsExist(eno) {
		eno = 0
	}
	if eno == 0 {
		return nil
	}
	return eno
}

func (n *JfsObjects) putObject(ctx context.Context, bucket, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (err error) {
	tmpname := n.tpath(bucket, "tmp", minio.MustGetUUID())
	_ = n.mkdirAll(ctx, path.Dir(tmpname), os.FileMode(n.gConf.DirMode))
	f, eno := n.fs.Create(mctx, tmpname, n.gConf.Mode)
	if eno != 0 {
		logger.Errorf("create %s: %s", tmpname, eno)
		err = eno
		return
	}
	defer func() { _ = n.fs.Delete(mctx, tmpname) }()
	var buf = buffPool.Get().(*[]byte)
	defer buffPool.Put(buf)
	for {
		var n int
		n, err = io.ReadFull(r, *buf)
		if n == 0 {
			if err == io.EOF {
				err = nil
			}
			break
		}
		_, eno := f.Write(mctx, (*buf)[:n])
		if eno != 0 {
			err = eno
			break
		}
	}
	if err == nil {
		eno = f.Close(mctx)
		if eno != 0 {
			err = eno
		}
	} else {
		_ = f.Close(mctx)
	}
	if err != nil {
		return
	}
	dir := path.Dir(object)
	if dir != "" {
		_ = n.mkdirAll(ctx, dir, os.FileMode(n.gConf.DirMode))
	}
	if eno := n.fs.Rename(mctx, tmpname, object, 0); eno != 0 {
		err = jfsToObjectErr(ctx, eno, bucket, object)
		return
	}
	return
}

func (n *JfsObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if err = n.checkBucket(ctx, bucket); err != nil {
		return
	}

	p := n.path(bucket, object)
	if strings.HasSuffix(object, sep) {
		if err = n.mkdirAll(ctx, p, os.FileMode(n.gConf.DirMode)); err != nil {
			err = jfsToObjectErr(ctx, err, bucket, object)
			return
		}
		if r.Size() > 0 {
			err = minio.ObjectExistsAsDirectory{
				Bucket: bucket,
				Object: object,
				Err:    syscall.EEXIST,
			}
			return
		}
	} else if err = n.putObject(ctx, bucket, p, r, opts); err != nil {
		return
	}
	fi, eno := n.fs.Stat(mctx, p)
	if eno != 0 {
		return objInfo, jfsToObjectErr(ctx, eno, bucket, object)
	}
	etag := r.MD5CurrentHexString()
	if n.gConf.KeepEtag {
		eno = n.fs.SetXattr(mctx, p, s3Etag, []byte(etag), 0)
		if eno != 0 {
			logger.Errorf("set xattr error, path: %s,xattr: %s,value: %s,flags: %d", p, s3Etag, etag, 0)
		}
	}
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    etag,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: fi.ModTime(),
	}, nil
}

func (n *JfsObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	if err = n.checkBucket(ctx, bucket); err != nil {
		return
	}
	uploadID = minio.MustGetUUID()
	p := n.upath(bucket, uploadID)
	err = n.mkdirAll(ctx, p, os.FileMode(n.gConf.DirMode))
	if err == nil {
		eno := n.fs.SetXattr(mctx, p, uploadKeyName, []byte(object), 0)
		if eno != 0 {
			logger.Warnf("set object %s on upload %s: %s", object, uploadID, eno)
		}
	}
	return
}

const uploadKeyName = "s3-object"
const s3Etag = "s3-etag"

func (n *JfsObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, err error) {
	if err = n.checkBucket(ctx, bucket); err != nil {
		return
	}
	f, eno := n.fs.Open(mctx, n.tpath(bucket, "uploads"), 0)
	if eno != 0 {
		return // no found
	}
	defer f.Close(mctx)
	entries, eno := f.ReaddirPlus(mctx, 0)
	if eno != 0 {
		err = jfsToObjectErr(ctx, eno, bucket)
		return
	}
	lmi.Prefix = prefix
	lmi.KeyMarker = keyMarker
	lmi.UploadIDMarker = uploadIDMarker
	lmi.MaxUploads = maxUploads
	for _, e := range entries {
		uploadID := string(e.Name)
		if uploadID > uploadIDMarker {
			object_, _ := n.fs.GetXattr(mctx, n.upath(bucket, uploadID), uploadKeyName)
			object := string(object_)
			if strings.HasPrefix(object, prefix) && object > keyMarker {
				lmi.Uploads = append(lmi.Uploads, minio.MultipartInfo{
					Object:    object,
					UploadID:  uploadID,
					Initiated: time.Unix(e.Attr.Atime, int64(e.Attr.Atimensec)),
				})
			}
		}
	}
	if len(lmi.Uploads) > maxUploads {
		lmi.IsTruncated = true
		lmi.Uploads = lmi.Uploads[:maxUploads]
		lmi.NextKeyMarker = keyMarker
		lmi.NextUploadIDMarker = lmi.Uploads[maxUploads-1].UploadID
	}
	return lmi, jfsToObjectErr(ctx, err, bucket)
}

func (n *JfsObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string) (err error) {
	if err = n.checkBucket(ctx, bucket); err != nil {
		return
	}
	_, eno := n.fs.Stat(mctx, n.upath(bucket, uploadID))
	return jfsToObjectErr(ctx, eno, bucket, object, uploadID)
}

func (n *JfsObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}
	f, e := n.fs.Open(mctx, n.upath(bucket, uploadID), 0)
	if e != 0 {
		err = jfsToObjectErr(ctx, e, bucket, object, uploadID)
		return
	}
	defer func() { _ = f.Close(mctx) }()
	entries, e := f.ReaddirPlus(mctx, 0)
	if e != 0 {
		err = jfsToObjectErr(ctx, e, bucket, object, uploadID)
		return
	}
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.PartNumberMarker = partNumberMarker
	result.MaxParts = maxParts
	for _, entry := range entries {
		num, er := strconv.Atoi(string(entry.Name))
		if er == nil && num > partNumberMarker {
			etag, _ := n.fs.GetXattr(mctx, n.ppath(bucket, uploadID, string(entry.Name)), s3Etag)
			result.Parts = append(result.Parts, minio.PartInfo{
				PartNumber:   num,
				Size:         int64(entry.Attr.Length),
				LastModified: time.Unix(entry.Attr.Mtime, 0),
				ETag:         string(etag),
			})
		}
	}
	sort.Slice(result.Parts, func(i, j int) bool {
		return result.Parts[i].PartNumber < result.Parts[j].PartNumber
	})
	if len(result.Parts) > maxParts {
		result.IsTruncated = true
		result.Parts = result.Parts[:maxParts]
		result.NextPartNumberMarker = result.Parts[maxParts-1].PartNumber
	}
	return
}

func (n *JfsObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int,
	startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (result minio.PartInfo, err error) {
	if !n.isValidBucketName(srcBucket) {
		err = minio.BucketNameInvalid{Bucket: srcBucket}
		return
	}
	if err = n.checkUploadIDExists(ctx, dstBucket, dstObject, uploadID); err != nil {
		return
	}
	// TODO: use CopyFileRange
	return n.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}

func (n *JfsObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return
	}
	p := n.ppath(bucket, uploadID, strconv.Itoa(partID))
	if err = n.putObject(ctx, bucket, p, r, opts); err != nil {
		err = jfsToObjectErr(ctx, err, bucket, object)
		return
	}
	etag := r.MD5CurrentHexString()
	if n.fs.SetXattr(mctx, p, s3Etag, []byte(etag), 0) != 0 {
		logger.Warnf("set xattr error, path: %s,xattr: %s,value: %s,flags: %d", p, s3Etag, etag, 0)
	}
	info.PartNumber = partID
	info.ETag = etag
	info.LastModified = minio.UTCNow()
	info.Size = r.Reader.Size()
	return
}

func (n *JfsObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (result minio.MultipartInfo, err error) {
	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return
	}
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	return
}

func (n *JfsObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return
	}

	tmp := n.ppath(bucket, uploadID, "complete")
	_ = n.fs.Delete(mctx, tmp)
	f, eno := n.fs.Create(mctx, tmp, n.gConf.Mode)
	if eno != 0 {
		err = jfsToObjectErr(ctx, eno, bucket, object, uploadID)
		logger.Errorf("create complete: %s", err)
		return
	}
	defer func() {
		_ = f.Close(mctx)
	}()
	var total uint64
	for _, part := range parts {
		p := n.ppath(bucket, uploadID, strconv.Itoa(part.PartNumber))
		copied, eno := n.fs.CopyFileRange(mctx, p, 0, tmp, total, 1<<30)
		if eno != 0 {
			err = jfsToObjectErr(ctx, eno, bucket, object, uploadID)
			logger.Errorf("merge parts: %s", err)
			return
		}
		total += copied
	}

	name := n.path(bucket, object)
	dir := path.Dir(name)
	if dir != "" {
		if err = n.mkdirAll(ctx, dir, os.FileMode(n.gConf.DirMode)); err != nil {
			_ = n.fs.Delete(mctx, tmp)
			err = jfsToObjectErr(ctx, err, bucket, object, uploadID)
			return
		}
	}

	eno = n.fs.Rename(mctx, tmp, name, 0)
	if eno != 0 {
		_ = n.fs.Delete(mctx, tmp)
		err = jfsToObjectErr(ctx, eno, bucket, object, uploadID)
		logger.Errorf("Rename %s -> %s: %s", tmp, name, err)
		return
	}

	fi, eno := n.fs.Stat(mctx, name)
	if eno != 0 {
		_ = n.fs.Delete(mctx, name)
		err = jfsToObjectErr(ctx, eno, bucket, object, uploadID)
		return
	}

	// remove parts
	_ = n.fs.Rmr(mctx, n.upath(bucket, uploadID))

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5 := minio.ComputeCompleteMultipartMD5(parts)
	if n.gConf.KeepEtag {
		eno = n.fs.SetXattr(mctx, name, s3Etag, []byte(s3MD5), 0)
		if eno != 0 {
			logger.Warnf("set xattr error, path: %s,xattr: %s,value: %s,flags: %d", name, s3Etag, s3MD5, 0)
		}
	}
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    s3MD5,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: fi.ModTime(),
	}, nil
}

func (n *JfsObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, option minio.ObjectOptions) (err error) {
	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return
	}
	eno := n.fs.Rmr(mctx, n.upath(bucket, uploadID))
	return jfsToObjectErr(ctx, eno, bucket, object, uploadID)
}

func (n *JfsObjects) cleanup() {
	for t := range time.Tick(24 * time.Hour) {
		// default bucket tmp dirs
		tmpDirs := []string{".sys/tmp/", ".sys/uploads/"}
		if n.gConf.MultiBucket {
			buckets, err := n.ListBuckets(context.Background())
			if err != nil {
				logger.Errorf("list buckets error: %v", err)
				continue
			}
			for _, bucket := range buckets {
				tmpDirs = append(tmpDirs, fmt.Sprintf(".sys/%s/tmp", bucket.Name))
				tmpDirs = append(tmpDirs, fmt.Sprintf(".sys/%s/uploads", bucket.Name))
			}
		}
		for _, dir := range tmpDirs {
			f, errno := n.fs.Open(mctx, dir, 0)
			if errno != 0 {
				continue
			}
			entries, _ := f.ReaddirPlus(mctx, 0)
			for _, entry := range entries {
				if _, err := uuid.Parse(string(entry.Name)); err != nil {
					continue
				}
				if t.Sub(time.Unix(entry.Attr.Mtime, 0)) > 7*24*time.Hour {
					p := n.path(dir, string(entry.Name))
					if errno := n.fs.Rmr(mctx, p); errno != 0 {
						logger.Errorf("failed to delete expired temporary files path: %s,", p)
					} else {
						logger.Infof("delete expired temporary files path: %s, mtime: %s", p, time.Unix(entry.Attr.Mtime, 0).Format(time.RFC3339))
					}
				}
			}
		}
	}
}
