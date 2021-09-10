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

package hdfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/user"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	krb "github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/minio/cli"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/env"
	xnet "github.com/minio/pkg/net"
)

const (
	hdfsSeparator = minio.SlashSeparator
)

func init() {
	const hdfsGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} HDFS-NAMENODE [HDFS-NAMENODE...]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
HDFS-NAMENODE:
  HDFS namenode URI

EXAMPLES:
  1. Start minio gateway server for HDFS backend
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.HelpName}} hdfs://namenode:8200

  2. Start minio gateway server for HDFS with edge caching enabled
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_DRIVES{{.AssignmentOperator}}"/mnt/drive1,/mnt/drive2,/mnt/drive3,/mnt/drive4"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_EXCLUDE{{.AssignmentOperator}}"bucket1/*,*.png"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_QUOTA{{.AssignmentOperator}}90
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_AFTER{{.AssignmentOperator}}3
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_LOW{{.AssignmentOperator}}75
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_HIGH{{.AssignmentOperator}}85
     {{.Prompt}} {{.HelpName}} hdfs://namenode:8200
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               minio.HDFSBackendGateway,
		Usage:              "Hadoop Distributed File System (HDFS)",
		Action:             hdfsGatewayMain,
		CustomHelpTemplate: hdfsGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway hdfs' command line.
func hdfsGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, minio.HDFSBackendGateway, 1)
	}

	minio.StartGateway(ctx, &HDFS{args: ctx.Args()})
}

// HDFS implements Gateway.
type HDFS struct {
	args []string
}

// Name implements Gateway interface.
func (g *HDFS) Name() string {
	return minio.HDFSBackendGateway
}

func getKerberosClient() (*krb.Client, error) {
	cfg, err := config.Load(env.Get("KRB5_CONFIG", "/etc/krb5.conf"))
	if err != nil {
		return nil, err
	}

	u, err := user.Current()
	if err != nil {
		return nil, err
	}

	keytabPath := env.Get("KRB5KEYTAB", "")
	if keytabPath != "" {
		kt, err := keytab.Load(keytabPath)
		if err != nil {
			return nil, err
		}

		username := env.Get("KRB5USERNAME", "")
		realm := env.Get("KRB5REALM", "")
		if username == "" || realm == "" {
			return nil, errors.New("empty KRB5USERNAME or KRB5REALM")

		}

		return krb.NewWithKeytab(username, realm, kt, cfg), nil
	}

	// Determine the ccache location from the environment, falling back to the default location.
	ccachePath := env.Get("KRB5CCNAME", fmt.Sprintf("/tmp/krb5cc_%s", u.Uid))
	if strings.Contains(ccachePath, ":") {
		if strings.HasPrefix(ccachePath, "FILE:") {
			ccachePath = strings.TrimPrefix(ccachePath, "FILE:")
		} else {
			return nil, fmt.Errorf("unable to use kerberos ccache: %s", ccachePath)
		}
	}

	ccache, err := credentials.LoadCCache(ccachePath)
	if err != nil {
		return nil, err
	}

	return krb.NewFromCCache(ccache, cfg)
}

// NewGatewayLayer returns hdfs gatewaylayer.
func (g *HDFS) NewGatewayLayer(creds madmin.Credentials) (minio.ObjectLayer, error) {
	dialFunc := (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}).DialContext

	hconfig, err := hadoopconf.LoadFromEnvironment()
	if err != nil {
		return nil, err
	}

	opts := hdfs.ClientOptionsFromConf(hconfig)
	opts.NamenodeDialFunc = dialFunc
	opts.DatanodeDialFunc = dialFunc

	// Not addresses found, load it from command line.
	var commonPath string
	if len(opts.Addresses) == 0 {
		var addresses []string
		for _, s := range g.args {
			u, err := xnet.ParseURL(s)
			if err != nil {
				return nil, err
			}
			if u.Scheme != "hdfs" {
				return nil, fmt.Errorf("unsupported scheme %s, only supports hdfs://", u)
			}
			if commonPath != "" && commonPath != u.Path {
				return nil, fmt.Errorf("all namenode paths should be same %s", g.args)
			}
			if commonPath == "" {
				commonPath = u.Path
			}
			addresses = append(addresses, u.Host)
		}
		opts.Addresses = addresses
	}

	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("unable to lookup local user: %s", err)
	}

	if opts.KerberosClient != nil {
		opts.KerberosClient, err = getKerberosClient()
		if err != nil {
			return nil, fmt.Errorf("unable to initialize kerberos client: %s", err)
		}
	} else {
		opts.User = env.Get("HADOOP_USER_NAME", u.Username)
	}

	clnt, err := hdfs.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize hdfsClient: %v", err)
	}

	if err = clnt.MkdirAll(minio.PathJoin(commonPath, hdfsSeparator, minioMetaTmpBucket), os.FileMode(0755)); err != nil {
		return nil, err
	}

	return &hdfsObjects{clnt: clnt, subPath: commonPath, listPool: minio.NewTreeWalkPool(time.Minute * 30)}, nil
}

func (n *hdfsObjects) Shutdown(ctx context.Context) error {
	return n.clnt.Close()
}

func (n *hdfsObjects) LocalStorageInfo(ctx context.Context) (si minio.StorageInfo, errs []error) {
	return n.StorageInfo(ctx)
}

func (n *hdfsObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo, errs []error) {
	fsInfo, err := n.clnt.StatFs()
	if err != nil {
		return minio.StorageInfo{}, []error{err}
	}
	si.Disks = []madmin.Disk{{
		UsedSpace: fsInfo.Used,
	}}
	si.Backend.Type = madmin.Gateway
	si.Backend.GatewayOnline = true
	return si, nil
}

// hdfsObjects implements gateway for Minio and S3 compatible object storage servers.
type hdfsObjects struct {
	minio.GatewayUnsupported
	clnt     *hdfs.Client
	subPath  string
	listPool *minio.TreeWalkPool
}

func hdfsToObjectErr(ctx context.Context, err error, params ...string) error {
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

	switch {
	case os.IsNotExist(err):
		if uploadID != "" {
			return minio.InvalidUploadID{
				UploadID: uploadID,
			}
		}
		if object != "" {
			return minio.ObjectNotFound{Bucket: bucket, Object: object}
		}
		return minio.BucketNotFound{Bucket: bucket}
	case os.IsExist(err):
		if object != "" {
			return minio.PrefixAccessDenied{Bucket: bucket, Object: object}
		}
		return minio.BucketAlreadyOwnedByYou{Bucket: bucket}
	case errors.Is(err, syscall.ENOTEMPTY):
		if object != "" {
			return minio.PrefixAccessDenied{Bucket: bucket, Object: object}
		}
		return minio.BucketNotEmpty{Bucket: bucket}
	default:
		logger.LogIf(ctx, err)
		return err
	}
}

// hdfsIsValidBucketName verifies whether a bucket name is valid.
func hdfsIsValidBucketName(bucket string) bool {
	return s3utils.CheckValidBucketNameStrict(bucket) == nil
}

func (n *hdfsObjects) hdfsPathJoin(args ...string) string {
	return minio.PathJoin(append([]string{n.subPath, hdfsSeparator}, args...)...)
}

func (n *hdfsObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	if !hdfsIsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	if forceDelete {
		return hdfsToObjectErr(ctx, n.clnt.RemoveAll(n.hdfsPathJoin(bucket)), bucket)
	}
	return hdfsToObjectErr(ctx, n.clnt.Remove(n.hdfsPathJoin(bucket)), bucket)
}

func (n *hdfsObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	if opts.LockEnabled || opts.VersioningEnabled {
		return minio.NotImplemented{}
	}

	if !hdfsIsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	return hdfsToObjectErr(ctx, n.clnt.Mkdir(n.hdfsPathJoin(bucket), os.FileMode(0755)), bucket)
}

func (n *hdfsObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	fi, err := n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return bi, hdfsToObjectErr(ctx, err, bucket)
	}
	// As hdfs.Stat() doesn't carry anything other than ModTime(), use ModTime() as CreatedTime.
	return minio.BucketInfo{
		Name:    bucket,
		Created: fi.ModTime(),
	}, nil
}

func (n *hdfsObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	entries, err := n.clnt.ReadDir(n.hdfsPathJoin())
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, hdfsToObjectErr(ctx, err)
	}

	for _, entry := range entries {
		// Ignore all reserved bucket names and invalid bucket names.
		if isReservedOrInvalidBucket(entry.Name(), false) {
			continue
		}
		buckets = append(buckets, minio.BucketInfo{
			Name: entry.Name(),
			// As hdfs.Stat() doesnt carry CreatedTime, use ModTime() as CreatedTime.
			Created: entry.ModTime(),
		})
	}

	// Sort bucket infos by bucket name.
	sort.Sort(byBucketName(buckets))
	return buckets, nil
}

func (n *hdfsObjects) isLeafDir(bucket, leafPath string) bool {
	return n.isObjectDir(context.Background(), bucket, leafPath)
}

func (n *hdfsObjects) isLeaf(bucket, leafPath string) bool {
	return !strings.HasSuffix(leafPath, hdfsSeparator)
}

func (n *hdfsObjects) listDirFactory() minio.ListDirFunc {
	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []string, delayIsLeaf bool) {
		f, err := n.clnt.Open(n.hdfsPathJoin(bucket, prefixDir))
		if err != nil {
			if os.IsNotExist(err) {
				err = nil
			}
			logger.LogIf(minio.GlobalContext, err)
			return
		}
		defer f.Close()
		fis, err := f.Readdir(0)
		if err != nil {
			logger.LogIf(minio.GlobalContext, err)
			return
		}
		if len(fis) == 0 {
			return true, nil, false
		}
		for _, fi := range fis {
			if fi.IsDir() {
				entries = append(entries, fi.Name()+hdfsSeparator)
			} else {
				entries = append(entries, fi.Name())
			}
		}
		entries, delayIsLeaf = minio.FilterListEntries(bucket, prefixDir, entries, prefixEntry, n.isLeaf)
		return false, entries, delayIsLeaf
	}

	// Return list factory instance.
	return listDir
}

// ListObjects lists all blobs in HDFS bucket filtered by prefix.
func (n *hdfsObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	fileInfos := make(map[string]os.FileInfo)
	targetPath := n.hdfsPathJoin(bucket, prefix)

	var targetFileInfo os.FileInfo

	if targetFileInfo, err = n.populateDirectoryListing(targetPath, fileInfos); err != nil {
		return loi, hdfsToObjectErr(ctx, err, bucket)
	}

	// If the user is trying to list a single file, bypass the entire directory-walking code below
	// and just return the single file's information.
	if !targetFileInfo.IsDir() {
		return minio.ListObjectsInfo{
			IsTruncated: false,
			NextMarker:  "",
			Objects: []minio.ObjectInfo{
				fileInfoToObjectInfo(bucket, prefix, targetFileInfo),
			},
			Prefixes: []string{},
		}, nil
	}

	getObjectInfo := func(ctx context.Context, bucket, entry string) (minio.ObjectInfo, error) {
		filePath := path.Clean(n.hdfsPathJoin(bucket, entry))
		fi, ok := fileInfos[filePath]

		// If the file info is not known, this may be a recursive listing and filePath is a
		// child of a sub-directory. In this case, obtain that sub-directory's listing.
		if !ok {
			parentPath := path.Dir(filePath)

			if _, err := n.populateDirectoryListing(parentPath, fileInfos); err != nil {
				return minio.ObjectInfo{}, hdfsToObjectErr(ctx, err, bucket)
			}

			fi, ok = fileInfos[filePath]

			if !ok {
				err = fmt.Errorf("could not get FileInfo for path '%s'", filePath)
				return minio.ObjectInfo{}, hdfsToObjectErr(ctx, err, bucket, entry)
			}
		}

		objectInfo := fileInfoToObjectInfo(bucket, entry, fi)

		delete(fileInfos, filePath)

		return objectInfo, nil
	}

	return minio.ListObjects(ctx, n, bucket, prefix, marker, delimiter, maxKeys, n.listPool, n.listDirFactory(), n.isLeaf, n.isLeafDir, getObjectInfo, getObjectInfo)
}

func fileInfoToObjectInfo(bucket string, entry string, fi os.FileInfo) minio.ObjectInfo {
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    entry,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: fi.(*hdfs.FileInfo).AccessTime(),
	}
}

// Lists a path's direct, first-level entries and populates them in the `fileInfos` cache which maps
// a path entry to an `os.FileInfo`. It also saves the listed path's `os.FileInfo` in the cache.
func (n *hdfsObjects) populateDirectoryListing(filePath string, fileInfos map[string]os.FileInfo) (os.FileInfo, error) {
	dirReader, err := n.clnt.Open(filePath)

	if err != nil {
		return nil, err
	}

	dirStat := dirReader.Stat()
	key := path.Clean(filePath)

	if !dirStat.IsDir() {
		return dirStat, nil
	}

	fileInfos[key] = dirStat
	infos, err := dirReader.Readdir(0)

	if err != nil {
		return nil, err
	}

	for _, fileInfo := range infos {
		filePath := minio.PathJoin(filePath, fileInfo.Name())
		fileInfos[filePath] = fileInfo
	}

	return dirStat, nil
}

// deleteObject deletes a file path if its empty. If it's successfully deleted,
// it will recursively move up the tree, deleting empty parent directories
// until it finds one with files in it. Returns nil for a non-empty directory.
func (n *hdfsObjects) deleteObject(basePath, deletePath string) error {
	if basePath == deletePath {
		return nil
	}

	// Attempt to remove path.
	if err := n.clnt.Remove(deletePath); err != nil {
		if errors.Is(err, syscall.ENOTEMPTY) {
			// Ignore errors if the directory is not empty. The server relies on
			// this functionality, and sometimes uses recursion that should not
			// error on parent directories.
			return nil
		}
		return err
	}

	// Trailing slash is removed when found to ensure
	// slashpath.Dir() to work as intended.
	deletePath = strings.TrimSuffix(deletePath, hdfsSeparator)
	deletePath = path.Dir(deletePath)

	// Delete parent directory. Errors for parent directories shouldn't trickle down.
	n.deleteObject(basePath, deletePath)

	return nil
}

// ListObjectsV2 lists all blobs in HDFS bucket filtered by prefix
func (n *hdfsObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	// fetchOwner is not supported and unused.
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}
	resultV1, err := n.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
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

func (n *hdfsObjects) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	err := hdfsToObjectErr(ctx, n.deleteObject(n.hdfsPathJoin(bucket), n.hdfsPathJoin(bucket, object)), bucket, object)
	return minio.ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, err
}

func (n *hdfsObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]minio.DeletedObject, len(objects))
	for idx, object := range objects {
		_, errs[idx] = n.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if errs[idx] == nil {
			dobjects[idx] = minio.DeletedObject{
				ObjectName: object.ObjectName,
			}
		}
	}
	return dobjects, errs
}

func (n *hdfsObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	objInfo, err := n.GetObjectInfo(ctx, bucket, object, opts)
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
		nerr := n.getObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(nerr)
	}()

	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts, pipeCloser)

}

func (n *hdfsObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.ObjectInfo, error) {
	cpSrcDstSame := minio.IsStringEqual(n.hdfsPathJoin(srcBucket, srcObject), n.hdfsPathJoin(dstBucket, dstObject))
	if cpSrcDstSame {
		return n.GetObjectInfo(ctx, srcBucket, srcObject, minio.ObjectOptions{})
	}

	return n.PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, minio.ObjectOptions{
		ServerSideEncryption: dstOpts.ServerSideEncryption,
		UserDefined:          srcInfo.UserDefined,
	})
}

func (n *hdfsObjects) getObject(ctx context.Context, bucket, key string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	if _, err := n.clnt.Stat(n.hdfsPathJoin(bucket)); err != nil {
		return hdfsToObjectErr(ctx, err, bucket)
	}
	rd, err := n.clnt.Open(n.hdfsPathJoin(bucket, key))
	if err != nil {
		return hdfsToObjectErr(ctx, err, bucket, key)
	}
	defer rd.Close()
	_, err = io.Copy(writer, io.NewSectionReader(rd, startOffset, length))
	if err == io.ErrClosedPipe {
		// hdfs library doesn't send EOF correctly, so io.Copy attempts
		// to write which returns io.ErrClosedPipe - just ignore
		// this for now.
		err = nil
	}
	return hdfsToObjectErr(ctx, err, bucket, key)
}

func (n *hdfsObjects) isObjectDir(ctx context.Context, bucket, object string) bool {
	f, err := n.clnt.Open(n.hdfsPathJoin(bucket, object))
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		logger.LogIf(ctx, err)
		return false
	}
	defer f.Close()
	fis, err := f.Readdir(1)
	if err != nil && err != io.EOF {
		logger.LogIf(ctx, err)
		return false
	}
	// Readdir returns an io.EOF when len(fis) == 0.
	return len(fis) == 0
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (n *hdfsObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return objInfo, hdfsToObjectErr(ctx, err, bucket)
	}
	if strings.HasSuffix(object, hdfsSeparator) && !n.isObjectDir(ctx, bucket, object) {
		return objInfo, hdfsToObjectErr(ctx, os.ErrNotExist, bucket, object)
	}

	fi, err := n.clnt.Stat(n.hdfsPathJoin(bucket, object))
	if err != nil {
		return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
	}
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: fi.(*hdfs.FileInfo).AccessTime(),
	}, nil
}

func (n *hdfsObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return objInfo, hdfsToObjectErr(ctx, err, bucket)
	}

	name := n.hdfsPathJoin(bucket, object)

	// If its a directory create a prefix {
	if strings.HasSuffix(object, hdfsSeparator) && r.Size() == 0 {
		if err = n.clnt.MkdirAll(name, os.FileMode(0755)); err != nil {
			n.deleteObject(n.hdfsPathJoin(bucket), name)
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
	} else {
		tmpname := n.hdfsPathJoin(minioMetaTmpBucket, minio.MustGetUUID())
		var w *hdfs.FileWriter
		w, err = n.clnt.Create(tmpname)
		if err != nil {
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
		defer n.deleteObject(n.hdfsPathJoin(minioMetaTmpBucket), tmpname)
		if _, err = io.Copy(w, r); err != nil {
			w.Close()
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
		dir := path.Dir(name)
		if dir != "" {
			if err = n.clnt.MkdirAll(dir, os.FileMode(0755)); err != nil {
				w.Close()
				n.deleteObject(n.hdfsPathJoin(bucket), dir)
				return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
			}
		}
		w.Close()
		if err = n.clnt.Rename(tmpname, name); err != nil {
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
	}
	fi, err := n.clnt.Stat(name)
	if err != nil {
		return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
	}
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    r.MD5CurrentHexString(),
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: fi.(*hdfs.FileInfo).AccessTime(),
	}, nil
}

func (n *hdfsObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return uploadID, hdfsToObjectErr(ctx, err, bucket)
	}

	uploadID = minio.MustGetUUID()
	if err = n.clnt.CreateEmptyFile(n.hdfsPathJoin(minioMetaTmpBucket, uploadID)); err != nil {
		return uploadID, hdfsToObjectErr(ctx, err, bucket)
	}

	return uploadID, nil
}

func (n *hdfsObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return lmi, hdfsToObjectErr(ctx, err, bucket)
	}

	// It's decided not to support List Multipart Uploads, hence returning empty result.
	return lmi, nil
}

func (n *hdfsObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string) (err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(minioMetaTmpBucket, uploadID))
	if err != nil {
		return hdfsToObjectErr(ctx, err, bucket, object, uploadID)
	}
	return nil
}

// GetMultipartInfo returns multipart info of the uploadId of the object
func (n *hdfsObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (result minio.MultipartInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return result, hdfsToObjectErr(ctx, err, bucket)
	}

	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	return result, nil
}

func (n *hdfsObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return result, hdfsToObjectErr(ctx, err, bucket)
	}

	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}

	// It's decided not to support List parts, hence returning empty result.
	return result, nil
}

func (n *hdfsObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int,
	startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.PartInfo, error) {
	return n.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}

func (n *hdfsObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return info, hdfsToObjectErr(ctx, err, bucket)
	}

	var w *hdfs.FileWriter
	w, err = n.clnt.Append(n.hdfsPathJoin(minioMetaTmpBucket, uploadID))
	if err != nil {
		return info, hdfsToObjectErr(ctx, err, bucket, object, uploadID)
	}
	defer w.Close()
	_, err = io.Copy(w, r.Reader)
	if err != nil {
		return info, hdfsToObjectErr(ctx, err, bucket, object, uploadID)
	}

	info.PartNumber = partID
	info.ETag = r.MD5CurrentHexString()
	info.LastModified = minio.UTCNow()
	info.Size = r.Reader.Size()

	return info, nil
}

func (n *hdfsObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return objInfo, hdfsToObjectErr(ctx, err, bucket)
	}

	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return objInfo, err
	}

	name := n.hdfsPathJoin(bucket, object)
	dir := path.Dir(name)
	if dir != "" {
		if err = n.clnt.MkdirAll(dir, os.FileMode(0755)); err != nil {
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
	}

	err = n.clnt.Rename(n.hdfsPathJoin(minioMetaTmpBucket, uploadID), name)
	// Object already exists is an error on HDFS
	// remove it and then create it again.
	if os.IsExist(err) {
		if err = n.clnt.Remove(name); err != nil {
			if dir != "" {
				n.deleteObject(n.hdfsPathJoin(bucket), dir)
			}
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
		if err = n.clnt.Rename(n.hdfsPathJoin(minioMetaTmpBucket, uploadID), name); err != nil {
			if dir != "" {
				n.deleteObject(n.hdfsPathJoin(bucket), dir)
			}
			return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
		}
	}
	fi, err := n.clnt.Stat(name)
	if err != nil {
		return objInfo, hdfsToObjectErr(ctx, err, bucket, object)
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
		AccTime: fi.(*hdfs.FileInfo).AccessTime(),
	}, nil
}

func (n *hdfsObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (err error) {
	_, err = n.clnt.Stat(n.hdfsPathJoin(bucket))
	if err != nil {
		return hdfsToObjectErr(ctx, err, bucket)
	}
	return hdfsToObjectErr(ctx, n.clnt.Remove(n.hdfsPathJoin(minioMetaTmpBucket, uploadID)), bucket, object, uploadID)
}
