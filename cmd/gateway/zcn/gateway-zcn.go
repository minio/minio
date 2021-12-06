package zcn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	zerror "github.com/0chain/errors"
	"github.com/0chain/gosdk/zboxcore/sdk"
	"github.com/minio/cli"
	"github.com/minio/madmin-go"
	minio "github.com/minio/minio/cmd"
)

const (
	RootPath       = "/"
	RootBucketName = "root"
)

var configDir string
var allocationId string

var zFlags = []cli.Flag{
	cli.StringFlag{
		Name:        "configDir",
		Usage:       "Config directory containing config.yaml, wallet.json, allocation.txt, etc.",
		Destination: &configDir,
	},
	cli.StringFlag{
		Name:        "allocationId",
		Usage:       "Allocation id of an allocation",
		Destination: &allocationId,
	},
}

func init() {
	const zcnGateWayTemplate = `NAME:
	{{.HelpName}} - {{.Usage}}
  
  USAGE:
	{{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} ZCN-NAMENODE [ZCN-NAMENODE...]
  {{if .VisibleFlags}}
  FLAGS:
	{{range .VisibleFlags}}{{.}}
	{{end}}{{end}}
  ZCN-NAMENODE:
	ZCN namenode URI
  
  EXAMPLES:
	1. Start minio gateway server for ZeroChain backend
	   {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}accesskey
	   {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}secretkey
	   {{.Prompt}} {{.HelpName}} zcn://namenode:8200
  
	2. Start minio gateway server for ZCN with edge caching enabled
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
		Name:               minio.ZCNBAckendGateway,
		Usage:              "0chain dStorage",
		Action:             zcnGatewayMain,
		CustomHelpTemplate: zcnGateWayTemplate,
		Flags:              zFlags,
		HideHelpCommand:    true,
	})
}

func zcnGatewayMain(ctx *cli.Context) {
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, minio.ZCNBAckendGateway, 1)
	}

	minio.StartGateway(ctx, &ZCN{args: ctx.Args()})
}

type ZCN struct {
	args []string
}

func (z *ZCN) Name() string {
	return minio.ZCNBAckendGateway
}

func (z *ZCN) NewGatewayLayer(creds madmin.Credentials) (minio.ObjectLayer, error) {
	err := initializeSDK(configDir, allocationId)
	if err != nil {
		return nil, err
	}

	allocation, err := sdk.GetAllocation(allocationId)
	if err != nil {
		return nil, err
	}

	zob := &zcnObjects{
		alloc:   allocation,
		metrics: minio.NewMetrics(),
	}

	return zob, nil
}

type zcnObjects struct {
	minio.GatewayUnsupported
	alloc   *sdk.Allocation
	metrics *minio.BackendMetrics
}

func (zob *zcnObjects) Shutdown(ctx context.Context) error {
	os.RemoveAll(tempdir)
	return nil
}

func (zob *zcnObjects) Production() bool {
	return false
}

func (zob *zcnObjects) GetMetrics(ctx context.Context) (*minio.BackendMetrics, error) {
	return zob.metrics, nil
}

func (zob *zcnObjects) DeleteBucket(ctx context.Context, bucketName string, opts minio.DeleteBucketOptions) error {
	//Delete empty bucket. May need to check if directory contains empty directories inside
	if bucketName == RootBucketName {
		return errors.New("cannot remove root path")
	}

	remotePath := filepath.Join(RootPath, bucketName)
	if opts.Force {
		return zob.alloc.DeleteFile(remotePath)
	}

	ref, err := getSingleRegularRef(zob.alloc, remotePath)
	if err != nil {
		return err
	}

	// if ref.
	_ = ref.Type == Dir //?

	_ = ref
	//Find a way to find if ref is not empty
	return nil
}

func (zob *zcnObjects) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (oInfo minio.ObjectInfo, err error) {
	var remotePath string
	if bucket == RootBucketName {
		remotePath = filepath.Join(RootPath, object)
	} else {
		remotePath = filepath.Join(RootPath, bucket, object)
	}

	var ref *sdk.ORef
	ref, err = getSingleRegularRef(zob.alloc, remotePath)
	if err != nil {
		return
	}

	err = zob.alloc.DeleteFile(remotePath)
	if err != nil {
		return
	}

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    ref.Name,
		ModTime: time.Now(),
		Size:    ref.ActualFileSize,
		IsDir:   ref.Type == Dir,
	}, nil
}

func (zob *zcnObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) (delObs []minio.DeletedObject, errs []error) {
	var rootPath string
	if bucket == RootBucketName {
		rootPath = RootPath
	} else {
		rootPath = filepath.Join(RootPath, bucket)
	}

	for _, object := range objects {
		remotePath := filepath.Join(rootPath, object.ObjectName)
		err := zob.alloc.DeleteFile(remotePath)
		if err != nil {
			errs = append(errs, err)
		} else {
			delObs = append(delObs, minio.DeletedObject{
				ObjectName: object.ObjectName,
			})
		}
	}
	return
}

func (zob *zcnObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	var remotePath string
	if bucket == RootBucketName {
		remotePath = RootPath
	} else {
		remotePath = filepath.Join(RootPath, bucket)
	}

	var ref *sdk.ORef
	ref, err = getSingleRegularRef(zob.alloc, remotePath)
	if err != nil {
		return
	}

	return minio.BucketInfo{Name: ref.Name, Created: ref.CreatedAt}, nil
}

func (zob *zcnObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	var remotePath string
	if bucket == RootBucketName {
		remotePath = filepath.Join(RootPath, object)
	} else {
		remotePath = filepath.Join(RootPath, bucket, object)
	}

	var ref *sdk.ORef
	ref, err = getSingleRegularRef(zob.alloc, remotePath)
	if err != nil {
		return
	}

	return minio.ObjectInfo{
		Bucket:      bucket,
		Name:        ref.Name,
		ModTime:     ref.UpdatedAt,
		Size:        ref.ActualFileSize,
		IsDir:       ref.Type == Dir,
		AccTime:     time.Now(),
		ContentType: ref.MimeType,
	}, nil
}

//GetObjectNInfo Provides reader with read cursor placed at offset upto some length
func (zob *zcnObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	fmt.Printf("\nData shards: %v\n", zob.alloc.DataShards)
	defer func() {
		fmt.Printf("\nError: %v\n", err)
	}()
	var remotePath string
	if bucket == RootBucketName {
		remotePath = filepath.Join(RootPath, object)
	} else {
		remotePath = filepath.Join(RootPath, bucket, object)
	}

	ref, err := getSingleRegularRef(zob.alloc, remotePath)
	if err != nil {
		return nil, err
	}

	objectInfo := minio.ObjectInfo{
		Bucket:  bucket,
		Name:    ref.Name,
		ModTime: ref.UpdatedAt,
		Size:    ref.ActualFileSize,
		IsDir:   ref.Type == Dir,
	}

	f, localPath, err := getFileReader(ctx, zob.alloc, remotePath)
	fCloser := func() {
		f.Close()
		os.Remove(localPath)
	}
	if err != nil {
		return nil, err
	}

	finfo, err := f.Stat()
	if err != nil {
		return nil, err
	}

	startOffset, length, err := rs.GetOffsetLength(finfo.Size())
	if err != nil {
		return nil, err
	}

	r := io.NewSectionReader(f, startOffset, length)
	gr, err = minio.NewGetObjectReaderFromReader(r, objectInfo, opts, fCloser)
	return
}

//ListBuckets Lists directories of root path(/) as buckets.
func (zob *zcnObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	rootRef, err := getSingleRegularRef(zob.alloc, RootPath)
	if err != nil {
		return nil, err
	}

	dirRefs, err := listRootDir(zob.alloc, "d")
	if err != nil {
		return nil, err
	}

	//Consider root path as bucket as well.
	buckets = append(buckets, minio.BucketInfo{
		Name:    RootBucketName,
		Created: rootRef.CreatedAt,
	})

	for _, dirRef := range dirRefs {
		buckets = append(buckets, minio.BucketInfo{
			Name:    dirRef.Name,
			Created: dirRef.CreatedAt,
		})
	}
	return
}

func (zob *zcnObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	var resultV1 minio.ListObjectsInfo
	resultV1, err = zob.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return
	}

	result.Objects = resultV1.Objects
	result.Prefixes = resultV1.Prefixes
	result.ContinuationToken = continuationToken
	result.NextContinuationToken = resultV1.NextMarker
	result.IsTruncated = resultV1.IsTruncated
	return
}

//ListObjects Lists files of directories as objects
func (zob *zcnObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	//prefix, marker and maxKeys can be implemented
	//delimiter can be implemented as well but would make less sense. so don't use it.
	fmt.Printf("\nList objects; bucket: %v\n, prefix: %v\n, marker: %v\n, delimeter: %v\n, maxkeys: %v\n,", bucket, prefix, marker, delimiter, maxKeys)
	var remotePath, fileType string
	if bucket == RootBucketName {
		remotePath = filepath.Join(RootPath, prefix)
		fileType = File
	} else {
		remotePath = filepath.Join(RootPath, bucket, prefix)
	}

	var ref *sdk.ORef
	ref, err = getSingleRegularRef(zob.alloc, remotePath)
	if err != nil {
		return
	}

	if ref.Type == File {
		return minio.ListObjectsInfo{
				IsTruncated: false,
				Objects: []minio.ObjectInfo{
					{
						Bucket:  bucket,
						Name:    getCommonPrefix(ref.Path),
						Size:    ref.ActualFileSize,
						IsDir:   false,
						ModTime: ref.UpdatedAt,
						AccTime: time.Now(),
					},
				},
			},
			nil
	}

	var objects []minio.ObjectInfo
	var isDelimited bool
	if delimiter != "" {
		isDelimited = true
	}

	refs, isTruncated, nextMarker, prefixes, err := listRegularRefs(zob.alloc, remotePath, marker, fileType, maxKeys, isDelimited)
	if err != nil {
		return minio.ListObjectsInfo{}, err
	}

	fmt.Printf("\nprefixes: %v\n", prefixes)
	for _, ref := range refs {
		fmt.Printf("\n%+v\n", ref)
		// var isDir bool
		// var size int64
		if ref.Type == Dir {
			// fmt.Println("Ref is dir")
			// // size = ref.Size
			// isDir = true
			continue

		}
		// size = ref.ActualFileSize

		// fmt.Println("isDir is ", isDir)
		objInfo := minio.ObjectInfo{
			Bucket:  bucket,
			Name:    ref.Name,
			ModTime: ref.UpdatedAt,
			Size:    ref.ActualFileSize,
			IsDir:   false,
			// ContentType: ref.MimeType,
			ETag: ref.ActualFileHash,
		}

		fmt.Printf("\nObject info: %+v\n", objInfo)
		objects = append(objects, objInfo)
	}

	// prefixes = []string{}
	result.IsTruncated = isTruncated
	result.NextMarker = nextMarker
	result.Objects = objects
	result.Prefixes = prefixes
	return
}

func (zob *zcnObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	//Create a directory; ignore opts
	remotePath := filepath.Join(RootPath, bucket)
	return zob.alloc.CreateDir(remotePath)
}

func (zob *zcnObjects) PutObject(ctx context.Context, bucket, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	fmt.Println("Put object is called with bucket and object; ", bucket, object)
	fmt.Printf("%+v\n", opts)
	var remotePath string
	if bucket == RootBucketName {
		remotePath = filepath.Join(RootPath, object)
	} else {
		remotePath = filepath.Join(RootPath, bucket, object)
	}

	var ref *sdk.ORef
	var isUpdate bool
	ref, err = getSingleRegularRef(zob.alloc, remotePath)
	if err != nil {
		fmt.Println("error occurred: ", err)
		switch zerr := err.(type) {
		case *zerror.Error:
			if zerr.Code != PathDoesNotExist {
				return
			}
		default:
			return
		}
	}

	if ref != nil {
		isUpdate = true
	}

	contentType := opts.UserDefined["content-type"]
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	err = putFile(ctx, zob.alloc, remotePath, contentType, r, r.Size(), isUpdate, false)
	if err != nil {
		return
	}

	objInfo = minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		Size:    r.Size(),
		ModTime: time.Now(),
	}
	return
}

func (zob *zcnObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo, _ []error) {
	si.Backend.Type = madmin.Gateway
	si.Backend.GatewayOnline = true
	return
}
