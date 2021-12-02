package zcn

import (
	"context"
	"net/http"

	"github.com/0chain/gosdk/zboxcore/sdk"
	"github.com/minio/cli"
	"github.com/minio/madmin-go"
	minio "github.com/minio/minio/cmd"
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
		alloc: allocation,
	}

	return zob, nil
}

type zcnObjects struct {
	minio.GatewayUnsupported
	alloc   *sdk.Allocation
	metrics *minio.BackendMetrics
}

func (zob *zcnObjects) Shutdown(ctx context.Context) error {
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
	return nil
}

func (zob *zcnObjects) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, nil
}

func (zob *zcnObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	return nil, nil
}

func (zob *zcnObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, e error) {
	return minio.BucketInfo{}, nil
}

func (zob *zcnObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	return
}

func (zob *zcnObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	return
}

//ListBuckets Lists directories of root path(/) as buckets.
func (zob *zcnObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	//Return directories of Root path as buckets as well as Root path itself as bucket.
	return
}

//ListObjects Lists files of directories as objects
func (zob *zcnObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	//prefix, marker and maxKeys can be implemented
	//delimiter can be implemented as well but would make less sense. so don't use it.
	return
}

func (zob *zcnObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	//Create a directory; ignore opts
	return nil
}

func (zob *zcnObjects) PutObject(ctx context.Context, bucket, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	return
}

func (zob *zcnObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo, _ []error) {
	si.Backend.Type = madmin.Gateway
	si.Backend.GatewayOnline = true
	return
}
