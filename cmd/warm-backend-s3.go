package cmd

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/pkg/madmin"
)

type warmBackendS3 struct {
	client       *minio.Client
	Bucket       string
	Prefix       string
	StorageClass string
}

func (s3 *warmBackendS3) getDest(object string) string {
	destObj := object
	if s3.Prefix != "" {
		destObj = fmt.Sprintf("%s/%s", s3.Prefix, object)
	}
	return destObj
}
func (s3 *warmBackendS3) Put(ctx context.Context, object string, r io.Reader, length int64) error {
	_, err := s3.client.PutObject(ctx, s3.Bucket, s3.getDest(object), r, length, minio.PutObjectOptions{StorageClass: s3.StorageClass})
	return err
}

func (s3 *warmBackendS3) Get(ctx context.Context, object string, opts warmBackendGetOpts) (r io.ReadCloser, err error) {
	gopts := minio.GetObjectOptions{}

	if opts.startOffset >= 0 && opts.length >= 0 {
		if err := gopts.SetRange(opts.startOffset, opts.startOffset+opts.length-1); err != nil {
			return nil, err
		}
	}

	return s3.client.GetObject(ctx, s3.Bucket, s3.getDest(object), gopts)
}

func (s3 *warmBackendS3) Remove(ctx context.Context, object string) error {
	return s3.client.RemoveObject(ctx, s3.Bucket, s3.getDest(object), minio.RemoveObjectOptions{})
}

func newWarmBackendS3(conf madmin.TierS3) (*warmBackendS3, error) {
	u, err := url.Parse(conf.Endpoint)
	if err != nil {
		return nil, err
	}
	creds := credentials.NewStaticV4(conf.AccessKey, conf.SecretKey, "")
	getRemoteTargetInstanceTransportOnce.Do(func() {
		getRemoteTargetInstanceTransport = newGatewayHTTPTransport(10 * time.Minute)
	})
	opts := &minio.Options{
		Creds:     creds,
		Secure:    u.Scheme == "https",
		Transport: getRemoteTargetInstanceTransport,
	}
	client, err := minio.New(u.Host, opts)
	if err != nil {
		return nil, err
	}
	return &warmBackendS3{
		client:       client,
		Bucket:       conf.Bucket,
		Prefix:       strings.TrimSuffix(conf.Prefix, slashSeparator),
		StorageClass: conf.StorageClass,
	}, nil
}
